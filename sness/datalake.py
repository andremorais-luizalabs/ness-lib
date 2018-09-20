"""
Read - Data Lake Integration
Library to abstract the load of dataframes from S3 Data Lake
"""

import datetime
import json
from collections import OrderedDict
import operator
import pprint
from IPython.core.display import display, HTML
import re
from .utils.log import get_logger


_NOW = datetime.datetime.utcnow()
_CURRENT_YEAR = ('%04d' % _NOW.year)
_CURRENT_MONTH = ('%02d' % _NOW.month)
_CURRENT_DAY = ('%02d' % _NOW.day)
_CURRENT_DATE = _CURRENT_YEAR + _CURRENT_MONTH + _CURRENT_DAY
get_logger().debug('_CURRENT_DATE: %s', _CURRENT_DATE)

DEFAULT_PARTITIONS_SIZE = 256

_CLUSTER_MEM = 60

_MEM_AVAILABLE = 0.5

_CATALOG_SIZES = ['short', 'full']
_CATALOG_OUTPUTS = ['terminal', 'list']


def _table2html(table):
    doc, tag, text = Doc().tagtext()
    headers = table.pop(0)
    with tag(
            'table',
            id="catalog",
            klass="table table-striped table-bordered",
            cellspacing="0",
            width="100%"):
        with tag('thead'):
            with tag('tr'):
                for header in headers:
                    with tag('th'):
                        text(header)
        with tag('tbody'):
            for item in table:
                with tag('tr'):
                    for value in item:
                        with tag('th'):
                            if type(value) is datetime.datetime:
                                text(value.strftime('%Y-%m-%d %H:%M:%S'))
                            else:
                                text(str(value) if value else '')

    jsTable = """
<script src="https://code.jquery.com/jquery-1.12.4.js"></script>
<script src="https://cdn.datatables.net/1.10.16/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/1.10.16/js/dataTables.bootstrap.min.js"></script>
<link rel="stylesheet" type="text/css" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">
<link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.10.16/css/dataTables.bootstrap.min.css">
%s
<br><br>
<script>
$(document).ready(function() {
    $('#catalog').DataTable( {
        "pagingType": "full_numbers"
    } );
} );
</script>
""" % doc.getvalue()
    return jsTable


def _filter_catalog(fcatalog, keys=None):
    headers = fcatalog.pop(0)
    if keys:
        pass
    if keys.get('namespace'):
        fcatalog = [row for row in fcatalog if keys.get('namespace') == row[1]]
    if keys.get('dataset'):
        fcatalog = [row for row in fcatalog if keys.get('dataset') == row[2]]
    if keys.get('zone'):
        fcatalog = [row for row in fcatalog if keys.get('zone') == row[0]]

    fcatalog.insert(0, headers)
    return fcatalog


def catalog(namespace=None,
            dataset=None,
            zone=None,
            etl_type=None,
            output_type="html",
            mode="cache"):
    """
    Print the catalog of namespaces, datasets and zone of the existing data.
    namespace is namespace filter
    dataset is dataset filter
    zone is zone filter
    etl_type is etl_type filter
    output_type is output format. Accepted values are html, terminal and json.
    """
    complete = False
    if mode == "complete":
        headers = ['Zone', 'Namespace', 'Dataset', 'Etl Type', 'Partition Columns', 'Updated At']
        complete = True
        output_type = "json"
    else:
        raise Exception("The accepted value of \"mode\" is \"complete \" (so far)")


    catalog = [headers]
    if namespace is None and dataset is None and zone is None:
        catalog = show_all_catalog(catalog, complete=complete)
    elif dataset is None and zone is None:
        catalog = show_all_catalog(catalog, [namespace], complete=complete)
    elif namespace is None and zone is None:
        catalog = show_all_catalog(catalog, [dataset], complete=complete)
    elif namespace is None and dataset is None:
        catalog = show_all_catalog(catalog, [zone], complete=complete)
    elif zone is None:
        catalog = show_all_catalog(catalog, [namespace, dataset], complete=complete)
    elif namespace is None:
        catalog = show_all_catalog(catalog, [zone, dataset], complete=complete)
    else:
        catalog = show_all_catalog(catalog, [zone, namespace], complete=complete)

    if output_type == "html":
        display(HTML(_table2html(catalog)))
    elif output_type == "json":
        return json.dumps(catalog)
    else:
        raise Exception('Output type not recognized!')

    return


def show_all_catalog(catalogs, sub_path=None, complete=False):
    """
    Print the entire catalog of datasets.
    This method is called when all the arguments of the new_catalog method are none.
    """
    metadata_files = get_list_of_files(paths.METADATA_BUCKET,
                                       'ifood-databricks/etl/')
    metadata_keys = []
    sub = 'metadata'
    if sub_path:
        sub_path.append(sub)
        matches = metadata_files
        for path in sub_path:
            path = "/" + path + "/"
            matches = find_pattern(matches, path)
        for match in matches:
            metadata_keys.append(
                match.split('/metadata/')[0] + "/" + sub + "/")
    else:
        for zone in metadata_files:
            if sub in zone:
                metadata_keys.append(
                    zone.split('/metadata/')[0] + "/" + sub + "/")
    metadata_keys = list(set(metadata_keys))
    metadata_keys.sort()

    catalogs = get_catalog(metadata_keys, catalogs, complete)
    return catalogs


def get_catalog(metadata_keys, catalogs, complete=False):
    """
    Get specifc catalog with the provided arguments. Print this specifc dataset.
    """
    bucket = paths.METADATA_BUCKET
    list_of_files = list(
    path_files = []
    for metadata_key in metadata_keys:
        matches = find_pattern(list_of_files, metadata_key)
        if matches:
            path_files.append(max(matches))

    for path in path_files:
        keys = path.split('/')
        timestamp = keys[6]
        timestamp = time_id_to_string(timestamp)
        if complete is False:
            catalog_list = [keys[2], keys[3], keys[4], timestamp]
        else:
            etltype = ''
            partition_columns = ''
            tmp_metadata = load_metadata("s3://%s/%s" % (bucket, path))
            if tmp_metadata:
                data_info = tmp_metadata.get('data_information')
                if data_info:
                    etltype = data_info.get('etl_type')
                    partition_columns = data_info.get('partition_columns')
                    if partition_columns:
                        partition_columns = ",".join(partition_columns)
            catalog_list = [keys[2], keys[3], keys[4], etltype, partition_columns, timestamp.strftime('%Y-%m-%d %H:%M:%S')]

        catalogs.append(catalog_list)

    return catalogs


def find_pattern(paths, pattern):
    """
    Find pattter in a list of strings and return other list with the items that match the pattern
    """
    regex = re.compile(".*" + pattern + ".*")
    matches = matches = [string for string in paths if re.match(regex, string)]
    return matches
