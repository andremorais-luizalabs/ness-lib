INSERT_METADATA = "INSERT INTO dataset_catalog (zone, namespace, dataset, " \
                  "columns, last_update, file_format, bucket, source_path, " \
                  "destination_path, partition_columns) VALUES ({zone}, " \
                  "{namespace}, {dataset}, {columns}, {last_update}, {file_format}, " \
                  "{bucket}, {source_path}, {destination_path}, {partition_columns});"

GET_METADATA = "SELECT * from dataset_catalog where zone = {zone} and namespace = {namespace} " \
               "and dataset = {dataset} and bucket = {bucket} ORDER BY last_update DESC;"
