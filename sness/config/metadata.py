INSERT_METADATA = '''INSERT INTO dataset_catalog (zone, namespace, dataset, 
columns, last_update, file_format, bucket, source_path, destination_path, 
partition_columns, etl_type) VALUES ('{zone}', '{namespace}', '{dataset}', '{columns}', 
to_timestamp('{last_update}', 'dd-mm-yyyy hh24:mi:ss'), '{file_format}', '{bucket}', '{source_path}', 
'{destination_path}', '{partition_columns}', '{etl_type}');'''

GET_METADATA = '''SELECT zone, namespace, dataset, 
columns, last(last_update), file_format, bucket, source_path, destination_path, 
partition_columns, etl_type from dataset_catalog ORDER BY last_update DESC GROUP BY zone, namespace, dataset;'''
