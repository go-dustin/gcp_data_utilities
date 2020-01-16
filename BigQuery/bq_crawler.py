import sys
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from collections import Counter
import argparse
import csv
from collections import OrderedDict
import datetime as dt


parser = argparse.ArgumentParser(description='Crawl all datasets & tables in a project and save the table details')
parser.add_argument('--project',         type=str, help='The project that contains the BigQuery', required=True )
parser.add_argument('--csv_path',        type=str, help='Output dir for image')
parser.add_argument('--output_bq_table', type=str, help='Table to write to in BigQuery. Ex: mydataset.mytable')
parser.add_argument('--count_incr',      type=int, help='Log out every x tables. Choose an integer to use as a divisor', default=10)
args = parser.parse_args()
project = args.project
csv_path = args.csv_path
output_bq_table = args.output_bq_table
count_incr = args.count_incr


if output_bq_table != None:
    dataset_n, table_n = output_bq_table.split('.')

if csv_path == None and output_bq_table == None:
    sys.exit('No output target, set --csv_path or --output_bq_table')


# create bigquery connection obj
client = bigquery.Client(project=project)

# BigQuery output table schema
# BigQuery output table schema
schema = [bigquery.SchemaField("log_date",              "DATETIME", mode="NULLABLE"),
          bigquery.SchemaField("project",               "STRING",   mode="NULLABLE"), 
          bigquery.SchemaField("dataset",               "STRING",   mode="NULLABLE"),
          bigquery.SchemaField("table_path",            "STRING",   mode="NULLABLE"),
          bigquery.SchemaField("full_table_id",         "STRING",   mode="NULLABLE"),
          bigquery.SchemaField("table_name",            "STRING",   mode="NULLABLE"),
          bigquery.SchemaField("friendly_name",         "STRING",   mode="NULLABLE"),
          bigquery.SchemaField("table_type",            "STRING",   mode="NULLABLE"),
          bigquery.SchemaField("created",               "DATETIME", mode="NULLABLE"),
          bigquery.SchemaField("modified",              "DATETIME", mode="NULLABLE"),
          bigquery.SchemaField("expires",               "DATETIME", mode="NULLABLE"),
          bigquery.SchemaField("location",              "STRING",   mode="NULLABLE"),
          bigquery.SchemaField("description",           "STRING",   mode="NULLABLE"),
          bigquery.SchemaField("labels",                "STRING",   mode="NULLABLE"),
          bigquery.SchemaField("column_count",          "INT64",    mode="NULLABLE"),
          bigquery.SchemaField("column_names",          "STRING",   mode="REPEATED"),
          bigquery.SchemaField("partitioning_type",     "STRING",   mode="NULLABLE"),
          bigquery.SchemaField("range_part_field",      "STRING",   mode="NULLABLE"),
          bigquery.SchemaField("range_part_end",        "INT64",    mode="NULLABLE"),
          bigquery.SchemaField("range_part_interval",   "INT64",    mode="NULLABLE"),
          bigquery.SchemaField("range_part_start",      "INT64",    mode="NULLABLE"),
          bigquery.SchemaField("time_partition_field",  "STRING",   mode="NULLABLE"),
          bigquery.SchemaField("time_partition_type",   "STRING",   mode="NULLABLE"),
          bigquery.SchemaField("clustering_fields",     "STRING",   mode="REPEATED"),
          bigquery.SchemaField("size_mb",                "INT64",   mode="NULLABLE"),
          bigquery.SchemaField("num_rows",               "INT64",   mode="NULLABLE"),
          bigquery.SchemaField("avg_byte_per_row",       "NUMERIC", mode="NULLABLE"),
          bigquery.SchemaField("avg_kbyte_per_row",      "NUMERIC", mode="NULLABLE"),
          bigquery.SchemaField("FLOAT",                  "INT64",   mode="NULLABLE"),
          bigquery.SchemaField("DATETIME",               "INT64",   mode="NULLABLE"),
          bigquery.SchemaField("DATE",                   "INT64",   mode="NULLABLE"),
          bigquery.SchemaField("REPEATED",               "INT64",   mode="NULLABLE"),
          bigquery.SchemaField("RECORD",                 "INT64",   mode="NULLABLE"),
          bigquery.SchemaField("TIMESTAMP",              "INT64",   mode="NULLABLE"),
          bigquery.SchemaField("TIME",                   "INT64",   mode="NULLABLE"),
          bigquery.SchemaField("NUMERIC",                "INT64",   mode="NULLABLE"),
          bigquery.SchemaField("BYTES",                  "INT64",   mode="NULLABLE"),
          bigquery.SchemaField("STRUCT",                 "INT64",   mode="NULLABLE"),
          bigquery.SchemaField("BOOLEAN",                "INT64",   mode="NULLABLE"),
          bigquery.SchemaField("INTEGER",                "INT64",   mode="NULLABLE"),
          bigquery.SchemaField("GEOGRAPHY",              "INT64",   mode="NULLABLE"),
          bigquery.SchemaField("STRING",                 "INT64",   mode="NULLABLE"),
          ]


def crawler(project):
    """
    Crawl all of the datasets and tables in the project
    """
    
    client.list_datasets(project=project)
    datasets = list(client.list_datasets())
    all_tables = []
    counter = 0
    
    for dataset in datasets:
        dataset_nm = dataset.dataset_id
        tables_list = list(client.list_tables(dataset_nm))
        
        for table in tables_list:
            dataset_table_name = dataset_nm + '.' + table.table_id
            all_tables.append(dataset_table_name)
            counter += 1
            if counter % count_incr == 0:
                print(counter, 'tables crawled')
                
    print(counter, 'tables crawled')

    all_table_details = []
    for table in all_tables:
        table_details = get_table_details(table)
        all_table_details.append(table_details)
        
    return all_table_details


def get_table_details(dataset_tablename):
    """
    Extract details using the BQ API
    """
    dataset = dataset_tablename.split('.')[0]
    table = client.get_table(dataset_tablename)
    
    type_list = list()
    table_schema = table.schema
    for i in table_schema:
        type_list.append(i.field_type)
    schema_types_count= dict(Counter(type_list))

    column_list = list()
    for i in table_schema:
        column_list.append(i.name)
    column_list.sort()
    
    table_doc = OrderedDict()
    table_doc['log_date'] = dt.datetime.now()
    table_doc['project'] = table.project
    table_doc['dataset'] = dataset
    table_doc['table_path'] = table.path
    table_doc['full_table_id'] = table.full_table_id
    table_doc['table_name'] = dataset_tablename
    table_doc['friendly_name'] = table.friendly_name
    table_doc['table_type'] = table.table_type
    table_doc['created'] = table.created
    table_doc['modified'] = table.modified
    table_doc['expires'] = table.expires
    table_doc['location'] = table.location
    table_doc['description'] = table.description
    table_doc['labels'] = str(table.labels) # conver this to tuples in an array?
    table_doc['column_count'] = len(column_list)
    table_doc['column_names'] = column_list
    table_doc['partitioning_type'] = table.partitioning_type
    
    if table.range_partitioning != None:
        range_part = str(table.range_partitioning)
        range_part_field, range_part_end, range_part_interval, range_part_start = range_part.replace('RangePartitioning(', '').replace('range_=PartitionRange(','').replace(')','').split(',')
        table_doc['range_part_field'] = range_part_field.split('=')[1].replace("'","")
        table_doc['range_part_end'] = int(range_part_end.split('=')[1].replace("'",""))
        table_doc['range_part_interval'] = int(range_part_interval.split('=')[1].replace("'",""))
        table_doc['range_part_start'] = int(range_part_start.split('=')[1].replace("'",""))
    else:
        table_doc['range_part_field'] = None
        table_doc['range_part_end'] = None
        table_doc['range_part_interval'] = None
        table_doc['range_part_start'] = None
    try:
        time_partition_field, time_partition_type = table.time_partitioning.replace('TimePartitioning(','').replace(')', '').split(',')
        table_doc['time_partition_field'] = time_partition_field.split('=')[1]
        table_doc['time_partition_type']  = time_partition_type.split('=')[1]
    except:
        table_doc['time_partition_field'] = None
        table_doc['time_partition_type']  = None
    if table.clustering_fields != None:
        table_doc['clustering_fields'] = table.clustering_fields
    else:
        table_doc['clustering_fields'] = []
    table_doc['size_mb'] = int(table.num_bytes / 1000000)

    table_doc['num_rows'] = table.num_rows
    try:
        table_doc['avg_byte_per_row'] = round(table.num_bytes / table.num_rows, 2)
        table_doc['avg_kbyte_per_row'] = round(int(table.num_bytes / 1000) / table.num_rows, 2)
    except:
        table_doc['avg_byte_per_row'] = None
        table_doc['avg_kbyte_per_row'] = None

    table_doc['FLOAT']     = None
    table_doc['DATETIME']  = None
    table_doc['DATE']      = None
    table_doc['REPEATED']  = None
    table_doc['RECORD']    = None
    table_doc['TIMESTAMP'] = None
    table_doc['TIME']      = None
    table_doc['NUMERIC']   = None
    table_doc['BYTES']     = None
    table_doc['STRUCT']    = None
    table_doc['BOOLEAN']   = None
    table_doc['INTEGER']   = None
    table_doc['GEOGRAPHY'] = None
    table_doc['STRING']    = None
    
    table_doc.update(schema_types_count)
    
    return table_doc


def write_to_csv(all_table_details):
    """
    Write the table details to a local csv
    """
    keys = all_table_details[0].keys()
    with open(csv_path, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(all_table_details)
    print('CSV saved to:', csv_path)
    
    
# --- write to BQ
def create_table(output_bq_table, dataset_n, table_n):
    """
    Checks if the output table exists and creates it if needed
    
    """
    dataset = client.dataset(dataset_n)
    table_ref = dataset.table(table_n)
    
    print('checking if output table exists')
    try:
        client.get_table(table_ref)
        table_exists = True
    except NotFound:
        table_exists = False
    
    if table_exists == False:
        full_table_name = project + '.' + output_bq_table
        table = bigquery.Table(full_table_name, schema=schema)
        client.create_table(table)
        print("Created table ", output_bq_table)
        

def write_to_bq(all_table_details):
    """
    Write the table details to the BigQuery output table
    """
    rows = [tuple(row.values()) for row in all_table_details]
    print('num of rows to write',len(rows))
    errors = client.insert_rows(output_bq_table, rows, selected_fields=schema)
    if errors == []:
        print(len(rows), 'written to', output_bq_table )
    else:
        print(errors) 
    

def main():
    print('Starting crawl')
    all_table_details = crawler(project)
    result_count = len(all_table_details)
    print(result_count, ' tables found')
    if csv_path != None:
        write_to_csv(all_table_details)
    
    if output_bq_table != None:
        create_table(output_bq_table, dataset_n, table_n)
        write_to_bq(all_table_details)
    print('Crawl completed')

    
if __name__ == '__main__':
    main()
