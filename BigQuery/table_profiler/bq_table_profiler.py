import sys
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from collections import Counter
import argparse
import csv
from collections import OrderedDict
import datetime as dt
import json

# CLI argument parser
parser = argparse.ArgumentParser(description='Crawl all datasets & tables in a project and save the table details')
parser.add_argument('--project',         type=str, help='The project that contains the BigQuery', required=True )
parser.add_argument('--csv_path',        type=str, help='Output dir for CSV')
parser.add_argument('--json_path',       type=str, help='Output dir for JSON')
parser.add_argument('--output_bq_table', type=str, help='Table to write to in BigQuery. Ex: mydataset.mytable')
parser.add_argument('--count_incr',      type=int, help='Log out every x tables. Choose an integer to use as a divisor', default=10)
args = parser.parse_args()
project         = args.project
csv_path        = args.csv_path
json_path       = args.json_path
output_bq_table = args.output_bq_table
count_incr      = args.count_incr


# BigQuery output
if output_bq_table != None:
    des_proj, dataset_n, table_n = output_bq_table.split('.')

# CSV ouput
if csv_path == None and output_bq_table == None and json_path == None:
    sys.exit('No output target, set --csv_path or --output_bq_table')


# create bigquery connection obj
client = bigquery.Client(project=project)


# Schema for BigQuery output table
schema = [bigquery.SchemaField("log_date",              "DATETIME", mode="NULLABLE", description='Date & time of the crawl'),
          bigquery.SchemaField("project",               "STRING",   mode="NULLABLE"), 
          bigquery.SchemaField("dataset",               "STRING",   mode="NULLABLE"),
          bigquery.SchemaField("table_path",            "STRING",   mode="NULLABLE"),
          bigquery.SchemaField("full_table_id",         "STRING",   mode="NULLABLE"),
          bigquery.SchemaField("table_name",            "STRING",   mode="NULLABLE"),
          bigquery.SchemaField("friendly_name",         "STRING",   mode="NULLABLE"), #this is legacy, might not be relevant anymore
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
          bigquery.SchemaField("range_part_field",      "STRING",   mode="NULLABLE", description='Integer partition field'),
          bigquery.SchemaField("range_part_end",        "INT64",    mode="NULLABLE", description='Integer partition end point'),
          bigquery.SchemaField("range_part_interval",   "INT64",    mode="NULLABLE", description='Integer partition increment interval'),
          bigquery.SchemaField("range_part_start",      "INT64",    mode="NULLABLE", description='Integer partition start point'),
          bigquery.SchemaField("time_partition_field",  "STRING",   mode="NULLABLE"),
          bigquery.SchemaField("time_partition_type",   "STRING",   mode="NULLABLE"),
          bigquery.SchemaField("clustering_fields",     "STRING",   mode="REPEATED"),
          bigquery.SchemaField("size_mb",               "INT64",    mode="NULLABLE"),
          bigquery.SchemaField("num_rows",              "INT64",    mode="NULLABLE"),
          bigquery.SchemaField("avg_byte_per_row",      "NUMERIC",  mode="NULLABLE"),
          bigquery.SchemaField("avg_kbyte_per_row",     "NUMERIC",  mode="NULLABLE"),
          bigquery.SchemaField("float",                 "INT64",    mode="NULLABLE", description='Number of FLOAT columns in the table'),
          bigquery.SchemaField("datetime",              "INT64",    mode="NULLABLE", description='Number of DATETIME columns in the table'),
          bigquery.SchemaField("date",                  "INT64",    mode="NULLABLE", description='Number of DATE columns in the table'),
          bigquery.SchemaField("repeated",              "INT64",    mode="NULLABLE", description='Number of REPEATED columns in the table'),
          bigquery.SchemaField("record",                "INT64",    mode="NULLABLE", description='Number of RECORD columns in the table'),
          bigquery.SchemaField("timestamp",             "INT64",    mode="NULLABLE", description='Number of TIMESTAMP columns in the table'),
          bigquery.SchemaField("time",                  "INT64",    mode="NULLABLE", description='Number of TIME columns in the table'),
          bigquery.SchemaField("numeric",               "INT64",    mode="NULLABLE", description='Number of NUMERIC columns in the table'),
          bigquery.SchemaField("bytes",                 "INT64",    mode="NULLABLE", description='Number of BYTES columns in the table'),
          bigquery.SchemaField("struct",                "INT64",    mode="NULLABLE", description='Number of STRUCT columns in the table'),
          bigquery.SchemaField("boolean",               "INT64",    mode="NULLABLE", description='Number of BOOLEAN columns in the table'),
          bigquery.SchemaField("integer",               "INT64",    mode="NULLABLE", description='Number of INTEGER columns in the table'),
          bigquery.SchemaField("geography",             "INT64",    mode="NULLABLE", description='Number of GEOGRAPHY columns in the table'),
          bigquery.SchemaField("string",                "INT64",    mode="NULLABLE", description='Number of STRING columns in the table'),
          ]


def crawler(project):
    """
    Crawl all of the datasets and tables in the project
    """
    # Set the project that will be crawled
    client.list_datasets(project=project)
    datasets = list(client.list_datasets())
    all_tables = []
    counter = 0
    
    # generate a list of all the tables in the project
    for dataset in datasets:
        dataset_nm = dataset.dataset_id
        tables_list = list(client.list_tables(dataset_nm)) # API call
        for table in tables_list:
            dataset_table_name = dataset_nm + '.' + table.table_id
            all_tables.append(dataset_table_name)
            counter += 1
            if counter % count_incr == 0:
                print(counter, 'tables crawled')
    
    # Prints final count
    print(counter, 'tables crawled')

    all_table_details = []
    for table in all_tables:
        # Extract the table metadata 
        table_details = get_table_details(table)
        all_table_details.append(table_details)

    return all_table_details


def get_table_details(dataset_tablename):
    """
    Extract table details
    """
    
    dataset = dataset_tablename.split('.')[0]
    table_doc = OrderedDict() 
    table_doc['log_date']    = dt.datetime.now()
    table_doc['dataset']     = dataset
    table_doc['table_name']  = dataset_tablename
    
    try:
        table = client.get_table(dataset_tablename) # API call
        table_schema = table.schema
        type_list = []
        column_list = []
        for i in table_schema:
            if i.mode == 'REPEATED':
                name = i.name
                for j in i.fields:
                    new_name = name + '.' + j.name
                    column_list.append(new_name)
                    type_list.append(j.field_type)
            else:
                column_list.append(i.name)
                type_list.append(i.field_type)

        column_list.sort()
        schema_types_count = dict(Counter(type_list))
            #lower case to match column naming convention 
        schema_types_count = {key.lower() if type(key) == str else key: value for key, value in schema_types_count.items()} 

        # Create an ordered dict to ensure column positions don't change & add metadata   
        table_doc['project']           = table.project
        table_doc['table_path']        = table.path
        table_doc['full_table_id']     = table.full_table_id
        table_doc['friendly_name']     = table.friendly_name
        table_doc['table_type']        = table.table_type
        table_doc['created']           = table.created
        table_doc['modified']          = table.modified
        table_doc['expires']           = table.expires
        table_doc['location']          = table.location
        table_doc['description']       = table.description
        table_doc['labels']            = str(table.labels) # convert this to tuples in an array?
        table_doc['column_count']      = len(column_list)
        table_doc['column_names']      = column_list
        table_doc['partitioning_type'] = table.partitioning_type

        # Check if integer partitioning exits if not null the value
        if table.range_partitioning != None:
            table_doc['range_part_field']    = table.range_partitioning.field
            table_doc['range_part_end']      = table.range_partitioning.range_.end
            table_doc['range_part_interval'] = table.range_partitioning.range_.interval
            table_doc['range_part_start']    = table.range_partitioning.range_.start
        else:
            table_doc['range_part_field']    = None
            table_doc['range_part_end']      = None
            table_doc['range_part_interval'] = None
            table_doc['range_part_start']    = None

        # Check if time partitioning exits if not null the value
        if table.time_partitioning != None:
            table_doc['time_partition_field'] = table.time_partitioning.field
            table_doc['time_partition_type']  = table.time_partitioning.type_
        else:
            table_doc['time_partition_field'] = None
            table_doc['time_partition_type']  = None

        # Check if there are clustered fields exits if not null the value
        if table.clustering_fields != None:
            table_doc['clustering_fields'] = table.clustering_fields
        else:
            table_doc['clustering_fields']    = []

        table_doc['size_mb']  = int(table.num_bytes / 1000000)
        table_doc['num_rows'] = table.num_rows
        try:
            table_doc['avg_byte_per_row']  = round(table.num_bytes / table.num_rows, 2)
            table_doc['avg_kbyte_per_row'] = round(int(table.num_bytes / 1000) / table.num_rows, 2)
        except:
            table_doc['avg_byte_per_row']  = None
            table_doc['avg_kbyte_per_row'] = None

        table_doc['float']     = None
        table_doc['datetime']  = None
        table_doc['date']      = None
        table_doc['repeated']  = None
        table_doc['record']    = None
        table_doc['timestamp'] = None
        table_doc['time']      = None
        table_doc['numeric']   = None
        table_doc['bytes']     = None
        table_doc['struct']    = None
        table_doc['boolean']   = None
        table_doc['integer']   = None
        table_doc['geography'] = None
        table_doc['string']    = None

        table_doc.update(schema_types_count)
    except:
        schema_cols = [ 'project','table_path','full_table_id','friendly_name','table_type',
                        'created','modified','expires','location','description','labels','column_count','column_names',
                        'partitioning_type','range_part_field','range_part_end','range_part_interval','range_part_start',
                        'time_partition_field','time_partition_type','size_mb','num_rows','avg_byte_per_row','avg_kbyte_per_row',
                        'float','datetime','date','repeated','record','timestamp','time','numeric','bytes','struct','boolean',
                        'integer','geography','string']

        for i in schema_cols:
            table_doc[i] = None

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
        full_table_name = output_bq_table
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

        
def write_to_json(all_table_details):
    """
    Write the table details to a JSON output file
    """

    def json_date_fixer(dic_vals):
            if isinstance(dic_vals, dt.datetime):
                return dic_vals.__str__()

    with open(json_path, 'w') as outfile:
        json.dump(all_table_details, outfile, default=json_date_fixer)
    

def main():

    print('Starting crawl')

    all_table_details = crawler(project)
    result_count = len(all_table_details)

    print(result_count, ' tables found')

    if csv_path != None:
        write_to_csv(all_table_details)

    if json_path != None:
        write_to_json(all_table_details)

    if output_bq_table != None:
        create_table(output_bq_table, dataset_n, table_n)
        write_to_bq(all_table_details)

    print('Crawl completed')

    
if __name__ == '__main__':
    main()
