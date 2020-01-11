from google.cloud import bigquery
from collections import Counter
import argparse
import csv


parser = argparse.ArgumentParser(description='Videos to images')
parser.add_argument('project', type=str, help='The project that contains the BigQuery')
parser.add_argument('csv_path', type=str, help='Output dir for image')
parser.add_argument('count_incr', type=int, default=10, help='Log out every x tables. Choose an integer to use as a divisor', )
args = parser.parse_args()
project = args.project
csv_path = args.csv_path
count_incr = args.count_incr

# create bigquery connection obj
client = bigquery.Client(project=project)


def get_table_details(dataset_tablename):
    """
    Extract details using the BQ API
    """
    table = client.get_table(dataset_tablename)
    table_doc = {}
    type_list = list()
    table_schema = table.schema
    for i in table_schema:
        type_list.append(i.field_type)
    schema_type_count = dict(Counter(type_list))
    
    column_list = list()
    for i in table_schema:
        column_list.append(i.name)
    column_list.sort()
    
    table_doc['table_name'] = dataset_tablename
    table_doc['full_table_id'] = table.full_table_id
    table_doc['friendly_name'] = table.friendly_name
    table_doc['project'] = table.project
    table_doc['num_rows'] = table.num_rows
    table_doc['size_mb'] = int(table.num_bytes / 1000000)
    try:
        table_doc['avg_byte_per_row'] = round(table.num_bytes / table.num_rows, 2)
        table_doc['avg_kbyte_per_row'] = round(int(table.num_bytes / 1000) / table.num_rows, 2)
    except:
        table_doc['avg_byte_per_row'] = None
        table_doc['avg_kbyte_per_row'] = None
    table_doc['clustering_fields'] = table.clustering_fields
    table_doc['created'] = table.created.isoformat()
    table_doc['description'] = table.description
    table_doc['expires'] = table.expires
    table_doc['labels'] = table.labels
    table_doc['location'] = table.location
    table_doc['modified'] = table.modified.isoformat()
    table_doc['table_type'] = table.table_type
    table_doc['table_path'] = table.path
    table_doc['range_partitioning'] = table.range_partitioning
    table_doc['partitioning_type'] = table.partitioning_type
    table_doc['time_partitioning'] = table.time_partitioning
    table_doc['schema_type_count'] = schema_type_count
    table_doc['column_names'] = column_list
    
    return table_doc


def crawler(project):
    """
    Crawl all of the datasets and tables in the project
    """
    
    client.list_datasets(project=project)
    datasets = list(client.list_datasets())
    all_tables = []
    counter = 0
    for dataset in datasets:
        dataset_id = dataset.dataset_id
        tables_list = list(client.list_tables(dataset_id)) 
        for table in tables_list:
            dataset_table_name = dataset.dataset_id + '.' + table.table_id
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


def write_to_csv(all_table_details):
    
    keys = all_table_details[0].keys()
    with open(csv_path, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(all_table_details)
        
    print('CSV saved to:', csv_path)
    


def main():
    print('Starting crawl')
    all_table_details = crawler(project)
    result_count = len(all_table_details)
    print(result_count, ' tables found')
    write_to_csv(all_table_details)
    print('Crawl completed')

if __name__ == '__main__':
    main()