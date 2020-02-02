#!/usr/bin/env python
# coding: utf-8

from google.cloud import bigquery
import json
import csv
import argparse
import datetime
from pprint import pprint as prt


parser = argparse.ArgumentParser(description='Table profiler with SQL genertor')
parser.add_argument('-p', '--project',           type=str, help='The project that will execute the BigQuery job', required=True )
parser.add_argument('-P', '--table_project',     type=str, help='The project that contains the BigQuery table')
parser.add_argument('-t', '--dataset_tablename', type=str, help='The name of the dataset & table: dataset.tablename')
parser.add_argument('-o', '--output_dir',        type=str, help='Output directory where you wan to save CSV & JSON data to')
parser.add_argument('-l', '--table_size_limit',  type=int, help='Allows the script to run queries against tables that are larger than 1TB')
parser.add_argument('-r', '--run_query',                   help='Run query and save results to a local file, requires -c CSV or -j JSON paramters',    action='store_true')
parser.add_argument('-s', '--save_sql',                    help='Save the profiling SQL to a local SQL file',     action='store_true')
parser.add_argument('-c', '--save_csv',                    help='Save the profiling CSV to a local SQL file',     action='store_true')
parser.add_argument('-j', '--save_json',                   help='Save the profiling JSON to a local SQL file',    action='store_true')
parser.add_argument('-d', '--show_sql',                    help='Print the SQL query to the terminal',            action='store_true')
parser.add_argument('-D', '--show_profile',                help='Print the query results to the terminal',        action='store_true')

args = parser.parse_args()
project           = args.project
table_project     = args.table_project
dataset_tablename = args.dataset_tablename
output_dir        = args.output_dir
table_size_limit  = args.table_size_limit
run_query         = args.run_query
save_sql          = args.save_sql
save_csv          = args.save_csv
save_json         = args.save_json
show_sql          = args.show_sql
show_profile      = args.show_profile


### Get table metadata
def get_schema(table_project, dataset_tablename):

    client = bigquery.Client(project=table_project)
    table = client.get_table(dataset_tablename)
    table_schema = table.schema

    fields_ls = []
    unnest_cols = []
    for field in table_schema:
        path = field.name
        if len(field.fields) > 0:
            for i in field.fields:
                path2 = path + '__' + i.name
                unnest_cols.append(path)
                if len(i.fields) > 1:
                    for a in i.fields:
                        path3 = path2 + '__' + a.name
                        unnest_cols.append(i.name)
                        if len(a.fields) > 1:
                            for b in a.fields:
                                path4 = path3 + '__' + b.name
                                unnest_cols.append((a.name))
                                rec = {'name': path4, 'col_name': b.name, 'mode': b.mode, 'field_type' : b.field_type, 'path':  a.name}
                                fields_ls.append(rec)
                        else:
                            rec = {'name': path3, 'col_name': a.name, 'mode': a.mode, 'field_type' : a.field_type, 'path':  i.name }
                            fields_ls.append(rec)
                else:
                    rec = {'name': path2, 'col_name': i.name, 'mode': i.mode, 'field_type' : i.field_type, 'path':  field.name }
                    fields_ls.append(rec)
        else:
            rec = {'name': path, 'col_name': field.name, 'mode': field.mode, 'field_type' : field.field_type, 'path':  None }
            fields_ls.append(rec)

    # nested columns need to be in order to unnest properly
    seen = set()
    unnest_cols = [x for x in unnest_cols if (x not in seen) and (not seen.add(x))]
    
    return fields_ls, unnest_cols


def sql_cols(fields_ls):

    table_schema = { 'STRING'   : [],
                     'NUMBERS'  : [],
                     'TIME'     : [],
                     'BOOLEAN'  : [],
                     'REPEATED' : [],
                     'STRUCT'   : []}

    for sf in fields_ls:
        if sf['mode'] == 'REPEATED':
            table_schema['REPEATED'].append((sf['col_name'], sf['name'], sf['mode']))
        elif sf['field_type'] == 'NUMERIC' or sf['field_type'] == 'FLOAT' or sf['field_type'] == 'INTEGER':
            table_schema['NUMBERS'].append((sf['col_name'], sf['name'], sf['mode']))
        elif sf['field_type'] == 'DATE' or sf['field_type'] == 'DATETIME' or sf['field_type'] == 'DATETIME' or sf['field_type'] == 'TIMESTAMP':
            table_schema['TIME'].append((sf['col_name'], sf['name'], sf['mode']))
        elif sf['field_type'] == 'BYTES':
            pass
        else:
            table_schema[sf['field_type']].append((sf['col_name'], sf['name'], sf['mode']))

    return table_schema


### SQL Generator
def empty_null_counter(comment, column_name):
    
    query_snipit = """{spacer}{comment}
        ROUND(
              IEEE_DIVIDE( SUM(CASE
                                    WHEN `{column_name}` IS NULL THEN 1
                                    WHEN  CAST(`{column_name}` AS STRING) = "" THEN 1
                                    ELSE 0
                               END),
                           count(`{column_name}`) 
                         ), 1
              ){spacer}  AS {alias_name}_null_empty_perct,"""

    return query_snipit.replace('{column_name}', column_name).replace('{comment}', comment)


def string_profiler(column_name, alias_name, field_mode):
    
    comment = "# ▼ Column: {column_name}, Type: String ▼"
    query_snipit = """        COUNT(DISTINCT `{column_name}`) {spacer} AS {alias_name}_count_distinct,
        COUNT(`{column_name}`) {spacer} AS {alias_name}_count,
        MIN(LENGTH(`{column_name}`)) {spacer} AS {alias_name}_char_length_min,
        CAST(ROUND(AVG(LENGTH(`{column_name}`)), 0)AS INT64) {spacer} AS {alias_name}_char_length_avg,
        MAX(LENGTH(`{column_name}`)) {spacer} AS {alias_name}_char_length_max,
        SUM(LENGTH(`{column_name}`)) {spacer} AS {alias_name}_char_total_count,
        APPROX_QUANTILES(CHAR_LENGTH(`{column_name}`), 10) {spacer} AS {alias_name}_quantiles,
        # ▲ """
    
    if field_mode == 'NULLABLE':
        query_snipit = empty_null_counter(comment, column_name) + '\n' + query_snipit
        
    return query_snipit.replace('{column_name}', column_name).replace('{alias_name}', alias_name)


def numbers_profiler(column_name, alias_name, field_mode):
    
    #NUMERIC handles INT64 overflow
    comment = "# ▼ Column: {column_name}, Type: Numeric ▼"
    query_snipit = """        COUNT(`{column_name}`) {spacer} AS {alias_name}_count,
        COUNT(DISTINCT `{column_name}`) {spacer} AS {alias_name}_count_distinct,
        MIN(`{column_name}`) {spacer} AS {alias_name}_min,
        AVG(`{column_name}`) {spacer} AS {alias_name}_avg,
        MAX(`{column_name}`) {spacer} AS {alias_name}_max,
        SUM( CAST(`{column_name}` AS NUMERIC) ) {spacer} AS {alias_name}_sum,
        APPROX_QUANTILES(`{column_name}`, 10) {spacer} AS {alias_name}_approx_quantiles,
        # ▲ """
    
    if field_mode == 'NULLABLE':
        query_snipit = empty_null_counter(comment, column_name) + '\n' + query_snipit
        
    return query_snipit.replace('{column_name}', column_name).replace('{alias_name}', alias_name)


def time_profiler(column_name, alias_name, field_mode):
    
    comment = "# ▼ Column: {column_name}, Type: Time ▼"
    query_snipit = """        COUNT(`{column_name}`) {spacer} AS {alias_name}_count,
        COUNT(DISTINCT `{column_name}`) {spacer} AS {alias_name}_count_distinct,
        MIN(`{column_name}`) {spacer} AS {alias_name}_min,
        MAX(`{column_name}`) {spacer} AS {alias_name}_max,
        DATE_DIFF(MAX(CAST(`{column_name}` AS DATE)), MIN(CAST(`{column_name}` AS DATE)),  DAY) {spacer} AS {alias_name}_day_count,
        DATE_DIFF(MAX(CAST(`{column_name}` AS DATE)), MIN(CAST(`{column_name}` AS DATE)),  YEAR) {spacer} AS {alias_name}_year_count,
        DATE_DIFF(MAX(CAST(`{column_name}` AS DATE)), MIN(CAST(`{column_name}` AS DATE)),  MONTH) {spacer} AS {alias_name}_month_count,
        # ▲ """
    
    if field_mode == 'NULLABLE':
        query_snipit = empty_null_counter(comment, column_name) + '\n' + query_snipit
        
    return query_snipit.replace('{column_name}', column_name).replace('{alias_name}', alias_name)


def boolean_profiler(column_name, alias_name, field_mode):
    
    comment = "# ▼ Column: {column_name}, Type: Boolean ▼"
    query_snipit = """        COUNT(received_timestamp) {spacer} AS {alias_name}_count,
        SUM(CASE 
                WHEN als_unit = True THEN 1
                ELSE 0
            END) {spacer} AS {alias_name}_true,
        SUM(CASE 
                WHEN als_unit = False THEN 1
                ELSE 0
            END) {spacer} AS {alias_name}_false,"""
    
    if field_mode == 'NULLABLE':
        query_snipit = empty_null_counter(comment, column_name) + '\n' + query_snipit
        
    return query_snipit.replace('{column_name}', column_name).replace('{alias_name}', alias_name)


def array_struct_profiler(column_name, alias_name, field_mode):
    
    comment = "# ▼ Column: {column_name}, Type:  ▼"
    query_snipit = """MIN(ARRAY_LENGTH(`{column_name}`)) {spacer} AS {alias_name}_min_array_len,
        CAST(AVG(ARRAY_LENGTH(`{column_name}`)) AS INT64) {spacer} AS {alias_name}_avg_array_len,
        MAX(ARRAY_LENGTH(`{column_name}`)) {spacer} AS {alias_name}_max_array_len,
        # ▲ """
    
    if field_mode == 'NULLABLE':
        query_snipit = empty_null_counter(comment, column_name) + '\n' + query_snipit
        
    return query_snipit.replace('{column_name}', column_name).replace('{alias_name}', alias_name)


def sql_gen(sql_cols_dic, unnest_cols):

    select_statement_ls = []

    for column_type, column_names in sql_cols_dic.items():
        if len(column_names) > 0:
            for column_name, alias_name, field_mode in column_names:
                if column_type == 'STRING':
                    string_statement = string_profiler(column_name, alias_name, field_mode)
                    select_statement_ls.append(string_statement)
                elif column_type == 'NUMBERS':
                    string_statement = numbers_profiler(column_name, alias_name, field_mode)
                    select_statement_ls.append(string_statement)
                elif column_type == 'TIME':
                    string_statement = time_profiler(column_name, alias_name, field_mode)
                    select_statement_ls.append(string_statement)
                elif column_type == 'BOOLEAN':
                    string_statement = boolean_profiler(column_name, alias_name, field_mode)
                    select_statement_ls.append(string_statement)
                elif  column_type == 'REPEATED':
                    string_statement = array_struct_profiler(column_name, alias_name, field_mode)
                    select_statement_ls.append(string_statement)
                elif column_type == 'STRUCT':
                    string_statement = array_struct_profiler(column_name, alias_name, field_mode)
                    select_statement_ls.append(string_statement)
                else:
                    print('Miss:\t', column_name)


    full_table_name = "`{table_project}.{dataset_table}`".replace('{table_project}', table_project).replace('{dataset_table}', dataset_tablename)
    char_length = 0

    # Finds the longest statement and sets the spacers char width
    select_statement_unformatted = '\n'.join(select_statement_ls)
    for i in select_statement_unformatted.split('\n'):
        if 'AS ' in i or '#' in i:
            statement = i.split(' {spacer} ')[0].rstrip()
            statement_len = len(statement)
            if statement_len > char_length:
                char_length = statement_len

    char_length = char_length + 2 # add padding for longest line
    # resizes the spacer so the aliases line up on the right side
    select_statement_formated = []
    for i in select_statement_unformatted.split('\n'):
        if 'AS ' in i:
            statement = i.split('{spacer}')[0].rstrip()
            statement_len = len(statement)
            spacer = ' ' * (char_length - statement_len)
            new_statement = i.replace('{spacer}', spacer)
            select_statement_formated.append(new_statement)
        elif '{comment}' in i or '{spacer}# ▼' in i:
            spacer = ' ' * (char_length + 2)
            new_statement = i.replace('{spacer}', spacer)#.replace('\r', '\n')
            select_statement_formated.append(new_statement)
        else:
            select_statement_formated.append(i)

    # fill the skelton in with the select & unnest statements
    query_skelton = """
    # Created by BigQuery Table Profiler: https://github.com/go-dustin/gcp_data_utilities
    # Empty & Null profile returns Infinity if a divide by zero occurs
    SELECT 
    {select_statement}
    FROM   {full_table_name}"""

    # create the unnest statement 
    nested_statement = ',\n'
    for nested in unnest_cols:
        nested_statement = nested_statement + '        UNNEST({}),\n'.format(nested)
    # remove the trailing chars that will causes an error
    if nested_statement[-2:] == ',\n':
        nested_statement = nested_statement[:-2]
    # add the unnest statement below the FROM statement
    query_skelton = query_skelton + nested_statement
    # convert the list of select statements to a string
    select_statement = '\n'.join(select_statement_formated)
    # fill in the select statements & the table name
    query = query_skelton.replace('{select_statement}', select_statement).replace('{full_table_name}', full_table_name)

    return query

### End SQL geneerator 


def get_estimate(project, query):

    client = bigquery.Client(project=project)
    job_config = bigquery.QueryJobConfig(dry_run=True)
    query_job = client.query((query),job_config=job_config,)
    total_bytes = query_job.total_bytes_processed 
    total_megabytes = int(total_bytes / 1048576)
    total_gigabytes = round(total_bytes / 1073741824, 2)
    
    return total_bytes, total_megabytes, total_gigabytes


def run_profiler(query):
    
    client = bigquery.Client(project=project)
    job_config = bigquery.QueryJobConfig(use_query_cache=False)
    query_job = client.query((query),job_config=job_config,)
    table_profile = dict(list(query_job.result())[0])
    
    return table_profile 



def write_json(output_dir, table_profile):
    
    def datetime_handler(x):
        
        if isinstance(x, datetime.datetime):
            return x.isoformat()
        raise TypeError("Unknown type")
    
    json_path_filename = output_dir + '/' + 'profile_' + dataset_tablename.replace('.', '_') + '.json'
    with open(json_path_filename, 'w') as f:
        json.dump(table_profile, f, indent=4, default=datetime_handler)
        

def write_csv(output_dir, table_profile):
    csv_path_filename = output_dir + '/' + 'profile_' + dataset_tablename.replace('.', '_') + '.csv'
    with open(csv_path_filename, 'w', newline='') as csvfile:
        fieldnames = list(table_profile.keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(table_profile)
    

def write_sql(output_dir, query):
    sql_path_filename = output_dir + '/' + 'profile_' + dataset_tablename.replace('.', '_') + '.sql'
    with open(sql_path_filename, "w") as f:
        f.write(query)


def main():
    
    global project, table_project, dataset_tablename, output_dir, table_size_limit, run_query
    global save_sql, save_csv, save_json, show_sql, show_profile

    if table_project == None:
        table_project = project

    # Generate query  &  dry run check
    fields_ls, unnest_cols = get_schema(table_project, dataset_tablename)
    sql_cols_dic = sql_cols(fields_ls)
    query = sql_gen(sql_cols_dic, unnest_cols)
    total_bytes, total_megabytes, total_gigabytes = get_estimate(project, query)
    print('KB: {}\nMB: {}\nGB: {}'.format(total_bytes, total_megabytes, total_gigabytes))

    if save_sql == True:
        write_sql(output_dir, query)

    if show_sql == True:
        print('Display query', query)

    # run query 
    if total_gigabytes <= table_size_limit and run_query == True and save_csv == True or save_json == True or show_profile == True:
        table_profile = run_profiler(query)
        # Replace objects tht can't be serialized to json
        for k, v in table_profile.items():
            if 'sum' in k:
                table_profile[k] = int(v.to_eng_string())
            elif v == float('inf'):
                table_profile[k] = None
            elif isinstance(v, datetime.datetime) == True:
                table_profile[k] = v.isoformat()
        
        if save_csv == True:
            write_csv(output_dir, table_profile)
        
        if save_json == True:
            write_json(output_dir, table_profile)
                
        if show_profile == True:
            prt(table_profile)
    else:
        print('Query did not run')

        


if __name__ == '__main__':
    main()


