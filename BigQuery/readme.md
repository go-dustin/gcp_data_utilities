# BigQuery meta data crawler

This script will crawl all of BigQuery datasets, extracting the metadata from each table. It will extract and export the meta data to a CSV, JSON or BigQuery table for easy analysis. 

#### What the metadata contains:
log_date = When the metadata was extracted
Project  = Project name
dataset = Dataset name
table_path / full_table_id, table_name = Table name, various formats
table_type = Table, model, view, etc
created = Date the table was created
modified = Last date the table was updated
expires = The end date if an expiration date has been set
location = Where the table is stored US, EU ..
description = The description text
labels = an array of k/v records of the custom labels 
column_count = Number of columns in the table
column_names = Array of all the column names, order is not retained
partitioning_type = Partion column type, Date, Integer, etc...
range_part_field = Integer partition field
range_part_start, range_part_end, range_part_interval = Integer partitioning parameters 
time_partition_field, time_partition_type = Time partitioning parameters
clustering_fields = Columns that have been set as clustered
size_mb = Total storage size in megabytes
num_rows = Number of rows in the table
avg_byte_per_row, avg_kbyte_per_row = Avg size per row (size_mb / num_rows) 
float, datetime, date, repeated, record, timestamp, etc..... = Number of columns for each column type

#### Some example use cases:
Audit and track table changes over time. Such as table storage size, row count, column changes, column data type changes. 

Find a table in the public data sets that meets certain criteria. So if you need to find a table that has partitioning and clustering, with strings, floats & integers columns with more than 100M rows and 10GB of data. 

## Getting Started

Clone this repo

git clone https://github.com/go-dustin/gcp_data_utilities.git

### Prerequisites

Python 3.5 + 

pip

venv, pyenv (using virtual environment isn't necessary but it's a best practice)

### Installing

pip install -r requirements.txt

pip3 install -r requirements.txt


## Contributing

Please feel free to make changes and request pulls

## Authors

Dustin Williams 

## License

This work is licensed under a [Creative Commons Attribution 4.0 International License]
http://creativecommons.org/licenses/by/4.0

