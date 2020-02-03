
# BigQuery Table statistics profiler

This script will generate an SQL Query that will create common statistics. Generates hundreds/thousands of lines of SQL code with one very easy command.

**example:**
python3 bq_table_profiler.py -p myProj -o /home/myusr/data -t medicare.medicare_comments_sim_prepped -l 2000  -r -j -D -s -c 

## Features

1. Generate SQL query 
2. Save SQL query to a .sql file
3. Print the SQL to the terminal
4. Run the SQL query
   - Display results in the terminal's stdout 
   - Save the results to a local JSON file
   - Save the results to a local CSV file
5. Default table size limit of 1TB which can be overrode for larger tables
6. Unpacks nested columns and flattens column name with '__' to indicate a '.' 
7. Automatically converts integers to numeric types to avoid overflows on large sums
8. Data sampling to reduce the time it takes to profile very large tables

#### What the query contains:
* Count distinct
* Sum/ Min/Max/Avg for all relevant fields
* Percentage of NULLs & empty strings in a column
* Quantiles
* String lengths
* Column date range

#### Some example use cases:
Data science & analyst data profiling for discovery 

Track table changes over time

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
