# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime
import sqlite3
import requests

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n') 

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    response = requests.get(url)

    parsed_html = BeautifulSoup(response.text, 'html.parser')
    
    tables = parsed_html.find_all('table')
    market_capitalization_table = tables[0]

    market_capitalization_dict = {table : [] for table in table_attribs}

    for tr in market_capitalization_table.find_all('tr')[1:]:
        if len(tr) != 0 : # remove empty rows
            cells = tr.find_all('td')
            market_capitalization_dict[table_attribs[0]].append(cells[1].text.strip())
            market_capitalization_dict[table_attribs[1]].append(float(cells[2].text.strip()))

    log_progress('Data extraction complete. Initiating Transformation process')
    df = pd.DataFrame(market_capitalization_dict) 
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
	information, and adds three columns to the data frame, each
	containing the transformed version of Market Cap column to
	respective currencies'''
    
    exchange_rate_df = pd.read_csv(exchange_rate_csv)
    exchange_rate_df = exchange_rate_df.set_index('Currency')
  
    EUR_rate = exchange_rate_df.loc['EUR','Rate']
    GBP_rate = exchange_rate_df.loc['GBP','Rate']
    INR_rate = exchange_rate_df.loc['INR','Rate']
    
    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * GBP_rate, 2)
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * EUR_rate, 2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * INR_rate, 2)

    log_progress('Data transformation complete. Initiating Loading process')

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
	the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)
    log_progress('Data saved to CSV file')

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
	table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress('Data loaded to Database as a table, Executing queries')
    

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    result = pd.read_sql_query(query_statement, sql_connection)
    print(query_statement)
    log_progress(f'executed: {query_statement}')
    log_progress('Process Complete')
    print(result)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

# initializing variables
url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_csv = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
output_csv = './Largest_banks_data.csv'
db = 'Banks.db'
db_table_name = 'Largest_banks'
log_file = 'code_log.txt'
table_attribs = ['Name', 'MC_USD_Billion']

log_progress('Preliminaries complete. Initiating ETL process')

# extract
market_capitalization_df = extract(url, table_attribs)

#transform
transformed_market_capitalization_df = transform(market_capitalization_df, exchange_rate_csv)

#load
load_to_csv(transformed_market_capitalization_df, output_csv)


# Connect to the SQLite3 service
conn = sqlite3.connect(db)
log_progress('SQL Connection initiated')

load_to_db(transformed_market_capitalization_df, conn, db_table_name)

#Query
query_1 = f'SELECT * FROM Largest_banks'
run_query(query_1, conn)
query_2 = f'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
run_query(query_2, conn)
query_3 = f'SELECT Name from Largest_banks LIMIT 5'
run_query(query_3, conn)

# close connection to SQLite3 service
conn.close()
log_progress('Server Connection closed')
