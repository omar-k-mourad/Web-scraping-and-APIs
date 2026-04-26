# Web scraping and APIs
Web scraping and APIs to get Data from websites with ETL processes.

### Notebook 1 (ETL for GDP data from wikipedia)
  - Write a data extraction function to retrieve the relevant information from the required URL.

- Transform the available GDP information into 'Billion USD' from 'Million USD'.

- Load the transformed information to the required CSV file and as a database file.

- Run the required query on the database.

- Log the progress of the code with appropriate timestamps.

### Notebook 2 (NbA API)
  [API doucmentation](https://pypi.org/project/nba_api/)
- we will use the NBA API to determine how well the Golden State Warriors performed against the Toronto Raptors.

- determine the number of points the Golden State Warriors won or lost by for each game. 

### Web scraping banking project
- Write a function to extract the tabular information from the given URL under the heading By Market Capitalization, and save it to a data frame.

- Write a function to transform the data frame by adding columns for Market Capitalization in GBP, EUR, and INR, rounded to 2 decimal places, based on the exchange rate information shared as a CSV file.

- Write a function to load the transformed data frame to an output CSV file.

- Write a function to load the transformed data frame to an SQL database server as a table.

- Write a function to run queries on the database table.

- Run the following queries on the database table: a. Extract the information for the London office, that is Name and MC_GBP_Billion b. Extract the information for the Berlin office, that is Name and MC_EUR_Billion c. Extract the information for New Delhi office, that is Name and MC_INR_Billion

- Write a function to log the progress of the code.

- While executing the data initialization commands and function calls, maintain appropriate log entries.
