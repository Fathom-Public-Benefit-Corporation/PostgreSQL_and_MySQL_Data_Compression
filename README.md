# PostgreSQL_and_MySQL_Data_Compression



## Summary:

### Convert MySQL model to a PostgreSQL

#### Main.py

This code is a Python script that performs a comparison between two databases, MySQL and PostgreSQL. It uses SQLAlchemy, a popular SQL toolkit for Python, to connect to the databases and perform SQL queries. The script contains several functions:

1. `query_database`: This function takes a database engine (created using SQLAlchemy) and an SQL query as input, and returns the result of the query as a Pandas DataFrame. It connects to the database, executes the query, and retrieves the data as a DataFrame.

2. `matching_amounts`: This function compares the total count of records in each table of the MySQL and PostgreSQL databases. It first retrieves the list of table names from the MySQL database using the `query_database` function. Then, for each table, it retrieves the count of records from both the MySQL and PostgreSQL databases using separate SQL queries. It then compares the counts and creates a Pandas DataFrame with information such as table name, match status (True or False), MySQL count, PostgreSQL count, and the difference between the counts.

3. `compair_columns`: This function is not used in the current code as it is commented out. It is intended to compare the columns of a specific table in the MySQL and PostgreSQL databases, but the implementation is incomplete and may not work as expected.

The `if __name__ == '__main__':` block at the end of the code is the entry point of the script. It creates MySQL and PostgreSQL database engine objects using SQLAlchemy, and then calls the `matching_amounts` function with these engine objects to perform the comparison. The result is printed to the console, showing the tables with matching record counts sorted by match status and table name. There are also some commented-out lines that suggest the script may have been intended to perform additional comparisons using the `compair_columns` function, but it is currently not being used.

# Author:
- [John Hellrung](https://www.github.com/hellrungj) 
