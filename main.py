import pandas as pd
import numpy as np
import pymysql
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.sql import text


def query_database(database_engine: sqlalchemy.Engine, query: str) -> pd.DataFrame:
    """Connect to a database and then queries that database based on SQL Engine and Query provided. Returns a dataframe.

    Args:
        database_engine (sqlalchemy.Engine): SQL Alchemy Database Connection Engine
        query (str): SQL Query

    Returns:
        pd.DataFrame: The data returned from the query provided.
    """
    frame: pd.DataFrame
    with database_engine.connect() as dbConnection:
        frame = pd.read_sql(text(query), dbConnection)
        pd.set_option('display.expand_frame_repr', False)
    return frame


def matching_amounts(mySQL_Engine: sqlalchemy.Engine, postgreSQL_Engine: sqlalchemy.Engine) -> pd.DataFrame:
    """Comparing the total count of records (ALIAS: Amount) on each table in the MYSQL and PostgreSQL databases.

    Args:
        mySQL_Engine (sqlalchemy.Engine): MYSQL Connection Object
        postgreSQL_Engine (sqlalchemy.Engine): MYSQL Connection Object

    Returns:
        pd.DataFrame: Data table of matching table counts comparing each table in MSSQL and PostgresSQL databases.
    """
    mySQL_tables_query: any = query_database(
        mySQL_Engine, "SHOW TABLES FROM test;")

    df = pd.DataFrame()
    for table_name in mySQL_tables_query["Tables_in_test"]:
        mySQL_query: any = query_database(
            mySQL_Engine, f"SELECT COUNT(*) AS amount FROM test.{table_name};")
        postgreSQL_query: any = query_database(
            postgreSQL_Engine, f"SELECT COUNT(*) AS amount FROM public.{table_name};")

        mssql_amount: int = mySQL_query.iloc[0]["amount"]
        postgreSQL_amount: int = postgreSQL_query.iloc[0]["amount"]

        res: pd.DataFrame = pd.DataFrame.from_dict({"table_name": [table_name], "match": [mssql_amount == postgreSQL_amount],
                                                    'mssql_count': [mssql_amount], 'postgresql_count': [postgreSQL_amount],
                                                    'difference': [mssql_amount-postgreSQL_amount]})

        df: pd.DataFrame = pd.concat([df, res])
    return df


def compair_columns(mySQL_query: any, postgreSQL_query: any) -> pd.DataFrame:
    """Compares columns on one table for MYSQL and PostgresSQL databases.

    Args:
        mySQL_query (any): MYSQL query object
        postgreSQL_query (any): PostgreSQL query object

    Returns:
        pd.DataFrame: Returns data table of compares mssql_value and postgresql_value.
    """    df = pd.DataFrame()
    for mySQL_index, mySQL_row in mySQL_query.iterrows():
        for postgreSQL_index, postgreSQL_row in postgreSQL_query.iterrows():
            var = str(mySQL_row)
            var2 = str(postgreSQL_row)
            if (var != var2):
                res: pd.DataFrame = pd.DataFrame.from_dict({"table_index": [mySQL_index], 'mssql_value': [mySQL_row],
                                                            'postgresql_value': [postgreSQL_row]})
                df: pd.DataFrame = pd.concat([df, res])
    return df


if __name__ == '__main__':
    mySQL_Engine: sqlalchemy.Engine = create_engine(
        'mysql+pymysql://root:Natioh22@127.0.0.1', pool_recycle=3600)
    postgreSQL_Engine: sqlalchemy.Engine = create_engine(
        'postgresql://postgres:Natioh22@127.0.0.1', pool_recycle=3600)

    result: pd.DataFrame = matching_amounts(mySQL_Engine, postgreSQL_Engine)
    print(result.sort_values(by=['match', 'table_name']))

    # mySQL_query: any = query_database(
    #     mySQL_Engine, f"SELECT element_id, survey_item_number,scale_id AS amount FROM test.survey_booklet_locations;")
    # postgreSQL_query: any = query_database(
    #     postgreSQL_Engine, f"SELECT element_id, survey_item_number,scale_id AS amount FROM public.survey_booklet_locations;")

    # result = compair_columns(mySQL_query, postgreSQL_query)
    # print(result)
