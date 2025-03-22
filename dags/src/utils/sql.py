from src.utils.decorators import time_it
import pandas as pd
from sqlalchemy import text
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os

class SQLEngine:
    '''
    Class to connect to a MySQL database and execute queries, allows with statement for connection management.
    Atributes:
        _SQL_USER : str : MySQL user
        _SQL_PASSWORD : str : MySQL password
        _SQL_HOST : str : MySQL host
        _DATABASE : str : MySQL database
        connection : sqlalchemy.engine.base.Connection : connection to the database
    Methods:
        get_connection() : None : connects to the database
        close_connection() : None : closes the connection
        get_data(sql_query_file : str) -> pd.DataFrame : returns the result of a query as a DataFrame
        export_data(dataframe : pd.DataFrame, table_name : str, if_exists : str = 'replace') : None : exports a DataFrame into a MySQL table
    '''
    def __init__(self) -> None:
        load_dotenv(r"dags/src/secrets/.env")

        self._SQL_USER = os.getenv('SQL_USER')
        self._SQL_PASSWORD = os.getenv('SQL_PASSWORD')
        # The host should be host.docker.internal if you want to connect to
        # the host machine's SQL server, and not to the Docker container's:
        self._SQL_HOST = os.getenv('SQL_HOST')

        self.connection = None

    def create_database_if_not_exists(self, database: str = None) -> None:
        '''
        Creates the database if it doesn't already exist.
        '''
        try:
            connection_string_without_db = f"mysql+pymysql://{self._SQL_USER}:{self._SQL_PASSWORD}@{self._SQL_HOST}"
            engine = create_engine(connection_string_without_db)
            with engine.connect() as conn:
                conn.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
                print(f"Database '{database}' ensured to exist.")
        except SQLAlchemyError as e:
            print(f"Error creating database: {e}")
            raise

    def get_connection(self, database: str = None) -> None:
        '''
        Establishes a connection to the database and defines the self.connection as the sqlalchemy.engine.base.Connection.
        '''
        # Ensure the database exists
        self.create_database_if_not_exists(database)
        # Connect to database
        self.connection_string = f"mysql+pymysql://{self._SQL_USER}:{self._SQL_PASSWORD}@{self._SQL_HOST}/{database}"
        self.engine = create_engine(self.connection_string)
        self.connection = self.engine.connect()
    
    def close_connection(self) -> None:
        '''
        Closes the connection to the database.
        '''
        self.connection.close()
        self.connection = None

    @time_it
    def get_data(self, sql_query: str) -> pd.DataFrame:
        """
        Executes a SQL query and returns the result as a pandas DataFrame.
        
        Parameters:
            sql_query : str
                The SQL query to execute.
        
        Returns:
            pd.DataFrame
                The result of the query.
        """
        return pd.read_sql(sql_query, self.connection)
    
    @time_it
    def insert_data(self, dataframe: pd.DataFrame, table_name: str, if_exists: str = 'append') -> None:
        """
        Inserts the DataFrame into the specified MySQL table.
        
        If the table exists, the DataFrame rows will be appended to it.
        If the table does not exist, it will be created.
        
        Parameters:
            dataframe : pd.DataFrame
                DataFrame containing the data to insert.
            table_name : str
                Name of the table into which data will be inserted.
            if_exists : str, optional
                Behavior when the table already exists. Default is 'append'.
                Other options include 'fail' or 'replace'.
        """
        dataframe.to_sql(table_name, self.connection, if_exists=if_exists, index=False)

    @time_it
    def update_cell_value(self, table_name: str, column: str, new_value, where_clause: str, where_params: dict) -> None:
        """
        Updates a single cell value in the specified MySQL table.

        Parameters:
            table_name : str
                The name of the table in which the update will occur.
            column : str
                The column that will be updated.
            new_value
                The new value to assign to the cell.
            where_clause : str
                The condition (without the "WHERE" keyword) that identifies the row(s) to update.
                For example: "id = :id" or "channelId = :channelId".
            where_params : dict
                A dictionary of parameters corresponding to the placeholders in the where_clause.
                For example: {"channelId": "ABC123"}.
        """
        try:
            # Construct the SQL query string with parameter placeholders.
            query_str = f"UPDATE {table_name} SET {column} = :new_value WHERE {where_clause}"
            # Wrap the query with SQLAlchemy's text() so that named parameters are properly handled.
            query = text(query_str)
            
            # Prepare the parameters dictionary.
            params = {"new_value": new_value}
            if where_params:
                params.update(where_params)
            
            # Execute the update query.
            self.connection.execute(query, params)
            print(f"Updated {table_name}: set {column} = {new_value} where {where_clause} with params {where_params}")
        except SQLAlchemyError as e:
            print(f"Error updating cell value: {e}")
            raise

    def __enter__(self):
        print(f"Conectando a {self._SQL_HOST}...")
        self.get_connection()
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close_connection()