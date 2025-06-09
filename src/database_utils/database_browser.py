from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from os import getenv
from pandas import read_sql_query, DataFrame


class DatabaseBrowser:
    def __init__(self, connection_string: str):
        url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        self.engine = create_engine(url)
        self.connection = self.engine.connect()

    
    @classmethod
    def new_with_jundsoft_connection(cls):
        # example connection string: "DRIVER={ODBC Driver 17 for SQL Server};SERVER=dagger;DATABASE=test;UID=user;PWD=password"
        conn_string = getenv("JUNDSOFT_CONNECTION_STRING")
        if conn_string is None:
            raise ValueError("JUNDSOFT_CONNECTION_STRING is not set")
        return cls(conn_string)


    def get_query_result(self, query: str) -> DataFrame:
        df = read_sql_query(query, self.connection)
        return df
    

    def __del__(self):
        self.connection.close()
        self.engine.dispose()


if __name__ == "__main__":
    db_browser = DatabaseBrowser.new_with_jundsoft_connection()
    query = "SELECT * FROM TB_SY_05;"
    result = db_browser.get_query_result(query)
    print(result)
