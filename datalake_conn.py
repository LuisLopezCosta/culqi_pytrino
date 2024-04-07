from trino.dbapi import connect
from trino.auth import BasicAuthentication
import pandas as pd
from dotenv import load_dotenv
import os


load_dotenv()

class DatalakeConn:
    _instance = None

    def __new__(cls, catalog=None):
        if not cls._instance:
            cls._instance = super(DatalakeConn, cls).__new__(cls)
            cls._instance._connection = None
            cls._instance.catalog = catalog
        return cls._instance

    def get_connection(self):
        if not self._connection:
            self._connection = connect(
                host=os.getenv('HOST'),
                auth=BasicAuthentication(os.getenv('USER'),os.getenv('PASS')),
                port=os.getenv('PORT'),
                timezone='America/Lima',
                http_scheme="https",
                catalog=self.catalog
            )
        return self._connection

    def execute_query(self, query):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            cols = [i[0] for i in cursor.description]
            df = pd.DataFrame(result,columns=cols)
            return df
        except Exception as err:
            print(f"Error: {err}")
        finally:
            cursor.close()
    
    # TODO: Puedes agregar aquí tus implementaciones para métodos CREATE/UPDATE/DELETE