from trino.dbapi import connect
from trino.auth import BasicAuthentication
import pandas as pd
from dotenv import load_dotenv
from pypika import Query, Table
import queue
from concurrent import futures
import os
import datetime

load_dotenv()

class DatalakeConn:
    _instance = None
    _workers = 8
    _multi_row_insert_limit = 1000
    
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

    def _execute(self, query):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            cursor.fast_executemany = True
            cursor.execute(query)
            result = cursor.fetchall()
            cols = [i[0] for i in cursor.description]
            df = pd.DataFrame(result,columns=cols)
            return df
        except Exception as err:
            print(f"Error: {err}")
        finally:
            cursor.close()
                    
    def _multi_row_insert(self, batch, cols, table_name, table_schema):
        row_expressions = []
        flg_queue = isinstance(batch, queue.Queue)
        n_rows = batch.qsize() if flg_queue else len(batch)
            
        for i in range(n_rows):
            row_data = []
            row = batch.get() if flg_queue else batch[i]
            
            for v in row:
                if isinstance(v,str):
                    row_data.append(f"'{v}'")
                elif isinstance(v,(int,float,complex)):
                    row_data.append(f"{v}")
                elif isinstance(v,datetime.datetime):
                    row_data.append(f"timestamp '{v}'")
                elif isinstance(v,datetime.date):
                    row_data.append(f"date '{v}'")
            str_values = ','.join(row_data)
            str_row_data = f'({str_values})'
            row_expressions.append(str_row_data)
        
        str_expression = ','.join(row_expressions)
        str_cols = ','.join(cols)
        str_source_cols = ','.join(['source.'+ str(c) for c in cols])
        sql_template = f'''MERGE INTO "{table_schema}"."{table_name}" target 
            USING (
                SELECT * FROM
                (VALUES {str_expression}) as t({str_cols})
            ) source
            ON false WHEN NOT MATCHED THEN INSERT VALUES ({str_source_cols})
            '''
        return self._execute(str(sql_template))
    
    def _process_row(self, row, batch,cols, table_name, table_schema):
        batch.put(row)

        if batch.full():
            self._multi_row_insert(batch,cols, table_name, table_schema)

        return batch
    
    def _bulk_insert(self, df, table_name, table_schema):
        batch = queue.Queue(self._multi_row_insert_limit)
        result = None
        cols = df.columns

        with futures.ThreadPoolExecutor(max_workers=self._workers) as executor:
            todo = []

            for index, row in df.iterrows():
                future = executor.submit(
                    self._process_row, row, batch,cols, table_name, table_schema
                )
                todo.append(future)

            for future in futures.as_completed(todo):
                result = future.result()

        if result is not None and not result.empty():
            return self._multi_row_insert(result, cols, table_name, table_schema)

    def _import_data_concurrent(self, df, table_name, table_schema):
        self._bulk_insert(df, table_name, table_schema)
    
    def get_query(self, query):
        return self._execute(query)
    
    def import_data(self, df, table_name, table_schema):
        total = len(df)
        num_chunks = range(0, total,self._multi_row_insert_limit)
        for i in num_chunks:
            df_chunk = df[i:i+self._multi_row_insert_limit]
            batch = list(df_chunk.itertuples(index=False, name=None))
            cols = df_chunk.columns
            
            self._multi_row_insert(batch,cols,table_name,table_schema)
            print('Progreso de importación: ',i+len(df_chunk),'/',total)
        

    