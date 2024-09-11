import pandas as pd, os

from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

class DB_Airflow:
    def __init__(self, conn_id, database):
        try:
            self.pg_hook = PostgresHook(postgres_conn_id=conn_id, database=database)
            self.connection = self.pg_hook.get_conn()
            self.cur = self.connection.cursor()

        except TimeoutError as timeout_err:
            print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [TIMEOUT] {timeout_err}")
        except Exception as error:
            print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [ERROR] {error}")
            
    def query_df(self, sql_query):
        fetched_df = self.pg_hook.get_pandas_df(sql=sql_query)
        return fetched_df
    
    def query(self, sql_query):
        self.cur.execute(sql_query)
        data = {'header': [i[0] for i in self.cur.description], 'data': self.cur.fetchall()}

        return pd.DataFrame(columns=data['header'], data=data['data'])