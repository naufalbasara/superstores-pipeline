import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv


class DB_Connection():
    load_dotenv()

    def __init__(self, db):
        """initialized object with database name, example: bipi"""
        database = os.environ.get(f'{db}_DB')
        username = os.environ.get(f'{db}_USER')
        password = os.environ.get(f'{db}_PASS')
        host = os.environ.get(f'{db}_HOST')
        port = os.environ.get(f'{db}_PORT')

        self.con = psycopg2.connect(dbname=database, user=username, password=password, host=host, port=port)
        self.cur = self.con.cursor()
        self.cur.execute('''set timezone to 'Asia/Jakarta';''')

    def query(self, sql):
        self.cur.execute(sql)
        return {'header': [i[0] for i in self.cur.description], 'data': self.cur.fetchall()}

    def query_df(self, sql):
        self.cur.execute(sql)
        data = {'header': [i[0] for i in self.cur.description], 'data': self.cur.fetchall()}
        return pd.DataFrame(columns=data['header'], data=data['data'])

    def close_conn(self):
        self.cur.close()
        self.con.close()