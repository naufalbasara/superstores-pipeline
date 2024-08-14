import os, time, pandas as pd, re

from datetime import datetime
from tools.db_conn import DB_Connection
from etl.extract import extract_dim_time, extract_dim_customer, extract_dim_location, extract_dim_product, extract_sales_with_product, extract_marketing

if __name__ == "__main__":
    prod_conn = DB_Connection('TRANSACT')
    dim_time = extract_dim_time(db_conn=prod_conn)
    dim_customer = extract_dim_customer(db_conn=prod_conn)
    dim_location = extract_dim_location(db_conn=prod_conn)
    dim_product = extract_dim_product(db_conn=prod_conn)
    
    # Load to staging
    try:
        staging_conn = DB_Connection('STAGING')
    except TimeoutError as timeout_err:
        print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [TIMEOUT] {timeout_err}")
    except Exception as error:
        print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [ERROR] {error}")