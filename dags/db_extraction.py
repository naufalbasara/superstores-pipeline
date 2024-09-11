import os, pandas as pd, logging

from datetime import datetime
from etl.db_airflow import DB_Airflow
from etl.extract import (
    extract_dim_time,extract_dim_customer,extract_dim_location,extract_dim_product
)
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow import DAG
import pendulum

# important
local_tz = pendulum.timezone("Asia/Jakarta")

def extract_time(last_update=datetime.today()):
    last_update = datetime.strftime(last_update, format='%H-%M_%Y-%m-%d')
    db_instance = DB_Airflow(conn_id='prod_pg', database='postgres')

    # Extraction
    try:
        logging.info('Extracting time dimension')
        dim_time = extract_dim_time(db_object=db_instance)
        dim_time.to_csv(os.path.join('data', f'dim_time.csv'))

        return True
    except TimeoutError as timeout_err:
        logging.error(f"[TIMEOUT] {timeout_err}")
    except Exception as error:
        logging.error(f"[ERROR] {error}")

def extract_location(last_update=datetime.today()):
    last_update = datetime.strftime(last_update, format='%H-%M_%Y-%m-%d')
    db_instance = DB_Airflow(conn_id='prod_pg', database='postgres')

    # Extraction
    try:
        logging.info('Extracting location dimension')
        dim_location = extract_dim_location(db_object=db_instance)
        dim_location.to_csv(os.path.join('data', f'dim_location.csv'))
        return True
    except TimeoutError as timeout_err:
        logging.error(f"[TIMEOUT] {timeout_err}")
    except Exception as error:
        logging.error(f"[ERROR] {error}")

def extract_customer(last_update=datetime.today()):
    last_update = datetime.strftime(last_update, format='%H-%M_%Y-%m-%d')
    db_instance = DB_Airflow(conn_id='prod_pg', database='postgres')

    # Extraction
    try:
        logging.info('Extracting customer dimension')
        dim_customer = extract_dim_customer(db_object=db_instance)
        dim_customer.to_csv(os.path.join('data', f'dim_customer.csv'))
        return True
    except TimeoutError as timeout_err:
        logging.error(f"[TIMEOUT] {timeout_err}")
    except Exception as error:
        logging.error(f"[ERROR] {error}")

def extract_product(last_update=datetime.today()):
    last_update = datetime.strftime(last_update, format='%H-%M_%Y-%m-%d')
    db_instance = DB_Airflow(conn_id='prod_pg', database='postgres')

    # Extraction
    try:
        dim_product = extract_dim_product(db_object=db_instance)
        dim_product.to_csv(os.path.join('data', f'dim_product.csv'))
        return True
    except TimeoutError as timeout_err:
        logging.error(f"[TIMEOUT] {timeout_err}")
    except Exception as error:
        logging.error(f"[ERROR] {error}")

def generate_fact_sales(last_update=datetime.today()):
    last_update = datetime.strftime(last_update, format='%H-%M_%Y-%m-%d')
    db_instance = DB_Airflow(conn_id='prod_pg', database='postgres')

    # Extraction
    try:
        sales_df = db_instance.query_df("select * from sales;")
        sale_product_df = db_instance.query_df("select * from sale_product;")
        sales_merged = pd.merge(left=sale_product_df, right=sales_df, on='sales_id')[['customer_id', 'ship_to', 'product_id', 'order_date', 'quantity', 'subtotal']]
        sales_merged = sales_merged.groupby(['customer_id', 'ship_to', 'product_id', 'order_date']).sum().reset_index()
        dim_time = pd.read_csv('data/dim_time.csv')
        dim_time['datum'] = dim_time['datum'].apply(pd.to_datetime)

        fact_sales = pd.merge(left=sales_merged, right=dim_time, left_on='order_date', right_on='datum')[
            ['customer_id', 'ship_to', 'product_id', 'time_key', 'quantity', 'subtotal']
        ].rename(columns={
            'customer_id':'customer_key', 'ship_to':'location_key', 'product_id':'product_key'
            })
        
        fact_sales.to_csv('data/fct_sales_staging.csv')

        return True
    except TimeoutError as timeout_err:
        logging.error(f"[TIMEOUT] {timeout_err}")
    except Exception as error:
        logging.error(f"[ERROR] {error}")

with DAG(
    dag_id='db_extraction',
    schedule='*/15 * * * *',
    start_date=datetime(year=2024, month=9, day=11, tzinfo=local_tz),
) as dag:
    info_log_start = BashOperator(
        task_id='bash1',
        bash_command=f'echo "Starting data extraction from DB production";'
    )

    cleanup_dir = BashOperator(
        task_id='starting_cleanup',
        bash_command=f'rm data/* & true'
    )

    # Fetching all dimension
    fetch_time_dim = PythonOperator(
        task_id='extract_time',
        python_callable=extract_time
    )

    fetch_location_dim = PythonOperator(
        task_id='extract_location',
        python_callable=extract_location
    )

    fetch_customer_dim = PythonOperator(
        task_id='extract_customer',
        python_callable=extract_customer
    )

    fetch_product_dim = PythonOperator(
        task_id='extract_product',
        python_callable=extract_product
    )
    
    # Transformation start
    transform_fact_sales = PythonOperator(
        task_id='generate_fact_sales',
        python_callable=generate_fact_sales,
    )

    # Sensor waiting for a file
    check_files = FileSensor(
        task_id='check_fact_files',
        filepath='data/fct_sales_staging.csv'
    )

    info_log_end = BashOperator(
        task_id='bash2',
        bash_command=f'echo "[INFO]: Starting data extraction from DB production";'
    )

    info_log_start >> [fetch_time_dim, fetch_location_dim, fetch_product_dim, fetch_customer_dim] >> transform_fact_sales
    transform_fact_sales >> check_files >> info_log_end

# web_visits = extract_visits(db_object=prod_conn)
# sales_df = extract_sales(db_object=prod_conn)
# sale_product_df = extract_sale_product(db_object=prod_conn)
# print(f"Extraction finished in {time.time() - extract_start}s")

# # generate sales fact
# sales_merged = pd.merge(left=sale_product_df, right=sales_df, on='sales_id')[['customer_id', 'ship_to', 'product_id', 'order_date', 'quantity', 'subtotal']]
# sales_merged = sales_merged.groupby(['customer_id', 'ship_to', 'product_id', 'order_date']).sum().reset_index()

# fact_sales = pd.merge(left=sales_merged, right=dim_time, left_on='order_date', right_on='datum')[
# ['customer_id', 'ship_to', 'product_id', 'time_key', 'quantity', 'subtotal']
# ].rename(columns={
# 'customer_id':'customer_key', 'ship_to':'location_key', 'product_id':'product_key'
# })