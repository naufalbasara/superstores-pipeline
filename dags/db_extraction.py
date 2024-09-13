import os, pandas as pd, logging

from datetime import datetime, timedelta
from etl.db_airflow import DB_Airflow
from etl.extract import (
    extract_dim_time,extract_dim_customer,extract_dim_location,extract_dim_product,extract_visits
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
    last_update = datetime.strftime(last_update, format='%Y-%m-%d')
    db_instance = DB_Airflow(conn_id='prod_pg', database='postgres')

    # Extraction
    try:
        logging.info('Extracting time dimension')
        dim_time = extract_dim_time(db_object=db_instance)
        dim_time.set_index('time_key').to_csv(os.path.join('data', f'dim_time.csv'))

        return True
    except TimeoutError as timeout_err:
        logging.error(f"[TIMEOUT] {timeout_err}")
    except Exception as error:
        logging.error(error)

def extract_location(last_update=datetime.today()):
    last_update = datetime.strftime(last_update, format='%Y-%m-%d')
    db_instance = DB_Airflow(conn_id='prod_pg', database='postgres')

    # Extraction
    try:
        logging.info('Extracting location dimension')
        dim_location = extract_dim_location(db_object=db_instance)
        dim_location.set_index('location_key').to_csv(os.path.join('data', f'dim_location.csv'))
        return True
    except TimeoutError as timeout_err:
        logging.error(f"[TIMEOUT] {timeout_err}")
    except Exception as error:
        logging.error(error)

def extract_customer(last_update=datetime.today()):
    last_update = datetime.strftime(last_update, format='%Y-%m-%d')
    db_instance = DB_Airflow(conn_id='prod_pg', database='postgres')

    # Extraction
    try:
        logging.info('Extracting customer dimension')
        dim_customer = extract_dim_customer(db_object=db_instance)
        dim_customer.set_index('customer_key').to_csv(os.path.join('data', f'dim_customer.csv'))
        return True
    except TimeoutError as timeout_err:
        logging.error(f"[TIMEOUT] {timeout_err}")
    except Exception as error:
        logging.error(error)

def extract_product(last_update=datetime.today()):
    last_update = datetime.strftime(last_update, format='%Y-%m-%d')
    db_instance = DB_Airflow(conn_id='prod_pg', database='postgres')

    # Extraction
    try:
        dim_product = extract_dim_product(db_object=db_instance)
        dim_product.set_index('product_key').to_csv(os.path.join('data', f'dim_product.csv'))
        return True
    except TimeoutError as timeout_err:
        logging.error(f"[TIMEOUT] {timeout_err}")
    except Exception as error:
        logging.error(error)

def generate_fact_sales(last_update=datetime.today()):
    last_update = datetime.strftime(datetime.today() if last_update == None else last_update, format='%Y-%m-%d')
    db_instance = DB_Airflow(conn_id='prod_pg', database='postgres')

    # Extraction
    try:
        sales_df = db_instance.query_df(f"select * from sales where order_date{'<=' if last_update == datetime.today() else '>='}'{last_update}'")
        sale_product_df = db_instance.query_df(f"""
            select * from sale_product where sales_id in (
                select sales_id from sales where order_date {'<=' if last_update == datetime.today() else '>='} '{last_update}'
            )
        """)
        sales_merged = pd.merge(left=sale_product_df, right=sales_df, on='sales_id')[['customer_id', 'ship_to', 'product_id', 'order_date', 'quantity', 'subtotal']]
        sales_merged = sales_merged.groupby(['customer_id', 'ship_to', 'product_id', 'order_date']).sum().reset_index()
        dim_time = pd.read_csv('data/dim_time.csv')
        dim_time['date'] = dim_time['date'].apply(pd.to_datetime)

        fact_sales = pd.merge(left=sales_merged, right=dim_time, left_on='order_date', right_on='date', how='left')[
            ['customer_id', 'ship_to', 'product_id', 'time_key', 'quantity', 'subtotal']
        ].rename(columns={
            'customer_id':'customer_key', 'ship_to':'location_key', 'product_id':'product_key'
            })
        
        fact_sales.set_index('customer_key').to_csv('data/fct_sales_staging.csv')
        sales_df.to_csv('data/sales_data.csv')

        return True
    except TimeoutError as timeout_err:
        logging.error(f"[TIMEOUT] {timeout_err}")
    except Exception as error:
        logging.error(error)

def generate_fact_marketing(last_update=datetime.today()):
    last_update = datetime.strftime(datetime.today() if last_update == None else last_update, format='%Y-%m-%d')
    db_instance = DB_Airflow(conn_id='prod_pg', database='postgres')

    # Extraction
    try:
        dim_time = pd.read_csv('data/dim_time.csv')
        dim_time['date'] = dim_time['date'].apply(pd.to_datetime)

        sales_df = pd.read_csv('data/sales_data.csv')
        sales_df = sales_df.drop(columns=['Unnamed: 0'])
        sales_df['order_date'] = sales_df['order_date'].apply(pd.to_datetime)
        sales_df = sales_df.groupby(['customer_id', 'ship_to', 'order_date']).agg({'sales_id':'size', 'total':'sum'})
        sales_df = sales_df.rename(columns={'sales_id': 'orders'}).reset_index()

        dim_customer = pd.read_csv('data/dim_customer.csv')
        
        if os.path.exists('data/customer_visit.csv'):
            visits_df = pd.read_csv('data/customer_visit.csv')
            visits_df = visits_df.drop(columns=['Unnamed: 0'])
        else:
            visits_df = extract_visits(last_update=datetime.today(), db_object=db_instance)
            visits_df.to_csv('data/customer_visit.csv')

        fact_marketing = pd.merge(left=dim_customer, right=visits_df, on='customer_key', how='left')[['customer_key', 'visits']]
        fact_marketing = pd.merge(left=fact_marketing, right=sales_df, left_on='customer_key', right_on='customer_id', how='left')[['customer_key', 'ship_to', 'order_date', 'visits','orders', 'total']]
        fact_marketing = pd.merge(fact_marketing, dim_time, left_on='order_date', right_on='date', how='left')[['customer_key', 'ship_to', 'time_key', 'visits', 'orders', 'total']]

        fact_marketing['conversion_rate'] = fact_marketing['orders'] / fact_marketing['visits']
        fact_marketing['avg_purchase_value'] = fact_marketing['total'] / fact_marketing['orders']
        fact_marketing = fact_marketing.loc[:, ['customer_key', 'ship_to', 'time_key', 'conversion_rate', 'avg_purchase_value']].rename(columns={'ship_to': 'location_key'})

        fact_marketing.set_index('customer_key').to_csv('data/fct_marketing_staging.csv')

        return True
    except TimeoutError as timeout_err:
        logging.error(f"[TIMEOUT] {timeout_err}")
    except Exception as error:
        logging.error(error)

with DAG(
    dag_id='db_extraction',
    schedule='@weekly',
    start_date=datetime(year=2024, month=9, day=9, tzinfo=local_tz),
) as dag:
    info_log_start = BashOperator(
        task_id='starting_log',
        bash_command=f'echo "Starting data extraction from DB production";'
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
        op_kwargs={'last_update': datetime(year=2000, month=1, day=1)} # MAKE SURE THE LAST EXTRACTION DATE
    )

    transform_fact_marketing = PythonOperator(
        task_id='generate_fact_marketing',
        python_callable=generate_fact_marketing,
        op_kwargs={'last_update': datetime(year=2000, month=1, day=1)} # MAKE SURE THE LAST EXTRACTION DATE
    )

    # Sensor waiting for a file
    check_sales_file = FileSensor(
        task_id='check_fact_sales',
        fs_conn_id='data_path',
        filepath='fct_sales_staging.csv',
        max_wait=timedelta(seconds=30),
        retries=2
    )

    check_marketing_file = FileSensor(
        task_id='check_fact_marketing',
        fs_conn_id='data_path',
        filepath='fct_marketing_staging.csv',
        max_wait=timedelta(seconds=30),
        retries=2
    )

    info_log_end = BashOperator(
        task_id='ending_log',
        bash_command=f'echo "Data extraction completed.";'
    )

    info_log_start >> [fetch_time_dim, fetch_location_dim, fetch_product_dim, fetch_customer_dim] >> transform_fact_sales
    transform_fact_sales >> check_sales_file >> transform_fact_marketing >> check_marketing_file >> info_log_end