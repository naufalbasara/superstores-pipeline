import os, pandas as pd, logging

from datetime import datetime, timedelta
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

# Summary Data ETL to staging implementing SCD type 1

# important
local_tz = pendulum.timezone("Asia/Jakarta")

def load_time_dim():
    # Full extraction ETL.
    db_instance = DB_Airflow(conn_id='staging_db', database='postgres')
    try:
        logging.info(f'Loading table time.')
        file_path = f'data/dim_time.csv'

        temp_db = """
            DROP TABLE IF EXISTS temp_time;
            CREATE TEMP TABLE temp_time (LIKE dim_time);
            COPY temp_time FROM stdin (DELIMITER ',', HEADER TRUE, FORMAT CSV);
        """
        db_instance.pg_hook.copy_expert(sql=temp_db, filename=file_path)
        
        merge_query = """WITH updated_rows AS (
                UPDATE dim_time
                SET date = temp_time.date,
                    day_of_month = temp_time.day_of_month,
                    week_of_month = temp_time.week_of_month,
                    month = temp_time.month,
                    year = temp_time.year,
                    quarter_of_year = temp_time.quarter_of_year
                FROM temp_time
                WHERE dim_time.time_key = temp_time.time_key
                RETURNING dim_time.time_key
            ),
            inserted_rows AS (
                INSERT INTO dim_time (time_key,day_of_month,week_of_month,month,year,quarter_of_year, date)
                SELECT time_key, day_of_month, week_of_month, month, year, quarter_of_year, date
                FROM temp_time
                WHERE temp_time.time_key NOT IN (SELECT time_key FROM updated_rows)
                RETURNING time_key
            )
            SELECT
                (SELECT COUNT(*) FROM updated_rows) AS updated_count,
                (SELECT COUNT(*) FROM inserted_rows) AS inserted_count;"""

        db_instance.cur.execute(query=merge_query)
        db_instance.connection.commit()
        merge_result = db_instance.cur.fetchall()

        logging.info(f'Merge to time table resulting {merge_result[0][0]} UPDATED rows and {merge_result[0][1]} NEW INSERTED rows')
        logging.info(f'Table time load into staging DB.')
        
    except TimeoutError as timeout_err:
        logging.error(f"[TIMEOUT] {timeout_err}")
    except Exception as error:
        logging.error(f'Failed to load time table due to: {error}')

def load_location_dim():
    # Full extraction ETL.
    db_instance = DB_Airflow(conn_id='staging_db', database='postgres')
    try:
        logging.info(f'Loading table location.')
        file_path = f'data/dim_location.csv'

        temp_db = """
            DROP TABLE IF EXISTS temp_location;
            CREATE TEMP TABLE temp_location (LIKE dim_location);
            COPY temp_location FROM stdin (DELIMITER ',', HEADER TRUE, FORMAT CSV);
        """
        db_instance.pg_hook.copy_expert(sql=temp_db, filename=file_path)
        
        merge_query = """WITH updated_rows AS (
                UPDATE dim_location
                
                SET country = temp_location.country,
                    city = temp_location.city,
                    state = temp_location.state,
                    postal_code = temp_location.postal_code,
                    region = temp_location.region
                FROM temp_location
                WHERE dim_location.location_key = temp_location.location_key
                RETURNING dim_location.location_key
            ),
            inserted_rows AS (
                INSERT INTO dim_location (location_key,country,city,state,postal_code,region)
                SELECT location_key,country,city,state,postal_code,region
                FROM temp_location
                WHERE temp_location.location_key NOT IN (SELECT location_key FROM updated_rows)
                RETURNING location_key
            )
            SELECT
                (SELECT COUNT(*) FROM updated_rows) AS updated_count,
                (SELECT COUNT(*) FROM inserted_rows) AS inserted_count;"""

        db_instance.cur.execute(query=merge_query)
        db_instance.connection.commit()
        merge_result = db_instance.cur.fetchall()

        logging.info(f'Merge to location table resulting {merge_result[0][0]} UPDATED rows and {merge_result[0][1]} NEW INSERTED rows')
        logging.info(f'Table location load into staging DB.')
        
    except TimeoutError as timeout_err:
        logging.error(f"[TIMEOUT] {timeout_err}")
    except Exception as error:
        logging.error(f'Failed to load location table due to: {error}')

def load_product_dim():
    # Full extraction ETL.
    db_instance = DB_Airflow(conn_id='staging_db', database='postgres')
    try:
        logging.info(f'Loading table product.')
        file_path = f'data/dim_product.csv'

        temp_db = """
            DROP TABLE IF EXISTS temp_product;
            CREATE TEMP TABLE temp_product (LIKE dim_product);
            COPY temp_product FROM stdin (DELIMITER ',', HEADER TRUE, FORMAT CSV);
        """
        db_instance.pg_hook.copy_expert(sql=temp_db, filename=file_path)
        
        merge_query = """WITH updated_rows AS (
                UPDATE dim_product
                SET category = temp_product.category,
                    subcategory = temp_product.subcategory,
                    product_name = temp_product.product_name,
                    ratings = temp_product.ratings
                FROM temp_product
                WHERE dim_product.product_key = temp_product.product_key
                RETURNING dim_product.product_key
            ),
            inserted_rows AS (
                INSERT INTO dim_product (product_key,category,subcategory,product_name,ratings)
                SELECT product_key,category,subcategory,product_name,ratings
                FROM temp_product
                WHERE temp_product.product_key NOT IN (SELECT product_key FROM updated_rows)
                RETURNING product_key
            )
            SELECT
                (SELECT COUNT(*) FROM updated_rows) AS updated_count,
                (SELECT COUNT(*) FROM inserted_rows) AS inserted_count;"""

        db_instance.cur.execute(query=merge_query)
        db_instance.connection.commit()
        merge_result = db_instance.cur.fetchall()

        logging.info(f'Merge to product table resulting {merge_result[0][0]} UPDATED rows and {merge_result[0][1]} NEW INSERTED rows')
        logging.info(f'Table product load into staging DB.')
        
    except TimeoutError as timeout_err:
        logging.error(f"[TIMEOUT] {timeout_err}")
    except Exception as error:
        logging.error(f'Failed to load product table due to: {error}')

def load_customer_dim():
    # Full extraction ETL.
    db_instance = DB_Airflow(conn_id='staging_db', database='postgres')
    try:
        logging.info(f'Loading table customer.')
        file_path = f'data/dim_customer.csv'

        temp_db = """
            DROP TABLE IF EXISTS temp_customer;
            CREATE TEMP TABLE temp_customer (LIKE dim_customer);
            COPY temp_customer FROM stdin (DELIMITER ',', HEADER TRUE, FORMAT CSV);
        """
        db_instance.pg_hook.copy_expert(sql=temp_db, filename=file_path)

        merge_query = """
            MERGE INTO dim_customer USING temp_customer
            ON dim_customer.customer_key = temp_customer.customer_key
            WHEN MATCHED THEN UPDATE SET
                name = temp_customer.name,
                segment = temp_customer.segment,
                registered = temp_customer.registered,
                sex = temp_customer.sex,
                points = temp_customer.points
            WHEN NOT MATCHED THEN INSERT (
                name,
                segment,
                registered,
                sex,
                points
            ) VALUES (
                temp_customer.name,
                temp_customer.segment,
                temp_customer.registered,
                temp_customer.sex,
                temp_customer.points
            );
            """
        
        merge_query = """WITH updated_rows AS (
                UPDATE dim_customer
                SET name = temp_customer.name,
                    segment = temp_customer.segment,
                    registered = temp_customer.registered,
                    sex = temp_customer.sex,
                    points = temp_customer.points
                FROM temp_customer
                WHERE dim_customer.customer_key = temp_customer.customer_key
                RETURNING dim_customer.customer_key
            ),
            inserted_rows AS (
                INSERT INTO dim_customer (customer_key, name, segment, registered, sex, points)
                SELECT customer_key, name, segment, registered, sex, points
                FROM temp_customer
                WHERE temp_customer.customer_key NOT IN (SELECT customer_key FROM updated_rows)
                RETURNING customer_key
            )
            SELECT
                (SELECT COUNT(*) FROM updated_rows) AS updated_count,
                (SELECT COUNT(*) FROM inserted_rows) AS inserted_count;"""

        db_instance.cur.execute(query=merge_query)
        db_instance.connection.commit()
        merge_result = db_instance.cur.fetchall()

        logging.info(f'Merge to customer table resulting {merge_result[0][0]} UPDATED rows and {merge_result[0][1]} NEW INSERTED rows')
        logging.info(f'Table customer load into staging DB.')
        
    except TimeoutError as timeout_err:
        logging.error(f"[TIMEOUT] {timeout_err}")
    except Exception as error:
        logging.error(f'Failed to load customer table due to: {error}')

# Full Extraction (Drop and Load)
with DAG(
    dag_id='staging_dimensions_load',
    schedule='@weekly',
    start_date=datetime(year=2024, month=9, day=9, tzinfo=local_tz)
) as dag:
    starting_log = BashOperator(
        task_id='starting_log',
        bash_command=f'echo "Starting dimension tables load process to DB staging";'
    )

    check_dim_customer = FileSensor(
        task_id='check_dim_customer',
        fs_conn_id='data_path',
        filepath='dim_customer.csv',
        max_wait=timedelta(seconds=30)
    )
    check_dim_location = FileSensor(
        task_id='check_dim_location',
        fs_conn_id='data_path',
        filepath='dim_location.csv',
        max_wait=timedelta(seconds=30)
    )
    check_dim_product = FileSensor(
        task_id='check_dim_product',
        fs_conn_id='data_path',
        filepath='dim_product.csv',
        max_wait=timedelta(seconds=30)
    )
    check_dim_time = FileSensor(
        task_id='check_dim_time',
        fs_conn_id='data_path',
        filepath='dim_time.csv',
        max_wait=timedelta(seconds=30)
    )

    time_loading = PythonOperator(
        task_id='time_loading',
        python_callable=load_time_dim
    )

    product_loading = PythonOperator(
        task_id='product_loading',
        python_callable=load_product_dim
    )

    location_loading = PythonOperator(
        task_id='location_loading',
        python_callable=load_location_dim
    )
    
    customer_loading = PythonOperator(
        task_id='customer_loading',
        python_callable=load_customer_dim
    )

    starting_log >> [check_dim_customer, check_dim_location, check_dim_product, check_dim_time]
    check_dim_customer >> customer_loading
    check_dim_time >> time_loading
    check_dim_product >> product_loading
    check_dim_location >> location_loading

# Incremental Extraction (Only update the data changed from the last extraction)
with DAG(
    dag_id='staging_facts_load',
    schedule='@weekly',
    start_date=datetime(year=2024, month=9, day=9, tzinfo=local_tz)
) as dag:
    starting_log = BashOperator(
        task_id='starting_log',
        bash_command=f'echo "Starting data load process to DB staging";'
    )

    check_sales_file = FileSensor(
        task_id='check_fact_sales',
        fs_conn_id='data_path',
        filepath='fct_sales_staging.csv',
        max_wait=timedelta(seconds=30)
    )

    check_marketing_file = FileSensor(
        task_id='check_marketing_file',
        fs_conn_id='data_path',
        filepath='fct_marketing_staging.csv',
        max_wait=timedelta(seconds=30)
    )


    starting_log >> [check_sales_file, check_marketing_file]