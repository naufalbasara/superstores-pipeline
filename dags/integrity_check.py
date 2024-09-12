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