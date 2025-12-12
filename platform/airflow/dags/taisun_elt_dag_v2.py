from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

# --- COMMON CONFIG ---
SPARK_CONN_ID = "spark_default" # Đã config trong docker-compose
SPARK_APPS_DIR = "/opt/spark-apps"

default_args = {
    'owner': 'XavieLee',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='taisun_elt_dag_spark_submit',
    default_args=default_args,
    start_date=datetime(2025, 11, 1, tzinfo=local_tz),
    schedule='00 7 * * *',
    catchup=False,
    tags=['taisun', 'spark', 'elt']
) as dag:

    # 1. Ingest: Oracle -> Bronze
    bronze = SparkSubmitOperator(
        task_id='ingest_bronze',
        application=f"{SPARK_APPS_DIR}/elt_oracel_bronze.py",
        conn_id=SPARK_CONN_ID,
        # conf=spark_conf,
        verbose=True,
        # Nếu code Python cần tham số (ví dụ ngày chạy), dùng application_args
        # application_args=["--date", "{{ ds }}"], 
    )

    # 2. Transform: Bronze -> Silver
    silver = SparkSubmitOperator(
        task_id='transform_silver',
        application=f"{SPARK_APPS_DIR}/elt_bronze_silver.py",
        conn_id=SPARK_CONN_ID,
        conf=spark_conf,
        verbose=True
    )

    # 3. Transform: Silver -> Gold
    gold = SparkSubmitOperator(
        task_id='transform_gold',
        application=f"{SPARK_APPS_DIR}/elt_silver_gold.py",
        conn_id=SPARK_CONN_ID,
        # conf=spark_conf,
        verbose=True
    )

    # 4. Register Tables
    register = SparkSubmitOperator(
        task_id='register_tables',
        application=f"{SPARK_APPS_DIR}/register_tables.py",
        conn_id=SPARK_CONN_ID,
        # conf=spark_conf,
        verbose=True
    )

    bronze >> silver >> gold >> register