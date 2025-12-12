from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import pendulum

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    'owner': 'XavieLee',
    'depends_on_past': False,
    'retries': 1,  
}

with DAG(
    dag_id='taisun_elt_dag',
    default_args=default_args,
    start_date=datetime(2025, 11, 1, tzinfo=local_tz),
    schedule ='00 7 * * *',
    catchup=False,
    tags=['taisun', 'spark', 'elt']
) as dag:
    bronze = BashOperator(
        task_id='spark_bronze',
        bash_command="""docker exec spark-master /opt/spark/bin/spark-submit \
                     --master spark://spark-master:7077 /opt/spark-apps/elt_oracel_bronze.py"""
    )

    silver = BashOperator(
        task_id='spark_silver',
        bash_command="""docker exec spark-master /opt/spark/bin/spark-submit \
                     --master spark://spark-master:7077 /opt/spark-apps/elt_bronze_silver.py"""
    )

    gold = BashOperator(
        task_id='spark_gold',
        bash_command="""docker exec spark-master /opt/spark/bin/spark-submit \
                     --master spark://spark-master:7077 /opt/spark-apps/elt_silver_gold.py"""
    )

    register = BashOperator(
        task_id='register_hive_tables',
        bash_command="""docker exec spark-master /opt/spark/bin/spark-submit \
                     --master spark://spark-master:7077 /opt/spark-apps/register_tables.py"""
    )

bronze >> silver >> gold >> register