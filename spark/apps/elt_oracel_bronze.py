from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, lit
import os
import time

from utils.minio_client import get_spark_session, init_minio_bucket

import boto3
from botocore.client import Config

def load_env(env_path=None):
    if env_path and os.path.exists(env_path):
        load_dotenv(env_path)
    elif os.path.exists(".env"):
        load_env(".env")

# Lấy credential từ biến môi trường
ORACLE_USER = os.getenv("ORACLE_USER", "")
ORACLE_PWD = os.getenv("ORACLE_PASSWORD", "")
ORACLE_SERVICE_NAME = os.getenv("ORACLE_SERVICE_NAME")
ORACLE_HOST = os.getenv("ORACLE_HOST")
ORACLE_PORT = os.getenv("ORACLE_PORT")
ORACLE_URL = f"jdbc:oracle:thin:@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE_NAME}"
BUCKET_NAME = "datalake"
PREFIX_PATH = f"s3a://{BUCKET_NAME}"
LAYER = "bronze"
DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver"

def get_table_count(spark, table_name):
    """Đếm tổng số dòng để quyết định chiến lược"""
    query = f"(SELECT COUNT(1) as cnt FROM DCC_TAISUN.\"{table_name}\") tmp"
    df = spark.read \
        .format("jdbc") \
        .option("url", ORACLE_URL) \
        .option("dbtable", query) \
        .option("user", ORACLE_USER) \
        .option("password", ORACLE_PWD) \
        .option("driver", DRIVER_CLASS) \
        .load()
    return df.collect()[0]['CNT']

def ingest_table(spark, table_name):
    target_path = f"{PREFIX_PATH}/{LAYER}/{table_name.lower()}"
    print(f"\n>>> Bắt đầu xử lý: {table_name}")
    
    # 1. Lấy tổng số dòng
    total_rows = get_table_count(spark, table_name)
    print(f"    Tổng số dòng: {total_rows}")
    # 2. Chiến lược đọc (Threshold: 500,000 dòng)
    if total_rows < 500000:
        # --- CHIẾN LƯỢC 1: Đọc đơn luồng (Cho bảng nhỏ 64K) ---
        print("    -> Sử dụng: Single Thread (Bảng nhỏ)")
        df = spark.read \
            .format("jdbc") \
            .option("url", ORACLE_URL) \
            .option("dbtable", f"DCC_TAISUN.\"{table_name}\"") \
            .option("user", ORACLE_USER) \
            .option("password", ORACLE_PWD) \
            .option("driver", DRIVER_CLASS) \
            .option("fetchsize", "2000") \
            .load()
    else:
        # --- CHIẾN LƯỢC 2: Parallel Read với ROWNUM (Cho bảng lớn 2M) ---
        print("    -> Sử dụng: Parallel Read với ROWNUM Wrapper")
        
        # Num partitions = Số core * 2 (Ví dụ 2 worker x 2 core = 4 partitions)
        num_partitions = 4
        
        # Subquery tạo cột giả SPARK_PID
        dbtable_query = f"""
            (SELECT T.*, ROWNUM as SPARK_PID 
             FROM DCC_TAISUN.\"{table_name}\" T) tmp
        """
        
        df = spark.read \
            .format("jdbc") \
            .option("url", ORACLE_URL) \
            .option("dbtable", dbtable_query) \
            .option("user", ORACLE_USER) \
            .option("password", ORACLE_PWD) \
            .option("driver", DRIVER_CLASS) \
            .option("partitionColumn", "SPARK_PID") \
            .option("lowerBound", "1") \
            .option("upperBound", str(int(total_rows))) \
            .option("numPartitions", str(num_partitions)) \
            .option("fetchsize", "10000") \
            .load() \
            .drop("SPARK_PID") # Xóa cột tạm trước khi lưu
    print("download df successfully")
    print("    -> Đang ghi xuống MinIO...")
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(target_path)
    print(f"    -> Hoàn tất: {table_name}")
    print("==="*40)

if __name__ == "__main__":
    spark = get_spark_session(app_name="ELT_OracelToBronze")
    init_minio_bucket(bucket_name=BUCKET_NAME)
    
    # Danh sách bảng lấy từ hình ảnh của bạn
    # Lưu ý: Oracle case-sensitive nên cần để chính xác tên bảng
    tables = [
        "IMM_FILE", # Header phiếu kho
        "IMN_FILE", # Detail Phiếu kho
        "IMA_FILE", # Danh mục vật tư, hàng hóa
        "SMD_FILE", # Quy đổi theo mã hàng
        "SMC_FILE", # Quy đổi đơn vị chung

    ]
    report_name = "AIMR324"
    
    for tbl in tables:
        start_time = time.time()
        ingest_table(spark, tbl)
        print("+"*20)
        print('\n'*3)
        print(f"Time to process table {tbl}: {round(time.time() - start_time, 2)} seconds")
        print('\n'*3)
        print("+"*20)
        
    spark.stop()
spark.stop()