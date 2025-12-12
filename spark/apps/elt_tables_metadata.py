from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
import os
import time

from minio_client import MinioClient

import boto3
from botocore.client import Config
def load_env(env_path=None):
    if env_path and os.path.exists(env_path):
        load_dotenv(env_path)
    elif os.path.exists(".env"):
        load_env(".env")

# MinIO/S3 Config
MINIO_ENDPOINT = "http://minio:9000" # URL nội bộ Docker hoặc IP máy chủ
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY") # Lấy từ env cho an toàn
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")


# Lấy credential từ biến môi trường (An toàn hơn hardcode)
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

def get_spark_session():
    spark = (SparkSession.builder
            .appName("ELT_TableMetadata")
            .master("spark://spark-master:7077")
            .getOrCreate())
    return spark

def init_minio_bucket():
    """
    Kiểm tra và tạo bucket trên MinIO nếu chưa tồn tại bằng boto3
    """
    print(f"\n[MinIO Init] Đang kiểm tra bucket: '{BUCKET_NAME}'...")
    try:
        s3 = boto3.resource('s3',
                            endpoint_url=MINIO_ENDPOINT,
                            aws_access_key_id=MINIO_ACCESS_KEY,
                            aws_secret_access_key=MINIO_SECRET_KEY,
                            config=Config(signature_version='s3v4'),
                            region_name='us-east-1') # MinIO thường dùng region mặc định này
        
        bucket = s3.Bucket(BUCKET_NAME)
        
        # Kiểm tra xem bucket có tồn tại không
        if not bucket.creation_date:
            print(f"[MinIO Init] Bucket '{BUCKET_NAME}' chưa tồn tại. Đang tạo mới...")
            s3.create_bucket(Bucket=BUCKET_NAME)
            print(f"[MinIO Init] -> Đã tạo bucket '{BUCKET_NAME}' thành công!")
        else:
            print(f"[MinIO Init] Bucket '{BUCKET_NAME}' đã tồn tại. Sẵn sàng ghi dữ liệu.")
            
    except Exception as e:
        print(f"[MinIO Init] LỖI CỰC KỲ NGHIÊM TRỌNG: Không thể kết nối hoặc tạo bucket MinIO!")
        print(f"Lỗi chi tiết: {str(e)}")
        # Tùy chọn: Raise lỗi để dừng chương trình nếu không có bucket thì không làm gì được tiếp
        raise e

def get_all_tables_metadata(spark):
    """
    Lấy danh sách bảng, dung lượng, số dòng (ước tính) và số cột
    bằng cách query System View của Oracle.
    """
    
    # SQL Query "Thần thánh" để lấy Metadata
    # - ALL_TABLES: Lấy tên bảng và số dòng (Last Analyzed)
    # - ALL_TAB_COLUMNS: Đếm số cột
    # - USER_SEGMENTS: Tính dung lượng đĩa cứng (Bytes)
    inventory_query = f"""
    (
        SELECT 
            t.TABLE_NAME,
            t.NUM_ROWS AS APPROX_ROW_COUNT,
            t.LAST_ANALYZED,
            NVL(c.COL_COUNT, 0) AS COL_COUNT,
            ROUND(NVL(s.BYTES, 0) / 1024 / 1024, 2) AS SIZE_MB
        FROM ALL_TABLES t
        -- Join để đếm số cột
        LEFT JOIN (
            SELECT TABLE_NAME, COUNT(*) AS COL_COUNT 
            FROM ALL_TAB_COLUMNS 
            WHERE OWNER = '{ORACLE_USER}' 
            GROUP BY TABLE_NAME
        ) c ON t.TABLE_NAME = c.TABLE_NAME
        -- Join để tính dung lượng
        LEFT JOIN (
            SELECT SEGMENT_NAME, SUM(BYTES) AS BYTES 
            FROM USER_SEGMENTS 
            GROUP BY SEGMENT_NAME
        ) s ON t.TABLE_NAME = s.SEGMENT_NAME
        WHERE t.OWNER = '{ORACLE_USER}'
        ORDER BY SIZE_MB DESC
    ) tmp
    """

    print(">>> Đang lấy Metadata từ Oracle System Views...")
    
    df = spark.read \
        .format("jdbc") \
        .option("url", ORACLE_URL) \
        .option("dbtable", inventory_query) \
        .option("user", ORACLE_USER) \
        .option("password", ORACLE_PWD) \
        .option("driver", DRIVER_CLASS) \
        .load()
        
    return df
def verify_empty_tables(spark, df_candidates):
    """
    Nhận vào DataFrame các bảng nghi ngờ rỗng,
    Chạy COUNT(1) thật để kiểm chứng.
    """
    real_empty_tables = []
    
    # Convert sang list để loop
    candidates = [row.TABLE_NAME for row in df_candidates.collect()]
    print(f">>> Đang kiểm tra thực tế {len(candidates)} bảng nghi ngờ...")

    for table in candidates:
        try:
            # Query count trực tiếp (Rất nhanh với bảng rỗng)
            count = spark.read \
                .format("jdbc") \
                .option("url", ORACLE_URL) \
                .option("query", f"SELECT COUNT(1) as cnt FROM DCC_TAISUN.\"{table}\"") \
                .option("user", ORACLE_USER) \
                .option("password", ORACLE_PWD) \
                .option("driver", DRIVER_CLASS) \
                .load().collect()[0]['CNT']
            
            if count == 0:
                real_empty_tables.append(table)
                # print(f"  [CONFIRMED] {table} is empty.")
            else:
                print(f"  [FALSE POSITIVE] {table} thực tế có {count} dòng.")
                
        except Exception as e:
            print(f"  [ERROR] Không thể check bảng {table}: {e}")

    return real_empty_tables

# Sử dụng:
# real_empty_list = verify_empty_tables(spark, df_empty_tables)
# print("Danh sách bảng thực sự rỗng:", real_empty_list)

if __name__ == "__main__":
    spark = get_spark_session()
    df_inventory = get_all_tables_metadata(spark)
    # 1. Lọc lấy các bảng khác rỗng
    df_not_empty_tables = df_inventory.filter(col("APPROX_ROW_COUNT") > 0)
    # Or solution 2:
    # df_not_empty_tables = df_inventory.filter(
    # (col("APPROX_ROW_COUNT").isNotNull()) & 
    # (col("APPROX_ROW_COUNT") != 0)
    # )
    # 2. Lọc lấy các bảng có số dòng = 0 HOẶC là null
    # Chúng ta lọc cả NULL vì bảng chưa analyze thường là bảng mới/trống
    df_empty_tables = df_inventory.filter(
        (col("APPROX_ROW_COUNT") == 0) | 
        (col("APPROX_ROW_COUNT").isNull())
    )
    
    # 3. Sắp xếp theo tên bảng cho dễ nhìn
    df_empty_tables = df_empty_tables.orderBy("TABLE_NAME")

    print(">>> store metadata to MinIO")
    df_inventory.write \
                .format("delta") \
                .mode("overwrite") \
                .save("s3a://datalake/metadata/oracle_tables")
    print("store successfully")
    spark.stop()