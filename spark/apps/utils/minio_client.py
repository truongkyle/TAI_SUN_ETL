from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.sql import SparkSession
import os
import time

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

def get_spark_session(app_name):
    spark = (SparkSession.builder
            .appName(app_name)
            .master("spark://spark-master:7077")
            .getOrCreate())
    return spark

def init_minio_bucket(bucket_name):
    """
    Kiểm tra và tạo bucket trên MinIO nếu chưa tồn tại bằng boto3
    """
    print(f"\n[MinIO Init] Đang kiểm tra bucket: '{bucket_name}'...")
    try:
        s3 = boto3.resource('s3',
                            endpoint_url=MINIO_ENDPOINT,
                            aws_access_key_id=MINIO_ACCESS_KEY,
                            aws_secret_access_key=MINIO_SECRET_KEY,
                            config=Config(signature_version='s3v4'),
                            region_name='us-east-1') # MinIO thường dùng region mặc định này
        
        bucket = s3.Bucket(bucket_name)
        
        # Kiểm tra xem bucket có tồn tại không
        if not bucket.creation_date:
            print(f"[MinIO Init] Bucket '{bucket_name}' chưa tồn tại. Đang tạo mới...")
            s3.create_bucket(Bucket=bucket_name)
            print(f"[MinIO Init] -> Đã tạo bucket '{bucket_name}' thành công!")
        else:
            print(f"[MinIO Init] Bucket '{bucket_name}' đã tồn tại. Sẵn sàng ghi dữ liệu.")
            
    except Exception as e:
        print(f"[MinIO Init] LỖI CỰC KỲ NGHIÊM TRỌNG: Không thể kết nối hoặc tạo bucket MinIO!")
        print(f"Lỗi chi tiết: {str(e)}")
        # Tùy chọn: Raise lỗi để dừng chương trình nếu không có bucket thì không làm gì được tiếp
        raise e