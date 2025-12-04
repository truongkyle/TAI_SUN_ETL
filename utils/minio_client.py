from minio import Minio
from minio.error import S3Error
import logging
import os
from glob import glob
import pandas as pd
from dotenv import load_dotenv
# from download_nyc_data import DATASETS

def load_env(env_path=None):
    if env_path and os.path.exists(env_path):
        load_dotenv(env_path)
    elif os.path.exists(".env"):
        load_env(".env")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MinioClient:
    def __init__(self,
                 endpoint: str = None,
                 access_key: str = None,
                 secret_key: str = None,
                 secure: bool = False,
                 env_path: str = "./.env",
                 stage: str = 'container'):
        load_env(env_path)
        self.endpoint = endpoint or (os.getenv("LOCAL_S3_ENDPOINT", "http://localhost:9000") if stage == "local" else os.getenv("S3_ENDPOINT", "http://localhost:9000"))
        self.access_key = access_key or os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
        self.secret_key = secret_key or os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
        print(self.endpoint)
        try:
            self.client = Minio(
                endpoint=self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=secure
            )
            print(f"Connected to Minio at {self.endpoint}")
        except Exception as e:
            print(f"Failed to connect to Minio: {e}")
            self.client =None

    def ensure_bucket(self, bucket_name: str):
        if not self.client:
            print("No Minio connection available.")
            return False
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                print(f"Bucket {bucket_name} created.")
            else:
                print(f"Bucket {bucket_name} already exists.")
            return True
        except S3Error as e:
            print(f"Error creating bucket {bucket_name}: {e}")
            return False
    
    def upload_file(self, bucket_name: str, object_name: str, file_path: str) -> bool:
        if not self.ensure_bucket(bucket_name):
            return False
        
        try:
            self.client.fput_object(bucket_name, object_name, file_path)
            print(f"Upload: {file_path} -> s3://{bucket_name}/{object_name}")
            return True
        except S3Error as e:
            print(f"Failed to upload {file_path}: {e}")
            return False
        
    def upload_folder(self, bucket_name: str, local_folder: str, prefix: str = "", allowed_ext: tuple = None) -> bool:
        if not self.ensure_bucket(bucket_name):
            return False
        success = True
        local_files = glob(local_folder)
        for local_file in local_files:
            file_name = os.path.basename(local_file)
            # relative_path = os.path.relpath(prefix, local_folder)
            # object_name = os.path.join(prefix, relative_path).replace("\\", "/")
            object_name = f"{prefix}/{file_name}"
            if not self.upload_file(bucket_name, object_name, local_file):
                success = False
        return success
    
    def list_files(self, bucket_name: str, prefix: str ="") -> list:
        try:
            return [obj.object_name for obj in self.client.list_objects(bucket_name, prefix=prefix, recursive=True)]
        except Exception as e:
            print(f"Failed to list objects in {bucket_name}: {e}")
            return []
        
    def _get_storage_options(self):
        """Tạo dictionary cấu hình cho s3fs"""
        return {
            "key": self.access_key,
            "secret": self.secret_key,
            "client_kwargs": {
                "endpoint_url": self.endpoint
            }
        }
    
    def read_parquet(self, bucket_name: str, file_name: str) -> pd.DataFrame:
        """
        Đọc file Parquet từ MinIO trả về DataFrame
        :param bucket_name: Tên bucket
        :param file_name: Tên file hoặc đường dẫn file
        :return: pandas DataFrame
        """
        full_path = f"s3://{bucket_name}/{file_name}"
        logger.info(f"Đang bắt đầu đọc file: {full_path}")

        try:
            df = pd.read_parquet(
                full_path,
                storage_options=self._get_storage_options(),
                engine='pyarrow'
            )
            logger.info(f"Đọc thành công! Kích thước: {df.shape}")
            return df
            
        except FileNotFoundError:
            logger.error(f" Không tìm thấy file: {full_path}")
            raise # Ném lỗi ra ngoài để luồng chính biết
        except Exception as e:
            logger.error(f" Lỗi không xác định khi đọc MinIO: {str(e)}")
            raise

if __name__ == "__main__":
    for prefix_name in os.listdir("./data/raw/nyc_raw"):
        local_folder = os.path.join("./data/raw/nyc_raw", prefix_name, "*.parquet")
        prefix = f"raw/nyc_taxi/{prefix_name}"
        minio_client = MinioClient(stage="local")
        minio_client.upload_folder(bucket_name="datalake", local_folder=local_folder, prefix=prefix)
    
        