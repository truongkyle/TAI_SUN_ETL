import os
import logging
import pandas as pd
import s3fs
from dotenv import load_dotenv

# Load biến môi trường từ file .env ngay khi import module
load_dotenv()

# Cấu hình Logging chuẩn (thay vì dùng print)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MinIOConnector:
    """
    Class quản lý kết nối và đọc ghi dữ liệu với MinIO
    """
    def __init__(self):
        # Lấy cấu hình từ biến môi trường (Bảo mật)
        # self.endpoint = os.getenv('LOCAL_S3_ENDPOINT', 'http://localhost:9000')
        self.endpoint = 'http://localhost:9000'
        self.access_key = os.getenv('MINIO_ACCESS_KEY')
        self.secret_key = os.getenv('MINIO_SECRET_KEY')
        
        # Validate cấu hình
        if not self.access_key or not self.secret_key:
            raise ValueError(" Lỗi: Chưa cấu hình MINIO_ACCESS_KEY hoặc MINIO_SECRET_KEY trong file .env")

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