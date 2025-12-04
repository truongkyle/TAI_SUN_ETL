import airbyte as ab
import os
from dotenv import load_dotenv
import json

load_dotenv()
config={
            "s3_bucket_name": os.getenv("TARGET_BUCKET_NAME", "bronze-v2"),
            "s3_bucket_path": "bronze-v2", # Sub-folder trong bucket
            "s3_bucket_region": "us-east-1", # MinIO không quan trọng region, để default
            "access_key_id": os.getenv("MINIO_ACCESS_KEY"),
            "secret_access_key": os.getenv("MINIO_SECRET_KEY"),
            "s3_endpoint": os.getenv("AIRBYTE_MINIO_ENDPOINT"), # QUAN TRỌNG: Endpoint cho Docker container
            "s3_path_style_access": True,
            "format": {
                "format_type": "Parquet", # Khuyên dùng Parquet cho Data Lake
                "compression_codec": "SNAPPY"
            },
        }
print(config)
destination = ab.get_destination(
        "destination-s3",
        config=config
    )
# Kiểm tra kết nối Destination
try:
    print("🔌 Đang kiểm tra kết nối MinIO...")
    destination.check()
    print("✅ Kết nối MinIO thành công!")
except Exception as e:
    print(e)