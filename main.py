import airbyte as ab
import os
from dotenv import load_dotenv

load_dotenv()

def run_migration():
    print("🚀 Bắt đầu cấu hình Source Oracle...")

    source = ab.get_source(
        "source-oracle",
        config={
            "host": os.getenv("ORACLE_HOST"),
            "port": int(os.getenv("ORACLE_PORT")),
            "username": os.getenv("ORACLE_USER"),
            "password": os.getenv("ORACLE_PASSWORD"),
            "service_name": os.getenv("ORACLE_SERVICE_NAME"),
            
            # QUAN TRỌNG: Tên Schema phải viết hoa y hệt trong Database
            "schemas": ["DCC_TAISUN"], 
            
            "encryption": {
                "encryption_method": "unencrypted"
            },
            # Tạm thời chưa bật CDC để test kết nối trước
            # "replication_method": "Standard" 
        }
    )

    # Kiểm tra kết nối
    print("🔍 Đang kiểm tra kết nối tới Oracle (10.0.0.250)...")
    try:
        source.check()
        print("✅ Kết nối Oracle thành công!")
    except Exception as e:
        print(f"❌ Lỗi kết nối Oracle: {e}")
        return

    # # Chọn tất cả các bảng trong schema DCC_TAISUN
    # source.select_all_streams()
    # Lưu ý: Tên bảng trong Oracle thường Viết Hoa (Case Sensitive với PyAirbyte)
    source.select_streams(["2022_DOMESTIC_SALES"]) 
    
    # Nếu muốn chọn nhiều bảng lẻ tẻ thì thêm vào list:
    # source.select_streams(["2022_DOMESTIC_SALES", "CUSTOMERS", "ORDERS"])

    # Cấu hình MinIO (Đích)
    destination = ab.get_destination(
        "destination-s3",
        config={
            "s3_bucket_name": "bronze-layer",
            "s3_bucket_path": "dcc_taisun_data", # Lưu gọn gàng vào thư mục này
            "s3_bucket_region": "us-east-1",
            "access_key_id": os.getenv("MINIO_USER"),
            "secret_access_key": os.getenv("MINIO_PASSWORD"),
            "s3_endpoint": "http://host.docker.internal:9000",
            "format": {
                "format_type": "parquet",
                "compression_codec": "SNAPPY"
            }
        }
    )

    print("⏳ Đang đồng bộ dữ liệu...")
    source.read(destination=destination)
    print("🎉 Hoàn tất!")

if __name__ == "__main__":
    run_migration()