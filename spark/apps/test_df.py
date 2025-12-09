from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, regexp_replace, current_timestamp, count, when
import re

# --- CẤU HÌNH ---
BRONZE_BUCKET = "s3a://datalake/bronze"
# Bạn có thể tạo bucket mới tên 'silver' trên MinIO hoặc dùng prefix
SILVER_BUCKET = "s3a://datalake/silver" 

def get_spark_session():
    return SparkSession.builder \
        .appName("ELT_TestDf") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

def process_bronze_to_silver(spark, table_name, report_name,unique_keys = "col"):
    """
    Logix xử lý chính: Read Bronze -> Transform -> Merge Silver
    """
    bronze_path = f"{BRONZE_BUCKET}/{report_name}/{table_name.lower()}"
    silver_path = f"{SILVER_BUCKET}/{report_name}/{table_name.lower()}"
    
    print(f"\n>>> Đang xử lý bảng: {table_name}")
    
    # 1. Đọc Bronze (Delta)
    try:
        df_bronze = spark.read.format("delta").load(bronze_path)
        print("="*50)
        print("\n"*10)
        print(df_bronze)
        print("\n"*10)
        print("="*50)
    except Exception as e:
        print()
        print(f"!!! Không tìm thấy bảng Bronze tại: {bronze_path}")
        return

if __name__ == "__main__":
    spark = get_spark_session()
    
    # --- CẤU HÌNH CÁC BẢNG CẦN CHẠY ---
    # format: ("TÊN_BẢNG_GỐC", ["DANH_SÁCH_KHOÁ_CHÍNH"])
    # Key dùng để định danh duy nhất dòng dữ liệu để tránh trùng lặp
    tables = [
        "IMM_FILE", # Header phiếu kho
        # "IMN_FILE", # Detail Phiếu kho
        # "IMA_FILE", # Danh mục vật tư, hàng hóa
        # "SMD_FILE", # Quy đổi theo mã hàng
        # "SMC_FILE", # Quy đổi đơn vị chung

    ]
    
    etl_jobs = [
        # Ví dụ bảng Sales, dùng Order ID làm khóa
        ("2022_DOMESTIC_SALES", ["order_id"]),
        ("CCH_FILE", []),
        
        # Ví dụ bảng nhỏ, nếu không có khóa thì để list rỗng [] (sẽ append hoặc overwrite tuỳ logic, 
        # nhưng tốt nhất nên tìm 1 cột định danh, ví dụ file_name hoặc id)
        ("2504_TC_ABB_FILE", ["col1"]) 
    ]
    report_name = "AIMR324"
    
    # for tbl, keys in etl_jobs:
    #     # Lưu ý: Nếu keys rỗng thì script trên cần sửa nhẹ để chỉ overwrite. 
    #     # Nhưng ở đây ta giả định data warehouse luôn cần key.
    #     # if not keys:
    #     #      print(f"Cảnh báo: Bảng {tbl} chưa cấu hình Primary Key. Bỏ qua Merge.")
    #     #      # Logic overwrite đơn giản nếu cần:
    #     #      # transform_data(spark.read...).write.mode("overwrite").save(...)
    #     # else:
    #     #     process_bronze_to_silver(spark, tbl, keys)
    #     process_bronze_to_silver(spark, tbl, report_name)
    for tbl in tables:
        process_bronze_to_silver(spark, tbl, report_name)
            
    spark.stop()