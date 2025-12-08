from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, regexp_replace, current_timestamp, count, when
import re

# --- CẤU HÌNH ---
BRONZE_BUCKET = "s3a://datalake/bronze"
# Bạn có thể tạo bucket mới tên 'silver' trên MinIO hoặc dùng prefix
SILVER_BUCKET = "s3a://datalake/silver" 

def get_spark_session():
    return SparkSession.builder \
        .appName("ELT_BronzeToSilver") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
def drop_null_columns(df):
    """
    Hàm tự động quét toàn bộ DataFrame,
    Tìm các cột chứa 100% giá trị NULL và loại bỏ chúng.
    """
    print("    -> Đang quét các cột toàn NULL (All-Null Columns)...")
    
    # 1. Tính toán số lượng giá trị NON-NULL cho từng cột
    # (Spark count(col) chỉ đếm các dòng không null)
    counts = df.select([count(c).alias(c) for c in df.columns]).first().asDict()
    
    # 2. Lọc ra danh sách các cột có count == 0
    cols_to_drop = [c for c, val in counts.items() if val == 0]
    
    if cols_to_drop:
        print(f"    -> [PHÁT HIỆN] Các cột sau toàn NULL và sẽ bị xóa: {cols_to_drop}")
        # Xóa các cột này khỏi DataFrame
        df_clean = df.drop(*cols_to_drop)
        return df_clean, True # Trả về cờ True (có thay đổi schema)
    else:
        print("    -> Không có cột nào toàn NULL.")
        return df, False # False (không thay đổi)

def normalize_column_name(col_name):
    """
    Chuyển đổi tên cột:
    Ví dụ: 'ORDER_ID' -> 'order_id', 'Customer Name' -> 'customer_name'
    """
    # Thay thế khoảng trắng và ký tự đặc biệt bằng _
    clean_name = re.sub(r'[^\w]', '_', col_name)
    # Chuyển về chữ thường
    return clean_name.lower()

def transform_data(df):
    """
    Khu vực làm sạch dữ liệu chung cho mọi bảng
    """
    # Xóa các col Null
    df, schema_changed = drop_null_columns(df)
    # 1. Chuẩn hóa tên cột
    for old_col in df.columns:
        new_col = normalize_column_name(old_col)
        df = df.withColumnRenamed(old_col, new_col)
    
    # 2. Trim string (xóa khoảng trắng thừa ở đầu/cuối các cột string)
    for dtype in df.dtypes:
        if dtype[1] == 'string':
            df = df.withColumn(dtype[0], trim(col(dtype[0])))
            
    # 3. Thêm cột audit time
    df = df.withColumn("silver_ingest_time", current_timestamp())

    # 4. xóa duplicate
    df = df.dropDuplicates()

    
    return df, schema_changed

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
    except Exception as e:
        print(f"!!! Không tìm thấy bảng Bronze tại: {bronze_path}")
        return

    # 2. Transform
    df_silver_clean, schema_changed = transform_data(df_bronze)
    
    # 3. Ghi vào Silver (Sử dụng MERGE/UPSERT)
    # Nếu bảng Silver đã tồn tại -> Merge
    # Nếu chưa -> Create
    if schema_changed:
        df_silver_clean.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(silver_path)
    else:
        df_silver_clean.write \
                .format("delta") \
                .mode("overwrite") \
                .save(silver_path)
    
    # if DeltaTable.isDeltaTable(spark, silver_path):
    #     print(f"    -> Bảng Silver đã tồn tại. Thực hiện MERGE theo keys: {unique_keys}")
    #     delta_table = DeltaTable.forPath(spark, silver_path)
        
    #     # Tạo điều kiện merge: target.id = source.id AND ...
    #     merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in unique_keys])
        
    #     # Thực thi Merge
    #     delta_table.alias("target").merge(
    #         df_silver_clean.alias("source"),
    #         merge_condition
    #     ).whenMatchedUpdateAll() \
    #      .whenNotMatchedInsertAll() \
    #      .execute()
         
    # else:
    #     print("    -> Bảng Silver chưa tồn tại. Tạo mới (Full Load)...")
    #     df_silver_clean.write \
    #         .format("delta") \
    #         .mode("overwrite") \
    #         .save(silver_path)
            
    # 4. Đăng ký lên Hive Metastore (để Trino query được)
    # Lưu ý: Tên bảng trong Hive nên dùng prefix silver_
    spark.sql(f"CREATE TABLE IF NOT EXISTS default.silver_{table_name.lower()} USING DELTA LOCATION '{silver_path}'")
    
    print(f"    -> Hoàn tất: silver_{table_name.lower()}")

if __name__ == "__main__":
    spark = get_spark_session()
    
    # --- CẤU HÌNH CÁC BẢNG CẦN CHẠY ---
    # format: ("TÊN_BẢNG_GỐC", ["DANH_SÁCH_KHOÁ_CHÍNH"])
    # Key dùng để định danh duy nhất dòng dữ liệu để tránh trùng lặp
    tables = [
        "IMM_FILE", # Header phiếu kho
        "IMN_FILE", # Detail Phiếu kho
        "IMA_FILE", # Danh mục vật tư, hàng hóa
        "SMD_FILE", # Quy đổi theo mã hàng
        "SMC_FILE", # Quy đổi đơn vị chung

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