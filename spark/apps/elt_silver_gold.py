from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# --- 1. CẤU HÌNH HỆ THỐNG ---
SILVER_BUCKET = "s3a://datalake/silver"
GOLD_BUCKET   = "s3a://datalake/gold"
REPORT_NAME   = "AIMR324"  # Tên dự án/Báo cáo
TARGET_UNIT   = "CTN"      # Đơn vị đích muốn quy đổi (Thùng)

def get_spark_session():
    return SparkSession.builder \
        .appName("ELT_SilverToGold_SalesMart") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

# --- 2. HÀM LOGIC NGHIỆP VỤ (BUSINESS LOGIC) ---

def calculate_conversion_factor(df_txn, df_smd, df_smc):
    """
    Tái hiện logic hàm Oracle S_UMFCHK bằng Spark Code.
    Tính toán hệ số quy đổi (conversion_factor) cho từng dòng giao dịch.
    """
    print("    -> Đang tính toán hệ số quy đổi đơn vị (Logic S_UMFCHK)...")

    # Chuẩn bị dữ liệu tham chiếu (Master Data)
    # Lưu ý: Vì ở bước Bronze-to-Silver bạn đã lower case tên cột, nên ở đây ta dùng chữ thường.
    
    # Bảng quy đổi riêng (theo mã hàng)
    smd_clean = df_smd.select(
        F.col("smd01").alias("smd_item"),
        F.col("smd02").alias("smd_unit1"),
        F.col("smd03").alias("smd_unit2"),
        F.col("smd04").cast(DoubleType()).alias("smd_val1"),
        F.col("smd06").cast(DoubleType()).alias("smd_val2")
    )

    # Bảng quy đổi chung
    smc_clean = df_smc.filter(F.col("smcacti") == 'Y').select(
        F.col("smc01").alias("smc_unit1"),
        F.col("smc02").alias("smc_unit2"),
        F.col("smc03").cast(DoubleType()).alias("smc_val1"),
        F.col("smc04").cast(DoubleType()).alias("smc_val2")
    )

    # --- THỰC HIỆN JOIN 3 BƯỚC (Dùng Broadcast để tối ưu vì bảng Master thường nhỏ) ---
    
    # 1. Join Ưu tiên 1 (Quy đổi xuôi theo mã hàng)
    df_step1 = df_txn.join(
        F.broadcast(smd_clean).alias("smd1"),
        (F.col("ma_hang") == F.col("smd1.smd_item")) & 
        (F.col("don_vi_goc") == F.col("smd1.smd_unit1")) & 
        (F.lit(TARGET_UNIT) == F.col("smd1.smd_unit2")),
        "left"
    )

    # 2. Join Ưu tiên 2 (Quy đổi ngược theo mã hàng)
    df_step2 = df_step1.join(
        F.broadcast(smd_clean).alias("smd2"),
        (F.col("ma_hang") == F.col("smd2.smd_item")) & 
        (F.col("don_vi_goc") == F.col("smd2.smd_unit2")) &  # Đảo chiều
        (F.lit(TARGET_UNIT) == F.col("smd2.smd_unit1")),   # Đảo chiều
        "left"
    )

    # 3. Join Ưu tiên 3 (Quy đổi chung)
    df_final_join = df_step2.join(
        F.broadcast(smc_clean).alias("smc"),
        (F.col("don_vi_goc") == F.col("smc.smc_unit1")) & 
        (F.lit(TARGET_UNIT) == F.col("smc.smc_unit2")),
        "left"
    )

    # --- TÍNH TOÁN FACTOR ---
    df_result = df_final_join.withColumn("conversion_factor",
        F.when(
            (F.col("don_vi_goc") == F.lit(TARGET_UNIT)) | (F.col("ma_hang").startswith("MISC")), 
            F.lit(1.0)
        ).otherwise(
            F.coalesce(
                (F.col("smd1.smd_val2") / F.col("smd1.smd_val1")), # Ưu tiên 1
                (F.col("smd2.smd_val1") / F.col("smd2.smd_val2")), # Ưu tiên 2 (Ngược)
                (F.col("smc.smc_val2") / F.col("smc.smc_val1")),   # Ưu tiên 3
                F.lit(1.0) # Mặc định
            )
        )
    )

    # Xử lý Null/Zero division safety
    df_result = df_result.withColumn("conversion_factor", 
        F.when(F.col("conversion_factor").isNull() | (F.col("conversion_factor") == 0), 1.0)
         .otherwise(F.col("conversion_factor"))
    )

    return df_result

# --- 3. HÀM CHÍNH: XỬ LÝ SILVER -> GOLD ---

def create_gold_sales_mart(spark):
    print("\n>>> BẮT ĐẦU QUY TRÌNH SILVER -> GOLD: SALES MART")
    
    # 1. Đọc dữ liệu từ Silver (Delta Format)
    # Lưu ý: Tên cột đã được normalized (lowercase) ở bước Bronze->Silver
    try:
        df_imm = spark.read.format("delta").load(f"{SILVER_BUCKET}/imm_file")
        df_imn = spark.read.format("delta").load(f"{SILVER_BUCKET}/imn_file")
        df_ima = spark.read.format("delta").load(f"{SILVER_BUCKET}/ima_file")
        df_smd = spark.read.format("delta").load(f"{SILVER_BUCKET}/smd_file")
        df_smc = spark.read.format("delta").load(f"{SILVER_BUCKET}/smc_file")
    except Exception as e:
        print(f"!!! LỖI: Không đọc được bảng Silver. Chi tiết: {str(e)}")
        return

    # 2. Join các bảng Transaction & Master (IMM + IMN + IMA)
    # Chọn trước các cột cần thiết để tối ưu bộ nhớ
    txn_base = df_imm.join(df_imn, df_imm.imm01 == df_imn.imn01, "inner") \
        .join(df_ima, df_imn.imn03 == df_ima.ima01, "inner") \
        .select(
            df_imm.imm01.alias("so_chung_tu"),
            df_imm.imm02.alias("ngay_chung_tu"),
            df_imm.imm14.alias("bo_phan"),
            df_imn.imn03.alias("ma_hang"),
            df_ima.ima02.alias("ten_hang"),
            df_imn.imn15.alias("ma_kho"),
            df_imn.imn16.alias("ma_vi_tri"),
            df_imn.imn10.alias("so_luong_goc"),
            df_imm.immuser.alias("nguoi_yeu_cau"),
            # df_imm.immudo1.alias("ghi_chu_header"),
            df_imn.imn20.alias("don_vi_goc"), # Cần cột này để tính quy đổi
            df_ima.imaud06.alias("brand_raw") # Cột chứa Brand dạng "NHOM/THUONGHIEU"
        )

    # 3. Áp dụng Logic Quy đổi đơn vị (Gọi hàm đã viết ở trên)
    txn_with_factor = calculate_conversion_factor(txn_base, df_smd, df_smc)

    # 4. Tính toán cuối cùng & Làm sạch (Final Transformation)
    df_gold_final = txn_with_factor.select(
        F.col("so_chung_tu"),
        F.col("ngay_chung_tu"),
        F.col("ma_hang"),
        F.col("ten_hang"),
        F.col("ma_kho"),
        F.col("bo_phan"),
        F.col("so_luong_goc"),
        F.col("don_vi_goc"),
        F.col("nguoi_yeu_cau"),
        # F.col("ghi_chu_header"),
        
        # Logic tách Brand: Lấy phần tử thứ 2 sau dấu /
        # Ví dụ: "FOOD/VISSAN" -> "VISSAN"
        F.when(F.col("brand_raw").isNull(), "UNKNOWN")
         .otherwise(F.split(F.col("brand_raw"), "/").getItem(1))
         .alias("thuong_hieu"),
         
        # Tính số lượng quy đổi ra Thùng (CTN)
        F.round(F.col("so_luong_goc") * F.col("conversion_factor"), 2).alias("so_luong_thung"),
        
        F.current_timestamp().alias("gold_ingest_time")
    )

    # 5. Ghi xuống lớp Gold (Delta Lake)
    gold_path = f"{GOLD_BUCKET}/{REPORT_NAME}/sales_transaction_mart"
    
    print(f"    -> Đang ghi dữ liệu xuống Gold: {gold_path}")
    
    # Ở lớp Gold Mart, ta thường dùng chế độ OVERWRITE (Làm mới báo cáo) 
    # hoặc MERGE nếu dữ liệu cực lớn. Với báo cáo này, Overwrite là an toàn nhất.
    df_gold_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(gold_path)

    # # 6. Đăng ký lên Hive Metastore
    # spark.sql(f"CREATE DATABASE IF NOT EXISTS gold_{REPORT_NAME.lower()}")
    # spark.sql(f"""
    #     CREATE TABLE IF NOT EXISTS gold_{REPORT_NAME.lower()}.sales_transaction_mart 
    #     USING DELTA LOCATION '{gold_path}'
    # """)
    
    # 7. (Optional) Optimize & Vacuum để dọn dẹp file rác Delta
    # try:
    #     deltaTable = DeltaTable.forPath(spark, gold_path)
    #     deltaTable.optimize().executeCompaction() # Gom các file nhỏ thành file lớn
    #     print("    -> Đã tối ưu hóa (Optimize) bảng Delta.")
    # except:
    #     pass

    print(f"    -> [HOÀN TẤT] Bảng Gold đã sẵn sàng phục vụ BI!")

if __name__ == "__main__":
    spark = get_spark_session()
    
    # Chạy quy trình tạo bảng Gold Sales Mart
    create_gold_sales_mart(spark)
            
    spark.stop()