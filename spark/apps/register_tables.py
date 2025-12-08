from pyspark.sql import SparkSession

GOLD_BUCKET   = "s3a://datalake/gold"
REPORT_NAME   = "AIMR324"  # Tên dự án/Báo cáo
gold_path = f"{GOLD_BUCKET}/{REPORT_NAME}/sales_transaction_mart"

spark = (SparkSession.builder
         .appName("Register-Tables")
         .master("spark://spark-master:7077")
         .config("hive.metastore.uris", "thrift://hive-metastore:9083")
         .enableHiveSupport()
         .getOrCreate())

spark.sql(f"CREATE DATABASE IF NOT EXISTS gold_{REPORT_NAME.lower()}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold_{REPORT_NAME.lower()}.sales_transaction_mart 
    USING DELTA LOCATION '{gold_path}'
""")
# spark.sql("""
#   CREATE TABLE IF NOT EXISTS nyc_gold.daily_revenue_by_zone
#   USING DELTA
#   LOCATION 's3a://datalake/gold/nyc_taxi/daily_revenue_by_zone'
# """)
# spark.sql("""
#   CREATE TABLE IF NOT EXISTS nyc_gold.hourly_demand_by_zone
#   USING DELTA
#   LOCATION 's3a://datalake/gold/nyc_taxi/hourly_demand_by_zone'
# """)
spark.stop()