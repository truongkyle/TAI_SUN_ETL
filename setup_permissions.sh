#!/bin/bash

# Lấy đường dẫn gốc của dự án
PROJECT_ROOT=$(pwd)

echo "=== BẮT ĐẦU CẤU HÌNH QUYỀN CHO DỰ ÁN DATA LAKE ==="
echo "Đường dẫn gốc: $PROJECT_ROOT"

# -------------------------------------------------------
# 1. XỬ LÝ AIRFLOW (Nằm trong platform/airflow)
# Container chạy UID 50000. Cần quyền GHI tuyệt đối.
# -------------------------------------------------------
echo "--> [1/4] Cấu hình Airflow (UID 50000)..."
mkdir -p platform/airflow/dags platform/airflow/logs platform/airflow/plugins
sudo chown -R 50000:0 platform/airflow

# -------------------------------------------------------
# 2. XỬ LÝ SPARK (Nằm ở root/spark)
# Container chạy UID 185.
# - logs: Cần quyền GHI.
# - apps/conf: Chỉ cần quyền ĐỌC (để bạn code ở ngoài, container đọc vào).
# -------------------------------------------------------
echo "--> [2/4] Cấu hình Spark (UID 185)..."
mkdir -p spark/logs spark/apps spark/conf

# Cấp quyền ghi cho thư mục logs
sudo chown -R 185:0 spark/logs

# Với thư mục code (apps) và config, giữ nguyên owner là bạn (để bạn sửa code),
# nhưng cấp quyền 755 để container đọc được.
sudo chmod -R 755 spark/apps spark/conf

# -------------------------------------------------------
# 3. XỬ LÝ METABASE (Nằm trong platform/metabase-data)
# Metabase H2 DB file rất hay bị lock permission.
# Cách an toàn nhất cho local dev là cấp full quyền ghi.
# -------------------------------------------------------
echo "--> [3/4] Cấu hình Metabase Data..."
mkdir -p platform/metabase-data
sudo chmod -R 777 platform/metabase-data

# -------------------------------------------------------
# 4. XỬ LÝ JUPYTER & TRINO (root/spark-jupyter & root/trino)
# Các thư mục này chủ yếu cần quyền đọc config hoặc ghi notebook.
# Mặc định UID 1000 (user của bạn) thường tương thích tốt với Jupyter image.
# Nhưng để chắc chắn, ta cấp quyền ghi cho group.
# -------------------------------------------------------
echo "--> [4/4] Cấu hình Jupyter & Trino..."
sudo chmod -R 775 spark-jupyter
sudo chmod -R 755 trino

echo "=== HOÀN TẤT! ==="
echo "Bây giờ hãy cd vào 'platform' và chạy 'docker compose up -d'"