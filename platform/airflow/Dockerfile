FROM apache/airflow:2.9.1

USER root

# 1. Cài đặt Docker CLI (để chạy lệnh docker bên trong container)
RUN apt-get update && \
    apt-get install -y docker.io

# 2. Tạo nhóm docker với GID trùng với Host (ví dụ 999)
# Nếu group đã tồn tại thì dùng groupmod, nếu chưa thì groupadd
RUN groupadd -g 981 docker || true

# 3. Thêm user airflow vào nhóm docker
RUN usermod -aG docker airflow

USER airflow