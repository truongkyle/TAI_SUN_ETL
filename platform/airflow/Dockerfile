FROM apache/airflow:2.9.1

USER root

# 1. Cài đặt OpenJDK 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Thiết lập biến môi trường (HARDCODE ĐỂ TRÁNH LỖI)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark

# Quan trọng: Ghi cứng đường dẫn của Airflow + Java + Spark + System
ENV PATH="/home/airflow/.local/bin:${JAVA_HOME}/bin:${SPARK_HOME}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

# 3. Copy Spark (Offline Mode)
COPY airflow/libs/spark-3.5.0-bin-hadoop3.tgz /tmp/spark.tgz

RUN tar -xvzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-3.5.0-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz

# 4. Cài Jars (Giữ nguyên phần curl của bạn)
RUN curl -L -o ${SPARK_HOME}/jars/ojdbc8.jar https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.9.0.0/ojdbc8-21.9.0.0.jar && \
    curl -L -o ${SPARK_HOME}/jars/delta-spark_2.12-3.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    curl -L -o ${SPARK_HOME}/jars/delta-storage-3.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar && \
    curl -L -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# 5. Chuyển User và Cài thư viện
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark