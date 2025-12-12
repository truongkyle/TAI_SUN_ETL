# Spark 3.5.0 + Scala 2.12
FROM apache/spark:3.5.0

USER root
RUN apt-get update && apt-get install -y curl wget && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

ENV SPARK_HOME=/opt/spark
ENV SPARK_JARS_DIR=$SPARK_HOME/jars
ENV PATH=$PATH:$SPARK_HOME/bin

# --- S3A (MinIO) ---
# Giữ nguyên Hadoop core từ image; CHỈ bổ sung hadoop-aws + aws sdk BUNDLE KHỚP 3.3.4
RUN curl -L -o ${SPARK_JARS_DIR}/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# --- Delta Lake (Spark 3.5 compatible) ---
# Dùng single-jar delta-spark 3.2.0 (khớp Scala 2.12)
# Delta Lake
RUN curl -L -o ${SPARK_JARS_DIR}/delta-spark_2.12-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    curl -L -o ${SPARK_JARS_DIR}/delta-storage-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar


# --- Kafka Structured Streaming ---
# Connector + token provider cho Spark 3.5.0
RUN curl -L -o ${SPARK_JARS_DIR}/spark-sql-kafka-0-10_2.12-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o ${SPARK_JARS_DIR}/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

# (Tùy chọn) Ghim kafka-clients, chỉnh version khớp Kafka broker (ví dụ 3.6.0)
# RUN curl -L -o ${SPARK_JARS_DIR}/kafka-clients-3.6.0.jar \
#       https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.0/kafka-clients-3.6.0.jar
# --- Oracle JDBC Driver (CRITICAL) ---
# Tải ojdbc8.jar (Tương thích tốt với Oracle 12c, 19c)
RUN curl -L -o ${SPARK_JARS_DIR}/ojdbc8.jar \
    https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.9.0.0/ojdbc8-21.9.0.0.jar

# Giữ container ở foreground khi chạy các daemons
ENV SPARK_NO_DAEMONIZE=true

EXPOSE 7077 8080 4040

