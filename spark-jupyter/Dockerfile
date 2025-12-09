# Spark + Jupyter (PySpark) có sẵn
FROM jupyter/pyspark-notebook:spark-3.5.0

USER root
# Cài đặt công cụ tiện ích
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    delta-spark==2.4.0 \
    boto3 \
    minio \
    pyhive[trino] \
    ruamel.yaml \
    findspark \
    pandas \
    matplotlib \
    seaborn

# Thêm thư viện để Spark dùng s3a (MinIO)
# Lưu ý: phiên bản này khớp Hadoop 3.3.x đi kèm image
ENV SPARK_JARS_DIR=/usr/local/spark/jars
RUN curl -L -o ${SPARK_JARS_DIR}/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
RUN curl -L -o ${SPARK_JARS_DIR}/delta-spark_2.12-2.4.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/2.4.0/delta-spark_2.12-2.4.0.jar

RUN curl -L -o ${SPARK_JARS_DIR}/delta-spark_2.12-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    curl -L -o ${SPARK_JARS_DIR}/delta-storage-3.2.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar
# Bắt Spark load JAR Delta lúc khởi động
ENV PYSPARK_SUBMIT_ARGS="--jars /usr/local/spark/jars/delta-spark_2.12-2.4.0.jar pyspark-shell"


# # Không bắt buộc nhưng hữu ích
# ENV PYSPARK_PYTHON=python3

# Trả quyền lại cho user mặc định (jovyan)
USER ${NB_UID}


