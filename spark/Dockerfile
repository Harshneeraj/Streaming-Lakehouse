# Spark 3.5.1 + Hudi 0.15.0 (bundle for Spark 3.5 + Scala 2.12) + Hadoop AWS for S3A + Postgres JDBC for HMS
FROM bitnamilegacy/spark:3.5.1

USER root

ENV SPARK_HOME=/opt/bitnami/spark
ENV HUDI_VERSION=0.15.0
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
ENV POSTGRES_JDBC_VERSION=42.7.3
ENV SPARK_KAFKA_VERSION=3.5.1
ENV KAFKA_CLIENTS_VERSION=3.4.1
ENV COMMONS_POOL2_VERSION=2.11.1

# Curl is already present; install just in case + python deps for the streaming job
RUN install_packages curl ca-certificates \
 && pip install --no-cache-dir \
      "kafka-python==2.0.2" \
      "boto3==1.35.0" \
      "minio==7.2.7" \
      "rich==13.7.1"

# Drop Spark's bundled hadoop-client-runtime (it shades classes that conflict with hadoop-aws)
# Bitnami's image keeps hadoop-client-* in $SPARK_HOME/jars
RUN set -eux; \
    cd "${SPARK_HOME}/jars"; \
    # Apache Hudi spark bundle for Spark 3.5 / Scala 2.12
    curl -fSL -o hudi-spark3.5-bundle_2.12-${HUDI_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/${HUDI_VERSION}/hudi-spark3.5-bundle_2.12-${HUDI_VERSION}.jar; \
    # Hadoop AWS + AWS SDK bundle for S3A
    curl -fSL -o hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar; \
    curl -fSL -o aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar; \
    # Postgres JDBC (needed if Spark talks to HMS-backed Postgres directly; harmless either way)
    curl -fSL -o postgresql-${POSTGRES_JDBC_VERSION}.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar; \
    # Spark-Kafka structured streaming connector + transitive deps
    curl -fSL -o spark-sql-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_KAFKA_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar; \
    curl -fSL -o spark-token-provider-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_KAFKA_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar; \
    curl -fSL -o kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar; \
    curl -fSL -o commons-pool2-${COMMONS_POOL2_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar; \
    chown -R 1001:0 "${SPARK_HOME}/jars"

# Pre-create ivy cache dir owned by spark user
RUN mkdir -p /home/spark/.ivy2 && chown -R 1001:0 /home/spark

USER 1001
