# -----------------------------------------------------------
# DeltaTableService — Arrow Flight server backed by PySpark
# -----------------------------------------------------------
# Base image: Apache Spark 3.5.5 with Scala 2.12 and Java 11
FROM apache/spark:3.5.5-scala2.12-java11-ubuntu

ARG RESOURCE_REAPER_SESSION_ID="00000000-0000-0000-0000-000000000000"
LABEL "org.testcontainers.resource-reaper-session"=$RESOURCE_REAPER_SESSION_ID

# Delta Lake and Python dependency versions
ENV DELTA_VERSION=3.2.1
ENV FLIGHT_PORT=8815

# Switch to root for package installation
USER root

# ---- Install Python pip and system dependencies ----
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip && \
    rm -rf /var/lib/apt/lists/*

# ---- Install Python dependencies ----
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# ---- Download Delta Lake JARs ----
RUN curl -L \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar \
    -o $SPARK_HOME/jars/delta-spark_2.12-${DELTA_VERSION}.jar

RUN curl -L \
    https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
    -o $SPARK_HOME/jars/delta-storage-${DELTA_VERSION}.jar

RUN curl -L \
    https://repo1.maven.org/maven2/io/delta/delta-contribs_2.12/${DELTA_VERSION}/delta-contribs_2.12-${DELTA_VERSION}.jar \
    -o $SPARK_HOME/jars/delta-contribs_2.12-${DELTA_VERSION}.jar

# ---- Upgrade Hadoop runtime from 3.3.4 → 3.4.1 for ABFSS / OneLake support ----
# The base image ships Hadoop 3.3.4 JARs, but hadoop-azure 3.4.1 (which contains
# FixedSASTokenProvider) requires classes from Hadoop 3.4.1 (e.g. BackReference).
# We replace the four base-image Hadoop JARs with their 3.4.1 counterparts and
# add the ABFSS connector plus its transitive dependencies.
ENV HADOOP_VERSION=3.4.1

# Remove Hadoop 3.3.4 JARs that shipped with the base image
RUN rm -f $SPARK_HOME/jars/hadoop-client-api-3.3.4.jar \
          $SPARK_HOME/jars/hadoop-client-runtime-3.3.4.jar \
          $SPARK_HOME/jars/hadoop-shaded-guava-1.1.1.jar \
          $SPARK_HOME/jars/hadoop-yarn-server-web-proxy-3.3.4.jar

# Download Hadoop 3.4.1 core runtime JARs
RUN curl -fL \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar \
    -o $SPARK_HOME/jars/hadoop-client-api-${HADOOP_VERSION}.jar && \
    curl -fL \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar \
    -o $SPARK_HOME/jars/hadoop-client-runtime-${HADOOP_VERSION}.jar && \
    curl -fL \
    https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.3.0/hadoop-shaded-guava-1.3.0.jar \
    -o $SPARK_HOME/jars/hadoop-shaded-guava-1.3.0.jar && \
    curl -fL \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-yarn-server-web-proxy/${HADOOP_VERSION}/hadoop-yarn-server-web-proxy-${HADOOP_VERSION}.jar \
    -o $SPARK_HOME/jars/hadoop-yarn-server-web-proxy-${HADOOP_VERSION}.jar

# Download ABFSS connector (hadoop-azure) and its transitive dependencies
#   1. hadoop-azure 3.4.1  — SecureAzureBlobFileSystem + FixedSASTokenProvider
#   2. azure-storage 7.0.1 — legacy Azure Storage SDK (required by hadoop-azure)
#   3. jetty-util-ajax     — JSON parser used by azure-storage
#   4. wildfly-openssl     — TLS support for hadoop-azure
RUN curl -fL \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/${HADOOP_VERSION}/hadoop-azure-${HADOOP_VERSION}.jar \
    -o $SPARK_HOME/jars/hadoop-azure-${HADOOP_VERSION}.jar && \
    curl -fL \
    https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/7.0.1/azure-storage-7.0.1.jar \
    -o $SPARK_HOME/jars/azure-storage-7.0.1.jar && \
    curl -fL \
    https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util-ajax/9.4.53.v20231009/jetty-util-ajax-9.4.53.v20231009.jar \
    -o $SPARK_HOME/jars/jetty-util-ajax-9.4.53.v20231009.jar && \
    curl -fL \
    https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.1.3.Final/wildfly-openssl-1.1.3.Final.jar \
    -o $SPARK_HOME/jars/wildfly-openssl-1.1.3.Final.jar

# ---- Copy application code ----
WORKDIR /opt/delta-table-service
COPY app/ ./app/
COPY v1/run.py .

# ---- Expose Arrow Flight port ----
EXPOSE ${FLIGHT_PORT}

# ---- Run as non-root (Spark image default user) ----
USER 185

# ---- Entrypoint ----
ENTRYPOINT ["python3", "run.py"]
CMD ["--port", "8815", "--host", "0.0.0.0"]
