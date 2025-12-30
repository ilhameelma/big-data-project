FROM python:3.9-slim

LABEL maintainer="ENSA Al-Hoceima"

ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

# Installation dépendances système
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    nano \
    openjdk-21-jre-headless \
    postgresql-client \
    netcat-openbsd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Installation Hadoop
ARG HADOOP_VERSION=3.2.1
RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ \
    && mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
    && rm hadoop-${HADOOP_VERSION}.tar.gz

ENV HADOOP_HOME=/opt/hadoop \
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 \
    PATH=$PATH:/opt/hadoop/bin

# Installation Trino CLI
RUN wget -q https://repo1.maven.org/maven2/io/trino/trino-cli/400/trino-cli-400-executable.jar \
    -O /usr/local/bin/trino \
    && chmod +x /usr/local/bin/trino

# Configuration Python
WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Structure de dossiers
RUN mkdir -p \
    scripts/utils \
    data/raw/orders \
    data/raw/stock \
    data/generated \
    output/supplier_orders \
    logs \
    config \
    sql

# Variables d'environnement
ENV PYTHONPATH=/app/scripts:/app \
    HDFS_NAMENODE=hdfs://namenode:9000 \
    PRESTO_HOST=presto \
    PRESTO_PORT=8080 \
    POSTGRES_HOST=postgres \
    POSTGRES_PORT=5432 \
    POSTGRES_DB=procurement_db \
    POSTGRES_USER=procurement_user \
    POSTGRES_PASSWORD=procurement_pass \
    TZ=Africa/Casablanca

# Utilisateur non-root
RUN useradd -m -u 1000 procurement && \
    chown -R procurement:procurement /app

USER procurement

VOLUME ["/app/data", "/app/output", "/app/logs"]

CMD ["tail", "-f", "/dev/null"]