networks:
  hadoop_net:
    driver: bridge

volumes:
  namenode_data:
  datanode_data:
  postgres_data:
  # presto_data:  # Supprimons le volume nommé

services:
  # -------------------------
  # Hadoop NameNode
  # -------------------------
  hadoop-namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: hadoop-namenode
    environment:
      - CLUSTER_NAME=procurement
      - CORE_CONF_fs_defaultFS=hdfs://hadoop-namenode:9000
    ports:
      - "9870:9870"
      - "9000:9000"
    networks:
      - hadoop_net
    volumes:
      - namenode_data:/hadoop/dfs/name

  # -------------------------
  # Hadoop DataNode
  # -------------------------
  hadoop-datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: hadoop-datanode
    environment:
      - CORE_CONF_fs_defaultFS=hdfs://hadoop-namenode:9000
    depends_on:
      - hadoop-namenode
    networks:
      - hadoop_net
    ports:
      - "9864:9864"
    volumes:
      - datanode_data:/hadoop/dfs/data

  # -------------------------
  # PostgreSQL
  # -------------------------
  postgres:
    image: postgres:15
    container_name: postgres
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: procurement
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - hadoop_net

  # -------------------------
  # Presto (Trino) - Version corrigée pour Windows
  # -------------------------
  presto-coordinator:
    image: trinodb/trino:latest
    container_name: presto-coordinator
    depends_on:
      - hadoop-namenode
      - hadoop-datanode
      - postgres
    networks:
      - hadoop_net
    ports:
      - "8080:8080"
    volumes:
      - ./presto-config:/etc/trino
      - ./presto-data:/var/trino/data  # Répertoire local
    environment:
      - NODE_ENVIRONMENT=production
    # SUPPRIMER la commande personnalisée - utiliser celle par défaut

  # -------------------------
  # Python ETL / Orchestrateur
  # -------------------------
  etl-orchestrator:
    image: python:3.11-slim
    container_name: etl-orchestrator
    volumes:
      - ./scripts:/scripts
      - ./data:/data
      - ./logs:/logs
      - ./presto-config:/presto-config:ro
    working_dir: /scripts
    depends_on:
      - presto-coordinator
      - postgres
    networks:
      - hadoop_net
    environment:
      - HDFS_NAMENODE=hadoop-namenode:9000
      - POSTGRES_HOST=postgres
      - POSTGRES_PORT=5432
      - PRESTO_HOST=presto-coordinator
      - PRESTO_PORT=8080
    command: ["tail", "-f", "/dev/null"]