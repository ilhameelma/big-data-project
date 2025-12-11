# Procurement Pipeline Project

Pipeline Big Data pour système d'approvisionnement

## Architecture
- HDFS: Stockage distribué
- PostgreSQL: Base de données relationnelle
- Python: Traitement des données

## Installation
1. docker-compose up -d
2. python scripts/generate_data_hdfs.py
3. python scripts/pipeline_bigdata.py

## Structure HDFS
- /raw/orders/: Commandes clients
- /raw/stocks/: Niveaux de stock
- /processed/: Données transformées
- /output/: Commandes fournisseurs
- /logs/: Logs et exceptions