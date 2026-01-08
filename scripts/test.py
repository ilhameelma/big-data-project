#!/usr/bin/env python3
"""
Script de diagnostic pour vérifier les données dans Trino/Hive
"""

import subprocess
import json
import sys
from datetime import datetime

def run_trino_query(query):
    """Exécute une requête Trino et retourne le résultat"""
    cmd = ['docker-compose', 'exec', '-T', 'trino', 'trino', '--output-format', 'JSON', '--execute', query]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            print(f"❌ Erreur: {result.stderr}")
            return []
        
        data = []
        for line in result.stdout.strip().split('\n'):
            if line.strip():
                try:
                    data.append(json.loads(line))
                except json.JSONDecodeError:
                    print(f"Ligne non-JSON: {line[:100]}")
        
        return data
        
    except Exception as e:
        print(f"Exception: {e}")
        return []

def diagnose():
    """Diagnostique les problèmes"""
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"🔍 Diagnostic pour {today}")
    print("="*80)
    
    # 1. Vérifier les bases de données
    print("\n1. 📁 Bases de données disponibles:")
    databases = run_trino_query("SHOW SCHEMAS FROM hive")
    for db in databases:
        print(f"   - {db.get('Schema', 'N/A')}")
    
    # 2. Vérifier les tables dans procurement
    print("\n2. 📊 Tables dans hive.procurement:")
    tables = run_trino_query("SHOW TABLES FROM hive.procurement")
    if not tables:
        print("   ❌ La base 'procurement' n'existe pas ou est vide")
        
        # Créer la base si elle n'existe pas
        print("\n   Tentative de création de la base...")
        create_db = run_trino_query("CREATE SCHEMA IF NOT EXISTS hive.procurement")
        print(f"   Création: {'✅' if create_db is not None else '❌'}")
        
        # Vérifier à nouveau
        tables = run_trino_query("SHOW TABLES FROM hive.procurement")
    
    for table in tables:
        table_name = table.get('Table', 'N/A')
        print(f"   - {table_name}")
        
        # Vérifier la structure
        print(f"     Structure de {table_name}:")
        try:
            desc = run_trino_query(f"DESCRIBE hive.procurement.{table_name}")
            for col in desc[:5]:  # Afficher les 5 premières colonnes
                print(f"       {col.get('Column', 'N/A')} - {col.get('Type', 'N/A')}")
            if len(desc) > 5:
                print(f"       ... et {len(desc)-5} autres colonnes")
        except:
            print("       (Impossible de récupérer la structure)")
    
    # 3. Vérifier les données dans orders_raw
    print("\n3. 🔎 Données dans orders_raw:")
    
    # Vérifier les dates disponibles
    print("   Dates disponibles:")
    dates = run_trino_query("SELECT DISTINCT date FROM hive.procurement.orders_raw ORDER BY date DESC LIMIT 10")
    if dates:
        for d in dates:
            print(f"     - {d.get('date', 'N/A')}")
    else:
        print("     ❌ Aucune date trouvée")
    
    # Vérifier le nombre total d'enregistrements
    print("   Nombre total d'enregistrements:")
    count = run_trino_query("SELECT COUNT(*) as total FROM hive.procurement.orders_raw")
    if count:
        print(f"     - {count[0].get('total', 0)} enregistrements")
    
    # Vérifier les données pour aujourd'hui
    print(f"   Données pour {today}:")
    today_data = run_trino_query(f"""
    SELECT 
        date,
        sku_id,
        quantity,
        order_id
    FROM hive.procurement.orders_raw 
    WHERE date = '{today}'
    LIMIT 5
    """)
    
    if today_data:
        print(f"     ✅ {len(today_data)} enregistrements trouvés")
        for i, row in enumerate(today_data):
            print(f"       {i+1}. SKU: {row.get('sku_id', 'N/A')}, Qty: {row.get('quantity', 'N/A')}")
    else:
        print(f"     ❌ Aucune donnée pour {today}")
        
        # Vérifier avec différents formats de date
        print("\n   Recherche avec différents formats de date:")
        date_formats = [
            today.replace('-', ''),  # 20260108
            today.replace('-', '/'), # 2026/01/08
            today.split('-')[2] + '/' + today.split('-')[1] + '/' + today.split('-')[0], # 08/01/2026
        ]
        
        for date_fmt in date_formats:
            test_data = run_trino_query(f"""
            SELECT COUNT(*) as count 
            FROM hive.procurement.orders_raw 
            WHERE date = '{date_fmt}' OR CAST(date AS VARCHAR) LIKE '%{date_fmt}%'
            """)
            if test_data and test_data[0].get('count', 0) > 0:
                print(f"     ✅ Données trouvées avec format: {date_fmt}")
                break
    
    # 4. Vérifier les données de stock
    print("\n4. 📦 Données dans stock_raw:")
    
    # Vérifier les dates disponibles
    stock_dates = run_trino_query("SELECT DISTINCT date FROM hive.procurement.stock_raw ORDER BY date DESC LIMIT 5")
    if stock_dates:
        print("   Dates disponibles:")
        for d in stock_dates:
            print(f"     - {d.get('date', 'N/A')}")
    
    # Vérifier la structure
    print("   Structure de la table (premières colonnes):")
    stock_desc = run_trino_query("DESCRIBE hive.procurement.stock_raw")
    for i, col in enumerate(stock_desc[:10]):
        print(f"     {i+1}. {col.get('Column', 'N/A')} - {col.get('Type', 'N/A')}")
    
    # 5. Vérifier les données PostgreSQL
    print("\n5. 🐘 Données PostgreSQL (produits):")
    
    try:
        products = run_trino_query("SELECT COUNT(*) as count FROM postgresql.public.products")
        if products:
            print(f"   ✅ {products[0].get('count', 0)} produits trouvés")
        
        suppliers = run_trino_query("SELECT COUNT(*) as count FROM postgresql.public.suppliers")
        if suppliers:
            print(f"   ✅ {suppliers[0].get('count', 0)} fournisseurs trouvés")
            
    except:
        print("   ❌ Impossible d'accéder à PostgreSQL")
    
    print("\n" + "="*80)
    print("🎯 ACTIONS RECOMMANDÉES:")
    
    if not tables:
        print("1. Créer les tables Hive manuellement:")
        print("""
        docker-compose exec trino trino --execute "
        CREATE TABLE hive.procurement.orders_raw (
            order_id VARCHAR,
            sku_id VARCHAR,
            quantity VARCHAR,
            date VARCHAR
        ) WITH (
            format = 'TEXTFILE',
            external_location = 'hdfs://namenode:8020/raw/orders/'
        )"
        """)
        
        print("""
        docker-compose exec trino trino --execute "
        CREATE TABLE hive.procurement.stock_raw (
            sku_id VARCHAR,
            available_stock VARCHAR,
            reserved_stock VARCHAR,
            safety_stock VARCHAR,
            date VARCHAR
        ) WITH (
            format = 'TEXTFILE',
            external_location = 'hdfs://namenode:8020/raw/stock/'
        )"
        """)
    
    print("\n2. Vérifier le format des données dans HDFS:")
    print("   docker-compose exec namenode hdfs dfs -cat /raw/orders/date=2026-01-08/* | head -5")
    print("   docker-compose exec namenode hdfs dfs -cat /raw/stock/date=2026-01-08/* | head -5")
    
    print("\n3. Tester une requête simple pour voir les colonnes:")
    print(f"""
    docker-compose exec trino trino --execute "
    SELECT * 
    FROM hive.procurement.orders_raw 
    LIMIT 1
    "
    """)

if __name__ == "__main__":
    diagnose()