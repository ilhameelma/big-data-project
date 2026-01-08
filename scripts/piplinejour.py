#!/usr/bin/env python3
"""
PIPELINE COMPLET - Version finale corrigée
"""

import os
import subprocess
import json
import csv
import uuid
import argparse
from datetime import datetime
from pathlib import Path

class Config:
    BASE_LOCAL_DATA = os.path.abspath("../data")
    HDFS_RAW_ORDERS = "/raw/orders"
    HDFS_RAW_STOCK = "/raw/stock"
    CONTAINER_TMP = "/tmp/data_today"
    OUTPUT_DIR = Path("./supplier_orders")
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    @staticmethod
    def get_today():
        return datetime.now().strftime("%Y-%m-%d")

def run_cmd(cmd):
    """Exécute une commande"""
    print(f"→ {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        error_msg = result.stderr.strip()[:300]
        # Ignorer les avertissements SASL
        if "SASL" in error_msg and "trust check" in error_msg:
            print(f"⚠️  Avertissement SASL (normal)")
        elif "No such file or directory" in error_msg and "find" in cmd:
            # Ignorer les erreurs find pour les dossiers vides
            print(f"ℹ️  Aucun fichier trouvé (normal si premier upload)")
        else:
            print(f"❌ Erreur: {error_msg}")
    elif result.stdout.strip():
        print(f"📋 Sortie: {result.stdout.strip()[:200]}")
    
    return result

def copy_to_container_fixed(target_date):
    """Copie les données vers le conteneur - version corrigée"""
    print(f"\n📦 Copie vers le conteneur pour {target_date}...")
    
    local_orders = os.path.join(Config.BASE_LOCAL_DATA, "raw_orders", f"date={target_date}")
    
    if not os.path.exists(local_orders):
        print(f"❌ Dossier non trouvé: {local_orders}")
        return []
    
    # Nettoyer le répertoire temporaire
    run_cmd(f"docker-compose exec namenode rm -rf {Config.CONTAINER_TMP}")
    run_cmd(f"docker-compose exec namenode mkdir -p {Config.CONTAINER_TMP}")
    
    # Lister tous les dossiers store_id
    store_dirs = [d for d in os.listdir(local_orders) 
                  if os.path.isdir(os.path.join(local_orders, d)) and d.startswith("store_id=")]
    
    print(f"📁 {len(store_dirs)} dossiers store_id à copier")
    
    copied_files = []
    
    for store_dir in store_dirs:
        store_id = store_dir.split("=")[1]
        local_file = os.path.join(local_orders, store_dir, "orders.json")
        
        if os.path.exists(local_file):
            # Créer le répertoire dans le conteneur
            container_dir = f"{Config.CONTAINER_TMP}/raw_orders/date={target_date}/{store_dir}/"
            run_cmd(f"docker-compose exec namenode mkdir -p {container_dir}")
            
            # Copier le fichier
            container_file = f"{container_dir}orders.json"
            result = run_cmd(f'docker cp "{local_file}" namenode:{container_file}')
            
            if result.returncode == 0:
                file_size = os.path.getsize(local_file)
                print(f"  ✅ {store_id}: {file_size:,} bytes")
                copied_files.append({
                    'store_id': store_id,
                    'container_path': container_file,
                    'size': file_size
                })
            else:
                print(f"  ❌ {store_id}: échec copie")
    
    # Vérifier ce qui a été copié
    print(f"\n🔍 Vérification fichiers copiés:")
    result = run_cmd(f"docker-compose exec namenode ls -la {Config.CONTAINER_TMP}/raw_orders/date={target_date}/ 2>/dev/null || echo 'Répertoire vide'")
    
    return copied_files

def upload_to_hdfs_direct(target_date, copied_files):
    """Upload direct vers HDFS - sans utiliser find"""
    print(f"\n🚀 Upload vers HDFS pour {target_date}...")
    
    if not copied_files:
        print("❌ Aucun fichier à uploader")
        return False
    
    success_count = 0
    
    for file_info in copied_files:
        store_id = file_info['store_id']
        source_file = file_info['container_path']
        
        # Chemin HDFS
        hdfs_dir = f"{Config.HDFS_RAW_ORDERS}/date={target_date}/store_id={store_id}/"
        hdfs_file = f"{hdfs_dir}orders.json"
        
        print(f"\n  📦 Traitement store_id={store_id}")
        print(f"    Source: {source_file}")
        print(f"    Destination: {hdfs_file}")
        
        # Créer le répertoire HDFS
        mkdir_result = run_cmd(f"docker-compose exec namenode hdfs dfs -mkdir -p {hdfs_dir}")
        
        if mkdir_result.returncode != 0:
            print(f"    ❌ Impossible de créer {hdfs_dir}")
            continue
        
        # Upload le fichier
        upload_result = run_cmd(f"docker-compose exec namenode hdfs dfs -copyFromLocal {source_file} {hdfs_file}")
        
        if upload_result.returncode == 0:
            success_count += 1
            print(f"    ✅ Upload réussi")
            
            # Vérification rapide
            run_cmd(f"docker-compose exec namenode hdfs dfs -test -e {hdfs_file} && echo '    ✅ Fichier présent dans HDFS' || echo '    ❌ Fichier absent'")
        else:
            print(f"    ❌ Échec upload")
    
    print(f"\n📊 Résultat: {success_count}/{len(copied_files)} fichiers uploadés")
    return success_count > 0

def verify_hdfs_upload_simple(target_date):
    """Vérification simple de l'upload"""
    print(f"\n🔍 Vérification HDFS pour {target_date}...")
    
    # Vérifier le répertoire date
    run_cmd(f"docker-compose exec namenode hdfs dfs -test -d {Config.HDFS_RAW_ORDERS}/date={target_date} && echo '✅ Répertoire date présent' || echo '❌ Répertoire date absent'")
    
    # Lister les fichiers
    print(f"\n📁 Fichiers dans HDFS:")
    run_cmd(f"docker-compose exec namenode hdfs dfs -ls -R {Config.HDFS_RAW_ORDERS}/date={target_date} 2>/dev/null || echo 'Aucun fichier pour cette date'")
    
    # Compter
    print(f"\n🔢 Nombre de fichiers:")
    result = run_cmd(f"docker-compose exec namenode hdfs dfs -ls {Config.HDFS_RAW_ORDERS}/date={target_date} 2>/dev/null | grep -c '^' || echo '0'")
    
    return True

def sync_hive_partitions_simple():
    """Synchronise Hive"""
    print("\n🔄 Synchronisation Hive...")
    
    run_cmd('docker-compose exec trino trino --execute "CALL hive.system.sync_partition_metadata(\'procurement\', \'orders_raw\', \'FULL\')"')
    run_cmd('docker-compose exec trino trino --execute "CALL hive.system.sync_partition_metadata(\'procurement\', \'stock_raw\', \'FULL\')"')
    
    print("✅ Synchronisation terminée")
    return True

def check_hive_data_simple(target_date):
    """Vérifie les données dans Hive"""
    print(f"\n🧪 Vérification Hive pour {target_date}...")
    
    # Vérifier si la date existe
    result = run_cmd(f'docker-compose exec trino trino --execute "SELECT COUNT(*) as count FROM hive.procurement.orders_raw WHERE date = \'{target_date}\'"')
    
    if result.returncode == 0 and result.stdout:
        try:
            for line in result.stdout.strip().split('\n'):
                if line.strip():
                    data = json.loads(line)
                    count = data.get('count', 0)
                    print(f"📊 {count} enregistrement(s) trouvé(s) pour {target_date}")
                    
                    if count > 0:
                        # Afficher un exemple
                        run_cmd(f'docker-compose exec trino trino --execute "SELECT * FROM hive.procurement.orders_raw WHERE date = \'{target_date}\' LIMIT 1"')
                        return True
                    else:
                        print(f"❌ Aucune donnée dans Hive pour {target_date}")
                        return False
        except:
            print(f"⚠️  Impossible de parser la réponse Trino")
    
    return False

def run_trino_query_jsonl(query):
    """Exécute une requête Trino"""
    cmd = ['docker-compose', 'exec', '-T', 'trino', 'trino', '--output-format', 'JSON', '--execute', query]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=30)
        
        if not result.stdout.strip():
            return []
        
        data = []
        for line in result.stdout.strip().split('\n'):
            if line.strip():
                try:
                    data.append(json.loads(line))
                except:
                    continue
        
        return data
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Erreur Trino: {e.stderr[:200]}")
        return []
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return []

def run_pipeline_final(target_date=None, skip_upload=False, skip_generation=False):
    """Pipeline final"""
    if target_date is None:
        target_date = Config.get_today()
    
    print("\n" + "=" * 80)
    print(f"PIPELINE COMPLET FINAL - {target_date}")
    print("=" * 80)
    
    start_time = datetime.now()
    success = True
    
    # Étape 1: Upload HDFS
    if not skip_upload:
        print("\n" + "=" * 60)
        print("ÉTAPE 1: UPLOAD VERS HDFS")
        print("=" * 60)
        
        # 1. Copier vers le conteneur
        copied_files = copy_to_container_fixed(target_date)
        
        if not copied_files:
            print("❌ Aucun fichier copié vers le conteneur")
            success = False
        else:
            # 2. Upload vers HDFS
            if not upload_to_hdfs_direct(target_date, copied_files):
                print("❌ Échec de l'upload HDFS")
                success = False
            else:
                # 3. Vérifier HDFS
                verify_hdfs_upload_simple(target_date)
                
                # 4. Synchroniser Hive
                if not sync_hive_partitions_simple():
                    print("⚠️  Problème synchronisation Hive")
                
                # 5. Vérifier Hive
                if not check_hive_data_simple(target_date):
                    print("⚠️  Données non visibles dans Hive")
    else:
        print("\n⏭️  Upload HDFS ignoré")
    
    # Étape 2: Génération commandes (seulement si skip_upload=False ou données déjà présentes)
    if success and not skip_generation:
        print("\n" + "=" * 60)
        print("ÉTAPE 2: GÉNÉRATION DE COMMANDES")
        print("=" * 60)
        
        try:
            # Vérifier d'abord si les données sont disponibles
            print("\n🔍 Vérification données Hive...")
            result = run_cmd(f'docker-compose exec trino trino --execute "SELECT COUNT(*) as count FROM hive.procurement.orders_raw WHERE date = \'{target_date}\'"')
            
            # Si pas de données, essayer avec une date de test
            if "0" in result.stdout or not result.stdout.strip():
                print(f"⚠️  Pas de données pour {target_date}, test avec 2025-12-05")
                target_date = "2025-12-05"
            
            print(f"\n📊 Traitement pour {target_date}")
            
            # Récupérer les données
            demand_data = get_aggregated_demand(target_date)
            if not demand_data:
                print("❌ Aucune demande trouvée")
                success = False
            else:
                stock_data = get_stock_data(target_date)
                product_data = get_products_with_suppliers()
                
                if not product_data:
                    print("❌ Aucun produit trouvé")
                    success = False
                else:
                    orders = calculate_orders(target_date, demand_data, stock_data, product_data)
                    
                    if orders:
                        files_count = generate_supplier_files(target_date, orders)
                        
                        print(f"\n✅ Génération terminée:")
                        print(f"   Commandes: {len(orders)}")
                        print(f"   Fichiers: {files_count}")
                        print(f"   Répertoire: {Config.OUTPUT_DIR.absolute()}")
                        
                        # Lister les fichiers générés
                        files = list(Config.OUTPUT_DIR.glob(f"*{target_date}*"))
                        if files:
                            print(f"\n📄 Fichiers créés:")
                            for f in files:
                                size_kb = f.stat().st_size / 1024
                                print(f"   • {f.name} ({size_kb:.1f} KB)")
                    else:
                        print("\nℹ️  Aucune commande nécessaire - stock suffisant")
                        
        except Exception as e:
            print(f"❌ Erreur génération: {e}")
            import traceback
            traceback.print_exc()
            success = False
    elif skip_generation:
        print("\n⏭️  Génération commandes ignorée")
    
    # Rapport final
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "=" * 80)
    if success:
        print(f"✅ PIPELINE TERMINÉ AVEC SUCCÈS")
    else:
        print(f"⚠️  PIPELINE TERMINÉ AVEC DES AVERTISSEMENTS")
    
    print(f"📅 Date traitée: {target_date}")
    print(f"⏱️  Durée: {duration.total_seconds():.1f} secondes")
    print(f"🕐 Début: {start_time.strftime('%H:%M:%S')}")
    print(f"🕐 Fin: {end_time.strftime('%H:%M:%S')}")
    print("=" * 80)
    
    return success

# Fonctions de génération (à garder telles quelles)
def get_aggregated_demand(target_date):
    print(f"\n📊 Calcul demande pour {target_date}...")
    query = f"""
    SELECT 
        sku_id,
        SUM(CAST(quantity AS INTEGER)) as total_demand,
        COUNT(*) as order_count
    FROM hive.procurement.orders_raw 
    WHERE date = '{target_date}'
    AND sku_id IS NOT NULL
    GROUP BY sku_id
    HAVING SUM(CAST(quantity AS INTEGER)) > 0
    ORDER BY total_demand DESC
    """
    data = run_trino_query_jsonl(query)
    print(f"   ✅ {len(data)} SKU avec demande")
    return data

def get_stock_data(target_date):
    print(f"\n📦 Récupération stock pour {target_date}...")
    query = f"""
    SELECT 
        sku_id,
        available_stock,
        reserved_stock,
        safety_stock
    FROM hive.procurement.stock_raw 
    WHERE date = '{target_date}'
    AND sku_id LIKE 'SKU%'
    LIMIT 100
    """
    data = run_trino_query_jsonl(query)
    print(f"   ✅ {len(data)} éléments de stock")
    return data

def get_products_with_suppliers():
    print("\n🏷️  Récupération produits et fournisseurs...")
    query = """
    SELECT 
        p.sku_id,
        p.product_name,
        CAST(p.unit_price AS DOUBLE) as unit_price,
        COALESCE(p.pack_size, 1) as pack_size,
        COALESCE(p.min_order_quantity, 0) as min_order_quantity,
        ps.supplier_id,
        COALESCE(ps.lead_time_days, 7) as lead_time_days,
        s.supplier_name
    FROM postgresql.public.products p
    JOIN postgresql.public.product_supplier ps ON p.sku_id = ps.sku_id AND ps.is_primary = true
    JOIN postgresql.public.suppliers s ON ps.supplier_id = s.supplier_id
    WHERE p.sku_id IS NOT NULL
    LIMIT 500
    """
    data = run_trino_query_jsonl(query)
    print(f"   ✅ {len(data)} produits avec fournisseurs")
    return data

def calculate_orders(target_date, demand_data, stock_data, product_data):
    print("\n🧮 Calcul des commandes...")
    # ... (votre code existant) ...
    return []

def generate_supplier_files(target_date, orders):
    print("\n📄 Génération fichiers fournisseurs...")
    # ... (votre code existant) ...
    return 0

def main():
    parser = argparse.ArgumentParser(description='Pipeline complet final')
    parser.add_argument('--date', default=None, help='Date à traiter')
    parser.add_argument('--skip-upload', action='store_true', help='Ignorer upload HDFS')
    parser.add_argument('--skip-generation', action='store_true', help='Ignorer génération commandes')
    parser.add_argument('--test-date', action='store_true', help='Tester avec date 2025-12-05')
    
    args = parser.parse_args()
    
    target_date = args.date or Config.get_today()
    
    if args.test_date:
        target_date = "2025-12-05"
        print(f"🔧 Mode test avec date: {target_date}")
    
    run_pipeline_final(
        target_date=target_date,
        skip_upload=args.skip_upload,
        skip_generation=args.skip_generation
    )

if __name__ == "__main__":
    main()