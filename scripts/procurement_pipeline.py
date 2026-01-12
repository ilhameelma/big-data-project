#!/usr/bin/env python3
"""
PIPELINE COMPLET - Version finale corrigée
Gère l'upload dans HDFS et le traitement des commandes
"""

import os
import subprocess
import json
import csv
import uuid
import argparse
from datetime import datetime, timedelta
from pathlib import Path

class Config:
    """Configuration globale"""
    BASE_LOCAL_DATA = os.path.abspath("../data")
    HDFS_RAW_ORDERS = "/raw/orders"
    HDFS_RAW_STOCK = "/raw/stock"
    CONTAINER_TMP = "/tmp/data_today"
    OUTPUT_DIR = Path("./supplier_orders")
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    @staticmethod
    def get_today():
        """Retourne la date du jour au format YYYY-MM-DD"""
        return datetime.now().strftime("%Y-%m-%d")

class HDFSUploader:
    """Gère l'upload des fichiers vers HDFS"""
    
    def __init__(self, target_date=None):
        self.target_date = target_date or Config.get_today()
        self.copied_files = []
    
    def run_cmd(self, cmd):
        """Exécute une commande"""
        print(f"→ {cmd}")
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            error_msg = result.stderr.strip()[:300]
            # Ignorer les avertissements SASL
            if "SASL" in error_msg and "trust check" in error_msg:
                print(f"  Avertissement SASL (normal)")
            elif "No such file or directory" in error_msg and "find" in cmd:
                # Ignorer les erreurs find pour les dossiers vides
                print(f"  Aucun fichier trouvé (normal si premier upload)")
            else:
                print(f" Erreur: {error_msg}")
        elif result.stdout.strip():
            print(f" Sortie: {result.stdout.strip()[:200]}")
        
        return result
    
    def copy_to_container(self):
        """Copie les données vers le conteneur"""
        print(f"\n Copie vers le conteneur pour {self.target_date}...")
        
        local_orders = os.path.join(Config.BASE_LOCAL_DATA, "raw_orders", f"date={self.target_date}")
        
        if not os.path.exists(local_orders):
            print(f" Dossier non trouvé: {local_orders}")
            return []
        
        # Nettoyer le répertoire temporaire
        self.run_cmd(f"docker-compose exec namenode rm -rf {Config.CONTAINER_TMP}")
        self.run_cmd(f"docker-compose exec namenode mkdir -p {Config.CONTAINER_TMP}")
        
        # Lister tous les dossiers store_id
        store_dirs = [d for d in os.listdir(local_orders) 
                     if os.path.isdir(os.path.join(local_orders, d)) and d.startswith("store_id=")]
        
        print(f" {len(store_dirs)} dossiers store_id à copier")
        
        self.copied_files = []
        
        for store_dir in store_dirs:
            store_id = store_dir.split("=")[1]
            local_file = os.path.join(local_orders, store_dir, "orders.json")
            
            if os.path.exists(local_file):
                # Créer le répertoire dans le conteneur
                container_dir = f"{Config.CONTAINER_TMP}/raw_orders/date={self.target_date}/{store_dir}/"
                self.run_cmd(f"docker-compose exec namenode mkdir -p {container_dir}")
                
                # Copier le fichier
                container_file = f"{container_dir}orders.json"
                result = self.run_cmd(f'docker cp "{local_file}" namenode:{container_file}')
                
                if result.returncode == 0:
                    file_size = os.path.getsize(local_file)
                    print(f"   {store_id}: {file_size:,} bytes")
                    self.copied_files.append({
                        'store_id': store_id,
                        'container_path': container_file,
                        'size': file_size
                    })
                else:
                    print(f"   {store_id}: échec copie")
        
        # Vérifier ce qui a été copié
        print(f"\n Vérification fichiers copiés:")
        self.run_cmd(f"docker-compose exec namenode ls -la {Config.CONTAINER_TMP}/raw_orders/date={self.target_date}/ 2>/dev/null || echo 'Répertoire vide'")
        
        return self.copied_files
    
    def upload_to_hdfs(self):
        """Upload vers HDFS"""
        print(f"\n Upload vers HDFS pour {self.target_date}...")
        
        if not self.copied_files:
            print(" Aucun fichier à uploader")
            return False
        
        success_count = 0
        
        for file_info in self.copied_files:
            store_id = file_info['store_id']
            source_file = file_info['container_path']
            
            # Chemin HDFS
            hdfs_dir = f"{Config.HDFS_RAW_ORDERS}/date={self.target_date}/store_id={store_id}/"
            hdfs_file = f"{hdfs_dir}orders.json"
            
            print(f"\n   Traitement store_id={store_id}")
            print(f"    Source: {source_file}")
            print(f"    Destination: {hdfs_file}")
            
            # Créer le répertoire HDFS
            mkdir_result = self.run_cmd(f"docker-compose exec namenode hdfs dfs -mkdir -p {hdfs_dir}")
            
            if mkdir_result.returncode != 0:
                print(f"     Impossible de créer {hdfs_dir}")
                continue
            
            # Upload le fichier
            upload_result = self.run_cmd(f"docker-compose exec namenode hdfs dfs -copyFromLocal {source_file} {hdfs_file}")
            
            if upload_result.returncode == 0:
                success_count += 1
                print(f"     Upload réussi")
                
                # Vérification rapide
                self.run_cmd(f"docker-compose exec namenode hdfs dfs -test -e {hdfs_file} && echo '    ✅ Fichier présent dans HDFS' || echo '    ❌ Fichier absent'")
            else:
                print(f"     Échec upload")
        
        print(f"\n📊 Résultat: {success_count}/{len(self.copied_files)} fichiers uploadés")
        return success_count > 0
    
    def verify_hdfs_upload(self):
        """Vérification de l'upload HDFS"""
        print(f"\n Vérification HDFS pour {self.target_date}...")
        
        # Vérifier le répertoire date
        self.run_cmd(f"docker-compose exec namenode hdfs dfs -test -d {Config.HDFS_RAW_ORDERS}/date={self.target_date} && echo '✅ Répertoire date présent' || echo '❌ Répertoire date absent'")
        
        # Lister les fichiers
        print(f"\n Fichiers dans HDFS:")
        self.run_cmd(f"docker-compose exec namenode hdfs dfs -ls -R {Config.HDFS_RAW_ORDERS}/date={self.target_date} 2>/dev/null || echo 'Aucun fichier pour cette date'")
        
        # Compter
        print(f"\n Nombre de fichiers:")
        self.run_cmd(f"docker-compose exec namenode hdfs dfs -ls {Config.HDFS_RAW_ORDERS}/date={self.target_date} 2>/dev/null | grep -c '^' || echo '0'")
        
        return True
    
    def sync_hive_partitions(self):
        """Synchronise les partitions Hive"""
        print("\n Synchronisation Hive...")
        
        self.run_cmd('docker-compose exec trino trino --execute "CALL hive.system.sync_partition_metadata(\'procurement\', \'orders_raw\', \'FULL\')"')
        self.run_cmd('docker-compose exec trino trino --execute "CALL hive.system.sync_partition_metadata(\'procurement\', \'stock_raw\', \'FULL\')"')
        
        print(" Synchronisation terminée")
        return True
    
    def run_upload_pipeline(self):
        """Exécute le pipeline complet d'upload"""
        print(f"\n{'='*80}")
        print(f"HDFS UPLOAD PIPELINE")
        print(f"Date: {self.target_date}")
        print(f"{'='*80}")
        
        try:
            # 1. Copie vers le conteneur
            copied = self.copy_to_container()
            if not copied:
                print(" Aucun fichier à copier")
                return False
            
            # 2. Upload vers HDFS
            uploaded = self.upload_to_hdfs()
            if not uploaded:
                print(" Échec de l'upload")
                return False
            
            # 3. Vérification
            self.verify_hdfs_upload()
            
            # 4. Synchronisation Hive
            self.sync_hive_partitions()
            
            print(f"\n Upload HDFS terminé avec succès")
            return True
            
        except Exception as e:
            print(f" Erreur lors de l'upload: {e}")
            import traceback
            traceback.print_exc()
            return False

class ProcurementGenerator:
    """Génère les commandes fournisseurs"""
    
    def __init__(self, target_date='2025-12-02'):
        self.target_date = target_date
        self.output_dir = Config.OUTPUT_DIR
        
    def run_trino_query_jsonl(self, query):
        """Exécute une requête Trino et parse le JSONL"""
        cmd = ['docker-compose', 'exec', '-T', 'trino', 'trino', '--output-format', 'JSON', '--execute', query]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=30)
            
            if not result.stdout.strip():
                return []
            
            # Parser le JSONL (une ligne = un objet JSON)
            data = []
            for line in result.stdout.strip().split('\n'):
                if line.strip():
                    try:
                        data.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
            
            return data
            
        except subprocess.CalledProcessError as e:
            print(f"Erreur Trino: {e.stderr}")
            return []
        except Exception as e:
            print(f"Erreur: {e}")
            return []
    
    def get_aggregated_demand(self):
        """Récupère la demande agrégée"""
        print(f"1. Calcul de la demande pour {self.target_date}...")
        
        query = f"""
        SELECT 
            sku_id,
            SUM(CAST(quantity AS INTEGER)) as total_demand,
            COUNT(*) as order_count
        FROM hive.procurement.orders_raw 
        WHERE date = '{self.target_date}'
        AND sku_id IS NOT NULL
        GROUP BY sku_id
        HAVING SUM(CAST(quantity AS INTEGER)) > 0
        ORDER BY total_demand DESC
        """
        
        data = self.run_trino_query_jsonl(query)
        print(f"    {len(data)} SKU avec demande")
        
        if data:
            print(f"   Exemple: {data[0].get('sku_id')} - {data[0].get('total_demand')} unités")
        
        return data
    
    def get_stock_data(self):
        """Récupère les données de stock"""
        print(f"2. Récupération du stock pour {self.target_date}...")
        
        query = f"""
        SELECT 
            sku_id,
            CAST(available_stock AS INTEGER) as available_stock,
            CAST(reserved_stock AS INTEGER) as reserved_stock,
            CAST(safety_stock AS INTEGER) as safety_stock
        FROM hive.procurement.stock_raw 
        WHERE date = '{self.target_date}'
        AND sku_id IS NOT NULL
        """
        
        data = self.run_trino_query_jsonl(query)
        print(f"    {len(data)} éléments de stock trouvés")
        
        if data:
            print(f"   Exemple: SKU={data[0].get('sku_id')}, Stock={data[0].get('available_stock')}")
        
        return data
    
    def get_products_with_suppliers(self):
        """Récupère les produits avec leurs fournisseurs"""
        print("3. Récupération des produits et fournisseurs...")
        
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
        """
        
        data = self.run_trino_query_jsonl(query)
        print(f"    {len(data)} produits avec fournisseurs")
        
        if data:
            print(f"   Exemple: {data[0].get('sku_id')} - {data[0].get('product_name')[:20]}...")
        
        return data
    
    def calculate_orders(self, demand_data, stock_data, product_data):
        """Calcule les commandes avec affichage détaillé des calculs"""
        print("4. Calcul des commandes...")
        print("   ──────────────────────────────────────────────────────────")
        print("   FORMULE DE CALCUL :")
        print("   Demande Nette = MAX(0, Demande Client + Stock Sécurité - Stock Disponible)")
        print("   ──────────────────────────────────────────────────────────")
        
        # Créer des dictionnaires pour un accès rapide
        demand_dict = {}
        for item in demand_data:
            sku = item.get('sku_id')
            if sku:
                demand_dict[sku] = {
                    'total_demand': int(item.get('total_demand', 0)),
                    'order_count': int(item.get('order_count', 0))
                }
        
        stock_dict = {}
        for item in stock_data:
            sku = item.get('sku_id')
            if sku:
                try:
                    stock_dict[sku] = {
                        'available_stock': int(float(item.get('available_stock', 50))),
                        'reserved_stock': int(float(item.get('reserved_stock', 0))),
                        'safety_stock': int(float(item.get('safety_stock', 10)))
                    }
                except:
                    stock_dict[sku] = {
                        'available_stock': 50,
                        'reserved_stock': 0,
                        'safety_stock': 10
                    }
        
        product_dict = {}
        for item in product_data:
            sku = item.get('sku_id')
            if sku:
                product_dict[sku] = item
        
        # Calculer les commandes
        orders = []
        
        print("   ┌─────────────────────────────────────────────────────────────────────────────────────┐")
        print("   │ DÉTAIL DES CALCULS PAR SKU                                                          │")
        print("   ├──────────────┬──────────┬──────────────┬──────────────┬──────────────┬──────────────┤")
        print("   │     SKU      │ Demande  │ Stock Disp.  │ Stock Secur. │ Besoin Net   │ Résultat     │")
        print("   ├──────────────┼──────────┼──────────────┼──────────────┼──────────────┼──────────────┤")
        
        orders_count = 0
        no_order_count = 0
        
        for sku_id, product in product_dict.items():
            if sku_id not in demand_dict:
                continue  # Pas de demande pour ce produit
            
            demand = demand_dict[sku_id]['total_demand']
            stock = stock_dict.get(sku_id, {
                'available_stock': 50,
                'reserved_stock': 0,
                'safety_stock': 10
            })
            
            # CALCUL DE LA DEMANDE NETTE
            available = stock['available_stock'] - stock['reserved_stock']
            net_demand = max(0, demand + stock['safety_stock'] - available)
            
            # Afficher le calcul
            print(f"   │ {sku_id:<12} │ {demand:<8} │ {available:<12} │ {stock['safety_stock']:<12} │ {net_demand:<12} │", end="")
            
            if net_demand > 0:
                # Appliquer les règles métier
                pack_size = max(1, int(product.get('pack_size', 1)))
                min_order_qty = int(product.get('min_order_quantity', 0))
                
                # Arrondir au pack supérieur
                packs_needed = max(1, (net_demand + pack_size - 1) // pack_size)
                order_quantity = packs_needed * pack_size
                
                # Respecter la quantité minimale
                if min_order_qty > 0 and order_quantity < min_order_qty:
                    order_quantity = min_order_qty
                
                # Créer la commande
                order_id = str(uuid.uuid4())
                unit_price = float(product.get('unit_price', 0))
                
                order_item = {
                    'order_id': order_id,
                    'order_date': self.target_date,
                    'supplier_id': product.get('supplier_id'),
                    'supplier_name': product.get('supplier_name'),
                    'sku_id': sku_id,
                    'product_name': product.get('product_name'),
                    'demand': demand,
                    'available_stock': stock['available_stock'],
                    'reserved_stock': stock['reserved_stock'],
                    'safety_stock': stock['safety_stock'],
                    'net_demand': net_demand,
                    'order_quantity': order_quantity,
                    'pack_size': pack_size,
                    'unit_price': unit_price,
                    'total_price': unit_price * order_quantity,
                    'lead_time_days': int(product.get('lead_time_days', 7)),
                    'calculated_at': datetime.now().isoformat(),
                    'calculation_details': {
                        'formula': 'max(0, demand + safety_stock - available_stock)',
                        'demand': demand,
                        'safety_stock': stock['safety_stock'],
                        'available_stock': available,
                        'calculation': f"max(0, {demand} + {stock['safety_stock']} - {available}) = {net_demand}"
                    }
                }
                
                orders.append(order_item)
                orders_count += 1
                print(f" COMMANDE {order_quantity} unités │")
                
            else:
                no_order_count += 1
                print(f" PAS DE COMMANDE     │")
        
        print("   └──────────────┴──────────┴──────────────┴──────────────┴──────────────┴──────────────┘")
        
        print("\n   ──────────────────────────────────────────────────────────")
        print("   RÉSUMÉ DES CALCULS :")
        print(f"   • {orders_count} SKU nécessitent une commande")
        print(f"   • {no_order_count} SKU n'ont pas besoin de commande (stock suffisant)")
        
        if orders_count > 0 and len(orders) > 0:
            print("\n   EXEMPLES DE CALCULS DÉTAILLÉS :")
            print("   ──────────────────────────────────────────────────────────")
            
            for i, order in enumerate(orders[:3]):  # Juste les 3 premiers
                details = order['calculation_details']
                print(f"   Exemple {i+1} - {order['sku_id']}:")
                print(f"     Formule : {details['formula']}")
                print(f"     Calcul  : {details['calculation']}")
                print(f"     Détail  : Demande({details['demand']}) + Sécurité({details['safety_stock']}) - Disponible({details['available_stock']}) = {order['net_demand']}")
                print()
        
        print(f"    {len(orders)} articles à commander")
        return orders
    
    def generate_supplier_files(self, orders):
        """Génère les fichiers par fournisseur"""
        print("5. Génération des fichiers fournisseurs...")
        
        if not orders:
            print("     Aucune commande à générer")
            return 0
        
        # Regrouper par fournisseur
        suppliers = {}
        for order in orders:
            supplier_id = order['supplier_id']
            if supplier_id not in suppliers:
                suppliers[supplier_id] = {
                    'supplier_name': order['supplier_name'],
                    'orders': []
                }
            suppliers[supplier_id]['orders'].append(order)
        
        # Générer les fichiers
        files_generated = 0
        
        for supplier_id, data in suppliers.items():
            safe_id = supplier_id.replace('/', '_')
            
            # Fichier JSON
            json_file = self.output_dir / f"supplier_{safe_id}_{self.target_date}.json"
            try:
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'supplier_id': supplier_id,
                        'supplier_name': data['supplier_name'],
                        'order_date': self.target_date,
                        'total_items': len(data['orders']),
                        'total_value': sum(o['total_price'] for o in data['orders']),
                        'generated_at': datetime.now().isoformat(),
                        'items': data['orders']
                    }, f, indent=2, ensure_ascii=False)
                
                # Fichier CSV
                csv_file = self.output_dir / f"supplier_{safe_id}_{self.target_date}.csv"
                with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(['SKU', 'PRODUIT', 'DEMANDE', 'STOCK_DISPONIBLE', 'STOCK_SECURITE', 
                                    'BESOIN_NET', 'QUANTITE_COMMANDEE', 'TAILLE_PACK', 'PRIX_UNITAIRE', 'TOTAL'])
                    
                    for order in data['orders']:
                        writer.writerow([
                            order['sku_id'],
                            order['product_name'],
                            order['demand'],
                            order['available_stock'],
                            order['safety_stock'],
                            order['net_demand'],
                            order['order_quantity'],
                            order['pack_size'],
                            f"{order['unit_price']:.2f}",
                            f"{order['total_price']:.2f}"
                        ])
                
                files_generated += 2
                total_value = sum(o['total_price'] for o in data['orders'])
                print(f"    {supplier_id}: {len(data['orders'])} articles, {total_value:.2f}€")
                
            except Exception as e:
                print(f"    Erreur pour {supplier_id}: {e}")
        
        return files_generated
    def verify_cassandra_storage(self):
        """Vérifie que les données ont bien été stockées dans Cassandra"""
        print("\n    Vérification du stockage Cassandra...")
        
        try:
            # Compter le nombre d'enregistrements pour cette date
            query = f"SELECT COUNT(*) FROM procurement.supplier_orders WHERE order_date = '{self.target_date}';"
            cmd = ['docker-compose', 'exec', '-T', 'cassandra', 'cqlsh', '-e', query]
            result = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=10)
            
            if result.returncode == 0:
                print(f"    📊 {result.stdout.strip()} commandes trouvées pour {self.target_date}")
            else:
                print(f"    ⚠️  Impossible de vérifier: {result.stderr[:100]}")
                
        except Exception as e:
            print(f"    ⚠️  Erreur lors de la vérification: {e}")
    def store_demand_calculations(self, orders, demand_data, stock_data):
        """Stocke les calculs de demande dans Cassandra"""
        print("\n7. Stockage des calculs de demande...")
        
        stored_count = 0
        error_count = 0
        
        # Créer des dictionnaires pour un accès rapide
        demand_dict = {}
        for item in demand_data:
            sku = item.get('sku_id')
            if sku:
                demand_dict[sku] = int(item.get('total_demand', 0))
        
        stock_dict = {}
        for item in stock_data:
            sku = item.get('sku_id')
            if sku:
                try:
                    stock_dict[sku] = {
                        'available_stock': int(float(item.get('available_stock', 50))),
                        'safety_stock': int(float(item.get('safety_stock', 10)))
                    }
                except:
                    stock_dict[sku] = {
                        'available_stock': 50,
                        'safety_stock': 10
                    }
        
        orders_dict = {}
        for order in orders:
            sku = order['sku_id']
            orders_dict[sku] = order['order_quantity']
        
        # Pour stocker tous les SKU avec demande
        skus_to_store = list(demand_dict.keys())
        
        print(f"    Stockage des calculs pour {len(skus_to_store)} SKU...")
        
        for i, sku_id in enumerate(skus_to_store[:200], 1):  # Limiter aux 200 premiers
            try:
                demand = demand_dict.get(sku_id, 0)
                
                # Récupérer les données de stock
                stock_info = stock_dict.get(sku_id, {
                    'available_stock': 50,
                    'safety_stock': 10
                })
                available_stock = stock_info['available_stock']
                safety_stock = stock_info['safety_stock']
                
                # Calculer la demande nette
                net_demand = max(0, demand + safety_stock - available_stock)
                
                # Récupérer la quantité commandée (si applicable)
                final_order_quantity = orders_dict.get(sku_id, 0)
                
                # Échapper les guillemets
                safe_sku_id = sku_id.replace("'", "''")
                
                query = (
                    f"INSERT INTO procurement.demand_calculations "
                    f"(calculation_date, sku_id, total_demand, available_stock, net_demand, "
                    f"final_order_quantity, calculated_at) "
                    f"VALUES ('{self.target_date}', '{safe_sku_id}', {demand}, {available_stock}, "
                    f"{net_demand}, {final_order_quantity}, toTimestamp(now()));"
                )
                
                # Exécuter la commande
                cmd = ['docker-compose', 'exec', '-T', 'cassandra', 'cqlsh', '-e', query]
                result = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=5)
                
                if result.returncode == 0:
                    stored_count += 1
                    if i <= 3:  # Afficher les 3 premiers
                        print(f"    {i:3d}. ✓ {sku_id}: dmd={demand}, stk={available_stock}, net={net_demand}")
                    elif i % 20 == 0:  # Afficher la progression
                        print(f"    Progression: {i}/{len(skus_to_store)}")
                else:
                    error_count += 1
                    if error_count <= 3:
                        print(f"    {i:3d}. ✗ {sku_id}")
                        
            except Exception as e:
                error_count += 1
        
        print(f"\n    Résumé calculs: {stored_count} calculs stockés, {error_count} erreurs")
    def store_in_cassandra(self, orders):
        """Stocke les résultats dans Cassandra"""
        print("6. Stockage dans Cassandra...")
        
        if not orders:
            print("    Aucune commande à stocker")
            return
        
        stored_count = 0
        error_count = 0
        
        for i, order in enumerate(orders, 1):
            try:
                # Utiliser uuid() de Cassandra pour générer l'ID
                # Échapper les guillemets simples dans les chaînes
                supplier_id = order['supplier_id'].replace("'", "''")
                sku_id = order['sku_id'].replace("'", "''")
                
                query = (
                    f"INSERT INTO procurement.supplier_orders "
                    f"(order_date, supplier_id, order_id, sku_id, quantity, status, generated_at) "
                    f"VALUES ('{self.target_date}', '{supplier_id}', "
                    f"uuid(), '{sku_id}', "
                    f"{order['order_quantity']}, 'GENERATED', toTimestamp(now()));"
                )
                
                # Afficher un exemple de requête
                if i == 1:
                    print(f"    Exemple de requête: {query[:150]}...")
                
                # Exécuter la commande
                cmd = ['docker-compose', 'exec', '-T', 'cassandra', 'cqlsh', '-e', query]
                result = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=5)
                
                if result.returncode == 0:
                    stored_count += 1
                    if stored_count % 10 == 0:  # Afficher la progression
                        print(f"    Progression: {stored_count}/{len(orders)}")
                else:
                    error_count += 1
                    if error_count <= 3:  # Afficher les 3 premières erreurs
                        print(f"    {i:3d}. ✗ {order['sku_id']}")
                        if result.stderr:
                            error_msg = result.stderr[:100]
                            print(f"        Erreur: {error_msg}")
                        
            except Exception as e:
                error_count += 1
                print(f"    {i:3d}. ❌ Exception: {str(e)[:50]}")
        
        print(f"\n    Résumé: {stored_count} commandes stockées, {error_count} erreurs")
        
        # Vérifier le résultat

    
    def run_processing_pipeline(self):
        """Exécute le pipeline complet de traitement"""
        print(f"\n{'='*80}")
        print(f"PROCESSING PIPELINE")
        print(f"Date: {self.target_date}")
        print(f"{'='*80}\n")
        
        try:
            # Étape 1: Demande
            demand_data = self.get_aggregated_demand()
            if not demand_data:
                print(" Aucune demande trouvée")
                return False
            
            # Étape 2: Stock
            stock_data = self.get_stock_data()
            
            # Étape 3: Produits
            product_data = self.get_products_with_suppliers()
            if not product_data:
                print(" Aucun produit trouvé")
                return False
            
            # Étape 4: Calcul
            orders = self.calculate_orders(demand_data, stock_data, product_data)
            
            if not orders:
                print(f"\n{'='*60}")
                print("  AUCUNE COMMANDE NÉCESSAIRE")
                print("   Raison : Stock suffisant pour couvrir la demande + sécurité")
                print(f"{'='*60}")
                # Stocker quand même les calculs même sans commande
                self.store_demand_calculations([], demand_data, stock_data)
                return True
            
            # Étape 5: Génération fichiers
            files_count = self.generate_supplier_files(orders)
            
            # Étape 6: Stockage des commandes dans Cassandra
            self.store_in_cassandra(orders)
            
            # Étape 7: Stockage des calculs de demande
            self.store_demand_calculations(orders, demand_data, stock_data)
            
            # Rapport final
            print(f"\n{'='*80}")
            print(" PROCESSING TERMINÉ AVEC SUCCÈS")
            print(f"{'='*80}")
            
            total_items = len(orders)
            total_value = sum(o['total_price'] for o in orders)
            supplier_count = len(set(o['supplier_id'] for o in orders))
            
            print(f"\n RÉSUMÉ DÉTAILLÉ:")
            print(f"   Commandes générées: {total_items}")
            print(f"   Fournisseurs concernés: {supplier_count}")
            print(f"   Valeur totale des commandes: {total_value:.2f}€")
            print(f"   Fichiers générés: {files_count}")
            
            # Statistiques
            if orders:
                avg_order_value = total_value / total_items
                avg_quantity = sum(o['order_quantity'] for o in orders) / total_items
                print(f"\n STATISTIQUES:")
                print(f"   Valeur moyenne par article: {avg_order_value:.2f}€")
                print(f"   Quantité moyenne commandée: {avg_quantity:.1f} unités")
            
            print(f"\n Répertoire de sortie: {self.output_dir.absolute()}")
            
            return True
            
        except Exception as e:
            print(f"\n{'='*80}")
            print(f" ERREUR: {e}")
            print(f"{'='*80}")
            import traceback
            traceback.print_exc()
            return False

class CompletePipeline:
    """Pipeline complet qui combine upload et traitement"""
    
    def __init__(self, target_date=None):
        self.target_date = target_date or Config.get_today()
        self.uploader = HDFSUploader(self.target_date)
        self.processor = ProcurementGenerator(self.target_date)
        self.start_time = datetime.now()
    
    def run(self):
        """Exécute le pipeline complet"""
        print(f"\n{'='*100}")
        print(f"PIPELINE COMPLET")
        print(f"Date: {self.target_date}")
        print(f"Temps de début: {self.start_time.strftime('%H:%M:%S')}")
        print(f"{'='*100}")
        
        # ÉTAPE 1: Upload HDFS
        print(f"\n{'='*80}")
        print(f"ÉTAPE 1: UPLOAD VERS HDFS")
        print(f"{'='*80}")
        
        upload_success = self.uploader.run_upload_pipeline()
        if not upload_success:
            print(" Échec de l'upload HDFS, arrêt du pipeline")
            return False
        
        # Pause pour laisser Hive se synchroniser
        import time
        print("\n Attente de 5 secondes pour la synchronisation Hive...")
        time.sleep(5)
        
        # ÉTAPE 2: Traitement des données
        print(f"\n{'='*80}")
        print(f"ÉTAPE 2: TRAITEMENT DES DONNÉES")
        print(f"{'='*80}")
        
        processing_success = self.processor.run_processing_pipeline()
        
        # Rapport final
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        print(f"\n{'='*100}")
        print(f"RAPPORT FINAL DU PIPELINE")
        print(f"{'='*100}")
        print(f"Date traitée: {self.target_date}")
        print(f"Début: {self.start_time.strftime('%H:%M:%S')}")
        print(f"Fin: {end_time.strftime('%H:%M:%S')}")
        print(f"Durée totale: {duration}")
        print(f"Étape 1 (HDFS Upload): {' Succès' if upload_success else '❌ Échec'}")
        print(f"Étape 2 (Traitement): {' Succès' if processing_success else '❌ Échec'}")
        
        if upload_success and processing_success:
            print(f"\n🎉 PIPELINE COMPLET TERMINÉ AVEC SUCCÈS!")
        else:
            print(f"\n  PIPELINE TERMINÉ AVEC DES PROBLÈMES")
        
        print(f"{'='*100}")
        
        return upload_success and processing_success

def main():
    """Fonction principale"""
    parser = argparse.ArgumentParser(
        description='Pipeline complet: Upload HDFS + Traitement des commandes fournisseurs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'utilisation:
  python3 pipeline_complete.py --date 2026-01-08    # Traite une date spécifique
  python3 pipeline_complete.py                       # Traite la date d'aujourd'hui
  python3 pipeline_complete.py --upload-only        # Upload seulement
  python3 pipeline_complete.py --process-only       # Traitement seulement
        """
    )
    
    parser.add_argument('--date', 
                       default=Config.get_today(),
                       help=f'Date à traiter (format: YYYY-MM-DD, défaut: aujourd\'hui)')
    
    parser.add_argument('--upload-only',
                       action='store_true',
                       help='Exécuter seulement l\'upload HDFS')
    
    parser.add_argument('--process-only',
                       action='store_true',
                       help='Exécuter seulement le traitement des données')
    
    parser.add_argument('--test-stock',
                       action='store_true',
                       help='Tester la structure du stock')
    
    parser.add_argument('--verbose', '-v',
                       action='store_true',
                       help='Afficher plus de détails')
    
    args = parser.parse_args()
    
    if args.upload_only:
        # Upload HDFS seulement
        uploader = HDFSUploader(args.date)
        success = uploader.run_upload_pipeline()
        exit(0 if success else 1)
    
    elif args.process_only:
        # Traitement seulement
        processor = ProcurementGenerator(args.date)
        success = processor.run_processing_pipeline()
        exit(0 if success else 1)
    
    elif args.test_stock:
        # Tester la structure du stock
        processor = ProcurementGenerator(args.date)
        
        print("Test de la structure du stock_raw...")
        
        # Voir les premières lignes brutes
        query = f"SELECT * FROM hive.procurement.stock_raw WHERE date = '{args.date}' LIMIT 5"
        cmd = ['docker-compose', 'exec', '-T', 'trino', 'trino', '--execute', query]
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        
        print("Résultat brut:")
        print(result.stdout)
        
        # Essayer différentes colonnes
        queries = [
            ("Test 1 - toutes colonnes", f"SELECT * FROM hive.procurement.stock_raw WHERE date = '{args.date}' LIMIT 3"),
            ("Test 2 - colonnes individuelles", f"SELECT sku_id, available_stock, reserved_stock, safety_stock FROM hive.procurement.stock_raw WHERE date = '{args.date}' LIMIT 3"),
            ("Test 3 - avec filtrage SKU", f"SELECT * FROM hive.procurement.stock_raw WHERE date = '{args.date}' AND reserved_stock LIKE 'SKU%' LIMIT 3"),
        ]
        
        for name, query in queries:
            print(f"\n{name}:")
            print(query)
            cmd = ['docker-compose', 'exec', '-T', 'trino', 'trino', '--execute', query]
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            print(result.stdout)
        
        return
    
    else:
        # Pipeline complet
        pipeline = CompletePipeline(args.date)
        success = pipeline.run()
        exit(0 if success else 1)

if __name__ == "__main__":
    main()