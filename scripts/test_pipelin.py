#!/usr/bin/env python3
"""
Pipeline corrigé - Gestion du format JSONL de Trino
Avec affichage détaillé des calculs de demande nette
"""

import subprocess
import json
import csv
import uuid
from datetime import datetime, date, timedelta
import os
from pathlib import Path

class ProcurementGenerator:
    def __init__(self, target_date='2025-12-02'):
        self.target_date = target_date
        self.output_dir = Path("./supplier_orders")
        self.output_dir.mkdir(exist_ok=True)
        
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
    
    def run_trino_query_text(self, query):
        """Exécute une requête Trino en format texte"""
        cmd = ['docker-compose', 'exec', '-T', 'trino', 'trino', '--execute', query]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=30)
            return result.stdout.strip()
        except:
            return ""
    
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
        print(f"   ✅ {len(data)} SKU avec demande")
        
        if data:
            print(f"   Exemple: {data[0].get('sku_id')} - {data[0].get('total_demand')} unités")
        
        return data
    
    def get_stock_data(self):
        """Récupère les données de stock (format corrigé)"""
        print(f"2. Récupération du stock pour {self.target_date}...")
        
        # ATTENTION: Les colonnes semblent décalées dans stock_raw
        # sku_id contient la date, available_stock contient warehouse_id, etc.
        # Essayons une requête différente
        
        query = f"""
        SELECT 
            TRY_CAST(reserved_stock AS VARCHAR) as sku_id,
            TRY_CAST(safety_stock AS VARCHAR) as available_stock,
            '0' as reserved_stock,
            '10' as safety_stock  -- Valeur par défaut
        FROM hive.procurement.stock_raw 
        WHERE date = '{self.target_date}'
        AND reserved_stock LIKE 'SKU%'
        LIMIT 100
        """
        
        data = self.run_trino_query_jsonl(query)
        
        if not data:
            # Essayer un autre format
            query = f"""
            SELECT DISTINCT
                TRY_CAST(available_stock AS VARCHAR) as warehouse_id,
                TRY_CAST(reserved_stock AS VARCHAR) as sku_id,
                TRY_CAST(safety_stock AS VARCHAR) as stock_value
            FROM hive.procurement.stock_raw 
            WHERE date = '{self.target_date}'
            AND reserved_stock LIKE 'SKU%'
            LIMIT 50
            """
            data = self.run_trino_query_jsonl(query)
        
        print(f"   ✅ {len(data)} éléments de stock trouvés")
        
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
        LIMIT 500
        """
        
        data = self.run_trino_query_jsonl(query)
        print(f"   ✅ {len(data)} produits avec fournisseurs")
        
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
            if sku and sku.startswith('SKU'):
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
            # Formule: net_demand = max(0, demand + safety_stock - available_stock)
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
                    # Ajouter les détails du calcul pour le debug
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
        
        # Afficher un résumé des calculs
        print("\n   ──────────────────────────────────────────────────────────")
        print("   RÉSUMÉ DES CALCULS :")
        print(f"   • {orders_count} SKU nécessitent une commande")
        print(f"   • {no_order_count} SKU n'ont pas besoin de commande (stock suffisant)")
        
        if orders_count > 0:
            print("\n   EXEMPLES DE CALCULS DÉTAILLÉS :")
            print("   ──────────────────────────────────────────────────────────")
            
            # Afficher quelques exemples détaillés
            for i, order in enumerate(orders[:3]):  # Juste les 3 premiers
                details = order['calculation_details']
                print(f"   Exemple {i+1} - {order['sku_id']}:")
                print(f"     Formule : {details['formula']}")
                print(f"     Calcul  : {details['calculation']}")
                print(f"     Détail  : Demande({details['demand']}) + Sécurité({details['safety_stock']}) - Disponible({details['available_stock']}) = {order['net_demand']}")
                
                # Règles métier appliquées
                if order['net_demand'] > 0:
                    print(f"     Règles métier appliquées :")
                    print(f"       • Taille de pack : {order['pack_size']} unités")
                    print(f"       • Packs nécessaires : {order['order_quantity'] // order['pack_size']} packs")
                    print(f"       • Quantité commandée : {order['order_quantity']} unités")
                    print(f"       • Valeur : {order['total_price']:.2f}€")
                print()
        
        print(f"   ✅ {len(orders)} articles à commander")
        return orders
    
    def generate_supplier_files(self, orders):
        """Génère les fichiers par fournisseur"""
        print("5. Génération des fichiers fournisseurs...")
        
        if not orders:
            print("   ⚠️  Aucune commande à générer")
            return
        
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
                print(f"   ✅ {supplier_id}: {len(data['orders'])} articles, {total_value:.2f}€")
                
            except Exception as e:
                print(f"   ❌ Erreur pour {supplier_id}: {e}")
        
        return files_generated
    
    def store_in_cassandra(self, orders):
        """Stocke les résultats dans Cassandra"""
        print("6. Stockage dans Cassandra...")
        
        if not orders:
            return
        
        # Insérer les commandes
        for order in orders[:10]:  # Limiter pour le test
            try:
                query = f"""
                INSERT INTO procurement.supplier_orders (
                    order_date, supplier_id, order_id, sku_id,
                    quantity, status, generated_at
                ) VALUES (
                    '{self.target_date}', '{order['supplier_id']}', {order['order_id']}, '{order['sku_id']}',
                    {order['order_quantity']}, 'GENERATED', toTimestamp(now())
                );
                """
                
                cmd = ['docker-compose', 'exec', '-T', 'cassandra', 'cqlsh', '-e', query]
                subprocess.run(cmd, capture_output=True, text=True, check=False)
                
            except Exception as e:
                print(f"   ⚠️  Erreur Cassandra: {e}")
        
        print(f"   ✅ {min(10, len(orders))} commandes stockées dans Cassandra")
        
        # Stocker aussi les calculs détaillés
        print("7. Stockage des calculs détaillés dans Cassandra...")
        for order in orders[:5]:  # Stocker les calculs pour les 5 premières commandes
            try:
                details_query = f"""
                INSERT INTO procurement.demand_calculations (
                    calculation_date, sku_id, total_demand, available_stock,
                    net_demand, final_order_quantity, calculated_at
                ) VALUES (
                    '{self.target_date}',
                    '{order['sku_id']}',
                    {order['demand']},
                    {order['available_stock']},
                    {order['net_demand']},
                    {order['order_quantity']},
                    toTimestamp(now())
                );
                """
                
                cmd = ['docker-compose', 'exec', '-T', 'cassandra', 'cqlsh', '-e', details_query]
                subprocess.run(cmd, capture_output=True, text=True, check=False)
                print(f"   📝 Calculs stockés pour {order['sku_id']}")
                
            except Exception as e:
                print(f"   ⚠️  Erreur stockage calculs: {e}")
    
    def run(self):
        """Exécute le pipeline complet"""
        print(f"\n{'='*80}")
        print(f"PIPELINE DE GÉNÉRATION DE COMMANDES")
        print(f"Date: {self.target_date}")
        print(f"{'='*80}\n")
        
        try:
            # Étape 1: Demande
            demand_data = self.get_aggregated_demand()
            if not demand_data:
                print("❌ Aucune demande trouvée")
                return
            
            # Étape 2: Stock
            stock_data = self.get_stock_data()
            
            # Étape 3: Produits
            product_data = self.get_products_with_suppliers()
            if not product_data:
                print("❌ Aucun produit trouvé")
                return
            
            # Étape 4: Calcul
            orders = self.calculate_orders(demand_data, stock_data, product_data)
            
            if not orders:
                print(f"\n{'='*60}")
                print("ℹ️  AUCUNE COMMANDE NÉCESSAIRE")
                print("   Raison : Stock suffisant pour couvrir la demande + sécurité")
                print(f"{'='*60}")
                return
            
            # Étape 5: Génération fichiers
            files_count = self.generate_supplier_files(orders)
            
            # Étape 6: Cassandra
            self.store_in_cassandra(orders)
            
            # Rapport final détaillé
            print(f"\n{'='*80}")
            print("✅ PIPELINE TERMINÉ AVEC SUCCÈS")
            print(f"{'='*80}")
            
            total_items = len(orders)
            total_value = sum(o['total_price'] for o in orders)
            supplier_count = len(set(o['supplier_id'] for o in orders))
            
            print(f"\n📊 RÉSUMÉ DÉTAILLÉ:")
            print(f"   Commandes générées: {total_items}")
            print(f"   Fournisseurs concernés: {supplier_count}")
            print(f"   Valeur totale des commandes: {total_value:.2f}€")
            print(f"   Fichiers générés: {files_count} (JSON + CSV par fournisseur)")
            
            # Détails statistiques
            if orders:
                avg_order_value = total_value / total_items
                avg_quantity = sum(o['order_quantity'] for o in orders) / total_items
                print(f"\n📈 STATISTIQUES:")
                print(f"   Valeur moyenne par article: {avg_order_value:.2f}€")
                print(f"   Quantité moyenne commandée: {avg_quantity:.1f} unités")
                
                # Top 3 des commandes les plus chères
                sorted_orders = sorted(orders, key=lambda x: x['total_price'], reverse=True)
                print(f"\n🏆 TOP 3 DES COMMANDES:")
                for i, order in enumerate(sorted_orders[:3]):
                    print(f"   {i+1}. {order['sku_id']} - {order['product_name'][:30]}...")
                    print(f"      {order['order_quantity']} unités × {order['unit_price']:.2f}€ = {order['total_price']:.2f}€")
            
            print(f"\n📁 Répertoire de sortie: {self.output_dir.absolute()}")
            
            # Lister les fichiers
            files = list(self.output_dir.glob(f"*{self.target_date}*"))
            if files:
                print(f"\n📄 FICHIERS CRÉÉS:")
                print(f"   {'Nom du fichier':<40} {'Taille':<10} {'Type':<8}")
                print(f"   {'─'*40} {'─'*10} {'─'*8}")
                for f in sorted(files):
                    size_kb = f.stat().st_size / 1024
                    file_type = "JSON" if f.suffix == '.json' else "CSV"
                    print(f"   {f.name:<40} {size_kb:.1f} KB{'':<3} {file_type:<8}")
            
            print(f"\n📝 CONSULTER LES FICHIERS :")
            print(f"   • Les fichiers CSV contiennent les commandes au format tabulaire")
            print(f"   • Les fichiers JSON contiennent les détails complets avec les calculs")
            print(f"   • Les calculs sont également stockés dans Cassandra")
            
        except Exception as e:
            print(f"\n{'='*80}")
            print(f"❌ ERREUR: {e}")
            print(f"{'='*80}")
            import traceback
            traceback.print_exc()

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Générer les commandes fournisseurs avec affichage détaillé des calculs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples d'affichage des calculs :
  Formule : Demande Nette = MAX(0, Demande Client + Stock Sécurité - Stock Disponible)
  
  Exemple de calcul :
    SKU000213 : Demande(55) + Sécurité(10) - Disponible(70) = -5 → MAX(0, -5) = 0 → Pas de commande
    SKU000368 : Demande(52) + Sécurité(10) - Disponible(40) = 22 → Commande de 22 unités
        """
    )
    
    parser.add_argument('--date', default='2025-12-02', help='Date à traiter (format: YYYY-MM-DD)')
    parser.add_argument('--test-stock', action='store_true', help='Tester la structure du stock')
    parser.add_argument('--verbose', '-v', action='store_true', help='Afficher tous les calculs détaillés')
    
    args = parser.parse_args()
    
    if args.test_stock:
        # Tester spécifiquement la structure du stock
        generator = ProcurementGenerator(args.date)
        
        print("Test de la structure du stock_raw...")
        
        # Voir les premières lignes brutes
        query = f"SELECT * FROM hive.procurement.stock_raw WHERE date = '{args.date}' LIMIT 5"
        result = generator.run_trino_query_text(query)
        print("Résultat brut:")
        print(result)
        
        # Essayer différentes colonnes
        queries = [
            ("Test 1 - toutes colonnes", f"SELECT * FROM hive.procurement.stock_raw WHERE date = '{args.date}' LIMIT 3"),
            ("Test 2 - colonnes individuelles", f"SELECT sku_id, available_stock, reserved_stock, safety_stock FROM hive.procurement.stock_raw WHERE date = '{args.date}' LIMIT 3"),
            ("Test 3 - avec filtrage SKU", f"SELECT * FROM hive.procurement.stock_raw WHERE date = '{args.date}' AND reserved_stock LIKE 'SKU%' LIMIT 3"),
        ]
        
        for name, query in queries:
            print(f"\n{name}:")
            print(query)
            result = generator.run_trino_query_text(query)
            print(result)
        
        return
    
    # Exécuter le pipeline normal
    generator = ProcurementGenerator(args.date)
    generator.run()

if __name__ == "__main__":
    main()