#!/usr/bin/env python3
"""
Pipeline de génération de commandes - Version debug
"""

import subprocess
import json
import csv
import uuid
from datetime import datetime, date, timedelta
import os
from pathlib import Path

class ProcurementDebugger:
    def __init__(self, target_date='2025-12-02'):
        self.target_date = target_date
        self.output_dir = Path("./debug_output")
        self.output_dir.mkdir(exist_ok=True)
        
    def run_trino_query_debug(self, query, name="Query"):
        """Exécute une requête avec debug"""
        print(f"\n{'='*60}")
        print(f"DEBUG: {name}")
        print(f"Query: {query[:100]}...")
        print(f"{'='*60}")
        
        cmd = ['docker-compose', 'exec', '-T', 'trino', 'trino', '--output-format', 'JSON', '--execute', query]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            print(f"Return code: {result.returncode}")
            print(f"Stdout length: {len(result.stdout)}")
            
            if result.stdout.strip():
                print("\nRaw output (first 500 chars):")
                print(result.stdout[:500])
                
                try:
                    data = json.loads(result.stdout)
                    print(f"\nParsed JSON type: {type(data)}")
                    
                    if isinstance(data, list):
                        print(f"List length: {len(data)}")
                        if len(data) > 0:
                            print(f"First item type: {type(data[0])}")
                            print(f"First item keys: {list(data[0].keys()) if isinstance(data[0], dict) else 'Not a dict'}")
                            
                            # Afficher quelques exemples
                            for i, item in enumerate(data[:3]):
                                print(f"\nItem {i}:")
                                for key, value in item.items():
                                    print(f"  {key}: {value} (type: {type(value).__name__})")
                    elif isinstance(data, dict):
                        print("Dictionary keys:", list(data.keys()))
                    else:
                        print(f"Unexpected type: {type(data)}")
                    
                    return data
                except json.JSONDecodeError as e:
                    print(f"JSON decode error: {e}")
                    print("Trying to parse as text...")
                    
                    lines = result.stdout.strip().split('\n')
                    if len(lines) > 0:
                        print(f"Lines: {len(lines)}")
                        print("First line:", lines[0])
                        if len(lines) > 1:
                            print("Second line:", lines[1])
                    
                    return result.stdout
            else:
                print("Empty stdout")
                return []
                
        except subprocess.TimeoutExpired:
            print("Query timeout!")
            return []
        except Exception as e:
            print(f"Error: {e}")
            return []
    
    def debug_demand_calculation(self):
        """Debug du calcul de demande"""
        print(f"\n{'='*60}")
        print(f"DEBUG: CALCUL DE DEMANDE POUR {self.target_date}")
        print(f"{'='*60}")
        
        # 1. Vérifier les commandes
        query = f"""
        SELECT 
            sku_id,
            quantity,
            store_id,
            order_id
        FROM hive.procurement.orders_raw 
        WHERE date = '{self.target_date}'
        LIMIT 10
        """
        
        orders = self.run_trino_query_debug(query, "Sample Orders")
        
        # 2. Vérifier le stock
        query = f"""
        SELECT 
            sku_id,
            available_stock,
            reserved_stock,
            safety_stock
        FROM hive.procurement.stock_raw 
        WHERE date = '{self.target_date}'
        LIMIT 10
        """
        
        stock = self.run_trino_query_debug(query, "Sample Stock")
        
        # 3. Vérifier les produits
        query = """
        SELECT 
            p.sku_id,
            p.product_name,
            p.unit_price,
            p.pack_size,
            p.min_order_quantity,
            ps.supplier_id,
            ps.lead_time_days,
            s.supplier_name
        FROM postgresql.public.products p
        LEFT JOIN postgresql.public.product_supplier ps ON p.sku_id = ps.sku_id
        LEFT JOIN postgresql.public.suppliers s ON ps.supplier_id = s.supplier_id
        WHERE ps.is_primary = true OR ps.is_primary IS NULL
        LIMIT 10
        """
        
        products = self.run_trino_query_debug(query, "Sample Products")
        
        # 4. Tester une requête d'agrégation simple
        query = f"""
        SELECT 
            sku_id,
            SUM(CAST(quantity AS INTEGER)) as total_quantity,
            COUNT(*) as order_count
        FROM hive.procurement.orders_raw 
        WHERE date = '{self.target_date}'
        AND sku_id IS NOT NULL
        GROUP BY sku_id
        ORDER BY total_quantity DESC
        LIMIT 5
        """
        
        aggregation = self.run_trino_query_debug(query, "Demand Aggregation")
        
        return orders, stock, products, aggregation
    
    def test_specific_sku(self, sku_id):
        """Tester un SKU spécifique"""
        print(f"\n{'='*60}")
        print(f"TEST SKU SPÉCIFIQUE: {sku_id}")
        print(f"{'='*60}")
        
        # 1. Commandes pour ce SKU
        query = f"""
        SELECT 
            SUM(CAST(quantity AS INTEGER)) as total_demand,
            COUNT(DISTINCT store_id) as store_count
        FROM hive.procurement.orders_raw 
        WHERE date = '{self.target_date}'
        AND sku_id = '{sku_id}'
        """
        
        demand = self.run_trino_query_debug(query, f"Demand for {sku_id}")
        
        # 2. Stock pour ce SKU
        query = f"""
        SELECT 
            available_stock,
            reserved_stock,
            safety_stock
        FROM hive.procurement.stock_raw 
        WHERE date = '{self.target_date}'
        AND sku_id = '{sku_id}'
        LIMIT 1
        """
        
        stock = self.run_trino_query_debug(query, f"Stock for {sku_id}")
        
        # 3. Produit info
        query = f"""
        SELECT 
            p.sku_id,
            p.product_name,
            p.unit_price,
            p.pack_size,
            p.min_order_quantity,
            ps.supplier_id,
            s.supplier_name
        FROM postgresql.public.products p
        LEFT JOIN postgresql.public.product_supplier ps ON p.sku_id = ps.sku_id
        LEFT JOIN postgresql.public.suppliers s ON ps.supplier_id = s.supplier_id
        WHERE p.sku_id = '{sku_id}'
        LIMIT 1
        """
        
        product = self.run_trino_query_debug(query, f"Product info for {sku_id}")
        
        return demand, stock, product
    
    def run_full_debug(self):
        """Exécute un debug complet"""
        print(f"\n{'='*60}")
        print(f"DEBUG COMPLET DU PIPELINE")
        print(f"Date: {self.target_date}")
        print(f"{'='*60}")
        
        # Étape 1: Debug des données de base
        orders, stock, products, aggregation = self.debug_demand_calculation()
        
        # Étape 2: Trouver un SKU avec des données
        print(f"\n{'='*60}")
        print("RECHERCHE D'UN SKU AVEC DES DONNÉES")
        print(f"{'='*60}")
        
        # Essayer de trouver un SKU commun
        query = f"""
        SELECT 
            o.sku_id,
            SUM(CAST(o.quantity AS INTEGER)) as total_demand,
            COUNT(*) as order_count
        FROM hive.procurement.orders_raw o
        WHERE o.date = '{self.target_date}'
        AND o.sku_id IS NOT NULL
        GROUP BY o.sku_id
        ORDER BY total_demand DESC
        LIMIT 1
        """
        
        top_sku_result = self.run_trino_query_debug(query, "Top SKU")
        
        if top_sku_result and isinstance(top_sku_result, list) and len(top_sku_result) > 0:
            top_sku = top_sku_result[0]
            sku_id = top_sku.get('sku_id')
            
            if sku_id:
                print(f"\nSKU trouvé avec demande: {sku_id}")
                self.test_specific_sku(sku_id)
            else:
                print("Aucun SKU valide trouvé")
        else:
            print("Aucun résultat pour la requête Top SKU")
        
        # Étape 3: Vérifier la structure des tables
        print(f"\n{'='*60}")
        print("STRUCTURE DES TABLES")
        print(f"{'='*60}")
        
        tables = ['orders_raw', 'stock_raw', 'products', 'suppliers', 'product_supplier']
        
        for table in tables:
            if table in ['orders_raw', 'stock_raw']:
                schema = 'hive.procurement'
            else:
                schema = 'postgresql.public'
                
            query = f"DESCRIBE {schema}.{table}"
            self.run_trino_query_debug(query, f"Structure de {table}")
        
        # Étape 4: Générer un rapport de debug
        self.generate_debug_report()

    def generate_debug_report(self):
        """Génère un rapport de debug"""
        report = {
            'debug_date': datetime.now().isoformat(),
            'target_date': self.target_date,
            'checks': []
        }
        
        # Vérifications simples
        checks = [
            ("orders_raw count", f"SELECT COUNT(*) FROM hive.procurement.orders_raw WHERE date = '{self.target_date}'"),
            ("stock_raw count", f"SELECT COUNT(*) FROM hive.procurement.stock_raw WHERE date = '{self.target_date}'"),
            ("products count", "SELECT COUNT(*) FROM postgresql.public.products"),
            ("suppliers count", "SELECT COUNT(*) FROM postgresql.public.suppliers"),
            ("product_supplier count", "SELECT COUNT(*) FROM postgresql.public.product_supplier WHERE is_primary = true"),
        ]
        
        for name, query in checks:
            try:
                cmd = ['docker-compose', 'exec', '-T', 'trino', 'trino', '--execute', query]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
                report['checks'].append({
                    'name': name,
                    'query': query,
                    'result': result.stdout.strip() if result.returncode == 0 else f"ERROR: {result.stderr}"
                })
            except Exception as e:
                report['checks'].append({
                    'name': name,
                    'query': query,
                    'result': f"EXCEPTION: {str(e)}"
                })
        
        # Sauvegarder le rapport
        report_file = self.output_dir / f"debug_report_{self.target_date}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n📄 Rapport de debug sauvegardé: {report_file}")
        
        # Afficher un résumé
        print(f"\n{'='*60}")
        print("RÉSUMÉ DU DEBUG")
        print(f"{'='*60}")
        
        for check in report['checks']:
            print(f"{check['name']}: {check['result']}")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Debug du pipeline procurement')
    parser.add_argument('--date', default='2025-12-02', help='Date à debugger')
    parser.add_argument('--sku', help='SKU spécifique à tester')
    
    args = parser.parse_args()
    
    debugger = ProcurementDebugger(args.date)
    
    if args.sku:
        debugger.test_specific_sku(args.sku)
    else:
        debugger.run_full_debug()

if __name__ == "__main__":
    main()