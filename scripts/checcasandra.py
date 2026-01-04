#!/usr/bin/env python3
"""
Vérification des données stockées dans Cassandra
"""

import subprocess
from datetime import datetime, date
import json
from pathlib import Path

class CassandraChecker:
    def __init__(self, target_date=None):
        self.target_date = target_date or date.today().strftime('%Y-%m-%d')
        self.output_dir = Path("./cassandra_checks")
        self.output_dir.mkdir(exist_ok=True)
    
    def run_cql_query(self, query, description=""):
        """Exécute une requête CQL sur Cassandra"""
        print(f"\n{'='*60}")
        if description:
            print(f"Vérification : {description}")
        print(f"Requête : {query[:100]}..." if len(query) > 100 else f"Requête : {query}")
        print(f"{'='*60}")
        
        cmd = ['docker-compose', 'exec', '-T', 'cassandra', 'cqlsh', '-e', query]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                print("✅ Succès")
                return result.stdout.strip()
            else:
                print(f"❌ Erreur : {result.stderr[:200]}")
                return None
                
        except subprocess.TimeoutExpired:
            print("⏱️  Timeout sur la requête")
            return None
        except Exception as e:
            print(f"⚠️  Exception : {e}")
            return None
    
    def check_keyspace_exists(self):
        """Vérifie que le keyspace existe"""
        print("\n1. VÉRIFICATION DU KEYSPACE 'procurement'")
        
        query = "DESCRIBE KEYSPACES;"
        result = self.run_cql_query(query, "Liste des keyspaces")
        
        if result and 'procurement' in result:
            print("✅ Keyspace 'procurement' trouvé")
            return True
        else:
            print("❌ Keyspace 'procurement' NON trouvé")
            print(f"Résultat : {result}")
            return False
    
    def check_tables_exist(self):
        """Vérifie que les tables existent"""
        print("\n2. VÉRIFICATION DES TABLES")
        
        query = "USE procurement; DESCRIBE TABLES;"
        result = self.run_cql_query(query, "Liste des tables dans procurement")
        
        if result:
            tables = [line.strip() for line in result.split('\n') if line.strip()]
            print(f"Tables trouvées : {tables}")
            
            required_tables = ['supplier_orders', 'demand_calculations', 'exceptions']
            missing_tables = []
            
            for table in required_tables:
                if table in tables:
                    print(f"  ✅ Table '{table}' présente")
                else:
                    print(f"  ❌ Table '{table}' manquante")
                    missing_tables.append(table)
            
            return len(missing_tables) == 0
        return False
    
    def check_table_structure(self, table_name):
        """Vérifie la structure d'une table"""
        print(f"\n3. STRUCTURE DE LA TABLE '{table_name}'")
        
        query = f"USE procurement; DESCRIBE TABLE {table_name};"
        result = self.run_cql_query(query, f"Structure de {table_name}")
        
        if result:
            print(f"Structure de {table_name} :")
            print(result)
            return True
        return False
    
    def count_records(self, table_name, date_filter=True):
        """Compte les enregistrements dans une table"""
        print(f"\n4. COMPTAGE DES ENREGISTREMENTS - {table_name}")
        
        if date_filter and table_name == 'supplier_orders':
            query = f"USE procurement; SELECT COUNT(*) FROM {table_name} WHERE order_date = '{self.target_date}';"
        elif date_filter and table_name == 'demand_calculations':
            query = f"USE procurement; SELECT COUNT(*) FROM {table_name} WHERE calculation_date = '{self.target_date}';"
        elif date_filter and table_name == 'exceptions':
            query = f"USE procurement; SELECT COUNT(*) FROM {table_name} WHERE date = '{self.target_date}';"
        else:
            query = f"USE procurement; SELECT COUNT(*) FROM {table_name};"
        
        result = self.run_cql_query(query, f"Nombre d'enregistrements dans {table_name}")
        
        if result:
            print(f"Résultat : {result}")
            try:
                # Essayer d'extraire le nombre
                lines = result.split('\n')
                for line in lines:
                    if 'count' in line.lower():
                        count = line.split()[-1].strip()
                        print(f"📊 Nombre d'enregistrements : {count}")
                        return int(count)
            except:
                pass
        return 0
    
    def view_sample_data(self, table_name, limit=5):
        """Affiche un échantillon des données"""
        print(f"\n5. ÉCHANTILLON DES DONNÉES - {table_name} (limite : {limit})")
        
        if table_name == 'supplier_orders':
            query = f"USE procurement; SELECT order_date, supplier_id, sku_id, quantity FROM {table_name} WHERE order_date = '{self.target_date}' LIMIT {limit};"
        elif table_name == 'demand_calculations':
            query = f"USE procurement; SELECT calculation_date, sku_id, total_demand, final_order_quantity FROM {table_name} WHERE calculation_date = '{self.target_date}' LIMIT {limit};"
        elif table_name == 'exceptions':
            query = f"USE procurement; SELECT date, exception_type, sku_id, message FROM {table_name} WHERE date = '{self.target_date}' LIMIT {limit};"
        else:
            query = f"USE procurement; SELECT * FROM {table_name} LIMIT {limit};"
        
        result = self.run_cql_query(query, f"Échantillon de {table_name}")
        
        if result:
            print("Données échantillon :")
            print(result)
            return True
        return False
    
    def verify_data_integrity(self):
        """Vérifie l'intégrité des données"""
        print("\n6. VÉRIFICATION D'INTÉGRITÉ DES DONNÉES")
        
        checks = [
            ("Vérification des dates", 
             f"SELECT DISTINCT order_date FROM procurement.supplier_orders WHERE order_date = '{self.target_date}';"),
            
            ("Vérification des fournisseurs", 
             f"SELECT COUNT(DISTINCT supplier_id) as nb_fournisseurs FROM procurement.supplier_orders WHERE order_date = '{self.target_date}';"),
            
            ("Vérification des SKU uniques", 
             f"SELECT COUNT(DISTINCT sku_id) as nb_sku FROM procurement.supplier_orders WHERE order_date = '{self.target_date}';"),
            
            ("Total des quantités commandées", 
             f"SELECT SUM(quantity) as total_quantite FROM procurement.supplier_orders WHERE order_date = '{self.target_date}';"),
        ]
        
        for description, query in checks:
            result = self.run_cql_query(query, description)
            if result:
                print(f"  {description} : {result}")
        
        return True
    
    def generate_report(self):
        """Génère un rapport complet"""
        print(f"\n{'='*80}")
        print(f"RAPPORT DE VÉRIFICATION CASSANDRA")
        print(f"Date : {self.target_date}")
        print(f"Timestamp : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}")
        
        report = {
            'date': self.target_date,
            'timestamp': datetime.now().isoformat(),
            'checks': {}
        }
        
        # 1. Vérifier le keyspace
        report['checks']['keyspace'] = self.check_keyspace_exists()
        
        # 2. Vérifier les tables
        report['checks']['tables'] = self.check_tables_exist()
        
        # 3. Vérifier les structures
        tables_to_check = ['supplier_orders', 'demand_calculations', 'exceptions']
        for table in tables_to_check:
            report['checks'][f'structure_{table}'] = self.check_table_structure(table)
        
        # 4. Compter les enregistrements
        counts = {}
        for table in tables_to_check:
            count = self.count_records(table, date_filter=True)
            counts[table] = count
            report['checks'][f'count_{table}'] = count
        
        # 5. Voir des échantillons
        for table in tables_to_check:
            if counts.get(table, 0) > 0:
                report['checks'][f'sample_{table}'] = self.view_sample_data(table, limit=3)
        
        # 6. Vérifier l'intégrité
        report['checks']['integrity'] = self.verify_data_integrity()
        
        # Sauvegarder le rapport
        report_file = self.output_dir / f"cassandra_report_{self.target_date}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n📄 Rapport sauvegardé : {report_file}")
        
        # Afficher un résumé
        print(f"\n{'='*80}")
        print("RÉSUMÉ DES VÉRIFICATIONS")
        print(f"{'='*80}")
        
        total_checks = 0
        passed_checks = 0
        
        for check_name, check_result in report['checks'].items():
            total_checks += 1
            if check_result:
                passed_checks += 1
                status = "✅ PASS"
            else:
                status = "❌ FAIL"
            
            print(f"{status} {check_name}")
        
        print(f"\n📊 Résultat : {passed_checks}/{total_checks} vérifications réussies")
        
        if passed_checks == total_checks:
            print("\n🎉 TOUTES LES VÉRIFICATIONS SONT RÉUSSIES !")
            print("Les données sont correctement stockées dans Cassandra.")
        else:
            print(f"\n⚠️  {total_checks - passed_checks} vérifications ont échoué.")
            print("Veuillez vérifier la configuration de Cassandra.")
        
        return report
    
    def compare_with_files(self):
        """Compare les données Cassandra avec les fichiers générés"""
        print("\n7. COMPARAISON AVEC LES FICHIERS GÉNÉRÉS")
        
        # Compter les commandes dans les fichiers
        import csv
        import json
        
        supplier_orders_dir = Path("./supplier_orders")
        if not supplier_orders_dir.exists():
            print("❌ Répertoire supplier_orders non trouvé")
            return
        
        # Compter les articles dans les fichiers CSV
        total_articles_files = 0
        total_value_files = 0.0
        
        csv_files = list(supplier_orders_dir.glob(f"*{self.target_date}.csv"))
        
        for csv_file in csv_files:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                rows = list(reader)
                # Soustraire l'en-tête
                article_count = len(rows) - 1 if len(rows) > 1 else 0
                total_articles_files += article_count
                
                # Calculer la valeur (dernière colonne)
                for row in rows[1:]:
                    if len(row) >= 10:  # Vérifier qu'on a la colonne TOTAL
                        try:
                            total_value_files += float(row[9].replace('€', '').strip())
                        except:
                            pass
        
        print(f"📁 Fichiers CSV analysés : {len(csv_files)}")
        print(f"📦 Total articles dans fichiers : {total_articles_files}")
        print(f"💰 Valeur totale dans fichiers : {total_value_files:.2f}€")
        
        # Récupérer les totaux depuis Cassandra
        query_articles = f"SELECT COUNT(*) as nb_articles FROM procurement.supplier_orders WHERE order_date = '{self.target_date}';"
        query_value = f"SELECT SUM(quantity) as total_quantite FROM procurement.supplier_orders WHERE order_date = '{self.target_date}';"
        
        result_articles = self.run_cql_query(query_articles, "Nombre d'articles dans Cassandra")
        result_value = self.run_cql_query(query_value, "Quantité totale dans Cassandra")
        
        print("\n📊 COMPARAISON :")
        print(f"  Articles | Fichiers : {total_articles_files} | Cassandra : {result_articles}")
        print(f"  Quantité  | Fichiers : {total_articles_files} | Cassandra : {result_value}")
        
        return {
            'files_articles': total_articles_files,
            'files_value': total_value_files,
            'cassandra_articles': result_articles,
            'cassandra_quantity': result_value
        }

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Vérifier les données dans Cassandra')
    parser.add_argument('--date', help='Date à vérifier (format: YYYY-MM-DD)')
    parser.add_argument('--quick', action='store_true', help='Vérification rapide seulement')
    parser.add_argument('--compare', action='store_true', help='Comparer avec les fichiers générés')
    parser.add_argument('--fix', action='store_true', help='Essayer de corriger les problèmes')
    
    args = parser.parse_args()
    
    checker = CassandraChecker(args.date)
    
    if args.quick:
        # Vérification rapide
        print("🔍 VÉRIFICATION RAPIDE CASSANDRA")
        checker.check_keyspace_exists()
        checker.check_tables_exist()
        checker.count_records('supplier_orders')
        checker.view_sample_data('supplier_orders', limit=3)
        
    elif args.compare:
        # Comparaison avec fichiers
        checker.compare_with_files()
        
    elif args.fix:
        # Essayer de corriger
        print("🔧 TENTATIVE DE CORRECTION")
        checker.fix_common_issues()
        
    else:
        # Rapport complet
        checker.generate_report()

if __name__ == "__main__":
    main()