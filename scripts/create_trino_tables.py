import subprocess
import time

def run_trino_command(sql_command):
    """Exécute une commande SQL dans Trino"""
    print(f"\n>>> {sql_command[:80]}..." if len(sql_command) > 80 else f"\n>>> {sql_command}")
    
    cmd = f'docker-compose exec trino trino --catalog hive --execute "{sql_command}"'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        error_msg = result.stderr.split('\n')[-2] if '\n' in result.stderr else result.stderr[:200]
        print(f"   ❌ Erreur: {error_msg}")
        return False, result.stderr
    else:
        if result.stdout.strip():
            print(f"   ✅ Succès:\n{result.stdout.strip()}")
        else:
            print(f"   ✅ Succès")
        return True, result.stdout

def setup_hive_schema():
    """Configure le schéma Hive"""
    print("="*70)
    print("CONFIGURATION DU SCHÉMA HIVE")
    print("="*70)
    
    # 1. Créer le schéma procurement
    success, _ = run_trino_command("CREATE SCHEMA IF NOT EXISTS hive.procurement")
    
    if not success:
        # Essayer avec default
        print("\nEssai avec le schéma default...")
        run_trino_command("CREATE SCHEMA IF NOT EXISTS hive.default")
        return "default"
    
    return "procurement"

def create_external_tables(schema_name):
    """Crée les tables externes"""
    print(f"\n" + "="*70)
    print(f"CRÉATION DES TABLES DANS LE SCHÉMA {schema_name}")
    print("="*70)
    
    # 1. Table des produits
    print("\n1. Table 'products'...")
    create_products = f"""
    CREATE TABLE IF NOT EXISTS hive.{schema_name}.products (
        sku_id VARCHAR,
        product_name VARCHAR,
        category VARCHAR,
        unit_price DOUBLE,
        pack_size INTEGER,
        min_order_quantity INTEGER
    )
    WITH (
        format = 'CSV',
        skip_header_line_count = 1,
        external_location = 'hdfs://namenode:9000/raw/master/products.csv'
    )
    """
    run_trino_command(create_products)
    
    # 2. Table des fournisseurs
    print("\n2. Table 'suppliers'...")
    create_suppliers = f"""
    CREATE TABLE IF NOT EXISTS hive.{schema_name}.suppliers (
        supplier_id VARCHAR,
        supplier_name VARCHAR
    )
    WITH (
        format = 'CSV',
        skip_header_line_count = 1,
        external_location = 'hdfs://namenode:9000/raw/master/suppliers.csv'
    )
    """
    run_trino_command(create_suppliers)
    
    # 3. Table product_supplier
    print("\n3. Table 'product_supplier'...")
    create_product_supplier = f"""
    CREATE TABLE IF NOT EXISTS hive.{schema_name}.product_supplier (
        sku_id VARCHAR,
        supplier_id VARCHAR,
        lead_time_days INTEGER,
        is_primary BOOLEAN
    )
    WITH (
        format = 'CSV',
        skip_header_line_count = 1,
        external_location = 'hdfs://namenode:9000/raw/master/product_supplier.csv'
    )
    """
    run_trino_command(create_product_supplier)
    
    # 4. Table safety_stock
    print("\n4. Table 'safety_stock'...")
    create_safety_stock = f"""
    CREATE TABLE IF NOT EXISTS hive.{schema_name}.safety_stock (
        sku_id VARCHAR,
        warehouse_id VARCHAR,
        safety_stock_level INTEGER
    )
    WITH (
        format = 'CSV',
        skip_header_line_count = 1,
        external_location = 'hdfs://namenode:9000/raw/master/safety_stock.csv'
    )
    """
    run_trino_command(create_safety_stock)
    
    # 5. Table des commandes (JSON - nécessite un traitement spécial)
    print("\n5. Table 'orders_raw' (JSON brut)...")
    create_orders_raw = f"""
    CREATE TABLE IF NOT EXISTS hive.{schema_name}.orders_raw (
        order_id VARCHAR,
        sku_id VARCHAR,
        quantity INTEGER,
        order_timestamp VARCHAR,
        date VARCHAR,
        store_id VARCHAR
    )
    WITH (
        format = 'JSON',
        external_location = 'hdfs://namenode:9000/raw/orders/'
    )
    """
    run_trino_command(create_orders_raw)
    
    # 6. Table du stock (CSV brut)
    print("\n6. Table 'stock_raw'...")
    create_stock_raw = f"""
    CREATE TABLE IF NOT EXISTS hive.{schema_name}.stock_raw (
        snapshot_date VARCHAR,
        warehouse_id VARCHAR,
        sku_id VARCHAR,
        available_stock INTEGER,
        reserved_stock INTEGER
    )
    WITH (
        format = 'CSV',
        skip_header_line_count = 1,
        external_location = 'hdfs://namenode:9000/raw/stock/'
    )
    """
    run_trino_command(create_stock_raw)
    
    return schema_name

def test_tables(schema_name):
    """Teste les tables créées"""
    print(f"\n" + "="*70)
    print(f"TESTS DES TABLES DANS {schema_name}")
    print("="*70)
    
    # 1. Lister toutes les tables
    print("\n1. Liste des tables créées:")
    run_trino_command(f"SHOW TABLES FROM hive.{schema_name}")
    
    # 2. Tester chaque table
    test_queries = [
        ("products", f"SELECT COUNT(*) as nb_produits FROM hive.{schema_name}.products"),
        ("suppliers", f"SELECT COUNT(*) as nb_fournisseurs FROM hive.{schema_name}.suppliers"),
        ("product_supplier", f"SELECT COUNT(*) as nb_liens FROM hive.{schema_name}.product_supplier"),
        ("safety_stock", f"SELECT COUNT(*) as nb_stock_securite FROM hive.{schema_name}.safety_stock"),
        ("orders_raw", f"SELECT COUNT(*) as nb_commandes FROM hive.{schema_name}.orders_raw WHERE date = '2025-12-02' AND store_id = 'ST0000'"),
        ("stock_raw", f"SELECT COUNT(*) as nb_stock FROM hive.{schema_name}.stock_raw WHERE snapshot_date = '2025-12-02'")
    ]
    
    for table_name, query in test_queries:
        print(f"\n2. Test table '{table_name}':")
        run_trino_command(query)
    
    # 3. Aperçu des données
    print("\n3. Aperçu des données:")
    run_trino_command(f"SELECT * FROM hive.{schema_name}.products LIMIT 3")
    run_trino_command(f"SELECT * FROM hive.{schema_name}.orders_raw WHERE date = '2025-12-02' AND store_id = 'ST0000' LIMIT 3")

def create_analysis_views(schema_name):
    """Crée des vues pour l'analyse"""
    print(f"\n" + "="*70)
    print(f"CRÉATION DES VUES D'ANALYSE")
    print("="*70)
    
    # 1. Vue pour les commandes nettoyées
    print("\n1. Vue 'orders_clean'...")
    view_orders = f"""
    CREATE OR REPLACE VIEW hive.{schema_name}.orders_clean AS
    SELECT 
        order_id,
        store_id,
        sku_id,
        quantity,
        CAST(order_timestamp AS TIMESTAMP) as order_timestamp,
        date as order_date
    FROM hive.{schema_name}.orders_raw
    WHERE date IS NOT NULL 
      AND store_id IS NOT NULL 
      AND sku_id IS NOT NULL
    """
    run_trino_command(view_orders)
    
    # 2. Vue pour le stock nettoyé
    print("\n2. Vue 'stock_clean'...")
    view_stock = f"""
    CREATE OR REPLACE VIEW hive.{schema_name}.stock_clean AS
    SELECT 
        warehouse_id,
        sku_id,
        available_stock,
        reserved_stock,
        CAST(snapshot_date AS DATE) as stock_date
    FROM hive.{schema_name}.stock_raw
    WHERE snapshot_date IS NOT NULL 
      AND sku_id IS NOT NULL
    """
    run_trino_command(view_stock)
    
    # 3. Test des vues
    print("\n3. Test des vues...")
    run_trino_command(f"SELECT COUNT(*) as nb FROM hive.{schema_name}.orders_clean")
    run_trino_command(f"SELECT COUNT(*) as nb FROM hive.{schema_name}.stock_clean")

def demonstrate_queries(schema_name):
    """Montre des exemples de requêtes utiles"""
    print(f"\n" + "="*70)
    print(f"EXEMPLES DE REQUÊTES UTILES")
    print("="*70)
    
    queries = [
        ("Top 10 produits par quantité vendue", f"""
        SELECT 
            p.sku_id,
            p.product_name,
            SUM(o.quantity) as total_vendu
        FROM hive.{schema_name}.orders_clean o
        JOIN hive.{schema_name}.products p ON o.sku_id = p.sku_id
        WHERE o.order_date = DATE '2025-12-02'
        GROUP BY p.sku_id, p.product_name
        ORDER BY total_vendu DESC
        LIMIT 10
        """),
        
        ("Stock par catégorie", f"""
        SELECT 
            p.category,
            SUM(s.available_stock) as stock_disponible,
            SUM(s.reserved_stock) as stock_reserve
        FROM hive.{schema_name}.stock_clean s
        JOIN hive.{schema_name}.products p ON s.sku_id = p.sku_id
        WHERE s.stock_date = DATE '2025-12-02'
        GROUP BY p.category
        ORDER BY stock_disponible DESC
        """),
        
        ("Fournisseurs principaux par produit", f"""
        SELECT 
            p.sku_id,
            p.product_name,
            s.supplier_name,
            ps.lead_time_days
        FROM hive.{schema_name}.products p
        JOIN hive.{schema_name}.product_supplier ps ON p.sku_id = ps.sku_id
        JOIN hive.{schema_name}.suppliers s ON ps.supplier_id = s.supplier_id
        WHERE ps.is_primary = true
        LIMIT 10
        """)
    ]
    
    for title, query in queries:
        print(f"\n{title}:")
        run_trino_command(query)

def main():
    """Fonction principale"""
    print("="*70)
    print("CONFIGURATION COMPLÈTE TRINO/HIVE")
    print("="*70)
    
    print("\n⚠️  Vérification de la connexion HDFS...")
    # Test HDFS
    hdfs_test = subprocess.run(
        "docker-compose exec namenode hdfs dfs -test -e /raw/master/products.csv",
        shell=True,
        capture_output=True,
        text=True
    )
    
    if hdfs_test.returncode != 0:
        print("❌ ERREUR: Fichiers HDFS non accessibles!")
        print("   Vérifiez que les données sont bien dans HDFS")
        return
    
    print("✅ HDFS accessible")
    
    # Configurer le schéma
    schema_name = setup_hive_schema()
    
    # Créer les tables
    create_external_tables(schema_name)
    
    # Tester
    test_tables(schema_name)
    
    # Créer les vues
    create_analysis_views(schema_name)
    
    # Montrer des exemples
    demonstrate_queries(schema_name)
    
    print(f"\n" + "="*70)
    print("✅ CONFIGURATION TERMINÉE AVEC SUCCÈS!")
    print("="*70)
    
    print(f"\n📊 Schéma: hive.{schema_name}")
    print("🔗 Interface Web: http://localhost:8080")
    print("💻 CLI: docker-compose exec trino trino")
    
    print(f"\n📋 Commandes rapides:")
    print(f"  USE hive.{schema_name};")
    print(f"  SELECT * FROM products LIMIT 5;")
    print(f"  SELECT COUNT(*) FROM orders_clean;")

if __name__ == "__main__":
    main()