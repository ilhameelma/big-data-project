import pandas as pd
import psycopg2
import os
from datetime import datetime

def load_master_data():
    # Connexion PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        database="procurement_db",
        user="procurement_user",
        password="procurement_pass"
    )
    cur = conn.cursor()
    
    # Chemin vers les données dans le conteneur
    data_dir = "../data/master"
    
    # 1. Charger les produits
    products_df = pd.read_csv(os.path.join(data_dir, "products.csv"))
    for _, row in products_df.iterrows():
        cur.execute("""
            INSERT INTO products (sku_id, product_name, category, unit_price, pack_size, min_order_quantity)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (sku_id) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                unit_price = EXCLUDED.unit_price
        """, (row['sku_id'], row['product_name'], row['category'], 
              row['unit_price'], row['pack_size'], row['min_order_quantity']))
    
    # 2. Charger les fournisseurs
    suppliers_df = pd.read_csv(os.path.join(data_dir, "suppliers.csv"))
    for _, row in suppliers_df.iterrows():
        cur.execute("""
            INSERT INTO suppliers (supplier_id, supplier_name)
            VALUES (%s, %s)
            ON CONFLICT (supplier_id) DO NOTHING
        """, (row['supplier_id'], row['supplier_name']))
    
    # 3. Charger product_supplier
    product_supplier_df = pd.read_csv(os.path.join(data_dir, "product_supplier.csv"))
    for _, row in product_supplier_df.iterrows():
        cur.execute("""
            INSERT INTO product_supplier (sku_id, supplier_id, lead_time_days, is_primary)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (sku_id, supplier_id) DO UPDATE SET
                lead_time_days = EXCLUDED.lead_time_days
        """, (row['sku_id'], row['supplier_id'], 
              row['lead_time_days'], row['is_primary']))
    
    # 4. Charger safety_stock
    safety_stock_df = pd.read_csv(os.path.join(data_dir, "safety_stock.csv"))
    for _, row in safety_stock_df.iterrows():
        cur.execute("""
            INSERT INTO safety_stock (sku_id, warehouse_id, safety_stock_level)
            VALUES (%s, %s, %s)
            ON CONFLICT (sku_id, warehouse_id) DO UPDATE SET
                safety_stock_level = EXCLUDED.safety_stock_level
        """, (row['sku_id'], row['warehouse_id'], row['safety_stock_level']))
    
    conn.commit()
    cur.close()
    conn.close()
    print("Données maîtres chargées dans PostgreSQL avec succès!")

if __name__ == "__main__":
    load_master_data()