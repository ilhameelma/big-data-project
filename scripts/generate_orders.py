import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import os
import json

fake = Faker()
np.random.seed(42)

# --- CONFIGURATION VOLUMINEUSE ---
NUM_SKUS = 2000
NUM_STORES = 50
NUM_WAREHOUSES = 5
DAYS_TO_GENERATE = 30
MAX_ORDERS_PER_DAY_PER_STORE = 200

# MODIFICATION IMPORTANTE : Chemin Windows local
# Option A : Chemin relatif dans votre projet
BASE_DIR = "./data"  # Créera un dossier "data" dans le répertoire courant

# Option B : Chemin absolu Windows (personnalisez selon votre système)
# BASE_DIR = "C:/Users/admin/Desktop/procurement_pipeline/data"

# Option C : Utiliser le répertoire du script
# import pathlib
# BASE_DIR = str(pathlib.Path(__file__).parent.parent / "data")

def generate_master_data(num_skus=500):
    """Génère des fichiers CSV pour les données maîtres (produits, fournisseurs, stock sécurité)."""
    print("Génération des données maîtres...")
    
    # 1. Produits
    categories = ['Dairy', 'Bakery', 'Beverages', 'Grocery', 'Cleaning', 'Personal Care', 'Frozen', 'Snacks']
    products = []
    for i in range(num_skus):
        sku_id = f"SKU{i:06d}"
        cat = np.random.choice(categories, p=[0.15, 0.1, 0.2, 0.25, 0.1, 0.1, 0.05, 0.05])
        products.append({
            'sku_id': sku_id,
            'product_name': f"{fake.word().capitalize()} {cat} {fake.random_element(['Pro', 'Gold', 'Basic', 'Organic'])}",
            'category': cat,
            'unit_price': round(np.random.uniform(0.5, 20.0), 2),
            'pack_size': np.random.choice([1, 6, 12, 24], p=[0.3, 0.3, 0.3, 0.1]),
            'min_order_quantity': np.random.choice([1, 5, 10, 24, 48], p=[0.2, 0.3, 0.3, 0.1, 0.1])
        })
    
    # Utiliser os.path.join pour une compatibilité multiplateforme
    master_dir = os.path.join(BASE_DIR, "master")
    os.makedirs(master_dir, exist_ok=True)
    
    pd.DataFrame(products).to_csv(os.path.join(master_dir, "products.csv"), index=False)
    
    # 2. Fournisseurs (associer aléatoirement aux produits)
    suppliers = [{'supplier_id': f'SUP{j:03d}', 'supplier_name': fake.company()} for j in range(20)]
    pd.DataFrame(suppliers).to_csv(os.path.join(master_dir, "suppliers.csv"), index=False)
    
    # 3. Lien Produit-Fournisseur
    product_supplier = []
    for prod in products:
        # Assigner 1 à 3 fournisseurs par produit, avec un primaire
        num_sup = np.random.randint(1, 4)
        chosen_sups = np.random.choice([s['supplier_id'] for s in suppliers], size=num_sup, replace=False)
        for i, sup_id in enumerate(chosen_sups):
            product_supplier.append({
                'sku_id': prod['sku_id'],
                'supplier_id': sup_id,
                'lead_time_days': np.random.randint(1, 7),
                'is_primary': (i == 0)
            })
    pd.DataFrame(product_supplier).to_csv(os.path.join(master_dir, "product_supplier.csv"), index=False)
    
    # 4. Stock de Sécurité (par SKU et entrepôt)
    safety_stock = []
    for prod in products[:1000]:  # On en met que pour une partie des SKUs
        for wh in [f'WH{w:02d}' for w in range(NUM_WAREHOUSES)]:
            safety_stock.append({
                'sku_id': prod['sku_id'],
                'warehouse_id': wh,
                'safety_stock_level': np.random.randint(5, 50)
            })
    pd.DataFrame(safety_stock).to_csv(os.path.join(master_dir, "safety_stock.csv"), index=False)
    
    print(f"Données maîtres générées: {num_skus} produits, 20 fournisseurs.")

def generate_daily_data(start_date, days):
    """Génère des commandes et des stocks pour une période."""
    print(f"Génération des données quotidiennes sur {days} jours...")
    
    all_skus = [f"SKU{i:06d}" for i in range(NUM_SKUS)]
    store_ids = [f'ST{s:04d}' for s in range(NUM_STORES)]
    warehouse_ids = [f'WH{w:02d}' for w in range(NUM_WAREHOUSES)]
    
    for day in range(days):
        current_date = start_date + timedelta(days=day)
        date_str = current_date.strftime("%Y-%m-%d")
        print(f"  Jour {date_str}...")
        
        # ---- ORDRES CLIENTS (JSON ou CSV par magasin) ----
        for store in store_ids[:10]:  # Générer pour un sous-ensemble de magasins chaque jour
            num_orders_day = np.random.randint(50, MAX_ORDERS_PER_DAY_PER_STORE)
            orders = []
            for _ in range(num_orders_day):
                order_id = f"ORD{date_str.replace('-','')}{fake.unique.random_number(digits=6)}"
                num_items = np.random.randint(1, 10)
                for __ in range(num_items):
                    sku = np.random.choice(all_skus[:500])  # Seuls 500 SKUs sont fréquemment achetés
                    orders.append({
                        'order_id': order_id,
                        'store_id': store,
                        'sku_id': sku,
                        'quantity': np.random.randint(1, 5),
                        'order_timestamp': current_date.replace(hour=np.random.randint(8, 20), minute=np.random.randint(0,59)).isoformat()
                    })
            
            # Sauvegarder en JSON (un fichier par magasin par jour)
            store_dir = os.path.join(BASE_DIR, "raw_orders", f"date={date_str}", f"store_id={store}")
            os.makedirs(store_dir, exist_ok=True)
            with open(os.path.join(store_dir, "orders.json"), 'w') as f:
                json.dump(orders, f, indent=2)
        
        # ---- SNAPSHOT STOCK (CSV par entrepôt) ----
        for warehouse in warehouse_ids:
            stock_data = []
            # Prendre un échantillon de SKUs présents dans cet entrepôt
            skus_in_wh = np.random.choice(all_skus, size=300, replace=False)
            for sku in skus_in_wh:
                stock_data.append({
                    'snapshot_date': date_str,
                    'warehouse_id': warehouse,
                    'sku_id': sku,
                    'available_stock': np.random.randint(0, 200),
                    'reserved_stock': np.random.randint(0, 50)
                })
            
            wh_dir = os.path.join(BASE_DIR, "raw_stock", f"date={date_str}")
            os.makedirs(wh_dir, exist_ok=True)
            pd.DataFrame(stock_data).to_csv(os.path.join(wh_dir, f"stock_{warehouse}.csv"), index=False)
    
    print(f"Données quotidiennes générées. ~{days * NUM_STORES * 100} lignes de commandes estimées.")

if __name__ == "__main__":
    # Créer les répertoires principaux
    os.makedirs(os.path.join(BASE_DIR, "master"), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, "raw_orders"), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, "raw_stock"), exist_ok=True)
    
    # 1. Générer les données de référence (maîtres)
    generate_master_data(NUM_SKUS)
    
    # 2. Générer les données opérationnelles historiques (30 derniers jours)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=DAYS_TO_GENERATE)
    generate_daily_data(start_date, DAYS_TO_GENERATE)
    
    print("\n--- Génération terminée ---")
    print(f"Données sauvegardées dans : {os.path.abspath(BASE_DIR)}")
    print("Structure :")
    print(f"  {os.path.join(BASE_DIR, 'master')} -> produits, fournisseurs, etc.")
    print(f"  {os.path.join(BASE_DIR, 'raw_orders', 'date=YYYY-MM-DD', 'store_id=XXXX')} -> fichiers JSON")
    print(f"  {os.path.join(BASE_DIR, 'raw_stock', 'date=YYYY-MM-DD')} -> fichiers CSV")
    
    # Statistiques
    total_files = sum([len(files) for r, d, files in os.walk(BASE_DIR)])
    print(f"\nTotal de fichiers générés : {total_files}")