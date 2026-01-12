import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import os
import json

# =========================
# CONFIGURATION
# =========================
fake = Faker()
np.random.seed(42)

NUM_SKUS = 2000
NUM_STORES = 50
NUM_WAREHOUSES = 5
MAX_ORDERS_PER_DAY_PER_STORE = 200

BASE_DIR = "./data"  # dossier de sortie

# =========================
# DONNÉES MAÎTRES
# =========================
def generate_master_data(num_skus):
    print("Génération des données maîtres...")

    categories = [
        'Dairy', 'Bakery', 'Beverages', 'Grocery',
        'Cleaning', 'Personal Care', 'Frozen', 'Snacks'
    ]

    products = []
    for i in range(num_skus):
        sku_id = f"SKU{i:06d}"
        cat = np.random.choice(categories)
        products.append({
            "sku_id": sku_id,
            "product_name": f"{fake.word().capitalize()} {cat}",
            "category": cat,
            "unit_price": round(np.random.uniform(0.5, 20), 2),
            "pack_size": np.random.choice([1, 6, 12, 24]),
            "min_order_quantity": np.random.choice([1, 5, 10, 24])
        })

    master_dir = os.path.join(BASE_DIR, "master")
    os.makedirs(master_dir, exist_ok=True)

    pd.DataFrame(products).to_csv(
        os.path.join(master_dir, "products.csv"), index=False
    )

    # Fournisseurs
    suppliers = [
        {"supplier_id": f"SUP{j:03d}", "supplier_name": fake.company()}
        for j in range(20)
    ]
    pd.DataFrame(suppliers).to_csv(
        os.path.join(master_dir, "suppliers.csv"), index=False
    )

    # Lien produit-fournisseur
    product_supplier = []
    for p in products:
        chosen = np.random.choice(
            [s["supplier_id"] for s in suppliers],
            size=np.random.randint(1, 4),
            replace=False
        )
        for i, sup in enumerate(chosen):
            product_supplier.append({
                "sku_id": p["sku_id"],
                "supplier_id": sup,
                "lead_time_days": np.random.randint(1, 7),
                "is_primary": i == 0
            })

    pd.DataFrame(product_supplier).to_csv(
        os.path.join(master_dir, "product_supplier.csv"), index=False
    )

    print("✔ Données maîtres générées")

# =========================
# DONNÉES DU JOUR
# =========================
def generate_today_data(date):
    print(f"Génération des données pour : {date}")

    all_skus = [f"SKU{i:06d}" for i in range(NUM_SKUS)]
    store_ids = [f"ST{s:04d}" for s in range(NUM_STORES)]
    warehouse_ids = [f"WH{w:02d}" for w in range(NUM_WAREHOUSES)]

    # ---------- COMMANDES ----------
    for store in store_ids[:10]:
        orders = []
        num_orders = np.random.randint(50, MAX_ORDERS_PER_DAY_PER_STORE)

        for _ in range(num_orders):
            order_id = f"ORD{date.replace('-', '')}{fake.random_number(6)}"
            for __ in range(np.random.randint(1, 10)):
                orders.append({
                    "order_id": order_id,
                    "store_id": store,
                    "sku_id": np.random.choice(all_skus[:500]),
                    "quantity": np.random.randint(1, 5),
                    "order_timestamp": datetime.now().isoformat()
                })

        store_dir = os.path.join(
            BASE_DIR, "raw_orders", f"date={date}", f"store_id={store}"
        )
        os.makedirs(store_dir, exist_ok=True)

        with open(os.path.join(store_dir, "orders.json"), "w") as f:
            json.dump(orders, f, indent=2)

    # ---------- STOCK ----------
    for wh in warehouse_ids:
        stock = []
        for sku in np.random.choice(all_skus, 300, replace=False):
            stock.append({
                "snapshot_date": date,
                "warehouse_id": wh,
                "sku_id": sku,
                "available_stock": np.random.randint(0, 200),
                "reserved_stock": np.random.randint(0, 50)
            })

        wh_dir = os.path.join(BASE_DIR, "raw_stock", f"date={date}")
        os.makedirs(wh_dir, exist_ok=True)

        pd.DataFrame(stock).to_csv(
            os.path.join(wh_dir, f"stock_{wh}.csv"),
            index=False
        )

    print("✔ Données du jour générées")

# =========================
# MAIN
# =========================
if __name__ == "__main__":

    os.makedirs(BASE_DIR, exist_ok=True)

    # Générer les données maîtres (une seule fois)
    generate_master_data(NUM_SKUS)

    # Générer uniquement aujourd'hui
    today = datetime.now().strftime("%Y-%m-%d")
    generate_today_data(today)

    print("\n--- FIN ---")
    print(f"Données disponibles dans : {os.path.abspath(BASE_DIR)}")
