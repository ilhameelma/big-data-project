import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime
import os
import json
import sys

# =========================
# CONFIGURATION
# =========================
fake = Faker()
np.random.seed(42)

NUM_SKUS = 2000
NUM_STORES = 50
NUM_WAREHOUSES = 5
MAX_ORDERS_PER_DAY_PER_STORE = 200

BASE_DIR = "./data"

# =========================
# DONNÉES MAÎTRES (UNE FOIS)
# =========================
def generate_master_data():
    print("🔹 Génération des données maîtres...")

    categories = [
        "Dairy", "Bakery", "Beverages", "Grocery",
        "Cleaning", "Personal Care", "Frozen", "Snacks"
    ]

    products = []
    for i in range(NUM_SKUS):
        products.append({
            "sku_id": f"SKU{i:06d}",
            "product_name": f"{fake.word().capitalize()}",
            "category": np.random.choice(categories),
            "unit_price": round(np.random.uniform(0.5, 20), 2),
            "pack_size": np.random.choice([1, 6, 12, 24]),
            "min_order_quantity": np.random.choice([1, 5, 10, 24])
        })

    master_dir = os.path.join(BASE_DIR, "master")
    os.makedirs(master_dir, exist_ok=True)

    pd.DataFrame(products).to_csv(
        os.path.join(master_dir, "products.csv"), index=False
    )

    suppliers = [
        {"supplier_id": f"SUP{i:03d}", "supplier_name": fake.company()}
        for i in range(20)
    ]
    pd.DataFrame(suppliers).to_csv(
        os.path.join(master_dir, "suppliers.csv"), index=False
    )

    product_supplier = []
    for p in products:
        for i, sup in enumerate(
            np.random.choice([s["supplier_id"] for s in suppliers],
                             size=np.random.randint(1, 4),
                             replace=False)
        ):
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
# DONNÉES POUR UNE DATE
# =========================
def generate_daily_data(target_date):
    print(f"\n🔹 Génération des données pour : {target_date}")

    all_skus = [f"SKU{i:06d}" for i in range(NUM_SKUS)]
    store_ids = [f"ST{i:04d}" for i in range(NUM_STORES)]
    warehouse_ids = [f"WH{i:02d}" for i in range(NUM_WAREHOUSES)]

    # ---------- ORDERS ----------
    for store in store_ids[:10]:
        orders = []
        num_orders = np.random.randint(50, MAX_ORDERS_PER_DAY_PER_STORE)

        for _ in range(num_orders):
            order_id = f"ORD{target_date.replace('-', '')}{fake.random_number(6)}"
            for _ in range(np.random.randint(1, 10)):
                orders.append({
                    "order_id": order_id,
                    "store_id": store,
                    "sku_id": np.random.choice(all_skus[:500]),
                    "quantity": np.random.randint(1, 5),
                    "order_timestamp": f"{target_date}T{fake.time()}"
                })

        store_dir = os.path.join(
            BASE_DIR, "raw_orders",
            f"date={target_date}", f"store_id={store}"
        )
        os.makedirs(store_dir, exist_ok=True)

        with open(os.path.join(store_dir, "orders.json"), "w") as f:
            json.dump(orders, f, indent=2)

    # ---------- STOCK ----------
    stock_dir = os.path.join(BASE_DIR, "raw_stock", f"date={target_date}")
    os.makedirs(stock_dir, exist_ok=True)

    for wh in warehouse_ids:
        stock = []
        for sku in np.random.choice(all_skus, 300, replace=False):
            stock.append({
                "snapshot_date": target_date,
                "warehouse_id": wh,
                "sku_id": sku,
                "available_stock": np.random.randint(0, 200),
                "reserved_stock": np.random.randint(0, 50)
            })

        pd.DataFrame(stock).to_csv(
            os.path.join(stock_dir, f"stock_{wh}.csv"),
            index=False
        )

    print("✔ Données journalières générées")


# =========================
# MAIN
# =========================
if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("❌ Usage : python generate_data.py YYYY-MM-DD")
        sys.exit(1)

    target_date = sys.argv[1]

    try:
        datetime.strptime(target_date, "%Y-%m-%d")
    except ValueError:
        print("❌ Format de date invalide. Exemple : 2025-01-15")
        sys.exit(1)

    os.makedirs(BASE_DIR, exist_ok=True)

    # Générer master une seule fois
    if not os.path.exists(os.path.join(BASE_DIR, "master", "products.csv")):
        generate_master_data()
    else:
        print("✔ Données maîtres déjà existantes")

    generate_daily_data(target_date)

    print("\n✅ FIN DU SCRIPT")
    print(f"📁 Données dans : {os.path.abspath(BASE_DIR)}")
