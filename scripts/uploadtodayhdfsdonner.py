import os
import subprocess
from datetime import datetime

# ==============================
# CONFIG
# ==============================
BASE_LOCAL_DATA = os.path.abspath("../data")
HDFS_RAW_ORDERS = "/raw/orders"
HDFS_RAW_STOCK = "/raw/stock"

TODAY = datetime.now().strftime("%Y-%m-%d")

LOCAL_ORDERS_TODAY = os.path.join(BASE_LOCAL_DATA, "raw_orders", f"date={TODAY}")
LOCAL_STOCK_TODAY = os.path.join(BASE_LOCAL_DATA, "raw_stock", f"date={TODAY}")

CONTAINER_TMP = "/tmp/data_today"

# ==============================
# UTILS
# ==============================
def run_cmd(cmd):
    print(f"→ {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"❌ Erreur: {result.stderr.strip()[:200]}")
    return result

# ==============================
# CHECK DATA
# ==============================
def check_today_data():
    print("\n📅 Date du jour :", TODAY)

    if not os.path.exists(LOCAL_ORDERS_TODAY):
        print(f"❌ Données orders non trouvées: {LOCAL_ORDERS_TODAY}")
        return False

    if not os.path.exists(LOCAL_STOCK_TODAY):
        print(f"❌ Données stock non trouvées: {LOCAL_STOCK_TODAY}")
        return False

    print("✅ Données du jour trouvées")
    return True

# ==============================
# COPY TO CONTAINER
# ==============================
def copy_to_container():
    print("\n📦 Copie vers le conteneur namenode...")

    run_cmd(f"docker-compose exec namenode rm -rf {CONTAINER_TMP}")
    run_cmd(f"docker-compose exec namenode mkdir -p {CONTAINER_TMP}")

    run_cmd(f'docker cp "{LOCAL_ORDERS_TODAY}" namenode:{CONTAINER_TMP}/raw_orders')
    run_cmd(f'docker cp "{LOCAL_STOCK_TODAY}" namenode:{CONTAINER_TMP}/raw_stock')

    print("✅ Copie locale → conteneur OK")

# ==============================
# UPLOAD TO HDFS
# ==============================
def upload_to_hdfs():
    print("\n🚀 Upload vers HDFS...")

    # Orders
    run_cmd(f"""
    docker-compose exec namenode hdfs dfs -mkdir -p {HDFS_RAW_ORDERS}/date={TODAY}
    """)
    run_cmd(f"""
    docker-compose exec namenode hdfs dfs -put -f \
    {CONTAINER_TMP}/raw_orders/* \
    {HDFS_RAW_ORDERS}/date={TODAY}/
    """)

    # Stock
    run_cmd(f"""
    docker-compose exec namenode hdfs dfs -mkdir -p {HDFS_RAW_STOCK}/date={TODAY}
    """)
    run_cmd(f"""
    docker-compose exec namenode hdfs dfs -put -f \
    {CONTAINER_TMP}/raw_stock/* \
    {HDFS_RAW_STOCK}/date={TODAY}/
    """)

    print("✅ Upload HDFS terminé")

# ==============================
# VERIFY
# ==============================
def verify_hdfs():
    print("\n🔍 Vérification HDFS")

    run_cmd(f"""
    docker-compose exec namenode hdfs dfs -ls {HDFS_RAW_ORDERS}/date={TODAY}
    """)

    run_cmd(f"""
    docker-compose exec namenode hdfs dfs -ls {HDFS_RAW_STOCK}/date={TODAY}
    """)

# ==============================
# MAIN
# ==============================
def main():
    print("=" * 60)
    print("UPLOAD HDFS — DONNÉES DU JOUR SEULEMENT")
    print("=" * 60)

    if not check_today_data():
        print("\n⛔ Arrêt : données manquantes")
        return

    copy_to_container()
    upload_to_hdfs()
    verify_hdfs()

    print("\n✅ PIPELINE DU JOUR TERMINÉ")

if __name__ == "__main__":
    main()
