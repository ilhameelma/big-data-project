import os
import subprocess
import time

def run_cmd(cmd, verbose=True):
    """Exécute une commande et retourne le résultat"""
    if verbose:
        print(f"> {cmd}")
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        if verbose:
            print(f"  ERREUR: {result.stderr[:200]}")
    else:
        if verbose and result.stdout.strip():
            print(f"  OK")
    
    return result

def create_hdfs_structure():
    """Crée toute la structure HDFS"""
    print("=== CRÉATION DE LA STRUCTURE HDFS ===")
    
    directories = [
        "/raw/orders",
        "/raw/stock",
        "/raw/master",
        "/processed/aggregated_orders",
        "/processed/net_demand",
        "/output/supplier_orders",
        "/logs/exceptions"
    ]
    
    for directory in directories:
        run_cmd(f"docker-compose exec namenode hdfs dfs -mkdir -p {directory}")
    
    print("Structure HDFS créée ✓")

def upload_orders_data():
    """Upload toutes les données de commandes"""
    print("\n=== UPLOAD DES COMMANDES ===")
    
    # D'abord, copier tout le dossier data vers le conteneur (si pas déjà fait)
    data_path = os.path.abspath("../data")
    print(f"Copie de {data_path} vers le conteneur...")
    run_cmd(f'docker cp "{data_path}" namenode:/tmp/data_full')
    
    # Trouver toutes les dates
    result = run_cmd('docker-compose exec namenode find /tmp/data_full/raw_orders -maxdepth 1 -type d -name "date=*"', verbose=False)
    
    if not result.stdout.strip():
        print("Aucune donnée de commandes trouvée")
        return
    
    dates = [line.strip().split('=')[-1] for line in result.stdout.split('\n') if line.strip()]
    print(f"Dates trouvées: {len(dates)} dates")
    
    # Pour chaque date
    for date in dates[:3]:  # Limiter à 3 dates pour commencer
        print(f"\nTraitement de la date: {date}")
        
        # Trouver tous les magasins pour cette date
        result = run_cmd(f'docker-compose exec namenode find /tmp/data_full/raw_orders/date={date} -maxdepth 1 -type d -name "store_id=*"', verbose=False)
        
        if not result.stdout.strip():
            continue
            
        stores = [line.strip().split('=')[-1] for line in result.stdout.split('\n') if line.strip()]
        print(f"  Magasins trouvés: {len(stores)}")
        
        # Upload pour chaque magasin
        for store in stores[:5]:  # Limiter à 5 magasins par date
            hdfs_path = f"/raw/orders/date={date}/store_id={store}"
            
            # Créer le dossier dans HDFS
            run_cmd(f"docker-compose exec namenode hdfs dfs -mkdir -p {hdfs_path}", verbose=False)
            
            # Upload le fichier
            local_path = f"/tmp/data_full/raw_orders/date={date}/store_id={store}/orders.json"
            run_cmd(f'docker-compose exec namenode hdfs dfs -put -f "{local_path}" "{hdfs_path}/"', verbose=False)
            
            print(f"  ✓ Store {store} uploadé")
    
    print("Upload des commandes terminé ✓")

def upload_stock_data():
    """Upload toutes les données de stock"""
    print("\n=== UPLOAD DU STOCK ===")
    
    # Trouver toutes les dates
    result = run_cmd('docker-compose exec namenode find /tmp/data_full/raw_stock -maxdepth 1 -type d -name "date=*"', verbose=False)
    
    if not result.stdout.strip():
        print("Aucune donnée de stock trouvée")
        return
    
    dates = [line.strip().split('=')[-1] for line in result.stdout.split('\n') if line.strip()]
    print(f"Dates trouvées: {len(dates)} dates")
    
    # Pour chaque date
    for date in dates[:3]:  # Limiter à 3 dates pour commencer
        print(f"\nTraitement de la date: {date}")
        
        # Créer le dossier dans HDFS
        hdfs_path = f"/raw/stock/date={date}"
        run_cmd(f"docker-compose exec namenode hdfs dfs -mkdir -p {hdfs_path}", verbose=False)
        
        # Trouver tous les fichiers CSV pour cette date
        result = run_cmd(f'docker-compose exec namenode find /tmp/data_full/raw_stock/date={date} -name "stock_*.csv"', verbose=False)
        
        if not result.stdout.strip():
            continue
            
        files = [line.strip() for line in result.stdout.split('\n') if line.strip()]
        
        # Upload chaque fichier
        for file_path in files:
            file_name = os.path.basename(file_path)
            run_cmd(f'docker-compose exec namenode hdfs dfs -put -f "{file_path}" "{hdfs_path}/{file_name}"', verbose=False)
            
            print(f"  ✓ {file_name} uploadé")
    
    print("Upload du stock terminé ✓")

def upload_master_data():
    """Upload toutes les données maîtres"""
    print("\n=== UPLOAD DES DONNÉES MAÎTRES ===")
    
    # Créer le dossier dans HDFS
    run_cmd("docker-compose exec namenode hdfs dfs -mkdir -p /raw/master")
    
    # Trouver tous les fichiers CSV dans master
    result = run_cmd('docker-compose exec namenode find /tmp/data_full/master -name "*.csv"', verbose=False)
    
    if not result.stdout.strip():
        print("Aucun fichier master trouvé")
        return
    
    files = [line.strip() for line in result.stdout.split('\n') if line.strip()]
    
    # Upload chaque fichier
    for file_path in files:
        file_name = os.path.basename(file_path)
        run_cmd(f'docker-compose exec namenode hdfs dfs -put -f "{file_path}" "/raw/master/{file_name}"')
        print(f"  ✓ {file_name} uploadé")
    
    print("Upload des données maîtres terminé ✓")

def verify_upload():
    """Vérifie l'upload complet"""
    print("\n=== VÉRIFICATION FINALE ===")
    
    print("\n1. Structure complète de /raw:")
    result = run_cmd("docker-compose exec namenode hdfs dfs -ls -R /raw")
    
    print("\n2. Nombre de fichiers par catégorie:")
    run_cmd("docker-compose exec namenode hdfs dfs -count /raw/orders")
    run_cmd("docker-compose exec namenode hdfs dfs -count /raw/stock")
    run_cmd("docker-compose exec namenode hdfs dfs -count /raw/master")
    
    print("\n3. Taille des données:")
    run_cmd("docker-compose exec namenode hdfs dfs -du -h /raw")
    
    print("\n4. Exemple de contenu:")
    # Afficher un fichier exemple
    run_cmd('docker-compose exec namenode hdfs dfs -cat /raw/master/products.csv | head -5')

def cleanup():
    """Nettoie les fichiers temporaires"""
    print("\n=== NETTOYAGE ===")
    run_cmd("docker-compose exec namenode rm -rf /tmp/data_full")
    print("Fichiers temporaires nettoyés ✓")

def main():
    """Fonction principale"""
    print("=== DÉBUT DE L'UPLOAD COMPLET VERS HDFS ===")
    
    start_time = time.time()
    
    # 1. Créer la structure
    create_hdfs_structure()
    
    # 2. Upload des données
    upload_orders_data()
    upload_stock_data()
    upload_master_data()
    
    # 3. Vérification
    verify_upload()
    
    # 4. Nettoyage
    cleanup()
    
    end_time = time.time()
    
    print(f"\n=== UPLOAD TERMINÉ EN {end_time - start_time:.2f} secondes ===")
    print("\nInterface Web HDFS: http://localhost:9870")

if __name__ == "__main__":
    main()