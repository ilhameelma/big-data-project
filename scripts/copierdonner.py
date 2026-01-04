import os
import subprocess
import time

def check_docker_status():
    """Vérifie si Docker et les conteneurs sont en cours d'exécution"""
    print("Vérification de Docker...")
    
    # Vérifier si Docker Desktop est lancé
    try:
        result = subprocess.run("docker version", shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            print("❌ Docker Desktop n'est pas lancé ou installé")
            print("   Lancez Docker Desktop depuis le menu Démarrer")
            return False
    except:
        print("❌ Docker n'est pas installé ou accessible")
        return False
    
    print("✓ Docker Desktop est en cours d'exécution")
    
    # Vérifier les conteneurs Hadoop
    result = subprocess.run("docker-compose ps", shell=True, capture_output=True, text=True, cwd="../")
    
    if result.returncode != 0:
        print("❌ Problème avec docker-compose")
        print("   Essayez depuis le dossier procurement_pipeline:")
        print("   docker-compose up -d")
        return False
    
    print("\nÉtat des conteneurs:")
    print(result.stdout)
    
    # Vérifier si namenode est "Up"
    if "namenode" in result.stdout and "Up" in result.stdout:
        print("\n✅ Namenode est en cours d'exécution")
        return True
    else:
        print("\n❌ Namenode n'est pas en cours d'exécution")
        print("   Lancez: docker-compose up -d")
        return False

def start_docker_containers():
    """Démarre les conteneurs Docker si nécessaire"""
    print("\nDémarrage des conteneurs Docker...")
    
    # Aller dans le dossier procurement_pipeline
    original_dir = os.getcwd()
    os.chdir("..")  # Remonter d'un niveau
    
    try:
        # Lancer docker-compose
        result = subprocess.run("docker-compose up -d", shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Erreur: {result.stderr}")
            return False
        
        print("✓ Conteneurs démarrés")
        
        # Attendre que Hadoop soit prêt
        print("Attente que Hadoop démarre...")
        time.sleep(30)  # Attendre 30 secondes
        
        # Vérifier que namenode est prêt
        for attempt in range(5):
            print(f"Vérification {attempt+1}/5...")
            check_result = subprocess.run(
                "docker-compose exec namenode hdfs dfsadmin -report",
                shell=True, capture_output=True, text=True
            )
            
            if check_result.returncode == 0:
                print("✓ Hadoop est prêt!")
                return True
            
            time.sleep(10)  # Attendre 10 secondes entre les tentatives
        
        print("⚠ Hadoop met du temps à démarrer, continuation...")
        return True
        
    finally:
        os.chdir(original_dir)  # Revenir au dossier original

def run_cmd(cmd, verbose=True):
    """Exécute une commande dans le conteneur"""
    if verbose:
        print(f"  → {cmd}")
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0 and verbose:
        if result.stderr:
            print(f"  ❌ Erreur: {result.stderr.strip()[:200]}")
    
    return result

def upload_orders_data():
    """Upload les données vers HDFS"""
    print("\n" + "="*60)
    print("UPLOAD DES DONNÉES VERS HDFS")
    print("="*60)
    
    # 1. Vérifier/copier les données
    print("\n1. Préparation des données...")
    
    data_path = os.path.abspath("../data")
    print(f"   Chemin local: {data_path}")
    
    if not os.path.exists(data_path):
        print(f"❌ Dossier non trouvé: {data_path}")
        return
    
    # Copier les données vers le conteneur
    print("   Copie des données vers le conteneur...")
    result = run_cmd(f'docker cp "{data_path}" namenode:/tmp/data_full')
    
    if result.returncode != 0:
        print("❌ Échec de la copie des données")
        return
    
    print("   ✓ Données copiées")
    
    # 2. Préparer HDFS
    print("\n2. Préparation HDFS...")
    
    # Créer la structure de base
    run_cmd("docker-compose exec namenode hdfs dfs -mkdir -p /raw/orders", verbose=False)
    
    # 3. Upload
    print("\n3. Upload des fichiers...")
    
    # Trouver tous les fichiers orders.json
    result = run_cmd(
        'docker-compose exec namenode find /tmp/data_full/raw_orders -name "orders.json" -type f',
        verbose=False
    )
    
    if not result.stdout.strip():
        print("❌ Aucun fichier orders.json trouvé")
        return
    
    files = [f.strip() for f in result.stdout.split('\n') if f.strip()]
    print(f"   Fichiers à uploader: {len(files)}")
    
    # Upload chaque fichier
    uploaded = 0
    for i, src_file in enumerate(files, 1):
        try:
            # Calculer le chemin HDFS
            rel_path = src_file.replace('/tmp/data_full/raw_orders/', '')
            hdfs_file = f"/raw/orders/{rel_path}"
            hdfs_dir = os.path.dirname(hdfs_file)
            
            # Créer le dossier
            run_cmd(f'docker-compose exec namenode hdfs dfs -mkdir -p "{hdfs_dir}"', verbose=False)
            
            # Upload avec -f (force)
            result = run_cmd(
                f'docker-compose exec namenode hdfs dfs -put -f "{src_file}" "{hdfs_file}"',
                verbose=False
            )
            
            if result.returncode == 0:
                uploaded += 1
                if uploaded % 10 == 0 or i == len(files):
                    print(f"   ✓ {i}/{len(files)}: {rel_path}")
            else:
                print(f"   ❌ {rel_path}")
                
        except Exception as e:
            print(f"   ❌ Erreur: {str(e)[:100]}")
    
    # 4. Vérification
    print("\n4. Vérification...")
    
    # Compter les fichiers dans HDFS
    result = run_cmd("docker-compose exec namenode hdfs dfs -ls -R /raw/orders | grep orders.json | wc -l", verbose=False)
    if result.returncode == 0:
        hdfs_count = result.stdout.strip()
        print(f"   Fichiers dans HDFS: {hdfs_count}")
    
    # Afficher un échantillon
    result = run_cmd("docker-compose exec namenode hdfs dfs -ls /raw/orders/*/* | head -5", verbose=False)
    if result.returncode == 0:
        print("\n   Échantillon HDFS:")
        for line in result.stdout.strip().split('\n'):
            if line.strip():
                print(f"   {line.strip()}")
    
    # Résumé
    print("\n" + "="*60)
    print(f"RÉSUMÉ: {uploaded}/{len(files)} fichiers uploadés")
    print("="*60)
    
    if uploaded > 0:
        print("\n✅ Test d'accès aux données...")
        
        # Tester la lecture avec Spark
        test_spark = '''
from pyspark.sql import SparkSession
spark = SparkSession.builder \\
    .appName("TestHDFS") \\
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \\
    .getOrCreate()

try:
    df = spark.read.json("hdfs://namenode:9000/raw/orders/date=2025-12-02/store_id=ST0000/orders.json")
    print(f"✓ Test réussi: {df.count()} lignes")
    df.show(2)
except Exception as e:
    print(f"❌ Erreur: {e}")
finally:
    spark.stop()
        '''
        
        # Écrire le script de test
        with open("test_hdfs.py", "w") as f:
            f.write(test_spark)
        
        print("   Exécutez: python test_hdfs.py")
        print("   pour vérifier l'accès aux données")

def main():
    """Fonction principale"""
    print("="*60)
    print("UPLOAD HDFS - PROCUREMENT PIPELINE")
    print("="*60)
    
    # Vérifier/initialiser Docker
    if not check_docker_status():
        choice = input("\nVoulez-vous démarrer les conteneurs? (o/n): ").strip().lower()
        if choice == 'o':
            if not start_docker_containers():
                return
        else:
            print("Annulation.")
            return
    
    # Upload des données
    upload_orders_data()
    
    print("\n" + "="*60)
    print("TERMINÉ")
    print("="*60)

if __name__ == "__main__":
    main()