import subprocess
import json
import pandas as pd
from io import StringIO

def run_hdfs_command(cmd):
    """Exécute une commande HDFS"""
    full_cmd = f"docker-compose exec namenode {cmd}"
    print(f"\n$ {cmd}")
    
    result = subprocess.run(full_cmd, shell=True, capture_output=True, text=True)
    return result

def analyze_orders():
    """Analyse les données de commandes"""
    print("=== ANALYSE DES DONNÉES DE COMMANDES ===")
    
    # Récupérer un fichier JSON
    print("\n1. Récupération d'un fichier orders.json...")
    result = run_hdfs_command("hdfs dfs -cat /raw/orders/date=2025-12-02/store_id=ST0000/orders.json")
    
    if result.returncode == 0 and result.stdout:
        try:
            data = json.loads(result.stdout)
            print(f"   ✓ Fichier chargé avec succès")
            print(f"   Nombre de lignes de commande: {len(data)}")
            
            if len(data) > 0:
                print(f"\n2. Premier enregistrement:")
                first_record = data[0]
                print(json.dumps(first_record, indent=2))
                
                # Convertir en DataFrame pour analyse
                df = pd.DataFrame(data)
                print(f"\n3. Statistiques du DataFrame:")
                print(f"   Colonnes: {list(df.columns)}")
                print(f"   Shape: {df.shape}")
                print(f"\n   Aperçu des 3 premières lignes:")
                print(df.head(3).to_string())
                
                print(f"\n   Quantités par SKU:")
                sku_counts = df.groupby('sku_id')['quantity'].sum().sort_values(ascending=False)
                print(sku_counts.head(10).to_string())
                
        except json.JSONDecodeError as e:
            print(f"   ✗ Erreur de parsing JSON: {e}")
            print(f"   Contenu brut (premiers 500 caractères):")
            print(result.stdout[:500])
    else:
        print(f"   ✗ Impossible de lire le fichier")

def analyze_stock():
    """Analyse les données de stock"""
    print("\n\n=== ANALYSE DES DONNÉES DE STOCK ===")
    
    # Récupérer un fichier CSV de stock
    print("\n1. Récupération d'un fichier stock_WH00.csv...")
    result = run_hdfs_command("hdfs dfs -cat /raw/stock/date=2025-12-02/stock_WH00.csv")
    
    if result.returncode == 0 and result.stdout:
        try:
            # Lire le CSV
            df = pd.read_csv(StringIO(result.stdout))
            print(f"   ✓ Fichier chargé avec succès")
            print(f"   Shape: {df.shape}")
            
            print(f"\n2. Aperçu des données:")
            print(df.head().to_string())
            
            print(f"\n3. Statistiques:")
            print(f"   Colonnes: {list(df.columns)}")
            print(f"   Nombre unique de SKUs: {df['sku_id'].nunique()}")
            print(f"   Stock total disponible: {df['available_stock'].sum()}")
            print(f"   Stock total réservé: {df['reserved_stock'].sum()}")
            
        except Exception as e:
            print(f"   ✗ Erreur: {e}")

def analyze_master():
    """Analyse les données maîtres"""
    print("\n\n=== ANALYSE DES DONNÉES MAÎTRES ===")
    
    fichiers = [
        ("products.csv", "Produits"),
        ("suppliers.csv", "Fournisseurs"),
        ("product_supplier.csv", "Lien Produit-Fournisseur"),
        ("safety_stock.csv", "Stock de Sécurité")
    ]
    
    for fichier, nom in fichiers:
        print(f"\n{nom} ({fichier}):")
        result = run_hdfs_command(f"hdfs dfs -cat /raw/master/{fichier}")
        
        if result.returncode == 0 and result.stdout:
            try:
                df = pd.read_csv(StringIO(result.stdout))
                print(f"   ✓ {df.shape[0]} lignes, {df.shape[1]} colonnes")
                print(f"   Colonnes: {list(df.columns)}")
                print(f"   Aperçu:")
                print(df.head(3).to_string(index=False))
                
                if fichier == "products.csv":
                    print(f"\n   Statistiques produits:")
                    print(f"   Catégories: {df['category'].nunique()}")
                    print(f"   Prix moyen: {df['unit_price'].mean():.2f} €")
                    print(f"   Pack size distribution: {df['pack_size'].value_counts().to_dict()}")
                    
            except Exception as e:
                print(f"   ✗ Erreur: {e}")

def verify_hdfs_structure():
    """Vérifie la structure HDFS"""
    print("=== VÉRIFICATION DE LA STRUCTURE HDFS ===")
    
    print("\n1. Structure /raw:")
    result = run_hdfs_command("hdfs dfs -ls /raw")
    
    print("\n2. Fichiers dans /raw/orders:")
    result = run_hdfs_command("hdfs dfs -count /raw/orders")
    if result.stdout:
        parts = result.stdout.strip().split()
        if len(parts) >= 4:
            print(f"   Dossiers: {parts[0]}, Fichiers: {parts[1]}, Taille: {int(parts[2])/1024:.2f} KB")
    
    print("\n3. Interface Web:")
    print("   http://localhost:9870")

def main():
    """Fonction principale"""
    print("="*60)
    print("ANALYSE COMPLÈTE DES DONNÉES DANS HDFS")
    print("="*60)
    
    verify_hdfs_structure()
    analyze_orders()
    analyze_stock()
    analyze_master()
    
    print("\n" + "="*60)
    print("ANALYSE TERMINÉE")
    print("="*60)

if __name__ == "__main__":
    main()