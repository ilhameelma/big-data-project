#!/usr/bin/env python

"""
Script de test de connectivité pour tous les composants du pipeline.
"""
import os
import sys
import time
import socket
import subprocess
import requests
import psycopg2
from hdfs import InsecureClient

def test_port(host, port, service_name):
    """Test la connectivité à un port spécifique."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print(f" {service_name} ({host}:{port}) - CONNECTÉ")
            return True
        else:
            print(f" {service_name} ({host}:{port}) - ÉCHEC")
            return False
    except Exception as e:
        print(f" {service_name} ({host}:{port}) - ERREUR: {e}")
        return False

def test_hdfs():
    """Test la connexion à HDFS."""
    try:
        client = InsecureClient(f'http://hadoop-namenode:9870', user='root')
        status = client.status('/')
        print(f" HDFS Web UI - ACCESSIBLE")
        print(f"  Capacité: {status.get('capacity', 'N/A')}")
        return True
    except Exception as e:
        print(f" HDFS Web UI - ERREUR: {e}")
        return False

def test_postgres():
    """Test la connexion à PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="procurement",
            user="postgres",
            password="postgres"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f" PostgreSQL - CONNECTÉ")
        print(f"  Version: {version[0]}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f" PostgreSQL - ERREUR: {e}")
        return False

def test_presto():
    """Test la connexion à Presto."""
    try:
        # Test via l'API REST de Presto
        response = requests.get("http://presto-coordinator:8080/v1/info", timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Presto - CONNECTÉ")
            print(f"  Version: {data.get('nodeVersion', {}).get('version', 'N/A')}")
            print(f"  Environnement: {data.get('environment', 'N/A')}")
            return True
        else:
            print(f"✗ Presto - Code HTTP: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Presto - ERREUR: {e}")
        return False

def create_hdfs_structure():
    """Crée la structure de dossiers HDFS requise."""
    try:
        client = InsecureClient(f'http://hadoop-namenode:9870', user='root')
        
        directories = [
            '/raw/orders',
            '/raw/stock',
            '/processed/aggregated_orders',
            '/processed/net_demand',
            '/output/supplier_orders',
            '/logs/exceptions',
            '/master'
        ]
        
        for directory in directories:
            if not client.content(directory, strict=False):
                client.makedirs(directory)
                print(f"✓ Créé: {directory}")
        
        print("✓ Structure HDFS créée avec succès")
        return True
    except Exception as e:
        print(f"✗ Erreur création HDFS: {e}")
        return False

def main():
    """Fonction principale."""
    print("=" * 60)
    print("TEST DE CONNECTIVITÉ - PIPELINE PROCUREMENT")
    print("=" * 60)
    
    # Attente initiale pour le démarrage des services
    print("\n Attente du démarrage des services (30 secondes)...")
    time.sleep(30)
    
    results = []
    
    # Test des ports
    print("\n🔌 TESTS DE CONNECTIVITÉ PAR PORT:")
    print("-" * 40)
    
    services = [
        ("hadoop-namenode", 9870, "HDFS NameNode Web UI"),
        ("hadoop-namenode", 9000, "HDFS Service"),
        ("hadoop-datanode", 9864, "HDFS DataNode Web UI"),
        ("postgres", 5432, "PostgreSQL"),
        ("presto-coordinator", 8080, "Presto Web UI"),
    ]
    
    for host, port, name in services:
        results.append(test_port(host, port, name))
    
    # Tests fonctionnels
    print("\n TESTS FONCTIONNELS:")
    print("-" * 40)
    
    print("\n1. Test HDFS:")
    results.append(test_hdfs())
    
    print("\n2. Test PostgreSQL:")
    results.append(test_postgres())
    
    print("\n3. Test Presto:")
    results.append(test_presto())
    
    # Création structure HDFS
    print("\n CRÉATION STRUCTURE HDFS:")
    print("-" * 40)
    results.append(create_hdfs_structure())
    
    # Résumé
    print("\n" + "=" * 60)
    print("RÉSUMÉ DES TESTS")
    print("=" * 60)
    
    successful = sum(results)
    total = len(results)
    
    print(f"\n Tests réussis: {successful}/{total}")
    print(f" Tests échoués: {total - successful}/{total}")
    
    if successful == total:
        print("\n TOUS LES TESTS SONT RÉUSSIS !")
        print("Vous pouvez passer à l'étape 2.")
        return 0
    else:
        print("\n  Certains tests ont échoué.")
        print("Veuillez vérifier les services et réexécuter les tests.")
        return 1

if __name__ == "__main__":
    sys.exit(main())