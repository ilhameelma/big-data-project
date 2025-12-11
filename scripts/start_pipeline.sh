#!/bin/bash

echo " Démarrage du pipeline Procurement..."
echo "========================================"

# Arrêter les services existants
echo " Arrêt des services existants..."
docker-compose down

# Démarrer les services
echo " Démarrage des services..."
docker-compose up -d

echo " Attente du démarrage des services..."
sleep 10

# Exécuter les tests de connectivité
echo " Exécution des tests de connectivité..."
docker exec etl-orchestrator python /scripts/test_connectivity.py

echo "======================================="
echo " Pipeline démarré avec succès!"
echo ""
echo " Interfaces Web:"
echo "  - HDFS: http://localhost:9870"
echo "  - Presto: http://localhost:8080"
echo "  - PostgreSQL: localhost:5432"