#!/bin/bash
# Script d'orchestration du pipeline quotidien

# Configuration
DATE=${1:-$(date -d "yesterday" '+%Y-%m-%d')}
LOG_DIR="/app/logs"
OUTPUT_DIR="/app/output"
LOG_FILE="$LOG_DIR/pipeline_${DATE}.log"

echo "=========================================="
echo "🛒 PIPELINE DE RÉAPPROVISIONNEMENT"
echo "Date: $DATE"
echo "=========================================="

# Créer les répertoires
mkdir -p "$LOG_DIR"
mkdir -p "$OUTPUT_DIR/supplier_orders/$DATE"
mkdir -p "$OUTPUT_DIR/reports/$DATE"

# Journalisation
exec > >(tee -a "$LOG_FILE") 2>&1
echo "Début: $(date '+%Y-%m-%d %H:%M:%S')"

# 1. Vérifier les prérequis
echo "Étape 1: Vérification des prérequis..."
python /app/scripts/test_pipeline.py

# 2. Exécuter le pipeline
echo ""
echo "Étape 2: Exécution du pipeline..."
python /app/scripts/procurement_pipeline.py "$DATE"

# 3. Copier les résultats dans HDFS
echo ""
echo "Étape 3: Archivage dans HDFS..."
hdfs dfs -mkdir -p "/output/supplier_orders/$DATE"
hdfs dfs -put "/app/output/supplier_orders/$DATE/"* "/output/supplier_orders/$DATE/" 2>/dev/null || true

hdfs dfs -mkdir -p "/output/reports/$DATE"
hdfs dfs -put "/app/output/reports/$DATE/"* "/output/reports/$DATE/" 2>/dev/null || true

# 4. Nettoyage (garder seulement 7 jours)
echo ""
echo "Étape 4: Nettoyage..."
find "/app/output/supplier_orders" -type d -mtime +7 -exec rm -rf {} \; 2>/dev/null || true
find "/app/output/reports" -type d -mtime +7 -exec rm -rf {} \; 2>/dev/null || true

echo ""
echo "=========================================="
echo "✅ PIPELINE TERMINÉ AVEC SUCCÈS"
echo "Date: $DATE"
echo "Logs: $LOG_FILE"
echo "Commandes: /app/output/supplier_orders/$DATE/"
echo "Rapports: /app/output/reports/$DATE/"
echo "Fin: $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="