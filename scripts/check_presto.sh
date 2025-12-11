#!/bin/bash

echo "🔍 Vérification de la configuration Presto..."
echo "=============================================="

# Vérifier les fichiers de configuration
echo "1. Vérification des fichiers de configuration:"
echo "----------------------------------------------"

if [ -f "./presto-config/config.properties" ]; then
    echo "✓ config.properties existe"
    
    # Vérifier node.environment
    if grep -q "node.environment" ./presto-config/config.properties; then
        echo "✓ node.environment est défini"
    else
        echo "✗ node.environment n'est pas défini"
        echo "  Ajoutez: node.environment=production"
    fi
    
    # Vérifier query.max-total-memory-per-node
    if grep -q "query.max-total-memory-per-node" ./presto-config/config.properties; then
        echo "✗ query.max-total-memory-per-node est présent (déprécié)"
        echo "  Supprimez cette ligne"
    fi
else
    echo "✗ config.properties n'existe pas"
fi

echo ""
echo "2. Vérification de node.properties:"
echo "-----------------------------------"

if [ -f "./presto-config/node.properties" ]; then
    echo "✓ node.properties existe"
    
    if grep -q "node.environment" ./presto-config/node.properties; then
        echo "✓ node.environment est défini dans node.properties"
    fi
    
    if grep -q "node.id" ./presto-config/node.properties; then
        echo "✓ node.id est défini"
    fi
    
    if grep -q "node.data-dir" ./presto-config/node.properties; then
        echo "✓ node.data-dir est défini"
    fi
else
    echo "✗ node.properties n'existe pas"
    echo "  Créez le fichier avec:"
    echo "  node.environment=production"
    echo "  node.id=presto-coordinator-1"
    echo "  node.data-dir=/var/trino/data"
fi

echo ""
echo "3. Structure des dossiers:"
echo "--------------------------"
ls -la ./presto-config/
echo ""
ls -la ./presto-config/catalog/

echo ""
echo "=============================================="
echo "Pour appliquer les corrections:"
echo ""
echo "1. Créez le fichier presto-config/node.properties:"
echo "   node.environment=production"
echo "   node.id=presto-coordinator-1"
echo "   node.data-dir=/var/trino/data"
echo ""
echo "2. Modifiez presto-config/config.properties:"
echo "   - Ajoutez: node.environment=production"
echo "   - Supprimez: query.max-total-memory-per-node"
echo ""
echo "3. Redémarrez: docker-compose restart presto-coordinator"