# Script pour corriger les erreurs Presto
Write-Host "🔧 Correction des erreurs de configuration Presto" -ForegroundColor Cyan
Write-Host "======================================================"

# 1. Arrêter Presto
Write-Host "`n🛑 Arrêt de Presto..." -ForegroundColor Yellow
docker-compose stop presto-coordinator

# 2. Corriger config.properties
Write-Host "`n📝 Correction de config.properties..." -ForegroundColor Yellow
$configFile = ".\presto-config\config.properties"
if (Test-Path $configFile) {
    # Lire le contenu
    $content = Get-Content $configFile
    
    # Supprimer les propriétés problématiques
    $newContent = @()
    foreach ($line in $content) {
        if (-not ($line -match "http-server.authentication.type" -or 
                  $line -match "query.max-total-memory-per-node")) {
            $newContent += $line
        }
    }
    
    # Ajouter les propriétés correctes
    $newContent += "http-server.authentication.type=NONE"
    
    # Écrire le fichier
    $newContent | Out-File -FilePath $configFile -Encoding UTF8
    Write-Host "✓ config.properties corrigé" -ForegroundColor Green
}

# 3. Corriger node.properties
Write-Host "`n📝 Correction de node.properties..." -ForegroundColor Yellow
@"
node.environment=production
node.id=presto-coordinator-1
node.data-dir=/tmp/trino-data
"@ | Out-File -FilePath ".\presto-config\node.properties" -Encoding UTF8
Write-Host "✓ node.properties corrigé" -ForegroundColor Green

# 4. Créer une configuration minimale si nécessaire
Write-Host "`n📁 Vérification des fichiers de configuration..." -ForegroundColor Yellow

# Vérifier que tous les fichiers nécessaires existent
$requiredFiles = @(
    ".\presto-config\config.properties",
    ".\presto-config\node.properties",
    ".\presto-config\jvm.config",
    ".\presto-config\catalog\hdfs.properties",
    ".\presto-config\catalog\postgresql.properties"
)

foreach ($file in $requiredFiles) {
    if (-not (Test-Path $file)) {
        Write-Host "⚠️  Fichier manquant: $file" -ForegroundColor Yellow
        
        # Créer les fichiers manquants
        if ($file -match "hdfs.properties") {
            @"
connector.name=hive-hadoop2
hive.metastore.uri=thrift://localhost:9083
"@ | Out-File -FilePath $file -Encoding UTF8
        }
        elseif ($file -match "postgresql.properties") {
            @"
connector.name=postgresql
connection-url=jdbc:postgresql://postgres:5432/procurement
connection-user=postgres
connection-password=postgres
"@ | Out-File -FilePath $file -Encoding UTF8
        }
        elseif ($file -match "jvm.config") {
            @"
-server
-Xmx2G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+UseGCOverheadLimit
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
"@ | Out-File -FilePath $file -Encoding UTF8
        }
        
        Write-Host "✓ Fichier créé: $file" -ForegroundColor Green
    }
}

# 5. Vérifier le format des fichiers
Write-Host "`n🔍 Vérification du format des fichiers..." -ForegroundColor Yellow

# Vérifier node.id
$nodeProps = Get-Content ".\presto-config\node.properties"
if ($nodeProps -match 'node\.id=.*\${.*}.*') {
    Write-Host "❌ node.id contient des variables non résolues" -ForegroundColor Red
    (Get-Content ".\presto-config\node.properties") -replace '\${.*}', '1' | Out-File -FilePath ".\presto-config\node.properties" -Encoding UTF8
    Write-Host "✓ node.id corrigé" -ForegroundColor Green
}

# 6. Redémarrer Presto
Write-Host "`n🚀 Redémarrage de Presto..." -ForegroundColor Yellow
docker-compose up -d presto-coordinator

# 7. Attendre et vérifier
Write-Host "`n⏳ Attente du démarrage (20 secondes)..." -ForegroundColor Yellow
Start-Sleep -Seconds 20

# 8. Vérifier les logs
Write-Host "`n📋 Logs de Presto :" -ForegroundColor Yellow
docker logs presto-coordinator --tail 20

# 9. Tester
Write-Host "`n🌐 Test de connexion..." -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri "http://localhost:8080/v1/info" -TimeoutSec 10
    Write-Host "✅ SUCCÈS! Presto fonctionne!" -ForegroundColor Green
    Write-Host "   Version: $($response.nodeVersion.version)" -ForegroundColor White
    Write-Host "   Environnement: $($response.environment)" -ForegroundColor White
    Write-Host "   Interface Web: http://localhost:8080" -ForegroundColor Cyan
    
    # Tester une requête simple
    Write-Host "`n🔍 Test d'une requête SQL..." -ForegroundColor Yellow
    $query = @{
        query = "SELECT 1 as test"
    } | ConvertTo-Json
    
    try {
        $sqlResponse = Invoke-RestMethod -Uri "http://localhost:8080/v1/statement" -Method Post -Body $query -ContentType "application/json" -TimeoutSec 10
        Write-Host "✓ Requête SQL acceptée" -ForegroundColor Green
    } catch {
        Write-Host "⚠️  Requête SQL échouée (peut être normal)" -ForegroundColor Yellow
    }
    
} catch {
    Write-Host "❌ ÉCHEC: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "`n🔧 Dépannage avancé..." -ForegroundColor Yellow
    
    # Vérifier l'état du conteneur
    $containerStatus = docker inspect presto-coordinator --format "{{.State.Status}}"
    Write-Host "État du conteneur: $containerStatus" -ForegroundColor White
    
    # Vérifier les erreurs détaillées
    docker logs presto-coordinator --tail 50
}

Write-Host "`n======================================================"
Write-Host "Corrections appliquées!" -ForegroundColor Cyan