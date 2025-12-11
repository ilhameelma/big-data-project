# Script PowerShell pour initialiser Presto sur Windows
Write-Host "🔧 Initialisation de Presto pour Windows" -ForegroundColor Cyan
Write-Host "=========================================="

# 1. Arrêter les services
Write-Host "`n🛑 Arrêt des services..." -ForegroundColor Yellow
docker-compose down

# 2. Créer les répertoires locaux
Write-Host "`n📁 Création des répertoires..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path ".\presto-data"
New-Item -ItemType Directory -Force -Path ".\presto-config\catalog"
New-Item -ItemType Directory -Force -Path ".\scripts"
New-Item -ItemType Directory -Force -Path ".\data"
New-Item -ItemType Directory -Force -Path ".\logs"

# 3. Vérifier/Créer la configuration
Write-Host "`n📝 Configuration de Presto..." -ForegroundColor Yellow

# node.properties
if (-not (Test-Path ".\presto-config\node.properties")) {
    @"
node.environment=production
node.id=presto-coordinator-1
node.data-dir=/var/trino/data
"@ | Out-File -FilePath ".\presto-config\node.properties" -Encoding UTF8
    Write-Host "✓ node.properties créé" -ForegroundColor Green
}

# config.properties
if (-not (Test-Path ".\presto-config\config.properties")) {
    @"
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
query.max-memory=2GB
query.max-memory-per-node=1GB
discovery.uri=http://presto-coordinator:8080
node.environment=production
"@ | Out-File -FilePath ".\presto-config\config.properties" -Encoding UTF8
    Write-Host "✓ config.properties créé" -ForegroundColor Green
} else {
    # S'assurer que node.environment existe
    $content = Get-Content ".\presto-config\config.properties"
    if (-not ($content -match "node.environment")) {
        "node.environment=production" | Add-Content ".\presto-config\config.properties"
        Write-Host "✓ node.environment ajouté" -ForegroundColor Green
    }
}

# jvm.config
if (-not (Test-Path ".\presto-config\jvm.config")) {
    @"
-server
-Xmx2G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+UseGCOverheadLimit
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
"@ | Out-File -FilePath ".\presto-config\jvm.config" -Encoding UTF8
    Write-Host "✓ jvm.config créé" -ForegroundColor Green
}

# catalog/hdfs.properties
if (-not (Test-Path ".\presto-config\catalog\hdfs.properties")) {
    @"
connector.name=hive-hadoop2
hive.metastore.uri=thrift://hive-metastore:9083
hive.allow-drop-table=true
"@ | Out-File -FilePath ".\presto-config\catalog\hdfs.properties" -Encoding UTF8
    Write-Host "✓ hdfs.properties créé" -ForegroundColor Green
}

# catalog/postgresql.properties
if (-not (Test-Path ".\presto-config\catalog\postgresql.properties")) {
    @"
connector.name=postgresql
connection-url=jdbc:postgresql://postgres:5432/procurement
connection-user=postgres
connection-password=postgres
"@ | Out-File -FilePath ".\presto-config\catalog\postgresql.properties" -Encoding UTF8
    Write-Host "✓ postgresql.properties créé" -ForegroundColor Green
}

# 4. Démarrer Presto seul
Write-Host "`n🚀 Démarrage de Presto..." -ForegroundColor Yellow
docker-compose up -d presto-coordinator

# 5. Attendre et vérifier
Write-Host "`n⏳ Attente du démarrage (20 secondes)..." -ForegroundColor Yellow
Start-Sleep -Seconds 20

# 6. Vérifier les logs
Write-Host "`n📋 Logs de Presto :" -ForegroundColor Yellow
docker logs presto-coordinator --tail 20

# 7. Tester la connexion
Write-Host "`n🌐 Test de l'API REST..." -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8080/v1/info" -UseBasicParsing -TimeoutSec 10
    if ($response.StatusCode -eq 200) {
        Write-Host "✅ Presto est accessible !" -ForegroundColor Green
        Write-Host "   Interface Web: http://localhost:8080" -ForegroundColor Cyan
    }
} catch {
    Write-Host "❌ Impossible de se connecter à Presto" -ForegroundColor Red
    Write-Host "   Erreur: $_" -ForegroundColor Red
}

Write-Host "`n=========================================="
Write-Host "Initialisation terminée!" -ForegroundColor Cyan