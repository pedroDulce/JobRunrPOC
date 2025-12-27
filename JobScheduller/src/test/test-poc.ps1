# test-poc-fixed.ps1
Write-Host "PRUEBAS CORREGIDAS - Batch Scheduler POC" -ForegroundColor Cyan
Write-Host "==========================================="

$baseUrl = "http://localhost:8080"

# Eliminar un job específico
Invoke-RestMethod -X DELETE http://localhost:8080/api/jobs/550e8400-e29b-41d4-a716-446655440000

# Eliminar múltiples jobs
#Invoke-RestMethod -X DELETE http://localhost:8080/api/jobs -H "Content-Type: application/json" -d '["id1", "id2", "id3"]'

# Eliminar todos los jobs de tipo "reportGeneration"
#Invoke-RestMethod -X DELETE http://localhost:8080/api/jobs/type/reportGeneration

# Cancelar un job programado
#Invoke-RestMethod -X POST http://localhost:8080/api/jobs/550e8400-e29b-41d4-a716-446655440000/cancel

# Eliminar jobs fallidos
#Invoke-RestMethod -X DELETE http://localhost:8080/api/jobs/state/FAILED

# 1. Health Check
Write-Host "`n0. Verificando salud del servicio..." -ForegroundColor Yellow
try {
    $health = Invoke-RestMethod -Uri "$baseUrl/api/v1/health" -Method GET
    Write-Host "   OK - $($health.status) - $($health.service)" -ForegroundColor Green
} catch {
    Write-Host "   ERROR: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}


Write-Host "`n1. Programando job con enfoque sincrono en su ejecucion..." -ForegroundColor Yellow
$scheduleBody = @{
    jobName = "ResumenDiarioClientesSync"
    cronExpression = "0 */8 * * * *"
    processDate = "2024-01-15"
    metadata = @{
        "emailRecipient" = "admin@company.com"
        "customerFilter" = "*"
        "url" = "http://localhost:8082/api/jobs/execute-sync"
    }
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri "$baseUrl/api/v1/jobs/schedule-remote-sync" `
        -Method POST `
        -Headers @{"Content-Type" = "application/json"} `
        -Body $scheduleBody

    Write-Host "   OK - Job programado: $($response.jobId)" -ForegroundColor Green
    Write-Host "   Cron: $($response.cronExpression)" -ForegroundColor Green

} catch {
    Write-Host "   ERROR: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.ErrorDetails.Message) {
        Write-Host "   Response: $($_.ErrorDetails.Message)" -ForegroundColor Red
    }
}



Write-Host "`n2. Programando job con enfoque asincrono en su ejecucion..." -ForegroundColor Yellow
$scheduleBody = @{
    jobName = "ResumenDiarioClientesAsync"
    cronExpression = "0 */5 * * * *"
    processDate = "2024-01-15"
    metadata = @{
        "emailRecipient" = "admin@company.com"
        "customerFilter" = "*"
    }
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri "$baseUrl/api/v1/jobs/schedule-remote-async" `
        -Method POST `
        -Headers @{"Content-Type" = "application/json"} `
        -Body $scheduleBody

    Write-Host "   OK - Job programado: $($response.jobId)" -ForegroundColor Green
    Write-Host "   Cron: $($response.cronExpression)" -ForegroundColor Green

} catch {
    Write-Host "   ERROR: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.ErrorDetails.Message) {
        Write-Host "   Response: $($_.ErrorDetails.Message)" -ForegroundColor Red
    }
}


# 3. Ejecutar job inmediatamente
Write-Host "`n3. Ejecutando job inmediatamente..." -ForegroundColor Yellow
$immediateBody = @{
    jobName = "ResumenInmediato"
    processDate = "2024-01-15"
    metadata = @{
        "emailRecipient" = "admin@company.com"
        "customerFilter" = "*"
    }
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri "$baseUrl/api/v1/jobs/execute-now" `
        -Method POST `
        -Headers @{"Content-Type" = "application/json"} `
        -Body $immediateBody

    Write-Host "   OK - Job encolado: $($response.jobId)" -ForegroundColor Green
    Write-Host "   Hora: $($response.executionTime)" -ForegroundColor Green

} catch {
    Write-Host "   ERROR: $($_.Exception.Message)" -ForegroundColor Red
}

# 4. Dashboard
Write-Host "`n4. URLs del sistema..." -ForegroundColor Yellow
Write-Host "   Dashboard JobRunr: http://localhost:8000" -ForegroundColor Cyan
Write-Host "   API Docs: http://localhost:8080/swagger-ui.html" -ForegroundColor Cyan

# 5. Esperar y verificar
Write-Host "`n5. Esperando 3 segundos..." -ForegroundColor Yellow
Start-Sleep -Seconds 3

Write-Host "`n===========================================" -ForegroundColor Cyan
Write-Host "PRUEBAS COMPLETADAS" -ForegroundColor Green
Write-Host ""
Write-Host "Pasos siguientes:" -ForegroundColor Yellow
Write-Host "   1. Abre http://localhost:8000 para ver el dashboard" -ForegroundColor White
Write-Host "   2. Verifica que los jobs aparezcan" -ForegroundColor White
Write-Host "   3. Revisa los logs de Spring Boot" -ForegroundColor White
Write-Host ""
Write-Host "Presiona cualquier tecla para salir..." -ForegroundColor Yellow
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")