# test-poc-fixed.ps1
Write-Host "Registro de JOBs en JobRunr Scheduller - Batch Scheduler POC" -ForegroundColor Cyan
Write-Host "==========================================="

$baseUrl = "http://localhost:8080"

# 1. Health Check
Write-Host "`n1. Verificando salud del servicio..." -ForegroundColor Yellow
try {
    $health = Invoke-RestMethod -Uri "$baseUrl/api/v1/health" -Method GET
    Write-Host "   OK - $($health.status) - $($health.service)" -ForegroundColor Green
} catch {
    Write-Host "   ERROR: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# 2. Programar job recurrente
Write-Host "`n2. Programando job recurrente (cada 5 minutos)..." -ForegroundColor Yellow
$scheduleBody = @{
    jobName = "ResumenDiarioClientes"
    cronExpression = "0 */5 * * * *"
    processDate = "2024-01-15"
    sendEmail = $true
    emailRecipient = "admin@company.com"
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri "$baseUrl/api/v1/jobs/schedule" `
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
    processDate = "2024-01-15"
    sendEmail = $true
    emailRecipient = "team@company.com"
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