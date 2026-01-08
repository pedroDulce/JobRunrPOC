
Write-Host "PRUEBAS en MODO ASINCRONO - Job Scheduler" -ForegroundColor Cyan
Write-Host "==========================================="

$baseUrl = "http://localhost:8080"

Write-Host "Programando spring batch inmediato con enfoque asincrono en su ejecución..." -ForegroundColor Yellow
$scheduleBody = @{
    jobName = "springBatchSample"
    businessDomain = "job-executor-service"
    heartBeatLapse = "5"
    jobType = "BATCH_PROCESSING"
    priority = "MEDIUM"
    parameters = @{
        "processDate" = "2025-12-22"
        "emailRecipient" = "admin@company.com"
        "customerFilter" = "*"
    }
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri "$baseUrl/api/v1/jobs/inmediate-remote-async" `
        -Method POST `
        -Headers @{"Content-Type" = "application/json"} `
        -Body $scheduleBody

    Write-Host "  OK - Job de lanzamiento inmediato: $($response.jobId)" -ForegroundColor Green

} catch {
    Write-Host "  ERROR: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.ErrorDetails.Message) {
        Write-Host "  Response: $($_.ErrorDetails.Message)" -ForegroundColor Red
    }
}


# 4. Dashboard
Write-Host "URLs del sistema..." -ForegroundColor Yellow
Write-Host "  Dashboard JobRunr: http://localhost:8000/dashboard" -ForegroundColor Cyan
Write-Host "  API Docs: http://localhost:8080/swagger-ui.html" -ForegroundColor Cyan

# 5. Esperar y verificar
Write-Host "`Esperando 2 segundos..." -ForegroundColor Yellow
Start-Sleep -Seconds 2

Write-Host "===========================================" -ForegroundColor Cyan
Write-Host "REGISTRO COMPLETADO" -ForegroundColor Green
Write-Host ""
Write-Host "Pasos siguientes:" -ForegroundColor Yellow
Write-Host "   1. Abre http://localhost:8000 para ver el dashboard" -ForegroundColor White
Write-Host "   2. Verifica que los jobs aparezcan" -ForegroundColor White
Write-Host "   3. Revisa los logs de Spring Boot" -ForegroundColor White
Write-Host ""
Write-Host "Presiona cualquier tecla para salir..." -ForegroundColor Yellow
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")