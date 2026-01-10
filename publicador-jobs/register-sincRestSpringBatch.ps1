
Write-Host "PRUEBAS en MODO ASINCRONO - Job Scheduler" -ForegroundColor Cyan
Write-Host "==========================================="

$baseUrl = "http://localhost:8080"

Write-Host "Lanzando Spring Batch JOB remoto de forma inmediata con orden de ejecución API Rest (modo sincrono)..." -ForegroundColor Yellow
$scheduleBody = @{
    jobName = "dailyTransactionBatchJob"
    businessDomain = "job-executor-service"
    urlMicroDestino = "http://localhost:8082"
    jobType = "REST_BASED"
    priority = "MEDIUM"
    parameters = @{
        "processDate" = "2025-12-04"
        "emailRecipient" = "admin@company.com"
        "customerFilter" = "*"
    }
} | ConvertTo-Json

try {
    $response = Invoke-RestMethod -Uri "$baseUrl/api/v1/jobs/inmediate-remote-sync" `
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

# 5. Esperar y verificar
Write-Host "`Esperando 1 segundo..." -ForegroundColor Yellow
Start-Sleep -Seconds 1

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