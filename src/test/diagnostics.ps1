# diagnose-jobrunr.ps1
Write-Host "DIAGNOSTICO JOBBRUNR DASHBOARD" -ForegroundColor Cyan
Write-Host "================================="

# 1. Verificar puerto 8000
Write-Host "`n1. Verificando puerto 8000..." -ForegroundColor Yellow
try {
    $portCheck = Test-NetConnection -ComputerName localhost -Port 8000
    if ($portCheck.TcpTestSucceeded) {
        Write-Host "   OK - Puerto 8000 está abierto" -ForegroundColor Green
    } else {
        Write-Host "   ERROR - Puerto 8000 no responde" -ForegroundColor Red
    }
} catch {
    Write-Host "   ERROR - No se pudo verificar puerto: $_" -ForegroundColor Red
}

# 2. Verificar logs de JobRunr en la app
Write-Host "`n2. Buscando logs de JobRunr en Spring Boot..." -ForegroundColor Yellow
Write-Host "   En la consola de Spring Boot, busca:" -ForegroundColor White
Write-Host "   - 'JobRunr' (cualquier mensaje)" -ForegroundColor Gray
Write-Host "   - 'dashboard'" -ForegroundColor Gray
Write-Host "   - 'port 8000'" -ForegroundColor Gray

# 3. Verificar tablas de JobRunr en PostgreSQL
Write-Host "`n3. Verificando tablas JobRunr en base de datos..." -ForegroundColor Yellow
Write-Host "   Ejecuta en PostgreSQL:" -ForegroundColor White
Write-Host "   \c batch_scheduler" -ForegroundColor Gray
Write-Host "   \dt jobrunr_*" -ForegroundColor Gray
Write-Host "   Deberías ver 4 tablas: jobrunr_jobs, jobrunr_recurring_jobs, etc." -ForegroundColor Gray

# 4. Configuración alternativa - puerto diferente
Write-Host "`n4. Prueba alternativa: Cambiar a puerto 8001..." -ForegroundColor Yellow
Write-Host "   En application.yml cambia:" -ForegroundColor White
Write-Host "   dashboard:" -ForegroundColor Gray
Write-Host "     enabled: true" -ForegroundColor Gray
Write-Host "     port: 8001" -ForegroundColor Gray
Write-Host "   Luego accede a: http://localhost:8001" -ForegroundColor Cyan

Write-Host "`n=================================" -ForegroundColor Cyan
Write-Host "PASOS PARA SOLUCIONAR:" -ForegroundColor Yellow
Write-Host "1. Revisa application.yml (la configuración de arriba)" -ForegroundColor White
Write-Host "2. Busca 'JobRunr' en los logs de inicio de Spring Boot" -ForegroundColor White
Write-Host "3. Prueba con puerto 8001 si 8000 está ocupado" -ForegroundColor White
Write-Host "4. Comparte los logs COMPLETOS de inicio de Spring Boot" -ForegroundColor White

Write-Host "`nPresiona cualquier tecla para continuar..." -ForegroundColor Yellow
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")