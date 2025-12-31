# Configurar variables
$baseUrl = "http://localhost:8080"
$jobId = "019b74be-70ef-7b2c-a2c3-3b8ccaa8ccf3" # Job Id del trabajo a eliminar
$endpoint = "/api/v1/deleteJobById/$jobId"

# Realizar la petición DELETE
try {
    $response = Invoke-RestMethod `
        -Uri "$baseUrl$endpoint" `
        -Method Delete `
        -ContentType "application/json"

    Write-Host "Job eliminado exitosamente: $jobName"
    Write-Host "Respuesta: $response"
}
catch {
    Write-Host "Error al eliminar el trabajo: $($_.Exception.Message)"

    # Mostrar más detalles del error si está disponible
    if ($_.Exception.Response) {
        $errorStream = $_.Exception.Response.GetResponseStream()
        $reader = New-Object System.IO.StreamReader($errorStream)
        $errorBody = $reader.ReadToEnd()
        Write-Host "Detalles del error: $errorBody"
    }
}