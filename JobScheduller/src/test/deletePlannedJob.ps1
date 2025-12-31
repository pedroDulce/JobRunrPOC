# Configurar variables
$baseUrl = "http://localhost:8080"
$jobId = "019b7432-43f9-70e1-8490-813a4eadd474" # Job Id del trabajo a eliminar
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