# Configurar variables
$baseUrl = "http://localhost:8080"
$jobId = "xxxxxxx" # Job ID del trabajo a eliminar
$endpoint = "/api/v1/info/$jobId"

# Realizar la petición DELETE
try {
    $response = Invoke-RestMethod `
        -Uri "$baseUrl$endpoint" `
        -Method Get `
        -ContentType "application/json"

    Write-Host "Buscando Job: $jobId"
    Write-Host "Respuesta: $response"
}
catch {
    Write-Host "Error al obtener info del trabajo: $($_.Exception.Message)"

    # Mostrar más detalles del error si está disponible
    if ($_.Exception.Response) {
        $errorStream = $_.Exception.Response.GetResponseStream()
        $reader = New-Object System.IO.StreamReader($errorStream)
        $errorBody = $reader.ReadToEnd()
        Write-Host "Detalles del error: $errorBody"
    }
}
