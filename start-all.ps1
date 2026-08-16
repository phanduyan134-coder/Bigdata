$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
$sparkSubmit = Join-Path $projectRoot ".venv\Scripts\spark-submit.cmd"

Set-Location -LiteralPath $projectRoot
Write-Host "Starting Kafka..." -ForegroundColor Cyan
docker compose up -d

$topicReady = $false
for ($attempt = 1; $attempt -le 12; $attempt++) {
    try {
        docker exec air-quality-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic air-quality --partitions 1 --replication-factor 1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Kafka topic is not ready."
        }
        $topicReady = $true
        break
    } catch {
        Start-Sleep -Seconds 5
    }
}
if (-not $topicReady) {
    throw "Kafka did not become ready within 60 seconds. Check Docker Desktop."
}

function Open-ProjectTerminal([string]$title, [string]$command) {
    Start-Process powershell.exe -ArgumentList @(
        "-NoExit",
        "-ExecutionPolicy", "Bypass",
        "-Command",
        "`$Host.UI.RawUI.WindowTitle = '$title'; Set-Location -LiteralPath '$projectRoot'; $command"
    ) | Out-Null
}

Open-ProjectTerminal "Spark ingestion" "& '$sparkSubmit' --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 streaming\spark_streaming.py"
Start-Sleep -Seconds 5
Open-ProjectTerminal "Open-Meteo producer" "& '$python' streaming\live_producer.py"
Open-ProjectTerminal "24-hour prediction stream" "& '$python' streaming\spark_multi_horizon_prediction_stream.py"
Open-ProjectTerminal "Streamlit dashboard" "& '$python' -m streamlit run app.py --server.address 0.0.0.0 --server.port 8501"

Write-Host "System started. Dashboard: http://localhost:8501" -ForegroundColor Green
