param(
    [Parameter(Mandatory=$true)]
    [ValidateSet('init','start','stop','recover','status','validate')]
    [string]$Action
)
$ErrorActionPreference = 'Stop'
$envFile = Join-Path $PSScriptRoot '.env'
if ($Action -eq 'init') {
    if (Test-Path -LiteralPath $envFile) { throw 'infra/.env already exists; preserved.' }
    $lines = foreach ($name in @('POSTGRES_PASSWORD','APP_DB_PASSWORD','PREFECT_DB_PASSWORD','GRAFANA_PASSWORD')) {
        $bytes = New-Object byte[] 32
        $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
        try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
        $value = [BitConverter]::ToString($bytes).Replace('-','').ToLowerInvariant()
        "$name=$value"
    }
    [IO.File]::WriteAllLines($envFile, $lines, (New-Object Text.UTF8Encoding $false))
    Write-Output 'Created ignored infra/.env with random local credentials.'
    exit 0
}
if (!(Test-Path -LiteralPath $envFile)) { throw 'Run ./infra/manage.ps1 init first.' }
$composeArgs = @('compose','--project-name','a-share-step03','--env-file',$envFile,'-f',(Join-Path $PSScriptRoot 'compose.yaml'))
switch ($Action) {
    'validate' { $composeArgs += @('config','--quiet') }
    'start' { $composeArgs += @('up','-d','--build','--wait','--wait-timeout','240') }
    'recover' { $composeArgs += @('up','-d','--wait','--wait-timeout','240') }
    'stop' { $composeArgs += @('--profile','*','stop') }
    'status' { $composeArgs += @('--profile','*','ps','--all') }
}
& docker @composeArgs
if ($LASTEXITCODE -ne 0) { throw "Docker command failed with exit code $LASTEXITCODE" }


