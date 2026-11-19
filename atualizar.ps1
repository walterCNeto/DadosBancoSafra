# Atualizacao diaria do safra-dados: ativa o ambiente conda "dadossafra" e roda `safra-dados atualizar`
# (so as fontes vencidas pelo calendario, depois a camada gold). O log detalhado de cada execucao fica em
# data\logs\atualizar_AAAAMMDD_HHMMSS.log; falhas ao ativar o ambiente vao para data\logs\agendador.log.
#
# Uso manual:   powershell -NoProfile -ExecutionPolicy Bypass -File .\atualizar.ps1
# Agendamento:  ver "Agendamento no Windows" no README.md (a tarefa NAO e registrada automaticamente).

$ErrorActionPreference = "Stop"
$raiz = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $raiz
$logs = Join-Path $raiz "data\logs"
New-Item -ItemType Directory -Force $logs | Out-Null
$agendador = Join-Path $logs "agendador.log"

function Escrever-Log([string]$mensagem) {
    "$(Get-Date -Format 's') $mensagem" | Add-Content -Path $agendador -Encoding utf8
}

try {
    # localiza o conda: CONDA_EXE, PATH ou instalacoes comuns
    $conda = $env:CONDA_EXE
    if (-not $conda) {
        $cmd = Get-Command conda -ErrorAction SilentlyContinue
        if ($cmd) { $conda = $cmd.Source }
    }
    if (-not $conda) {
        foreach ($base in @("anaconda3", "miniconda3", "miniforge3", "mambaforge")) {
            foreach ($raizConda in @($env:USERPROFILE, "C:\ProgramData")) {
                $candidato = Join-Path $raizConda "$base\Scripts\conda.exe"
                if (Test-Path $candidato) { $conda = $candidato; break }
            }
            if ($conda) { break }
        }
    }
    if (-not $conda) { throw "conda nao encontrado (defina CONDA_EXE ou adicione ao PATH)" }

    (& $conda "shell.powershell" "hook") | Out-String | Invoke-Expression
    conda activate dadossafra
    if ($env:CONDA_DEFAULT_ENV -ne "dadossafra") { throw "nao consegui ativar o ambiente conda 'dadossafra'" }
}
catch {
    Escrever-Log "FALHA ao preparar o ambiente: $_"
    exit 2
}

Escrever-Log "inicio"
& safra-dados atualizar
$codigo = $LASTEXITCODE
Escrever-Log "fim (codigo de saida $codigo)"
exit $codigo
