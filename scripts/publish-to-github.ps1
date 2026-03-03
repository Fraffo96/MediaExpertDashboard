# Publish progetto su GitHub: login (una volta) + crea repo + push
# Esegui da PowerShell nella cartella del progetto

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

$REPO_NAME = "MediaExpertDashboard"

# 1. Login gh (solo se non loggato)
$ErrorActionPreference = "Continue"
gh auth status 2>$null
$needLogin = $LASTEXITCODE -ne 0
$ErrorActionPreference = "Stop"
if ($needLogin) {
    Write-Host "Apri il browser e completa il login GitHub (una tantum)..." -ForegroundColor Yellow
    gh auth login --web --hostname github.com
    if ($LASTEXITCODE -ne 0) { exit 1 }
}

# 2. Crea repo su GitHub e push (source=cartella corrente, remote=origin)
Write-Host "Creazione repo $REPO_NAME e push..." -ForegroundColor Cyan
gh repo create $REPO_NAME --public --source=. --remote=origin --push

if ($LASTEXITCODE -eq 0) {
    Write-Host "Fatto. Repo: https://github.com/$(gh repo view --json nameWithOwner -q .nameWithOwner)" -ForegroundColor Green
} else {
    # Se la repo esiste gia', aggiungi remote e push
    Write-Host "Repo gia' esistente? Aggiungo remote e push..." -ForegroundColor Yellow
    git remote remove origin 2>$null
    $owner = (gh api user -q .login 2>$null)
    if ($owner) {
        git remote add origin "https://github.com/${owner}/${REPO_NAME}.git"
        git branch -M main
        git push -u origin main
    }
}
