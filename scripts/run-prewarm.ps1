# Riscalda cache MI/BC/CLP/marketing (stesso codice di GET /internal/prewarm).
# Opzionale: $env:PREWARM_BRAND_IDS = "1,2,8" per brand espliciti senza Firestore.
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
$env:PYTHONPATH = (Get-Location).Path
python scripts/run_prewarm.py
