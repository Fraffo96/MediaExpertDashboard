# Rigenerazione completa: seed BigQuery -> derive -> precalc (no cache: usare POST /api/admin/clear-cache).
# Prerequisiti: PYTHONPATH = root repo, credenziali GCP (Application Default Credentials), progetto/dataset configurati.
$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
$env:PYTHONPATH = $root

Write-Host "==> generate_seed_data.py (dim_product_generated.sql)"
python scripts/generate_seed_data.py

Write-Host "==> run_bigquery_schema.py (schema + seed + derive)"
python scripts/run_bigquery_schema.py

Write-Host "==> refresh_precalc_tables.py"
python scripts/refresh_precalc_tables.py

Write-Host "Fatto. Svuotare cache: POST /api/admin/clear-cache (admin) oppure restart servizio."
