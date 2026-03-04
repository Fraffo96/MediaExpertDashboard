# Architettura: nostra dashboard e database su GCP

La **dashboard è la nostra applicazione** (FastAPI + template HTML/JS), non un tool esterno. I **database** sono su Google Cloud e l’app si connette a essi.

## Componenti

| Componente | Ruolo |
|------------|--------|
| **App dashboard** | FastAPI in `app/`: legge da BigQuery, espone API e pagina con grafici/tabelle. Puoi personalizzare template in `app/templates/` e query in `app/db/bigquery_client.py`. |
| **BigQuery** | Dataset `raw` e `mart` nel progetto GCP. Tabelle e dati per vendite, promo, categorie. L’app usa il client BigQuery con le credenziali del servizio (Cloud Run o locale). |
| **Cloud SQL (opzionale)** | Se in futuro vuoi usare anche i dati Postgres (schema in `migrations.sql`, `mart.sql`) su GCP, puoi creare un’istanza Cloud SQL e collegarla all’app. Per ora l’app usa solo BigQuery. |
| **Cloud Run** | Hosting dell’app: ogni push su `main` fa build + deploy della nostra app (non più Metabase). |

## Database su GCP

### BigQuery (obbligatorio per la dashboard)

1. **Crea i dataset** (se non esistono):
   ```powershell
   bq mk --dataset --location=EU mediaexpertdashboard:raw
   bq mk --dataset --location=EU mediaexpertdashboard:mart
   ```
   Oppure usa `.\scripts\setup-gcp.ps1` che crea anche SA e dataset.

2. **Schema e dati**: nella **Console BigQuery** (progetto `mediaexpertdashboard`) apri `bigquery/schema_and_seed.sql` ed esegui tutto (crea tabelle dim_*, fact_* e inserisce i dati 2025).

3. **Connessione dall’app**: l’app usa `google-cloud-bigquery` e il **progetto** impostato con la variabile d’ambiente `GCP_PROJECT_ID` (su Cloud Run è impostata a `mediaexpertdashboard`). Su Cloud Run le credenziali sono quelle del service account del servizio (default: Compute Engine default SA con permessi sul progetto). Nessun file JSON da montare se usi il default.

### Cloud SQL / Postgres (opzionale)

Se vuoi usare anche i dati del retailer da Postgres su GCP:

1. Crea un’istanza **Cloud SQL for PostgreSQL** (es. db-f1-micro) nella stessa region (europe-west1).
2. Crea un database (es. `retailer_pl`), esegui `migrations.sql` e `mart.sql`, poi eventualmente `seed.py` verso l’IP/connection name dell’istanza.
3. Nell’app puoi aggiungere un client Postgres (es. `asyncpg` o `psycopg2`) e leggere le variabili di connessione da Secret Manager o env (es. `DATABASE_URL`). Le query in `dashboard_queries.sql` sono per Postgres; puoi riutilizzarle nell’app o esporre endpoint aggiuntivi.

Per ora la **dashboard usa solo BigQuery** (dataset `mart`).

## Personalizzare la dashboard

- **Grafici e layout**: modifica `app/templates/dashboard.html` e `app/templates/base.html`, e gli stili in `app/static/css/style.css`.
- **Nuove query / nuove viste**: aggiungi funzioni in `app/db/bigquery_client.py` e nuovi endpoint in `app/main.py`; poi chiamali dalla pagina (JavaScript) o aggiungi nuove sezioni nel template.
- **Filtri aggiuntivi**: estendi il form in `dashboard.html` e i parametri delle API (es. brand, tipo promo).

## Deploy (Cloud Build)

Il file `cloudbuild.yaml` ora:

1. Fa la build dell’immagine Docker della **nostra app** (Dockerfile in root).
2. Fa push su Container Registry.
3. Esegue il deploy su Cloud Run del servizio **dashboard** (porta 8080, env `GCP_PROJECT_ID=mediaexpertdashboard`).

Non viene più deployato Metabase. Il trigger su push su `main` riguarda solo questa app.
