# Setup Metabase – passo-passo

## 1. Deploy Cloud Run

```bash
export PROJECT_ID=mediaexpertdashboard
./scripts/deploy-metabase.sh
```

Oppure con gcloud diretto:

```bash
gcloud run deploy metabase \
  --image=metabase/metabase:latest \
  --platform=managed \
  --region=europe-west1 \
  --project=mediaexpertdashboard \
  --allow-unauthenticated \
  --set-env-vars="MB_DB_FILE=/metabase-data/metabase.db" \
  --memory=1Gi --cpu=1 --port=3000
```

Apri l’URL restituito (es. `https://metabase-xxx.run.app`).

---

## 2. Primo avvio e configurazione iniziale

1. **Setup iniziale**: compila admin email e password.
2. **Aggiungi database**: Admin → Databases → Add database.
   - **Database type:** BigQuery.
   - **Display name:** Media Expert BigQuery.
   - **Project ID:** `mediaexpertdashboard`.
   - **Service Account JSON:** incolla il contenuto di `metabase-bq-key.json` (creato da `scripts/setup-gcp.sh`).
3. **Save** e **Sync database** (Sync now).

---

## 3. Row Level Security (filtro per brand)

### Creare i gruppi

1. **Admin** → **People** → **Groups** → **Create a group**.
   - `Sodastream_group`
   - `Nespresso_group`
   - `Samsung_group`

### Impostare i filtri sui dati

1. **Admin** → **Permissions** → **Data**.
2. Seleziona il database **Media Expert BigQuery**.
3. Per **Sodastream_group**:
   - Su **fact_sales_daily**: click su "Filter" (o "Limited") → **Add filter** → colonna `brand_name` = `Sodastream`.
   - Stesso filtro su **fact_promo_performance**.
4. Ripeti per **Nespresso_group** con `brand_name = 'Nespresso'` e **Samsung_group** con `brand_name = 'Samsung'`.

In questo modo ogni gruppo vede solo le righe del proprio brand.

---

## 4. Creazione utenti

1. **Admin** → **People** → **Add someone**.
2. Crea i tre utenti:

| Username (o email) | Password   | Gruppo          |
|-------------------|------------|------------------|
| Sodastream        | hcg2026    | Sodastream_group |
| Nespresso         | hcg2026    | Nespresso_group  |
| Samsung           | hcg2026    | Samsung_group    |

3. Assegna ogni utente al gruppo corrispondente (modifica utente → Groups).
4. **Cambio password al primo login**: in **Admin** → **Authentication** verifica se è disponibile l’opzione “Require password change on first login” (dipende dalla versione). In alternativa, comunica agli utenti di cambiare password al primo accesso.

---

## 5. Dashboard

- Crea una **Dashboard** e aggiungi le **Questions** (le query in `dashboard/bigquery_queries.sql` vanno salvate come Native query o come Saved questions).
- Imposta i **filtri** della dashboard: Data (period_start, period_end), Category, Promo type.
- Le domande useranno automaticamente i filtri RLS del gruppo dell’utente (solo il proprio brand).

---

## 6. Variabili nelle query

In Metabase, per le Native query puoi usare:

- `{{period_start}}` e `{{period_end}}` (tipo Date).
- `{{category_id}}` (tipo Number, opzionale).
- `{{promo_type}}` (tipo Text, opzionale).

Nelle query BigQuery sostituisci ad esempio:

- `PARSE_DATE('%Y-%m-%d', @period_start)` con `PARSE_DATE('%Y-%m-%d', '{{period_start}}')`  
  oppure passando date in formato stringa da Metabase.

Esempio per “Category Sales” in Metabase Native query:

```sql
SELECT
  f.date,
  c.category_name,
  SUM(f.gross_pln) AS gross_pln
FROM mart.fact_sales_daily f
JOIN mart.dim_category c ON c.category_id = f.category_id
WHERE f.date BETWEEN {{period_start}} AND {{period_end}}
GROUP BY f.date, c.category_name
ORDER BY f.date, gross_pln DESC
```

Con **Variables** in Metabase: `period_start` e `period_end` di tipo Date.
