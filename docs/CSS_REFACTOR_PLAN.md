# Piano refactoring CSS – Market Intelligence / Brand Comparison

> Obiettivo: layout coerente, uso corretto dello spazio, niente overlapping né spazi vuoti inutili.

---

## 1. Problemi attuali

| Problema | Dove | Causa probabile |
|----------|------|-----------------|
| **Spazi vuoti** tra chart e insights | `mi-col-center` ↔ `mi-col-right` | Grid `1fr minmax(200px, 300px)` + chart con `max-width: 540px` centrato → spazio morto |
| **Label vs valore** con gap eccessivo | `mi-info-row` | `grid-template-columns: 1fr auto` con label lunghe → valore spinto troppo a destra |
| **Wrapping** "vs previous year. Investigate causes." | `mi-ranking`, `mi-info-value.mi-wrap` | Testo lungo senza `max-width` → wrap strano |
| **Filtri** troppo stretti o troppo larghi | `mi-pie-controls` labels | `flex: 1 1 140px` + `min-width`/`max-width` contrastanti |
| **Overlapping** (se presente) | Vari | `overflow: visible`, `min-width: 0` mancante, z-index |
| **Line-height** inconsistente | `mi-info-panel` | `gap: .4rem` fisso, nessuna `line-height` uniforme |

---

## 2. Principi guida

1. **Sistema a griglia** – Una griglia base per tutte le sezioni MI/BC.
2. **Proporzioni fisse** – Chart e insights con rapporti prevedibili (es. 60/40 o 55/45).
3. **Spaziatura coerente** – Usare `--space-*` (es. `--space-xs`, `--space-sm`, `--space-md`).
4. **Contenimento** – `overflow: hidden` dove serve, `min-width: 0` sui flex/grid children.
5. **Responsive** – Breakpoint chiaro (es. 900px) per stack verticale.

---

## 3. Struttura proposta

### 3.1 Variabili CSS (in `:root`)

```css
:root {
  /* Esistenti... */
  --space-xs: 0.25rem;
  --space-sm: 0.5rem;
  --space-md: 0.75rem;
  --space-lg: 1rem;
  --space-xl: 1.25rem;
  --space-2xl: 1.5rem;
  --mi-chart-insights-ratio: 1.4;  /* chart più largo degli insights */
  --mi-section-gap: var(--space-2xl);
}
```

### 3.2 Layout sezione (`.mi-layout-3col`)

**Attuale:**
```
[filters (full width)]
[chart (1fr)     | insights (200-300px)]
```

**Proposto:**
```
[filters (full width, gap uniforme)]
[chart + insights in flex/grid bilanciato, senza spazio morto]
```

- **Filtri**: una riga, `display: flex`, `gap: var(--space-md)`, `flex-wrap: wrap`. Ogni label+dropdown con `min-width` ragionevole (es. 140px) e `flex: 0 1 auto` per evitare stretch eccessivo.
- **Chart + insights**: 
  - `display: grid`
  - `grid-template-columns: minmax(280px, 1fr) minmax(200px, 320px)` 
  - `gap: var(--space-xl)`
  - Chart: `min-width: 0` per evitare overflow, `max-width: 100%`
  - Insights: `min-width: 0`, `max-width: 320px` per non diventare troppo larghi su schermi grandi

### 3.3 Pannello insights (`.mi-info-panel`)

**Problema**: `grid-template-columns: 1fr auto` crea gap enormi con label lunghe.

**Soluzione**:
- Opzione A: `grid-template-columns: minmax(0, max-content) 1fr` – label occupa lo spazio necessario, valore il resto.
- Opzione B: `grid-template-columns: 1fr 1fr` con `max-width` sulla prima colonna (es. 180px) – label troncata se serve.
- Opzione C (consigliata): layout a due colonne con `minmax(8ch, 140px) minmax(0, 1fr)` – label limitata, valore flessibile.

Per il testo `.mi-ranking` che fa wrap:
- `max-width: 100%`
- `line-height: 1.5`
- `overflow-wrap: break-word`

### 3.4 Filtri (`.mi-pie-controls` e simili)

- Rimuovere `flex: 1 1 140px` che crea comportamenti imprevedibili.
- Usare `display: flex`, `flex-wrap: wrap`, `gap: var(--space-md)`.
- Ogni item: `flex: 0 0 auto` con `min-width: 120px`, `max-width: 200px`.
- Allineamento: `align-items: center`.

### 3.5 Chart card (`.mi-charts .chart-card`)

- `padding` uniforme: `var(--space-lg)`.
- `min-height` ragionevole per evitare collasso.
- `overflow: hidden` sul container per evitare overlap.

---

## 4. Ordine di intervento

| # | Intervento | File | Impatto |
|---|------------|------|---------|
| 1 | Aggiungere variabili `--space-*` | style.css | Base |
| 2 | Refactor `.mi-info-panel` e `.mi-info-row` | style.css | Insights allineati |
| 3 | Refactor `.mi-layout-3col` (grid, gap, proporzioni) | style.css | Layout generale |
| 4 | Refactor filtri (`.mi-pie-controls` etc.) | style.css | Filtri coerenti |
| 5 | Fix `.mi-ranking` e `.mi-info-value.mi-wrap` | style.css | Testo che fa wrap |
| 6 | Controllo `overflow` e `min-width: 0` | style.css | Evitare overlap |
| 7 | Test responsive (breakpoint 900px) | style.css | Mobile |

---

## 5. Snippet CSS di riferimento

### 5.1 mi-info-panel (nuovo)

```css
.mi-info-panel {
  display: flex;
  flex-direction: column;
  gap: var(--space-sm, 0.5rem);
  padding: var(--space-md, 0.75rem) var(--space-lg, 1rem);
  min-width: 0;
  max-width: 320px;
  font-size: 0.9rem;
  line-height: 1.5;
}
.mi-info-panel .mi-info-row {
  display: grid;
  grid-template-columns: minmax(0, max-content) minmax(0, 1fr);
  gap: var(--space-md, 0.75rem);
  align-items: baseline;
  min-width: 0;
}
.mi-info-panel .mi-info-label {
  color: var(--text-muted);
  font-weight: 500;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 180px;
}
.mi-info-panel .mi-info-value {
  font-weight: 600;
  color: var(--white);
  text-align: right;
  font-variant-numeric: tabular-nums;
  justify-self: end;
}
.mi-info-panel .mi-ranking {
  margin-top: var(--space-sm);
  padding-top: var(--space-sm);
  border-top: 1px solid var(--border);
  line-height: 1.5;
  max-width: 100%;
  overflow-wrap: break-word;
}
```

### 5.2 mi-layout-3col (nuovo)

```css
.mi-layout-3col {
  display: grid;
  grid-template-columns: 1fr minmax(200px, 320px);
  grid-template-rows: auto 1fr;
  grid-template-areas: "filters filters" "chart insights";
  gap: var(--space-xl, 1.25rem);
  align-items: start;
  min-width: 0;
}
.mi-layout-3col .mi-col-center {
  min-width: 0;  /* critico per evitare overflow */
}
.mi-layout-3col .mi-col-center .chart-wrap {
  width: 100%;
  max-width: 100%;
  margin: 0;
}
.mi-layout-3col .mi-col-right {
  min-width: 0;
  align-self: stretch;
}
```

---

## 6. Checklist pre-merge

- [ ] Nessun overlap visibile a 1920px, 1440px, 1024px
- [ ] Insights leggibili, label e valore allineati
- [ ] Filtri in una riga quando c’è spazio, wrap pulito quando no
- [ ] Chart e insights usano bene lo spazio orizzontale
- [ ] Breakpoint 900px: stack verticale funzionante
- [ ] Stesso comportamento su Brand Comparison

---

## 7. Note

- Non modificare l’HTML se non strettamente necessario: il refactor deve essere solo CSS.
- I dropdown custom (`.mi-cat-dropdown-wrap`) hanno già `min-width`; verificare che non confliggano con il nuovo layout.
- Il `mi-summary-row` (Competitor, Revenue, Avg Discount) è separato: va valutato in un secondo momento.
