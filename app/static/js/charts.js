/**
 * Grafici Chart.js: init, aggiornamento dati, resize.
 * Dipende da Chart (globale da CDN).
 */

const CHART_OPT = { responsive: true, maintainAspectRatio: false };

function applyChartDefaults() {
  if (typeof Chart === "undefined") return;
  Chart.defaults.color = "#94a3b8";
  Chart.defaults.borderColor = "#334155";
  if (Chart.defaults.scales && Chart.defaults.scales.grid) {
    Chart.defaults.scales.grid.color = "#334155";
  } else if (Chart.defaults.grid) {
    Chart.defaults.grid.color = "#334155";
  }
}

/**
 * Crea i 5 grafici (promo share, YoY, peak, ROI, vendite per categoria) se non esistono già.
 */
export function initCharts() {
  applyChartDefaults();
  const c1 = document.getElementById("chartPromoShare");
  const c2 = document.getElementById("chartYoy");
  const c3 = document.getElementById("chartPeak");
  const c4 = document.getElementById("chartRoi");
  const c5 = document.getElementById("chartCategorySales");

  if (c1 && !window.chartPromoShare) {
    var ds1 = { data: [0, 0], backgroundColor: ["#334155", "#FFD700"], borderWidth: 0, spacing: 3, borderRadius: 2 };
    if (typeof ChartStyles !== "undefined" && ChartStyles.doughnut3DColors) {
      ds1.backgroundColor = ChartStyles.doughnut3DColors(2, 1);
    }
    window.chartPromoShare = new Chart(c1, {
      type: "doughnut",
      data: { labels: ["Non promo", "Promo"], datasets: [ds1] },
      options: (typeof ChartStyles !== "undefined" && ChartStyles.doughnut3DOptions) ? ChartStyles.doughnut3DOptions(65) : CHART_OPT,
    });
  }
  if (c2 && !window.chartYoy) {
    window.chartYoy = new Chart(c2, {
      type: "bar",
      data: {
        labels: [],
        datasets: [{ label: "Sales (PLN)", data: [], backgroundColor: "rgba(255,215,0,0.8)", borderRadius: 8 }],
      },
      options: (typeof ChartStyles !== "undefined" && ChartStyles.barGradientOptions) ? ChartStyles.barGradientOptions() : CHART_OPT,
    });
  }
  if (c3 && !window.chartPeak) {
    window.chartPeak = new Chart(c3, {
      type: "bar",
      data: {
        labels: [],
        datasets: [{ label: "Gross PLN", data: [], backgroundColor: "rgba(255,179,71,0.8)", borderRadius: 8 }],
      },
      options: (typeof ChartStyles !== "undefined" && ChartStyles.barGradientOptions) ? ChartStyles.barGradientOptions() : CHART_OPT,
    });
  }
  if (c4 && !window.chartRoi) {
    window.chartRoi = new Chart(c4, {
      type: "bar",
      data: {
        labels: [],
        datasets: [{ label: "ROI", data: [], backgroundColor: "#fbbf24" }],
      },
      options: CHART_OPT,
    });
  }
  if (c5 && !window.chartCategorySales) {
    var hbarOpt = (typeof ChartStyles !== "undefined" && ChartStyles.barGradientOptions) ? ChartStyles.barGradientOptions("y") : Object.assign({}, CHART_OPT, { indexAxis: "y" });
    window.chartCategorySales = new Chart(c5, {
      type: "bar",
      data: {
        labels: [],
        datasets: [{ label: "Gross PLN", data: [], backgroundColor: "rgba(255,215,0,0.75)", borderRadius: 8 }],
      },
      options: hbarOpt,
    });
  }
}

/**
 * Aggiorna i grafici con i dati dalla API.
 * @param {{ promo_share: Array, yoy: Array, peak_events: Array, promo_roi: Array }} data
 */
export function updateCharts(data) {
  if (!data) return;
  const promoShare = data.promo_share || [];
  const yoy = data.yoy || [];
  const peak = data.peak_events || [];
  const roi = data.promo_roi || [];

  const p = promoShare[0];
  const promoEl = document.getElementById("promo-share-value");
  if (promoEl) promoEl.textContent = p ? `${p.promo_share_pct}% sales in promo` : "--";

  function chartData(ch) {
    if (!ch) return null;
    return ch.data ?? (ch.config && ch.config.data) ?? null;
  }
  function safeUpdate(ch, d) {
    if (!ch || !d || !Array.isArray(d.datasets) || !d.datasets[0]) return;
    ch.update("none");
  }
  const ch1 = window.chartPromoShare;
  const d1 = chartData(ch1);
  if (d1 && d1.datasets && d1.datasets[0]) {
    d1.datasets[0].data = [
      p ? Number(p.total_gross) - Number(p.promo_gross) : 0,
      p ? Number(p.promo_gross) : 0,
    ];
    safeUpdate(ch1, d1);
  }
  const ch2 = window.chartYoy;
  const d2 = chartData(ch2);
  if (d2 && d2.datasets && d2.datasets[0]) {
    d2.labels = yoy.map((r) => String(r.year));
    d2.datasets[0].data = yoy.map((r) => Number(r.total_gross));
    safeUpdate(ch2, d2);
  }
  const ch3 = window.chartPeak;
  const d3 = chartData(ch3);
  if (d3 && d3.datasets && d3.datasets[0]) {
    d3.labels = peak.map((r) => r.peak_event);
    d3.datasets[0].data = peak.map((r) => Number(r.gross_pln));
    safeUpdate(ch3, d3);
  }
  const ch4 = window.chartRoi;
  const d4 = chartData(ch4);
  if (d4 && d4.datasets && d4.datasets[0]) {
    d4.labels = roi.map((r) => r.promo_name);
    d4.datasets[0].data = roi.map((r) => Number(r.roi));
    safeUpdate(ch4, d4);
  }

  const categorySales = data.category_sales || [];
  const byCategory = {};
  categorySales.forEach((r) => {
    const name = r.category_name || "Altro";
    byCategory[name] = (byCategory[name] || 0) + Number(r.gross_pln || 0);
  });
  const catLabels = Object.keys(byCategory).sort((a, b) => byCategory[b] - byCategory[a]).slice(0, 15);
  const catValues = catLabels.map((l) => byCategory[l]);
  const ch5 = window.chartCategorySales;
  const d5 = chartData(ch5);
  if (d5 && d5.datasets && d5.datasets[0]) {
    d5.labels = catLabels;
    d5.datasets[0].data = catValues;
    safeUpdate(ch5, d5);
  }
}

/**
 * Ridimensiona tutti i grafici (dopo resize griglia).
 */
export function resizeCharts() {
  [window.chartPromoShare, window.chartYoy, window.chartPeak, window.chartRoi, window.chartCategorySales].forEach((ch) => {
    if (ch && ch.resize) ch.resize();
  });
}
