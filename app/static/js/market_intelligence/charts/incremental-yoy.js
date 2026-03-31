/**
 * Market Intelligence – Average Incremental YoY: vendite e vendite promo per anno.
 * Bar chart con due serie: Vendite (total_gross), Vendite promo (promo_gross).
 */
(function() {
  function update(d) {
    var core = window.MICore;
    if (!core || !d) return;

    var rows = d.incremental_yoy || [];
    var el = document.getElementById('chart-incremental-yoy');
    if (!el || typeof Chart === 'undefined') return;

    if (rows.length) {
      var labels = rows.map(function(x) { return String(x.year || ''); });
      var totalData = rows.map(function(x) { return Number(x.total_gross) || 0; });
      var promoData = rows.map(function(x) { return Number(x.promo_gross) || 0; });
      var bar = core.barData(labels, totalData, promoData);
      bar.datasets[0].label = 'Sales (PLN)';
      bar.datasets[1].label = 'Promo sales (PLN)';
      bar.datasets[0].backgroundColor = 'rgba(255,215,0,0.8)';
      bar.datasets[0].borderColor = '#FFD700';
      bar.datasets[1].backgroundColor = 'rgba(100,116,139,0.6)';
      bar.datasets[1].borderColor = '#64748b';
      if (el.chart) { el.chart.destroy(); el.chart = null; }
      el.chart = new Chart(el.getContext('2d'), {
        type: 'bar',
        data: bar,
        options: core.MI_BAR_OPT || {}
      });
    } else if (el.chart) {
      el.chart.data.labels = [];
      el.chart.data.datasets.forEach(function(ds) { ds.data = []; });
      el.chart.update();
    }
  }

  window.MIChartsIncrementalYoy = { update: update };
})();
