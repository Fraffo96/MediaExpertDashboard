/**
 * Market Intelligence – Peak events: bar chart (Samsung vs Category Avg), come grafico 4.
 */
(function() {
  function update(d) {
    var core = window.MICore;
    if (!core || !d) return;

    var pe = d.peak_events || [];
    var peakEl = document.getElementById('chart-peak');
    if (!peakEl || typeof Chart === 'undefined') return;

    if (window._miDebugPeak) {
      console.log('[MIChartsPeak] pe.length=' + pe.length + ' canvas=' + (peakEl ? peakEl.offsetWidth + 'x' + peakEl.offsetHeight : 'n/a'));
      if (pe.length) console.log('[MIChartsPeak] first:', pe[0], 'labels sample:', pe.slice(0,3).map(function(x){return x.peak_event;}));
    }

    if (pe.length) {
      var labels = pe.map(function(x) { return x.peak_event || 'Other'; });
      var brandData = pe.map(function(x) { return Number(x.brand_pct_of_annual) || 0; });
      var mediaData = pe.map(function(x) { return Number(x.media_pct_of_annual) || 0; });
      var bar = core.barData(labels, brandData, mediaData);
      if (window._miDebugPeak) {
        console.log('[MIChartsPeak] bar data:', { labels: labels.slice(0,3), brandData: brandData.slice(0,5), mediaData: mediaData.slice(0,5) });
      }
      var brandLabel = (typeof window.MI_BRAND_NAME !== 'undefined' && window.MI_BRAND_NAME) ? window.MI_BRAND_NAME : 'Your Brand';
      bar.datasets[0].label = brandLabel;
      bar.datasets[1].label = (d.competitor_name || d.second_series_label) || 'Category Avg';
      /* Stesso orientamento del grafico 4: barre verticali */
      var opts = Object.assign({}, core.MI_BAR_OPT || {});
      if (peakEl.chart) { peakEl.chart.destroy(); peakEl.chart = null; }
      peakEl.chart = new Chart(peakEl.getContext('2d'), {
        type: 'bar',
        data: bar,
        options: opts
      });
    } else if (peakEl.chart) {
      peakEl.chart.data.labels = [];
      peakEl.chart.data.datasets.forEach(function(ds) { ds.data = []; });
      peakEl.chart.update();
    }
  }

  window.MIChartsPeak = { update: update };
})();
