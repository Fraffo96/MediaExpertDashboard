/**
 * Market Intelligence – core: params, buildUrl, barData helper, chart options.
 * Depends on: Chart, BAR_OPT, DOUGHNUT_OPT, CHART_TOOLTIP, showLoadingLight, showError (from base).
 */
(function() {
  var MI_TOOLTIP = typeof CHART_TOOLTIP !== 'undefined' ? Object.assign({}, CHART_TOOLTIP, {
    callbacks: {
      label: function(ctx) {
        var v = ctx.raw;
        if (v == null || (typeof v === 'number' && isNaN(v))) return (ctx.dataset.label || '') + ': —';
        var num = Number(v);
        var label = (ctx.dataset.label || '').trim();
        var s = label ? label + ': ' : '';
        var isPct = label && (label.indexOf('%') !== -1 || label.indexOf('Share') !== -1);
        if (isPct) return s + num.toFixed(1) + '%';
        var n = num.toLocaleString('en-US', { maximumFractionDigits: 0 });
        return s + n + (label && label.indexOf('PLN') !== -1 ? ' PLN' : (label && (label.indexOf('Units') !== -1 || label === 'Units') ? ' units' : ''));
      },
      afterBody: function(tooltipItems) {
        if (tooltipItems.length < 2) return '';
        var total = 0;
        tooltipItems.forEach(function(t) { total += Number(t.raw) || 0; });
        if (total <= 0) return '';
        var lines = [];
        tooltipItems.forEach(function(t) {
          var pct = total > 0 ? (100 * (Number(t.raw) || 0) / total).toFixed(1) : '0';
          lines.push(t.dataset.label + ': ' + pct + '% of total');
        });
        return ['', 'Share:'].concat(lines);
      }
    }
  }) : {};

  var MI_BAR_OPT = (typeof ChartStyles !== 'undefined' && ChartStyles.barGradientOptions)
    ? ChartStyles.barGradientOptions()
    : Object.assign({}, typeof BAR_OPT !== 'undefined' ? BAR_OPT : {}, {
        plugins: {
          legend: { display: true, position: 'top', labels: { color: '#E0E0E0', font: { size: 12 }, padding: 10, usePointStyle: true } },
          tooltip: MI_TOOLTIP
        },
        scales: {
          x: {
            grid: { color: 'rgba(42,42,42,0.6)', drawBorder: false },
            ticks: { color: '#999', font: { size: 11 }, maxRotation: 45, minRotation: 0, maxTicksLimit: 12, autoSkip: true }
          },
          y: { grid: { color: 'rgba(42,42,42,0.6)', drawBorder: false }, ticks: { color: '#999', font: { size: 11 }, callback: function(v) { return v >= 1000000 ? (v/1000000).toFixed(1) + 'M' : v >= 1000 ? (v/1000).toFixed(1) + 'k' : v; } } }
        },
        layout: { padding: { top: 8, bottom: 12, left: 8, right: 8 } }
      });

  var MI_DOUGHNUT_OPT = Object.assign({}, typeof DOUGHNUT_OPT !== 'undefined' ? DOUGHNUT_OPT : {}, {
    plugins: {
      legend: { display: true, position: 'bottom', labels: { color: '#E0E0E0', font: { size: 15 }, padding: 16, usePointStyle: true } },
      tooltip: typeof CHART_TOOLTIP !== 'undefined' ? Object.assign({}, CHART_TOOLTIP, {
        callbacks: {
          label: function(ctx) {
            var v = ctx.raw;
            if (v == null || (typeof v === 'number' && isNaN(v))) return (ctx.label || '') + ': —';
            var total = ctx.dataset.data.reduce(function(a,b) { return a + (Number(b) || 0); }, 0);
            var pct = total > 0 ? (100 * (Number(v) || 0) / total).toFixed(1) : '0';
            return (ctx.label || '') + ': ' + pct + '%';
          }
        }
      }) : {}
    },
    cutout: '60%',
    layout: { padding: { top: 12, bottom: 12 } }
  });

  var MI_PIE_OPT = {
    type: 'pie',
    plugins: {
      legend: { display: false },
      tooltip: { enabled: true }
    },
    layout: { padding: { top: 16, bottom: 16 } }
  };

  function getParams() {
    var psEl = document.getElementById('mi-ps');
    var peEl = document.getElementById('mi-pe');
    var catEl = document.getElementById('mi-category');
    var subEl = document.getElementById('mi-subcategory');
    return {
      period_start: (psEl && psEl.value) || '2023-01-01',
      period_end: (peEl && peEl.value) || '2025-12-31',
      category_id: (catEl && catEl.value) || undefined,
      subcategory_id: (subEl && subEl.value) || undefined
    };
  }

  function buildUrl() {
    var p = getParams();
    var q = [];
    if (p.period_start) q.push('period_start=' + encodeURIComponent(p.period_start));
    if (p.period_end) q.push('period_end=' + encodeURIComponent(p.period_end));
    return '/api/market-intelligence/base?' + q.join('&');
  }

  function barData(labels, brandData, mediaData) {
    var brandLabel = (typeof window.MI_BRAND_NAME !== 'undefined' && window.MI_BRAND_NAME) ? window.MI_BRAND_NAME : 'Your Brand';
    if (typeof ChartStyles !== 'undefined' && ChartStyles.barGradientDatasets) {
      var d = ChartStyles.barGradientDatasets(labels, brandData, mediaData, brandLabel, 'Category Avg');
      return d;
    }
    return {
      labels: labels,
      datasets: [
        { label: brandLabel, data: brandData, backgroundColor: 'rgba(255,215,0,0.8)', borderColor: '#FFD700', borderWidth: 1 },
        { label: 'Category Avg', data: mediaData, backgroundColor: 'rgba(100,116,139,0.6)', borderColor: '#64748b', borderWidth: 1 }
      ]
    };
  }

  window.MICore = {
    getParams: getParams,
    buildUrl: buildUrl,
    barData: barData,
    MI_BAR_OPT: MI_BAR_OPT,
    MI_DOUGHNUT_OPT: MI_DOUGHNUT_OPT,
    MI_PIE_OPT: MI_PIE_OPT
  };
})();
