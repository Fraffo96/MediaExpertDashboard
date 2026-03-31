/**
 * Check Live Promo – bar chart (top 15, expandable) + donuts always visible (aggregate or by SKU).
 * Metric dropdown: Units / Value (PLN). Filtri promo e timeframe applicati a tutte le API.
 */
(function() {
  var DEFAULT_BARS = 15;
  var BAR_HEIGHT = 28;
  var YELLOW = '#FFD700';
  var YELLOW_DIM = 'rgba(255, 215, 0, 0.6)';
  var COLORS = ['#FFD700', '#FFE44D', '#B89900', '#E0E0E0', '#888', '#64748b'];

  var _allRows = [];
  var _topProducts = [];
  var _metric = 'value';
  var _selectedProductId = null;
  var _expanded = false;

  function getMetricField() { return _metric === 'units' ? 'units' : 'gross_pln'; }
  function getMetricLabel() { return _metric === 'units' ? 'Units' : 'Value (PLN)'; }
  function fmtVal(v, isUnits) {
    if (v == null) return '—';
    var n = Number(v).toLocaleString('en-US', { maximumFractionDigits: 0 });
    return isUnits ? n : n + ' PLN';
  }

  function getBarOpts() {
    var base = (typeof ChartStyles !== 'undefined' && ChartStyles.barGradientOptions)
      ? ChartStyles.barGradientOptions('y') : {};
    var isUnits = _metric === 'units';
    return Object.assign({}, base, {
      responsive: true,
      maintainAspectRatio: false,
      indexAxis: 'y',
      plugins: Object.assign({}, base.plugins || {}, {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: function(ctx) {
              var v = ctx.raw;
              if (v == null) return '';
              return (ctx.dataset.label || '') + ': ' + fmtVal(v, isUnits);
            }
          }
        }
      }),
      scales: {
        x: Object.assign({}, base.scales && base.scales.x, {
          grid: { color: 'rgba(42,42,42,0.6)' },
          ticks: {
            color: '#999',
            callback: function(v) {
              return v >= 1e6 ? (v/1e6).toFixed(1) + 'M' : v >= 1000 ? (v/1000).toFixed(1) + 'k' : v;
            }
          }
        }),
        y: {
          type: 'category',
          grid: { display: false },
          ticks: { color: '#b0b0b0', font: { size: 12 }, maxRotation: 0, autoSkip: false }
        }
      },
      onClick: function(evt, elements) {
        if (elements.length === 0) return;
        var idx = elements[0].index;
        var productId = _topProducts[idx] && _topProducts[idx].product_id;
        if (productId != null) {
          _selectedProductId = productId;
          renderDonuts(productId);
          updateDonutsUI(true);
        }
      }
    });
  }

  function createDonutChart(canvasId, labels, data, isUnits) {
    var canvas = document.getElementById(canvasId);
    if (!canvas || typeof Chart === 'undefined') return;
    if (canvas.donutChart) {
      canvas.donutChart.destroy();
      canvas.donutChart = null;
    }
    if (!labels || labels.length === 0) {
      labels = ['No data'];
      data = [1];
    }
    var doughnutOpts = (typeof ChartStyles !== 'undefined' && ChartStyles.doughnut3DOptions)
      ? ChartStyles.doughnut3DOptions(60) : {};
    var colors = labels.map(function(_, i) { return COLORS[i % COLORS.length]; });
    canvas.donutChart = new Chart(canvas.getContext('2d'), {
      type: 'doughnut',
      data: {
        labels: labels,
        datasets: [{ data: data, backgroundColor: colors, borderWidth: 0, hoverOffset: 4 }]
      },
      options: Object.assign({}, doughnutOpts, {
        responsive: true,
        maintainAspectRatio: false,
        plugins: Object.assign({}, doughnutOpts.plugins || {}, {
          legend: { display: true, position: 'bottom' },
          tooltip: {
            callbacks: {
              label: function(ctx) {
                var v = ctx.raw;
                var total = ctx.dataset.data.reduce(function(a, b) { return a + b; }, 0);
                var pct = total > 0 ? (100 * v / total).toFixed(1) : '0';
                return ctx.label + ': ' + fmtVal(v, isUnits) + ' (' + pct + '%)';
              }
            }
          }
        })
      })
    });
  }

  function getChannelData(rows, field) {
    var byCh = {};
    (rows || []).forEach(function(r) {
      var ch = r.channel || 'All';
      if (ch === '') ch = 'All';
      if (!byCh[ch]) byCh[ch] = 0;
      byCh[ch] += Number(r[field]) || 0;
    });
    var lbls = Object.keys(byCh).sort();
    var dat = lbls.map(function(l) { return byCh[l]; });
    return { labels: lbls, data: dat };
  }

  function renderDonuts(productId, segmentPromise) {
    var ddMetric = document.getElementById('clp-deepdive-metric');
    var m = (ddMetric && ddMetric.value) || _metric;
    var field = m === 'units' ? 'units' : 'gross_pln';
    var isUnits = m === 'units';

    if (_allRows.length === 0) {
      createDonutChart('clp-donut-chart', ['No data'], [1], isUnits);
      createDonutChart('clp-segment-donut-chart', ['No data'], [1], isUnits);
      return;
    }

    if (productId != null) {
      var rows = _allRows.filter(function(r) { return r.product_id === productId && r.channel && r.channel !== ''; });
      if (rows.length === 0) rows = _allRows.filter(function(r) { return r.product_id === productId; });
      var chData = getChannelData(rows, field);
      createDonutChart('clp-donut-chart', chData.labels, chData.data, isUnits);

      var core = window.CLPCore;
      var url = core && core.buildSegmentUrl ? core.buildSegmentUrl(productId) : '/api/check-live-promo/segment-breakdown?product_id=' + productId;
      fetch(url, { credentials: 'include' })
        .then(function(r) {
          if (!r.ok) return r.text().then(function(t) { throw new Error(r.status + ': ' + (t || r.statusText)); });
          return r.json();
        })
        .then(function(d) {
          if (d && d.error) {
            createDonutChart('clp-segment-donut-chart', [d.error], [1], isUnits);
            return;
          }
          var segRows = (d && d.rows) ? d.rows : [];
          var lbls = segRows.map(function(r) { return (r.segment_name != null ? r.segment_name : 'Segment ' + (r.segment_id || '')); });
          var dat = segRows.map(function(r) { return Number(r[field]) || 0; });
          createDonutChart('clp-segment-donut-chart', lbls, dat, isUnits);
        })
        .catch(function(err) {
          console.warn('Segment breakdown fetch failed:', err);
          createDonutChart('clp-segment-donut-chart', ['Error loading'], [1], isUnits);
        });
    } else {
      var allRows = _allRows.filter(function(r) { return r.channel && r.channel !== ''; });
      if (allRows.length === 0) allRows = _allRows;
      var aggCh = getChannelData(allRows, field);
      createDonutChart('clp-donut-chart', aggCh.labels, aggCh.data, isUnits);

      function applySegmentData(segRows) {
        var lbls = (segRows || []).map(function(r) { return (r.segment_name != null ? r.segment_name : 'Segment ' + (r.segment_id || '')); });
        var dat = (segRows || []).map(function(r) { return Number(r[field]) || 0; });
        createDonutChart('clp-segment-donut-chart', lbls, dat, isUnits);
      }
      if (segmentPromise && typeof segmentPromise.then === 'function') {
        segmentPromise.then(applySegmentData);
      } else {
        var core = window.CLPCore;
        var url = core && core.buildSegmentUrl ? core.buildSegmentUrl(null) : '/api/check-live-promo/segment-breakdown';
        fetch(url, { credentials: 'include' })
          .then(function(r) {
            if (!r.ok) return r.text().then(function(t) { throw new Error(r.status + ': ' + (t || r.statusText)); });
            return r.json();
          })
          .then(function(d) {
            if (d && d.error) {
              createDonutChart('clp-segment-donut-chart', [d.error], [1], isUnits);
              return;
            }
            applySegmentData((d && d.rows) ? d.rows : []);
          })
          .catch(function(err) {
            console.warn('Segment breakdown fetch failed:', err);
            createDonutChart('clp-segment-donut-chart', ['Error loading'], [1], isUnits);
          });
      }
    }
  }

  function updateDonutsUI(selected) {
    var titleEl = document.getElementById('clp-donuts-title');
    var closeBtn = document.getElementById('clp-deepdive-close');
    var productName = '';
    if (selected && _selectedProductId != null) {
      var r = _allRows.find(function(p) { return p.product_id === _selectedProductId; });
      productName = (r && r.product_name) ? r.product_name + ' ' : '';
    }
    if (titleEl) titleEl.textContent = selected ? (productName + '(SKU ' + _selectedProductId + ') – by channel & segment') : 'All products – by channel & segment';
    if (closeBtn) closeBtn.style.display = selected ? '' : 'none';
  }

  function renderBarChart() {
    var canvas = document.getElementById('clp-chart');
    if (!canvas || typeof Chart === 'undefined') return;

    var field = getMetricField();
    var barRows = _allRows.filter(function(r) {
      var ch = (r.channel || '').toString();
      return ch === '';
    });
    if (barRows.length === 0) barRows = _allRows;

    var byProduct = {};
    barRows.forEach(function(r) {
      var key = r.product_id;
      if (!byProduct[key]) {
        byProduct[key] = { product_id: key, product_name: r.product_name };
        byProduct[key][field] = 0;
      }
      byProduct[key][field] += Number(r[field]) || 0;
    });
    var arr = Object.values(byProduct).sort(function(a, b) {
      return (b[field] || 0) - (a[field] || 0);
    });
    var totalCount = arr.length;
    var limit = _expanded ? arr.length : Math.min(DEFAULT_BARS, arr.length);
    _topProducts = arr.slice(0, limit);
    var labels = _topProducts.map(function(r) {
      var name = (r.product_name || '—').substring(0, 45);
      return name + (name.length >= 45 ? '…' : '');
    });
    var data = _topProducts.map(function(r) { return Number(r[field]) || 0; });

    var barCountEl = document.getElementById('clp-bar-count');
    var totalCountEl = document.getElementById('clp-total-count');
    if (barCountEl) barCountEl.textContent = limit;
    if (totalCountEl) totalCountEl.textContent = totalCount;

    var showAllBtn = document.getElementById('clp-show-all');
    var scrollWrap = document.getElementById('clp-chart-scroll');
    var chartWrap = document.getElementById('clp-chart-wrap');
    if (showAllBtn) {
      showAllBtn.style.display = totalCount > DEFAULT_BARS && !_expanded ? '' : 'none';
      showAllBtn.textContent = 'Show all (' + totalCount + ')';
    }
    if (scrollWrap && chartWrap) {
      var h = Math.max(360, _topProducts.length * BAR_HEIGHT);
      chartWrap.style.height = h + 'px';
      scrollWrap.style.maxHeight = _expanded ? (h + 40) + 'px' : '400px';
    }

    if (!canvas.chart) {
      canvas.chart = new Chart(canvas.getContext('2d'), {
        type: 'bar',
        data: {
          labels: labels,
          datasets: [{
            label: getMetricLabel(),
            data: data,
            backgroundColor: YELLOW_DIM,
            borderColor: YELLOW,
            borderWidth: 1,
            borderRadius: 4,
            barThickness: 'flex',
            maxBarThickness: 26
          }]
        },
        options: getBarOpts()
      });
    } else {
      canvas.chart.data.labels = labels;
      canvas.chart.data.datasets[0].data = data;
      canvas.chart.data.datasets[0].label = getMetricLabel();
      canvas.chart.options.plugins.tooltip.callbacks.label = getBarOpts().plugins.tooltip.callbacks.label;
      canvas.chart.update();
    }
  }

  function update(rows, segmentPromise) {
    var canvas = document.getElementById('clp-chart');
    if (!canvas || typeof Chart === 'undefined') return;

    _allRows = rows || [];
    _expanded = false;
    _selectedProductId = null;
    renderBarChart();
    renderDonuts(null, segmentPromise);
    updateDonutsUI(false);

    var ddMetric = document.getElementById('clp-deepdive-metric');
    var mainMetric = document.getElementById('clp-metric');
    if (ddMetric && mainMetric) ddMetric.value = mainMetric.value;

    var closeBtn = document.getElementById('clp-deepdive-close');
    if (closeBtn) {
      closeBtn.onclick = function() {
        _selectedProductId = null;
        renderDonuts(null, null);
        updateDonutsUI(false);
      };
    }

    var showAllBtn = document.getElementById('clp-show-all');
    if (showAllBtn) {
      showAllBtn.onclick = function() {
        _expanded = true;
        renderBarChart();
        showAllBtn.style.display = 'none';
      };
    }

    if (ddMetric) {
      ddMetric.onchange = function() {
        renderDonuts(_selectedProductId);
      };
    }
  }

  function setMetric(m) {
    _metric = m === 'units' ? 'units' : 'value';
    renderBarChart();
    renderDonuts(_selectedProductId);
  }

  window.CLPChart = { update: update, setMetric: setMetric };
})();
