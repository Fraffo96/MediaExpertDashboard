/**
 * Marketing – Needstates by Category: spider/radar chart.
 * Select category + segment to see needstate profile (invented from segment descriptions).
 */
(function() {
  var spiderChart = null;

  function getParams() {
    var p = {};
    var form = document.getElementById('filter-form');
    if (form) {
      var fd = new FormData(form);
      for (var pair of fd.entries()) p[pair[0]] = pair[1];
    }
    if (!p.category_id) p.category_id = document.getElementById('f-category-id')?.value || '1';
    if (!p.segment_id) p.segment_id = document.getElementById('f-segment-id')?.value || '1';
    return new URLSearchParams(p).toString();
  }

  function apiUrl() {
    return '/api/marketing/needstates?' + getParams();
  }

  function readPrecalcPayload() {
    var pc = typeof window.MKT_NEEDSTATES_PRECALC !== 'undefined' ? window.MKT_NEEDSTATES_PRECALC : null;
    if (!pc || typeof pc !== 'object') return null;
    var cat = String(document.getElementById('f-category-id')?.value || '1');
    var seg = String(document.getElementById('f-segment-id')?.value || '1');
    return pc[cat + ':' + seg] || null;
  }

  function renderSpider(data) {
    var dimensions = data.dimensions || [];
    var scores = data.scores || [];
    var categoryAvg = data.scores_category_avg;
    if (!categoryAvg || !categoryAvg.length) {
      categoryAvg = dimensions.map(function() { return 0; });
    }
    var segmentName = data.segment_name || 'Segment';

    if (!dimensions.length || !scores.length) return;

    var wrap = document.getElementById('mkt-needstates-charts');
    var loading = document.getElementById('mkt-needstates-loading');
    var noData = document.getElementById('mkt-needstates-no-data');
    var canvas = document.getElementById('mkt-needstates-chart');
    var subtitle = document.getElementById('mkt-needstates-subtitle');

    if (!canvas || typeof Chart === 'undefined') return;

    if (subtitle) subtitle.textContent = 'Share of needstates for ' + segmentName + ' in this category (%)';

    var rMax = 100;

    if (!spiderChart) {
      var opt = {
        responsive: true,
        maintainAspectRatio: false,
        layout: { padding: { top: 8, right: 8, bottom: 8, left: 8 } },
        scales: {
          r: {
            min: 0,
            max: rMax,
            suggestedMax: rMax,
            ticks: {
              stepSize: 20,
              color: '#e8e8e8',
              font: { size: 13, weight: '500' },
              backdropColor: 'rgba(26,26,26,0.92)',
              backdropPadding: 4
            },
            grid: { color: 'rgba(255,255,255,0.1)' },
            pointLabels: {
              color: '#f0f0f0',
              font: { size: 14, weight: '500' },
              padding: 22,
              display: true
            }
          }
        },
        plugins: {
          legend: {
            display: true,
            position: 'bottom',
            labels: {
              color: '#ddd',
              boxWidth: 14,
              padding: 18,
              font: { size: 15, weight: '500' }
            }
          },
          tooltip: typeof CHART_TOOLTIP !== 'undefined' ? CHART_TOOLTIP : {
            callbacks: {
              label: function(ctx) {
                var v = ctx.raw;
                var pct = typeof v === 'number' ? Math.round(v * 10) / 10 : v;
                if (ctx.datasetIndex === 1) return 'Category average: ' + pct + '%';
                return ctx.dataset.label + ': ' + pct + '%';
              }
            }
          }
        }
      };
      spiderChart = new Chart(canvas.getContext('2d'), {
        type: 'radar',
        data: { labels: [], datasets: [] },
        options: opt
      });
    }

    if (spiderChart.options.scales && spiderChart.options.scales.r) {
      spiderChart.options.scales.r.max = rMax;
      spiderChart.options.scales.r.suggestedMax = rMax;
    }
    spiderChart.data.labels = dimensions;
    spiderChart.data.datasets = [
      {
        label: segmentName,
        data: scores,
        backgroundColor: 'rgba(255, 215, 0, 0.25)',
        borderColor: '#FFD700',
        borderWidth: 2,
        pointBackgroundColor: '#FFD700',
        pointBorderColor: '#fff',
        pointHoverBackgroundColor: '#fff',
        pointHoverBorderColor: '#FFD700',
        order: 1
      },
      {
        label: 'Category average',
        data: categoryAvg,
        backgroundColor: 'rgba(128, 128, 128, 0.08)',
        borderColor: 'rgba(160, 160, 160, 0.9)',
        borderWidth: 2.5,
        borderDash: [6, 4],
        pointRadius: 0,
        pointHoverRadius: 0,
        order: 2
      }
    ];
    spiderChart.update();

    var takeoutsEl = document.getElementById('mkt-needstates-takeouts');
    if (takeoutsEl) {
      var pairs = dimensions.map(function(d, i) { return { label: d, score: scores[i] || 0 }; });
      pairs.sort(function(a, b) { return b.score - a.score; });
      var top3 = pairs.slice(0, 3);
      var rows = top3.map(function(p, i) {
        var ix = Math.round(p.score * 10) / 10;
        return (
          '<div class="mkt-takeout-row">' +
            '<div class="mkt-takeout-main">' +
              '<span class="mkt-takeout-rank">' + (i + 1) + '</span>' +
              '<span class="mkt-takeout-label">' + p.label + '</span>' +
            '</div>' +
            '<span class="mkt-takeout-score">' + ix + '%</span>' +
          '</div>'
        );
      }).join('');
      var top1 = top3[0];
      var foot = top1
        ? ('<p class="mkt-takeout-foot">Lead with <strong>' + top1.label + '</strong> in this category.</p>')
        : '';
      takeoutsEl.innerHTML = '<div class="mkt-takeouts-inner">' + rows + foot + '</div>';
    }

    loading.classList.add('hidden');
    wrap.style.display = '';
    noData.style.display = 'none';
  }

  async function loadData() {
    var loading = document.getElementById('mkt-needstates-loading');
    var wrap = document.getElementById('mkt-needstates-charts');
    var noData = document.getElementById('mkt-needstates-no-data');

    var inlined = readPrecalcPayload();
    if (inlined && inlined.dimensions && inlined.dimensions.length && inlined.scores && inlined.scores.length) {
      renderSpider(inlined);
      return;
    }
    if (inlined && (!inlined.dimensions || !inlined.dimensions.length)) {
      if (loading) loading.classList.add('hidden');
      if (wrap) wrap.style.display = 'none';
      if (noData) noData.style.display = 'block';
      return;
    }

    if (loading) loading.classList.remove('hidden');
    if (wrap) wrap.style.display = 'none';
    if (noData) noData.style.display = 'none';

    try {
      var r = await fetch(apiUrl(), { credentials: 'include' });
      var data;
      try {
        data = await r.json();
      } catch (_) {
        data = { error: r.status === 401 ? 'Please sign in' : r.status === 403 ? 'Access denied' : 'Server error (' + r.status + ')' };
      }
      if (r.ok) {
        if (!data.dimensions || !data.scores) {
          loading.classList.add('hidden');
          noData.style.display = 'block';
        } else {
          renderSpider(data);
        }
      } else {
        if (typeof showError === 'function') showError(data.error || data.detail || 'Failed to load');
        loading.classList.add('hidden');
      }
    } catch (e) {
      if (typeof showError === 'function') showError(e.message || 'Network error');
      loading.classList.add('hidden');
    }
  }

  window.loadData = loadData;

  document.addEventListener('DOMContentLoaded', function() {
    loadData();
    var catSelect = document.getElementById('f-category-id');
    var segSelect = document.getElementById('f-segment-id');
    if (catSelect) catSelect.addEventListener('change', loadData);
    if (segSelect) segSelect.addEventListener('change', loadData);
  });
})();
