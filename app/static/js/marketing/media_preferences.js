/**
 * Marketing – Media Preferences: social doughnut, comparison/AI/other bars, influence chart, take outs.
 */
(function() {
  var charts = {};
  var SOCIAL_COLORS = ['#FFD700', '#FFE44D', '#D4A012', '#94a3b8', '#64748b', '#475569', '#334155'];
  var state = { segmentId: '1', categoryId: '' };

  function destroyChart(key) {
    if (charts[key]) {
      charts[key].destroy();
      charts[key] = null;
    }
  }

  function destroyAll() {
    Object.keys(charts).forEach(destroyChart);
  }

  function apiUrl(segId, catId) {
    var q = 'segment_id=' + encodeURIComponent(segId || '1');
    if (catId) q += '&category_id=' + encodeURIComponent(catId);
    return '/api/marketing/media-preferences?' + q;
  }

  function hBarOptions(maxX) {
    return {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: function(ctx) {
              var lab = ctx.chart.data.labels[ctx.dataIndex] || '';
              return lab ? lab + ': ' + ctx.parsed.x + '%' : ctx.parsed.x + '%';
            }
          }
        }
      },
      scales: {
        x: {
          min: 0,
          max: maxX || 100,
          ticks: { color: '#aaa', font: { size: 10 } },
          grid: { color: 'rgba(255,255,255,0.06)' }
        },
        y: {
          ticks: { color: '#e0e0e0', font: { size: 10 } },
          grid: { display: false }
        }
      }
    };
  }

  function renderSocial(canvasId, items) {
    destroyChart('social');
    var canvas = document.getElementById(canvasId);
    if (!canvas || !items || !items.length) return;
    var sorted = items.slice().sort(function(a, b) { return (b.pct || 0) - (a.pct || 0); });
    var doughnutOpts = (typeof ChartStyles !== 'undefined' && ChartStyles.doughnut3DOptions)
      ? ChartStyles.doughnut3DOptions(55) : {};
    charts.social = new Chart(canvas.getContext('2d'), {
      type: 'doughnut',
      data: {
        labels: sorted.map(function(i) { return i.label; }),
        datasets: [{
          data: sorted.map(function(i) { return Number(i.pct) || 0; }),
          backgroundColor: sorted.map(function(_, i) { return SOCIAL_COLORS[i % SOCIAL_COLORS.length]; }),
          borderWidth: 0,
          hoverOffset: 6
        }]
      },
      options: Object.assign({}, doughnutOpts, {
        responsive: true,
        maintainAspectRatio: false,
        plugins: Object.assign({}, doughnutOpts.plugins || {}, {
          legend: { position: 'bottom', labels: { color: '#bbb', boxWidth: 10, font: { size: 10 } } }
        })
      })
    });
  }

  function renderHBar(key, canvasId, items, colorFn, maxX) {
    destroyChart(key);
    var canvas = document.getElementById(canvasId);
    if (!canvas || !items || !items.length) return;
    var sorted = items.slice().sort(function(a, b) { return (b.pct || 0) - (a.pct || 0); });
    charts[key] = new Chart(canvas.getContext('2d'), {
      type: 'bar',
      data: {
        labels: sorted.map(function(i) { return i.label; }),
        datasets: [{
          label: '%',
          data: sorted.map(function(i) { return Number(i.pct) || 0; }),
          backgroundColor: sorted.map(function(_, i) { return colorFn(i); }),
          borderWidth: 0
        }]
      },
      options: hBarOptions(maxX || 100)
    });
  }

  function renderInfluence(canvasId, infl) {
    destroyChart('influence');
    var canvas = document.getElementById(canvasId);
    if (!canvas || !infl) return;
    var g = Number(infl.get_influenced) || 0;
    var o = Number(infl.influencing_others) || 0;
    charts.influence = new Chart(canvas.getContext('2d'), {
      type: 'bar',
      data: {
        labels: ['Inspired by others', 'Influences peers'],
        datasets: [{
          data: [g, o],
          backgroundColor: ['rgba(255, 215, 0, 0.85)', 'rgba(148, 163, 184, 0.85)'],
          borderWidth: 0,
          borderRadius: 6
        }]
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: function(ctx) {
                return ctx.label + ': ' + ctx.parsed.x + '%';
              }
            }
          }
        },
        scales: {
          x: {
            min: 0,
            max: 100,
            ticks: { color: '#aaa', callback: function(v) { return v + '%'; } },
            grid: { color: 'rgba(255,255,255,0.06)' }
          },
          y: {
            ticks: { color: '#e8e8e8', font: { size: 11, weight: '500' } },
            grid: { display: false }
          }
        }
      }
    });
  }

  function fillTakeouts(seg) {
    var ul = document.getElementById('mkt-media-takeouts-list');
    if (!ul) return;
    var lines = seg.takeouts && seg.takeouts.length ? seg.takeouts : [];
    if (!lines.length && seg.summary) lines = [seg.summary];
    ul.innerHTML = lines.map(function(t) {
      return '<li>' + t.replace(/</g, '&lt;').replace(/>/g, '&gt;') + '</li>';
    }).join('');
  }

  function render(data) {
    var loading = document.getElementById('mkt-media-loading');
    var content = document.getElementById('mkt-media-content');
    var empty = document.getElementById('mkt-media-empty');
    var summaryEl = document.getElementById('mkt-media-summary');
    if (loading) loading.style.display = 'none';

    var seg = data && data.segment;
    if (!seg) {
      if (content) content.style.display = 'none';
      if (empty) empty.style.display = 'block';
      destroyAll();
      return;
    }
    if (empty) empty.style.display = 'none';
    if (content) content.style.display = '';
    if (summaryEl) {
      summaryEl.innerHTML = '<strong>' + (seg.name || 'Segment') + '.</strong> ' + (seg.summary || '');
    }

    fillTakeouts(seg);
    destroyAll();

    renderSocial('mkt-chart-social', seg.social || []);
    renderHBar('comparison', 'mkt-chart-comparison', seg.comparison_sites || [], function(i) {
      return 'rgba(255, 215, 0, ' + (0.45 + (i % 5) * 0.1) + ')';
    });
    renderHBar('ai', 'mkt-chart-ai', seg.ai_touchpoints || [], function(i) {
      return 'rgba(100, 200, 255, ' + (0.35 + (i % 4) * 0.12) + ')';
    });
    renderInfluence('mkt-chart-influence', seg.influence);
    renderHBar('other', 'mkt-chart-other', seg.other_channels || [], function(i) {
      return SOCIAL_COLORS[(i + 3) % SOCIAL_COLORS.length];
    });
  }

  function loadData() {
    var loading = document.getElementById('mkt-media-loading');
    var content = document.getElementById('mkt-media-content');
    var pc = typeof window.MKT_MEDIA_PRECALC !== 'undefined' ? window.MKT_MEDIA_PRECALC : null;
    var usePrecalc = !state.categoryId && pc && pc.segments && state.segmentId;
    if (usePrecalc) {
      var block = pc.segments[state.segmentId];
      if (loading) loading.style.display = 'none';
      render({
        segment_id: parseInt(state.segmentId, 10),
        segment: block || null
      });
      return;
    }

    if (loading) loading.style.display = '';
    if (content) content.style.display = 'none';

    fetch(apiUrl(state.segmentId, state.categoryId), { credentials: 'include' })
      .then(function(r) { return r.ok ? r.json() : {}; })
      .then(render)
      .catch(function() {
        if (loading) loading.style.display = 'none';
        var empty = document.getElementById('mkt-media-empty');
        if (empty) empty.style.display = 'block';
        destroyAll();
      });
  }

  function initSegmentDropdown() {
    var segs = typeof MKT_MEDIA_SEGMENTS !== 'undefined' ? MKT_MEDIA_SEGMENTS : [];
    var items = segs.map(function(s) {
      return { value: String(s.segment_id), label: s.segment_name || ('Segment ' + s.segment_id) };
    });
    if (!items.length) items = [{ value: '1', label: 'Liberals' }];
    state.segmentId = String(items[0].value);
    if (window.MIGenericDropdown && window.MIGenericDropdown.create) {
      window.MIGenericDropdown.create('mkt-media-segment', {
        items: items,
        initialValue: state.segmentId,
        minWidth: 240,
        onChange: function(v) {
          state.segmentId = v || '1';
          loadData();
        }
      });
    }
  }

  function initCategorySelect() {
    var sel = document.getElementById('mkt-media-category');
    if (!sel) return;
    state.categoryId = sel.value || '';
    sel.addEventListener('change', function() {
      state.categoryId = sel.value || '';
      loadData();
    });
  }

  document.addEventListener('DOMContentLoaded', function() {
    initCategorySelect();
    initSegmentDropdown();
    loadData();
  });
})();
