/**
 * Marketing Overview – segment donut (stesso pattern dropdown di Market Intelligence).
 */
(function() {
  var COLORS = ['#FFD700', '#FFE44D', '#B89900', '#E0E0E0', '#888', '#64748b'];
  var chart = null;
  var state = {
    year: '',
    channel: '',
    category_id: '',
    subcategory_id: '',
    metric: 'value'
  };

  function subsForParent(parentId) {
    var subs = typeof MKT_SUBCATEGORIES !== 'undefined' ? MKT_SUBCATEGORIES : [];
    return subs.filter(function(s) {
      var pid = s.parent_category_id;
      if (pid == null && s.category_id >= 100) pid = Math.floor(s.category_id / 100);
      return String(pid) === String(parentId);
    });
  }

  function categoryLabel(id) {
    var cats = typeof MKT_OVERVIEW_CATEGORIES !== 'undefined' ? MKT_OVERVIEW_CATEGORIES : [];
    var c = cats.find(function(x) { return String(x.category_id) === String(id); });
    return c ? (c.category_name || id) : id;
  }

  function subcategoryLabel(id) {
    var subs = typeof MKT_SUBCATEGORIES !== 'undefined' ? MKT_SUBCATEGORIES : [];
    var s = subs.find(function(x) { return String(x.category_id) === String(id); });
    return s ? (s.category_name || id) : id;
  }

  function refreshSubcategoryDropdown() {
    if (!window.MIGenericDropdown || !window.MIGenericDropdown.create) return;
    var subs = subsForParent(state.category_id);
    var subItems = subs.map(function(s) { return { value: String(s.category_id), label: s.category_name || String(s.category_id) }; });
    var valid = state.subcategory_id && subs.some(function(s) { return String(s.category_id) === state.subcategory_id; });
    if (!valid && subs.length) state.subcategory_id = String(subs[0].category_id);
    window.MIGenericDropdown.create('mkt-overview-subcategory', {
      items: subItems.length ? subItems : [{ value: '', label: '—' }],
      initialValue: state.subcategory_id || '',
      minWidth: 200,
      onChange: function(v) {
        state.subcategory_id = v || '';
        loadData();
      }
    });
  }

  function initDropdowns() {
    if (!window.MIGenericDropdown || !window.MIGenericDropdown.create) return;

    var years = (typeof MKT_AVAILABLE_YEARS !== 'undefined' && MKT_AVAILABLE_YEARS.length)
      ? MKT_AVAILABLE_YEARS.map(function(y) { return { value: String(y), label: String(y) }; })
      : [{ value: '2025', label: '2025' }];
    state.year = state.year || String(years[years.length - 1].value);
    window.MIGenericDropdown.create('mkt-overview-year', {
      items: years,
      initialValue: state.year,
      minWidth: 120,
      onChange: function(v) {
        state.year = v || state.year;
        loadData();
      }
    });

    window.MIGenericDropdown.create('mkt-overview-channel', {
      items: [
        { value: '', label: 'All' },
        { value: 'web', label: 'Web' },
        { value: 'app', label: 'App' },
        { value: 'store', label: 'Store' }
      ],
      initialValue: state.channel || '',
      minWidth: 120,
      onChange: function(v) {
        state.channel = v || '';
        loadData();
      }
    });

    var cats = typeof MKT_OVERVIEW_CATEGORIES !== 'undefined' ? MKT_OVERVIEW_CATEGORIES : [];
    var catItems = cats.map(function(c) { return { value: String(c.category_id), label: c.category_name || '' }; });
    if (!state.category_id && catItems.length) state.category_id = catItems[0].value;
    window.MIGenericDropdown.create('mkt-overview-category', {
      items: catItems,
      initialValue: state.category_id,
      minWidth: 200,
      onChange: function(v) {
        state.category_id = v || '';
        state.subcategory_id = '';
        var subs = subsForParent(state.category_id);
        if (subs.length) state.subcategory_id = String(subs[0].category_id);
        refreshSubcategoryDropdown();
        loadData();
      }
    });

    refreshSubcategoryDropdown();

    window.MIGenericDropdown.create('mkt-overview-metric', {
      items: [{ value: 'value', label: 'Value (PLN)' }, { value: 'units', label: 'Units' }],
      initialValue: state.metric || 'value',
      minWidth: 140,
      onChange: function(v) {
        state.metric = v || 'value';
        loadData();
      }
    });
  }

  function buildUrl() {
    var q = ['year=' + encodeURIComponent(state.year || ''), 'category_id=' + encodeURIComponent(state.category_id || '')];
    if (state.subcategory_id && parseInt(state.subcategory_id, 10) >= 100) {
      q.push('subcategory_id=' + encodeURIComponent(state.subcategory_id));
    }
    if (state.channel) q.push('channel=' + encodeURIComponent(state.channel));
    return '/api/marketing/segment-by-category?' + q.join('&');
  }

  function fmtVal(v, isUnits) {
    if (v == null) return '—';
    var n = Number(v).toLocaleString('en-US', { maximumFractionDigits: 0 });
    return isUnits ? n : n + ' PLN';
  }

  function renderDonut(labels, data, isUnits) {
    var canvas = document.getElementById('mkt-overview-chart');
    if (!canvas || typeof Chart === 'undefined') return;
    if (chart) {
      chart.destroy();
      chart = null;
    }
    var doughnutOpts = (typeof ChartStyles !== 'undefined' && ChartStyles.doughnut3DOptions)
      ? ChartStyles.doughnut3DOptions(60) : {};
    var colors = labels.map(function(_, i) { return COLORS[i % COLORS.length]; });
    chart = new Chart(canvas.getContext('2d'), {
      type: 'doughnut',
      data: {
        labels: labels,
        datasets: [{ data: data, backgroundColor: colors, borderWidth: 0, hoverOffset: 4 }]
      },
      options: Object.assign({}, doughnutOpts, {
        responsive: true,
        maintainAspectRatio: false,
        plugins: Object.assign({}, doughnutOpts.plugins || {}, {
          legend: { display: true, position: 'bottom', labels: { color: '#ccc' } },
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

  function scopeLabel() {
    var cn = categoryLabel(state.category_id);
    var sn = state.subcategory_id && parseInt(state.subcategory_id, 10) >= 100
      ? subcategoryLabel(state.subcategory_id) : '';
    return sn ? (cn + ' – ' + sn) : cn;
  }

  function updateInsight(rows) {
    var el = document.getElementById('mkt-overview-key-insight');
    if (!el) return;
    var isUnits = state.metric === 'units';
    var brand = typeof MKT_BRAND_NAME !== 'undefined' ? MKT_BRAND_NAME : 'Your brand';
    if (!rows || rows.length === 0) {
      el.innerHTML = '<div class="mi-ranking">No sales for this selection.</div>';
      return;
    }
    var field = isUnits ? 'units' : 'gross_pln';
    var total = rows.reduce(function(s, r) { return s + (Number(r[field]) || 0); }, 0);
    var top = rows[0];
    var topPct = total > 0 ? (100 * (Number(top[field]) || 0) / total).toFixed(1) : '0';
    var scope = scopeLabel();
    var rowsHtml = '<div class="mi-info-row"><span class="mi-info-label">Scope</span><span class="mi-info-value">' + scope + '</span></div>' +
      '<div class="mi-info-row"><span class="mi-info-label">Brand</span><span class="mi-info-value">' + brand + '</span></div>' +
      '<div class="mi-info-row"><span class="mi-info-label">Top segment</span><span class="mi-info-value">' + (top.segment_name || '—') + '</span></div>' +
      '<div class="mi-info-row"><span class="mi-info-label">Share</span><span class="mi-info-value">' + topPct + '%</span></div>' +
      '<div class="mi-info-row"><span class="mi-info-label">Total</span><span class="mi-info-value">' + fmtVal(total, isUnits) + '</span></div>';
    var top3 = rows.slice(0, 3).map(function(r) {
      var pct = total > 0 ? (100 * (Number(r[field]) || 0) / total).toFixed(1) : '0';
      return '<div class="mi-info-row"><span class="mi-info-label">' + (r.segment_name || '—') + '</span><span class="mi-info-value">' + pct + '%</span></div>';
    }).join('');
    var msg = (top.segment_name || '—') + ' is the main segment for this scope (' + topPct + '% of sales).';
    el.innerHTML = rowsHtml + '<div class="mi-info-label" style="margin-top:.5rem;">Top 3 segments</div>' + top3 + '<div class="mi-ranking">' + msg + '</div>';
  }

  function loadData() {
    if (!window.MKT_OVERVIEW_BRAND_ID) {
      renderDonut(['—'], [1], false);
      var insight = document.getElementById('mkt-overview-key-insight');
      if (insight) insight.innerHTML = '';
      return;
    }
    var loading = document.getElementById('mkt-overview-loading');
    if (loading) loading.classList.remove('hidden');
    var isUnits = state.metric === 'units';
    fetch(buildUrl(), { credentials: 'include' })
      .then(function(r) { return r.ok ? r.json() : { rows: [], error: true }; })
      .then(function(d) {
        var rows = (d && d.rows) || [];
        var field = isUnits ? 'units' : 'gross_pln';
        var labels = rows.map(function(r) { return r.segment_name || ('Segment ' + r.segment_id); });
        var data = rows.map(function(r) { return Number(r[field]) || 0; });
        if (!labels.length) {
          labels = ['No data'];
          data = [1];
        }
        renderDonut(labels, data, isUnits);
        updateInsight(rows.length && data[0] !== 1 ? rows : null);
      })
      .catch(function() {
        renderDonut(['Error'], [1], false);
      })
      .finally(function() {
        if (loading) loading.classList.add('hidden');
      });
  }

  document.addEventListener('DOMContentLoaded', function() {
    initDropdowns();
    loadData();
  });
})();
