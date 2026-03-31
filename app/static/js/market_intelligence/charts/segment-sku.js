/**
 * Market Intelligence – Segment by SKU: donut chart + key takeaways.
 * Seleziona una SKU per vedere quali segmenti la comprano.
 */
(function() {
  var COLORS = ['#FFD700', '#FFE44D', '#B89900', '#E0E0E0', '#888', '#64748b'];

  function buildTopProductsUrl() {
    var state = window.MIScopeState || {};
    var year = state.year_segment_sku || state.year_category_pie || '';
    var cat = state.segment_sku_category_id || state.category_pie_id || '';
    var sub = state.segment_sku_subcategory_id || state.subcategory_pie_id || '';
    var ch = state.channel_segment_sku || '';
    var q = ['year=' + encodeURIComponent(year)];
    if (cat && parseInt(cat, 10) <= 10) q.push('category_id=' + encodeURIComponent(cat));
    if (sub && parseInt(sub, 10) >= 100) q.push('subcategory_id=' + encodeURIComponent(sub));
    if (ch) q.push('channel=' + encodeURIComponent(ch));
    return '/api/market-intelligence/top-products?' + q.join('&');
  }

  function buildSegmentUrl(productId) {
    var state = window.MIScopeState || {};
    var year = state.year_segment_sku || state.year_category_pie || '';
    var cat = state.segment_sku_category_id || state.category_pie_id || '';
    var sub = state.segment_sku_subcategory_id || state.subcategory_pie_id || '';
    var ch = state.channel_segment_sku || '';
    var scopeId = sub && parseInt(sub, 10) >= 100 ? sub : cat;
    var q = ['product_id=' + encodeURIComponent(productId), 'year=' + encodeURIComponent(year)];
    if (scopeId) q.push('category_id=' + encodeURIComponent(scopeId));
    if (ch) q.push('channel=' + encodeURIComponent(ch));
    return '/api/market-intelligence/segment-by-sku?' + q.join('&');
  }

  /** Una riga per segment_id (somma PLN/units) – difesa da doppioni API/precalc. */
  function consolidateSegmentRows(rows, field) {
    if (!rows || !rows.length) return [];
    var byId = {};
    rows.forEach(function(r) {
      var id = r.segment_id != null ? String(r.segment_id) : '';
      if (!id) return;
      if (!byId[id]) {
        byId[id] = {
          segment_id: r.segment_id,
          segment_name: r.segment_name || ('Segment ' + id),
          gross_pln: 0,
          units: 0
        };
      }
      byId[id].gross_pln += Number(r.gross_pln) || 0;
      byId[id].units += Number(r.units) || 0;
      if (r.segment_name) byId[id].segment_name = r.segment_name;
    });
    var out = Object.keys(byId).map(function(k) { return byId[k]; });
    out.sort(function(a, b) { return (Number(b[field]) || 0) - (Number(a[field]) || 0); });
    return out;
  }

  function fmtVal(v, isUnits) {
    if (v == null) return '—';
    var n = Number(v).toLocaleString('en-US', { maximumFractionDigits: 0 });
    return isUnits ? n : n + ' PLN';
  }

  function createDonutChart(labels, data, isUnits) {
    var canvas = document.getElementById('chart-segment-sku');
    if (!canvas || typeof Chart === 'undefined') return;
    if (canvas.segmentChart) {
      canvas.segmentChart.destroy();
      canvas.segmentChart = null;
    }
    var doughnutOpts = (typeof ChartStyles !== 'undefined' && ChartStyles.doughnut3DOptions)
      ? ChartStyles.doughnut3DOptions(60) : {};
    var colors = labels.map(function(_, i) { return COLORS[i % COLORS.length]; });
    canvas.segmentChart = new Chart(canvas.getContext('2d'), {
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

  function updateKeyInsight(segRows, productName, productId, isUnits) {
    var el = document.getElementById('mi-segment-sku-key-insight');
    if (!el) return;
    var brandLabel = (typeof window.MI_BRAND_NAME !== 'undefined' && window.MI_BRAND_NAME) ? window.MI_BRAND_NAME : 'Your Brand';
    var brandSpan = '<span class="mi-brand-label">' + brandLabel + '</span>';
    if (!segRows || segRows.length === 0) {
      el.innerHTML = '<div class="mi-ranking">Select a SKU to see which segments buy it.</div>';
      return;
    }
    var field = isUnits ? 'units' : 'gross_pln';
    var total = segRows.reduce(function(s, r) { return s + (Number(r[field]) || 0); }, 0);
    var top = segRows[0];
    var topPct = total > 0 ? (100 * (Number(top[field]) || 0) / total).toFixed(1) : '0';
    var rows = '<div class="mi-info-row"><span class="mi-info-label">SKU</span><span class="mi-info-value">' + (productName || productId) + '</span></div>' +
      '<div class="mi-info-row"><span class="mi-info-label">Top segment</span><span class="mi-info-value">' + (top.segment_name || '—') + '</span></div>' +
      '<div class="mi-info-row"><span class="mi-info-label">Share</span><span class="mi-info-value">' + topPct + '%</span></div>' +
      '<div class="mi-info-row"><span class="mi-info-label">Value</span><span class="mi-info-value">' + fmtVal(top[field], isUnits) + '</span></div>';
    var top3 = segRows.slice(0, 3).map(function(r) {
      var pct = total > 0 ? (100 * (Number(r[field]) || 0) / total).toFixed(1) : '0';
      return '<div class="mi-info-row"><span class="mi-info-label">' + (r.segment_name || '—') + '</span><span class="mi-info-value">' + pct + '%</span></div>';
    }).join('');
    var msg = (top.segment_name || '—') + ' is the main segment for this SKU (' + topPct + '% of sales).';
    el.innerHTML = rows + '<div class="mi-info-label" style="margin-top:.5rem;">Top 3 segments</div>' + top3 + '<div class="mi-ranking">' + msg + '</div>';
  }

  function loadProductsAndPopulate() {
    var state = window.MIScopeState || {};
    var container = document.getElementById('mi-segment-sku-product-select');
    if (!container) return;
    var url = buildTopProductsUrl();
    fetch(url, { credentials: 'include' })
      .then(function(r) { return r.ok ? r.json() : { rows: [] }; })
      .then(function(d) {
        var rows = (d && d.rows) || [];
        var state = window.MIScopeState || {};
        state._segmentSkuProducts = rows;
        if (!window.MIGenericDropdown) return;
        var items = [{ value: '', label: '— Select SKU —' }].concat(
          rows.map(function(r) {
            var name = (r.product_name || 'Product ' + r.product_id).substring(0, 50);
            return { value: String(r.product_id), label: name + ' (SKU ' + r.product_id + ')' };
          })
        );
        var curVal = state.segment_sku_product_id || '';
        if (window.MIGenericDropdown.create) {
          window.MIGenericDropdown.create('mi-segment-sku-product-select', {
            items: items,
            initialValue: curVal,
            minWidth: 160,
            onChange: function(v) {
              state.segment_sku_product_id = v || '';
              if (v) loadSegmentAndRender(v);
              else {
                createDonutChart(['Select SKU'], [1], false);
                updateKeyInsight(null, null, null, false);
              }
            }
          });
        }
      })
      .catch(function() {
        if (window.MIGenericDropdown) {
          window.MIGenericDropdown.create('mi-segment-sku-product-select', {
            items: [{ value: '', label: '— No products —' }],
            initialValue: '',
            minWidth: 160
          });
        }
      });
  }

  function loadSegmentAndRender(productId) {
    var state = window.MIScopeState || {};
    var isUnits = (state.segment_sku_metric || 'value') === 'units';
    var url = buildSegmentUrl(productId);
    var loading = document.querySelector('.chart-loading[data-chart-scope="segment-sku"]');
    if (loading) loading.classList.remove('hidden');
    fetch(url, { credentials: 'include' })
      .then(function(r) {
        if (!r.ok) return r.json().then(function(d) { throw new Error(d.error || r.statusText); });
        return r.json();
      })
      .then(function(d) {
        var raw = (d && d.rows) || [];
        var field = isUnits ? 'units' : 'gross_pln';
        var rows = consolidateSegmentRows(raw, field);
        var labels = rows.map(function(r) { return r.segment_name || 'Segment ' + r.segment_id; });
        var data = rows.map(function(r) { return Number(r[field]) || 0; });
        if (labels.length === 0) {
          labels = ['No data'];
          data = [1];
        }
        createDonutChart(labels, data, isUnits);
        var products = state._segmentSkuProducts || [];
        var prod = products.find(function(p) { return String(p.product_id) === String(productId); });
        updateKeyInsight(rows, prod && prod.product_name, productId, isUnits);
      })
      .catch(function(err) {
        createDonutChart(['Error'], [1], false);
        updateKeyInsight(null, null, productId, false);
      })
      .finally(function() {
        if (loading) loading.classList.add('hidden');
      });
  }

  function init(state, brandCats, brandSubcats, callbacks) {
    var yearConfig = { id: 'mi-year-segment-sku', stateKey: 'year_segment_sku', scope: 'segment_sku' };
    var channelConfig = { id: 'mi-channel-segment-sku', stateKey: 'channel_segment_sku', scope: 'segment_sku' };
    var availableYears = (window._miDataByYear && Object.keys(window._miDataByYear)) || [];
    var defY = availableYears.length ? String(availableYears[availableYears.length - 1]) : '';
    state.year_segment_sku = state.year_segment_sku || defY;
    state.channel_segment_sku = state.channel_segment_sku || '';
    state.segment_sku_category_id = state.segment_sku_category_id || (brandCats[0] && String(brandCats[0].category_id)) || '';
    var firstSubs = brandSubcats[state.segment_sku_category_id] || [];
    state.segment_sku_subcategory_id = state.segment_sku_subcategory_id || (firstSubs[0] && String(firstSubs[0].category_id)) || '';

    if (window.MIGenericDropdown) {
      var yearItems = availableYears.map(function(y) { return { value: String(y), label: String(y) }; });
      if (yearItems.length) {
        window.MIGenericDropdown.create('mi-year-segment-sku', {
          items: yearItems,
          initialValue: state.year_segment_sku || defY,
          minWidth: 120,
          onChange: function(v) {
            state.year_segment_sku = v || defY;
            loadProductsAndPopulate();
            if (state.segment_sku_product_id) loadSegmentAndRender(state.segment_sku_product_id);
          }
        });
      }
      var chItems = [{ value: '', label: 'All' }, { value: 'web', label: 'Web' }, { value: 'app', label: 'App' }, { value: 'store', label: 'Store' }];
      window.MIGenericDropdown.create('mi-channel-segment-sku', {
        items: chItems,
        initialValue: state.channel_segment_sku || '',
        minWidth: 120,
        onChange: function(v) {
          state.channel_segment_sku = v || '';
          loadProductsAndPopulate();
          if (state.segment_sku_product_id) loadSegmentAndRender(state.segment_sku_product_id);
        }
      });
      var catItems = (brandCats || []).map(function(c) { return { value: String(c.category_id), label: c.category_name || '' }; });
      function refreshSubcategoryDropdown() {
        var subs = (brandSubcats[state.segment_sku_category_id] || []);
        var subItems = subs.map(function(s) { return { value: String(s.category_id), label: s.category_name || '' }; });
        var valid = state.segment_sku_subcategory_id && subs.some(function(s) { return String(s.category_id) === state.segment_sku_subcategory_id; });
        if (!valid && subs.length) state.segment_sku_subcategory_id = String(subs[0].category_id);
        window.MIGenericDropdown.create('mi-segment-sku-subcategory-select', {
          items: subItems,
          initialValue: state.segment_sku_subcategory_id || '',
          minWidth: 180,
          onChange: function(v) {
            state.segment_sku_subcategory_id = v || '';
            loadProductsAndPopulate();
            if (state.segment_sku_product_id) loadSegmentAndRender(state.segment_sku_product_id);
          }
        });
      }
      window.MIGenericDropdown.create('mi-segment-sku-category-select', {
        items: catItems,
        initialValue: state.segment_sku_category_id || '',
        minWidth: 140,
        onChange: function(v) {
          state.segment_sku_category_id = v || '';
          state.segment_sku_subcategory_id = '';
          var subs = (brandSubcats[v] || []);
          if (subs.length) state.segment_sku_subcategory_id = String(subs[0].category_id);
          refreshSubcategoryDropdown();
          loadProductsAndPopulate();
          if (state.segment_sku_product_id) loadSegmentAndRender(state.segment_sku_product_id);
        }
      });
      refreshSubcategoryDropdown();
      var metricItems = [{ value: 'value', label: 'Value (PLN)' }, { value: 'units', label: 'Units' }];
      state.segment_sku_metric = state.segment_sku_metric || 'value';
      window.MIGenericDropdown.create('mi-segment-sku-metric', {
        items: metricItems,
        initialValue: state.segment_sku_metric,
        minWidth: 120,
        onChange: function(v) {
          state.segment_sku_metric = v || 'value';
          if (state.segment_sku_product_id) loadSegmentAndRender(state.segment_sku_product_id);
        }
      });
    }
    loadProductsAndPopulate();
    createDonutChart(['Select SKU'], [1], false);
    updateKeyInsight(null, null, null, false);
  }

  window.MIChartsSegmentSku = {
    init: init,
    loadProductsAndPopulate: loadProductsAndPopulate,
    loadSegmentAndRender: loadSegmentAndRender,
    updateKeyInsight: updateKeyInsight
  };
})();
