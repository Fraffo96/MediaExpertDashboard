/**
 * Marketing – Purchasing Process: Segment by SKU widget.
 * Same chart as Market Intelligence – select SKU to see segment breakdown.
 */
(function() {
  var COLORS = ['#FFD700', '#FFE44D', '#B89900', '#00d4ff', '#ff6b9d', '#64748b'];
  var segmentChart = null;
  var productsCache = [];

  function subcatsByParent() {
    var subs = (typeof MKT_SUBCATEGORIES !== 'undefined' && MKT_SUBCATEGORIES) ? MKT_SUBCATEGORIES : [];
    var map = {};
    subs.forEach(function(s) {
      var pid = s.parent_category_id;
      if (pid == null && s.category_id >= 100) pid = Math.floor(s.category_id / 100);
      if (!pid) return;
      if (!map[pid]) map[pid] = [];
      map[pid].push(s);
    });
    return map;
  }

  function buildTopProductsUrl() {
    var year = (document.getElementById('mkt-seg-sku-year') || {}).value || '';
    var cat = (document.getElementById('mkt-seg-sku-category') || {}).value || '';
    var sub = (document.getElementById('mkt-seg-sku-subcategory') || {}).value || '';
    var ch = (document.getElementById('mkt-seg-sku-channel') || {}).value || '';
    var q = ['year=' + encodeURIComponent(year)];
    if (cat && parseInt(cat, 10) <= 10) q.push('category_id=' + encodeURIComponent(cat));
    if (sub && parseInt(sub, 10) >= 100) q.push('subcategory_id=' + encodeURIComponent(sub));
    if (ch) q.push('channel=' + encodeURIComponent(ch));
    return '/api/marketing/top-products?' + q.join('&');
  }

  function buildSegmentUrl(productId) {
    var year = (document.getElementById('mkt-seg-sku-year') || {}).value || '';
    var cat = (document.getElementById('mkt-seg-sku-category') || {}).value || '';
    var sub = (document.getElementById('mkt-seg-sku-subcategory') || {}).value || '';
    var ch = (document.getElementById('mkt-seg-sku-channel') || {}).value || '';
    var scopeId = sub && parseInt(sub, 10) >= 100 ? sub : cat;
    var q = ['product_id=' + encodeURIComponent(productId), 'year=' + encodeURIComponent(year)];
    if (scopeId) q.push('category_id=' + encodeURIComponent(scopeId));
    if (ch) q.push('channel=' + encodeURIComponent(ch));
    return '/api/marketing/segment-by-sku?' + q.join('&');
  }

  function fmtVal(v, isUnits) {
    if (v == null) return '—';
    var n = Number(v).toLocaleString('en-US', { maximumFractionDigits: 0 });
    return isUnits ? n : n + ' PLN';
  }

  function createDonutChart(labels, data, isUnits) {
    var canvas = document.getElementById('mkt-segment-sku-chart');
    if (!canvas || typeof Chart === 'undefined') return;
    if (segmentChart) {
      segmentChart.destroy();
      segmentChart = null;
    }
    var colors = labels.map(function(_, i) { return COLORS[i % COLORS.length]; });
    var doughnutOpts = (typeof ChartStyles !== 'undefined' && ChartStyles.doughnut3DOptions)
      ? ChartStyles.doughnut3DOptions(60) : {};
    segmentChart = new Chart(canvas.getContext('2d'), {
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

  function updateInsight(segRows, productName, productId, isUnits) {
    var el = document.getElementById('mkt-segment-sku-insight');
    if (!el) return;
    if (!segRows || segRows.length === 0) {
      el.innerHTML = '<p class="mkt-segment-sku-insight-text">Select a SKU to see which segments buy it.</p>';
      return;
    }
    var field = isUnits ? 'units' : 'gross_pln';
    var total = segRows.reduce(function(s, r) { return s + (Number(r[field]) || 0); }, 0);
    var top = segRows[0];
    var topPct = total > 0 ? (100 * (Number(top[field]) || 0) / total).toFixed(1) : '0';
    var html = '<p class="mkt-segment-sku-insight-text"><strong>' + (productName || productId) + '</strong></p>' +
      '<p class="mkt-segment-sku-insight-text">Top segment: ' + (top.segment_name || '—') + ' (' + topPct + '%)</p>' +
      '<p class="mkt-segment-sku-insight-text">' + (top.segment_name || '—') + ' is the main segment for this SKU.</p>';
    el.innerHTML = html;
  }

  function loadProductsAndPopulate() {
    var sel = document.getElementById('mkt-seg-sku-product');
    if (!sel) return;
    fetch(buildTopProductsUrl(), { credentials: 'include' })
      .then(function(r) { return r.ok ? r.json() : { rows: [] }; })
      .then(function(d) {
        var rows = (d && d.rows) || [];
        productsCache = rows;
        var opts = '<option value="">— Select SKU —</option>';
        rows.forEach(function(r) {
          var name = ((r.product_name || 'Product ' + r.product_id) + ' (SKU ' + r.product_id)).substring(0, 50);
          opts += '<option value="' + r.product_id + '">' + name + '</option>';
        });
        sel.innerHTML = opts;
        sel.value = '';
        createDonutChart(['Select SKU'], [1], false);
        updateInsight(null, null, null, false);
      })
      .catch(function() {
        sel.innerHTML = '<option value="">— No products —</option>';
      });
  }

  function loadSegmentAndRender(productId) {
    var sel = document.getElementById('mkt-seg-sku-metric');
    var isUnits = (sel && sel.value) === 'units';
    if (!productId) {
      createDonutChart(['Select SKU'], [1], false);
      updateInsight(null, null, null, false);
      return;
    }
    fetch(buildSegmentUrl(productId), { credentials: 'include' })
      .then(function(r) {
        if (!r.ok) return r.json().then(function(d) { throw new Error(d.error || r.statusText); });
        return r.json();
      })
      .then(function(d) {
        var rows = (d && d.rows) || [];
        var field = isUnits ? 'units' : 'gross_pln';
        var labels = rows.map(function(r) { return r.segment_name || 'Segment ' + r.segment_id; });
        var data = rows.map(function(r) { return Number(r[field]) || 0; });
        if (labels.length === 0) {
          labels = ['No data'];
          data = [1];
        }
        createDonutChart(labels, data, isUnits);
        var prod = productsCache.find(function(p) { return String(p.product_id) === String(productId); });
        updateInsight(rows, prod && prod.product_name, productId, isUnits);
      })
      .catch(function() {
        createDonutChart(['Error'], [1], false);
        updateInsight(null, null, productId, false);
      });
  }

  function initDropdowns() {
    var years = (typeof MKT_AVAILABLE_YEARS !== 'undefined' && MKT_AVAILABLE_YEARS.length) ? MKT_AVAILABLE_YEARS : ['2024', '2023'];
    var yearSel = document.getElementById('mkt-seg-sku-year');
    if (yearSel) {
      yearSel.innerHTML = years.map(function(y) { return '<option value="' + y + '">' + y + '</option>'; }).join('');
      yearSel.value = years[years.length - 1] || years[0];
    }

    var chSel = document.getElementById('mkt-seg-sku-channel');
    if (chSel) {
      chSel.innerHTML = '<option value="">All</option><option value="web">Web</option><option value="app">App</option><option value="store">Store</option>';
    }

    var cats = (typeof MKT_CATEGORIES !== 'undefined' && MKT_CATEGORIES) ? MKT_CATEGORIES : [];
    var catSel = document.getElementById('mkt-seg-sku-category');
    if (catSel) {
      catSel.innerHTML = cats.map(function(c) {
        return '<option value="' + c.category_id + '">' + (c.category_name || '') + '</option>';
      }).join('');
      catSel.value = (cats[0] && cats[0].category_id) || '';
    }

    var subMap = subcatsByParent();
    function refreshSubcategory() {
      var catVal = (catSel && catSel.value) || '';
      var pid = parseInt(catVal, 10) || 0;
      var subs = subMap[pid] || [];
      var subSel = document.getElementById('mkt-seg-sku-subcategory');
      if (!subSel) return;
      subSel.innerHTML = subs.map(function(s) {
        return '<option value="' + s.category_id + '">' + (s.category_name || '') + '</option>';
      }).join('');
      subSel.value = (subs[0] && subs[0].category_id) || '';
      loadProductsAndPopulate();
    }

    if (catSel) catSel.addEventListener('change', function() { refreshSubcategory(); });
    if (yearSel) yearSel.addEventListener('change', function() { loadProductsAndPopulate(); });
    if (chSel) chSel.addEventListener('change', function() { loadProductsAndPopulate(); });

    var subSel = document.getElementById('mkt-seg-sku-subcategory');
    if (subSel) subSel.addEventListener('change', loadProductsAndPopulate);

    var prodSel = document.getElementById('mkt-seg-sku-product');
    if (prodSel) prodSel.addEventListener('change', function() {
      loadSegmentAndRender(prodSel.value || '');
    });

    var metricSel = document.getElementById('mkt-seg-sku-metric');
    if (metricSel) metricSel.addEventListener('change', function() {
      if (prodSel && prodSel.value) loadSegmentAndRender(prodSel.value);
    });

    refreshSubcategory();
    createDonutChart(['Select SKU'], [1], false);
    updateInsight(null, null, null, false);
  }

  document.addEventListener('DOMContentLoaded', function() {
    initDropdowns();
  });
})();
