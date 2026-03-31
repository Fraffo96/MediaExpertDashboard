/**
 * Marketing – Segment Summary: cards with mini charts (category doughnut, SKU bar).
 */
(function() {
  var segmentCharts = {};

  function getParams() {
    var p = {};
    var form = document.getElementById('filter-form');
    if (form) {
      var fd = new FormData(form);
      for (var pair of fd.entries()) p[pair[0]] = pair[1];
    }
    Object.keys(p).forEach(function(k) { if (!p[k]) delete p[k]; });
    return new URLSearchParams(p).toString();
  }

  function apiUrl() {
    return '/api/marketing/segments?' + getParams();
  }

  function getColors(n) {
    var c = (typeof COLORS !== 'undefined' && COLORS.cat10) ? COLORS.cat10 : ['#FFD700','#FFE44D','#B89900','#00d4ff','#ff6b9d','#64748b'];
    return Array.from({ length: n }, function(_, i) { return c[i % c.length]; });
  }

  function escapeHtml(s) {
    if (!s) return '';
    var d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
  }

  function fmt(n) {
    return n == null ? '--' : Number(n).toLocaleString('en-US', { maximumFractionDigits: 0 });
  }

  function truncate(s, len) {
    if (!s) return '';
    return s.length > len ? s.slice(0, len) + '…' : s;
  }

  function renderCard(s, idx) {
    var segId = s.segment_id || idx;
    var pain = (s.pain_points || []).map(function(p) { return '<span class="mkt-tag mkt-tag-pain">' + escapeHtml(p) + '</span>'; }).join('');
    var need = (s.needstates || []).map(function(n) { return '<span class="mkt-tag mkt-tag-need">' + escapeHtml(n) + '</span>'; }).join('');
    var catId = 'mkt-seg-' + segId + '-cat';
    var skuId = 'mkt-seg-' + segId + '-sku';

    var topCats = s.top_categories || [];
    var topSkus = s.top_skus || [];
    var hasCatChart = topCats.length > 0;
    var hasSkuChart = topSkus.length > 0;

    return '<div class="mkt-segment-card" data-segment-id="' + segId + '">' +
      '<h4 class="mkt-segment-title">' + escapeHtml(s.name || 'Segment ' + segId) + '</h4>' +
      (pain ? '<div class="mkt-tags"><span class="mkt-tag-label">Pain points</span>' + pain + '</div>' : '') +
      (need ? '<div class="mkt-tags"><span class="mkt-tag-label">Needstates</span>' + need + '</div>' : '') +
      (hasCatChart ? '<div class="mkt-mini-chart"><h5>Top categories</h5><div class="mkt-chart-inner"><canvas id="' + catId + '"></canvas></div></div>' : '') +
      (hasSkuChart ? '<div class="mkt-mini-chart"><h5>Top SKUs</h5><div class="mkt-chart-inner mkt-chart-hbar"><canvas id="' + skuId + '"></canvas></div></div>' : '') +
      '</div>';
  }

  function destroyCharts() {
    Object.keys(segmentCharts).forEach(function(id) {
      if (segmentCharts[id] && segmentCharts[id].destroy) segmentCharts[id].destroy();
    });
    segmentCharts = {};
  }

  function createCategoryChart(canvasId, topCats) {
    var canvas = document.getElementById(canvasId);
    if (!canvas || typeof Chart === 'undefined') return;
    var labels = topCats.map(function(c) { return c.category_name || ''; });
    var values = topCats.map(function(c) { return Number(c.gross_pln || 0); });
    var baseOpt = typeof DOUGHNUT_OPT !== 'undefined' ? Object.assign({}, DOUGHNUT_OPT) : { responsive: true, maintainAspectRatio: false };
    var opt = Object.assign({}, baseOpt, {
      cutout: '55%',
      plugins: {
        legend: { display: true, position: 'bottom', labels: { font: { size: 11 }, padding: 10, boxWidth: 12 } },
        tooltip: typeof CHART_TOOLTIP !== 'undefined' ? CHART_TOOLTIP : { callbacks: { label: function(ctx) { var t = ctx.dataset.data.reduce(function(a,b){return a+b;},0); var p = t>0 ? (100*ctx.raw/t).toFixed(1) : 0; return ctx.label + ': ' + fmt(ctx.raw) + ' PLN (' + p + '%)'; } } }
      }
    });
    var ch = new Chart(canvas.getContext('2d'), {
      type: 'doughnut',
      data: {
        labels: labels,
        datasets: [{ data: values, backgroundColor: getColors(labels.length), borderWidth: 0 }]
      },
      options: opt
    });
    segmentCharts[canvasId] = ch;
  }

  function createSkuChart(canvasId, topSkus) {
    var canvas = document.getElementById(canvasId);
    if (!canvas || typeof Chart === 'undefined') return;
    var labels = topSkus.map(function(s) { return (s.product_name || '') + ' (' + (s.brand_name || '') + ')'; });
    var values = topSkus.map(function(s) { return Number(s.gross_pln || 0); });
    var baseOpt = typeof HBAR_OPT !== 'undefined' ? Object.assign({}, HBAR_OPT) : { responsive: true, maintainAspectRatio: false, indexAxis: 'y', plugins: { legend: { display: false } } };
    var opt = Object.assign({}, baseOpt, {
      indexAxis: 'y',
      scales: Object.assign({}, baseOpt.scales || {}, {
        y: {
          ticks: { font: { size: 12 }, maxRotation: 0, autoSkip: false, callback: function(val, i, ticks) {
            var lbl = (this.chart && this.chart.data.labels && this.chart.data.labels[i]) || String(val);
            return lbl.length > 45 ? lbl.slice(0, 42) + '…' : lbl;
          } }
        }
      }),
      plugins: Object.assign({}, baseOpt.plugins || {}, {
        tooltip: { callbacks: { title: function(items) { var idx = items[0] && items[0].dataIndex; var d = topSkus[idx]; return d ? (d.product_name || '') + ' (' + (d.brand_name || '') + ')' : ''; }, label: function(ctx) { return fmt(ctx.raw) + ' PLN'; } } }
      })
    });
    var ch = new Chart(canvas.getContext('2d'), {
      type: 'bar',
      data: {
        labels: labels,
        datasets: [{ data: values, backgroundColor: getColors(labels.length), borderWidth: 0 }]
      },
      options: opt
    });
    segmentCharts[canvasId] = ch;
  }

  function renderCards(data) {
    var container = document.getElementById('mkt-segments-cards');
    var loading = document.getElementById('mkt-segments-loading');
    var noData = document.getElementById('mkt-segments-no-data');
    if (!container) return;

    var segments = data.segments || [];
    if (!segments.length) {
      loading.classList.add('hidden');
      noData.style.display = 'block';
      return;
    }

    destroyCharts();
    container.innerHTML = segments.map(function(s, i) { return renderCard(s, i + 1); }).join('');
    container.style.display = '';

    segments.forEach(function(s) {
      var segId = s.segment_id || 0;
      if ((s.top_categories || []).length) createCategoryChart('mkt-seg-' + segId + '-cat', s.top_categories);
      if ((s.top_skus || []).length) createSkuChart('mkt-seg-' + segId + '-sku', s.top_skus);
    });

    loading.classList.add('hidden');
    noData.style.display = 'none';
  }

  async function loadData() {
    var loading = document.getElementById('mkt-segments-loading');
    var container = document.getElementById('mkt-segments-cards');
    var noData = document.getElementById('mkt-segments-no-data');
    if (loading) loading.classList.remove('hidden');
    if (container) container.style.display = 'none';
    if (noData) noData.style.display = 'none';

    try {
      var r = await fetch(apiUrl(), { credentials: 'include' });
      var data = await r.json();
      if (r.ok) {
        renderCards(data);
      } else {
        if (typeof showError === 'function') showError(data.error || 'Failed to load');
        if (loading) loading.classList.add('hidden');
      }
    } catch (e) {
      if (typeof showError === 'function') showError('Network error');
      if (loading) loading.classList.add('hidden');
    }
  }

  window.loadData = loadData;

  function filterSubcategoriesByCategory() {
    var catSel = document.getElementById('f-category-id');
    var subSel = document.getElementById('f-subcategory-id');
    if (!catSel || !subSel) return;
    var catVal = catSel.value || '';
    var opts = subSel.querySelectorAll('option');
    opts.forEach(function(opt) {
      if (opt.value === '') { opt.style.display = ''; return; }
      var parent = opt.getAttribute('data-parent');
      if (!catVal) { opt.style.display = ''; return; }
      opt.style.display = (parent === catVal) ? '' : 'none';
      if (opt.style.display === 'none' && opt.selected) subSel.value = '';
    });
  }

  function populateCategorySubcategory(categories, subcategories) {
    var catSel = document.getElementById('f-category-id');
    var subSel = document.getElementById('f-subcategory-id');
    if (!catSel || !subSel) return;
    var cats = categories || [];
    var subs = subcategories || [];
    catSel.innerHTML = '<option value="">All</option>' + cats.map(function(c) {
      return '<option value="' + (c.category_id || c.id) + '">' + (c.category_name || c.name || '') + '</option>';
    }).join('');
    subSel.innerHTML = '<option value="">All</option>' + subs.map(function(s) {
      var pid = s.parent_category_id || s.parent_id || '';
      return '<option value="' + (s.category_id || s.id) + '" data-parent="' + pid + '">' + (s.category_name || s.name || '') + '</option>';
    }).join('');
    filterSubcategoriesByCategory();
  }

  async function onBrandChange() {
    var brandSel = document.getElementById('f-brand-id');
    var brandId = brandSel ? brandSel.value : '';
    if (!brandId) {
      var cats = (typeof window.MKT_CATEGORIES !== 'undefined') ? window.MKT_CATEGORIES : [];
      var subs = (typeof window.MKT_SUBCATEGORIES !== 'undefined') ? window.MKT_SUBCATEGORIES : [];
      populateCategorySubcategory(cats, subs);
      document.getElementById('f-category-id').value = '';
      document.getElementById('f-subcategory-id').value = '';
    } else {
      try {
        var r = await fetch('/api/marketing/categories-by-brand?brand_id=' + encodeURIComponent(brandId), { credentials: 'include' });
        var data = await r.json();
        populateCategorySubcategory(data.categories || [], data.subcategories || []);
        document.getElementById('f-category-id').value = '';
        document.getElementById('f-subcategory-id').value = '';
      } catch (e) {
        if (typeof showError === 'function') showError('Failed to load categories');
      }
    }
    loadData();
  }

  function initBrandFilter() {
    var brandSel = document.getElementById('f-brand-id');
    var brandId = brandSel ? brandSel.value : '';
    if (brandId) {
      fetch('/api/marketing/categories-by-brand?brand_id=' + encodeURIComponent(brandId), { credentials: 'include' })
        .then(function(r) { return r.json(); })
        .then(function(data) {
          populateCategorySubcategory(data.categories || [], data.subcategories || []);
        })
        .catch(function() {});
    }
  }

  document.addEventListener('DOMContentLoaded', function() {
    filterSubcategoriesByCategory();
    initBrandFilter();
    loadData();
    var segSelect = document.getElementById('f-segment-id');
    var catSelect = document.getElementById('f-category-id');
    var subSelect = document.getElementById('f-subcategory-id');
    var brandSelect = document.getElementById('f-brand-id');
    if (brandSelect) brandSelect.addEventListener('change', onBrandChange);
    if (segSelect) segSelect.addEventListener('change', loadData);
    if (catSelect) catSelect.addEventListener('change', function() { filterSubcategoriesByCategory(); subSelect.value = ''; loadData(); });
    if (subSelect) subSelect.addEventListener('change', loadData);
  });
})();
