/**
 * Check Live Promo – init, load active promos, load SKU data, render chart, filters.
 */
(function() {
  var core = window.CLPCore;
  if (!core) return;

  var _activePromos = [];

  function showSkeleton(v) {
    var skel = document.getElementById('clp-chart-skeleton');
    var cont = document.getElementById('clp-chart-container');
    var noData = document.getElementById('clp-no-data');
    if (skel) skel.style.display = v ? '' : 'none';
    if (cont) cont.style.display = v ? 'none' : '';
    if (noData) noData.style.display = 'none';
  }

  function showNoData() {
    var skel = document.getElementById('clp-chart-skeleton');
    var cont = document.getElementById('clp-chart-container');
    var noData = document.getElementById('clp-no-data');
    if (skel) skel.style.display = 'none';
    if (cont) cont.style.display = 'none';
    if (noData) noData.style.display = '';
  }

  function loadActivePromos() {
    var url = core.buildActiveUrl ? core.buildActiveUrl() : '/api/check-live-promo/active';
    fetch(url, { credentials: 'include' })
      .then(function(r) { return r.json(); })
      .then(function(d) {
        _activePromos = (d && d.active) ? d.active : [];
        renderPromoChips();
        populatePromoDropdown();
      })
      .catch(function() {
        _activePromos = [];
        renderPromoChips();
        populatePromoDropdown();
      });
  }

  function renderPromoChips() {
    var el = document.getElementById('clp-promo-chips');
    if (!el) return;
    if (_activePromos.length === 0) {
      el.innerHTML = '<span class="clp-chip-empty">No active promos in this period</span>';
      return;
    }
    el.innerHTML = _activePromos.map(function(p) {
      var name = (p.promo_name || 'Promo ' + p.promo_id).replace(/"/g, '&quot;');
      return '<span class="clp-chip" data-promo-id="' + p.promo_id + '">' + name + '</span>';
    }).join('');
    el.querySelectorAll('.clp-chip').forEach(function(chip) {
      chip.addEventListener('click', function() {
        var sel = document.getElementById('clp-promo');
        if (sel) sel.value = chip.getAttribute('data-promo-id') || '';
        loadData();
      });
    });
  }

  function populatePromoDropdown() {
    var sel = document.getElementById('clp-promo');
    if (!sel) return;
    var prev = sel.value;
    var html = '<option value="">All</option>';
    _activePromos.forEach(function(p) {
      html += '<option value="' + p.promo_id + '">' + (p.promo_name || 'Promo ' + p.promo_id) + '</option>';
    });
    sel.innerHTML = html;
    if (prev && _activePromos.some(function(p) { return String(p.promo_id) === prev; })) {
      sel.value = prev;
    }
  }

  function updateKpis(d) {
    var f = function(n) { return n == null ? '—' : Number(n).toLocaleString('en-US', { maximumFractionDigits: 0 }); };
    var el = function(id) { return document.getElementById(id); };
    if (el('clp-total-sales')) el('clp-total-sales').textContent = f(d.total_gross_pln);
    if (el('clp-total-units')) el('clp-total-units').textContent = f(d.total_units);
    if (el('clp-sku-count')) el('clp-sku-count').textContent = f(d.sku_count);
    if (el('clp-order-count')) el('clp-order-count').textContent = f(d.total_orders);
  }

  function loadData() {
    if (typeof showLoading === 'function') showLoading(true);
    showSkeleton(true);
    loadActivePromos();
    var skuUrl = core.buildSkuUrl();
    var segUrl = core.buildSegmentUrl ? core.buildSegmentUrl(null) : '/api/check-live-promo/segment-breakdown';
    var segPromise = fetch(segUrl, { credentials: 'include' })
      .then(function(r) { return r.ok ? r.json() : { rows: [] }; })
      .then(function(d) { return (d && d.rows) ? d.rows : []; })
      .catch(function() { return []; });
    fetch(skuUrl, { credentials: 'include' })
      .then(function(r) { return r.json(); })
      .then(function(d) {
        if (typeof showLoading === 'function') showLoading(false);
        if (d.error) {
          if (typeof showError === 'function') showError(d.error);
          showNoData();
          updateKpis({ total_gross_pln: 0, total_units: 0, sku_count: 0, total_orders: 0 });
          return;
        }
        if (typeof showError === 'function') showError('');
        var rows = d.rows || [];
        updateKpis(d);
        if (rows.length === 0) {
          showNoData();
        } else {
          showSkeleton(false);
          if (window.CLPChart) window.CLPChart.update(rows, segPromise);
          wireMetricDropdown();
        }
      })
      .catch(function(e) {
        if (typeof showLoading === 'function') showLoading(false);
        if (typeof showError === 'function') showError('Failed to load: ' + (e && e.message));
        showNoData();
      });
  }

  function wireMetricDropdown() {
    var sel = document.getElementById('clp-metric');
    if (!sel || !window.CLPChart) return;
    sel.onchange = function() {
      window.CLPChart.setMetric(sel.value);
    };
  }

  function initDatePresets() {
    var presets = document.querySelectorAll('.clp-preset');
    var customWrap = document.querySelector('.clp-date-custom');
    presets.forEach(function(btn) {
      btn.addEventListener('click', function() {
        presets.forEach(function(b) { b.classList.remove('active'); });
        btn.classList.add('active');
        if (btn.getAttribute('data-days') === 'custom') {
          if (customWrap) customWrap.style.display = '';
        } else {
          if (customWrap) customWrap.style.display = 'none';
          loadData();
        }
      });
    });
  }

  function init() {
    loadData();
    initDatePresets();
    var applyBtn = document.getElementById('clp-apply');
    if (applyBtn) applyBtn.addEventListener('click', loadData);
    window.loadData = loadData;
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
