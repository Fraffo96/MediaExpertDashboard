/**
 * Basic dashboard – Incremental YoY chart + promo picker.
 */
(function() {
  function setIncrementalYoyChart(d) {
    var core = window.BasicCore;
    if (!core) return;
    var c = core.gc('chartIncrYoy');
    if (!c) return;
    var byPromo = (d.incremental_yoy_by_promo || []);
    if (byPromo.length) {
      var years = [];
      var yearSet = {};
      byPromo.forEach(function(r) { if (!yearSet[r.year]) { yearSet[r.year] = true; years.push(r.year); } });
      years.sort();
      var promos = [];
      var promoSet = {};
      byPromo.forEach(function(r) { if (!promoSet[r.promo_id]) { promoSet[r.promo_id] = r.promo_name; promos.push({ id: r.promo_id, name: r.promo_name }); } });
      var totalGrossByYear = years.map(function(y) {
        var r = byPromo.find(function(x) { return x.year === y; });
        return r ? r.total_gross : 0;
      });
      var attributedByYear = years.map(function(y) {
        var sum = 0;
        byPromo.forEach(function(x) { if (x.year === y) sum += Number(x.attributed_sales) || 0; });
        return sum;
      });
      var contrastColor = COLORS.slate;
      var datasets = [];
      promos.forEach(function(p) {
        var data = years.map(function(y) {
          var r = byPromo.find(function(x) { return x.year === y && x.promo_id === p.id; });
          return r ? r.incremental_pct : 0;
        });
        datasets.push({ label: p.name + ' (Incr. %)', data: data, type: 'bar', backgroundColor: contrastColor, borderWidth: 0, yAxisID: 'y', _isPct: true, order: 2 });
      });
      datasets.push({ label: 'Total Sales (PLN)', data: totalGrossByYear, type: 'line', borderColor: COLORS.yellow, backgroundColor: 'transparent', borderWidth: 2, pointRadius: 5, pointBackgroundColor: COLORS.yellow, yAxisID: 'y1', order: 0 });
      datasets.push({ label: 'Attributed Sales (PLN)', data: attributedByYear, type: 'line', borderColor: contrastColor, backgroundColor: 'transparent', borderWidth: 2, pointRadius: 4, pointBackgroundColor: contrastColor, yAxisID: 'y1', order: 1 });
      setChart(c, years.map(String), datasets);
    } else {
      var iy = d.incremental_yoy || [];
      setChart(c, iy.map(function(r) { return String(r.year); }), [
        { label: 'Incremental %', data: iy.map(function(r) { return r.incremental_pct; }), backgroundColor: COLORS.slate, borderWidth: 0, yAxisID: 'y', _isPct: true, order: 2 },
        { label: 'Total Sales (PLN)', data: iy.map(function(r) { return r.total_gross; }), type: 'line', borderColor: COLORS.yellow, backgroundColor: 'transparent', borderWidth: 2, pointRadius: 5, pointBackgroundColor: COLORS.yellow, yAxisID: 'y1', order: 1 }
      ]);
    }
  }

  function getSelectedPromoIds() {
    var wrap = document.getElementById('promo-picker');
    if (!wrap) return [];
    var allCb = document.getElementById('promo-picker-all');
    if (allCb && allCb.checked) return [];
    var checked = wrap.querySelectorAll('.promo-picker-cb:checked');
    return [].slice.call(checked).map(function(cb) { return cb.value; });
  }

  function renderPromoChips() {
    var wrap = document.getElementById('promo-picker');
    if (!wrap) return;
    var chipsEl = document.getElementById('promo-picker-chips');
    var trigger = document.getElementById('promo-picker-trigger');
    var ids = getSelectedPromoIds();
    var allCb = document.getElementById('promo-picker-all');
    if (ids.length === 0 || (allCb && allCb.checked)) {
      if (chipsEl) chipsEl.innerHTML = '';
      if (trigger) trigger.textContent = 'Promo: All';
      return;
    }
    var names = ids.map(function(id) {
      var opt = wrap.querySelector('.promo-picker-option[data-value="' + id + '"]');
      return (opt && opt.getAttribute('data-name')) || id;
    });
    if (chipsEl) {
      chipsEl.innerHTML = names.map(function(n, i) {
        return '<span class="promo-picker-chip" data-id="' + ids[i] + '">' + n + ' <button type="button" class="promo-picker-chip-remove" aria-label="Remove">&times;</button></span>';
      }).join('');
      chipsEl.querySelectorAll('.promo-picker-chip-remove').forEach(function(btn) {
        btn.addEventListener('click', function() {
          var id = this.closest('.promo-picker-chip').getAttribute('data-id');
          var cb = wrap.querySelector('.promo-picker-cb[value="' + id + '"]');
          if (cb) {
            cb.checked = false;
            syncPromoPickerAll();
            renderPromoChips();
            try { localStorage.setItem('incremental_yoy_promo_ids', getSelectedPromoIds().join(',')); } catch (e) {}
            refreshIncrementalYoyOnly();
          }
        });
      });
    }
    if (trigger) trigger.textContent = 'Promo: ' + ids.length + ' selected';
  }

  function syncPromoPickerAll() {
    var allCb = document.getElementById('promo-picker-all');
    var list = document.querySelectorAll('.promo-picker-cb');
    if (!allCb || !list.length) return;
    var checked = document.querySelectorAll('.promo-picker-cb:checked').length;
    allCb.checked = checked === 0;
  }

  async function refreshIncrementalYoyOnly() {
    var core = window.BasicCore;
    var selected = getSelectedPromoIds();
    try {
      if (selected.length === 0) {
        if (core && core.getLastData()) setIncrementalYoyChart(core.getLastData());
        return;
      }
      var extra = { promo_ids: selected.join(',') };
      var res = await fetchAPI('/api/basic/incremental_yoy', extra);
      setIncrementalYoyChart(res);
    } catch (e) {
      showError('Error loading promo comparison: ' + (e.message || e));
    }
  }

  function update(d) {
    setIncrementalYoyChart(d);
  }

  function initPromoPicker() {
    var core = window.BasicCore;
    var wrap = document.getElementById('promo-picker');
    if (!wrap) return;
    var trigger = document.getElementById('promo-picker-trigger');
    var dropdown = document.getElementById('promo-picker-dropdown');
    var allCb = document.getElementById('promo-picker-all');
    try {
      var v = localStorage.getItem('incremental_yoy_promo_ids');
      if (v) {
        var ids = v.split(',').filter(Boolean);
        wrap.querySelectorAll('.promo-picker-cb').forEach(function(cb) { cb.checked = ids.indexOf(cb.value) !== -1; });
        if (allCb) allCb.checked = ids.length === 0;
      } else if (allCb) allCb.checked = true;
    } catch (e) {}
    syncPromoPickerAll();
    renderPromoChips();

    var cardEl = null;
    function closeDropdown() {
      if (dropdown) { dropdown.classList.remove('open'); dropdown.setAttribute('aria-hidden', 'true'); if (trigger) trigger.setAttribute('aria-expanded', 'false'); }
      if (cardEl) { cardEl.classList.remove('dropdown-open'); cardEl = null; }
    }
    function openDropdown() {
      if (dropdown) { dropdown.classList.add('open'); dropdown.setAttribute('aria-hidden', 'false'); if (trigger) trigger.setAttribute('aria-expanded', 'true'); }
      if (wrap) { cardEl = wrap.closest('.chart-card'); if (cardEl) cardEl.classList.add('dropdown-open'); }
    }
    if (trigger && dropdown) {
      trigger.addEventListener('click', function(e) { e.stopPropagation(); if (dropdown.classList.contains('open')) closeDropdown(); else openDropdown(); });
    }
    document.addEventListener('click', function() { closeDropdown(); });
    if (dropdown) dropdown.addEventListener('click', function(e) { e.stopPropagation(); });

    wrap.querySelectorAll('.promo-picker-option').forEach(function(opt) {
      var cb = opt.querySelector('input[type="checkbox"]');
      if (!cb) return;
      cb.addEventListener('change', function() {
        if (cb === allCb) {
          wrap.querySelectorAll('.promo-picker-cb').forEach(function(c) { c.checked = false; });
        } else {
          if (allCb) allCb.checked = false;
        }
        syncPromoPickerAll();
        renderPromoChips();
        try { localStorage.setItem('incremental_yoy_promo_ids', getSelectedPromoIds().join(',')); } catch (e) {}
        refreshIncrementalYoyOnly();
      });
    });
  }

  window.BasicChartsIncrementalYoy = {
    update: update,
    setIncrementalYoyChart: setIncrementalYoyChart,
    initPromoPicker: initPromoPicker
  };
})();
