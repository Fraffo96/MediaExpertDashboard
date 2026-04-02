/**
 * Brand Comparison – entry: init charts, loadBase, loadData, wire dropdowns.
 * Stessi 5 grafici di Market Intelligence, ma Brand vs Competitor.
 * Competitor richiesto prima di caricare i dati.
 */
(function() {
  console.debug('[BC dashboard] init');
  var core = window.BCCore;
  if (!core) {
    console.debug('[BC dashboard] SKIP: BCCore not found');
    return;
  }

  function initCharts() {
    if (typeof Chart === 'undefined') return;
    var barOpts = { type: 'bar', data: { labels: [], datasets: [] }, options: (window.MICore && window.MICore.MI_BAR_OPT) || (typeof BAR_OPT !== 'undefined' ? BAR_OPT : {}) };
    ['chart-promo-share', 'chart-promo-roi', 'chart-peak'].forEach(function(id) {
      var c = document.getElementById(id);
      if (c && !c.chart) c.chart = new Chart(c.getContext('2d'), barOpts);
    });
    var catSubOpts = { type: 'bar', data: { labels: [], datasets: [] }, options: Object.assign({ indexAxis: 'y' }, (window.MICore && window.MICore.MI_BAR_OPT) || {}) };
    ['chart-category-pie', 'chart-subcategory-pie'].forEach(function(id) {
      var c = document.getElementById(id);
      if (c && !c.chart) c.chart = new Chart(c.getContext('2d'), catSubOpts);
    });
  }

  function showChartLoadings() {
    document.querySelectorAll('.chart-loading').forEach(function(el) { el.classList.remove('hidden'); });
  }
  function hideChartLoadings() {
    document.querySelectorAll('.chart-loading').forEach(function(el) { el.classList.add('hidden'); });
  }

  function updateCharts(d, scope) {
    scope = scope || 'all';
    try {
      if (scope === 'all') {
        if (window.MIChartsSales) window.MIChartsSales.update(d);
        if (window.MIChartsPromo) window.MIChartsPromo.update(d);
        if (window.MIChartsPeak) window.MIChartsPeak.update(d);
        updatePromoKeyInsights(d);
        updatePeakKeyInsight(d);
      } else if (scope === 'category_pie' && window.MIChartsSales && window.MIChartsSales.updateCategoryPieOnly) {
        window.MIChartsSales.updateCategoryPieOnly(d);
      } else if (scope === 'subcategory_pie' && window.MIChartsSales && window.MIChartsSales.updateSubcategoryPieOnly) {
        window.MIChartsSales.updateSubcategoryPieOnly(d);
      } else if (scope === 'promo_share') {
        if (window.MIChartsPromo && window.MIChartsPromo.updatePromoShareOnly) {
          window.MIChartsPromo.updatePromoShareOnly(d);
        } else if (window.MIChartsPromo) {
          window.MIChartsPromo.update(d);
        }
        updatePromoShareKeyInsight(d);
      } else if (scope === 'promo_roi') {
        if (window.MIChartsPromo && window.MIChartsPromo.updatePromoRoiOnly) {
          window.MIChartsPromo.updatePromoRoiOnly(d);
        } else if (window.MIChartsPromo) {
          window.MIChartsPromo.update(d);
        }
        updatePromoRoiKeyInsight(d);
      } else if (scope === 'promo' && window.MIChartsPromo) {
        window.MIChartsPromo.update(d);
        updatePromoKeyInsights(d);
      } else if (scope === 'peak' && window.MIChartsPeak) {
        window.MIChartsPeak.update(d);
        updatePeakKeyInsight(d);
      }
    } catch (e) {
      if (typeof showError === 'function') showError('Failed to render: ' + (e && e.message));
    }
  }

  function buildCompositeFullData() {
    var byYear = window._miDataByYear || {};
    var catData = byYear[_scopeState.year_category_pie] || {};
    var subData = byYear[_scopeState.year_subcategory_pie] || {};
    var psData = byYear[_scopeState.year_promo_share] || {};
    var roiData = byYear[_scopeState.year_promo_roi] || {};
    var peakData = byYear[_scopeState.year_peak] || {};
    var composite = Object.assign({}, catData);
    composite.category_pie_brands_map_channel = catData.category_pie_brands_map_channel;
    composite.category_pie_brands_prev_map_channel = catData.category_pie_brands_prev_map_channel;
    composite.subcategory_pie_brands_map_channel = subData.subcategory_pie_brands_map_channel;
    composite.subcategory_pie_brands_prev_map_channel = subData.subcategory_pie_brands_prev_map_channel;
    composite.sales_value = catData.sales_value;
    composite.promo_share_by_category = psData.promo_share_by_category;
    composite.promo_share_by_category_channel = psData.promo_share_by_category_channel;
    composite.promo_share_by_subcategory_map = psData.promo_share_by_subcategory_map;
    composite.promo_share_by_subcategory_map_channel = psData.promo_share_by_subcategory_map_channel;
    composite.promo_roi = roiData.promo_roi;
    composite.promo_roi_map = roiData.promo_roi_map;
    composite.peak_events = peakData.peak_events || [];
    composite.peak_events_map = peakData.peak_events_map;
    composite.peak_events_map_channel = peakData.peak_events_map_channel;
    composite.discount_depth_selected_map = catData.discount_depth_selected_map;
    composite.competitor_name = catData.competitor_name || subData.competitor_name || psData.competitor_name || roiData.competitor_name || peakData.competitor_name || '';
    return composite;
  }

  function getChannelForScope(scope) {
    var key = scope === 'category_pie' ? 'channel_category_pie' : scope === 'subcategory_pie' ? 'channel_subcategory_pie' :
      scope === 'promo_share' ? 'channel_promo_share' : scope === 'promo_roi' ? 'channel_promo_roi' :
      scope === 'peak' ? 'channel_peak' : '';
    return _scopeState[key] || '';
  }

  function applyViewFromState(scope) {
    window._miFullData = buildCompositeFullData();
    var d = window._miFullData;
    if (!d) return;
    var view = Object.assign({}, d);
    var chCat = getChannelForScope('category_pie');
    var chSub = getChannelForScope('subcategory_pie');
    var chPs = getChannelForScope('promo_share');
    var chPeak = getChannelForScope('peak');
    var catMapChannel = d.category_pie_brands_map_channel || {};
    var subMapChannel = d.subcategory_pie_brands_map_channel || {};
    var catPrevMapChannel = d.category_pie_brands_prev_map_channel || {};
    var subPrevMapChannel = d.subcategory_pie_brands_prev_map_channel || {};
    var catMap = catMapChannel[chCat] || catMapChannel[''] || {};
    var subMap = subMapChannel[chSub] || subMapChannel[''] || {};
    var catPrevMap = catPrevMapChannel[chCat] || catPrevMapChannel[''] || {};
    var subPrevMap = subPrevMapChannel[chSub] || subPrevMapChannel[''] || {};
    var discMap = d.discount_depth_selected_map || {};
    view.category_pie_brands = catMap[_scopeState.category_pie_id] || [];
    view.subcategory_pie_brands = subMap[_scopeState.subcategory_pie_id] || [];
    view.category_pie_id = _scopeState.category_pie_id;
    view.subcategory_pie_id = _scopeState.subcategory_pie_id;
    view.subcategory_category_id = _scopeState.subcategory_category_id;
    view.category_pie_brands_prev_map = catPrevMap;
    view.subcategory_pie_brands_prev_map = subPrevMap;
    view.competitor_name = d.competitor_name || '';
    var discKey = _scopeState.disc_sub ? 'sub_' + _scopeState.disc_sub : (_scopeState.disc_cat ? 'cat_' + _scopeState.disc_cat : '');
    view.discount_depth_selected = discKey ? discMap[discKey] : null;
    var psCatMapCh = d.promo_share_by_category_channel || {};
    var psSubMapCh = d.promo_share_by_subcategory_map_channel || {};
    var psCatCh = (psCatMapCh[chPs] || psCatMapCh['']) || d.promo_share_by_category || [];
    var psSubCh = (psSubMapCh[chPs] || psSubMapCh['']) || d.promo_share_by_subcategory_map || {};
    if (_scopeState.promo_share_category_id && psSubCh[_scopeState.promo_share_category_id]) {
      view.promo_share_by_category = psSubCh[_scopeState.promo_share_category_id];
    } else {
      view.promo_share_by_category = Array.isArray(psCatCh) ? psCatCh : [];
    }
    var roiScopeKey = _scopeState.roi_sub ? 'sub_' + _scopeState.roi_sub : (_scopeState.roi_cat ? 'cat_' + _scopeState.roi_cat : '');
    var roiMap = d.promo_roi_map || {};
    var roiScoped = roiScopeKey ? roiMap[roiScopeKey] : null;
    if ((!Array.isArray(roiScoped) || !roiScoped.length) && _scopeState.roi_sub && _scopeState.roi_cat) {
      var roiCatKey = 'cat_' + _scopeState.roi_cat;
      var roiCatScoped = roiMap[roiCatKey];
      if (Array.isArray(roiCatScoped) && roiCatScoped.length) roiScoped = roiCatScoped;
    }
    view.promo_roi = (Array.isArray(roiScoped) && roiScoped.length) ? roiScoped : (roiMap[''] || []);
    var peakScopeKey = _scopeState.peak_sub ? 'sub_' + _scopeState.peak_sub : (_scopeState.peak_cat ? 'cat_' + _scopeState.peak_cat : '');
    var peakMapCh = d.peak_events_map_channel || {};
    var peakMap = (peakMapCh[chPeak] || peakMapCh[''] || peakMapCh[null]) || d.peak_events_map || {};
    var peakFromMap = (peakScopeKey && peakMap[peakScopeKey] && peakMap[peakScopeKey].length) ? peakMap[peakScopeKey] : (peakMap[''] || []);
    view.peak_events = (peakFromMap && peakFromMap.length) ? peakFromMap : (d.peak_events || []);
    window._miLastData = view;
    scope = scope || 'all';
    if (scope === 'all') {
      updateCharts(view);
      updateSummaryRow(view);
    } else if (scope === 'category_pie') {
      updateCharts(view, 'category_pie');
    } else if (scope === 'subcategory_pie') {
      updateCharts(view, 'subcategory_pie');
    } else if (scope === 'discount') {
      updateSummaryRow(view, 'discount');
    } else if (scope === 'sales') {
      updateCharts(view, 'category_pie');
      updateCharts(view, 'subcategory_pie');
      updateSummaryRow(view, 'all');
    } else if (scope === 'promo_share') {
      updateCharts(view, 'promo_share');
    } else if (scope === 'promo_roi') {
      updateCharts(view, 'promo_roi');
    } else if (scope === 'promo') {
      updateCharts(view, 'promo');
    } else if (scope === 'peak') {
      updateCharts(view, 'peak');
    }
  }

  var _scopeState = {
    year_category_pie: '', year_subcategory_pie: '', year_promo_share: '', year_promo_roi: '', year_peak: '',
    roi_cat: '', roi_sub: '', disc_cat: '', disc_sub: '', peak_cat: '', peak_sub: '',
    metric_cat: 'value', metric_sub: 'value',
    category_pie_id: '', subcategory_pie_id: '', subcategory_category_id: '',
    promo_share_category_id: '', competitor_id: '',
    channel_category_pie: '', channel_subcategory_pie: '', channel_promo_share: '', channel_promo_roi: '', channel_peak: ''
  };

  function onYearChange(scope, year) {
    var y = String(year || '');
    var byYear = window._miDataByYear || {};
    var compId = _scopeState.competitor_id;
    if (!byYear[y] && compId) {
      var url = core.buildAllUrl(y + '-01-01', y + '-12-31', compId, _scopeState.disc_cat, _scopeState.disc_sub);
      fetch(url, { credentials: 'include' }).then(function(r) { return r.json(); }).then(function(j) {
        if (j.error) return;
        window._miDataByYear[y] = j;
        if (scope === 'category_pie') _scopeState.year_category_pie = y;
        else if (scope === 'subcategory_pie') _scopeState.year_subcategory_pie = y;
        else if (scope === 'promo_share') _scopeState.year_promo_share = y;
        else if (scope === 'promo_roi') _scopeState.year_promo_roi = y;
        else if (scope === 'peak') _scopeState.year_peak = y;
        applyViewFromState(scope);
      });
      return;
    }
    if (!byYear[y]) return;
    if (scope === 'category_pie') _scopeState.year_category_pie = y;
    else if (scope === 'subcategory_pie') _scopeState.year_subcategory_pie = y;
    else if (scope === 'promo_share') _scopeState.year_promo_share = y;
    else if (scope === 'promo_roi') _scopeState.year_promo_roi = y;
    else if (scope === 'peak') _scopeState.year_peak = y;
    applyViewFromState(scope);
  }

  async function loadBase() {
    console.debug('[BC loadBase] START');
    var ps = '2023-01-01';
    var pe = '2025-12-31';
    if (window.MI_AVAILABLE_YEARS && window.MI_AVAILABLE_YEARS.length) {
      var firstY = window.MI_AVAILABLE_YEARS[0];
      var lastY = window.MI_AVAILABLE_YEARS[window.MI_AVAILABLE_YEARS.length - 1];
      ps = firstY + '-01-01';
      pe = lastY + '-12-31';
    }
    var url = core.buildCompetitorsUrl(ps, pe);
    console.debug('[BC loadBase] fetch', url);
    try {
      var r = await fetch(url, { credentials: 'include' });
      var resp = await r.json();
      console.debug('[BC loadBase] response', { ok: r.ok, status: r.status, hasError: !!resp.error, error: resp.error, competitorsCount: (resp.competitors || []).length });
      if (!r.ok || resp.error) {
        if (typeof showError === 'function') showError(resp.error || resp.detail || 'Request failed');
        return;
      }
      if (typeof showError === 'function') showError('');
      window._bcBase = resp;
      if (window.BCDropdowns && window.BCDropdowns.populateCompetitorOnly) {
        window.BCDropdowns.populateCompetitorOnly(resp.competitors || [], '', function(v) {
          _scopeState.competitor_id = v || '';
          loadData();
        });
      }
      var noDataEl = document.getElementById('mi-no-data');
      var chartsEl = document.getElementById('bc-charts');
      if (noDataEl) noDataEl.style.display = 'block';
      if (chartsEl) chartsEl.style.display = 'none';
      console.debug('[BC loadBase] SUCCESS, waiting for competitor selection');
    } catch (e) {
      console.debug('[BC loadBase] CATCH', e.message, e);
      if (typeof showError === 'function') showError('Failed to load base: ' + (e && e.message));
    }
  }

  async function loadDataCustomPeriod(ps, pe, labelHint) {
    var compId = _scopeState.competitor_id;
    var noDataEl = document.getElementById('mi-no-data');
    var chartsEl = document.getElementById('bc-charts');
    if (!compId) return;
    if (typeof showLoading === 'function') showLoading(true);
    showChartLoadings();
    try {
      var url = core.buildAllUrl(ps, pe, compId, _scopeState.disc_cat, _scopeState.disc_sub);
      var resp = await fetch(url, { credentials: 'include' }).then(function(r) { return r.json(); });
      if (resp.error) {
        if (typeof showError === 'function') showError(resp.error || resp.detail || 'Request failed');
        hideChartLoadings();
        if (typeof showLoading === 'function') showLoading(false);
        return;
      }
      window._bcCustomPeriod = true;
      document.body.classList.add('mi-custom-period');
      var slot = 'custom';
      window._miPeriodYearLabelMap = {};
      window._miPeriodYearLabelMap[slot] = labelHint || (ps + ' → ' + pe);
      window._miDataByYear = {};
      window._miDataByYear[slot] = resp;
      _scopeState.year_category_pie = slot;
      _scopeState.year_subcategory_pie = slot;
      _scopeState.year_promo_share = slot;
      _scopeState.year_promo_roi = slot;
      _scopeState.year_peak = slot;
      var fullData = resp;
      var metaForDropdowns = Object.assign({}, fullData, {
        available_years: [slot],
        competitors: window._bcBase ? (window._bcBase.competitors || []) : (fullData.competitors || [])
      });
      if (window.BCDropdowns) window.BCDropdowns.populate(metaForDropdowns, _scopeState, { applyViewFromState: applyViewFromState, loadData: loadData, onYearChange: onYearChange });
      applyViewFromState('all');
      updateSummaryRow(buildCompositeFullData());
      hideChartLoadings();
      if (chartsEl) chartsEl.style.display = '';
      if (noDataEl) noDataEl.style.display = 'none';
      if (typeof showLoading === 'function') showLoading(false);
      if (typeof showError === 'function') showError('');
    } catch (e) {
      if (typeof showError === 'function') showError('Failed to load: ' + (e && e.message));
      hideChartLoadings();
      if (typeof showLoading === 'function') showLoading(false);
    }
  }

  window.BCOnPeriodApply = function(ps, pe, mode, labelHint) {
    var compId = _scopeState.competitor_id;
    if (!compId) {
      if (typeof showError === 'function') showError('Select a competitor first');
      return;
    }
    if (mode === 'all_years') {
      window._bcCustomPeriod = false;
      window._miPeriodYearLabelMap = null;
      document.body.classList.remove('mi-custom-period');
      loadData();
      return;
    }
    var y = (ps && pe && ps.length >= 4) ? ps.slice(0, 4) : '';
    if (mode === 'year' && y && ps === y + '-01-01' && pe === y + '-12-31' && window._miDataByYear && window._miDataByYear[y] && !window._bcCustomPeriod) {
      _scopeState.year_category_pie = y;
      _scopeState.year_subcategory_pie = y;
      _scopeState.year_promo_share = y;
      _scopeState.year_promo_roi = y;
      _scopeState.year_peak = y;
      applyViewFromState('all');
      return;
    }
    loadDataCustomPeriod(ps, pe, labelHint);
  };

  async function loadData() {
    console.debug('[BC loadData] START', { competitor_id: _scopeState.competitor_id });
    var compId = _scopeState.competitor_id;
    var noDataEl = document.getElementById('mi-no-data');
    if (noDataEl) noDataEl.style.display = 'block';
    var chartsEl = document.getElementById('bc-charts');
    if (chartsEl) chartsEl.style.display = 'none';

    if (!compId) {
      console.debug('[BC loadData] SKIP: no competitor selected');
      if (noDataEl) noDataEl.textContent = 'Select a competitor to load data.';
      if (typeof showError === 'function') showError('');
      return;
    }

    if (typeof showLoading === 'function') showLoading(true);
    showChartLoadings();
    try {
      var allYearsUrl = core.buildAllYearsUrl(compId, _scopeState.disc_cat, _scopeState.disc_sub);
      console.debug('[BC loadData] fetch', allYearsUrl);
      var allRes = await fetch(allYearsUrl, { credentials: 'include' });
      var resp;
      try {
        resp = await allRes.json();
      } catch (parseErr) {
        console.debug('[BC loadData] JSON parse error', parseErr);
        if (noDataEl) noDataEl.textContent = 'Invalid response from server.';
        if (typeof showError === 'function') showError('Invalid response from server.');
        hideChartLoadings();
        if (typeof showLoading === 'function') showLoading(false);
        return;
      }
      var errMsg = resp.error || resp.detail || (Array.isArray(resp.detail) ? (resp.detail[0] && resp.detail[0].msg) : null);
      console.debug('[BC loadData] response', { ok: allRes.ok, status: allRes.status, hasError: !!resp.error, error: errMsg, byYearKeys: resp.by_year ? Object.keys(resp.by_year) : [] });
      if (!allRes.ok || resp.error) {
        if (typeof showError === 'function') showError(errMsg || 'Request failed');
        hideChartLoadings();
        if (typeof showLoading === 'function') showLoading(false);
        console.debug('[BC loadData] ERROR from API', errMsg);
        if (noDataEl) noDataEl.textContent = errMsg || 'Select a competitor to load data.';
        return;
      }
      if (typeof showError === 'function') showError('');
      window._bcCustomPeriod = false;
      window._miPeriodYearLabelMap = null;
      document.body.classList.remove('mi-custom-period');
      var byYear = resp.by_year || {};
      var availYears = resp.available_years || Object.keys(byYear);
      var defY = availYears.length ? String(availYears[availYears.length - 1]) : '';
      if (Object.keys(byYear).length === 0) {
        hideChartLoadings();
        if (typeof showLoading === 'function') showLoading(false);
        if (noDataEl) {
          noDataEl.style.display = 'block';
          noDataEl.textContent = 'No comparison data found for this competitor. Try another or ensure precalc tables are populated.';
        }
        if (chartsEl) chartsEl.style.display = 'none';
        console.debug('[BC loadData] empty by_year');
        return;
      }
      _scopeState.year_category_pie = defY;
      _scopeState.year_subcategory_pie = defY;
      _scopeState.year_promo_share = defY;
      _scopeState.year_promo_roi = defY;
      _scopeState.year_peak = defY;
      window._miDataByYear = byYear;
      window._miLastData = null;
      var fullData = byYear[defY] || byYear[Object.keys(byYear)[0]] || {};
      _scopeState.category_pie_id = fullData.category_pie_id || _scopeState.category_pie_id;
      _scopeState.subcategory_pie_id = fullData.subcategory_pie_id || _scopeState.subcategory_pie_id;
      _scopeState.subcategory_category_id = fullData.subcategory_category_id || _scopeState.subcategory_category_id;

      var metaForDropdowns = Object.assign({}, fullData, {
        available_years: resp.available_years || Object.keys(byYear),
        competitors: window._bcBase ? (window._bcBase.competitors || []) : (fullData.competitors || [])
      });
      if (window.BCDropdowns) window.BCDropdowns.populate(metaForDropdowns, _scopeState, { applyViewFromState: applyViewFromState, loadData: loadData, onYearChange: onYearChange });

      applyViewFromState('all');
      updateSummaryRow(buildCompositeFullData());
      hideChartLoadings();
      if (chartsEl) chartsEl.style.display = '';
      if (noDataEl) noDataEl.style.display = 'none';

      var d = buildCompositeFullData();
      var empty = !(d && (d.sales_value && d.sales_value.length) || (d.promo_share_by_category && d.promo_share_by_category.length) || (d.promo_roi && d.promo_roi.length) || (d.peak_events && d.peak_events.length));
      if (noDataEl) {
        noDataEl.style.display = empty ? 'block' : 'none';
        if (empty) noDataEl.textContent = 'No data available for the selected competitor.';
      }
      if (chartsEl && empty) chartsEl.style.display = 'none';
      console.debug('[BC loadData] SUCCESS', { years: Object.keys(byYear), defY: defY, empty: empty });

      if (typeof showLoading === 'function') showLoading(false);
    } catch (e) {
      console.debug('[BC loadData] CATCH', e.message, e);
      if (typeof showError === 'function') showError('Failed to load data: ' + (e && e.message));
      hideChartLoadings();
      if (typeof showLoading === 'function') showLoading(false);
      if (noDataEl) noDataEl.textContent = 'Failed to load. Ensure a competitor is selected.';
    }
  }

  function updatePromoShareKeyInsight(d) {
    var brandLabel = (typeof window.MI_BRAND_NAME !== 'undefined' && window.MI_BRAND_NAME) ? window.MI_BRAND_NAME : 'Your Brand';
    var compLabel = (d.competitor_name || d.second_series_label) || 'Competitor';
    var brandSpan = '<span class="mi-brand-label">' + brandLabel + '</span>';
    var ps = d.promo_share_by_category || [];
    var psEl = document.getElementById('mi-promo-share-key-insight');
    if (!psEl) return;
    if (!ps.length) {
      psEl.innerHTML = '<div class="mi-ranking">No data available.</div>';
      return;
    }
    var brandAvg = ps.reduce(function(s, r) { return s + (Number(r.brand_promo_share_pct) || 0); }, 0) / ps.length;
    var mediaAvg = ps.reduce(function(s, r) { return s + (Number(r.media_promo_share_pct) || 0); }, 0) / ps.length;
    var diff = (brandAvg - mediaAvg).toFixed(1);
    var topRow = ps.reduce(function(a, b) { return (Number(b.brand_promo_share_pct) || 0) > (Number(a.brand_promo_share_pct) || 0) ? b : a; }, ps[0]);
    var rows = '<div class="mi-info-row"><span class="mi-info-label">' + brandSpan + ' avg promo share</span><span class="mi-info-value">' + brandAvg.toFixed(1) + '%</span></div>' +
      '<div class="mi-info-row"><span class="mi-info-label">' + compLabel + '</span><span class="mi-info-value">' + mediaAvg.toFixed(1) + '%</span></div>' +
      '<div class="mi-info-row"><span class="mi-info-label">Gap vs ' + compLabel + '</span><span class="mi-info-value">' + (diff > 0 ? '+' : '') + diff + ' pp</span></div>' +
      '<div class="mi-info-row"><span class="mi-info-label">Top for ' + brandSpan + '</span><span class="mi-info-value">' + (topRow.category_name || '—') + ' (' + (Number(topRow.brand_promo_share_pct) || 0).toFixed(1) + '%)</span></div>';
    var msg = diff > 0 ? brandSpan + ' exceeds ' + compLabel + ' by ' + Math.abs(diff) + ' pp.' : (diff < 0 ? brandSpan + ' is below ' + compLabel + ' by ' + Math.abs(diff) + ' pp.' : 'Promo share in line with ' + compLabel + '.');
    psEl.innerHTML = rows + '<div class="mi-ranking">' + msg + '</div>';
  }

  function updatePromoRoiKeyInsight(d) {
    var brandLabel = (typeof window.MI_BRAND_NAME !== 'undefined' && window.MI_BRAND_NAME) ? window.MI_BRAND_NAME : 'Your Brand';
    var compLabel = (d.competitor_name || d.second_series_label) || 'Competitor';
    var brandSpan = '<span class="mi-brand-label">' + brandLabel + '</span>';
    var roi = d.promo_roi || [];
    var roiEl = document.getElementById('mi-promo-roi-key-insight');
    if (!roiEl) return;
    if (!roi.length) {
      roiEl.innerHTML = '<div class="mi-ranking">No data available.</div>';
      return;
    }
    var sorted = roi.slice().sort(function(a, b) { return (Number(b.brand_avg_roi) || 0) - (Number(a.brand_avg_roi) || 0); });
    var best = sorted[0];
    var top3 = sorted.slice(0, 3);
    var rows = '<div class="mi-info-row"><span class="mi-info-label">Best promo type</span><span class="mi-info-value">' + (best.promo_type || '—') + '</span></div>' +
      '<div class="mi-info-row"><span class="mi-info-label">' + brandSpan + ' ROI</span><span class="mi-info-value">' + (best.brand_avg_roi != null ? Number(best.brand_avg_roi).toFixed(2) : '—') + '</span></div>' +
      '<div class="mi-info-row"><span class="mi-info-label">' + compLabel + ' ROI</span><span class="mi-info-value">' + (best.media_avg_roi != null ? Number(best.media_avg_roi).toFixed(2) : '—') + '</span></div>';
    var tableRows = top3.map(function(r) {
      return '<div class="mi-info-row"><span class="mi-info-label">' + (r.promo_type || '—') + '</span><span class="mi-info-value">' + (Number(r.brand_avg_roi) || 0).toFixed(2) + ' / ' + (Number(r.media_avg_roi) || 0).toFixed(2) + '</span></div>';
    }).join('');
    var msg = 'Highest ROI for ' + brandSpan + ': ' + (best.promo_type || '—') + ' (' + (best.brand_avg_roi != null ? Number(best.brand_avg_roi).toFixed(2) : '—') + ').';
    roiEl.innerHTML = rows + '<div class="mi-info-label" style="margin-top:.5rem;">Top 3 (' + brandSpan + ' / ' + compLabel + ')</div>' + tableRows + '<div class="mi-ranking">' + msg + '</div>';
  }

  function updatePromoKeyInsights(d) {
    updatePromoShareKeyInsight(d);
    updatePromoRoiKeyInsight(d);
  }

  function updatePeakKeyInsight(d) {
    var peak = d.peak_events || [];
    var peakEl = document.getElementById('mi-peak-key-insight');
    if (!peakEl) return;
    var brandLabel = (typeof window.MI_BRAND_NAME !== 'undefined' && window.MI_BRAND_NAME) ? window.MI_BRAND_NAME : 'Your Brand';
    var compLabel = (d.competitor_name || d.second_series_label) || 'Competitor';
    var brandSpan = '<span class="mi-brand-label">' + brandLabel + '</span>';
    if (!peak.length) {
      peakEl.innerHTML = '<div class="mi-ranking">No data available.</div>';
      return;
    }
    var sorted = peak.slice().sort(function(a, b) { return (Number(b.brand_pct_of_annual) || 0) - (Number(a.brand_pct_of_annual) || 0); });
    var best = sorted[0];
    var top3 = sorted.slice(0, 3);
    var rows = '<div class="mi-info-row"><span class="mi-info-label">Main peak event</span><span class="mi-info-value">' + (best.peak_event || '—') + '</span></div>' +
      '<div class="mi-info-row"><span class="mi-info-label">' + brandSpan + ' %</span><span class="mi-info-value">' + (best.brand_pct_of_annual != null ? Number(best.brand_pct_of_annual).toFixed(1) : '—') + '%</span></div>' +
      '<div class="mi-info-row"><span class="mi-info-label">' + compLabel + ' %</span><span class="mi-info-value">' + (best.media_pct_of_annual != null ? Number(best.media_pct_of_annual).toFixed(1) : '—') + '%</span></div>';
    var tableRows = top3.map(function(r) {
      return '<div class="mi-info-row"><span class="mi-info-label">' + (r.peak_event || '—') + '</span><span class="mi-info-value">' + (Number(r.brand_pct_of_annual) || 0).toFixed(1) + '% / ' + (Number(r.media_pct_of_annual) || 0).toFixed(1) + '%</span></div>';
    }).join('');
    var msg = 'Highest share for ' + brandSpan + ': ' + (best.peak_event || '—') + ' (' + (best.brand_pct_of_annual != null ? Number(best.brand_pct_of_annual).toFixed(1) : '—') + '% of annual).';
    peakEl.innerHTML = rows + '<div class="mi-info-label" style="margin-top:.5rem;">Top 3 (' + brandSpan + ' / ' + compLabel + ')</div>' + tableRows + '<div class="mi-ranking">' + msg + '</div>';
  }

  function updateSummaryRow(d, scope) {
    scope = scope || 'all';
    var compLabel = (d.competitor_name || d.second_series_label) || 'Competitor';
    if (scope !== 'discount') {
      var chipsEl = document.getElementById('mi-revenue-chips');
      if (chipsEl) {
        var sv = d.sales_value || [];
        var total = sv.reduce(function(s, r) { return s + (Number(r.brand_gross_pln) || 0); }, 0);
        if (total > 0) {
          chipsEl.innerHTML = sv.map(function(r) {
            var pct = (100 * (Number(r.brand_gross_pln) || 0) / total).toFixed(1);
            return '<span class="mi-revenue-chip">' + (r.category_name || '') + ': ' + pct + '%</span>';
          }).join('');
        } else {
          chipsEl.textContent = '—';
        }
      }
    }
    if (scope !== 'chips') {
      var el = document.getElementById('mi-discount-depth-text');
      if (el) {
        var sel = d.discount_depth_selected;
        if (sel) {
          var brandLabel = (typeof window.MI_BRAND_NAME !== 'undefined' && window.MI_BRAND_NAME) ? window.MI_BRAND_NAME : 'Your Brand';
          el.innerHTML = '<span class="mi-brand-label">' + brandLabel + '</span>: ' + (sel.brand_avg_discount_depth || 0) + '%  |  ' + compLabel + ': ' + (sel.media_avg_discount_depth || 0) + '%';
          el.classList.remove('empty');
        } else {
          el.textContent = 'Select a category or subcategory.';
          el.classList.add('empty');
        }
      }
    }
  }

  function onResize() {
    var ids = ['chart-category-pie', 'chart-subcategory-pie', 'chart-promo-share', 'chart-promo-roi', 'chart-peak'];
    ids.forEach(function(id) {
      var el = document.getElementById(id);
      if (el && el.chart && typeof el.chart.resize === 'function') el.chart.resize();
    });
  }
  if (typeof window.addEventListener === 'function') {
    window.addEventListener('resize', onResize);
  }

  window.loadData = loadData;
  window.onYearChange = onYearChange;
  window.MIScopeState = _scopeState;
  initCharts();
  console.debug('[BC dashboard] calling loadBase()');
  loadBase();
})();
