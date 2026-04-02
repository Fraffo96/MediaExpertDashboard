/**
 * Market Intelligence – entry: init charts, loadData, wire dropdowns.
 */
(function() {
  console.debug('[MI dashboard] init');
  var core = window.MICore;
  if (!core) {
    console.debug('[MI dashboard] SKIP: MICore not found');
    return;
  }

  function initCharts() {
    if (typeof Chart === 'undefined') return;
    var barOpts = { type: 'bar', data: { labels: [], datasets: [] }, options: core.MI_BAR_OPT || (typeof BAR_OPT !== 'undefined' ? BAR_OPT : {}) };
    ['chart-promo-share', 'chart-promo-roi', 'chart-peak', 'chart-incremental-yoy'].forEach(function(id) {
      var c = document.getElementById(id);
      if (c && !c.chart) c.chart = new Chart(c.getContext('2d'), barOpts);
    });
    var pieOpts = { type: 'pie', data: { labels: [], datasets: [] }, options: core.MI_PIE_OPT || {} };
    ['chart-category-pie', 'chart-subcategory-pie'].forEach(function(id) {
      var c = document.getElementById(id);
      if (c && !c.chart && !c.segmentChart) c.chart = new Chart(c.getContext('2d'), pieOpts);
    });
  }

  function showChartLoadings() {
    document.querySelectorAll('.chart-loading').forEach(function(el) { el.classList.remove('hidden'); });
  }
  function hideChartLoadings() {
    document.querySelectorAll('.chart-loading').forEach(function(el) { el.classList.add('hidden'); });
  }
  function hideChartLoading(scope) {
    document.querySelectorAll('.mi-charts .chart-loading[data-chart-scope="' + scope + '"]').forEach(function(el) { el.classList.add('hidden'); });
  }

  function updateCharts(d, scope) {
    scope = scope || 'all';
    try {
      if (scope === 'all') {
        if (window.MIChartsSales) window.MIChartsSales.update(d);
        if (window.MIChartsPromo) window.MIChartsPromo.update(d);
        if (window.MIChartsPeak) window.MIChartsPeak.update(d);
        if (window.MIChartsIncrementalYoy) window.MIChartsIncrementalYoy.update(d);
        updatePromoKeyInsights(d);
        updatePeakKeyInsight(d);
        updateIncrementalYoYKeyInsight(d);
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
      } else if (scope === 'incremental_yoy') {
        if (window.MIChartsIncrementalYoy) window.MIChartsIncrementalYoy.update(d);
        updateIncrementalYoYKeyInsight(d);
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
    composite.incremental_yoy_map = window._miIncrementalYoY || {};
    composite.incremental_yoy_map_channel = window._miIncrementalYoYChannel || {};
    return composite;
  }

  function getChannelForScope(scope) {
    var key = scope === 'category_pie' ? 'channel_category_pie' : scope === 'subcategory_pie' ? 'channel_subcategory_pie' :
      scope === 'promo_share' ? 'channel_promo_share' : scope === 'promo_roi' ? 'channel_promo_roi' :
      scope === 'peak' ? 'channel_peak' : scope === 'incremental_yoy' ? 'channel_incremental_yoy' : '';
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
    var chIncr = getChannelForScope('incremental_yoy');
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
    var incrYoyScopeKey = _scopeState.incr_yoy_sub ? 'sub_' + _scopeState.incr_yoy_sub : (_scopeState.incr_yoy_cat ? 'cat_' + _scopeState.incr_yoy_cat : '');
    var incrYoyMapCh = d.incremental_yoy_map_channel || {};
    var incrYoyMap = (incrYoyMapCh[chIncr] || incrYoyMapCh[''] || incrYoyMapCh[null]) || d.incremental_yoy_map || {};
    view.incremental_yoy = (incrYoyScopeKey && incrYoyMap[incrYoyScopeKey] && incrYoyMap[incrYoyScopeKey].length) ? incrYoyMap[incrYoyScopeKey] : (incrYoyMap[''] || []);

    var peakScopeKey = _scopeState.peak_sub ? 'sub_' + _scopeState.peak_sub : (_scopeState.peak_cat ? 'cat_' + _scopeState.peak_cat : '');
    var peakMapCh = d.peak_events_map_channel || {};
    var peakMap = (peakMapCh[chPeak] || peakMapCh[''] || peakMapCh[null]) || d.peak_events_map || {};
    var peakFromMap = (peakScopeKey && peakMap[peakScopeKey] && peakMap[peakScopeKey].length) ? peakMap[peakScopeKey] : (peakMap[''] || []);
    view.peak_events = (peakFromMap && peakFromMap.length) ? peakFromMap : (d.peak_events || []);
    if (window._miDebugPeak) {
      console.log('[MI Peak Debug] scope=' + scope + ' peakScopeKey=' + peakScopeKey + ' ch=' + JSON.stringify(ch) +
        ' peakMapKeys=' + Object.keys(peakMap).join(',') + ' peakFromMapLen=' + (peakFromMap ? peakFromMap.length : 0) +
        ' d.peak_eventsLen=' + (d.peak_events ? d.peak_events.length : 0) + ' view.peak_eventsLen=' + (view.peak_events ? view.peak_events.length : 0) +
        ' year_peak=' + _scopeState.year_peak);
      if (view.peak_events && view.peak_events[0]) console.log('[MI Peak Debug] first item:', view.peak_events[0]);
    }
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
    } else if (scope === 'incremental_yoy') {
      updateCharts(view, 'incremental_yoy');
    }
  }

  var _scopeState = {
    year_category_pie: '', year_subcategory_pie: '', year_promo_share: '', year_promo_roi: '', year_peak: '',
    roi_cat: '', roi_sub: '', disc_cat: '', disc_sub: '', peak_cat: '', peak_sub: '',
    incr_yoy_cat: '', incr_yoy_sub: '',
    metric_cat: 'value', metric_sub: 'value',
    category_pie_id: '', subcategory_pie_id: '', subcategory_category_id: '',
    promo_share_category_id: '',
    channel_category_pie: '', channel_subcategory_pie: '', channel_promo_share: '', channel_promo_roi: '', channel_peak: '', channel_incremental_yoy: ''
  };

  function buildBaseUrl(ps, pe) {
    return '/api/market-intelligence/base?period_start=' + encodeURIComponent(ps) + '&period_end=' + encodeURIComponent(pe);
  }
  function buildAllUrl(ps, pe, discCat, discSub) {
    var q = ['period_start=' + encodeURIComponent(ps), 'period_end=' + encodeURIComponent(pe)];
    if (discCat) q.push('discount_category_id=' + encodeURIComponent(discCat));
    if (discSub) q.push('discount_subcategory_id=' + encodeURIComponent(discSub));
    return '/api/market-intelligence/all?' + q.join('&');
  }
  function buildAllYearsUrl(discCat, discSub) {
    var q = [];
    if (discCat) q.push('discount_category_id=' + encodeURIComponent(discCat));
    if (discSub) q.push('discount_subcategory_id=' + encodeURIComponent(discSub));
    return '/api/market-intelligence/all-years' + (q.length ? '?' + q.join('&') : '');
  }
  function buildSalesUrl(ps, pe, catIds, subIds, subCatId) {
    var q = ['period_start=' + encodeURIComponent(ps), 'period_end=' + encodeURIComponent(pe)];
    if (catIds && catIds.length) q.push('cat_ids=' + encodeURIComponent(catIds.join(',')));
    if (subIds && subIds.length) q.push('sub_ids=' + encodeURIComponent(subIds.join(',')));
    if (subCatId) q.push('subcategory_category_id=' + encodeURIComponent(subCatId));
    return '/api/market-intelligence/sales?' + q.join('&');
  }
  function buildPromoUrl(ps, pe) {
    return '/api/market-intelligence/promo?period_start=' + encodeURIComponent(ps) + '&period_end=' + encodeURIComponent(pe);
  }
  function buildPeakUrl(ps, pe) {
    return '/api/market-intelligence/peak?period_start=' + encodeURIComponent(ps) + '&period_end=' + encodeURIComponent(pe);
  }
  function buildDiscountUrl(ps, pe, discCat, discSub) {
    var q = ['period_start=' + encodeURIComponent(ps), 'period_end=' + encodeURIComponent(pe)];
    if (discCat) q.push('discount_category_id=' + encodeURIComponent(discCat));
    if (discSub) q.push('discount_subcategory_id=' + encodeURIComponent(discSub));
    return '/api/market-intelligence/discount?' + q.join('&');
  }

  function fetchJson(url) {
    return fetch(url, { credentials: 'include' }).then(function(r) { return r.json(); });
  }

  /** Cambio year: da cache se presente, altrimenti GET /all per singolo anno. */
  function onYearChange(scope, year) {
    var y = String(year || '');
    var byYear = window._miDataByYear || {};
    if (!byYear[y]) {
      var url = buildAllUrl(y + '-01-01', y + '-12-31', _scopeState.disc_cat, _scopeState.disc_sub);
      fetch(url, { credentials: 'include' }).then(function(r) { return r.json(); }).then(function(j) {
        if (j.error) return;
        window._miDataByYear[y] = j;
        if (scope === 'category_pie') _scopeState.year_category_pie = y;
        else if (scope === 'subcategory_pie') _scopeState.year_subcategory_pie = y;
        else if (scope === 'promo_share') _scopeState.year_promo_share = y;
        else if (scope === 'promo_roi') _scopeState.year_promo_roi = y;
        else if (scope === 'peak') _scopeState.year_peak = y;
        else if (scope === 'segment_sku' && window.MIScopeState) window.MIScopeState.year_segment_sku = y;
        applyViewFromState(scope);
      });
      return;
    }
    if (scope === 'category_pie') _scopeState.year_category_pie = y;
    else if (scope === 'subcategory_pie') _scopeState.year_subcategory_pie = y;
    else if (scope === 'promo_share') _scopeState.year_promo_share = y;
    else if (scope === 'promo_roi') _scopeState.year_promo_roi = y;
    else if (scope === 'peak') _scopeState.year_peak = y;
    else if (scope === 'segment_sku' && window.MIScopeState) window.MIScopeState.year_segment_sku = y;
    applyViewFromState(scope);
  }

  async function loadDataCustomPeriod(ps, pe, labelHint) {
    if (typeof showLoadingLight === 'function') showLoadingLight(true);
    showChartLoadings();
    try {
      var url = buildAllUrl(ps, pe, _scopeState.disc_cat, _scopeState.disc_sub);
      var resp = await fetchJson(url);
      if (resp.error) {
        if (typeof showError === 'function') showError(resp.error || resp.detail || 'Request failed');
        hideChartLoadings();
        if (typeof showLoadingLight === 'function') showLoadingLight(false);
        return;
      }
      window._miCustomPeriod = true;
      document.body.classList.add('mi-custom-period');
      var slot = 'custom';
      window._miPeriodYearLabelMap = {};
      window._miPeriodYearLabelMap[slot] = labelHint || (ps + ' → ' + pe);
      window._miIncrementalYoY = {};
      window._miIncrementalYoYChannel = {};
      window._miDataByYear = {};
      window._miDataByYear[slot] = resp;
      _scopeState.year_category_pie = slot;
      _scopeState.year_subcategory_pie = slot;
      _scopeState.year_promo_share = slot;
      _scopeState.year_promo_roi = slot;
      _scopeState.year_peak = slot;
      if (window.MIScopeState) window.MIScopeState.year_segment_sku = ps.slice(0, 4);
      var metaForDropdowns = Object.assign({}, resp, { available_years: [slot] });
      window._miPopulateInProgress = true;
      if (window.MIDropdowns) window.MIDropdowns.populate(metaForDropdowns, _scopeState, { applyViewFromState: applyViewFromState, loadData: loadData, onYearChange: onYearChange });
      window._miPopulateInProgress = false;
      if (window.MIChartsSegmentSku && window.MIChartsSegmentSku.init) {
        window.MIChartsSegmentSku.init(_scopeState, resp.brand_categories || [], resp.brand_subcategories || {}, { applyViewFromState: applyViewFromState });
      }
      applyViewFromState('all');
      updateSummaryRow(buildCompositeFullData());
      hideChartLoadings();
      if (typeof showLoadingLight === 'function') showLoadingLight(false);
      if (typeof showError === 'function') showError('');
    } catch (e) {
      if (typeof showError === 'function') showError('Failed to load: ' + (e && e.message));
      hideChartLoadings();
      if (typeof showLoadingLight === 'function') showLoadingLight(false);
    }
  }

  window.MIOnPeriodApply = function(ps, pe, mode, labelHint) {
    if (mode === 'all_years') {
      window._miCustomPeriod = false;
      window._miPeriodYearLabelMap = null;
      document.body.classList.remove('mi-custom-period');
      loadData();
      return;
    }
    var y = (ps && pe && ps.length >= 4) ? ps.slice(0, 4) : '';
    if (mode === 'year' && y && ps === y + '-01-01' && pe === y + '-12-31' && window._miDataByYear && window._miDataByYear[y] && !window._miCustomPeriod) {
      _scopeState.year_category_pie = y;
      _scopeState.year_subcategory_pie = y;
      _scopeState.year_promo_share = y;
      _scopeState.year_promo_roi = y;
      _scopeState.year_peak = y;
      if (window.MIScopeState) window.MIScopeState.year_segment_sku = y;
      applyViewFromState('all');
      return;
    }
    loadDataCustomPeriod(ps, pe, labelHint);
  };

  async function loadData(extraParams) {
    console.debug('[MI loadData] START', { extraParams: extraParams });
    if (extraParams && Object.keys(extraParams).length > 0) {
      if (extraParams.category_pie_id !== undefined) _scopeState.category_pie_id = String(extraParams.category_pie_id || '');
      if (extraParams.subcategory_pie_id !== undefined) _scopeState.subcategory_pie_id = String(extraParams.subcategory_pie_id || '');
      if (extraParams.subcategory_category_id !== undefined) _scopeState.subcategory_category_id = String(extraParams.subcategory_category_id || '');
      if (extraParams.roi_category_id !== undefined) _scopeState.roi_cat = String(extraParams.roi_category_id || '');
      if (extraParams.roi_subcategory_id !== undefined) _scopeState.roi_sub = String(extraParams.roi_subcategory_id || '');
      if (extraParams.discount_category_id !== undefined) _scopeState.disc_cat = String(extraParams.discount_category_id || '');
      if (extraParams.discount_subcategory_id !== undefined) _scopeState.disc_sub = String(extraParams.discount_subcategory_id || '');
      if (extraParams.peak_category_id !== undefined) _scopeState.peak_cat = String(extraParams.peak_category_id || '');
      if (extraParams.peak_subcategory_id !== undefined) _scopeState.peak_sub = String(extraParams.peak_subcategory_id || '');
      if (extraParams.promo_share_category_id !== undefined) _scopeState.promo_share_category_id = String(extraParams.promo_share_category_id || '');
      if (extraParams.year === undefined && window._miFullData) {
        console.debug('[MI loadData] early return: year unchanged, use cache');
        applyViewFromState();
        return;
      }
    }

    if (typeof showLoadingLight === 'function') showLoadingLight(true);
    showChartLoadings();
    try {
      console.debug('[MI loadData] fetch all-years (single round-trip)');
      var allYearsUrl = buildAllYearsUrl(_scopeState.disc_cat, _scopeState.disc_sub);
      var resp = await fetchJson(allYearsUrl);
      if (resp.error) {
        if (typeof showError === 'function') showError(resp.error || resp.detail || 'Request failed');
        hideChartLoadings();
        if (typeof showLoadingLight === 'function') showLoadingLight(false);
        return;
      }
      if (typeof showError === 'function') showError('');
      window._miCustomPeriod = false;
      window._miPeriodYearLabelMap = null;
      document.body.classList.remove('mi-custom-period');
      var availYears = resp.available_years || [];
      if (!availYears.length) {
        if (typeof showError === 'function') showError('No years available');
        hideChartLoadings();
        if (typeof showLoadingLight === 'function') showLoadingLight(false);
        return;
      }
      var defY = String(availYears[availYears.length - 1]);
      var byYear = resp.by_year || {};
      if (!byYear || Object.keys(byYear).length === 0) {
        if (typeof showError === 'function') showError(resp.error || 'No data for available years');
        hideChartLoadings();
        if (typeof showLoadingLight === 'function') showLoadingLight(false);
        return;
      }
      window._miIncrementalYoY = resp.incremental_yoy_map || {};
      window._miIncrementalYoYChannel = resp.incremental_yoy_map_channel || {};
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

      var metaForDropdowns = Object.assign({}, fullData, { available_years: availYears });
      window._miPopulateInProgress = true;
      if (window.MIDropdowns) window.MIDropdowns.populate(metaForDropdowns, _scopeState, { applyViewFromState: applyViewFromState, loadData: loadData, onYearChange: onYearChange });
      window._miPopulateInProgress = false;

      if (window.MIChartsSegmentSku && window.MIChartsSegmentSku.init) {
        window.MIChartsSegmentSku.init(_scopeState, fullData.brand_categories || [], fullData.brand_subcategories || {}, { applyViewFromState: applyViewFromState });
      }

      applyViewFromState('all');
      updateSummaryRow(buildCompositeFullData());
      hideChartLoadings();
      console.debug('[MI loadData] SUCCESS', { years: Object.keys(byYear), defY: defY });

      if (typeof showLoadingLight === 'function') showLoadingLight(false);
      var noData = document.getElementById('mi-no-data');
      if (noData) {
        var d = buildCompositeFullData();
        var empty = !(d && (d.sales_value && d.sales_value.length) || (d.promo_share_by_category && d.promo_share_by_category.length) || (d.promo_roi && d.promo_roi.length) || (d.peak_events && d.peak_events.length) || (d.incremental_yoy && d.incremental_yoy.length));
        noData.style.display = empty ? 'block' : 'none';
      }
    } catch (e) {
      console.debug('[MI loadData] CATCH', e.message, e);
      if (typeof showError === 'function') showError('Failed to load data: ' + (e && e.message));
      hideChartLoadings();
      if (typeof showLoadingLight === 'function') showLoadingLight(false);
    }
  }

  function updatePromoShareKeyInsight(d) {
    var brandLabel = (typeof window.MI_BRAND_NAME !== 'undefined' && window.MI_BRAND_NAME) ? window.MI_BRAND_NAME : 'Your Brand';
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
      '<div class="mi-info-row"><span class="mi-info-label">Category avg</span><span class="mi-info-value">' + mediaAvg.toFixed(1) + '%</span></div>' +
      '<div class="mi-info-row"><span class="mi-info-label">Gap vs category</span><span class="mi-info-value">' + (diff > 0 ? '+' : '') + diff + ' pp</span></div>' +
      '<div class="mi-info-row"><span class="mi-info-label">Top for ' + brandSpan + '</span><span class="mi-info-value">' + (topRow.category_name || '—') + ' (' + (Number(topRow.brand_promo_share_pct) || 0).toFixed(1) + '%)</span></div>';
    var msg = diff > 0 ? brandSpan + ' exceeds category average by ' + Math.abs(diff) + ' pp.' : (diff < 0 ? brandSpan + ' is below category average by ' + Math.abs(diff) + ' pp.' : 'Promo share in line with category average.');
    psEl.innerHTML = rows + '<div class="mi-ranking">' + msg + '</div>';
  }

  function updatePromoRoiKeyInsight(d) {
    var brandLabel = (typeof window.MI_BRAND_NAME !== 'undefined' && window.MI_BRAND_NAME) ? window.MI_BRAND_NAME : 'Your Brand';
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
      '<div class="mi-info-row"><span class="mi-info-label">Category avg ROI</span><span class="mi-info-value">' + (best.media_avg_roi != null ? Number(best.media_avg_roi).toFixed(2) : '—') + '</span></div>';
    var tableRows = top3.map(function(r) {
      return '<div class="mi-info-row"><span class="mi-info-label">' + (r.promo_type || '—') + '</span><span class="mi-info-value">' + (Number(r.brand_avg_roi) || 0).toFixed(2) + ' / ' + (Number(r.media_avg_roi) || 0).toFixed(2) + '</span></div>';
    }).join('');
    var msg = 'Highest ROI for ' + brandSpan + ': ' + (best.promo_type || '—') + ' (' + (best.brand_avg_roi != null ? Number(best.brand_avg_roi).toFixed(2) : '—') + ').';
    roiEl.innerHTML = rows + '<div class="mi-info-label" style="margin-top:.5rem;">Top 3 (' + brandSpan + ' / category)</div>' + tableRows + '<div class="mi-ranking">' + msg + '</div>';
  }

  function updatePromoKeyInsights(d) {
    updatePromoShareKeyInsight(d);
    updatePromoRoiKeyInsight(d);
  }

  function updateIncrementalYoYKeyInsight(d) {
    var rows = d.incremental_yoy || [];
    var el = document.getElementById('mi-incremental-yoy-key-insight');
    if (!el) return;
    var brandLabel = (typeof window.MI_BRAND_NAME !== 'undefined' && window.MI_BRAND_NAME) ? window.MI_BRAND_NAME : 'Your Brand';
    var brandSpan = '<span class="mi-brand-label">' + brandLabel + '</span>';
    if (!rows.length) {
      el.innerHTML = '<div class="mi-ranking">No data available.</div>';
      return;
    }
    var totalGross = rows.reduce(function(s, r) { return s + (Number(r.total_gross) || 0); }, 0);
    var totalIncr = rows.reduce(function(s, r) { return s + (Number(r.incremental_sales_pln) || Number(r.promo_gross) || 0); }, 0);
    var incrPct = totalGross > 0 ? (100 * totalIncr / totalGross).toFixed(1) : '0';
    var bestYearIncr = rows.reduce(function(a, b) { return (Number(b.incremental_sales_pln) || Number(b.promo_gross) || 0) > (Number(a.incremental_sales_pln) || Number(a.promo_gross) || 0) ? b : a; }, rows[0]);
    var yoyIncrGrowth = '';
    if (rows.length >= 2) {
      var first = rows[0];
      var last = rows[rows.length - 1];
      var firstIncr = Number(first.incremental_sales_pln) || Number(first.promo_gross) || 0;
      var lastIncr = Number(last.incremental_sales_pln) || Number(last.promo_gross) || 0;
      var growth = firstIncr > 0 ? (100 * (lastIncr - firstIncr) / firstIncr).toFixed(1) : '0';
      yoyIncrGrowth = growth > 0 ? '+' + growth + '%' : growth + '%';
    }
    function fmt(n) { return (Number(n) || 0).toLocaleString('en-US', { maximumFractionDigits: 0 }); }
    function fmtM(n) { var x = Number(n) || 0; return (x / 1e6).toFixed(0) + 'M'; }
    var rowsHtml = '<div class="mi-info-row"><span class="mi-info-label">Incremental sales</span><span class="mi-info-value highlight">' + fmt(totalIncr) + ' PLN</span></div>' +
      '<div class="mi-info-row"><span class="mi-info-label">Incremental % of total</span><span class="mi-info-value">' + incrPct + '%</span></div>' +
      '<div class="mi-info-row"><span class="mi-info-label">Best year (incremental)</span><span class="mi-info-value">' + (bestYearIncr.year || '—') + ' (' + fmt(bestYearIncr.incremental_sales_pln || bestYearIncr.promo_gross) + ' PLN)</span></div>';
    if (yoyIncrGrowth) {
      rowsHtml += '<div class="mi-info-row"><span class="mi-info-label">YoY incremental growth</span><span class="mi-info-value">' + yoyIncrGrowth + '</span></div>';
    }
    var totalPromo = rows.reduce(function(s, r) { return s + (Number(r.promo_gross) || 0); }, 0);
    var promoSharePct = totalGross > 0 ? Math.round(100 * totalPromo / totalGross) : 0;
    var totalSalesArr = rows.map(function(r) { return Number(r.total_gross) || 0; }).filter(function(x) { return x > 0; });
    var salesRange = totalSalesArr.length ? '~' + fmtM(Math.min.apply(null, totalSalesArr)) + '–' + fmtM(Math.max.apply(null, totalSalesArr)) + 'M' : '';
    var bestYearAmount = Number(bestYearIncr.incremental_sales_pln) || Number(bestYearIncr.promo_gross) || 0;
    var msgParts = [];
    msgParts.push('Promo-driven: ' + promoSharePct + '% of sales from promotions.');
    msgParts.push((bestYearIncr.year || '—') + ' strongest incremental (' + fmtM(bestYearAmount) + ' PLN).');
    if (salesRange) msgParts.push('Total sales stable ' + salesRange + ' PLN/year.');
    if (yoyIncrGrowth && parseFloat(yoyIncrGrowth) < 0) msgParts.push('Incremental impact declining YoY (' + yoyIncrGrowth + ').');
    else if (yoyIncrGrowth) msgParts.push('Incremental impact growing YoY (' + yoyIncrGrowth + ').');
    var msg = msgParts.join(' ');
    el.innerHTML = rowsHtml + '<div class="mi-ranking">' + msg + '</div>';
  }

  function updatePeakKeyInsight(d) {
    var peak = d.peak_events || [];
    var peakEl = document.getElementById('mi-peak-key-insight');
    if (!peakEl) return;
    var brandLabel = (typeof window.MI_BRAND_NAME !== 'undefined' && window.MI_BRAND_NAME) ? window.MI_BRAND_NAME : 'Your Brand';
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
      '<div class="mi-info-row"><span class="mi-info-label">Category avg %</span><span class="mi-info-value">' + (best.media_pct_of_annual != null ? Number(best.media_pct_of_annual).toFixed(1) : '—') + '%</span></div>';
    var tableRows = top3.map(function(r) {
      return '<div class="mi-info-row"><span class="mi-info-label">' + (r.peak_event || '—') + '</span><span class="mi-info-value">' + (Number(r.brand_pct_of_annual) || 0).toFixed(1) + '% / ' + (Number(r.media_pct_of_annual) || 0).toFixed(1) + '%</span></div>';
    }).join('');
    var msg = 'Highest share for ' + brandSpan + ': ' + (best.peak_event || '—') + ' (' + (best.brand_pct_of_annual != null ? Number(best.brand_pct_of_annual).toFixed(1) : '—') + '% of annual).';
    peakEl.innerHTML = rows + '<div class="mi-info-label" style="margin-top:.5rem;">Top 3 (' + brandSpan + ' / category)</div>' + tableRows + '<div class="mi-ranking">' + msg + '</div>';
  }

  function updateSummaryRow(d, scope) {
    scope = scope || 'all';
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
          el.innerHTML = '<span class="mi-brand-label">' + brandLabel + '</span>: ' + (sel.brand_avg_discount_depth || 0) + '%  |  Category Avg: ' + (sel.media_avg_discount_depth || 0) + '%';
          el.classList.remove('empty');
        } else {
          el.textContent = 'Select a category or subcategory.';
          el.classList.add('empty');
        }
      }
    }
  }

  function onResize() {
    var ids = ['chart-category-pie', 'chart-subcategory-pie', 'chart-promo-share', 'chart-promo-roi', 'chart-peak', 'chart-incremental-yoy'];
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
  if (typeof window.MI_DEFAULT_YEAR !== 'undefined' && window.MI_DEFAULT_YEAR != null && window.MI_DEFAULT_YEAR !== '') {
    var defY = String(window.MI_DEFAULT_YEAR);
    _scopeState.year_category_pie = defY;
    _scopeState.year_subcategory_pie = defY;
    _scopeState.year_promo_share = defY;
    _scopeState.year_promo_roi = defY;
    _scopeState.year_peak = defY;
  }
  initCharts();
  console.debug('[MI dashboard] calling loadData()');
  loadData();
})();
