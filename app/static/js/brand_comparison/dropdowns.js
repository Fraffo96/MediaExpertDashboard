/**
 * Brand Comparison – gestione dropdown.
 * Come MIDropdowns ma con Competitor come primo dropdown obbligatorio.
 * Popola: Competitor, Year, Channel, Category, Subcategory, Discount, ROI, Peak, Promo Share.
 */
(function() {
  'use strict';

  var YEAR_CONFIG = [
    { id: 'mi-year-category', stateKey: 'year_category_pie', scope: 'category_pie' },
    { id: 'mi-year-subcategory', stateKey: 'year_subcategory_pie', scope: 'subcategory_pie' },
    { id: 'mi-year-promo-share', stateKey: 'year_promo_share', scope: 'promo_share' },
    { id: 'mi-year-promo-roi', stateKey: 'year_promo_roi', scope: 'promo_roi' },
    { id: 'mi-year-peak', stateKey: 'year_peak', scope: 'peak' }
  ];
  var CHANNEL_CONFIG = [
    { id: 'mi-channel-category', stateKey: 'channel_category_pie', scope: 'category_pie' },
    { id: 'mi-channel-subcategory', stateKey: 'channel_subcategory_pie', scope: 'subcategory_pie' },
    { id: 'mi-channel-promo-share', stateKey: 'channel_promo_share', scope: 'promo_share' },
    { id: 'mi-channel-promo-roi', stateKey: 'channel_promo_roi', scope: 'promo_roi' },
    { id: 'mi-channel-peak', stateKey: 'channel_peak', scope: 'peak' }
  ];

  var _channelDropdowns = [];

  function populateCompetitor(competitors, selectedId, onChange) {
    var container = document.getElementById('bc-competitor-dropdown');
    if (!container || !window.MIGenericDropdown) return;
    var items = (competitors || []).map(function(c) {
      return { value: String(c.brand_id || c.id || ''), label: c.brand_name || c.name || '—' };
    });
    items.unshift({ value: '', label: '— Select competitor —' });
    window.MIGenericDropdown.create('bc-competitor-dropdown', {
      items: items,
      initialValue: selectedId || '',
      minWidth: 220,
      onChange: onChange
    });
  }

  function populate(d, state, callbacks) {
    var brandCats = d.brand_categories || [];
    var brandSubcats = d.brand_subcategories || {};
    var firstCatId = d.first_category_id ? String(d.first_category_id) : (brandCats[0] ? String(brandCats[0].category_id) : '');
    var availableYears = d.available_years || [];
    var competitors = d.competitors || [];
    var applyView = callbacks.applyViewFromState || function() {};
    var loadData = callbacks.loadData || function() {};
    var onYearChange = callbacks.onYearChange || function() {};

    if (!state.disc_cat && !state.disc_sub && firstCatId) state.disc_cat = firstCatId;
    if (!state.roi_cat && !state.roi_sub && firstCatId) state.roi_cat = firstCatId;
    if (!state.peak_cat && !state.peak_sub && firstCatId) state.peak_cat = firstCatId;
    if (!state.category_pie_id && firstCatId) state.category_pie_id = firstCatId;
    if (!state.subcategory_category_id && firstCatId) state.subcategory_category_id = firstCatId;
    var firstSub = (brandSubcats[firstCatId] || [])[0];
    if (!state.subcategory_pie_id && firstSub) state.subcategory_pie_id = String(firstSub.category_id);

    populateCompetitor(competitors, state.competitor_id, function(v) {
      state.competitor_id = v || '';
      if (loadData) loadData();
    });

    populateYearSelects(availableYears, state, onYearChange);
    populateChannelSelects(d.available_channels || [], state, applyView);
    populateCategoryPie(callbacks, brandCats, firstCatId, state);
    populateSubcategoryPie(callbacks, brandCats, brandSubcats, firstCatId, state);
    populateCategoryDropdowns(brandCats, brandSubcats, firstCatId, state, applyView);
    populateDeepDiveSelects(brandCats, brandSubcats, firstCatId, state, applyView);
    populateMetrics(state, applyView);
    setBrandLabels();
  }

  function populateChannelSelects(availableChannels, state, applyView) {
    if (!availableChannels || !availableChannels.length || !window.MIGenericDropdown) return;
    var items = availableChannels.map(function(c) { return { value: c.id || c, label: c.name || c.id || c }; });
    CHANNEL_CONFIG.forEach(function(cfg) {
      var container = document.getElementById(cfg.id);
      if (!container) return;
      var val = state[cfg.stateKey] || '';
      window.MIGenericDropdown.create(cfg.id, {
        items: items,
        initialValue: val,
        minWidth: 200,
        onChange: function(v) {
          state[cfg.stateKey] = v || '';
          applyView(cfg.scope);
        }
      });
    });
  }

  function populateYearSelects(availableYears, state, onYearChange) {
    if (!availableYears || !availableYears.length || !window.MIGenericDropdown) return;
    var items = availableYears.map(function(y) { return { value: String(y), label: String(y) }; });
    var defY = String(availableYears[availableYears.length - 1] || '');
    YEAR_CONFIG.forEach(function(cfg) {
      var container = document.getElementById(cfg.id);
      if (!container) return;
      var val = state[cfg.stateKey] || defY;
      window.MIGenericDropdown.create(cfg.id, {
        items: items,
        initialValue: val,
        minWidth: 200,
        onChange: function(v) {
          state[cfg.stateKey] = v || '';
          onYearChange(cfg.scope, v);
        }
      });
    });
  }

  function populateCategoryPie(callbacks, brandCats, firstCatId, state) {
    if (!window.MIGenericDropdown) return;
    var container = document.getElementById('mi-category-pie-select');
    if (!container) return;
    var items = brandCats.map(function(c) { return { value: String(c.category_id), label: c.category_name || '' }; });
    var val = state.category_pie_id || firstCatId || '';
    window.MIGenericDropdown.create('mi-category-pie-select', {
      items: items,
      initialValue: val,
      minWidth: 200,
      onChange: function(v) {
        state.category_pie_id = v || '';
        callbacks.applyViewFromState('category_pie');
      }
    });
  }

  function populateSubcategoryPie(callbacks, brandCats, brandSubcats, firstCatId, state) {
    if (!window.MIGenericDropdown) return;
    var subCatContainer = document.getElementById('mi-subcategory-category-select');
    var subPieContainer = document.getElementById('mi-subcategory-pie-select');
    if (!subCatContainer || !subPieContainer) return;

    var catItems = brandCats.map(function(c) { return { value: String(c.category_id), label: c.category_name || '' }; });
    var catVal = state.subcategory_category_id || firstCatId || '';
    var subCatDd = window.MIGenericDropdown.create('mi-subcategory-category-select', {
      items: catItems,
      initialValue: catVal,
      minWidth: 200,
      onChange: function(v) {
        state.subcategory_category_id = v || '';
        fillSubPie(v, false);
      }
    });

    function fillSubPie(catId, skipLoad) {
      catId = catId || (subCatDd && subCatDd.getValue ? subCatDd.getValue() : '');
      var subs = brandSubcats[catId] || [];
      var subItems = subs.map(function(s) { return { value: String(s.category_id), label: s.category_name || '' }; });
      var curSub = state.subcategory_pie_id;
      var valid = curSub && subs.some(function(s) { return String(s.category_id) === curSub; });
      var subVal = valid ? curSub : (subs[0] ? String(subs[0].category_id) : '');
      state.subcategory_pie_id = subVal;
      state.subcategory_category_id = catId || '';

      window.MIGenericDropdown.create('mi-subcategory-pie-select', {
        items: subItems,
        initialValue: subVal,
        minWidth: 200,
        onChange: function(v) {
          state.subcategory_pie_id = v || '';
          callbacks.applyViewFromState('subcategory_pie');
        }
      });
      if (!skipLoad) callbacks.applyViewFromState('subcategory_pie');
    }
    fillSubPie(catVal, true);
  }

  function populateCategoryDropdowns(brandCats, brandSubcats, firstCatId, state, applyView) {
    var items = brandCats.map(function(c) {
      return { id: c.category_id, name: c.category_name, subcategories: (brandSubcats[String(c.category_id)] || []).map(function(s) { return { id: s.category_id, name: s.category_name }; }) };
    });

    var discVal = state.disc_sub ? 'sub_' + state.disc_sub : (state.disc_cat ? 'cat_' + state.disc_cat : (firstCatId ? 'cat_' + firstCatId : ''));

    var baseOpts = { items: items, placeholder: 'Select category or subcategory', allLabel: 'Select category or subcategory', includeAll: false, minWidth: 200 };

    if (document.getElementById('mi-discount-scope-dropdown') && window.MICategoryDropdown) {
      window.MICategoryDropdown.create('mi-discount-scope-dropdown', Object.assign({}, baseOpts, {
        initialValue: discVal,
        onChange: function(v) {
          state.disc_cat = (v && v.startsWith('cat_')) ? v.replace('cat_', '') : '';
          state.disc_sub = (v && v.startsWith('sub_')) ? v.replace('sub_', '') : '';
          applyView('discount');
        }
      }));
    }

    var promoShareContainer = document.getElementById('mi-promo-share-category-select');
    if (promoShareContainer && window.MIGenericDropdown) {
      var psItems = [{ value: '', label: 'All categories' }].concat(brandCats.map(function(c) { return { value: String(c.category_id), label: c.category_name || '' }; }));
      var psVal = state.promo_share_category_id ? String(state.promo_share_category_id) : '';
      window.MIGenericDropdown.create('mi-promo-share-category-select', {
        items: psItems,
        initialValue: psVal,
        minWidth: 200,
        onChange: function(v) {
          state.promo_share_category_id = v || '';
          applyView('promo_share');
        }
      });
    }
  }

  function populateDeepDiveSelects(brandCats, brandSubcats, firstCatId, state, applyView) {
    var items = brandCats.map(function(c) {
      return { id: c.category_id, name: c.category_name, subcategories: (brandSubcats[String(c.category_id)] || []).map(function(s) { return { id: s.category_id, name: s.category_name }; }) };
    });

    var baseOpts = { items: items, placeholder: 'Select', allLabel: 'All categories', includeAll: false, minWidth: 200 };

    function setupDeepDive(containerId, stateKeys, applyScope) {
      var container = document.getElementById(containerId);
      if (!container || !window.MICategoryDropdown) return;
      var val = state[stateKeys.sub] ? 'sub_' + state[stateKeys.sub] : (state[stateKeys.cat] ? 'cat_' + state[stateKeys.cat] : (firstCatId ? 'cat_' + firstCatId : ''));
      window.MICategoryDropdown.create(containerId, Object.assign({}, baseOpts, {
        initialValue: val,
        onChange: function(v) {
          state[stateKeys.cat] = (v && v.startsWith('cat_')) ? v.replace('cat_', '') : '';
          state[stateKeys.sub] = (v && v.startsWith('sub_')) ? v.replace('sub_', '') : '';
          applyView(applyScope);
        }
      }));
    }

    setupDeepDive('mi-roi-scope-select', { cat: 'roi_cat', sub: 'roi_sub' }, 'promo_roi');
    setupDeepDive('mi-peak-scope-select', { cat: 'peak_cat', sub: 'peak_sub' }, 'peak');
  }

  function populateMetrics(state, applyView) {
    if (!window.MIGenericDropdown) return;
    var metricItems = [{ value: 'value', label: 'Value (PLN)' }, { value: 'volume', label: 'Volume (Units)' }];
    var metricCatContainer = document.getElementById('mi-metric-categories');
    var metricSubContainer = document.getElementById('mi-metric-subcategories');
    if (metricCatContainer) {
      window.MIGenericDropdown.create('mi-metric-categories', {
        items: metricItems,
        initialValue: state.metric_cat || 'value',
        minWidth: 200,
        onChange: function(v) { state.metric_cat = v; applyView('category_pie'); }
      });
    }
    if (metricSubContainer) {
      window.MIGenericDropdown.create('mi-metric-subcategories', {
        items: metricItems,
        initialValue: state.metric_sub || 'value',
        minWidth: 200,
        onChange: function(v) { state.metric_sub = v; applyView('subcategory_pie'); }
      });
    }
  }

  function setBrandLabels() {
    var brandLabel = (typeof window.MI_BRAND_NAME !== 'undefined' && window.MI_BRAND_NAME) ? window.MI_BRAND_NAME : 'Your Brand';
    var brandSpan = '<span class="mi-brand-label">' + brandLabel + '</span>';
    var promoLabel = document.getElementById('mi-promo-share-brand-label');
    if (promoLabel) promoLabel.innerHTML = brandSpan;
  }

  function populateCompetitorOnly(competitors, selectedId, onChange) {
    populateCompetitor(competitors, selectedId, onChange);
  }

  window.BCDropdowns = {
    populate: populate,
    populateCompetitorOnly: populateCompetitorOnly
  };
})();
