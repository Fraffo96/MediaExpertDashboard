/**
 * Basic dashboard – entry: create charts, wire listeners, updateCharts orchestrator.
 */
(function() {
  var core = window.BasicCore;
  var filters = window.BasicFilters;
  if (!core) return;

  core.mk('chartSales', 'hbar');
  core.mk('chartPromoShare', 'bar', core.STACKED_BAR_OPT);
  core.mk('chartRoi', 'hbar', core.ROI_BAR_OPT);
  core.mk('chartDiscount', 'bar');
  core.mk('chartIncrYoy', 'bar', core.INCR_YOY_OPT);
  core.mk('chartPeak', 'bar');
  core.mk('chartChannel', 'bar');
  core.mk('chartLoyalty', 'bar');
  core.mk('chartBuyerSeg', 'hbar');
  core.mk('chartDemo', 'bar');
  core.mk('chartChannelSeg', 'bar', core.CHANNEL_SEG_OPT);

  function updateCharts(d) {
    if (!d) return;
    core.setLastData(d);
    core.updateKPIs(d);
    if (window.BasicChartsSales) window.BasicChartsSales.update(d);
    if (window.BasicChartsPromo) window.BasicChartsPromo.update(d);
    if (window.BasicChartsPeak) window.BasicChartsPeak.update(d);
    if (window.BasicChartsBuyer) window.BasicChartsBuyer.update(d);
    if (window.BasicChartsProducts) window.BasicChartsProducts.update(d);
    if (window.BasicChartsIncrementalYoy) window.BasicChartsIncrementalYoy.update(d);
    if (window.BasicChartsCustomWidgets) window.BasicChartsCustomWidgets.update(d);
  }

  window.BasicUpdateCharts = updateCharts;

  document.querySelectorAll('.chart-metric-select[data-chart]').forEach(function(s) {
    var id = s.getAttribute('data-chart');
    var def = s.options[0] && s.options[0].value;
    var sv = core.savedMetric(id, def);
    if (sv) s.value = sv;
    s.addEventListener('change', function() {
      saveMetric(id, this.value);
      var data = core.getLastData();
      if (data) requestAnimationFrame(function() { updateCharts(data); });
    });
  });

  document.querySelectorAll('.chart-level-select[data-chart]').forEach(function(s) {
    var id = s.getAttribute('data-chart');
    var sv = core.savedLevel(id, 'parent');
    if (sv) s.value = sv;
    s.addEventListener('change', function() {
      saveLevel(id, this.value);
      var data = core.getLastData();
      if (data) requestAnimationFrame(function() { updateCharts(data); });
    });
  });

  document.querySelectorAll('.chart-dimension-select[data-chart]').forEach(function(s) {
    var id = s.getAttribute('data-chart');
    var def = s.options[0] && s.options[0].value;
    var sv = core.savedDimension(id, def);
    if (sv) {
      var opt = s.querySelector('option[value="' + sv + '"]');
      if (opt) s.value = sv;
    }
    s.addEventListener('change', function() {
      saveDimension(id, this.value);
      var data = core.getLastData();
      if (data) requestAnimationFrame(function() { updateCharts(data); });
    });
  });

  (function() {
    function syncPrimaryToPromo() {
      var typeP = document.getElementById('chart-filter-type');
      var valP = document.getElementById('chart-filter-value');
      var typePromo = document.getElementById('chart-filter-type-promo');
      var valPromo = document.getElementById('chart-filter-value-promo');
      if (typeP && valP && typePromo && valPromo) {
        typePromo.value = typeP.value || '';
        valPromo.value = valP.value || '';
        if (filters) filters.fillChartFilterValue('chart-filter-type-promo', 'chart-filter-value-promo', core.getLastData());
      }
    }
    function syncPromoToPrimary() {
      var typeP = document.getElementById('chart-filter-type');
      var valP = document.getElementById('chart-filter-value');
      var typePromo = document.getElementById('chart-filter-type-promo');
      var valPromo = document.getElementById('chart-filter-value-promo');
      if (typeP && valP && typePromo && valPromo) {
        typeP.value = typePromo.value || '';
        valP.value = valPromo.value || '';
        if (filters) filters.fillChartFilterValue('chart-filter-type', 'chart-filter-value', core.getLastData());
      }
    }
    var typeSel = document.getElementById('chart-filter-type');
    var valSel = document.getElementById('chart-filter-value');
    if (typeSel) typeSel.addEventListener('change', function() {
      var data = core.getLastData();
      syncPrimaryToPromo();
      if (data && filters) { filters.fillChartFilterValue('chart-filter-type', 'chart-filter-value', data); requestAnimationFrame(function() { updateCharts(data); }); }
    });
    if (valSel) valSel.addEventListener('change', function() {
      syncPrimaryToPromo();
      var data = core.getLastData();
      if (data) requestAnimationFrame(function() { updateCharts(data); });
    });
    var typePromo = document.getElementById('chart-filter-type-promo');
    var valPromo = document.getElementById('chart-filter-value-promo');
    if (typePromo) typePromo.addEventListener('change', function() {
      syncPromoToPrimary();
      var data = core.getLastData();
      if (data && filters) { filters.fillChartFilterValue('chart-filter-type-promo', 'chart-filter-value-promo', data); requestAnimationFrame(function() { updateCharts(data); }); }
    });
    if (valPromo) valPromo.addEventListener('change', function() {
      syncPromoToPrimary();
      var data = core.getLastData();
      if (data) requestAnimationFrame(function() { updateCharts(data); });
    });
  })();

  document.querySelectorAll('.chart-display-select[data-chart]').forEach(function(s) {
    s.addEventListener('change', function() {
      var data = core.getLastData();
      if (data) requestAnimationFrame(function() { updateCharts(data); });
    });
  });

  (function() {
    function onFilterChange() {
      if (core.applyFiltersOrRefetch) core.applyFiltersOrRefetch();
    }
    document.querySelectorAll('#filter-form select[name="segment_id"], #filter-form select[name="gender"], #filter-form select[name="brand_id"], #filter-form select[name="channel"]').forEach(function(s) {
      s.addEventListener('change', onFilterChange);
    });
    var catSel = document.getElementById('filter-category-id');
    var subcatSel = document.getElementById('filter-subcategory-id');
    if (catSel && subcatSel) {
      function filterSubcategoryOptions() {
        var parentId = catSel.value;
        var opts = subcatSel.querySelectorAll('option[data-parent]');
        var currentVal = subcatSel.value;
        var hasCurrent = false;
        opts.forEach(function(opt) {
          var show = !parentId || opt.getAttribute('data-parent') === parentId;
          opt.style.display = show ? '' : 'none';
          opt.disabled = show ? false : true;
          if (opt.value === currentVal) hasCurrent = show && (hasCurrent = true);
        });
        if (currentVal && !hasCurrent) subcatSel.value = '';
      }
      catSel.addEventListener('change', function() { filterSubcategoryOptions(); onFilterChange(); });
      subcatSel.addEventListener('change', onFilterChange);
      filterSubcategoryOptions();
    }
  })();

  if (window.BasicChartsIncrementalYoy && window.BasicChartsIncrementalYoy.initPromoPicker) {
    window.BasicChartsIncrementalYoy.initPromoPicker();
  }

  window.loadData = core.loadData;
  window.loadDataImmediate = function() { core.loadDataImmediate(false); };
  window.refreshData = function() { core.refreshData(); };

  /* Prefetch in background: la pagina è visibile subito, i dati si caricano in parallelo */
  if (typeof requestIdleCallback !== 'undefined') {
    requestIdleCallback(function() { core.loadDataImmediate(false); }, { timeout: 100 });
  } else {
    setTimeout(function() { core.loadDataImmediate(false); }, 0);
  }
})();
