/**
 * Basic dashboard – Custom widgets builder and update.
 */
(function() {
  var CUSTOM_KEY = 'me_basic_custom_widgets';

  function getCustomWidgets() {
    try { var r = localStorage.getItem(CUSTOM_KEY); return r ? JSON.parse(r) : []; } catch (e) { return []; }
  }

  function setCustomWidgets(arr) {
    try { localStorage.setItem(CUSTOM_KEY, JSON.stringify(arr)); } catch (e) {}
  }

  function buildCustomChartData(d, metric, groupBy) {
    var core = window.BasicCore;
    var toSharePct = core ? core.toSharePct : function(arr, getVal) {
      var sum = 0;
      for (var i = 0; i < arr.length; i++) sum += Number(getVal(arr[i])) || 0;
      if (!sum) return arr.map(function() { return 0; });
      return arr.map(function(r) { return 100 * (Number(getVal(r)) || 0) / sum; });
    };
    var labels = [], data = [];
    if (groupBy === 'category') {
      var rows = d.sales_by_category || [];
      labels = rows.map(function(r) { return r.category_name; });
      var getV = metric === 'units' ? function(r) { return r.units; } : function(r) { return r.gross_pln; };
      data = metric === 'share_pct' ? toSharePct(rows, getV) : rows.map(getV);
    } else if (groupBy === 'subcategory') {
      var rows = d.sales_by_subcategory || [];
      labels = rows.map(function(r) { return r.category_name; });
      var getV = metric === 'units' ? function(r) { return r.units; } : function(r) { return r.gross_pln; };
      data = metric === 'share_pct' ? toSharePct(rows, getV) : rows.map(getV);
    } else if (groupBy === 'brand') {
      var rows = d.sales_by_brand || [];
      labels = rows.map(function(r) { return r.brand_name; });
      var getV = metric === 'units' ? function(r) { return r.units; } : function(r) { return r.gross_pln; };
      data = metric === 'share_pct' ? toSharePct(rows, getV) : rows.map(getV);
    } else if (groupBy === 'peak') {
      var rows = d.peak_events || [];
      labels = rows.map(function(r) { return r.peak_event; });
      var getV = metric === 'units' ? function(r) { return r.units; } : function(r) { return r.gross_pln; };
      data = metric === 'share_pct' ? toSharePct(rows, getV) : rows.map(getV);
    } else if (groupBy === 'year') {
      var rows = d.yoy || [];
      labels = rows.map(function(r) { return String(r.year); });
      data = rows.map(function(r) { return r.total_gross; });
    }
    return { labels: labels, data: data };
  }

  function buildCustomChartDataForCompare(d, w) {
    if (!w.compareBy || w.compareBy === 'none' || w.groupBy !== 'category') return null;
    var bySegment = d.sales_by_category_by_segment || [];
    var byGender = d.sales_by_category_by_gender || [];
    var rows = w.compareBy === 'segment' ? bySegment : (w.compareBy === 'gender' ? byGender : []);
    if (!rows.length) return null;
    var dimName = w.compareBy === 'segment' ? 'segment_name' : 'gender';
    var labelsSet = {};
    var dimSet = {};
    rows.forEach(function(r) { labelsSet[r.category_name] = true; dimSet[r[dimName]] = true; });
    var labels = Object.keys(labelsSet).sort();
    var dims = Object.keys(dimSet).sort();
    var colorsByDim = { segment: [COLORS.yellow, COLORS.slate, COLORS.yellow, COLORS.slate, COLORS.yellow, COLORS.slate], gender: [COLORS.yellow, COLORS.slate, COLORS.yellow] };
    var colors = (colorsByDim[w.compareBy] || COLORS.cat10).slice(0, dims.length);
    var datasets = dims.map(function(dim, i) {
      var data = labels.map(function(cat) {
        var r = rows.find(function(x) { return x.category_name === cat && x[dimName] === dim; });
        return r ? (w.metric === 'units' ? Number(r.units) : Number(r.gross_pln)) : 0;
      });
      return { label: dim, data: data, backgroundColor: colors[i] || (i === 0 ? COLORS.yellow : COLORS.slate), borderWidth: 0 };
    });
    return { labels: labels, datasets: datasets };
  }

  function updateCustomWidgets(d) {
    var core = window.BasicCore;
    if (!core) return;
    var reg = core.reg;
    var list = getCustomWidgets();
    list.forEach(function(w) {
      var ch = reg[w.id];
      if (!ch || !ch.chart) return;
      var built = buildCustomChartDataForCompare(d, w);
      if (built && built.datasets && built.datasets.length) {
        setChart(ch.chart, built.labels, built.datasets);
      } else {
        var simple = buildCustomChartData(d, w.metric, w.groupBy);
        var label = w.metric === 'share_pct' ? 'Share %' : (w.metric === 'units' ? 'Units' : 'Sales (PLN)');
        setChart(ch.chart, simple.labels, [{ label: label, data: simple.data, backgroundColor: COLORS.yellow, borderWidth: 0, _isPct: w.metric === 'share_pct' }]);
      }
    });
  }

  function addCustomWidget() {
    var core = window.BasicCore;
    if (!core) return;
    var metric = document.getElementById('wb-metric') && document.getElementById('wb-metric').value;
    var groupBy = document.getElementById('wb-group') && document.getElementById('wb-group').value;
    var compareBy = document.getElementById('wb-compare') && document.getElementById('wb-compare').value;
    if (compareBy === '') compareBy = 'none';
    var chartType = document.getElementById('wb-type') && document.getElementById('wb-type').value;
    if (!metric || !groupBy || !chartType) return;
    if ((compareBy === 'segment' || compareBy === 'gender') && groupBy !== 'category') {
      alert('Compare by Segment/Gender is only available when Group by is Category.');
      return;
    }
    var list = getCustomWidgets();
    var id = 'custom_' + Date.now();
    list.push({ id: id, metric: metric, groupBy: groupBy, compareBy: compareBy, chartType: chartType });
    setCustomWidgets(list);

    var container = document.getElementById('custom-widgets-container');
    if (!container) return;
    var title = 'Custom: ' + groupBy + (compareBy !== 'none' ? ' by ' + compareBy : '');
    var card = document.createElement('div');
    card.className = 'chart-card';
    card.setAttribute('data-widget-id', id);
    card.setAttribute('data-widget-label', title);
    card.innerHTML = '<div class="chart-header"><h3>' + title + '</h3><button type="button" class="chart-remove-widget" data-id="' + id + '" title="Remove">&times;</button></div><div class="chart-container"><canvas id="' + id + '"></canvas></div>';
    container.appendChild(card);

    var realType = chartType === 'hbar' ? 'bar' : chartType;
    var opt = core.optFor(chartType);
    if (compareBy !== 'none') opt = Object.assign({}, opt, { plugins: Object.assign({}, opt.plugins || {}, { legend: { display: true, position: 'bottom' } }) });
    var ch = new Chart(document.getElementById(id), { type: realType, data: { labels: [], datasets: [] }, options: opt });
    core.reg[id] = { chart: ch, type: chartType, canvas: document.getElementById(id) };

    card.querySelector('.chart-remove-widget').addEventListener('click', function() {
      var rid = this.getAttribute('data-id');
      if (core.reg[rid]) { core.reg[rid].chart.destroy(); delete core.reg[rid]; }
      card.remove();
      setCustomWidgets(getCustomWidgets().filter(function(x) { return x.id !== rid; }));
    });
    core.loadData();
  }

  window.clearAllCustomCharts = function() {
    var core = window.BasicCore;
    if (!core || !confirm('Remove all custom charts?')) return;
    getCustomWidgets().forEach(function(w) {
      if (core.reg[w.id]) { core.reg[w.id].chart.destroy(); delete core.reg[w.id]; }
    });
    setCustomWidgets([]);
    var container = document.getElementById('custom-widgets-container');
    if (container) container.innerHTML = '';
  };

  function renderSavedCustomWidgets() {
    var core = window.BasicCore;
    if (!core) return;
    var container = document.getElementById('custom-widgets-container');
    if (!container) return;
    container.innerHTML = '';
    getCustomWidgets().forEach(function(w) {
      if (!w.compareBy) w.compareBy = 'none';
      var title = 'Custom: ' + w.groupBy + (w.compareBy !== 'none' ? ' by ' + w.compareBy : '');
      var card = document.createElement('div');
      card.className = 'chart-card';
      card.setAttribute('data-widget-id', w.id);
      card.setAttribute('data-widget-label', title);
      card.innerHTML = '<div class="chart-header"><h3>' + title + '</h3><button type="button" class="chart-remove-widget" data-id="' + w.id + '" title="Remove">&times;</button></div><div class="chart-container"><canvas id="' + w.id + '"></canvas></div>';
      container.appendChild(card);
      var realType = w.chartType === 'hbar' ? 'bar' : w.chartType;
      var opt = core.optFor(w.chartType);
      if (w.compareBy !== 'none') opt = Object.assign({}, opt, { plugins: Object.assign({}, opt.plugins || {}, { legend: { display: true, position: 'bottom' } }) });
      var ch = new Chart(document.getElementById(w.id), { type: realType, data: { labels: [], datasets: [] }, options: opt });
      core.reg[w.id] = { chart: ch, type: w.chartType, canvas: document.getElementById(w.id) };
      card.querySelector('.chart-remove-widget').addEventListener('click', function() {
        var rid = this.getAttribute('data-id');
        if (core.reg[rid]) { core.reg[rid].chart.destroy(); delete core.reg[rid]; }
        card.remove();
        setCustomWidgets(getCustomWidgets().filter(function(x) { return x.id !== rid; }));
      });
    });
  }

  var btnAdd = document.getElementById('wb-add');
  if (btnAdd) btnAdd.addEventListener('click', addCustomWidget);
  renderSavedCustomWidgets();
  var btnClear = document.getElementById('wb-clear-all');
  if (btnClear) btnClear.addEventListener('click', window.clearAllCustomCharts);

  window.BasicChartsCustomWidgets = { update: updateCustomWidgets };
})();
