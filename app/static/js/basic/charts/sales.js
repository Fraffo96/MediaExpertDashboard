/**
 * Basic dashboard – Sales chart (dimension + metric + topN).
 */
(function() {
  function getSalesSeriesForDimension(d, dimension, metric) {
    var getVal = metric === 'units' ? function(r) { return r.units; } : function(r) { return r.gross_pln; };
    var valueLabel = metric === 'units' ? 'Units' : 'Sales (PLN)';
    if (dimension === 'category') {
      var rows = d.sales_by_category || [];
      return { labels: rows.map(function(r) { return r.category_name; }), values: rows.map(getVal), valueLabel: valueLabel };
    }
    if (dimension === 'subcategory') {
      rows = d.sales_by_subcategory || [];
      return { labels: rows.map(function(r) { return r.category_name; }), values: rows.map(getVal), valueLabel: valueLabel };
    }
    if (dimension === 'brand') {
      rows = d.sales_by_brand || [];
      return { labels: rows.map(function(r) { return r.brand_name; }), values: rows.map(getVal), valueLabel: valueLabel };
    }
    if (dimension === 'segment') {
      rows = d.sales_by_category_by_segment || [];
      var bySeg = {};
      rows.forEach(function(r) {
        var k = r.segment_name;
        if (!bySeg[k]) bySeg[k] = { gross_pln: 0, units: 0 };
        bySeg[k].gross_pln += Number(r.gross_pln) || 0;
        bySeg[k].units += Number(r.units) || 0;
      });
      var segLabels = Object.keys(bySeg).sort();
      var segVals = segLabels.map(function(k) { return metric === 'units' ? bySeg[k].units : bySeg[k].gross_pln; });
      return { labels: segLabels, values: segVals, valueLabel: valueLabel };
    }
    if (dimension === 'year') {
      rows = d.yoy || [];
      return { labels: rows.map(function(r) { return String(r.year); }), values: rows.map(function(r) { return r.total_gross; }), valueLabel: 'Sales (PLN)' };
    }
    if (dimension === 'gender') {
      rows = d.sales_by_category_by_gender || [];
      bySeg = {};
      rows.forEach(function(r) {
        var k = r.gender;
        if (!bySeg[k]) bySeg[k] = { gross_pln: 0, units: 0 };
        bySeg[k].gross_pln += Number(r.gross_pln) || 0;
        bySeg[k].units += Number(r.units) || 0;
      });
      segLabels = Object.keys(bySeg).sort();
      segVals = segLabels.map(function(k) { return metric === 'units' ? bySeg[k].units : bySeg[k].gross_pln; });
      return { labels: segLabels, values: segVals, valueLabel: valueLabel };
    }
    return { labels: [], values: [], valueLabel: valueLabel };
  }

  function update(d) {
    var core = window.BasicCore;
    if (!core || !d) return;
    var dimSales = core.getDimension('chartSales');
    var seriesSales = getSalesSeriesForDimension(d, dimSales, core.getMetric('chartSales'));
    var topNsel = document.getElementById('topn-chartSales');
    var topN = topNsel ? topNsel.value : '10';
    var indices = seriesSales.values.map(function(v, i) { return i; }).sort(function(a, b) { return seriesSales.values[b] - seriesSales.values[a]; });
    if (topN !== 'all') indices = indices.slice(0, parseInt(topN, 10));
    var salesLabels = indices.map(function(i) { return seriesSales.labels[i]; });
    var salesVals = indices.map(function(i) { return seriesSales.values[i]; });
    var c = core.gc('chartSales');
    if (c) setChart(c, salesLabels, [{ label: seriesSales.valueLabel, data: salesVals, backgroundColor: COLORS.yellow, borderWidth: 0 }]);
  }

  window.BasicChartsSales = { update: update };
})();
