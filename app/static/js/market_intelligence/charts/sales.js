/**
 * Market Intelligence – Pie charts (categories, subcategories) con brand mix e ranking.
 * Brand Comparison: istogrammi orizzontali invece di pie/doughnut.
 */
(function() {
  var BRAND_COLOR = '#FFD700';
  var OTHER_COLOR = '#5a5a5a';
  var isBC = (typeof window.DASHBOARD_ID !== 'undefined' && window.DASHBOARD_ID === 'brand_comparison');

  function pieTooltip(brandName, pctVal, pctVol, grossPln, units) {
    var pctValStr = (pctVal != null ? Number(pctVal) : 0).toFixed(1) + '%';
    var pctVolStr = (pctVol != null ? Number(pctVol) : 0).toFixed(1) + '%';
    var valStr = (Number(grossPln) || 0).toLocaleString('en-US', {maximumFractionDigits:0}) + ' PLN';
    var unitsStr = (Number(units) || 0).toLocaleString('en-US', {maximumFractionDigits:0}) + ' units';
    return [(brandName || 'Unknown'), pctValStr + ' value · ' + pctVolStr + ' volume', valStr + ' · ' + unitsStr];
  }

  function createSalesChart(el, labels, data, colors, pieData, useBar, opts) {
    opts = opts || {};
    var tooltipLabel = opts.tooltipLabel;
    if (useBar) {
      var barOpts = (typeof ChartStyles !== 'undefined' && ChartStyles.barGradientOptions)
        ? ChartStyles.barGradientOptions('y') : { responsive: true, maintainAspectRatio: false, indexAxis: 'y' };
      return new Chart(el.getContext('2d'), {
        type: 'bar',
        data: {
          labels: labels,
          datasets: [{
            label: 'Value',
            data: data,
            backgroundColor: colors,
            borderWidth: 0,
            borderRadius: 6,
            barThickness: 'flex',
            maxBarThickness: 48
          }]
        },
        options: Object.assign({}, barOpts, {
          plugins: Object.assign({}, barOpts.plugins || {}, {
            legend: { display: false },
            tooltip: {
              callbacks: tooltipLabel ? {
                label: function(ctx) { return tooltipLabel(ctx); }
              } : undefined
            }
          }),
          scales: {
            x: {
              grid: { color: 'rgba(0,212,255,0.08)' },
              ticks: {
                color: '#b0b0b0',
                callback: function(v) {
                  return v >= 1e6 ? (v/1e6).toFixed(1) + 'M' : v >= 1000 ? (v/1000).toFixed(1) + 'k' : v;
                }
              }
            },
            y: {
              grid: { display: false },
              ticks: { color: '#b0b0b0', font: { size: 12 } }
            }
          }
        })
      });
    }
    var doughnutOpts = (typeof ChartStyles !== 'undefined' && ChartStyles.doughnut3DOptions)
      ? ChartStyles.doughnut3DOptions(60) : { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } };
    return new Chart(el.getContext('2d'), {
      type: 'doughnut',
      data: { labels: labels, datasets: [{ data: data, backgroundColor: colors, borderWidth: 0, spacing: 3, borderRadius: 2 }] },
      options: Object.assign({}, doughnutOpts, {
        plugins: Object.assign({}, doughnutOpts.plugins || {}, {
          tooltip: tooltipLabel ? { callbacks: { label: function(ctx) { return tooltipLabel(ctx); } } } : {}
        })
      })
    });
  }

  function update(d) {
    var core = window.MICore;
    if (!core || !d) return;

    var metricCat = (window.MIScopeState && window.MIScopeState.metric_cat) || 'value';
    var metricSub = (window.MIScopeState && window.MIScopeState.metric_sub) || 'value';
    var brandId = (typeof window.MI_BRAND_ID !== 'undefined' && window.MI_BRAND_ID != null) ? Number(window.MI_BRAND_ID) : null;
    var brandLabel = (typeof window.MI_BRAND_NAME !== 'undefined' && window.MI_BRAND_NAME) ? window.MI_BRAND_NAME : 'Your Brand';

    var catPie = d.category_pie_brands || [];
    var el = document.getElementById('chart-category-pie');
    if (el && catPie.length && typeof Chart !== 'undefined') {
      var isVal = metricCat === 'value';
      var labels = catPie.map(function(x) { return x.brand_name || 'Unknown'; });
      var data = catPie.map(function(x) { return isVal ? (Number(x.gross_pln) || 0) : (Number(x.units) || 0); });
      var brandIdx = catPie.findIndex(function(x) { return Number(x.brand_id) === brandId; });
      var colors = (typeof ChartStyles !== 'undefined' && ChartStyles.doughnut3DColors)
        ? ChartStyles.doughnut3DColors(data.length, brandIdx)
        : catPie.map(function(x) { return (Number(x.brand_id) === brandId) ? BRAND_COLOR : OTHER_COLOR; });
      var pieData = catPie;
      if (el.chart) { el.chart.destroy(); el.chart = null; }
      el.chart = createSalesChart(el, labels, data, colors, pieData, isBC, {
        tooltipLabel: function(ctx) {
          var r = pieData[ctx.dataIndex];
          return pieTooltip(r && r.brand_name, r && r.pct_value, r && r.pct_volume, r && r.gross_pln, r && r.units);
        }
      });
    }

    fillInfoPanel('mi-category-info', catPie, brandId, brandLabel, 'category', d.category_pie_brands_prev_map, d.category_pie_id, d.competitor_name);

    var subPie = d.subcategory_pie_brands || [];
    el = document.getElementById('chart-subcategory-pie');
    if (el && subPie.length && typeof Chart !== 'undefined') {
      var isValSub = metricSub === 'value';
      var labelsSub = subPie.map(function(x) { return x.brand_name || 'Unknown'; });
      var dataSub = subPie.map(function(x) { return isValSub ? (Number(x.gross_pln) || 0) : (Number(x.units) || 0); });
      var brandIdxSub = subPie.findIndex(function(x) { return Number(x.brand_id) === brandId; });
      var colorsSub = (typeof ChartStyles !== 'undefined' && ChartStyles.doughnut3DColors)
        ? ChartStyles.doughnut3DColors(dataSub.length, brandIdxSub)
        : subPie.map(function(x) { return (Number(x.brand_id) === brandId) ? BRAND_COLOR : OTHER_COLOR; });
      var subPieData = subPie;
      if (el.chart) { el.chart.destroy(); el.chart = null; }
      el.chart = createSalesChart(el, labelsSub, dataSub, colorsSub, subPieData, isBC, {
        tooltipLabel: function(ctx) {
          var r = subPieData[ctx.dataIndex];
          return pieTooltip(r && r.brand_name, r && r.pct_value, r && r.pct_volume, r && r.gross_pln, r && r.units);
        }
      });
    }

    fillInfoPanel('mi-subcategory-info', subPie, brandId, brandLabel, 'subcategory', d.subcategory_pie_brands_prev_map, d.subcategory_pie_id, d.competitor_name);
  }

  function fillInfoPanel(containerId, pieData, brandId, brandLabel, scope, prevMap, scopeId, competitorName) {
    var container = document.getElementById(containerId);
    if (!container) return;
    var panel = container.querySelector('.mi-info-panel');
    if (!panel) return;

    if (!pieData.length || !brandId) {
      panel.innerHTML = '';
      return;
    }

    var brandRow = pieData.find(function(x) { return Number(x.brand_id) === brandId; });
    var brandValue = brandRow ? (Number(brandRow.gross_pln) || 0) : 0;
    var brandUnits = brandRow ? (Number(brandRow.units) || 0) : 0;
    var pctVal = brandRow && brandRow.pct_value != null ? Number(brandRow.pct_value) : 0;
    var pctVol = brandRow && brandRow.pct_volume != null ? Number(brandRow.pct_volume) : 0;

    var totalCatValue, totalCatUnits;
    if (isBC && pctVal > 0 && pctVol > 0) {
      totalCatValue = brandValue * 100 / pctVal;
      totalCatUnits = brandUnits * 100 / pctVol;
    } else {
      totalCatValue = pieData.reduce(function(s, x) { return s + (Number(x.gross_pln) || 0); }, 0);
      totalCatUnits = pieData.reduce(function(s, x) { return s + (Number(x.units) || 0); }, 0);
      if (!pctVal && totalCatValue) pctVal = 100 * brandValue / totalCatValue;
      if (!pctVol && totalCatUnits) pctVol = 100 * brandUnits / totalCatUnits;
    }

    var sortedByValue = pieData.slice().sort(function(a, b) { return (Number(b.gross_pln) || 0) - (Number(a.gross_pln) || 0); });
    var pos = sortedByValue.findIndex(function(x) { return Number(x.brand_id) === brandId; });
    var ord = pos >= 0 ? pos + 1 : 0;
    var suf = ord === 1 ? 'st' : (ord === 2 ? 'nd' : (ord === 3 ? 'rd' : 'th'));
    var leader = sortedByValue[0];
    var leaderPct = leader && leader.pct_value != null ? Number(leader.pct_value) : (totalCatValue && leader ? 100 * (Number(leader.gross_pln) || 0) / totalCatValue : 0);
    var gapPct = ord > 1 && leader ? (leaderPct - pctVal).toFixed(1) : '';

    function fmtNum(n) { return (Number(n) || 0).toLocaleString('en-US', { maximumFractionDigits: 0 }); }
    function fmtPct(n) { return (Number(n) || 0).toFixed(1) + '%'; }

    var pctValPrev = null;
    if (prevMap && scopeId && brandId) {
      var scopePrev = prevMap[String(scopeId)];
      if (scopePrev) pctValPrev = scopePrev[String(brandId)];
    }
    var INSIGHTS = (typeof ChartStyles !== 'undefined' && ChartStyles.INSIGHTS) ? ChartStyles.INSIGHTS : null;
    var shareChangeHtml = '';
    if (pctValPrev != null && pctValPrev !== '') {
      var delta = (Number(pctVal) || 0) - (Number(pctValPrev) || 0);
      var deltaStr = (delta >= 0 ? '+' : '') + delta.toFixed(1) + 'pp';
      var shareText = '';
      if (INSIGHTS) {
        if (delta > 0) shareText = INSIGHTS.shareUp(delta.toFixed(1));
        else if (delta < 0) shareText = INSIGHTS.shareDown(Math.abs(delta).toFixed(1));
        else shareText = INSIGHTS.shareFlat();
      } else {
        if (delta > 0) shareText = 'Gained ' + deltaStr;
        else if (delta < 0) shareText = 'Lost ' + deltaStr;
        else shareText = 'Unchanged';
      }
      var shareColor = delta > 0 ? 'var(--green)' : (delta < 0 ? 'var(--red)' : 'var(--text-muted)');
      shareChangeHtml = '<div class="mi-info-row"><span class="mi-info-label">Market share vs prev year</span><span class="mi-info-value mi-wrap" style="color:' + shareColor + ';">' + shareText + '</span></div>';
    }

    var scopeLabel = scope === 'subcategory' ? 'subcategory' : 'category';
    var brandSpan = '<span class="mi-brand-label">' + (brandLabel || 'Your Brand') + '</span>';
    var rankHtml = '';
    if (ord >= 1) {
      if (ord === 1) {
        rankHtml = '<div class="mi-ranking leader">' + (INSIGHTS ? INSIGHTS.leader(brandSpan) : brandSpan + ' leads in market share.') + '</div>';
      } else {
        var rankText = INSIGHTS ? INSIGHTS.rank(brandSpan, ord, suf, leader && leader.brand_name, gapPct) : brandSpan + ' is #' + ord + suf + '.';
        rankHtml = '<div class="mi-ranking">' + rankText + '</div>';
        if (leader && leader.brand_name && !INSIGHTS) {
          rankHtml += '<div class="mi-ranking gap">Leader: ' + (leader.brand_name || 'Unknown') + (gapPct ? ' (+' + gapPct + '% vs you)' : '') + '</div>';
        }
      }
    }

    var rowsHtml;
    if (isBC && pieData.length === 2) {
      var compRow = pieData.find(function(x) { return Number(x.brand_id) !== brandId; });
      var compLabel = (competitorName || (compRow && compRow.brand_name) || 'Competitor');
      var compValue = compRow ? (Number(compRow.gross_pln) || 0) : 0;
      var compUnits = compRow ? (Number(compRow.units) || 0) : 0;
      var compPctVal = compRow && compRow.pct_value != null ? Number(compRow.pct_value) : (totalCatValue ? 100 * compValue / totalCatValue : 0);
      var compPctVol = compRow && compRow.pct_volume != null ? Number(compRow.pct_volume) : (totalCatUnits ? 100 * compUnits / totalCatUnits : 0);
      rowsHtml =
        '<div class="mi-info-row"><span class="mi-info-label">' + brandSpan + ' share (value)</span><span class="mi-info-value highlight">' + fmtPct(pctVal) + '</span></div>' +
        '<div class="mi-info-row"><span class="mi-info-label">' + compLabel + ' share (value)</span><span class="mi-info-value">' + fmtPct(compPctVal) + '</span></div>' +
        '<div class="mi-info-row"><span class="mi-info-label">' + brandSpan + ' share (units)</span><span class="mi-info-value highlight">' + fmtPct(pctVol) + '</span></div>' +
        '<div class="mi-info-row"><span class="mi-info-label">' + compLabel + ' share (units)</span><span class="mi-info-value">' + fmtPct(compPctVol) + '</span></div>' +
        shareChangeHtml +
        '<div class="mi-info-row"><span class="mi-info-label">Total category</span><span class="mi-info-value">' + fmtNum(totalCatValue) + ' PLN</span></div>' +
        '<div class="mi-info-row"><span class="mi-info-label">Total ' + brandSpan + '</span><span class="mi-info-value">' + fmtNum(brandValue) + ' PLN</span></div>' +
        '<div class="mi-info-row"><span class="mi-info-label">Total ' + compLabel + '</span><span class="mi-info-value">' + fmtNum(compValue) + ' PLN</span></div>' +
        rankHtml;
    } else {
      rowsHtml =
        '<div class="mi-info-row"><span class="mi-info-label">' + brandSpan + ' share (value)</span><span class="mi-info-value highlight">' + fmtPct(pctVal) + '</span></div>' +
        '<div class="mi-info-row"><span class="mi-info-label">' + brandSpan + ' share (units)</span><span class="mi-info-value">' + fmtPct(pctVol) + '</span></div>' +
        shareChangeHtml +
        '<div class="mi-info-row"><span class="mi-info-label">Total category</span><span class="mi-info-value">' + fmtNum(totalCatValue) + ' PLN</span></div>' +
        '<div class="mi-info-row"><span class="mi-info-label">Total ' + brandSpan + '</span><span class="mi-info-value">' + fmtNum(brandValue) + ' PLN</span></div>' +
        rankHtml;
    }
    panel.innerHTML = rowsHtml;
  }

  function updateCategoryPieOnly(d) {
    if (!d) return;
    var catPie = d.category_pie_brands || [];
    var el = document.getElementById('chart-category-pie');
    var metricCat = (window.MIScopeState && window.MIScopeState.metric_cat) || 'value';
    var brandId = (typeof window.MI_BRAND_ID !== 'undefined' && window.MI_BRAND_ID != null) ? Number(window.MI_BRAND_ID) : null;
    var brandLabel = (typeof window.MI_BRAND_NAME !== 'undefined' && window.MI_BRAND_NAME) ? window.MI_BRAND_NAME : 'Your Brand';
    if (el && catPie.length && typeof Chart !== 'undefined') {
      var isVal = metricCat === 'value';
      var labels = catPie.map(function(x) { return x.brand_name || 'Unknown'; });
      var data = catPie.map(function(x) { return isVal ? (Number(x.gross_pln) || 0) : (Number(x.units) || 0); });
      var brandIdx = catPie.findIndex(function(x) { return Number(x.brand_id) === brandId; });
      var colors = (typeof ChartStyles !== 'undefined' && ChartStyles.doughnut3DColors)
        ? ChartStyles.doughnut3DColors(data.length, brandIdx)
        : catPie.map(function(x) { return (Number(x.brand_id) === brandId) ? '#FFD700' : '#5a5a5a'; });
      if (el.chart) { el.chart.destroy(); el.chart = null; }
      el.chart = createSalesChart(el, labels, data, colors, catPie, isBC, {
        tooltipLabel: function(ctx) { var r = catPie[ctx.dataIndex]; return pieTooltip(r && r.brand_name, r && r.pct_value, r && r.pct_volume, r && r.gross_pln, r && r.units); }
      });
    }
    fillInfoPanel('mi-category-info', catPie, brandId, brandLabel, 'category', d.category_pie_brands_prev_map, d.category_pie_id, d.competitor_name);
  }

  function updateSubcategoryPieOnly(d) {
    if (!d) return;
    var subPie = d.subcategory_pie_brands || [];
    var el = document.getElementById('chart-subcategory-pie');
    var metricSub = (window.MIScopeState && window.MIScopeState.metric_sub) || 'value';
    var brandId = (typeof window.MI_BRAND_ID !== 'undefined' && window.MI_BRAND_ID != null) ? Number(window.MI_BRAND_ID) : null;
    var brandLabel = (typeof window.MI_BRAND_NAME !== 'undefined' && window.MI_BRAND_NAME) ? window.MI_BRAND_NAME : 'Your Brand';
    if (el && subPie.length && typeof Chart !== 'undefined') {
      var isValSub = metricSub === 'value';
      var labelsSub = subPie.map(function(x) { return x.brand_name || 'Unknown'; });
      var dataSub = subPie.map(function(x) { return isValSub ? (Number(x.gross_pln) || 0) : (Number(x.units) || 0); });
      var brandIdxSub = subPie.findIndex(function(x) { return Number(x.brand_id) === brandId; });
      var colorsSub = (typeof ChartStyles !== 'undefined' && ChartStyles.doughnut3DColors)
        ? ChartStyles.doughnut3DColors(dataSub.length, brandIdxSub)
        : subPie.map(function(x) { return (Number(x.brand_id) === brandId) ? '#FFD700' : '#5a5a5a'; });
      if (el.chart) { el.chart.destroy(); el.chart = null; }
      el.chart = createSalesChart(el, labelsSub, dataSub, colorsSub, subPie, isBC, {
        tooltipLabel: function(ctx) { var r = subPie[ctx.dataIndex]; return pieTooltip(r && r.brand_name, r && r.pct_value, r && r.pct_volume, r && r.gross_pln, r && r.units); }
      });
    }
    fillInfoPanel('mi-subcategory-info', subPie, brandId, brandLabel, 'subcategory', d.subcategory_pie_brands_prev_map, d.subcategory_pie_id, d.competitor_name);
  }

  window.MIChartsSales = { update: update, updateCategoryPieOnly: updateCategoryPieOnly, updateSubcategoryPieOnly: updateSubcategoryPieOnly };
})();
