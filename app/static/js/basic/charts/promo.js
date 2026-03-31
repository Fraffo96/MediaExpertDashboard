/**
 * Basic dashboard – Promo Share, Promo ROI, Discount charts (Filter by + Value).
 */
(function() {
  function update(d) {
    var core = window.BasicCore;
    var filters = window.BasicFilters;
    if (!core || !filters || !d) return;

    var chartFilterType = (document.getElementById('chart-filter-type') && document.getElementById('chart-filter-type').value) || '';
    var chartFilterValue = (document.getElementById('chart-filter-value') && document.getElementById('chart-filter-value').value) || '';

    var dimPromoShare = core.getDimension('chartPromoShare');
    var psc = null;
    if (chartFilterType && chartFilterValue && (d.promo_share_detail || []).length) {
      psc = filters.getPromoShareRowsFromDetail(d, chartFilterType, chartFilterValue, dimPromoShare);
    }
    if (!psc || !psc.length) {
      if (chartFilterType && chartFilterValue) {
        psc = [];
      } else if (dimPromoShare === 'subcategory' && (d.promo_share_detail || []).length) {
        psc = filters.getPromoShareRowsFromDetail(d, '', '', 'subcategory');
      }
      if (!psc || !psc.length) {
        psc = dimPromoShare === 'subcategory' ? (d.promo_share_by_subcategory || []) : (d.promo_share_by_category || []);
      }
    }
    filters.fillChartFilterValue('chart-filter-type', 'chart-filter-value', d);
    var typePromo = document.getElementById('chart-filter-type-promo');
    var valPromo = document.getElementById('chart-filter-value-promo');
    if (typePromo && valPromo) {
      typePromo.value = (document.getElementById('chart-filter-type') || {}).value || '';
      valPromo.value = (document.getElementById('chart-filter-value') || {}).value || '';
      filters.fillChartFilterValue('chart-filter-type-promo', 'chart-filter-value-promo', d);
    }
    var c = core.gc('chartPromoShare');
    if (c) setChart(c, psc.map(function(r) { return r.category_name; }), [
      { label: 'Non-Promo %', data: psc.map(function(r) { return (100 - Number(r.promo_share_pct)).toFixed(1); }), backgroundColor: COLORS.slate, borderWidth: 0 },
      { label: 'Promo %', data: psc.map(function(r) { return Number(r.promo_share_pct); }), backgroundColor: COLORS.yellow, borderWidth: 0, _isPct: true }
    ]);

    var dimRoi = core.getDimension('chartRoi');
    var mRoi = core.getMetric('chartRoi');
    var roiRows = null;
    if (chartFilterType && chartFilterValue && (d.promo_roi_detail || []).length) {
      roiRows = filters.getRoiRowsFromDetail(d, chartFilterType, chartFilterValue, dimRoi);
    }
    if (!roiRows || !roiRows.length) {
      roiRows = dimRoi === 'category' ? (d.promo_roi_by_category || []) : dimRoi === 'brand' ? (d.promo_roi_by_brand || []) : (d.promo_roi_by_type || []);
    }
    var roiLabelField = dimRoi === 'category' ? 'category_name' : dimRoi === 'brand' ? 'brand_name' : 'promo_name';
    var roiValField = mRoi === 'total_attributed' ? 'total_attributed' : mRoi === 'total_incremental' ? 'total_incremental' : 'avg_roi';
    var roiAxisLabel = mRoi === 'total_attributed' ? 'Attributed sales' : mRoi === 'total_incremental' ? 'Incremental sales' : 'Avg ROI';
    c = core.gc('chartRoi');
    if (c) setChart(c, roiRows.map(function(r) { return r[roiLabelField]; }),
      [{ label: roiAxisLabel, data: roiRows.map(function(r) { return r[roiValField]; }), backgroundColor: COLORS.yellow, borderWidth: 0 }]);

    var dimDiscount = core.getDimension('chartDiscount');
    var dd = null;
    if (chartFilterType && chartFilterValue && (d.discount_depth_detail || []).length) {
      dd = filters.getDiscountRowsFromDetail(d, chartFilterType, chartFilterValue, dimDiscount);
    }
    if (!dd || !dd.length) {
      if (chartFilterType && chartFilterValue) {
        dd = [];
      } else if (dimDiscount === 'subcategory' && (d.discount_depth_detail || []).length) {
        dd = filters.getDiscountRowsFromDetail(d, '', '', 'subcategory');
      }
      if (!dd || !dd.length) dd = d.discount_depth_by_category || [];
    }
    c = core.gc('chartDiscount');
    if (c) setChart(c, dd.map(function(r) { return r.category_name; }),
      [{ label: 'Avg Discount %', data: dd.map(function(r) { return r.avg_discount_depth; }), backgroundColor: COLORS.yellow, borderWidth: 0, _isPct: true }]);
  }

  window.BasicChartsPromo = { update: update };
})();
