/**
 * Basic dashboard – Filter by + Value: detail → rows for Promo Share, ROI, Discount.
 */
(function() {
  function getRoiRowsFromDetail(d, filterType, filterValue, dimRoi) {
    var detail = d.promo_roi_detail || [];
    if (!detail.length) return null;
    var filtered = detail;
    if (filterType === 'brand' && filterValue !== '' && filterValue != null) {
      var bid = parseInt(filterValue, 10);
      if (!isNaN(bid)) {
        filtered = detail.filter(function(r) { return Number(r.brand_id) === bid || String(r.brand_id) === String(filterValue); });
      }
    } else if (filterType === 'category' && filterValue !== '' && filterValue != null) {
      var cid = parseInt(filterValue, 10);
      if (!isNaN(cid)) {
        filtered = detail.filter(function(r) { return Number(r.category_id) === cid; });
      }
    } else if (filterType === 'subcategory' && filterValue !== '' && filterValue != null) {
      var sid = parseInt(filterValue, 10);
      var parentId = sid >= 100 ? Math.floor(sid / 100) : sid;
      if (!isNaN(sid)) {
        filtered = detail.filter(function(r) { return Number(r.category_id) === parentId; });
      }
    }
    if (!filtered.length) return [];
    var keyField = dimRoi === 'category' ? 'category_name' : dimRoi === 'brand' ? 'brand_name' : 'promo_id';
    var keyId = dimRoi === 'category' ? 'category_id' : dimRoi === 'brand' ? 'brand_id' : 'promo_id';
    var agg = {};
    filtered.forEach(function(r) {
      var key = dimRoi === 'promo_type' ? String(r.promo_id || r.promo_name) : (r[keyField] != null ? String(r[keyField]) : String(r[keyId]));
      if (!agg[key]) {
        agg[key] = { n: 0, sum_att: 0, sum_inc: 0, sum_roi_weight: 0 };
        if (dimRoi === 'promo_type') { agg[key].promo_name = r.promo_name; agg[key].promo_type = r.promo_type; }
        else if (dimRoi === 'category') { agg[key].category_id = r.category_id; agg[key].category_name = r.category_name; }
        else { agg[key].brand_id = r.brand_id; agg[key].brand_name = r.brand_name; }
      }
      agg[key].n += 1;
      agg[key].sum_att += Number(r.total_attributed) || 0;
      agg[key].sum_inc += Number(r.total_incremental) || 0;
      var w = Number(r.total_attributed) || 0;
      agg[key].sum_roi_weight += (Number(r.avg_roi) || 0) * (w || 1);
    });
    var out = Object.keys(agg).map(function(k) {
      var a = agg[k];
      var row = dimRoi === 'promo_type' ? { promo_name: a.promo_name, promo_type: a.promo_type } : dimRoi === 'category' ? { category_id: a.category_id, category_name: a.category_name } : { brand_id: a.brand_id, brand_name: a.brand_name };
      row.total_attributed = a.sum_att;
      row.total_incremental = a.sum_inc;
      row.avg_roi = a.sum_att ? Math.round((a.sum_roi_weight / a.sum_att) * 100) / 100 : (a.sum_roi_weight / (a.n || 1));
      return row;
    });
    return out.sort(function(a, b) { return (b.avg_roi || 0) - (a.avg_roi || 0); });
  }

  function fillRoiFilterValue(d) {
    var typeSel = document.getElementById('roi-filter-type');
    var valSel = document.getElementById('roi-filter-value');
    if (!typeSel || !valSel) return;
    var t = typeSel.value;
    var curVal = valSel.value;
    var valGroup = valSel.closest('.chart-control-value');
    if (valGroup) valGroup.hidden = !t;
    valSel.innerHTML = '<option value="">All</option>';
    if (!t || !d) return;
    var options = [];
    if (t === 'brand') {
      (d.sales_by_brand || []).forEach(function(r) { options.push({ value: r.brand_id, label: r.brand_name || ('Brand ' + r.brand_id) }); });
    } else if (t === 'category') {
      (d.sales_by_category || []).forEach(function(r) { options.push({ value: r.category_id, label: r.category_name || ('Category ' + r.category_id) }); });
    } else if (t === 'subcategory') {
      (d.sales_by_subcategory || []).forEach(function(r) { options.push({ value: r.category_id, label: r.category_name || ('Subcategory ' + r.category_id) }); });
    }
    options.forEach(function(o) {
      var opt = document.createElement('option');
      opt.value = o.value;
      opt.textContent = o.label;
      if (String(o.value) === String(curVal)) opt.selected = true;
      valSel.appendChild(opt);
    });
  }

  function fillChartFilterValue(typeSelId, valSelId, d) {
    var typeSel = document.getElementById(typeSelId);
    var valSel = document.getElementById(valSelId);
    if (!typeSel || !valSel) return;
    var t = typeSel.value;
    var curVal = valSel.value;
    var valGroup = valSel.closest('.chart-control-value');
    if (valGroup) valGroup.hidden = !t;
    valSel.innerHTML = '<option value="">All</option>';
    if (!t) return;
    var options = [];
    if (t === 'brand' && d) {
      (d.sales_by_brand || []).forEach(function(r) { options.push({ value: r.brand_id, label: r.brand_name || ('Brand ' + r.brand_id) }); });
    } else if (t === 'category' && d) {
      (d.sales_by_category || []).forEach(function(r) { options.push({ value: r.category_id, label: r.category_name || ('Category ' + r.category_id) }); });
    } else if (t === 'subcategory' && d) {
      (d.sales_by_subcategory || []).forEach(function(r) { options.push({ value: r.category_id, label: r.category_name || ('Subcategory ' + r.category_id) }); });
    } else if (t === 'segment') {
      var segSel = document.querySelector('select[name="segment_id"]');
      if (segSel) for (var i = 0; i < segSel.options.length; i++) { var o = segSel.options[i]; if (o.value) options.push({ value: o.value, label: o.textContent.trim() || o.value }); }
    } else if (t === 'gender') {
      var genSel = document.querySelector('select[name="gender"]');
      if (genSel) for (var j = 0; j < genSel.options.length; j++) { var o2 = genSel.options[j]; if (o2.value) options.push({ value: o2.value, label: o2.textContent.trim() || o2.value }); }
    } else if (t === 'channel') {
      options = [{ value: 'web', label: 'Web' }, { value: 'app', label: 'App' }, { value: 'store', label: 'Store' }];
    }
    options.forEach(function(o) {
      var opt = document.createElement('option');
      opt.value = o.value;
      opt.textContent = o.label;
      if (String(o.value) === String(curVal)) opt.selected = true;
      valSel.appendChild(opt);
    });
  }

  function getPromoShareRowsFromDetail(d, filterType, filterValue, dim) {
    var detail = d.promo_share_detail || [];
    if (!detail.length) return null;
    var filtered = detail;
    if (filterType === 'brand' && filterValue !== '' && filterValue != null) {
      var bid = parseInt(filterValue, 10);
      if (!isNaN(bid)) {
        filtered = detail.filter(function(r) { return Number(r.brand_id) === bid || String(r.brand_id) === String(filterValue); });
      }
    } else if (filterType === 'category' && filterValue !== '' && filterValue != null) {
      var cid = parseInt(filterValue, 10);
      if (!isNaN(cid)) {
        filtered = detail.filter(function(r) { return Number(r.parent_category_id) === cid; });
      }
    } else if (filterType === 'subcategory' && filterValue !== '' && filterValue != null) {
      var sid = parseInt(filterValue, 10);
      if (!isNaN(sid)) {
        filtered = detail.filter(function(r) { return Number(r.category_id) === sid; });
      }
    } else if (filterType === 'segment' && filterValue !== '' && filterValue != null) {
      var segId = parseInt(filterValue, 10);
      if (!isNaN(segId)) {
        filtered = detail.filter(function(r) { return Number(r.segment_id) === segId; });
      }
    } else if (filterType === 'gender' && filterValue !== '' && filterValue != null) {
      filtered = detail.filter(function(r) { return String(r.gender || '') === String(filterValue); });
    }
    /* channel not in promo_share_detail: no filter by channel */
    if (!filtered.length) return [];
    var keyField = dim === 'subcategory' ? 'category_id' : 'parent_category_id';
    var nameField = dim === 'subcategory' ? 'category_name' : 'parent_name';
    var agg = {};
    filtered.forEach(function(r) {
      var key = r[keyField];
      var name = r[nameField];
      if (!agg[key]) agg[key] = { category_id: key, category_name: name, total_gross: 0, promo_gross: 0 };
      agg[key].total_gross += Number(r.total_gross) || 0;
      agg[key].promo_gross += Number(r.promo_gross) || 0;
    });
    return Object.keys(agg).map(function(k) {
      var a = agg[k];
      return {
        category_id: a.category_id,
        category_name: a.category_name,
        total_gross: a.total_gross,
        promo_gross: a.promo_gross,
        promo_share_pct: a.total_gross ? Math.round(1000 * a.promo_gross / a.total_gross) / 10 : 0
      };
    }).sort(function(a, b) { return (b.promo_share_pct || 0) - (a.promo_share_pct || 0); });
  }

  function getDiscountRowsFromDetail(d, filterType, filterValue, dim) {
    var detail = d.discount_depth_detail || [];
    if (!detail.length) return null;
    var filtered = detail;
    if (filterType === 'brand' && filterValue !== '' && filterValue != null) {
      var bid = parseInt(filterValue, 10);
      if (!isNaN(bid)) {
        filtered = detail.filter(function(r) { return Number(r.brand_id) === bid || String(r.brand_id) === String(filterValue); });
      }
    } else if (filterType === 'category' && filterValue !== '' && filterValue != null) {
      var cid = parseInt(filterValue, 10);
      if (!isNaN(cid)) {
        filtered = detail.filter(function(r) { return Number(r.parent_category_id) === cid; });
      }
    } else if (filterType === 'subcategory' && filterValue !== '' && filterValue != null) {
      var sid = parseInt(filterValue, 10);
      if (!isNaN(sid)) {
        filtered = detail.filter(function(r) { return Number(r.category_id) === sid; });
      }
    } else if (filterType === 'segment' && filterValue !== '' && filterValue != null) {
      var segId = parseInt(filterValue, 10);
      if (!isNaN(segId)) {
        filtered = detail.filter(function(r) { return Number(r.segment_id) === segId; });
      }
    } else if (filterType === 'gender' && filterValue !== '' && filterValue != null) {
      filtered = detail.filter(function(r) { return String(r.gender || '') === String(filterValue); });
    }
    /* channel not in discount_depth_detail: no filter by channel */
    if (!filtered.length) return [];
    var keyField = dim === 'subcategory' ? 'category_id' : 'parent_category_id';
    var nameField = dim === 'subcategory' ? 'category_name' : 'parent_name';
    var agg = {};
    filtered.forEach(function(r) {
      var key = r[keyField];
      var name = r[nameField];
      if (!agg[key]) agg[key] = { category_id: key, category_name: name, sum: 0, n: 0 };
      agg[key].sum += Number(r.avg_discount_depth) || 0;
      agg[key].n += 1;
    });
    return Object.keys(agg).map(function(k) {
      var a = agg[k];
      return {
        category_id: a.category_id,
        category_name: a.category_name,
        avg_discount_depth: a.n ? Math.round(10 * a.sum / a.n) / 10 : 0
      };
    }).sort(function(a, b) { return (b.avg_discount_depth || 0) - (a.avg_discount_depth || 0); });
  }

  window.BasicFilters = {
    getRoiRowsFromDetail: getRoiRowsFromDetail,
    fillRoiFilterValue: fillRoiFilterValue,
    fillChartFilterValue: fillChartFilterValue,
    getPromoShareRowsFromDetail: getPromoShareRowsFromDetail,
    getDiscountRowsFromDetail: getDiscountRowsFromDetail
  };
})();
