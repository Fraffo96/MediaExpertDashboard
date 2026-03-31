/**
 * Basic dashboard – core: state, chart registry, bar filters, loadData, updateKPIs.
 * Depends on: Chart, COLORS, CHART_OPT, BAR_OPT, HBAR_OPT, LINE_OPT, DOUGHNUT_OPT, CHART_TOOLTIP, fetchAPI, showLoading, showError (from base).
 */
(function() {
  var reg = {};
  var _fullBasicData = null;
  var _lastBasicData = null;
  var _fullPeriod = { start: null, end: null };
  var _fetchInProgress = false;

  function optFor(t) {
    if (t === 'doughnut' || t === 'pie') return DOUGHNUT_OPT;
    if (t === 'hbar') return HBAR_OPT;
    if (t === 'line') return LINE_OPT;
    return BAR_OPT;
  }

  function mk(id, type, extraOpt) {
    var el = document.getElementById(id);
    if (!el) return null;
    var realType = type === 'hbar' ? 'bar' : type;
    var opt = extraOpt ? Object.assign({}, optFor(type), extraOpt) : optFor(type);
    var ch = new Chart(el, { type: realType, data: { labels: [], datasets: [] }, options: opt });
    reg[id] = { chart: ch, type: type, canvas: el };
    return ch;
  }

  window.switchType = function(id, newType) {
    var entry = reg[id];
    if (!entry) return;
    var data = JSON.parse(JSON.stringify(entry.chart.data));
    entry.chart.destroy();
    var realType = newType === 'hbar' ? 'bar' : newType;
    var ch = new Chart(entry.canvas, { type: realType, data: data, options: optFor(newType) });
    reg[id] = { chart: ch, type: newType, canvas: entry.canvas };
    try { localStorage.setItem('ct_' + id, newType); } catch (e) {}
  };

  function saved(id, def) { try { return localStorage.getItem('ct_' + id) || def; } catch (e) { return def; } }
  function savedMetric(id, def) { try { return localStorage.getItem('metric_' + id) || def; } catch (e) { return def; } }
  function savedLevel(id, def) { try { return localStorage.getItem('level_' + id) || def; } catch (e) { return def; } }
  function savedDimension(id, def) { try { return localStorage.getItem('dimension_' + id) || def; } catch (e) { return def; } }
  window.saveMetric = function(id, v) { try { localStorage.setItem('metric_' + id, v); } catch (e) {} };
  window.saveDisplay = function(id, v) { try { localStorage.setItem('display_' + id, v); } catch (e) {} };
  window.saveLevel = function(id, v) { try { localStorage.setItem('level_' + id, v); } catch (e) {} };
  window.saveDimension = function(id, v) { try { localStorage.setItem('dimension_' + id, v); } catch (e) {} };

  function toSharePct(arr, getVal) {
    var sum = 0;
    for (var i = 0; i < arr.length; i++) sum += Number(getVal(arr[i])) || 0;
    if (!sum) return arr.map(function() { return 0; });
    return arr.map(function(r) { return 100 * (Number(getVal(r)) || 0) / sum; });
  }

  var STACKED_BAR_OPT = Object.assign({}, CHART_OPT, {
    scales: {
      x: { stacked: true, grid: { color: '#2a2a2a' }, ticks: { color: '#999', font: { size: 12 } } },
      y: { stacked: true, max: 100, grid: { color: '#2a2a2a' },
        ticks: { color: '#999', font: { size: 12 }, callback: function(v) { return v + '%'; } } }
    },
    plugins: {
      legend: { display: true, position: 'bottom', labels: { color: '#999', font: { size: 12 }, padding: 12 } },
      tooltip: CHART_TOOLTIP
    }
  });

  var ROI_BAR_OPT = Object.assign({}, HBAR_OPT, {
    plugins: { legend: { display: false }, tooltip: CHART_TOOLTIP, annotation: undefined },
    scales: {
      x: { grid: { color: '#2a2a2a' }, ticks: { color: '#999', font: { size: 12 } } },
      y: { grid: { color: '#2a2a2a' }, ticks: { color: '#999', font: { size: 11 } } }
    }
  });

  var INCR_YOY_OPT = Object.assign({}, BAR_OPT, {
    plugins: {
      legend: { display: true, position: 'bottom', labels: { color: '#999', font: { size: 12 }, padding: 12 } },
      tooltip: CHART_TOOLTIP
    },
    scales: {
      x: { grid: { color: '#2a2a2a' }, ticks: { color: '#999', font: { size: 13 } } },
      y: { grid: { color: '#2a2a2a' }, position: 'left',
        ticks: { color: '#999', font: { size: 12 }, callback: function(v) { return v + '%'; } },
        title: { display: true, text: 'Incremental %', color: '#888', font: { size: 12 } } },
      y1: { grid: { drawOnChartArea: false }, position: 'right',
        ticks: { color: '#999', font: { size: 12 }, callback: function(v) { return (v / 1000000).toFixed(1) + 'M'; } },
        title: { display: true, text: 'Total Sales (PLN)', color: '#888', font: { size: 12 } } }
    }
  });

  var CHANNEL_SEG_OPT = Object.assign({}, BAR_OPT, {
    plugins: { legend: { display: true, position: 'bottom', labels: { color: '#999', font: { size: 12 } } }, tooltip: CHART_TOOLTIP },
    scales: { x: { stacked: true, grid: { color: '#2a2a2a' }, ticks: { color: '#999' } },
              y: { stacked: true, grid: { color: '#2a2a2a' }, ticks: { color: '#999' } } }
  });

  function gc(id) { return reg[id] ? reg[id].chart : null; }

  function getDimension(id) {
    var sel = document.querySelector('.chart-dimension-select[data-chart="' + id + '"]');
    if (sel && sel.value) return sel.value;
    var def = (id === 'chartRoi') ? 'promo_type' : (id === 'chartDiscount' || id === 'chartPromoShare') ? 'category' : 'category';
    return savedDimension(id, def);
  }

  function getMetric(id) {
    var sel = document.querySelector('.chart-metric-select[data-chart="' + id + '"]');
    return sel ? sel.value : savedMetric(id, 'gross_pln');
  }

  function getLevel(id) {
    var sel = document.querySelector('.chart-level-select[data-chart="' + id + '"]');
    return sel ? sel.value : savedLevel(id, 'parent');
  }

  function getFormFilters() {
    var form = document.getElementById('filter-form');
    if (!form) return {};
    var fd = new FormData(form);
    return {
      period_start: fd.get('period_start') || '',
      period_end: fd.get('period_end') || '',
      category_id: fd.get('category_id') || '',
      subcategory_id: fd.get('subcategory_id') || '',
      segment_id: fd.get('segment_id') || '',
      gender: fd.get('gender') || '',
      brand_id: fd.get('brand_id') || '',
      channel: fd.get('channel') || ''
    };
  }

  function subcategoryIdsForParent(parentId) {
    var p = parseInt(parentId, 10);
    if (p < 1 || p > 10) return [];
    var ids = [];
    var start = p * 100 + 1;
    var end = p <= 9 ? p * 100 + 8 : (p === 10 ? 1005 : p * 100 + 8);
    for (var i = start; i <= end; i++) ids.push(i);
    return ids;
  }

  /**
   * Deriva le viste aggregate dai dati granulari per filtri istantanei senza refetch.
   * @param {Object} d - Dati da API (deve contenere promo_share_detail, discount_depth_detail, sales_by_brand_detail, sales_by_category_by_gender, sales_by_category_by_segment, yoy_detail, peak_events_detail)
   * @param {string} cat - category_id (parent)
   * @param {string} subcat - subcategory_id
   * @param {string} seg - segment_id
   * @param {string} gender - M/F/other
   * @param {string} brand - brand_id
   * @param {string} channel - web/app/store
   * @returns {Object} Viste derivate da mergere in filterData (sales_by_brand, kpi, promo_share, yoy, peak_events, ecc.)
   */
  function deriveFromGranular(d, cat, subcat, seg, gender, brand, channel) {
    var out = {};
    var byCh = function(r) {
      if (!channel) return true;
      return (r.channel || '').toLowerCase() === String(channel).toLowerCase();
    };
    var bd = (d.sales_by_brand_detail || []).filter(byCh);
    var psd = (d.promo_share_detail || []).filter(byCh);
    var ddd = (d.discount_depth_detail || []).filter(byCh);
    var byGen = (d.sales_by_category_by_gender || []).filter(byCh);
    var bySeg = (d.sales_by_category_by_segment || []).filter(byCh);

    if (bd.length) {
      var brandRows = bd;
      if (brand) {
        var bid = parseInt(brand, 10);
        brandRows = brandRows.filter(function(r) { return Number(r.brand_id) === bid; });
      }
      if (cat) {
        var cid = parseInt(cat, 10);
        brandRows = brandRows.filter(function(r) { return Number(r.parent_category_id) === cid; });
      }
      if (subcat) {
        var sid = parseInt(subcat, 10);
        brandRows = brandRows.filter(function(r) { return Number(r.category_id) === sid; });
      }
      if (gender) brandRows = brandRows.filter(function(r) { return String(r.gender || '') === String(gender); });
      if (seg) brandRows = brandRows.filter(function(r) { return Number(r.segment_id) === parseInt(seg, 10); });
      var byBrand = {};
      brandRows.forEach(function(r) {
        var k = r.brand_id;
        if (!byBrand[k]) byBrand[k] = { brand_id: r.brand_id, brand_name: r.brand_name, gross_pln: 0, units: 0 };
        byBrand[k].gross_pln += Number(r.gross_pln) || 0;
        byBrand[k].units += Number(r.units) || 0;
      });
      out.sales_by_brand = Object.keys(byBrand).map(function(k) { return byBrand[k]; }).sort(function(a, b) { return (b.gross_pln || 0) - (a.gross_pln || 0); });
      /* Deriva sales_by_category e sales_by_subcategory da brandRows (già filtrati per gender/seg/channel) */
      if (brand && brandRows.length) {
        var catNameMap = {};
        (d.sales_by_category || []).forEach(function(r) { catNameMap[r.category_id] = r.category_name; });
        (d.sales_by_subcategory || []).forEach(function(r) { catNameMap[r.category_id] = r.category_name; });
        var byCatFromBrand = {};
        var bySubcatFromBrand = {};
        brandRows.forEach(function(r) {
          var pcId = r.parent_category_id;
          var cId = r.category_id;
          var pcName = catNameMap[pcId] || ('Cat ' + pcId);
          var cName = catNameMap[cId] || ('Subcat ' + cId);
          if (!byCatFromBrand[pcId]) byCatFromBrand[pcId] = { category_id: pcId, category_name: pcName, gross_pln: 0, units: 0 };
          byCatFromBrand[pcId].gross_pln += Number(r.gross_pln) || 0;
          byCatFromBrand[pcId].units += Number(r.units) || 0;
          if (!bySubcatFromBrand[cId]) bySubcatFromBrand[cId] = { category_id: cId, category_name: cName, gross_pln: 0, units: 0 };
          bySubcatFromBrand[cId].gross_pln += Number(r.gross_pln) || 0;
          bySubcatFromBrand[cId].units += Number(r.units) || 0;
        });
        out.sales_by_category = Object.keys(byCatFromBrand).map(function(k) { return byCatFromBrand[k]; }).sort(function(a, b) { return (b.gross_pln || 0) - (a.gross_pln || 0); });
        out.sales_by_subcategory = Object.keys(bySubcatFromBrand).map(function(k) { return bySubcatFromBrand[k]; }).sort(function(a, b) { return (b.gross_pln || 0) - (a.gross_pln || 0); });
      }
    }

    if (psd.length && (gender || seg || cat || subcat || brand || channel)) {
      var psRows = psd;
      if (gender) psRows = psRows.filter(function(r) { return String(r.gender || '') === String(gender); });
      if (seg) psRows = psRows.filter(function(r) { return Number(r.segment_id) === parseInt(seg, 10); });
      if (brand) psRows = psRows.filter(function(r) { return Number(r.brand_id) === parseInt(brand, 10); });
      if (cat) psRows = psRows.filter(function(r) { return Number(r.parent_category_id) === parseInt(cat, 10); });
      if (subcat) psRows = psRows.filter(function(r) { return Number(r.category_id) === parseInt(subcat, 10); });
      var byCat = {};
      var bySubcat = {};
      psRows.forEach(function(r) {
        var k = subcat ? r.category_id : r.parent_category_id;
        var n = subcat ? r.category_name : r.parent_name;
        if (!byCat[k]) byCat[k] = { category_id: k, category_name: n, total_gross: 0, promo_gross: 0 };
        byCat[k].total_gross += Number(r.total_gross) || 0;
        byCat[k].promo_gross += Number(r.promo_gross) || 0;
        var sk = r.category_id;
        var sn = r.category_name;
        if (!bySubcat[sk]) bySubcat[sk] = { category_id: sk, category_name: sn, total_gross: 0, promo_gross: 0 };
        bySubcat[sk].total_gross += Number(r.total_gross) || 0;
        bySubcat[sk].promo_gross += Number(r.promo_gross) || 0;
      });
      out.promo_share_by_category = Object.keys(byCat).map(function(k) {
        var a = byCat[k];
        return { category_id: a.category_id, category_name: a.category_name, total_gross: a.total_gross, promo_gross: a.promo_gross, promo_share_pct: a.total_gross ? Math.round(1000 * a.promo_gross / a.total_gross) / 10 : 0 };
      }).sort(function(a, b) { return (b.promo_share_pct || 0) - (a.promo_share_pct || 0); });
      out.promo_share_by_subcategory = Object.keys(bySubcat).map(function(k) {
        var a = bySubcat[k];
        return { category_id: a.category_id, category_name: a.category_name, total_gross: a.total_gross, promo_gross: a.promo_gross, promo_share_pct: a.total_gross ? Math.round(1000 * a.promo_gross / a.total_gross) / 10 : 0 };
      }).sort(function(a, b) { return (b.promo_share_pct || 0) - (a.promo_share_pct || 0); });
      out.sales_by_subcategory = Object.keys(bySubcat).map(function(k) {
        var a = bySubcat[k];
        return { category_id: a.category_id, category_name: a.category_name, gross_pln: a.total_gross, units: 0 };
      }).sort(function(a, b) { return (b.gross_pln || 0) - (a.gross_pln || 0); });
      var totGross = 0, totPromo = 0;
      psRows.forEach(function(r) { totGross += Number(r.total_gross) || 0; totPromo += Number(r.promo_gross) || 0; });
      out.promo_share = [{ total_gross: totGross, promo_gross: totPromo, promo_share_pct: totGross ? Math.round(1000 * totPromo / totGross) / 10 : 0 }];
    }

    if (ddd.length && (gender || seg || cat || subcat || brand || channel)) {
      var ddRows = ddd;
      if (gender) ddRows = ddRows.filter(function(r) { return String(r.gender || '') === String(gender); });
      if (seg) ddRows = ddRows.filter(function(r) { return Number(r.segment_id) === parseInt(seg, 10); });
      if (brand) ddRows = ddRows.filter(function(r) { return Number(r.brand_id) === parseInt(brand, 10); });
      if (cat) ddRows = ddRows.filter(function(r) { return Number(r.parent_category_id) === parseInt(cat, 10); });
      if (subcat) ddRows = ddRows.filter(function(r) { return Number(r.category_id) === parseInt(subcat, 10); });
      var byCatD = {};
      ddRows.forEach(function(r) {
        var k = subcat ? r.category_id : r.parent_category_id;
        var n = subcat ? r.category_name : r.parent_name;
        if (!byCatD[k]) byCatD[k] = { category_id: k, category_name: n, sum: 0, n: 0 };
        byCatD[k].sum += Number(r.avg_discount_depth) || 0;
        byCatD[k].n += 1;
      });
      out.discount_depth_by_category = Object.keys(byCatD).map(function(k) {
        var a = byCatD[k];
        return { category_id: a.category_id, category_name: a.category_name, avg_discount_depth: a.n ? Math.round(10 * a.sum / a.n) / 10 : 0 };
      }).sort(function(a, b) { return (b.avg_discount_depth || 0) - (a.avg_discount_depth || 0); });
    }

    if (byGen.length && (gender || channel)) {
      var genRows = gender ? byGen.filter(function(r) { return r.gender === gender; }) : byGen;
      var genAgg = { gross_pln: 0, units: 0 };
      genRows.forEach(function(r) { genAgg.gross_pln += Number(r.gross_pln) || 0; genAgg.units += Number(r.units) || 0; });
      if (!cat && !seg) out.kpi = [{ total_gross: genAgg.gross_pln, total_units: genAgg.units, promo_share_pct: (d.kpi && d.kpi[0]) ? d.kpi[0].promo_share_pct : null, promo_gross: null, avg_discount_depth: (d.kpi && d.kpi[0]) ? d.kpi[0].avg_discount_depth : null }];
      out.sales_by_category_by_gender = genRows;
      /* Aggrega sempre per category_id (genRows ha una riga per category+gender+channel) */
      var byCatGen = {};
      genRows.forEach(function(r) {
        var k = r.category_id;
        if (!byCatGen[k]) byCatGen[k] = { category_id: k, category_name: r.category_name, gross_pln: 0, units: 0 };
        byCatGen[k].gross_pln += Number(r.gross_pln) || 0;
        byCatGen[k].units += Number(r.units) || 0;
      });
      out.sales_by_category = Object.keys(byCatGen).map(function(k) { return byCatGen[k]; }).sort(function(a, b) { return (b.gross_pln || 0) - (a.gross_pln || 0); });
    }
    if (bySeg.length && (seg || channel)) {
      var segId = parseInt(seg, 10);
      var segRows = seg ? bySeg.filter(function(r) { return Number(r.segment_id) === segId; }) : bySeg;
      var segAgg = { gross_pln: 0, units: 0 };
      segRows.forEach(function(r) { segAgg.gross_pln += Number(r.gross_pln) || 0; segAgg.units += Number(r.units) || 0; });
      if (!cat && !gender) out.kpi = out.kpi || [{ total_gross: segAgg.gross_pln, total_units: segAgg.units, promo_share_pct: (d.kpi && d.kpi[0]) ? d.kpi[0].promo_share_pct : null, promo_gross: null, avg_discount_depth: (d.kpi && d.kpi[0]) ? d.kpi[0].avg_discount_depth : null }];
      out.sales_by_category_by_segment = segRows;
      /* Aggrega per categoria quando filtro segmento o canale: evita bug "stesso numero per segmenti diversi" */
      if ((seg || channel) && !out.sales_by_category) {
        var byCatSeg = {};
        segRows.forEach(function(r) {
          var k = r.category_id;
          if (!byCatSeg[k]) byCatSeg[k] = { category_id: k, category_name: r.category_name, gross_pln: 0, units: 0 };
          byCatSeg[k].gross_pln += Number(r.gross_pln) || 0;
          byCatSeg[k].units += Number(r.units) || 0;
        });
        out.sales_by_category = Object.keys(byCatSeg).map(function(k) { return byCatSeg[k]; }).sort(function(a, b) { return (b.gross_pln || 0) - (a.gross_pln || 0); });
      }
    }

    if (out.kpi && out.promo_share && out.promo_share[0]) {
      out.kpi[0].promo_share_pct = out.promo_share[0].promo_share_pct;
      out.kpi[0].promo_gross = out.promo_share[0].promo_gross;
    }
    if (out.kpi && out.discount_depth_by_category && out.discount_depth_by_category.length) {
      var wAvg = 0, wTot = 0;
      (out.discount_depth_by_category || []).forEach(function(r) { wAvg += (Number(r.avg_discount_depth) || 0) * 1; wTot += 1; });
      out.kpi[0].avg_discount_depth = wTot ? Math.round(10 * wAvg / wTot) / 10 : null;
    }
    if (brand && out.sales_by_brand && out.sales_by_brand.length === 1 && !out.kpi) {
      var ob = out.sales_by_brand[0];
      out.kpi = [{ total_gross: ob.gross_pln, total_units: ob.units, promo_share_pct: out.promo_share && out.promo_share[0] ? out.promo_share[0].promo_share_pct : null, promo_gross: out.promo_share && out.promo_share[0] ? out.promo_share[0].promo_gross : null, avg_discount_depth: null }];
      if (out.discount_depth_by_category && out.discount_depth_by_category.length) {
        var wAvgB = 0, wTotB = 0;
        out.discount_depth_by_category.forEach(function(r) { wAvgB += (Number(r.avg_discount_depth) || 0) * 1; wTotB += 1; });
        out.kpi[0].avg_discount_depth = wTotB ? Math.round(10 * wAvgB / wTotB) / 10 : null;
      }
    }

    if ((d.yoy_detail || []).length && (gender || seg)) {
      var yoyRows = d.yoy_detail;
      if (gender) yoyRows = yoyRows.filter(function(r) { return String(r.gender || '') === String(gender); });
      if (seg) yoyRows = yoyRows.filter(function(r) { return Number(r.segment_id) === parseInt(seg, 10); });
      var byYear = {};
      yoyRows.forEach(function(r) {
        var y = r.year;
        if (!byYear[y]) byYear[y] = { year: y, total_gross: 0, promo_gross: 0 };
        byYear[y].total_gross += Number(r.total_gross) || 0;
        byYear[y].promo_gross += Number(r.promo_gross) || 0;
      });
      var years = Object.keys(byYear).sort();
      out.yoy = years.map(function(y, i) {
        var v = byYear[y];
        var prior = i > 0 ? byYear[years[i - 1]].total_gross : 0;
        return { year: parseInt(y, 10), total_gross: v.total_gross, promo_gross: v.promo_gross, prior_gross: prior, yoy_pct: prior ? Math.round(1000 * (v.total_gross - prior) / prior) / 10 : null };
      });
    }

    if ((d.peak_events_detail || []).length && (gender || seg)) {
      var peakRows = d.peak_events_detail;
      if (gender) peakRows = peakRows.filter(function(r) { return String(r.gender || '') === String(gender); });
      if (seg) peakRows = peakRows.filter(function(r) { return Number(r.segment_id) === parseInt(seg, 10); });
      var byPeak = {};
      var annualTot = 0;
      peakRows.forEach(function(r) {
        var p = r.peak_event;
        if (!byPeak[p]) byPeak[p] = { peak_event: p, gross_pln: 0, units: 0, days_count: 0 };
        byPeak[p].gross_pln += Number(r.gross_pln) || 0;
        byPeak[p].units += Number(r.units) || 0;
        byPeak[p].days_count += Number(r.days_count) || 0;
        annualTot += Number(r.gross_pln) || 0;
      });
      out.peak_events = Object.keys(byPeak).map(function(p) {
        var v = byPeak[p];
        return { peak_event: p, gross_pln: v.gross_pln, units: v.units, days_count: v.days_count, pct_of_annual: annualTot ? Math.round(1000 * v.gross_pln / annualTot) / 10 : 0 };
      }).sort(function(a, b) { return (b.gross_pln || 0) - (a.gross_pln || 0); });
    }

    return out;
  }

  function filterTopProductsByChannel(rows, channel) {
    if (!channel || !rows || !rows.length) return rows;
    var ch = String(channel).toLowerCase();
    var filtered = rows.filter(function(r) { return (r.channel || '').toLowerCase() === ch; });
    var byProd = {};
    filtered.forEach(function(r) {
      var k = r.product_id;
      if (!byProd[k]) byProd[k] = { product_id: k, product_name: r.product_name, brand_id: r.brand_id, brand_name: r.brand_name, category_id: r.category_id, category_name: r.category_name, gross_pln: 0, units: 0 };
      byProd[k].gross_pln += Number(r.gross_pln) || 0;
      byProd[k].units += Number(r.units) || 0;
    });
    return Object.keys(byProd).map(function(k) { return byProd[k]; }).sort(function(a, b) { return (b.gross_pln || 0) - (a.gross_pln || 0); }).slice(0, 20);
  }

  function filterData(d, filters) {
    if (!d) return d;
    var cat = filters.category_id ? String(filters.category_id).trim() : '';
    var subcat = filters.subcategory_id ? String(filters.subcategory_id).trim() : '';
    var seg = filters.segment_id ? String(filters.segment_id).trim() : '';
    var gender = filters.gender ? String(filters.gender).trim() : '';
    var brand = filters.brand_id ? String(filters.brand_id).trim() : '';
    var channel = filters.channel ? String(filters.channel).trim() : '';
    if (!cat && !subcat && !seg && !gender && !brand && !channel) return d;

    var out = {};
    var derived = deriveFromGranular(d, cat, subcat, seg, gender, brand, channel);
    if (derived.sales_by_brand) out.sales_by_brand = derived.sales_by_brand;
    if (derived.promo_share_by_category) out.promo_share_by_category = derived.promo_share_by_category;
    if (derived.promo_share_by_subcategory) out.promo_share_by_subcategory = derived.promo_share_by_subcategory;
    if (derived.promo_share) out.promo_share = derived.promo_share;
    if (derived.discount_depth_by_category) out.discount_depth_by_category = derived.discount_depth_by_category;
    if (derived.kpi) out.kpi = derived.kpi;
    if (derived.sales_by_category) out.sales_by_category = derived.sales_by_category;
    if (derived.sales_by_subcategory) out.sales_by_subcategory = derived.sales_by_subcategory;
    if (derived.sales_by_category_by_gender) out.sales_by_category_by_gender = derived.sales_by_category_by_gender;
    if (derived.sales_by_category_by_segment) out.sales_by_category_by_segment = derived.sales_by_category_by_segment;
    if (derived.yoy) out.yoy = derived.yoy;
    if (derived.peak_events) out.peak_events = derived.peak_events;
    var k;

    if (subcat) {
      var subcatId = parseInt(subcat, 10);
      var parentId = subcatId >= 100 ? Math.floor(subcatId / 100) : subcatId;
      var srcSubcat = out.sales_by_subcategory || derived.sales_by_subcategory || d.sales_by_subcategory || [];
      var srcPscSub = out.promo_share_by_subcategory || derived.promo_share_by_subcategory || d.promo_share_by_subcategory || [];
      var srcCat = out.sales_by_category || derived.sales_by_category || d.sales_by_category || [];
      var srcPscCat = out.promo_share_by_category || derived.promo_share_by_category || d.promo_share_by_category || [];
      var srcDisc = out.discount_depth_by_category || derived.discount_depth_by_category || d.discount_depth_by_category || [];
      var subcatRows = srcSubcat.filter(function(r) { return Number(r.category_id) === subcatId; });
      var oneSub = subcatRows[0];
      var pscSub = srcPscSub.filter(function(r) { return Number(r.category_id) === subcatId; })[0];
      var catRows = srcCat.filter(function(r) { return r.category_id === parentId; });
      var parentCat = catRows[0];
      out.sales_by_category = catRows;
      out.sales_by_subcategory = subcatRows;
      out.promo_share_by_category = srcPscCat.filter(function(r) { return r.category_id === parentId; });
      out.promo_share_by_subcategory = pscSub ? [pscSub] : [];
      var discSubcat = srcDisc.filter(function(r) { return Number(r.category_id) === subcatId; })[0];
      var discParent = srcDisc.filter(function(r) { return Number(r.category_id) === parentId; })[0];
      out.discount_depth_by_category = discSubcat ? [discSubcat] : (discParent ? [discParent] : []);
      var avgDiscSubcat = (discSubcat || discParent) ? (discSubcat || discParent).avg_discount_depth : null;
      out.kpi = pscSub ? [{ total_gross: pscSub.total_gross, total_units: oneSub ? oneSub.units : pscSub.total_gross, promo_share_pct: pscSub.promo_share_pct, promo_gross: pscSub.promo_gross, avg_discount_depth: avgDiscSubcat }] : (d.kpi || []);
      var crosstab = d.sales_brand_category || [];
      if (brand) { var bid = parseInt(brand, 10); crosstab = crosstab.filter(function(r) { return Number(r.brand_id) === bid; }); }
      var catName = parentCat ? parentCat.category_name : null;
      if (catName && crosstab.length) {
        var byBrand = {};
        crosstab.filter(function(r) { return r.category_name === catName; }).forEach(function(r) {
          var b = r.brand_name;
          var bid = r.brand_id;
          if (!byBrand[b]) byBrand[b] = { brand_id: bid, brand_name: b, gross_pln: 0, units: 0 };
          byBrand[b].gross_pln += Number(r.gross_pln) || 0;
          byBrand[b].units += Number(r.units) || 0;
        });
        out.sales_by_brand = Object.keys(byBrand).map(function(key) { return byBrand[key]; }).sort(function(a, b) { return (b.gross_pln || 0) - (a.gross_pln || 0); });
      }
    } else if (cat) {
      var catId = parseInt(cat, 10);
      var srcCat2 = out.sales_by_category || derived.sales_by_category || d.sales_by_category || [];
      var srcPscCat2 = out.promo_share_by_category || derived.promo_share_by_category || d.promo_share_by_category || [];
      var srcSubcat2 = out.sales_by_subcategory || derived.sales_by_subcategory || d.sales_by_subcategory || [];
      var srcPscSub2 = out.promo_share_by_subcategory || derived.promo_share_by_subcategory || d.promo_share_by_subcategory || [];
      var srcDisc2 = out.discount_depth_by_category || derived.discount_depth_by_category || d.discount_depth_by_category || [];
      var catRows = srcCat2.filter(function(r) { return r.category_id === catId; });
      var oneCat = catRows[0];
      var pscRows = srcPscCat2.filter(function(r) { return r.category_id === catId; });
      var onePsc = pscRows[0];
      var subIds = subcategoryIdsForParent(catId);
      out.sales_by_category = catRows;
      out.sales_by_subcategory = srcSubcat2.filter(function(r) { return subIds.indexOf(Number(r.category_id)) !== -1; });
      out.promo_share_by_category = pscRows;
      out.promo_share_by_subcategory = srcPscSub2.filter(function(r) { return subIds.indexOf(Number(r.category_id)) !== -1; });
      out.discount_depth_by_category = srcDisc2.filter(function(r) { return r.category_id === catId; });
      out.kpi = onePsc ? [{ total_gross: onePsc.total_gross, total_units: oneCat ? oneCat.units : onePsc.total_gross, promo_share_pct: onePsc.promo_share_pct, promo_gross: onePsc.promo_gross, avg_discount_depth: (d.kpi && d.kpi[0]) ? d.kpi[0].avg_discount_depth : null }] : (d.kpi || []);
      var crosstab2 = d.sales_brand_category || [];
      if (brand) { var bid2 = parseInt(brand, 10); crosstab2 = crosstab2.filter(function(r) { return Number(r.brand_id) === bid2; }); }
      var catName = oneCat ? oneCat.category_name : null;
      if (catName && crosstab2.length) {
        var byBrand = {};
        crosstab2.filter(function(r) { return r.category_name === catName; }).forEach(function(r) {
          var b = r.brand_name;
          var bid = r.brand_id;
          if (!byBrand[b]) byBrand[b] = { brand_id: bid, brand_name: b, gross_pln: 0, units: 0 };
          byBrand[b].gross_pln += Number(r.gross_pln) || 0;
          byBrand[b].units += Number(r.units) || 0;
        });
        out.sales_by_brand = Object.keys(byBrand).map(function(key) { return byBrand[key]; }).sort(function(a, b) { return (b.gross_pln || 0) - (a.gross_pln || 0); });
      }
      var srcBySeg = derived.sales_by_category_by_segment || d.sales_by_category_by_segment || [];
      var srcByGen = derived.sales_by_category_by_gender || d.sales_by_category_by_gender || [];
      out.sales_by_category_by_segment = srcBySeg.filter(function(r) { return r.category_id === catId; });
      out.sales_by_category_by_gender = srcByGen.filter(function(r) { return r.category_id === catId; });
    } else {
      /* Non sovrascrivere se derived ha già i dati (es. filtro brand) */
      out.sales_by_category = out.sales_by_category || derived.sales_by_category || d.sales_by_category || [];
      out.sales_by_subcategory = out.sales_by_subcategory || derived.sales_by_subcategory || d.sales_by_subcategory || [];
      out.promo_share_by_category = out.promo_share_by_category || derived.promo_share_by_category || d.promo_share_by_category || [];
      out.promo_share_by_subcategory = out.promo_share_by_subcategory || derived.promo_share_by_subcategory || d.promo_share_by_subcategory || [];
      out.discount_depth_by_category = out.discount_depth_by_category || derived.discount_depth_by_category || d.discount_depth_by_category || [];
      out.kpi = out.kpi || derived.kpi || d.kpi || [];
    }

    var hasDerivedKpi = !!out.kpi;
    if (seg) {
      var segId = parseInt(seg, 10);
      var bySeg = out.sales_by_category_by_segment !== undefined ? out.sales_by_category_by_segment : (d.sales_by_category_by_segment || []);
      var segRows = bySeg.filter(function(r) { return Number(r.segment_id) === segId; });
      out.sales_by_category_by_segment = segRows;
      if (!cat && !hasDerivedKpi) {
        var segAgg = { gross_pln: 0, units: 0 };
        segRows.forEach(function(r) { segAgg.gross_pln += Number(r.gross_pln) || 0; segAgg.units += Number(r.units) || 0; });
        var psSeg = (out.promo_share && out.promo_share[0]) ? out.promo_share[0] : (derived.promo_share && derived.promo_share[0]) ? derived.promo_share[0] : (d.kpi && d.kpi[0]);
        var ddSeg = (out.discount_depth_by_category && out.discount_depth_by_category.length) ? out.discount_depth_by_category : (derived.discount_depth_by_category && derived.discount_depth_by_category.length) ? derived.discount_depth_by_category : null;
        var avgDisc = ddSeg && ddSeg.length ? (function() { var w = 0, t = 0; ddSeg.forEach(function(r) { w += (Number(r.avg_discount_depth) || 0) * 1; t += 1; }); return t ? Math.round(10 * w / t) / 10 : null; })() : (d.kpi && d.kpi[0]) ? d.kpi[0].avg_discount_depth : null;
        out.kpi = [{ total_gross: segAgg.gross_pln, total_units: segAgg.units, promo_share_pct: psSeg ? psSeg.promo_share_pct : null, promo_gross: psSeg ? psSeg.promo_gross : null, avg_discount_depth: avgDisc }];
      }
    } else {
      out.sales_by_category_by_segment = d.sales_by_category_by_segment || [];
    }

    if (gender) {
      var byGen = out.sales_by_category_by_gender !== undefined ? out.sales_by_category_by_gender : (d.sales_by_category_by_gender || []);
      var genRows = byGen.filter(function(r) { return r.gender === gender; });
      out.sales_by_category_by_gender = genRows;
      if (!cat && !seg && !hasDerivedKpi) {
        var genAgg = { gross_pln: 0, units: 0 };
        genRows.forEach(function(r) { genAgg.gross_pln += Number(r.gross_pln) || 0; genAgg.units += Number(r.units) || 0; });
        var psGen = (out.promo_share && out.promo_share[0]) ? out.promo_share[0] : (derived.promo_share && derived.promo_share[0]) ? derived.promo_share[0] : (d.kpi && d.kpi[0]);
        var ddGen = (out.discount_depth_by_category && out.discount_depth_by_category.length) ? out.discount_depth_by_category : (derived.discount_depth_by_category && derived.discount_depth_by_category.length) ? derived.discount_depth_by_category : null;
        var avgDiscGen = ddGen && ddGen.length ? (function() { var w = 0, t = 0; ddGen.forEach(function(r) { w += (Number(r.avg_discount_depth) || 0) * 1; t += 1; }); return t ? Math.round(10 * w / t) / 10 : null; })() : (d.kpi && d.kpi[0]) ? d.kpi[0].avg_discount_depth : null;
        out.kpi = [{ total_gross: genAgg.gross_pln, total_units: genAgg.units, promo_share_pct: psGen ? psGen.promo_share_pct : null, promo_gross: psGen ? psGen.promo_gross : null, avg_discount_depth: avgDiscGen }];
      }
    } else {
      out.sales_by_category_by_gender = d.sales_by_category_by_gender || [];
    }

    if (brand) {
      var brandId = parseInt(brand, 10);
      var baseBrand = out.sales_by_brand !== undefined ? out.sales_by_brand : (d.sales_by_brand || []);
      var brandRows = baseBrand.filter(function(r) { return Number(r.brand_id) === brandId; });
      var oneBrand = brandRows[0];
      out.sales_by_brand = brandRows;
      if (!cat && !subcat && !seg && !gender && !hasDerivedKpi) {
        var psBrand = (out.promo_share && out.promo_share[0]) ? out.promo_share[0] : (derived.promo_share && derived.promo_share[0]) ? derived.promo_share[0] : (d.kpi && d.kpi[0]);
        var ddBrand = (out.discount_depth_by_category && out.discount_depth_by_category.length) ? out.discount_depth_by_category : (derived.discount_depth_by_category && derived.discount_depth_by_category.length) ? derived.discount_depth_by_category : null;
        var avgDiscBrand = ddBrand && ddBrand.length ? (function() { var w = 0, t = 0; ddBrand.forEach(function(r) { w += (Number(r.avg_discount_depth) || 0) * 1; t += 1; }); return t ? Math.round(10 * w / t) / 10 : null; })() : (d.kpi && d.kpi[0]) ? d.kpi[0].avg_discount_depth : null;
        out.kpi = oneBrand ? [{ total_gross: oneBrand.gross_pln, total_units: oneBrand.units, promo_share_pct: psBrand ? psBrand.promo_share_pct : null, promo_gross: psBrand ? psBrand.promo_gross : null, avg_discount_depth: avgDiscBrand }] : (d.kpi || []);
      }
    } else if (out.sales_by_brand === undefined) {
      out.sales_by_brand = d.sales_by_brand || [];
    }

    if (channel) {
      var ch = channel.toLowerCase();
      var byCh = function(r) { return (r.channel || '').toLowerCase() === ch; };
      if (d.channel_mix) out.channel_mix = (d.channel_mix || []).filter(byCh);
      if (d.loyalty_breakdown) out.loyalty_breakdown = (d.loyalty_breakdown || []).filter(byCh);
      if (d.buyer_segments) out.buyer_segments = (d.buyer_segments || []).filter(byCh);
      if (d.buyer_demographics) out.buyer_demographics = (d.buyer_demographics || []).filter(byCh);
      if (d.channel_by_segment) out.channel_by_segment = (d.channel_by_segment || []).filter(byCh);
      if (d.repeat_rate_by_channel) {
        var rr = (d.repeat_rate_by_channel || []).filter(byCh)[0];
        out.repeat_rate = rr ? [rr] : (d.repeat_rate || []);
      }
      if (d.top_products) out.top_products = filterTopProductsByChannel(d.top_products, channel);
    }

    for (k in d) if (out[k] === undefined) out[k] = d[k];
    return out;
  }

  function updateKPIs(d) {
    var k = (d.kpi || [])[0] || {};
    var el;
    el = document.getElementById('kpi-total'); if (el) el.textContent = fmt(k.total_gross) + ' PLN';
    el = document.getElementById('kpi-units'); if (el) el.textContent = fmt(k.total_units) + ' units';
    el = document.getElementById('kpi-promo'); if (el) el.textContent = k.promo_share_pct != null ? fmtPct(k.promo_share_pct) : '--';
    var ps = (d.promo_share || [])[0];
    el = document.getElementById('kpi-promo-sub'); if (el) el.textContent = ps ? fmt(ps.promo_gross) + ' of ' + fmt(ps.total_gross) + ' PLN' : '--';
    el = document.getElementById('kpi-disc'); if (el) el.textContent = k.avg_discount_depth != null ? Number(k.avg_discount_depth).toFixed(1) + '%' : '--';

    var yoyArr = d.yoy || [];
    var lastYoy = yoyArr.length > 0 ? yoyArr[yoyArr.length - 1] : null;
    el = document.getElementById('kpi-yoy');
    if (el && lastYoy && lastYoy.yoy_pct != null) {
      var pct = Number(lastYoy.yoy_pct);
      el.textContent = (pct >= 0 ? '+' : '') + pct.toFixed(1) + '%';
      el.className = 'kpi-value ' + (pct >= 0 ? 'green' : 'red');
    } else if (el) {
      el.textContent = '--';
    }
    el = document.getElementById('kpi-yoy-sub');
    if (el && lastYoy) el.textContent = lastYoy.year + ' vs ' + (lastYoy.year - 1);
  }

  function applyFiltersFromForm() {
    var f = getFormFilters();
    var data = _fullBasicData ? filterData(_fullBasicData, f) : _fullBasicData;
    if (!data) return;
    _lastBasicData = data;
    updateKPIs(data);
    if (typeof window.BasicUpdateCharts === 'function') window.BasicUpdateCharts(data);
    var note = document.getElementById('channel-filter-note');
    if (note) {
      var ch = (f.channel || '').trim().toLowerCase();
      if (ch) {
        var chLabel = ch === 'web' ? 'Web' : ch === 'app' ? 'App' : ch === 'store' ? 'Store' : ch;
        note.textContent = 'Filtro canale \"' + chLabel + '\" applicato a tutta la dashboard: KPI, Categoria, Promo, Buyer e Prodotti.';
        note.style.display = 'block';
      } else {
        note.textContent = '';
        note.style.display = 'none';
      }
    }
  }

  function debounce(fn, ms) {
    var t;
    return function() {
      clearTimeout(t);
      var self = this, args = arguments;
      t = setTimeout(function() { fn.apply(self, args); }, ms);
    };
  }

  async function loadDataImmediate(forceFetch) {
    var f = getFormFilters();
    var periodChanged = forceFetch || !_fullBasicData ||
      _fullPeriod.start !== f.period_start || _fullPeriod.end !== f.period_end;
    if (periodChanged) {
      if (_fetchInProgress && !forceFetch) {
        /* Fetch già in corso: non avviarne un altro (salvo refresh esplicito) */
        return;
      }
      _fetchInProgress = true;
      showLoading(true);
      showError('');
      try {
        /* Fetch sempre senza channel: dati per tutti i canali, filtro channel applicato lato client istantaneo */
        var params = 'period_start=' + encodeURIComponent(f.period_start || '') + '&period_end=' + encodeURIComponent(f.period_end || '');
        var r = await fetch(window.location.origin + '/api/basic/granular?' + params);
        if (!r.ok) {
          var errText = await r.text();
          throw new Error(r.status + ': ' + (errText.substring(0, 80) || r.statusText));
        }
        var d = await r.json();
        _fullBasicData = d;
        _fullPeriod = { start: f.period_start, end: f.period_end };
      } catch (e) {
        showError('Error: ' + (e.message || e));
        return;
      } finally {
        _fetchInProgress = false;
        showLoading(false);
      }
    }
    applyFiltersFromForm();
  }

  function applyFiltersOrRefetch() {
    var f = getFormFilters();
    /* Refetch SOLO se cambia il periodo: category, segment, gender, brand, channel sono sempre filtro client-side istantaneo */
    var periodChanged = !_fullBasicData || _fullPeriod.start !== f.period_start || _fullPeriod.end !== f.period_end;
    if (periodChanged) loadDataImmediate(false);
    else applyFiltersFromForm();
  }

  var loadData = debounce(function() { loadDataImmediate(false); }, 350);

  function refreshData() {
    loadDataImmediate(true);
  }

  window.BasicCore = {
    reg: reg,
    gc: gc,
    mk: mk,
    optFor: optFor,
    getDimension: getDimension,
    getMetric: getMetric,
    getLevel: getLevel,
    getFormFilters: getFormFilters,
    filterData: filterData,
    subcategoryIdsForParent: subcategoryIdsForParent,
    updateKPIs: updateKPIs,
    applyFiltersFromForm: applyFiltersFromForm,
    applyFiltersOrRefetch: applyFiltersOrRefetch,
    loadDataImmediate: loadDataImmediate,
    loadData: loadData,
    refreshData: refreshData,
    toSharePct: toSharePct,
    saved: saved,
    savedMetric: savedMetric,
    savedLevel: savedLevel,
    savedDimension: savedDimension,
    STACKED_BAR_OPT: STACKED_BAR_OPT,
    ROI_BAR_OPT: ROI_BAR_OPT,
    INCR_YOY_OPT: INCR_YOY_OPT,
    CHANNEL_SEG_OPT: CHANNEL_SEG_OPT,
    getLastData: function() { return _lastBasicData; },
    setLastData: function(d) { _lastBasicData = d; },
    getFullData: function() { return _fullBasicData; },
    setFullData: function(d) { _fullBasicData = d; },
    getFullPeriod: function() { return _fullPeriod; },
    setFullPeriod: function(p) { _fullPeriod = p; }
  };
})();
