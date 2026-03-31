/**
 * Basic dashboard – Buyer analytics: Channel, Loyalty, Segments, Demographics, Channel×Segment + KPI buyer.
 */
(function() {
  function update(d) {
    var core = window.BasicCore;
    if (!core || !d) return;

    var rr = (d.repeat_rate || [])[0] || {};
    var el;
    el = document.getElementById('kpi-buyers'); if (el) el.textContent = fmt(rr.total_buyers);
    el = document.getElementById('kpi-repeat'); if (el) el.textContent = rr.repeat_rate_pct != null ? rr.repeat_rate_pct + '%' : '--';
    el = document.getElementById('kpi-repeat-sub'); if (el) el.textContent = fmt(rr.repeat_buyers) + ' repeat buyers';
    el = document.getElementById('kpi-freq'); if (el) el.textContent = rr.avg_frequency || '--';
    el = document.getElementById('kpi-ltv'); if (el) el.textContent = fmt(rr.avg_lifetime_spend) + ' PLN';

    var mCh = core.getMetric('chartChannel');
    var ch = d.channel_mix || [];
    var chVal = function(r) {
      if (mCh === 'buyers') return r.buyers;
      if (mCh === 'orders') return r.orders;
      if (mCh === 'aov') return r.aov;
      return r.gross_pln;
    };
    var chLabel = mCh === 'buyers' ? 'Buyers' : mCh === 'orders' ? 'Orders' : mCh === 'aov' ? 'AOV' : 'Sales (PLN)';
    var c = core.gc('chartChannel');
    if (c) setChart(c, ch.map(function(r) { return r.channel; }), [{ label: chLabel, data: ch.map(chVal), backgroundColor: COLORS.yellow, borderWidth: 0 }]);

    var mLy = core.getMetric('chartLoyalty');
    var ly = d.loyalty_breakdown || [];
    var lyVal = function(r) { if (mLy === 'buyers') return r.buyers; if (mLy === 'aov') return r.aov; return r.gross_pln; };
    var lyLabel = mLy === 'buyers' ? 'Buyers' : mLy === 'aov' ? 'AOV' : 'Sales (PLN)';
    c = core.gc('chartLoyalty');
    if (c) setChart(c, ly.map(function(r) { return r.loyalty_tier; }), [{ label: lyLabel, data: ly.map(lyVal), backgroundColor: COLORS.yellow, borderWidth: 0 }]);

    var mSeg = core.getMetric('chartBuyerSeg');
    var bs = d.buyer_segments || [];
    var bsVal = function(r) {
      if (mSeg === 'buyers') return r.buyers;
      if (mSeg === 'aov') return r.aov;
      if (mSeg === 'promo_pct') return r.promo_pct;
      return r.gross_pln;
    };
    var bsLabel = mSeg === 'buyers' ? 'Buyers' : mSeg === 'aov' ? 'AOV' : mSeg === 'promo_pct' ? 'Promo %' : 'Sales (PLN)';
    var bsIsPct = mSeg === 'promo_pct';
    c = core.gc('chartBuyerSeg');
    if (c) setChart(c, bs.map(function(r) { return r.segment_name; }), [{ label: bsLabel, data: bs.map(bsVal), backgroundColor: COLORS.yellow, borderWidth: 0, _isPct: bsIsPct }]);

    var dimDemo = document.getElementById('dim-chartDemo');
    var dim = dimDemo ? dimDemo.value : 'gender';
    var mDemo = core.getMetric('chartDemo');
    var demo = d.buyer_demographics || [];
    var grouped = {};
    demo.forEach(function(r) {
      var key = dim === 'age' ? r.age_band : r.gender;
      if (!grouped[key]) grouped[key] = { buyers: 0, gross_pln: 0 };
      grouped[key].buyers += Number(r.buyers || 0);
      grouped[key].gross_pln += Number(r.gross_pln || 0);
    });
    var demoLabels = Object.keys(grouped).sort();
    var demoVals = demoLabels.map(function(k) { return mDemo === 'gross_pln' ? grouped[k].gross_pln : grouped[k].buyers; });
    var demoLabel = mDemo === 'gross_pln' ? 'Sales (PLN)' : 'Buyers';
    c = core.gc('chartDemo');
    if (c) setChart(c, demoLabels, [{ label: demoLabel, data: demoVals, backgroundColor: COLORS.yellow, borderWidth: 0 }]);

    var csByS = d.channel_by_segment || [];
    var channels = [];
    var chSet = {};
    csByS.forEach(function(r) { if (!chSet[r.channel]) { chSet[r.channel] = true; channels.push(r.channel); } });
    var segNames = [];
    var segSet = {};
    csByS.forEach(function(r) { if (!segSet[r.segment_name]) { segSet[r.segment_name] = true; segNames.push(r.segment_name); } });
    var channelColors = [COLORS.yellow, COLORS.slate, COLORS.grayMid];
    c = core.gc('chartChannelSeg');
    if (c) setChart(c, segNames, channels.map(function(ch, i) {
      return {
        label: ch,
        stack: 's',
        data: segNames.map(function(s) { var r = csByS.find(function(x) { return x.segment_name === s && x.channel === ch; }); return r ? r.gross_pln : 0; }),
        backgroundColor: channelColors[i % channelColors.length],
        borderWidth: 0
      };
    }));
  }

  window.BasicChartsBuyer = { update: update };
})();
