/**
 * Basic dashboard – Peak Events chart (dimension year vs peak + metric).
 */
(function() {
  function update(d) {
    var core = window.BasicCore;
    if (!core || !d) return;
    var dimPeak = core.getDimension('chartPeak');
    var mPeak = core.getMetric('chartPeak');
    var peakVal = function(r) {
      if (mPeak === 'units') return r.units;
      if (mPeak === 'gross_pln') return r.gross_pln;
      return r.pct_of_annual;
    };
    var peakLabel = mPeak === 'units' ? 'Units' : (mPeak === 'gross_pln' ? 'Sales (PLN)' : '% of Annual Sales');
    var peakIsPct = mPeak === 'pct_of_annual';
    var c = core.gc('chartPeak');
    if (dimPeak === 'year') {
      var yoyRows = d.yoy || [];
      if (c) setChart(c, yoyRows.map(function(r) { return String(r.year); }),
        [{ label: 'Sales (PLN)', data: yoyRows.map(function(r) { return r.total_gross; }), backgroundColor: COLORS.yellow, borderWidth: 0 }]);
    } else {
      var peak = d.peak_events || [];
      if (c) setChart(c, peak.map(function(r) { return r.peak_event; }),
        [{ label: peakLabel, data: peak.map(peakVal), backgroundColor: COLORS.yellow, borderWidth: 0, _isPct: peakIsPct }]);
    }
  }

  window.BasicChartsPeak = { update: update };
})();
