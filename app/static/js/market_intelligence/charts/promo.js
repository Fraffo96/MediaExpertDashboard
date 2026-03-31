/**
 * Market Intelligence – Promo charts (share by category, ROI).
 * Graph 3: grouped bars (Samsung + Category average side by side), like graph 4.
 */
(function() {
  function updatePromoShare(d) {
    if (!d) return;
    var core = window.MICore;
    var ps = d.promo_share_by_category || [];
    var brandLabel = (typeof window.MI_BRAND_NAME !== 'undefined' && window.MI_BRAND_NAME) ? window.MI_BRAND_NAME : 'Your Brand';
    var promoShareEl = document.getElementById('chart-promo-share');
    if (promoShareEl && ps.length && typeof Chart !== 'undefined' && core) {
      var bar = core.barData(
        ps.map(function(x) { return x.category_name; }),
        ps.map(function(x) { return Number(x.brand_promo_share_pct) || 0; }),
        ps.map(function(x) { return Number(x.media_promo_share_pct) || 0; })
      );
      bar.datasets[0].label = brandLabel;
      bar.datasets[1].label = (d.competitor_name || d.second_series_label) || 'Category average';
      if (promoShareEl.chart) { promoShareEl.chart.destroy(); promoShareEl.chart = null; }
      promoShareEl.chart = new Chart(promoShareEl.getContext('2d'), {
        type: 'bar',
        data: bar,
        options: core.MI_BAR_OPT || {}
      });
    }
  }

  function updatePromoRoi(d) {
    if (!d) return;
    var core = window.MICore;
    var roi = d.promo_roi || [];
    var roiEl = document.getElementById('chart-promo-roi');
    if (!roiEl || typeof Chart === 'undefined' || !core) return;
    if (roiEl.chart) {
      roiEl.chart.destroy();
      roiEl.chart = null;
    }
    if (!roi.length) {
      return;
    }
    var bar = core.barData(
      roi.map(function(x) { return x.promo_type; }),
      roi.map(function(x) { return Number(x.brand_avg_roi) || 0; }),
      roi.map(function(x) { return Number(x.media_avg_roi) || 0; })
    );
    bar.datasets[1].label = (d.competitor_name || d.second_series_label) || 'Category average';
    roiEl.chart = new Chart(roiEl.getContext('2d'), {
      type: 'bar',
      data: bar,
      options: core.MI_BAR_OPT || {}
    });
  }

  function update(d) {
    updatePromoShare(d);
    updatePromoRoi(d);
  }

  window.MIChartsPromo = {
    update: update,
    updatePromoShareOnly: updatePromoShare,
    updatePromoRoiOnly: updatePromoRoi
  };
})();
