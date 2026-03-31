/**
 * Brand Comparison – core: buildUrl per BC API.
 * Stessa struttura di Market Intelligence ma endpoint /api/brand-comparison/*.
 */
(function() {
  function buildCompetitorsUrl(ps, pe) {
    return '/api/brand-comparison/competitors?period_start=' + encodeURIComponent(ps) + '&period_end=' + encodeURIComponent(pe);
  }

  function buildBaseUrl(ps, pe) {
    return '/api/brand-comparison/base?period_start=' + encodeURIComponent(ps) + '&period_end=' + encodeURIComponent(pe);
  }

  function buildAllUrl(ps, pe, competitorId, discCat, discSub) {
    var q = ['period_start=' + encodeURIComponent(ps), 'period_end=' + encodeURIComponent(pe), 'competitor_id=' + encodeURIComponent(competitorId || '')];
    if (discCat) q.push('discount_category_id=' + encodeURIComponent(discCat));
    if (discSub) q.push('discount_subcategory_id=' + encodeURIComponent(discSub));
    return '/api/brand-comparison/all?' + q.join('&');
  }

  function buildAllYearsUrl(competitorId, discCat, discSub) {
    var q = ['competitor_id=' + encodeURIComponent(competitorId || '')];
    if (discCat) q.push('discount_category_id=' + encodeURIComponent(discCat));
    if (discSub) q.push('discount_subcategory_id=' + encodeURIComponent(discSub));
    return '/api/brand-comparison/all-years?' + q.join('&');
  }

  window.BCCore = {
    buildCompetitorsUrl: buildCompetitorsUrl,
    buildBaseUrl: buildBaseUrl,
    buildAllUrl: buildAllUrl,
    buildAllYearsUrl: buildAllYearsUrl
  };
})();
