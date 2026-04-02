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

  function buildSalesUrl(ps, pe, competitorId, catIds, subIds, subCatId) {
    var q = ['period_start=' + encodeURIComponent(ps), 'period_end=' + encodeURIComponent(pe), 'competitor_id=' + encodeURIComponent(competitorId || '')];
    if (catIds && catIds.length) q.push('cat_ids=' + encodeURIComponent(catIds.join(',')));
    if (subIds && subIds.length) q.push('sub_ids=' + encodeURIComponent(subIds.join(',')));
    if (subCatId) q.push('subcategory_category_id=' + encodeURIComponent(subCatId));
    return '/api/brand-comparison/sales?' + q.join('&');
  }

  function buildPromoUrl(ps, pe, competitorId) {
    return '/api/brand-comparison/promo?period_start=' + encodeURIComponent(ps) + '&period_end=' + encodeURIComponent(pe) + '&competitor_id=' + encodeURIComponent(competitorId || '');
  }

  function buildPeakUrl(ps, pe, competitorId) {
    return '/api/brand-comparison/peak?period_start=' + encodeURIComponent(ps) + '&period_end=' + encodeURIComponent(pe) + '&competitor_id=' + encodeURIComponent(competitorId || '');
  }

  window.BCCore = {
    buildCompetitorsUrl: buildCompetitorsUrl,
    buildBaseUrl: buildBaseUrl,
    buildAllUrl: buildAllUrl,
    buildAllYearsUrl: buildAllYearsUrl,
    buildSalesUrl: buildSalesUrl,
    buildPromoUrl: buildPromoUrl,
    buildPeakUrl: buildPeakUrl
  };
})();
