/**
 * Promo Creator – core: params, buildUrl.
 * Depends on: showLoadingLight (from base).
 */
(function() {
  function getParams() {
    return {
      category_id: document.getElementById('pc-category').value || undefined,
      subcategory_id: document.getElementById('pc-subcategory').value || undefined,
      promo_type: document.getElementById('pc-promo-type').value || undefined,
      discount_depth: document.getElementById('pc-discount-depth').value || undefined
    };
  }

  function buildUrl() {
    var p = getParams();
    var q = [];
    if (p.category_id) q.push('category_id=' + encodeURIComponent(p.category_id));
    if (p.subcategory_id) q.push('subcategory_id=' + encodeURIComponent(p.subcategory_id));
    if (p.promo_type) q.push('promo_type=' + encodeURIComponent(p.promo_type));
    if (p.discount_depth) q.push('discount_depth=' + encodeURIComponent(p.discount_depth));
    return '/api/promo-creator?' + q.join('&');
  }

  window.PCCore = {
    getParams: getParams,
    buildUrl: buildUrl
  };
})();
