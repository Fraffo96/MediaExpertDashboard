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

  /** Category (parent), promo type, and discount depth are required before calling the API. */
  function validatePromoDraft() {
    var p = getParams();
    var missing = [];
    if (!p.promo_type || !String(p.promo_type).trim()) missing.push('promo type');
    if (p.discount_depth === undefined || p.discount_depth === null || String(p.discount_depth).trim() === '') {
      missing.push('discount depth (%)');
    } else {
      var dd = Number(String(p.discount_depth).trim());
      if (isNaN(dd) || dd < 0 || dd > 100) {
        return { ok: false, message: 'Discount depth must be a number from 0 to 100.' };
      }
    }
    if (!p.category_id || !String(p.category_id).trim()) missing.push('category');
    if (missing.length) {
      return { ok: false, message: 'Please select ' + missing.join(', ') + ' to get suggestions.' };
    }
    return { ok: true, message: '' };
  }

  window.PCCore = {
    getParams: getParams,
    buildUrl: buildUrl,
    validatePromoDraft: validatePromoDraft
  };
})();
