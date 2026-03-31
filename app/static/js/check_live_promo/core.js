/**
 * Check Live Promo – core: params, API helpers, state.
 * Presets use CLP_ANCHOR_END (min(today, max data date)) from the server.
 */
(function() {
  function anchorEnd() {
    var a = (window.CLP_ANCHOR_END || window.CLP_DATE_END || '').trim();
    return a || '2025-12-31';
  }

  function parseISODate(s) {
    var p = String(s).split('-');
    return new Date(parseInt(p[0], 10), parseInt(p[1], 10) - 1, parseInt(p[2], 10));
  }

  function fmt(d) {
    var y = d.getFullYear();
    var m = String(d.getMonth() + 1).padStart(2, '0');
    var day = String(d.getDate()).padStart(2, '0');
    return y + '-' + m + '-' + day;
  }

  function addDays(d, n) {
    var x = new Date(d.getTime());
    x.setDate(x.getDate() + n);
    return x;
  }

  /** Calendar month immediately before the month containing anchor. */
  function previousCalendarMonthRange(anchorStr) {
    var a = parseISODate(anchorStr);
    var y = a.getFullYear();
    var m = a.getMonth();
    var first = new Date(y, m - 1, 1);
    var last = new Date(y, m, 0);
    return { start: fmt(first), end: fmt(last) };
  }

  function getParams() {
    var preset = document.querySelector('.clp-preset.active');
    var days = preset && preset.getAttribute('data-days');
    var endAnchor = parseISODate(anchorEnd());
    var ds = window.CLP_DATE_START || '';
    var de = window.CLP_DATE_END || '';

    if (days === 'custom') {
      var startEl = document.getElementById('clp-date-start');
      var endEl = document.getElementById('clp-date-end');
      ds = (startEl && startEl.value) || ds;
      de = (endEl && endEl.value) || de;
    } else if (days === '1') {
      de = fmt(endAnchor);
      ds = de;
    } else if (days === '7') {
      de = fmt(endAnchor);
      ds = fmt(addDays(endAnchor, -6));
    } else if (days === 'month') {
      var mo = previousCalendarMonthRange(anchorEnd());
      ds = mo.start;
      de = mo.end;
    } else {
      de = fmt(endAnchor);
      ds = fmt(addDays(endAnchor, -6));
    }

    return {
      date_start: ds,
      date_end: de,
      promo_id: (document.getElementById('clp-promo') && document.getElementById('clp-promo').value) || '',
      category_id: (document.getElementById('clp-category') && document.getElementById('clp-category').value) || '',
      channel: (document.getElementById('clp-channel') && document.getElementById('clp-channel').value) || ''
    };
  }

  function buildActiveUrl() {
    var p = getParams();
    var q = [];
    if (p.date_start) q.push('date_start=' + encodeURIComponent(p.date_start));
    if (p.date_end) q.push('date_end=' + encodeURIComponent(p.date_end));
    if (p.promo_id) q.push('promo_id=' + encodeURIComponent(p.promo_id));
    if (p.category_id) q.push('category_id=' + encodeURIComponent(p.category_id));
    if (p.channel) q.push('channel=' + encodeURIComponent(p.channel));
    return '/api/check-live-promo/active?' + q.join('&');
  }

  function buildSegmentUrl(productId) {
    var p = getParams();
    var q = [];
    if (productId != null && productId !== '') q.push('product_id=' + encodeURIComponent(productId));
    if (p.date_start) q.push('date_start=' + encodeURIComponent(p.date_start));
    if (p.date_end) q.push('date_end=' + encodeURIComponent(p.date_end));
    if (p.promo_id) q.push('promo_id=' + encodeURIComponent(p.promo_id));
    if (p.category_id) q.push('category_id=' + encodeURIComponent(p.category_id));
    if (p.channel) q.push('channel=' + encodeURIComponent(p.channel));
    return '/api/check-live-promo/segment-breakdown?' + q.join('&');
  }

  function buildSkuUrl() {
    var p = getParams();
    var q = [];
    if (p.date_start) q.push('date_start=' + encodeURIComponent(p.date_start));
    if (p.date_end) q.push('date_end=' + encodeURIComponent(p.date_end));
    if (p.promo_id) q.push('promo_id=' + encodeURIComponent(p.promo_id));
    if (p.category_id) q.push('category_id=' + encodeURIComponent(p.category_id));
    if (p.channel) q.push('channel=' + encodeURIComponent(p.channel));
    return '/api/check-live-promo/sku?' + q.join('&');
  }

  window.CLPCore = {
    getParams: getParams,
    buildActiveUrl: buildActiveUrl,
    buildSegmentUrl: buildSegmentUrl,
    buildSkuUrl: buildSkuUrl
  };
})();
