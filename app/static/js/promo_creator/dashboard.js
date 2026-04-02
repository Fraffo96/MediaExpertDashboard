/**
 * Promo Creator – entry: wire Get suggestions button, load suggestions.
 */
(function() {
  var core = window.PCCore;
  if (!core) return;

  var btn = document.getElementById('pc-get-suggestions');
  if (!btn) return;

  function buildCardsHtml(d) {
    var cards = [];
    var n = (d.roi_benchmark && d.roi_benchmark[0]) ? (d.roi_benchmark[0].n_promos || 0) : 0;
    if (d.expected_roi != null && n > 0) {
      var meta = d.discount_depth_used != null
        ? 'Based on ' + n + ' similar promos at ~' + Math.round(d.discount_depth_used) + '% discount'
        : 'Based on ' + n + ' similar promos in category';
      var roiNum = Number(d.expected_roi);
      var negCls = roiNum < 0 ? ' pc-roi-negative' : '';
      cards.push('<div class="pc-card pc-card-expected-roi">' +
        '<div class="pc-card-title">Expected ROI (adjusted)</div>' +
        '<div class="pc-card-value' + negCls + '">' + roiNum.toFixed(2) + 'x</div>' +
        '<div class="pc-card-meta">' + meta + '</div>' +
        '</div>');
    }
    if (d.top_competitor && d.top_competitor.brand_name) {
      var compMeta = d.discount_depth_used != null
        ? 'Performs best at ~' + Math.round(d.discount_depth_used) + '% discount (ROI ' + Number(d.top_competitor.avg_roi).toFixed(2) + 'x)'
        : 'Performs best in this promo type (ROI ' + Number(d.top_competitor.avg_roi).toFixed(2) + 'x)';
      cards.push('<div class="pc-card pc-card-competitor">' +
        '<div class="pc-card-title">Top competitor</div>' +
        '<div class="pc-card-value">' + escapeHtml(d.top_competitor.brand_name) + '</div>' +
        '<div class="pc-card-meta">' + compMeta + '</div>' +
        '</div>');
    }
    if (d.top_segments && d.top_segments.length > 0) {
      var names = d.top_segments.map(function(s) { return escapeHtml(s.segment_name); }).join(', ');
      var pcts = d.top_segments.map(function(s) { return (s.promo_share_pct || 0) + '%'; }).join(', ');
      var gp = core.getParams && core.getParams();
      var ptyp = gp && gp.promo_type ? String(gp.promo_type).trim() : '';
      var segMeta = ptyp
        ? ('Category sales on ' + escapeHtml(ptyp) + ' promos: ' + pcts)
        : ('Promo share (all promo types): ' + pcts);
      cards.push('<div class="pc-card pc-card-segments">' +
        '<div class="pc-card-title">Most reactive segments</div>' +
        '<div class="pc-card-value">' + names + '</div>' +
        '<div class="pc-card-meta">' + segMeta + '</div>' +
        '</div>');
    }
    return cards.join('');
  }

  function escapeHtml(s) {
    if (!s) return '';
    var d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
  }

  function setPcLoading(on) {
    var loadEl = document.getElementById('pc-suggestions-loading');
    var bodyEl = document.getElementById('pc-suggestions-body');
    if (loadEl) loadEl.classList.toggle('hidden', !on);
    if (bodyEl) bodyEl.classList.toggle('pc-suggestions-dimmed', !!on);
    btn.disabled = !!on;
    if (typeof showLoadingLight === 'function') showLoadingLight(!!on);
  }

  btn.onclick = async function() {
    setPcLoading(true);
    try {
      var r = await fetch(core.buildUrl(), { credentials: 'include' });
      var d = await r.json();
      var cardsEl = document.getElementById('pc-suggestions-cards');
      var listEl = document.getElementById('pc-suggestions-list');
      if (!cardsEl || !listEl) return;

      if (d.error) {
        cardsEl.innerHTML = '';
        listEl.innerHTML = '<p class="pc-suggestions-error">' + escapeHtml(d.error) + '</p>';
        return;
      }

      var cardsHtml = buildCardsHtml(d);
      cardsEl.innerHTML = cardsHtml;

      var suggestions = d.suggestions || [];
      if (suggestions.length === 0 && !cardsHtml) {
        listEl.innerHTML = '<p class="pc-suggestions-empty">No specific suggestions for this configuration. Check category benchmarks in Market Intelligence.</p>';
      } else if (suggestions.length === 0) {
        listEl.innerHTML = '';
      } else {
        listEl.innerHTML = suggestions.map(function(s) {
          var cls = s.type === 'warning' ? 'pc-suggestion-warning' : s.type === 'benchmark' ? 'pc-suggestion-benchmark' : 'pc-suggestion-info';
          return '<div class="pc-suggestion ' + cls + '">' + escapeHtml(s.text) + '</div>';
        }).join('');
      }
    } catch (e) {
      var cardsEl = document.getElementById('pc-suggestions-cards');
      var listEl = document.getElementById('pc-suggestions-list');
      if (cardsEl) cardsEl.innerHTML = '';
      if (listEl) listEl.innerHTML = '<p class="pc-suggestions-error">Failed: ' + escapeHtml(e.message) + '</p>';
    } finally {
      setPcLoading(false);
    }
  };
})();
