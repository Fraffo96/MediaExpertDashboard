/**
 * Marketing – Purchasing Process: channel, source, peak events, pre-purchase searches.
 */
(function() {
  var charts = {};

  function getParams() {
    var p = {};
    var form = document.getElementById('filter-form');
    if (form) {
      var fd = new FormData(form);
      for (var pair of fd.entries()) p[pair[0]] = pair[1];
    }
    Object.keys(p).forEach(function(k) { if (p[k] === '' || p[k] === null || p[k] === undefined) delete p[k]; });
    return new URLSearchParams(p).toString();
  }

  function apiUrl() {
    return '/api/marketing/purchasing?' + getParams();
  }

  function fmt(n) { return n == null ? '--' : Number(n).toLocaleString('en-US', { maximumFractionDigits: 0 }); }

  function mkChart(id, type, opts) {
    var canvas = document.getElementById(id);
    if (!canvas || typeof Chart === 'undefined') return null;
    var opt = type === 'doughnut' ? (typeof DOUGHNUT_OPT !== 'undefined' ? DOUGHNUT_OPT : { responsive: true, maintainAspectRatio: false })
      : (typeof HBAR_OPT !== 'undefined' ? HBAR_OPT : { responsive: true, maintainAspectRatio: false, indexAxis: 'y' });
    if (opts) opt = Object.assign({}, opt, opts);
    return new Chart(canvas.getContext('2d'), {
      type: type === 'doughnut' ? 'doughnut' : 'bar',
      data: { labels: [], datasets: [] },
      options: opt
    });
  }

  function getColors(n) {
    var c = (typeof COLORS !== 'undefined' && COLORS.cat10) ? COLORS.cat10 : ['#FFD700','#FFE44D','#B89900','#00d4ff','#ff6b9d','#64748b','#E0E0E0','#888'];
    return Array.from({ length: n }, function(_, i) { return c[i % c.length]; });
  }

  function renderChannelMix(rows) {
    var byChannel = {};
    (rows || []).forEach(function(r) {
      var ch = (r.channel || 'unknown').toLowerCase();
      byChannel[ch] = (byChannel[ch] || 0) + Number(r.gross_pln || 0);
    });
    var labels = Object.keys(byChannel);
    var values = labels.map(function(k) { return byChannel[k]; });
    if (!labels.length) return;

    if (!charts.channel) {
      var channelTooltip = typeof CHART_TOOLTIP !== 'undefined' ? Object.assign({}, CHART_TOOLTIP) : {};
      channelTooltip.callbacks = {
        label: function(ctx) {
          var total = (ctx.dataset.data || []).reduce(function(a, b) { return a + (Number(b) || 0); }, 0);
          var pct = total > 0 ? (100 * (Number(ctx.raw) || 0) / total).toFixed(1) : '0';
          return (ctx.label || '') + ': ' + fmt(ctx.raw) + ' PLN (' + pct + '%)';
        }
      };
      charts.channel = mkChart('mkt-channel-chart', 'doughnut', {
        plugins: { tooltip: channelTooltip }
      });
    }
    if (!charts.channel) return;

    charts.channel.data.labels = labels.map(function(l) { return l.charAt(0).toUpperCase() + l.slice(1); });
    charts.channel.data.datasets = [{
      data: values,
      backgroundColor: getColors(labels.length),
      borderColor: 'rgba(0,0,0,0.2)',
      borderWidth: 1
    }];
    charts.channel.update();
  }

  function renderSourceMix(rows) {
    if (!rows || !rows.length) return;
    var labels = rows.map(function(r) { return r.source || ''; });
    var values = rows.map(function(r) { return Number(r.pct || 0); });

    if (!charts.source) {
      var sourceTooltip = typeof CHART_TOOLTIP !== 'undefined' ? Object.assign({}, CHART_TOOLTIP) : {};
      sourceTooltip.callbacks = {
        label: function(ctx) {
          return (ctx.label || '') + ': ' + (Number(ctx.raw) || 0).toFixed(1) + '%';
        }
      };
      charts.source = mkChart('mkt-source-chart', 'doughnut', {
        plugins: { tooltip: sourceTooltip }
      });
    }
    if (!charts.source) return;

    charts.source.data.labels = labels.map(function(l) { return l.charAt(0).toUpperCase() + l.slice(1); });
    charts.source.data.datasets = [{
      data: values,
      backgroundColor: getColors(labels.length),
      borderColor: 'rgba(0,0,0,0.2)',
      borderWidth: 1,
      _isPct: true
    }];
    charts.source.update();
  }

  function renderPeakChart(rows) {
    if (!rows || !rows.length) return;
    var byEvent = {};
    rows.forEach(function(r) {
      var ev = r.peak_event || 'Regular';
      if (!byEvent[ev]) byEvent[ev] = { sum: 0, n: 0 };
      byEvent[ev].sum += Number(r.orders_pct || 0);
      byEvent[ev].n += 1;
    });
    var labels = Object.keys(byEvent);
    var values = labels.map(function(k) { return Math.round(byEvent[k].sum / byEvent[k].n); });

    if (!charts.peak) charts.peak = mkChart('mkt-peak-chart', 'bar');
    if (!charts.peak) return;

    charts.peak.data.labels = labels;
    charts.peak.data.datasets = [{
      label: '% of orders',
      data: values,
      backgroundColor: getColors(labels.length),
      borderColor: 'rgba(0,0,0,0.2)',
      borderWidth: 1,
      _isPct: true
    }];
    charts.peak.update();
  }

  function renderSearchesChart(rows) {
    if (!rows || !rows.length) return;
    var labels = rows.map(function(r) { return r.search_type || ''; });
    var values = rows.map(function(r) { return Number(r.pct || 0); });

    if (!charts.searches) charts.searches = mkChart('mkt-searches-chart', 'bar');
    if (!charts.searches) return;

    charts.searches.data.labels = labels;
    charts.searches.data.datasets = [{
      label: '% of searches',
      data: values,
      backgroundColor: getColors(labels.length),
      borderColor: 'rgba(0,0,0,0.2)',
      borderWidth: 1,
      _isPct: true
    }];
    charts.searches.update();
  }

  async function loadData() {
    var loading = document.getElementById('mkt-purchasing-loading');
    var content = document.getElementById('mkt-purchasing-content');
    var noData = document.getElementById('mkt-purchasing-no-data');
    if (loading) loading.classList.remove('hidden');
    if (content) content.style.display = 'none';
    if (noData) noData.style.display = 'none';

    try {
      var url = apiUrl();
      console.log('Purchasing API URL:', url);
      var r = await fetch(url, { credentials: 'include' });
      var data = await r.json();
      if (r.ok) {
        var channelMix = data.channel_mix || [];
        var peakEvents = data.peak_events || [];
        var sourceMix = data.source_mix || [];
        var searches = data.pre_purchase_searches || [];
        var hasAny = channelMix.length || peakEvents.length || sourceMix.length || searches.length;

        if (!hasAny) {
          noData.style.display = 'block';
          var segLabel = document.getElementById('mkt-purchasing-segment-label');
          if (segLabel) segLabel.style.display = 'none';
        } else {
          renderChannelMix(channelMix);
          renderSourceMix(sourceMix);
          renderPeakChart(peakEvents);
          renderSearchesChart(searches);
          content.style.display = '';
          var segLabel = document.getElementById('mkt-purchasing-segment-label');
          if (segLabel) {
            var segId = data.segment_id;
            var segs = typeof MKT_SEGMENTS !== 'undefined' ? MKT_SEGMENTS : [];
            var segName = segId ? segs.find(function(s) { return String(s.segment_id) === String(segId); }) : null;
            segLabel.textContent = segId ? 'Data for: ' + (segName ? segName.segment_name : 'Segment ' + segId) : 'Data for: all segments';
            segLabel.style.display = '';
          }
        }
        loading.classList.add('hidden');
      } else {
        if (typeof showError === 'function') showError(data.error || 'Failed to load');
        loading.classList.add('hidden');
      }
    } catch (e) {
      if (typeof showError === 'function') showError('Network error');
      loading.classList.add('hidden');
    }
  }

  window.loadData = loadData;

  document.addEventListener('DOMContentLoaded', function() {
    loadData();
    var segSelect = document.getElementById('f-segment-id');
    if (segSelect) segSelect.addEventListener('change', loadData);
    var catSelect = document.getElementById('f-category-id');
    if (catSelect) catSelect.addEventListener('change', loadData);
  });
})();
