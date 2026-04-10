/**
 * Data Creator: profilo seed v2 da form (unica sorgente) + anteprima compile + full seed.
 */
(function () {
  var SEG = [
    [1, 'Liberals'],
    [2, 'Optimistic Doers'],
    [3, 'Go-Getters'],
    [4, 'Outcasts'],
    [5, 'Contributors'],
    [6, 'Floaters'],
  ];

  var DEFAULT_BEH = {
    1: { ps: 35, churn: 12, ly: 65, prem: 50, cw: 1, ca: 1, cs: 0, inc: 'high' },
    2: { ps: 42, churn: 10, ly: 55, prem: 70, cw: 1, ca: 1, cs: 0, inc: 'high' },
    3: { ps: 28, churn: 5, ly: 70, prem: 75, cw: 1, ca: 1, cs: 1, inc: 'high' },
    4: { ps: 58, churn: 28, ly: 25, prem: 20, cw: 1, ca: 1, cs: 0, inc: 'low' },
    5: { ps: 48, churn: 8, ly: 80, prem: 35, cw: 1, ca: 0, cs: 1, inc: 'low' },
    6: { ps: 52, churn: 15, ly: 45, prem: 25, cw: 0, ca: 0, cs: 1, inc: 'low' },
  };

  var DEFAULT_DISC = { 1: -15, 2: -10, 3: -5, 4: 40, 5: 30, 6: 35 };

  function toast(msg, type) {
    var el = document.getElementById('admin-toast');
    if (!el) return;
    el.textContent = msg;
    el.className = 'admin-toast ' + (type || 'success');
    el.setAttribute('aria-hidden', 'false');
    clearTimeout(el._t);
    el._t = setTimeout(function () {
      el.className = 'admin-toast';
      el.setAttribute('aria-hidden', 'true');
    }, 4000);
  }

  function el(id) {
    return document.getElementById(id);
  }

  function buildSegmentSharesHtml() {
    var h = '<div class="dc-seg-share-grid">';
    SEG.forEach(function (row) {
      var id = row[0];
      var name = row[1];
      h +=
        '<div class="dc-slider-row">' +
        '<label for="dc-share-' +
        id +
        '">' +
        name +
        '</label>' +
        '<input type="range" id="dc-share-' +
        id +
        '" min="0" max="100" value="17">' +
        '<span class="dc-val" id="dc-share-' +
        id +
        '-val">17</span>' +
        '</div>';
    });
    h += '</div>';
    return h;
  }

  function buildBehaviorHtml() {
    var h = '';
    SEG.forEach(function (row) {
      var id = row[0];
      var name = row[1];
      var d = DEFAULT_BEH[id];
      h += '<div class="dc-seg-block"><h5 class="dc-seg-title">' + name + '</h5>';
      h +=
        '<div class="dc-slider-row"><label>Promo sens %</label><input type="range" id="dc-beh-' +
        id +
        '-ps" min="0" max="100" value="' +
        d.ps +
        '"><span class="dc-val" id="dc-beh-' +
        id +
        '-ps-v">' +
        d.ps +
        '</span></div>';
      h +=
        '<div class="dc-slider-row"><label>Churn %</label><input type="range" id="dc-beh-' +
        id +
        '-ch" min="0" max="100" value="' +
        d.churn +
        '"><span class="dc-val" id="dc-beh-' +
        id +
        '-ch-v">' +
        d.churn +
        '</span></div>';
      h +=
        '<div class="dc-slider-row"><label>Loyalty %</label><input type="range" id="dc-beh-' +
        id +
        '-ly" min="0" max="100" value="' +
        d.ly +
        '"><span class="dc-val" id="dc-beh-' +
        id +
        '-ly-v">' +
        d.ly +
        '</span></div>';
      h +=
        '<div class="dc-slider-row"><label>Premium mix %</label><input type="range" id="dc-beh-' +
        id +
        '-pm" min="0" max="100" value="' +
        d.prem +
        '"><span class="dc-val" id="dc-beh-' +
        id +
        '-pm-v">' +
        d.prem +
        '</span></div>';
      h +=
        '<div class="dc-ch-row"><label>Canali</label>' +
        '<label class="dc-inline"><input type="checkbox" id="dc-beh-' +
        id +
        '-cw" ' +
        (d.cw ? 'checked' : '') +
        '> Web</label>' +
        '<label class="dc-inline"><input type="checkbox" id="dc-beh-' +
        id +
        '-ca" ' +
        (d.ca ? 'checked' : '') +
        '> App</label>' +
        '<label class="dc-inline"><input type="checkbox" id="dc-beh-' +
        id +
        '-cs" ' +
        (d.cs ? 'checked' : '') +
        '> Store</label></div>';
      h +=
        '<div class="admin-form-row"><label>Reddito</label><select id="dc-beh-' +
        id +
        '-inc"><option value="high"' +
        (d.inc === 'high' ? ' selected' : '') +
        '>high</option><option value="low"' +
        (d.inc === 'low' ? ' selected' : '') +
        '>low</option></select></div>';
      h += '</div>';
    });
    return h;
  }

  function buildDiscountBiasHtml() {
    var h = '';
    SEG.forEach(function (row) {
      var id = row[0];
      var name = row[1];
      var v = DEFAULT_DISC[id];
      h +=
        '<div class="dc-slider-row"><label>' +
        name +
        ' bias</label><input type="range" id="dc-db-' +
        id +
        '" min="-50" max="50" value="' +
        v +
        '"><span class="dc-val" id="dc-db-' +
        id +
        '-v">' +
        (v / 10).toFixed(1) +
        '</span></div>';
    });
    return h;
  }

  function buildCategoryMatrixHtml() {
    var cats = window.ADMIN_CATEGORIES || [];
    var h = '<div class="dc-cat-matrix">';
    SEG.forEach(function (row) {
      var sid = row[0];
      var sname = row[1];
      h += '<div class="dc-cat-col"><strong>' + sname + '</strong>';
      cats.forEach(function (c) {
        var cid = c.category_id;
        h +=
          '<label class="dc-cat-label"><input type="checkbox" class="dc-cat-cb" data-seg="' +
          sid +
          '" data-cat="' +
          cid +
          '"> ' +
          (c.category_name || cid) +
          '</label>';
      });
      h += '</div>';
    });
    h += '</div>';
    return h;
  }

  function buildBrandFocusHtml() {
    var brands = window.ADMIN_BRANDS || [];
    var cats = window.ADMIN_CATEGORIES || [];
    var h = '';
    brands.forEach(function (b) {
      var bid = b.brand_id;
      var bn = b.brand_name || 'Brand ' + bid;
      h += '<details class="dc-brand-det"><summary>' + bn + ' (id ' + bid + ')</summary><div class="dc-bf-cats">';
      cats.forEach(function (c) {
        h +=
          '<label class="dc-inline"><input type="checkbox" class="dc-bf-cb" data-brand="' +
          bid +
          '" data-cat="' +
          c.category_id +
          '"> ' +
          (c.category_name || c.category_id) +
          '</label>';
      });
      h += '</div></details>';
    });
    return h;
  }

  function buildBrandPromoAffHtml() {
    var brands = window.ADMIN_BRANDS || [];
    var h =
      '<div class="dc-aff-grid"><span class="admin-hint">Brand</span><span class="admin-hint">Moltiplicatore premium</span>';
    brands.forEach(function (b) {
      var bid = b.brand_id;
      h +=
        '<label for="dc-aff-' +
        bid +
        '">' +
        (b.brand_name || bid) +
        '</label><input type="number" id="dc-aff-' +
        bid +
        '" class="dc-aff-inp" data-brand="' +
        bid +
        '" value="" min="0.1" max="5" step="0.05" placeholder="1">';
    });
    h += '</div>';
    return h;
  }

  function wireSliderPair(rangeId, valId, scale) {
    var r = el(rangeId);
    var v = el(valId);
    if (!r || !v) return;
    function sync() {
      var x = parseInt(r.value, 10);
      var f = scale != null ? x * scale : x;
      v.textContent = scale != null ? f.toFixed(2) : String(x);
    }
    r.addEventListener('input', sync);
    sync();
  }

  function wireAllRangeLabels() {
    SEG.forEach(function (row) {
      var id = row[0];
      wireSliderPair('dc-share-' + id, 'dc-share-' + id + '-val', null);
      wireSliderPair('dc-beh-' + id + '-ps', 'dc-beh-' + id + '-ps-v', null);
      wireSliderPair('dc-beh-' + id + '-ch', 'dc-beh-' + id + '-ch-v', null);
      wireSliderPair('dc-beh-' + id + '-ly', 'dc-beh-' + id + '-ly-v', null);
      wireSliderPair('dc-beh-' + id + '-pm', 'dc-beh-' + id + '-pm-v', null);
      var db = el('dc-db-' + id);
      var dbv = el('dc-db-' + id + '-v');
      if (db && dbv) {
        db.addEventListener('input', function () {
          dbv.textContent = (parseInt(db.value, 10) / 10).toFixed(1);
        });
      }
    });
    wireSliderPair('dc-promo-slope', 'dc-promo-slope-val', 0.01);
    wireSliderPair('dc-promo-intercept', 'dc-promo-intercept-val', 0.01);
  }

  function buildProfile() {
    var g = {
      num_orders: parseInt(el('dc-num-orders').value, 10) || 380000,
      num_customers: parseInt(el('dc-num-customers').value, 10) || 24000,
      num_products: parseInt(el('dc-num-products').value, 10) || 1200,
    };
    var ds = el('dc-date-start').value;
    var de = el('dc-date-end').value;
    if (ds && de) g.date_range = { start: ds, end: de };

    var shares = {};
    var sum = 0;
    SEG.forEach(function (row) {
      var id = row[0];
      var w = parseInt(el('dc-share-' + id).value, 10) || 0;
      shares[String(id)] = w;
      sum += w;
    });
    if (sum <= 0) {
      SEG.forEach(function (row) {
        shares[String(row[0])] = 1;
      });
    }

    var seg_beh = {};
    SEG.forEach(function (row) {
      var id = row[0];
      seg_beh[String(id)] = {
        promo_sens: (parseInt(el('dc-beh-' + id + '-ps').value, 10) || 0) / 100,
        churn: (parseInt(el('dc-beh-' + id + '-ch').value, 10) || 0) / 100,
        loyalty_prob: (parseInt(el('dc-beh-' + id + '-ly').value, 10) || 0) / 100,
        prem: (parseInt(el('dc-beh-' + id + '-pm').value, 10) || 0) / 100,
        ch_web: el('dc-beh-' + id + '-cw').checked ? 1 : 0,
        ch_app: el('dc-beh-' + id + '-ca').checked ? 1 : 0,
        ch_store: el('dc-beh-' + id + '-cs').checked ? 1 : 0,
        inc: el('dc-beh-' + id + '-inc').value,
      };
    });

    var disc = {};
    SEG.forEach(function (row) {
      var id = row[0];
      disc[String(id)] = (parseInt(el('dc-db-' + id).value, 10) || 0) / 10;
    });

    var parent_cats = {};
    document.querySelectorAll('.dc-cat-cb:checked').forEach(function (cb) {
      var s = cb.getAttribute('data-seg');
      var c = parseInt(cb.getAttribute('data-cat'), 10);
      if (!parent_cats[s]) parent_cats[s] = [];
      parent_cats[s].push(c);
    });
    var hasCat = Object.keys(parent_cats).length > 0;

    var focus = {};
    document.querySelectorAll('.dc-bf-cb:checked').forEach(function (cb) {
      var b = cb.getAttribute('data-brand');
      var c = parseInt(cb.getAttribute('data-cat'), 10);
      if (!focus[b]) focus[b] = [];
      focus[b].push(c);
    });
    var hasFocus = Object.keys(focus).length > 0;

    var promo_aff = {};
    document.querySelectorAll('.dc-aff-inp').forEach(function (inp) {
      var v = inp.value.trim();
      if (v === '') return;
      var x = parseFloat(v);
      if (!isNaN(x) && x > 0) promo_aff[inp.getAttribute('data-brand')] = x;
    });
    var hasAff = Object.keys(promo_aff).length > 0;

    var guards = {};
    var go = el('dc-guard-orders').value.trim();
    var gc = el('dc-guard-customers').value.trim();
    if (go) guards.max_num_orders = parseInt(go, 10);
    if (gc) guards.max_num_customers = parseInt(gc, 10);

    var profile = {
      profile_version: 2,
      global: g,
      segment_rules: {
        customer_share_by_segment: shares,
      },
      segment_behavior: seg_beh,
      promo_rules: {
        promo_curve: {
          slope: (parseInt(el('dc-promo-slope').value, 10) || 0) * 0.01,
          intercept: (parseInt(el('dc-promo-intercept').value, 10) || 0) * 0.01,
        },
        segment_discount_bias: disc,
      },
    };
    if (hasCat) profile.segment_rules.parent_categories = parent_cats;
    if (hasFocus || hasAff) {
      profile.brand_rules = {};
      if (hasFocus) profile.brand_rules.focus_override = focus;
      if (hasAff) profile.brand_rules.promo_affinity = promo_aff;
    }
    if (Object.keys(guards).length) profile.guards = guards;

    return profile;
  }

  function applyProfile(p) {
    if (!p || p.profile_version !== 2) return;
    var g = p.global || {};
    if (g.num_orders != null) el('dc-num-orders').value = g.num_orders;
    if (g.num_customers != null) el('dc-num-customers').value = g.num_customers;
    if (g.num_products != null) el('dc-num-products').value = g.num_products;
    if (g.date_range && g.date_range.start) el('dc-date-start').value = String(g.date_range.start).slice(0, 10);
    if (g.date_range && g.date_range.end) el('dc-date-end').value = String(g.date_range.end).slice(0, 10);

    var sr = p.segment_rules || {};
    var sh = sr.customer_share_by_segment || {};
    var shareSum = 0;
    Object.keys(sh).forEach(function (k) {
      shareSum += parseFloat(sh[k]) || 0;
    });
    var sharesAsFraction = shareSum > 0 && shareSum <= 1.05;
    SEG.forEach(function (row) {
      var id = row[0];
      var rawv = sh[String(id)] != null ? sh[String(id)] : sh[id];
      if (rawv == null || !el('dc-share-' + id)) return;
      var fv = parseFloat(rawv);
      var v = sharesAsFraction ? Math.round(fv * 100) : Math.round(fv);
      el('dc-share-' + id).value = String(Math.min(100, Math.max(0, v)));
    });

    var sb = p.segment_behavior || {};
    SEG.forEach(function (row) {
      var id = row[0];
      var b = sb[String(id)] || sb[id] || {};
      if (b.promo_sens != null && el('dc-beh-' + id + '-ps'))
        el('dc-beh-' + id + '-ps').value = String(Math.round(parseFloat(b.promo_sens) * 100));
      if (b.churn != null && el('dc-beh-' + id + '-ch'))
        el('dc-beh-' + id + '-ch').value = String(Math.round(parseFloat(b.churn) * 100));
      if (b.loyalty_prob != null && el('dc-beh-' + id + '-ly'))
        el('dc-beh-' + id + '-ly').value = String(Math.round(parseFloat(b.loyalty_prob) * 100));
      if (b.prem != null && el('dc-beh-' + id + '-pm'))
        el('dc-beh-' + id + '-pm').value = String(Math.round(parseFloat(b.prem) * 100));
      if (b.ch_web != null) el('dc-beh-' + id + '-cw').checked = !!parseInt(b.ch_web, 10);
      if (b.ch_app != null) el('dc-beh-' + id + '-ca').checked = !!parseInt(b.ch_app, 10);
      if (b.ch_store != null) el('dc-beh-' + id + '-cs').checked = !!parseInt(b.ch_store, 10);
      if (b.inc && el('dc-beh-' + id + '-inc')) el('dc-beh-' + id + '-inc').value = b.inc;
    });

    var pr = p.promo_rules || {};
    var pc = pr.promo_curve || {};
    if (pc.slope != null) el('dc-promo-slope').value = String(Math.round(parseFloat(pc.slope) * 100));
    if (pc.intercept != null) el('dc-promo-intercept').value = String(Math.round(parseFloat(pc.intercept) * 100));
    var db = pr.segment_discount_bias || {};
    SEG.forEach(function (row) {
      var id = row[0];
      var v = db[String(id)] != null ? db[String(id)] : db[id];
      if (v != null && el('dc-db-' + id)) el('dc-db-' + id).value = String(Math.round(parseFloat(v) * 10));
    });

    document.querySelectorAll('.dc-cat-cb').forEach(function (cb) {
      cb.checked = false;
    });
    var pcat = sr.parent_categories || {};
    Object.keys(pcat).forEach(function (ks) {
      var arr = pcat[ks] || [];
      arr.forEach(function (cid) {
        var c = document.querySelector('.dc-cat-cb[data-seg="' + ks + '"][data-cat="' + cid + '"]');
        if (c) c.checked = true;
      });
    });

    document.querySelectorAll('.dc-bf-cb').forEach(function (cb) {
      cb.checked = false;
    });
    var br = p.brand_rules || {};
    var fo = br.focus_override || {};
    Object.keys(fo).forEach(function (bid) {
      (fo[bid] || []).forEach(function (cid) {
        var c = document.querySelector('.dc-bf-cb[data-brand="' + bid + '"][data-cat="' + cid + '"]');
        if (c) c.checked = true;
      });
    });

    document.querySelectorAll('.dc-aff-inp').forEach(function (inp) {
      inp.value = '';
    });
    var pa = br.promo_affinity || {};
    Object.keys(pa).forEach(function (bid) {
      var inp = el('dc-aff-' + bid);
      if (inp) inp.value = String(pa[bid]);
    });

    el('dc-guard-orders').value = '';
    el('dc-guard-customers').value = '';
    var gu = p.guards || {};
    if (gu.max_num_orders != null) el('dc-guard-orders').value = String(gu.max_num_orders);
    if (gu.max_num_customers != null) el('dc-guard-customers').value = String(gu.max_num_customers);

    wireAllRangeLabels();
  }

  function syncJsonTextarea() {
    var ta = el('dc-profile-json');
    if (ta) ta.value = JSON.stringify(buildProfile(), null, 2);
  }

  function formatCompilePreview(d) {
    if (!d || !d.ok) return JSON.stringify(d, null, 2);
    var lines = [];
    var c = d.compiled || {};
    var prev = c.preview || {};
    var cg = c.global || {};
    if (prev && Object.keys(prev).length) {
      lines.push('=== Anteprima compilazione ===');
      if (prev.segment_shares_expected)
        lines.push('Quote segmento (attese): ' + JSON.stringify(prev.segment_shares_expected));
      if (prev.segment_boundaries) lines.push('Soglie customer_id: ' + JSON.stringify(prev.segment_boundaries));
      if (prev.mean_promo_sensitivity != null)
        lines.push('Sensibilità promo media: ' + prev.mean_promo_sensitivity);
      if (prev.approx_order_promo_threshold != null)
        lines.push('Soglia promo ordini (stima): ' + prev.approx_order_promo_threshold);
      if (prev.parent_category_rows != null)
        lines.push('Righe categorie parent (pool): ' + prev.parent_category_rows);
    }
    if (cg && Object.keys(cg).length) {
      lines.push('--- Volumi / date (compilati) ---');
      lines.push(JSON.stringify(cg, null, 2));
    }
    var env = c.env || {};
    if (env.SEED_BRAND_FOCUS_JSON) lines.push('Env focus: SEED_BRAND_FOCUS_JSON impostato');
    if (env.SEED_BRAND_PROMO_AFFINITY_JSON) lines.push('Env catalogo: SEED_BRAND_PROMO_AFFINITY_JSON impostato');
    if (d.errors && d.errors.length) lines.push('Errori: ' + JSON.stringify(d.errors));
    return lines.length ? lines.join('\n') : JSON.stringify(d, null, 2);
  }

  async function runPreview() {
    var out = el('dc-preview-out');
    out.textContent = 'Caricamento…';
    try {
      var prof = buildProfile();
      syncJsonTextarea();
      var r = await fetch('/api/admin/seed/compile', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ profile: prof, include_sql: false }),
      });
      var d = await r.json();
      if (!r.ok) {
        out.textContent =
          typeof d.detail === 'string' ? d.detail : JSON.stringify(d.detail || d, null, 2);
        toast('Validazione / compile fallita', 'error');
        return;
      }
      out.textContent = formatCompilePreview(d);
      toast('Anteprima aggiornata');
    } catch (e) {
      out.textContent = String(e);
      toast(e.message, 'error');
    }
  }

  var _dcPoll = null;
  function pollJob(jobId) {
    if (_dcPoll) clearInterval(_dcPoll);
    var elSt = el('do-job-status');
    _dcPoll = setInterval(async function () {
      try {
        var r = await fetch('/api/admin/data-jobs/' + encodeURIComponent(jobId));
        var j = await r.json();
        if (!r.ok) {
          clearInterval(_dcPoll);
          _dcPoll = null;
          elSt.textContent = j.detail || 'Errore';
          return;
        }
        elSt.textContent =
          'Stato: ' + j.status + (j.message ? ' — ' + j.message : '') + (j.error_snippet ? ' — ' + j.error_snippet : '');
        if (j.status === 'ok' || j.status === 'error') {
          clearInterval(_dcPoll);
          _dcPoll = null;
          toast(j.status === 'ok' ? 'Job completato' : 'Job fallito', j.status === 'ok' ? 'success' : 'error');
        }
      } catch (e) {
        clearInterval(_dcPoll);
        _dcPoll = null;
        elSt.textContent = String(e);
      }
    }, 2000);
  }

  async function runFullSeed() {
    if (!window.confirm('Full seed riscrive molti dati su BigQuery e ricalcola precalc. Continuare?')) return;
    var prof = buildProfile();
    syncJsonTextarea();
    var st = el('do-job-status');
    st.textContent = 'Avvio…';
    try {
      var r = await fetch('/api/admin/data-jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ job_type: 'full_seed', profile_v2: prof }),
      });
      var d = await r.json();
      if (!r.ok) throw new Error(d.detail || d.error || r.statusText);
      st.textContent = 'Job ' + d.id + ' — runner: ' + (d.runner || '?');
      pollJob(d.id);
      toast('Job in coda');
    } catch (e) {
      st.textContent = e.message;
      toast(e.message, 'error');
    }
  }

  function resetDefaults() {
    el('dc-num-orders').value = '380000';
    el('dc-num-customers').value = '24000';
    el('dc-num-products').value = '1200';
    el('dc-date-start').value = '2023-01-01';
    el('dc-date-end').value = '2024-12-31';
    el('dc-promo-slope').value = '58';
    el('dc-promo-intercept').value = '17';
    el('dc-guard-orders').value = '';
    el('dc-guard-customers').value = '';
    el('dc-seg-shares').innerHTML = buildSegmentSharesHtml();
    el('dc-seg-behavior').innerHTML = buildBehaviorHtml();
    el('dc-discount-bias').innerHTML = buildDiscountBiasHtml();
    el('dc-seg-categories').innerHTML = buildCategoryMatrixHtml();
    el('dc-brand-focus').innerHTML = buildBrandFocusHtml();
    el('dc-brand-promo-aff').innerHTML = buildBrandPromoAffHtml();
    wireAllRangeLabels();
    syncJsonTextarea();
    el('dc-preview-out').textContent = 'Clicca «Aggiorna anteprima».';
    toast('Default ripristinati');
  }

  function init() {
    var root = document.querySelector('.dc-root');
    if (!root) return;

    el('dc-seg-shares').innerHTML = buildSegmentSharesHtml();
    el('dc-seg-behavior').innerHTML = buildBehaviorHtml();
    el('dc-discount-bias').innerHTML = buildDiscountBiasHtml();
    el('dc-seg-categories').innerHTML = buildCategoryMatrixHtml();
    el('dc-brand-focus').innerHTML = buildBrandFocusHtml();
    el('dc-brand-promo-aff').innerHTML = buildBrandPromoAffHtml();
    wireAllRangeLabels();
    syncJsonTextarea();

    el('dc-preset').addEventListener('click', resetDefaults);
    el('dc-preview').addEventListener('click', runPreview);
    el('dc-full-seed').addEventListener('click', runFullSeed);

    var ta = el('dc-profile-json');
    if (ta) {
      ta.addEventListener('blur', async function () {
        var raw = ta.value.trim();
        if (!raw) return;
        try {
          var p = JSON.parse(raw);
          var r = await fetch('/api/admin/seed/validate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(p),
          });
          var d = await r.json();
          if (!d.ok) {
            toast('JSON non valido: ' + (d.errors && d.errors[0] ? d.errors[0].message : 'error'), 'error');
            return;
          }
          applyProfile(d.normalized || p);
          syncJsonTextarea();
          toast('Form aggiornato da JSON');
        } catch (e) {
          toast('JSON non parsabile', 'error');
        }
      });
    }

    root.addEventListener('change', function () {
      syncJsonTextarea();
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
