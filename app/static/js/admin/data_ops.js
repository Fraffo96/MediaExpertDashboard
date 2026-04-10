/**
 * Admin tab Data ops: BigQuery stats, schema, cache, job pipeline, seed profiles.
 */
(function () {
  function toast(msg, type) {
    var el = document.getElementById('admin-toast');
    if (!el) {
      console.log(msg);
      return;
    }
    el.textContent = msg;
    el.className = 'admin-toast ' + (type || 'success');
    el.setAttribute('aria-hidden', 'false');
    clearTimeout(el._t);
    el._t = setTimeout(function () {
      el.className = 'admin-toast';
      el.setAttribute('aria-hidden', 'true');
    }, 4000);
  }

  var pollTimer = null;

  function stopPoll() {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }

  function pollJob(jobId) {
    stopPoll();
    var el = document.getElementById('do-job-status');
    pollTimer = setInterval(async function () {
      try {
        var r = await fetch('/api/admin/data-jobs/' + encodeURIComponent(jobId));
        var j = await r.json();
        if (!r.ok) {
          stopPoll();
          el.textContent = j.detail || 'Errore job';
          return;
        }
        var vr = j.verify_report;
        var vrTxt = '';
        if (vr && typeof vr === 'object') {
          try {
            vrTxt = ' | verify: ' + JSON.stringify(vr).slice(0, 400);
          } catch (e) {}
        }
        el.textContent =
          'Stato: ' +
          j.status +
          (j.message ? ' — ' + j.message : '') +
          (j.error_snippet ? ' — ' + j.error_snippet : '') +
          vrTxt;
        if (j.status === 'ok' || j.status === 'error') {
          stopPoll();
          toast(j.status === 'ok' ? 'Job completato' : 'Job fallito', j.status === 'ok' ? 'success' : 'error');
        }
      } catch (e) {
        stopPoll();
        el.textContent = String(e);
      }
    }, 2000);
  }

  async function dropBqTable(tableId) {
    if (!tableId) return;
    if (
      !window.confirm(
        'Eliminare definitivamente la tabella «' + tableId + '» nel dataset BigQuery mart? Operazione irreversibile.'
      )
    ) {
      return;
    }
    var again = window.prompt('Digita di nuovo il nome esatto della tabella per confermare:');
    if (again !== tableId) {
      toast('Annullato', 'error');
      return;
    }
    var safe = tableId.indexOf('precalc_') === 0 || tableId.indexOf('mv_') === 0;
    var force = !safe;
    if (
      force &&
      !window.confirm(
        'Questa tabella non è precalc_/mv_. Sul server deve essere impostato ENABLE_ADMIN_BQ_DROP_ANY=1. Continuare? (fallisce se non abilitato)'
      )
    ) {
      return;
    }
    try {
      var r = await fetch('/api/admin/bq/drop-table', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ table_id: tableId, confirm: tableId, force: force }),
      });
      var d = await r.json();
      if (!r.ok) {
        var msg = d.detail != null ? (typeof d.detail === 'string' ? d.detail : JSON.stringify(d.detail)) : r.statusText;
        throw new Error(msg);
      }
      toast('Tabella eliminata: ' + tableId);
      loadSummaryAndQuality(true);
      loadTables({ quiet: true });
    } catch (e) {
      toast(e.message, 'error');
    }
  }

  async function loadTables(opts) {
    opts = opts || {};
    var quiet = !!opts.quiet;
    var tbody = document.getElementById('do-bq-tables');
    tbody.innerHTML = '<tr><td colspan="5">Caricamento…</td></tr>';
    try {
      var r = await fetch('/api/admin/bq/tables');
      var d = await r.json();
      if (!r.ok) throw new Error(d.detail || r.statusText);
      tbody.innerHTML = '';
      (d.tables || []).forEach(function (t) {
        var tid = String(t.table_id || '');
        var tr = document.createElement('tr');
        var td0 = document.createElement('td');
        var code = document.createElement('code');
        code.textContent = tid;
        td0.appendChild(code);
        tr.appendChild(td0);
        var td1 = document.createElement('td');
        td1.textContent = t.row_count != null ? String(t.row_count) : '';
        tr.appendChild(td1);
        var td2 = document.createElement('td');
        td2.textContent = t.size_human != null ? String(t.size_human) : '';
        tr.appendChild(td2);
        var td3 = document.createElement('td');
        td3.textContent = t.last_modified_time != null ? String(t.last_modified_time) : '';
        tr.appendChild(td3);
        var td4 = document.createElement('td');
        var btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'admin-btn small danger';
        btn.textContent = 'Elimina';
        btn.setAttribute('data-bq-drop', tid);
        td4.appendChild(btn);
        tr.appendChild(td4);
        tbody.appendChild(tr);
      });
      if (!tbody.children.length) tbody.innerHTML = '<tr><td colspan="5">Nessuna tabella.</td></tr>';
      if (!quiet) toast('Tabelle aggiornate');
      loadSummaryAndQuality(true);
    } catch (e) {
      tbody.innerHTML = '<tr><td colspan="5">' + e.message + '</td></tr>';
      toast(e.message, 'error');
    }
  }

  function fmtNum(x) {
    if (x == null) return '';
    try {
      return Number(x).toLocaleString();
    } catch (e) {
      return String(x);
    }
  }

  function fmtBytes(n) {
    if (n == null || n === '') return '';
    var x = Number(n);
    if (isNaN(x) || x < 0) return '';
    if (x < 1024) return x.toFixed(0) + ' B';
    x /= 1024;
    if (x < 1024) return x.toFixed(1) + ' KiB';
    x /= 1024;
    if (x < 1024) return x.toFixed(1) + ' MiB';
    x /= 1024;
    return x.toFixed(2) + ' GiB';
  }

  async function loadSummaryAndQuality(silent) {
    var box = document.getElementById('do-bq-summary');
    var famBody = document.getElementById('do-bq-summary-families');
    var topBody = document.getElementById('do-bq-summary-top');
    var qBox = document.getElementById('do-bq-quality');
    if (!box || !famBody || !topBody || !qBox) return;
    box.textContent = 'Caricamento…';
    famBody.innerHTML = '<tr><td colspan="5">Caricamento…</td></tr>';
    topBody.innerHTML = '<tr><td colspan="4">Caricamento…</td></tr>';
    qBox.textContent = 'Caricamento…';
    try {
      var r = await fetch('/api/admin/bq/summary');
      var d = await r.json();
      if (!r.ok) throw new Error(d.detail || r.statusText);
      var totals = d.totals || {};
      var sz = fmtBytes(totals.size_bytes);
      box.innerHTML =
        '<strong>' +
        (d.project_id || '') +
        '</strong> · dataset <code>' +
        (d.dataset || 'mart') +
        '</code><br>' +
        'Totale: <strong>' +
        fmtNum(totals.table_count) +
        '</strong> tabelle, <strong>' +
        fmtNum(totals.row_count) +
        '</strong> righe, storage stimato <strong>' +
        (sz || '—') +
        '</strong> · ultima modifica (max): ' +
        (totals.last_modified_time || '—');

      famBody.innerHTML = '';
      (d.families || []).forEach(function (f) {
        var tr = document.createElement('tr');
        tr.innerHTML =
          '<td><code>' +
          (f.family || '') +
          '</code></td><td>' +
          fmtNum(f.table_count) +
          '</td><td>' +
          fmtNum(f.row_count) +
          '</td><td>' +
          fmtBytes(f.size_bytes) +
          '</td><td>' +
          (f.last_modified_time || '') +
          '</td>';
        famBody.appendChild(tr);
      });
      if (!famBody.children.length) famBody.innerHTML = '<tr><td colspan="5">Nessun dato.</td></tr>';

      topBody.innerHTML = '';
      (d.top_tables || []).forEach(function (t) {
        var tid = String(t.table_id || '');
        var tr = document.createElement('tr');
        var td0 = document.createElement('td');
        var code = document.createElement('code');
        code.textContent = tid;
        td0.appendChild(code);
        tr.appendChild(td0);
        var td1 = document.createElement('td');
        td1.textContent = fmtNum(t.row_count);
        tr.appendChild(td1);
        var td2 = document.createElement('td');
        td2.textContent = fmtBytes(t.size_bytes);
        tr.appendChild(td2);
        var td3 = document.createElement('td');
        td3.textContent = t.last_modified_time != null ? String(t.last_modified_time) : '';
        tr.appendChild(td3);
        var td4 = document.createElement('td');
        var btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'admin-btn small danger';
        btn.textContent = 'Elimina';
        btn.setAttribute('data-bq-drop', tid);
        td4.appendChild(btn);
        tr.appendChild(td4);
        topBody.appendChild(tr);
      });
      if (!topBody.children.length) topBody.innerHTML = '<tr><td colspan="5">Nessun dato.</td></tr>';
      if (!silent) toast('Riepilogo dataset aggiornato');
    } catch (e) {
      box.textContent = '';
      box.innerHTML = '<span class="admin-hint" style="color:#f66;">' + e.message + '</span>';
      famBody.innerHTML = '<tr><td colspan="5">' + e.message + '</td></tr>';
      topBody.innerHTML = '<tr><td colspan="5">' + e.message + '</td></tr>';
    }
    try {
      var rq = await fetch('/api/admin/bq/quality');
      var dq = await rq.json();
      if (!rq.ok) throw new Error(dq.detail || rq.statusText);
      var q = (dq || {}).quality || {};
      qBox.textContent =
        'customers=' +
        fmtNum(q.customers) +
        ', segments=' +
        fmtNum(q.segments_distinct) +
        ', orders=' +
        fmtNum(q.orders) +
        ', promo_rate=' +
        (q.orders_promo_rate != null ? (Number(q.orders_promo_rate) * 100).toFixed(1) + '%' : '') +
        ', items=' +
        fmtNum(q.order_items) +
        ', distinct_products_in_items=' +
        fmtNum(q.order_items_distinct_products);
    } catch (e) {
      qBox.textContent = e.message;
    }
  }

  async function loadSchema() {
    var box = document.getElementById('do-bq-schema');
    box.textContent = 'Caricamento…';
    try {
      var r = await fetch('/api/admin/bq/schema');
      var d = await r.json();
      if (!r.ok) throw new Error(d.detail || r.statusText);
      var by = d.schema_by_table || {};
      var names = Object.keys(by).sort();
      var frag = document.createDocumentFragment();
      names.forEach(function (tn) {
        var det = document.createElement('details');
        det.style.marginBottom = '0.35rem';
        var sm = document.createElement('summary');
        sm.textContent = tn + ' (' + (by[tn] || []).length + ' colonne)';
        det.appendChild(sm);
        var pre = document.createElement('pre');
        pre.style.margin = '0.25rem 0 0 0.5rem';
        pre.style.fontSize = '0.8rem';
        pre.style.whiteSpace = 'pre-wrap';
        pre.textContent = (by[tn] || [])
          .map(function (c) {
            return c.column_name + '  ' + c.data_type + (c.is_nullable === 'YES' ? ' NULL' : '');
          })
          .join('\n');
        det.appendChild(pre);
        frag.appendChild(det);
      });
      box.innerHTML = '';
      box.appendChild(frag);
      if (!names.length) box.textContent = 'Nessuna colonna.';
      toast('Schema caricato');
    } catch (e) {
      box.textContent = e.message;
      toast(e.message, 'error');
    }
  }

  async function loadLinks() {
    var p = document.getElementById('do-console-links');
    try {
      var r = await fetch('/api/admin/data-ops-links');
      var d = await r.json();
      if (!r.ok) throw new Error(d.detail || r.statusText);
      var L = d.links || {};
      p.innerHTML =
        '<a href="' +
        L.bigquery_dataset +
        '" target="_blank" rel="noopener">BigQuery dataset</a> · ' +
        '<a href="' +
        L.logging_job +
        '" target="_blank" rel="noopener">Logging</a> · ' +
        '<a href="' +
        L.run_jobs +
        '" target="_blank" rel="noopener">Cloud Run Jobs</a>';
    } catch (e) {
      p.textContent = e.message;
    }
  }

  async function clearCache() {
    try {
      var r = await fetch('/api/admin/clear-cache', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
      var d = await r.json();
      if (!r.ok) throw new Error(d.detail || r.statusText);
      toast('Cache svuotata');
    } catch (e) {
      toast(e.message, 'error');
    }
  }

  async function prewarm() {
    try {
      var r = await fetch('/api/admin/prewarm', { method: 'POST' });
      var d = await r.json();
      if (!r.ok) throw new Error(d.detail || r.statusText);
      toast('Prewarm avviato');
    } catch (e) {
      toast(e.message, 'error');
    }
  }

  async function refreshProfiles() {
    var sel = document.getElementById('do-seed-profile');
    var list = document.getElementById('sp-list');
    try {
      var r = await fetch('/api/admin/seed-profiles');
      var d = await r.json();
      if (!r.ok) throw new Error(d.detail || r.statusText);
      var cur = sel.value;
      sel.innerHTML = '<option value="">— default env —</option>';
      (d.profiles || []).forEach(function (p) {
        var o = document.createElement('option');
        o.value = p.id;
        o.textContent = (p.name || p.id) + ' (' + p.id + ')';
        sel.appendChild(o);
      });
      if (cur) sel.value = cur;
      list.innerHTML = '';
      (d.profiles || []).forEach(function (p) {
        var li = document.createElement('li');
        li.innerHTML =
          '<code>' +
          p.id +
          '</code> — ordini ' +
          p.num_orders +
          ', clienti ' +
          p.num_customers +
          ', SKU ' +
          p.product_count;
        list.appendChild(li);
      });
    } catch (e) {
      list.textContent = e.message;
    }
  }

  function defaultProfileV2() {
    return {
      profile_version: 2,
      global: { num_orders: 380000, num_customers: 24000, num_products: 1200 },
      segment_rules: {
        customer_share_by_segment: { '1': 1, '2': 1, '3': 1, '4': 1, '5': 1, '6': 1 },
      },
    };
  }

  function readDcProfile() {
    var ta = document.getElementById('dc-profile-json');
    var raw = (ta && ta.value) || '';
    return JSON.parse(raw);
  }

  function applySharesToJson() {
    var ta = document.getElementById('dc-profile-json');
    var o;
    try {
      o = readDcProfile();
    } catch (e) {
      toast('JSON non valido', 'error');
      return;
    }
    o.segment_rules = o.segment_rules || {};
    var shares = {};
    var i;
    var sum = 0;
    for (i = 1; i <= 6; i++) {
      var el = document.getElementById('dc-share-' + i);
      var w = el ? parseInt(el.value, 10) || 0 : 0;
      shares[String(i)] = w;
      sum += w;
    }
    if (sum <= 0) {
      toast('Somma quote = 0', 'error');
      return;
    }
    o.segment_rules.customer_share_by_segment = shares;
    ta.value = JSON.stringify(o, null, 2);
    toast('Quote applicate');
  }

  async function startJob(jobType, extraBody) {
    var sel = document.getElementById('do-seed-profile');
    var body = { job_type: jobType };
    if (extraBody) {
      Object.keys(extraBody).forEach(function (k) {
        body[k] = extraBody[k];
      });
    }
    if (sel && sel.value && !body.profile_v2) body.seed_profile_id = sel.value;
    var st = document.getElementById('do-job-status');
    st.textContent = 'Avvio…';
    try {
      var r = await fetch('/api/admin/data-jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
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

  function onTabDataOps() {
    loadLinks();
    refreshProfiles();
    loadSummaryAndQuality(true);
  }

  document.addEventListener('DOMContentLoaded', function () {
    var dataOps = document.getElementById('admin-data-ops');
    if (dataOps) {
      dataOps.addEventListener('click', function (ev) {
        var btn = ev.target.closest('[data-bq-drop]');
        if (!btn) return;
        ev.preventDefault();
        var tid = btn.getAttribute('data-bq-drop');
        if (tid) dropBqTable(tid);
      });
    }
    var btnTables = document.getElementById('do-load-tables');
    var btnSummary = document.getElementById('do-load-summary');
    var btnSchema = document.getElementById('do-load-schema');
    var btnCache = document.getElementById('do-clear-cache');
    var btnPrewarm = document.getElementById('do-prewarm');
    var btnPrecalc = document.getElementById('do-job-precalc');
    var btnFull = document.getElementById('do-job-full');
    var btnSaveProf = document.getElementById('sp-save');
    if (!btnTables) return;

    btnTables.addEventListener('click', loadTables);
    if (btnSummary) btnSummary.addEventListener('click', function () { loadSummaryAndQuality(false); });
    btnSchema.addEventListener('click', loadSchema);
    btnCache.addEventListener('click', clearCache);
    btnPrewarm.addEventListener('click', prewarm);
    btnPrecalc.addEventListener('click', function () {
      startJob('precalc');
    });
    btnFull.addEventListener('click', function () {
      if (!confirm('Full seed riscrive molti dati su BigQuery e può durare molto. Continuare?')) return;
      startJob('full_seed');
    });

    var dcPreset = document.getElementById('dc-preset');
    var dcApply = document.getElementById('dc-apply-shares');
    var dcVal = document.getElementById('dc-validate');
    var dcPrev = document.getElementById('dc-preview');
    var dcComp = document.getElementById('dc-compile');
    var dcFull = document.getElementById('dc-full-seed');
    if (dcPreset) {
      dcPreset.addEventListener('click', function () {
        var ta = document.getElementById('dc-profile-json');
        if (ta) ta.value = JSON.stringify(defaultProfileV2(), null, 2);
        toast('Preset caricato');
      });
    }
    if (dcApply) dcApply.addEventListener('click', applySharesToJson);
    if (dcVal) {
      dcVal.addEventListener('click', async function () {
        var out = document.getElementById('dc-out');
        try {
          var prof = readDcProfile();
          var r = await fetch('/api/admin/seed/validate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(prof),
          });
          var d = await r.json();
          out.textContent = JSON.stringify(d, null, 2);
          if (!r.ok) toast('Validazione fallita', 'error');
          else toast('OK');
        } catch (e) {
          out.textContent = String(e);
          toast(e.message, 'error');
        }
      });
    }
    if (dcPrev) {
      dcPrev.addEventListener('click', async function () {
        var out = document.getElementById('dc-out');
        try {
          var prof = readDcProfile();
          var r = await fetch('/api/admin/seed/preview', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ profile: prof, compile: true }),
          });
          var d = await r.json();
          out.textContent = JSON.stringify(d, null, 2);
          if (!r.ok) toast('Anteprima fallita', 'error');
        } catch (e) {
          out.textContent = String(e);
          toast(e.message, 'error');
        }
      });
    }
    if (dcComp) {
      dcComp.addEventListener('click', async function () {
        var out = document.getElementById('dc-out');
        try {
          var prof = readDcProfile();
          var r = await fetch('/api/admin/seed/compile', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ profile: prof, include_sql: false }),
          });
          var d = await r.json();
          out.textContent = JSON.stringify(d, null, 2);
          if (!r.ok) toast('Compile fallito', 'error');
          else toast('Compilato');
        } catch (e) {
          out.textContent = String(e);
          toast(e.message, 'error');
        }
      });
    }
    if (dcFull) {
      dcFull.addEventListener('click', async function () {
        if (!confirm('Full seed con profilo v2: riscrive dati su BigQuery. Continuare?')) return;
        var prof;
        try {
          prof = readDcProfile();
        } catch (e) {
          toast('JSON non valido', 'error');
          return;
        }
        var st = document.getElementById('do-job-status');
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
      });
    }
    btnSaveProf.addEventListener('click', async function () {
      var id = (document.getElementById('sp-id').value || '').trim();
      if (!id) {
        toast('ID profilo obbligatorio', 'error');
        return;
      }
      try {
        var r = await fetch('/api/admin/seed-profiles', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            id: id,
            name: document.getElementById('sp-name').value || id,
            num_orders: parseInt(document.getElementById('sp-orders').value, 10),
            num_customers: parseInt(document.getElementById('sp-customers').value, 10),
            product_count: parseInt(document.getElementById('sp-products').value, 10),
          }),
        });
        var d = await r.json();
        if (!r.ok) throw new Error(d.detail || r.statusText);
        toast('Profilo salvato');
        refreshProfiles();
      } catch (e) {
        toast(e.message, 'error');
      }
    });

    document.querySelectorAll('.admin-tab').forEach(function (tab) {
      tab.addEventListener('click', function () {
        if (tab.getAttribute('data-tab') === 'data-ops') onTabDataOps();
      });
    });
  });
})();
