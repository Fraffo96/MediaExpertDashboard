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
        el.textContent =
          'Stato: ' +
          j.status +
          (j.message ? ' — ' + j.message : '') +
          (j.error_snippet ? ' — ' + j.error_snippet : '');
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

  async function loadTables() {
    var tbody = document.getElementById('do-bq-tables');
    tbody.innerHTML = '<tr><td colspan="4">Caricamento…</td></tr>';
    try {
      var r = await fetch('/api/admin/bq/tables');
      var d = await r.json();
      if (!r.ok) throw new Error(d.detail || r.statusText);
      tbody.innerHTML = '';
      (d.tables || []).forEach(function (t) {
        var tr = document.createElement('tr');
        tr.innerHTML =
          '<td><code>' +
          (t.table_id || '') +
          '</code></td><td>' +
          (t.row_count != null ? t.row_count : '') +
          '</td><td>' +
          (t.size_human || '') +
          '</td><td>' +
          (t.last_modified_time || '') +
          '</td>';
        tbody.appendChild(tr);
      });
      if (!tbody.children.length) tbody.innerHTML = '<tr><td colspan="4">Nessuna tabella.</td></tr>';
      toast('Tabelle aggiornate');
    } catch (e) {
      tbody.innerHTML = '<tr><td colspan="4">' + e.message + '</td></tr>';
      toast(e.message, 'error');
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

  async function startJob(jobType) {
    var sel = document.getElementById('do-seed-profile');
    var body = { job_type: jobType };
    if (sel && sel.value) body.seed_profile_id = sel.value;
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
  }

  document.addEventListener('DOMContentLoaded', function () {
    var btnTables = document.getElementById('do-load-tables');
    var btnSchema = document.getElementById('do-load-schema');
    var btnCache = document.getElementById('do-clear-cache');
    var btnPrewarm = document.getElementById('do-prewarm');
    var btnPrecalc = document.getElementById('do-job-precalc');
    var btnFull = document.getElementById('do-job-full');
    var btnSaveProf = document.getElementById('sp-save');
    if (!btnTables) return;

    btnTables.addEventListener('click', loadTables);
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
