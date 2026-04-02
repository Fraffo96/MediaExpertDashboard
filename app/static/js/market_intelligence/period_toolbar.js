/**
 * Toolbar periodo: all years | anno | mese | settimana ISO. Chiama MIOnPeriodApply o BCOnPeriodApply.
 */
(function() {
  function pad(n) {
    return n < 10 ? '0' + n : '' + n;
  }

  function isoWeekRange(isoYear, week) {
    var jan4 = new Date(isoYear, 0, 4);
    var dow = jan4.getDay() || 7;
    var monday = new Date(jan4);
    monday.setDate(jan4.getDate() - dow + 1 + (week - 1) * 7);
    var sunday = new Date(monday);
    sunday.setDate(monday.getDate() + 6);
    function fmt(d) {
      return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate());
    }
    return [fmt(monday), fmt(sunday)];
  }

  function lastDayOfMonth(y, m) {
    return new Date(y, m, 0).getDate();
  }

  function fillYearSelect() {
    var sel = document.getElementById('mi-period-year-select');
    if (!sel) return;
    var yrs = (typeof window.MI_AVAILABLE_YEARS !== 'undefined' && window.MI_AVAILABLE_YEARS) ? window.MI_AVAILABLE_YEARS : [];
    sel.innerHTML = '';
    yrs.forEach(function(y) {
      var o = document.createElement('option');
      o.value = String(y);
      o.textContent = String(y);
      sel.appendChild(o);
    });
    if (yrs.length) sel.value = String(yrs[yrs.length - 1]);
  }

  function showPanels(mode) {
    ['mi-period-panel-year', 'mi-period-panel-month', 'mi-period-panel-week'].forEach(function(id) {
      var el = document.getElementById(id);
      if (el) el.style.display = 'none';
    });
    if (mode === 'year') {
      var y = document.getElementById('mi-period-panel-year');
      if (y) y.style.display = '';
    } else if (mode === 'month') {
      var m = document.getElementById('mi-period-panel-month');
      if (m) m.style.display = '';
    } else if (mode === 'week') {
      var w = document.getElementById('mi-period-panel-week');
      if (w) w.style.display = '';
      var iy = document.getElementById('mi-period-iso-year');
      var iw = document.getElementById('mi-period-iso-week');
      if (iy && !iy.value) {
        var yrs = window.MI_AVAILABLE_YEARS || [];
        iy.value = yrs.length ? String(yrs[yrs.length - 1]) : String(new Date().getFullYear());
      }
      if (iw && !iw.value) iw.value = '1';
    }
  }

  function readBounds() {
    var mode = document.getElementById('mi-period-granularity');
    mode = mode ? mode.value : 'all_years';
    if (mode === 'all_years') return { mode: 'all_years' };
    if (mode === 'year') {
      var sel = document.getElementById('mi-period-year-select');
      var y = sel && sel.value ? parseInt(sel.value, 10) : new Date().getFullYear();
      return { mode: 'year', ps: y + '-01-01', pe: y + '-12-31' };
    }
    if (mode === 'month') {
      var inp = document.getElementById('mi-period-month-input');
      var v = inp && inp.value;
      if (!v) return null;
      var parts = v.split('-');
      var yy = parseInt(parts[0], 10);
      var mm = parseInt(parts[1], 10);
      var ld = lastDayOfMonth(yy, mm);
      return { mode: 'month', ps: yy + '-' + pad(mm) + '-01', pe: yy + '-' + pad(mm) + '-' + pad(ld) };
    }
    if (mode === 'week') {
      var iyEl = document.getElementById('mi-period-iso-year');
      var iwEl = document.getElementById('mi-period-iso-week');
      var isoY = iyEl && iyEl.value ? parseInt(iyEl.value, 10) : new Date().getFullYear();
      var wk = iwEl && iwEl.value ? parseInt(iwEl.value, 10) : 1;
      if (wk < 1 || wk > 53) return null;
      var r = isoWeekRange(isoY, wk);
      return { mode: 'week', ps: r[0], pe: r[1] };
    }
    return null;
  }

  function init() {
    var gran = document.getElementById('mi-period-granularity');
    var apply = document.getElementById('mi-period-apply');
    if (!gran || !apply) return;

    fillYearSelect();

    var monthInp = document.getElementById('mi-period-month-input');
    if (monthInp && !monthInp.value) {
      var d = new Date();
      monthInp.value = d.getFullYear() + '-' + pad(d.getMonth() + 1);
    }

    gran.addEventListener('change', function() {
      showPanels(gran.value === 'all_years' ? '' : gran.value);
    });

    apply.addEventListener('click', function() {
      var b = readBounds();
      if (!b) {
        if (typeof showError === 'function') showError('Select a valid period');
        return;
      }
      if (b.mode === 'all_years') {
        if (typeof window.MIOnPeriodApply === 'function') window.MIOnPeriodApply('', '', 'all_years');
        else if (typeof window.BCOnPeriodApply === 'function') window.BCOnPeriodApply('', '', 'all_years');
        return;
      }
      var label = b.ps + ' → ' + b.pe;
      if (typeof window.MIOnPeriodApply === 'function') window.MIOnPeriodApply(b.ps, b.pe, b.mode, label);
      else if (typeof window.BCOnPeriodApply === 'function') window.BCOnPeriodApply(b.ps, b.pe, b.mode, label);
    });

    showPanels('');
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
