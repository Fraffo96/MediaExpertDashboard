/**
 * Controlli periodo (anno / mese / settimana ISO) per singolo widget MI/BC.
 * Stile allineato a MIGenericDropdown; fetch lato pagina tramite opts.fetchSlice.
 */
(function() {
  'use strict';

  function pad(n) {
    return n < 10 ? '0' + n : '' + n;
  }

  function lastDayOfMonth(y, m) {
    return new Date(y, m, 0).getDate();
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

  var granItems = [
    { value: 'year', label: 'Full year' },
    { value: 'month', label: 'Month' },
    { value: 'week', label: 'ISO week' }
  ];

  var _ddRegistry = {};
  var _widgetCfg = [];
  var _opts = null;
  var _debouncers = {};

  function readBounds(suffix, mode) {
    if (mode === 'year') return null;
    if (mode === 'month') {
      var yEl = document.getElementById('mi-period-month-y-' + suffix);
      var mEl = document.getElementById('mi-period-month-m-' + suffix);
      var yy = yEl && yEl.value ? parseInt(yEl.value, 10) : NaN;
      var mm = mEl && mEl.value ? parseInt(mEl.value, 10) : NaN;
      if (!yy || !mm || mm < 1 || mm > 12) return null;
      var ld = lastDayOfMonth(yy, mm);
      return { ps: yy + '-' + pad(mm) + '-01', pe: yy + '-' + pad(mm) + '-' + pad(ld) };
    }
    if (mode === 'week') {
      var iyEl = document.getElementById('mi-period-iso-y-' + suffix);
      var iwEl = document.getElementById('mi-period-iso-w-' + suffix);
      var isoY = iyEl && iyEl.value ? parseInt(iyEl.value, 10) : NaN;
      var wk = iwEl && iwEl.value ? parseInt(iwEl.value, 10) : NaN;
      if (!isoY || wk < 1 || wk > 53) return null;
      var r = isoWeekRange(isoY, wk);
      return { ps: r[0], pe: r[1] };
    }
    return null;
  }

  function updateRowsForWidget(w, state) {
    var mode = state[w.modeKey] || 'year';
    var showY = mode === 'year';
    var showM = mode === 'month';
    var showW = mode === 'week';
    function rowDisp(cls, on) {
      document.querySelectorAll(cls).forEach(function(el) {
        el.style.display = on ? '' : 'none';
      });
    }
    rowDisp('.mi-period-row-month-' + w.suffix, showM);
    rowDisp('.mi-period-row-week-' + w.suffix, showW);
    rowDisp('.mi-period-row-year-' + w.suffix, showY);
  }

  function defaultsForSuffix(suffix, defaultYear) {
    var mY = document.getElementById('mi-period-month-y-' + suffix);
    var mM = document.getElementById('mi-period-month-m-' + suffix);
    if (mY && !mY.dataset.miPeriodTouched && defaultYear) {
      mY.value = String(defaultYear);
    }
    if (mM && !mM.dataset.miPeriodTouched) {
      mM.value = String(new Date().getMonth() + 1);
    }
    var iy = document.getElementById('mi-period-iso-y-' + suffix);
    var iw = document.getElementById('mi-period-iso-w-' + suffix);
    if (iy && !iy.value && defaultYear) iy.value = String(defaultYear);
    if (iw && !iw.value) iw.value = '1';
  }

  function scheduleSlice(w) {
    if (!_opts) return;
    var state = _opts.state;
    var mode = state[w.modeKey] || 'year';
    if (mode === 'year') return;
    var b = readBounds(w.suffix, mode);
    if (!b) return;
    var key = w.scope;
    if (_debouncers[key]) clearTimeout(_debouncers[key]);
    _debouncers[key] = setTimeout(function() {
      _debouncers[key] = null;
      _opts.fetchSlice(w.scope, b.ps, b.pe);
    }, 380);
  }

  function init(opts) {
    _opts = opts;
    _widgetCfg = opts.widgets || [];
    var state = opts.state;
    if (!window.MIGenericDropdown) return;

    _widgetCfg.forEach(function(w) {
      if (!state[w.modeKey]) state[w.modeKey] = 'year';
      defaultsForSuffix(w.suffix, opts.defaultCalendarYear);

      _ddRegistry[w.scope] = window.MIGenericDropdown.create('mi-period-gran-' + w.suffix, {
        items: granItems,
        initialValue: state[w.modeKey],
        minWidth: 200,
        onChange: function(v) {
          state[w.modeKey] = v || 'year';
          updateRowsForWidget(w, state);
          if (state[w.modeKey] === 'year') {
            opts.liveOverrides[w.scope] = null;
            opts.applyViewFromState(w.scope);
          } else {
            scheduleSlice(w);
          }
        }
      });

      updateRowsForWidget(w, state);

      function onBoundInput() {
        if ((state[w.modeKey] || 'year') === 'year') return;
        scheduleSlice(w);
      }
      var mYEl = document.getElementById('mi-period-month-y-' + w.suffix);
      var mMEl = document.getElementById('mi-period-month-m-' + w.suffix);
      function onMonthInput(el) {
        if (el) el.dataset.miPeriodTouched = '1';
        onBoundInput();
      }
      if (mYEl) {
        mYEl.addEventListener('change', function() { onMonthInput(mYEl); });
        mYEl.addEventListener('input', function() { onMonthInput(mYEl); });
      }
      if (mMEl) {
        mMEl.addEventListener('change', function() { onMonthInput(mMEl); });
        mMEl.addEventListener('input', function() { onMonthInput(mMEl); });
      }
      var iyEl = document.getElementById('mi-period-iso-y-' + w.suffix);
      var iwEl = document.getElementById('mi-period-iso-w-' + w.suffix);
      if (iyEl) iyEl.addEventListener('change', onBoundInput);
      if (iwEl) iwEl.addEventListener('change', onBoundInput);
    });
  }

  function resetAllToYear(state, liveOverrides) {
    _widgetCfg.forEach(function(w) {
      state[w.modeKey] = 'year';
      if (liveOverrides) liveOverrides[w.scope] = null;
      var dd = _ddRegistry[w.scope];
      if (dd && dd.setValue) dd.setValue('year', true);
      updateRowsForWidget(w, state);
    });
  }

  function forceYearMode(scope) {
    if (!_widgetCfg.length || !_opts) return;
    var w = null;
    for (var i = 0; i < _widgetCfg.length; i++) {
      if (_widgetCfg[i].scope === scope) {
        w = _widgetCfg[i];
        break;
      }
    }
    if (!w) return;
    _opts.state[w.modeKey] = 'year';
    if (_opts.liveOverrides) _opts.liveOverrides[scope] = null;
    var dd = _ddRegistry[scope];
    if (dd && dd.setValue) dd.setValue('year', true);
    updateRowsForWidget(w, _opts.state);
  }

  window.MIPeriodWidgets = {
    init: init,
    resetAllToYear: resetAllToYear,
    forceYearMode: forceYearMode,
    pad: pad,
    lastDayOfMonth: lastDayOfMonth,
    isoWeekRange: isoWeekRange,
    readBounds: readBounds
  };
})();
