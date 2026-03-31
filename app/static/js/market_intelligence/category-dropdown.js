/**
 * Market Intelligence – dropdown unificato categorie/subcategorie.
 * Usa tema scuro (no bianco/arancione), subcategorie come bullet points.
 * Uso: MICategoryDropdown.create(containerId, options) → { getValue, setValue, onchange }
 */
(function() {
  'use strict';

  var dropdownRegistry = [];

  function closeAllExcept(exceptWrap) {
    dropdownRegistry.forEach(function(entry) {
      if (entry.wrap !== exceptWrap) entry.close();
    });
  }

  function positionPanelFixed(panel, trigger) {
    var rect = trigger.getBoundingClientRect();
    var minW = (trigger.closest && trigger.closest('.mi-dropdown-product-sku')) ? 320 : 260;
    var vw = window.innerWidth;
    var vh = window.innerHeight;
    var gap = 8;

    panel.style.position = 'fixed';
    panel.style.width = 'max-content';
    panel.style.minWidth = Math.max(200, minW) + 'px';
    panel.style.maxWidth = 'min(420px, 95vw)';
    panel.style.zIndex = '10002';

    var left = rect.right + gap;
    var top = rect.top;
    if (left + 420 > vw) left = Math.max(8, vw - 420 - 8);
    if (left < 8) left = 8;
    if (top + 360 > vh - 16) top = Math.max(8, vh - 360 - 16);
    if (top < 8) top = 8;

    panel.style.left = left + 'px';
    panel.style.top = top + 'px';
  }

  function create(containerId, options) {
    options = options || {};
    var container = document.getElementById(containerId);
    if (!container) return null;

    var items = options.items || [];
    var placeholder = options.placeholder || 'Select category or subcategory';
    var allLabel = options.allLabel || 'All categories';
    var includeAll = options.includeAll !== false;
    var minWidth = options.minWidth || 240;
    var onChange = options.onChange || function() {};

    var selectedValue = options.initialValue || '';
    var selectedLabel = '';

    function escapeHtml(s) {
      if (!s) return '';
      var d = document.createElement('div');
      d.textContent = s;
      return d.innerHTML;
    }

    function getLabelForValue(val) {
      if (!val) return includeAll ? allLabel : placeholder;
      for (var i = 0; i < items.length; i++) {
        var it = items[i];
        if (('cat_' + it.id) === val) return it.name;
        if (it.subcategories) {
          for (var j = 0; j < it.subcategories.length; j++) {
            var sub = it.subcategories[j];
            if (('sub_' + sub.id) === val) return sub.name;
          }
        }
      }
      return val;
    }

    function buildItems() {
      var list = [];
      if (includeAll) list.push({ value: '', label: allLabel, isSub: false });
      items.forEach(function(cat) {
        list.push({ value: 'cat_' + cat.id, label: cat.name, isSub: false });
        (cat.subcategories || []).forEach(function(sub) {
          list.push({ value: 'sub_' + sub.id, label: sub.name, isSub: true });
        });
      });
      return list;
    }

    var flatItems = buildItems();
    selectedLabel = getLabelForValue(selectedValue);

    var wrap = document.createElement('div');
    wrap.className = 'mi-cat-dropdown-wrap';
    wrap.style.minWidth = minWidth + 'px';

    var trigger = document.createElement('button');
    trigger.type = 'button';
    trigger.className = 'mi-cat-dropdown-trigger';
    trigger.setAttribute('aria-haspopup', 'listbox');
    trigger.setAttribute('aria-expanded', 'false');
    trigger.innerHTML = '<span class="mi-cat-dropdown-label" title="' + escapeHtml(selectedLabel || placeholder) + '">' + escapeHtml(selectedLabel || placeholder) + '</span><span class="mi-cat-dropdown-chevron"></span>';

    var panel = document.createElement('div');
    panel.className = 'mi-cat-dropdown-panel';
    panel.setAttribute('role', 'listbox');
    panel.style.display = 'none';

    flatItems.forEach(function(it) {
      var div = document.createElement('div');
      div.className = 'mi-cat-dropdown-item' + (it.isSub ? ' mi-cat-dropdown-sub' : '') + (it.value === selectedValue ? ' mi-cat-dropdown-selected' : '');
      div.setAttribute('role', 'option');
      div.setAttribute('data-value', it.value);
      div.innerHTML = it.isSub ? '<span class="mi-cat-dropdown-bullet">•</span>' + escapeHtml(it.label) : escapeHtml(it.label);
      div.addEventListener('click', function() {
        select(it.value, it.label, false);
        close();
      });
      div.addEventListener('mouseenter', function() { div.classList.add('mi-cat-dropdown-hover'); });
      div.addEventListener('mouseleave', function() { div.classList.remove('mi-cat-dropdown-hover'); });
      panel.appendChild(div);
    });

    function select(val, label, silent) {
      selectedValue = val;
      selectedLabel = label || getLabelForValue(val);
      var lbl = trigger.querySelector('.mi-cat-dropdown-label');
      if (lbl) { lbl.textContent = selectedLabel || placeholder; lbl.title = selectedLabel || placeholder; }
      panel.querySelectorAll('.mi-cat-dropdown-item').forEach(function(el) {
        el.classList.toggle('mi-cat-dropdown-selected', el.getAttribute('data-value') === val);
      });
      if (!silent) onChange(val);
    }

    var cardEl = null;
    var scrollHandler = null;
    var scrollElRef = null;

    function open() {
      closeAllExcept(wrap);
      document.body.appendChild(panel);
      positionPanelFixed(panel, trigger);
      panel.style.display = 'block';
      wrap.classList.add('open');
      cardEl = wrap.closest('.chart-card');
      if (cardEl) cardEl.classList.add('dropdown-open');
      trigger.setAttribute('aria-expanded', 'true');
      document.addEventListener('click', closeOnOutside);
      scrollHandler = function() { positionPanelFixed(panel, trigger); };
      scrollElRef = trigger.closest('.content') || window;
      scrollElRef.addEventListener('scroll', scrollHandler, true);
    }

    function close() {
      if (scrollHandler && scrollElRef) {
        scrollElRef.removeEventListener('scroll', scrollHandler, true);
        scrollHandler = null;
        scrollElRef = null;
      }
      wrap.appendChild(panel);
      panel.style.display = 'none';
      panel.style.position = '';
      panel.style.left = '';
      panel.style.top = '';
      panel.style.minWidth = '';
      panel.style.width = '';
      panel.style.maxWidth = '';
      wrap.classList.remove('open');
      if (cardEl) { cardEl.classList.remove('dropdown-open'); cardEl = null; }
      trigger.setAttribute('aria-expanded', 'false');
      document.removeEventListener('click', closeOnOutside);
    }

    function closeOnOutside(e) {
      if (!wrap.contains(e.target) && !panel.contains(e.target)) close();
    }

    trigger.addEventListener('click', function(e) {
      e.stopPropagation();
      if (panel.style.display === 'none') open();
      else close();
    });

    wrap.appendChild(trigger);
    wrap.appendChild(panel);
    container.innerHTML = '';
    container.appendChild(wrap);
    dropdownRegistry = dropdownRegistry.filter(function(e) { return document.body.contains(e.wrap); });
    dropdownRegistry.push({ wrap: wrap, close: close });

    return {
      getValue: function() { return selectedValue; },
      setValue: function(val, silent) { select(val, getLabelForValue(val), !!silent); },
      setItems: function(newItems) {
        items = newItems || [];
        flatItems = buildItems();
        panel.innerHTML = '';
        flatItems.forEach(function(it) {
          var div = document.createElement('div');
          div.className = 'mi-cat-dropdown-item' + (it.isSub ? ' mi-cat-dropdown-sub' : '') + (it.value === selectedValue ? ' mi-cat-dropdown-selected' : '');
          div.setAttribute('role', 'option');
          div.setAttribute('data-value', it.value);
          div.innerHTML = it.isSub ? '<span class="mi-cat-dropdown-bullet">•</span>' + escapeHtml(it.label) : escapeHtml(it.label);
          div.addEventListener('click', function() {
            select(it.value, it.label, false);
            close();
          });
          div.addEventListener('mouseenter', function() { div.classList.add('mi-cat-dropdown-hover'); });
          div.addEventListener('mouseleave', function() { div.classList.remove('mi-cat-dropdown-hover'); });
          panel.appendChild(div);
        });
      }
    };
  }

  /**
   * Dropdown generico per liste semplici – stesso stile visivo di MICategoryDropdown.
   * Uso: MIGenericDropdown.create(containerId, options)
   * options: { items: [{value, label}], initialValue, onChange, placeholder, minWidth }
   */
  function createGeneric(containerId, options) {
    options = options || {};
    var container = document.getElementById(containerId);
    if (!container) return null;

    var items = options.items || [];
    var placeholder = options.placeholder || 'Select…';
    var minWidth = options.minWidth || 200;
    var onChange = options.onChange || function() {};

    var selectedValue = options.initialValue || '';
    var selectedLabel = '';

    function escapeHtml(s) {
      if (!s) return '';
      var d = document.createElement('div');
      d.textContent = s;
      return d.innerHTML;
    }

    function getLabelForValue(val) {
      for (var i = 0; i < items.length; i++) {
        if (String(items[i].value) === String(val)) return items[i].label;
      }
      return placeholder;
    }

    selectedLabel = getLabelForValue(selectedValue);

    var wrap = document.createElement('div');
    wrap.className = 'mi-cat-dropdown-wrap';
    wrap.style.minWidth = minWidth + 'px';

    var trigger = document.createElement('button');
    trigger.type = 'button';
    trigger.className = 'mi-cat-dropdown-trigger';
    trigger.setAttribute('aria-haspopup', 'listbox');
    trigger.setAttribute('aria-expanded', 'false');
    trigger.innerHTML = '<span class="mi-cat-dropdown-label" title="' + escapeHtml(selectedLabel || placeholder) + '">' + escapeHtml(selectedLabel || placeholder) + '</span><span class="mi-cat-dropdown-chevron"></span>';

    var panel = document.createElement('div');
    panel.className = 'mi-cat-dropdown-panel';
    panel.setAttribute('role', 'listbox');
    panel.style.display = 'none';

    items.forEach(function(it) {
      var val = it.value;
      var lbl = it.label;
      var div = document.createElement('div');
      div.className = 'mi-cat-dropdown-item' + (String(val) === String(selectedValue) ? ' mi-cat-dropdown-selected' : '');
      div.setAttribute('role', 'option');
      div.setAttribute('data-value', val);
      div.textContent = lbl;
      div.addEventListener('click', function() {
        select(val, lbl, false);
        close();
      });
      div.addEventListener('mouseenter', function() { div.classList.add('mi-cat-dropdown-hover'); });
      div.addEventListener('mouseleave', function() { div.classList.remove('mi-cat-dropdown-hover'); });
      panel.appendChild(div);
    });

    function select(val, label, silent) {
      selectedValue = val;
      selectedLabel = label || getLabelForValue(val);
      var lbl = trigger.querySelector('.mi-cat-dropdown-label');
      if (lbl) { lbl.textContent = selectedLabel || placeholder; lbl.title = selectedLabel || placeholder; }
      panel.querySelectorAll('.mi-cat-dropdown-item').forEach(function(el) {
        el.classList.toggle('mi-cat-dropdown-selected', String(el.getAttribute('data-value')) === String(val));
      });
      if (!silent) onChange(val);
    }

    var cardEl = null;
    var scrollHandler = null;
    var scrollElRef = null;

    function open() {
      closeAllExcept(wrap);
      document.body.appendChild(panel);
      positionPanelFixed(panel, trigger);
      panel.style.display = 'block';
      wrap.classList.add('open');
      cardEl = wrap.closest('.chart-card');
      if (cardEl) cardEl.classList.add('dropdown-open');
      trigger.setAttribute('aria-expanded', 'true');
      document.addEventListener('click', closeOnOutside);
      scrollHandler = function() { positionPanelFixed(panel, trigger); };
      scrollElRef = trigger.closest('.content') || window;
      scrollElRef.addEventListener('scroll', scrollHandler, true);
    }

    function close() {
      if (scrollHandler && scrollElRef) {
        scrollElRef.removeEventListener('scroll', scrollHandler, true);
        scrollHandler = null;
        scrollElRef = null;
      }
      wrap.appendChild(panel);
      panel.style.display = 'none';
      panel.style.position = '';
      panel.style.left = '';
      panel.style.top = '';
      panel.style.minWidth = '';
      panel.style.width = '';
      panel.style.maxWidth = '';
      wrap.classList.remove('open');
      if (cardEl) { cardEl.classList.remove('dropdown-open'); cardEl = null; }
      trigger.setAttribute('aria-expanded', 'false');
      document.removeEventListener('click', closeOnOutside);
    }

    function closeOnOutside(e) {
      if (!wrap.contains(e.target) && !panel.contains(e.target)) close();
    }

    trigger.addEventListener('click', function(e) {
      e.stopPropagation();
      if (panel.style.display === 'none') open();
      else close();
    });

    wrap.appendChild(trigger);
    wrap.appendChild(panel);
    container.innerHTML = '';
    container.appendChild(wrap);
    dropdownRegistry = dropdownRegistry.filter(function(e) { return document.body.contains(e.wrap); });
    dropdownRegistry.push({ wrap: wrap, close: close });

    return {
      getValue: function() { return selectedValue; },
      setValue: function(val, silent) { select(val, getLabelForValue(val), !!silent); }
    };
  }

  window.MICategoryDropdown = { create: create };
  window.MIGenericDropdown = { create: createGeneric };
})();
