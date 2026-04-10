/**
 * Promo Creator – filters: category/subcategory cascade.
 */
(function() {
  function init() {
    var catSel = document.getElementById('pc-category');
    var subcatSel = document.getElementById('pc-subcategory');
    if (!catSel || !subcatSel) return;

    function filterSubcategoryOptions() {
      var cat = catSel.value;
      subcatSel.querySelectorAll('option').forEach(function(o) {
        if (o.value === '') { o.style.display = ''; return; }
        o.style.display = (!cat || o.getAttribute('data-parent') === cat) ? '' : 'none';
      });
      var sel = subcatSel.options[subcatSel.selectedIndex];
      if (sel && sel.value && cat && sel.getAttribute('data-parent') !== cat) {
        subcatSel.value = '';
      }
    }

    catSel.addEventListener('change', filterSubcategoryOptions);
    filterSubcategoryOptions();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
