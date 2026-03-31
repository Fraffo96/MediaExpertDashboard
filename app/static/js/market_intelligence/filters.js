/**
 * Market Intelligence – filters: category/subcategory cascade.
 */
(function() {
  function init() {
    var catSel = document.getElementById('mi-category');
    var subcatSel = document.getElementById('mi-subcategory');
    if (!catSel || !subcatSel) return;

    function filterSubcategoryOptions() {
      var cat = catSel.value;
      subcatSel.querySelectorAll('option').forEach(function(o) {
        if (o.value === '') { o.style.display = ''; return; }
        o.style.display = (!cat || o.getAttribute('data-parent') === cat) ? '' : 'none';
      });
    }

    catSel.addEventListener('change', filterSubcategoryOptions);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
