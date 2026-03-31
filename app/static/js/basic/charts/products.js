/**
 * Basic dashboard – Product Performance: tabella top prodotti.
 */
(function() {
  function fmt(n) {
    return n == null ? '--' : Number(n).toLocaleString('en-US', { maximumFractionDigits: 0 });
  }

  function update(d) {
    var table = document.getElementById('tableTopProducts');
    if (!table || !d) return;
    var tbody = table.querySelector('tbody');
    if (!tbody) return;
    var rows = d.top_products || [];
    tbody.innerHTML = rows.map(function(r) {
      return '<tr><td>' + (r.product_name || '—') + '</td><td>' + (r.brand_name || '—') + '</td><td>' + (r.category_name || '—') + '</td><td class="num">' + fmt(r.gross_pln) + '</td><td class="num">' + fmt(r.units) + '</td></tr>';
    }).join('');
  }

  window.BasicChartsProducts = { update: update };
})();
