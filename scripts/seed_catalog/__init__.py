"""Configurazione e generazione SQL per il catalogo prodotti seed (dim_product)."""

from __future__ import annotations

from .dim_product_sql import build_brand_subcat_pairs, generate_products

__all__ = ["build_brand_subcat_pairs", "generate_products"]
