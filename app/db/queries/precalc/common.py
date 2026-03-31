"""Helper comuni per query precalc: periodo, parametri."""
# pylint: disable=unused-import
from google.cloud import bigquery
from google.cloud.bigquery import ArrayQueryParameter

from app.db.client import run_query

DATASET = "mart"


def _params_year_cat(year: int, category_ids: list[int] | None = None):
    p = [bigquery.ScalarQueryParameter("year", "INT64", year)]
    if category_ids:
        p.append(ArrayQueryParameter("cat_ids", "INT64", category_ids))
    return p


def is_full_year_period(ps: str, pe: str) -> bool:
    """True se ps e pe sono esattamente 01-01 e 12-31 dello stesso anno."""
    if not ps or not pe:
        return False
    try:
        y = int(ps[:4])
        return ps == f"{y}-01-01" and pe == f"{y}-12-31"
    except (ValueError, IndexError):
        return False


def get_multi_year_full_years(ps: str, pe: str) -> list[int] | None:
    """Se ps..pe è un range di anni interi contigui, ritorna [y_start..y_end]. Altrimenti None."""
    if not ps or not pe:
        return None
    try:
        y_start = int(ps[:4])
        y_end = int(pe[:4])
        if y_start > y_end:
            return None
        if ps != f"{y_start}-01-01" or pe != f"{y_end}-12-31":
            return None
        return list(range(y_start, y_end + 1))
    except (ValueError, IndexError):
        return None
