"""Period helpers: date shift for YoY comparison on arbitrary ranges (MI/BC live paths)."""
from __future__ import annotations

import calendar
from datetime import date, datetime, timedelta


def shift_period_years(ps: str, pe: str, delta_years: int) -> tuple[str, str]:
    """Sposta finestra [ps,pe] di delta_years (es. -1 = stesso mese/giorno anno precedente)."""
    d0 = datetime.strptime(ps, "%Y-%m-%d").date()
    d1 = datetime.strptime(pe, "%Y-%m-%d").date()

    def add_years(d: date, dy: int) -> date:
        y = d.year + dy
        try:
            return date(y, d.month, d.day)
        except ValueError:
            # 29 feb → 28 feb
            return date(y, d.month, 28 if d.month == 2 else min(d.day, calendar.monthrange(y, d.month)[1]))

    return add_years(d0, delta_years).isoformat(), add_years(d1, delta_years).isoformat()


def iso_week_to_date_range(iso_year: int, iso_week: int) -> tuple[str, str]:
    """Lun-Dom della settimana ISO (Europe/Warsaw non necessario: solo indici settimana ISO)."""
    # Python: Monday week 1 contains Jan 4
    jan4 = date(iso_year, 1, 4)
    start = jan4 - timedelta(days=jan4.isoweekday() - 1) + timedelta(weeks=iso_week - 1)
    end = start + timedelta(days=6)
    return start.isoformat(), end.isoformat()
