#!/usr/bin/env python3
"""
Seed script per database retailer omnichannel PL (PLN).
Genera dati sintetici realistici: users, products, orders, promos, sessions/events.
Riproducibile con --seed. Richiede: psycopg2-binary, faker, numpy.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import random
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

import numpy as np

try:
    from faker import Faker
except ImportError:
    raise SystemExit("Installa: pip install faker")

try:
    import psycopg2
    from psycopg2.extras import execute_batch, execute_values
except ImportError:
    raise SystemExit("Installa: pip install psycopg2-binary")

# -----------------------------------------------------------------------------
# CONFIG (scala: 1.0 = full, 0.1 = 10% per test veloce)
# -----------------------------------------------------------------------------
START_DATE = date(2024, 1, 1)
END_DATE = date(2026, 12, 31)
CHANNELS = ("web", "app", "store")
PROMO_TYPES = (
    "sitewide_discount", "category_discount", "coupon_code", "cashback",
    "bundle", "coop_brand", "influencer", "app_only", "hit_dnia",
    "drugi_produkt_1zl", "gratis", "outlet_push"
)
FUNDING_TYPES = ("merchant_funded", "brand_funded", "mixed")
ORDER_STATUSES = ("confirmed", "shipped", "delivered", "delivered", "delivered", "cancelled")
EVENT_TYPES = ("page_view", "search", "view_item", "add_to_cart", "begin_checkout", "purchase")

# Macro categorie (livello 1) + sottocategorie esempio (livello 2/3)
CATEGORY_TREE = [
    ("TV, Audio i RTV", [
        ("Telewizory", ["Smart TV", "OLED", "QLED"]),
        ("Audio", ["Słuchawki", "Głośniki", "Soundbary"]),
    ]),
    ("AGD", [
        ("Pralki i suszarki", []),
        ("Lodówki", ["No Frost", "Americana"]),
    ]),
    ("AGD do zabudowy", [
        ("Płyty grzejne", []),
        ("Piekarniki", []),
    ]),
    ("AGD małe", [
        ("Odkurzacze", ["Wertykalne", "Robotyczne"]),
        ("Żelazka", []),
    ]),
    ("Komputery i tablety", [
        ("Laptopy", ["Gaming", "Biznes"]),
        ("Tablety", []),
    ]),
    ("Smartfony i zegarki", [
        ("Smartfony", []),
        ("Smartwatche", []),
    ]),
    ("Foto i kamery", [
        ("Aparaty", ["Kompaktowe", "Bezlusterkowce"]),
        ("Kamery", []),
    ]),
    ("Gaming", [
        ("Konsole", []),
        ("Gry", []),
    ]),
    ("Rowery i hulajnogi", [
        ("Rowery", ["Górskie", "Miejskie"]),
        ("Hulajnogi elektryczne", []),
    ]),
    ("Fitness i sport", [
        ("Bieżnie", []),
        ("Siłownia", []),
    ]),
    ("Dom", [
        ("Oświetlenie", []),
        ("Dekoracje", []),
    ]),
    ("Warsztat i ogród", [
        ("Narzędzia", []),
        ("Ogród", []),
    ]),
    ("Artykuły dla zwierząt", [
        ("Karma", []),
        ("Akcesoria", []),
    ]),
    ("Zabawki i LEGO", [
        ("LEGO", []),
        ("Zabawki", []),
    ]),
    ("Supermarket", [
        ("Elektronika codzienna", []),
        ("Akcesoria", []),
    ]),
    ("Książki, muzyka, film", [
        ("Książki", []),
        ("Filmy", []),
    ]),
    ("Moda", [
        ("Odzież", []),
        ("Obuwie", []),
    ]),
    ("Zdrowie i uroda", [
        ("Kosmetyki", []),
        ("Urządzenia", []),
    ]),
    ("Motoryzacja", [
        ("Elektronika samochodowa", []),
        ("Akcesoria", []),
    ]),
    ("Outlet", [
        ("Outlet RTV", []),
        ("Outlet AGD", []),
    ]),
]

# Brand names (esempi realistici per PL)
BRAND_NAMES = [
    "Samsung", "LG", "Sony", "Philips", "Bosch", "Whirlpool", "Electrolux",
    "Apple", "Xiaomi", "Huawei", "Dell", "HP", "Lenovo", "Asus", "Acer",
    "Canon", "Nikon", "GoPro", "JBL", "Sennheiser", "Dyson", "Rowenta",
    "Nintendo", "PlayStation", "Xbox", "LEGO", "Hasbro", "Gerber", "Purina",
    "Media Expert", "Beko", "Tefal", "Braun", "Oral-B", "Panasonic", "Toshiba",
    "Miele", "Siemens", "Grundig", "TCL", "Hisense", "Motorola", "OnePlus",
]


def db_conn(dsn: str | None = None):
    dsn = dsn or os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/retailer_pl"
    )
    return psycopg2.connect(dsn)


def random_date_in_range(start: date, end: date) -> date:
    d = (end - start).days
    return start + timedelta(days=random.randint(0, d) if d > 0 else 0)


def random_ts_in_range(start: date, end: date) -> datetime:
    d = random_date_in_range(start, end)
    return datetime.combine(d, datetime.min.time().replace(
        hour=random.randint(6, 22), minute=random.randint(0, 59), second=random.randint(0, 59)
    ))


def pln(amount: float) -> Decimal:
    return Decimal(str(round(amount, 2)))


# ---------- Categories ----------
def insert_categories(conn, cur) -> list[tuple[int, int | None, int, str, str]]:
    rows = []
    cat_id = 0
    for macro_name, children in CATEGORY_TREE:
        cat_id += 1
        path = macro_name
        rows.append((cat_id, None, 1, macro_name, path))
        parent_lev1 = cat_id
        for lev2_name, lev3_list in children:
            cat_id += 1
            path2 = f"{macro_name} > {lev2_name}"
            rows.append((cat_id, parent_lev1, 2, lev2_name, path2))
            parent_lev2 = cat_id
            for lev3_name in lev3_list:
                cat_id += 1
                path3 = f"{path2} > {lev3_name}"
                rows.append((cat_id, parent_lev2, 3, lev3_name, path3))
    execute_values(
        cur,
        """INSERT INTO core_commerce.categories (category_id, parent_category_id, level, category_name, category_path)
           VALUES %s ON CONFLICT (category_id) DO NOTHING""",
        [(r[0], r[1], r[2], r[3], r[4]) for r in rows],
        page_size=500
    )
    cur.execute("SELECT setval(pg_get_serial_sequence('core_commerce.categories', 'category_id'), (SELECT COALESCE(max(category_id), 1) FROM core_commerce.categories))")
    conn.commit()
    return rows


# ---------- Brands ----------
def insert_brands_fixed(conn, cur) -> list[int]:
    cur.execute(
        "INSERT INTO core_commerce.brands (brand_name) SELECT unnest(%s::text[]) RETURNING brand_id",
        (list(BRAND_NAMES),)
    )
    ids = [r[0] for r in cur.fetchall()]
    conn.commit()
    return ids


# ---------- Stores ----------
def insert_stores(conn, cur, fake: Faker, n: int = 80) -> list[int]:
    rows = [(fake.city(), fake.city() + " region", random.choice(["flagship", "standard", "outlet"])) for _ in range(n)]
    execute_values(cur, "INSERT INTO core_commerce.stores (city, region, format_type) VALUES %s RETURNING store_id", rows, page_size=100)
    ids = [r[0] for r in cur.fetchall()]
    conn.commit()
    return ids


# ---------- Users ----------
def insert_users(conn, cur, fake: Faker, scale: float) -> list[uuid.UUID]:
    n_users = int(250_000 * scale)  # 200k-500k target -> 250k at 1.0
    batch = 5000
    user_ids = []
    for start in range(0, n_users, batch):
        size = min(batch, n_users - start)
        rows = []
        for _ in range(size):
            uid = uuid.uuid4()
            user_ids.append(uid)
            email = fake.email()
            phone = fake.phone_number()[:20] if random.random() < 0.7 else None
            created = random_ts_in_range(START_DATE, END_DATE - timedelta(days=365))
            marketing = random.random() < 0.4
            sms = random.random() < 0.2
            emp = random.random() < 0.002
            first_purchase = random_ts_in_range(created.date(), END_DATE) if random.random() < 0.75 else None
            rows.append((
                uid, hashlib.sha256(email.encode()).hexdigest()[:64],
                hashlib.sha256((phone or "").encode()).hexdigest()[:64] if phone else None,
                created, "PL", marketing, sms, first_purchase, emp
            ))
        execute_values(
            cur,
            """INSERT INTO core_commerce.users (global_user_id, email_hash, phone_hash, created_at, country, marketing_opt_in, sms_opt_in, first_purchase_at, is_employee_flag)
               VALUES %s""",
            rows,
            page_size=1000
        )
        conn.commit()
        if (start // batch) % 10 == 0 and start > 0:
            print(f"  users {start + size}/{n_users}")
    return user_ids


# ---------- Categories (id list for products) ----------
def get_category_ids(cur) -> list[int]:
    cur.execute("SELECT category_id FROM core_commerce.categories ORDER BY level DESC, category_id")
    return [r[0] for r in cur.fetchall()]


# ---------- Products ----------
def insert_products(conn, cur, fake: Faker, brand_ids: list[int], category_ids: list[int], scale: float) -> list[int]:
    n_products = int(20_000 * scale)
    batch = 2000
    product_ids = []
    sku_set = set()
    for start in range(0, n_products, batch):
        size = min(batch, n_products - start)
        rows = []
        for _ in range(size):
            sku = fake.unique.bothify(text="??-####-??").upper()
            if sku in sku_set:
                sku = f"SKU-{uuid.uuid4().hex[:8].upper()}"
            sku_set.add(sku)
            brand_id = random.choice(brand_ids)
            cat_id = random.choice(category_ids)
            base = round(random.lognormvariate(4, 1.2), 2)
            base = max(9.99, min(49999, base))
            vat = random.choice([8, 23])
            cost = round(base * random.uniform(0.4, 0.85), 2)
            launch = random_date_in_range(START_DATE - timedelta(days=730), END_DATE)
            outlet = random.random() < 0.05
            name = fake.catch_phrase()[:200]
            rows.append((sku, brand_id, cat_id, name, base, vat, cost, launch, outlet))
        execute_values(
            cur,
            """INSERT INTO core_commerce.products (sku, brand_id, category_id, name, base_price_pln, vat_rate, cost_pln, launch_date, is_outlet_flag)
               VALUES %s RETURNING product_id""",
            rows,
            page_size=500
        )
        product_ids.extend([r[0] for r in cur.fetchall()])
        conn.commit()
        if (start // batch) % 5 == 0 and start > 0:
            print(f"  products {len(product_ids)}/{n_products}")
    return product_ids


# ---------- Promos (200+ con picchi BF / Xmas) ----------
def peak_weeks(d: date) -> str | None:
    # Black Friday (ultima settimana novembre)
    if d.month == 11 and d.day >= 22:
        return "black_friday"
    if d.month == 12 and d.day <= 31:
        return "xmas"
    if d.month == 8 or (d.month == 9 and d.day <= 15):
        return "back_to_school"
    if d.month == 1 and d.day <= 7:
        return "new_year"
    if d.month == 4 and 1 <= d.day <= 14:
        return "easter"
    return None


def insert_promos(conn, cur, fake: Faker, scale: float) -> list[int]:
    n_promos = max(200, int(250 * scale))
    promo_ids = []
    current = START_DATE
    while current <= END_DATE and len(promo_ids) < n_promos:
        peak = peak_weeks(current)
        if peak:
            count = random.randint(4, 12)
        else:
            count = random.randint(0, 2)
        for _ in range(count):
            start_ts = datetime.combine(current, datetime.min.time())
            dur = random.randint(3, 14) if peak else random.randint(1, 7)
            end_ts = start_ts + timedelta(days=dur)
            if end_ts.date() > END_DATE:
                continue
            promo_type = random.choice(PROMO_TYPES)
            if peak == "black_friday":
                promo_type = random.choice(["sitewide_discount", "category_discount", "hit_dnia", "coupon_code", "bundle"])
            if peak == "xmas":
                promo_type = random.choice(["sitewide_discount", "category_discount", "bundle", "drugi_produkt_1zl", "gratis"])
            funding = random.choice(FUNDING_TYPES)
            budget = round(random.lognormvariate(10, 1.5), 2) if random.random() < 0.6 else None
            cur.execute(
                """INSERT INTO promotions_marketing.promos (promo_name, promo_type, start_ts, end_ts, funding_type, planned_budget_pln, notes)
                   VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING promo_id""",
                (fake.catch_phrase()[:200], promo_type, start_ts, end_ts, funding, budget, peak)
            )
            promo_ids.append(cur.fetchone()[0])
            if len(promo_ids) >= n_promos:
                break
        current += timedelta(days=1)
    conn.commit()
    return promo_ids


# ---------- Orders & order_items ----------
def insert_orders_and_items(
    conn, cur, user_ids: list[uuid.UUID], product_ids: list[int],
    store_ids: list[int], promo_ids: list[int], category_ids: list[int], scale: float
) -> list[int]:
    n_orders = int(800_000 * scale)  # 500k-1.5M target
    cur.execute("SELECT product_id, base_price_pln, vat_rate FROM core_commerce.products")
    product_prices = {r[0]: (float(r[1]), float(r[2])) for r in cur.fetchall()}

    orders_inserted = []
    batch_orders = 2000
    for start in range(0, n_orders, batch_orders):
        size = min(batch_orders, n_orders - start)
        order_rows = []
        items_per_order = []  # list of list of (product_id, qty, unit_gross, unit_net, discount_gross, promo_id)
        for _ in range(size):
            use_guest = random.random() < 0.25
            gu = None if use_guest else random.choice(user_ids)
            guest_sid = uuid.uuid4() if use_guest else None
            order_ts = random_ts_in_range(START_DATE, END_DATE)
            channel = random.choices(CHANNELS, weights=[0.5, 0.25, 0.25])[0]
            store_id = random.choice(store_ids) if channel == "store" else None
            status = random.choice(ORDER_STATUSES)
            n_items = min(8, max(1, int(np.random.exponential(2)) + 1))
            product_sample = random.sample(product_ids, min(n_items, len(product_ids)))
            gross = Decimal("0")
            net = Decimal("0")
            vat = Decimal("0")
            shipping = pln(0 if random.random() < 0.4 else random.uniform(9.99, 29.99))
            items_this_order = []
            for pid in product_sample:
                pr = product_prices.get(pid)
                if not pr:
                    continue
                base, vat_rate = pr[0], pr[1]
                qty = random.randint(1, 2)
                promo_id = None
                disc = Decimal("0")
                if random.random() < 0.35 and promo_ids:
                    promo_id = random.choice(promo_ids)
                    depth = random.uniform(0.05, 0.35)
                    disc = pln(base * depth * qty)
                unit_net = (Decimal(str(base)) * qty - disc) / qty
                unit_gross = unit_net * (1 + Decimal(str(vat_rate)) / 100)
                item_net = unit_net * qty
                item_vat = item_net * Decimal(str(vat_rate)) / 100
                gross += item_net + item_vat
                net += item_net
                vat += item_vat
                items_this_order.append((pid, qty, unit_gross, unit_net, disc, promo_id))
            if not items_this_order and product_ids:
                pid = random.choice(product_ids)
                base, vat_rate = product_prices.get(pid, (99.0, 23.0))
                unit_net = pln(base)
                unit_gross = unit_net * (1 + Decimal(str(vat_rate)) / 100)
                gross, net, vat = unit_gross, unit_net, unit_gross - unit_net
                items_this_order.append((pid, 1, unit_gross, unit_net, Decimal("0"), None))
            order_rows.append((gu, guest_sid, order_ts, channel, store_id, status, gross, net, vat, shipping, random.choice(["card", "blik", "transfer", "cash"])))
            items_per_order.append(items_this_order)
        execute_values(
            cur,
            """INSERT INTO core_commerce.orders (global_user_id, guest_session_id, order_ts, channel, store_id, status, gross_pln, net_pln, vat_pln, shipping_pln, payment_method)
               VALUES %s RETURNING order_id""",
            order_rows,
            page_size=500
        )
        oids = [r[0] for r in cur.fetchall()]
        orders_inserted.extend(oids)
        all_items = []
        for oid, items in zip(oids, items_per_order):
            for (pid, qty, ug, un, disc, promo_id) in items:
                all_items.append((oid, pid, qty, ug, un, disc, promo_id))
        if all_items:
            execute_values(
                cur,
                """INSERT INTO core_commerce.order_items (order_id, product_id, qty, unit_gross_pln, unit_net_pln, discount_gross_pln, promo_applied_id)
                   VALUES %s""",
                all_items,
                page_size=1000
            )
        conn.commit()
        if (start // batch_orders) % 20 == 0 and start > 0:
            print(f"  orders {len(orders_inserted)}/{n_orders}")
    return orders_inserted


# ---------- Returns ----------
def insert_returns(conn, cur, order_ids: list[int], user_ids: list[uuid.UUID]) -> None:
    cur.execute("SELECT order_id, global_user_id, gross_pln FROM core_commerce.orders WHERE status = 'delivered' AND global_user_id IS NOT NULL LIMIT 50000")
    orders = cur.fetchall()
    subset = random.sample(orders, min(len(orders), int(len(orders) * 0.08)))
    rows = []
    for oid, uid, gross in subset:
        ref = round(float(gross) * random.uniform(0.3, 1.0), 2)
        rows.append((oid, uid, random_ts_in_range(START_DATE, END_DATE), random.choice(["defect", "wrong_item", "changed_mind", "other"]), ref))
    if rows:
        execute_values(cur, "INSERT INTO core_commerce.returns (order_id, global_user_id, return_ts, reason_code, refund_pln) VALUES %s", rows, page_size=500)
    conn.commit()


# ---------- Sessions & events ----------
def insert_sessions_events(
    conn, cur, user_ids: list[uuid.UUID], product_ids: list[int], category_ids: list[int], scale: float
) -> None:
    n_events_target = int(2_000_000 * scale)
    batch_sessions = 5000
    event_count = 0
    fake = Faker("pl_PL")
    Faker.seed(42)
    while event_count < n_events_target:
        n_sessions = min(batch_sessions, (n_events_target - event_count) // 15)
        session_rows = []
        for _ in range(n_sessions):
            guest = random.random() < 0.4
            gu = None if guest else random.choice(user_ids)
            gsid = uuid.uuid4() if guest else None
            start = random_ts_in_range(START_DATE, END_DATE)
            ch = random.choice(["web", "app"])
            session_rows.append((uuid.uuid4(), gu, gsid, start, ch, random.choice(["desktop", "mobile", "tablet"]), random.choice(["organic", "direct", "cpc", "email"]), random.choice(["none", "cpc", "organic"]), None))
        execute_values(
            cur,
            """INSERT INTO digital_analytics.sessions (session_id, global_user_id, guest_session_id, session_start_ts, channel, device_type, traffic_source, traffic_medium, campaign_id)
               VALUES %s""",
            [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8]) for r in session_rows],
            page_size=500
        )
        conn.commit()
        sids = [r[0] for r in session_rows]
        events_batch = []
        for sid in sids:
            n_ev = random.randint(1, 25)
            ts = random_ts_in_range(START_DATE, END_DATE)
            for _ in range(n_ev):
                et = random.choices(EVENT_TYPES, weights=[40, 10, 15, 10, 5, 2])[0]
                pid = random.choice(product_ids) if et in ("view_item", "add_to_cart", "purchase") and random.random() < 0.7 else None
                cid = random.choice(category_ids) if pid is None and random.random() < 0.3 else None
                events_batch.append((sid, ts, et, fake.uri()[:500], fake.uri()[:500] if random.random() < 0.5 else None, pid, cid, fake.word() if et == "search" and random.random() < 0.5 else None))
                ts += timedelta(seconds=random.randint(5, 300))
            event_count += n_ev
        for chunk_start in range(0, len(events_batch), 10000):
            chunk = events_batch[chunk_start:chunk_start + 10000]
            execute_values(
                cur,
                """INSERT INTO digital_analytics.events (session_id, event_ts, event_type, page_url, referrer, product_id, category_id, search_query)
                   VALUES %s""",
                chunk,
                page_size=2000
            )
        conn.commit()
        events_batch.clear()
        if event_count % 200_000 < batch_sessions * 15:
            print(f"  events ~{event_count}/{n_events_target}")
        if event_count >= n_events_target:
            break
    return


# ---------- Promo exposures & clicks (sample) ----------
def insert_promo_exposures_clicks(conn, cur, promo_ids: list[int], user_ids: list[uuid.UUID]) -> None:
    placements = ["homepage_banner", "category_tile", "search_banner", "product_page", "checkout", "newsletter"]
    channels = ["web", "app", "email", "sms", "push", "paid_ads"]
    cur.execute("SELECT order_id, global_user_id FROM core_commerce.orders WHERE global_user_id IS NOT NULL LIMIT 100000")
    orders = cur.fetchall()
    exposures = []
    for _ in range(min(150_000, len(orders) * 2)):
        oid, uid = random.choice(orders)
        pid = random.choice(promo_ids)
        exp_ts = random_ts_in_range(START_DATE, END_DATE)
        exposures.append((pid, uid, None, exp_ts, random.choice(channels), random.choice(placements), None))
    execute_values(
        cur,
        """INSERT INTO promotions_marketing.promo_exposures (promo_id, global_user_id, guest_session_id, exposure_ts, channel, placement, campaign_id)
           VALUES %s RETURNING exposure_id""",
        exposures,
        page_size=1000
    )
    exp_ids = [r[0] for r in cur.fetchall()]
    clicks = []
    for eid in random.sample(exp_ids, min(50000, len(exp_ids))):
        clicks.append((eid, random_ts_in_range(START_DATE, END_DATE), random.choice(["category", "product", "promo_lp"]), str(uuid.uuid4())[:16]))
    execute_values(cur, "INSERT INTO promotions_marketing.promo_clicks (exposure_id, click_ts, landing_type, landing_id) VALUES %s", clicks, page_size=1000)
    conn.commit()


# ---------- Promo attribution (sample) ----------
def insert_promo_attribution_costs(conn, cur, order_ids: list[int], promo_ids: list[int]) -> None:
    cur.execute("SELECT order_id, gross_pln FROM core_commerce.orders WHERE order_id = ANY(%s) LIMIT 50000", (order_ids[:50000],))
    orders = cur.fetchall()
    attr = []
    for oid, gross in random.sample(orders, min(30000, len(orders))):
        pid = random.choice(promo_ids)
        attr.append((oid, pid, random.choice(["last_touch", "first_touch", "linear"]), round(random.random(), 4), random.randint(1, 14)))
    execute_values(cur, "INSERT INTO promotions_marketing.promo_attribution (order_id, promo_id, attrib_model, attrib_weight, lookback_days) VALUES %s", attr, page_size=1000)
    costs = []
    for pid in random.sample(promo_ids, min(150, len(promo_ids))):
        for _ in range(random.randint(1, 4)):
            costs.append((pid, random.choice(["media_spend", "influencer_fee", "coop_fee", "discount_cost", "cashback_cost"]), round(random.uniform(100, 50000), 2), random_ts_in_range(START_DATE, END_DATE)))
    execute_values(cur, "INSERT INTO promotions_marketing.promo_costs (promo_id, cost_type, cost_pln, cost_ts) VALUES %s", costs, page_size=500)
    conn.commit()


# ---------- Identity stitch (sample) ----------
def insert_identity_stitch(conn, cur, user_ids: list[uuid.UUID]) -> None:
    cur.execute("SELECT DISTINCT guest_session_id FROM core_commerce.orders WHERE guest_session_id IS NOT NULL LIMIT 5000")
    guests = [r[0] for r in cur.fetchall()]
    rows = []
    for gsid in random.sample(guests, min(1000, len(guests))):
        uid = random.choice(user_ids)
        rows.append((gsid, uid, random_ts_in_range(START_DATE, END_DATE), random.choice(["signup", "login", "checkout_email_match"])))
    if rows:
        execute_values(cur, "INSERT INTO identity.identity_stitch (guest_session_id, global_user_id, stitched_ts, method) VALUES %s ON CONFLICT (guest_session_id) DO NOTHING", rows, page_size=500)
    conn.commit()


def main():
    ap = argparse.ArgumentParser(description="Seed retailer PL database")
    ap.add_argument("--dsn", default=os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/retailer_pl"), help="PostgreSQL DSN")
    ap.add_argument("--seed", type=int, default=42, help="Random seed")
    ap.add_argument("--scale", type=float, default=0.15, help="Scale factor 0.01-1.0 (default 0.15 for quicker run)")
    ap.add_argument("--skip-events", action="store_true", help="Skip sessions/events (slow)")
    args = ap.parse_args()
    random.seed(args.seed)
    np.random.seed(args.seed)
    Faker.seed(args.seed)

    conn = db_conn(args.dsn)
    cur = conn.cursor()
    fake = Faker("pl_PL")

    print("Seed: categories")
    insert_categories(conn, cur)
    category_ids = get_category_ids(cur)
    print("Seed: brands")
    brand_ids = insert_brands_fixed(conn, cur)
    print("Seed: stores")
    store_ids = insert_stores(conn, cur, fake)
    print("Seed: users")
    user_ids = insert_users(conn, cur, fake, args.scale)
    print("Seed: products")
    product_ids = insert_products(conn, cur, fake, brand_ids, category_ids, args.scale)
    print("Seed: promos")
    promo_ids = insert_promos(conn, cur, fake, args.scale)
    print("Seed: orders & order_items")
    order_ids = insert_orders_and_items(conn, cur, user_ids, product_ids, store_ids, promo_ids, category_ids, args.scale)
    print("Seed: returns")
    insert_returns(conn, cur, order_ids, user_ids)
    if not args.skip_events:
        print("Seed: sessions & events")
        insert_sessions_events(conn, cur, user_ids, product_ids, category_ids, args.scale)
    print("Seed: promo exposures & clicks")
    insert_promo_exposures_clicks(conn, cur, promo_ids, user_ids)
    print("Seed: promo attribution & costs")
    insert_promo_attribution_costs(conn, cur, order_ids, promo_ids)
    print("Seed: identity_stitch")
    insert_identity_stitch(conn, cur, user_ids)

    cur.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
