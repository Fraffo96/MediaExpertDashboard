-- =============================================================================
-- MEDIA EXPERT DASHBOARD - Database Migrations (PostgreSQL)
-- Retailer omnichannel PL (PLN) - OLTP + Tracking + Marketing
-- 3 logical schemas: core_commerce, promotions_marketing, digital_analytics
-- + identity_stitch + analytics_mart
-- =============================================================================

SET client_encoding = 'UTF8';

-- -----------------------------------------------------------------------------
-- EXTENSIONS
-- -----------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- -----------------------------------------------------------------------------
-- SCHEMAS
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS core_commerce;
CREATE SCHEMA IF NOT EXISTS promotions_marketing;
CREATE SCHEMA IF NOT EXISTS digital_analytics;
CREATE SCHEMA IF NOT EXISTS identity;
CREATE SCHEMA IF NOT EXISTS analytics_mart;

-- -----------------------------------------------------------------------------
-- ENUMS (shared where possible)
-- -----------------------------------------------------------------------------
CREATE TYPE core_commerce.channel_type AS ENUM ('web', 'app', 'store');
CREATE TYPE core_commerce.order_status_type AS ENUM ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled', 'returned');
CREATE TYPE core_commerce.address_type_enum AS ENUM ('billing', 'shipping', 'both');

CREATE TYPE promotions_marketing.promo_type_enum AS ENUM (
  'sitewide_discount', 'category_discount', 'coupon_code', 'cashback',
  'bundle', 'coop_brand', 'influencer', 'app_only', 'hit_dnia',
  'drugi_produkt_1zl', 'gratis', 'outlet_push'
);
CREATE TYPE promotions_marketing.funding_type_enum AS ENUM ('merchant_funded', 'brand_funded', 'mixed');
CREATE TYPE promotions_marketing.scope_enum AS ENUM ('sku', 'category', 'brand', 'cart_threshold');
CREATE TYPE promotions_marketing.discount_type_enum AS ENUM ('pct', 'amount', 'fixed_price');
CREATE TYPE promotions_marketing.channel_enum AS ENUM ('web', 'app', 'email', 'sms', 'push', 'paid_ads');
CREATE TYPE promotions_marketing.placement_enum AS ENUM (
  'homepage_banner', 'category_tile', 'search_banner', 'product_page', 'checkout', 'newsletter'
);
CREATE TYPE promotions_marketing.landing_type_enum AS ENUM ('category', 'product', 'promo_lp');
CREATE TYPE promotions_marketing.attrib_model_enum AS ENUM ('last_touch', 'first_touch', 'linear');
CREATE TYPE promotions_marketing.cost_type_enum AS ENUM (
  'media_spend', 'influencer_fee', 'coop_fee', 'discount_cost', 'cashback_cost'
);

CREATE TYPE digital_analytics.session_channel_enum AS ENUM ('web', 'app');
CREATE TYPE digital_analytics.event_type_enum AS ENUM (
  'page_view', 'search', 'view_item', 'add_to_cart', 'begin_checkout', 'purchase'
);
CREATE TYPE digital_analytics.cart_action_enum AS ENUM ('add', 'remove');

CREATE TYPE identity.stitch_method_enum AS ENUM ('signup', 'login', 'checkout_email_match');

-- -----------------------------------------------------------------------------
-- CORE_COMMERCE
-- -----------------------------------------------------------------------------

-- brands
CREATE TABLE core_commerce.brands (
  brand_id   SERIAL PRIMARY KEY,
  brand_name VARCHAR(255) NOT NULL
);
CREATE INDEX idx_brands_name ON core_commerce.brands (brand_name);

-- categories (hierarchy: level 1 = macro, 2-3 = sub)
CREATE TABLE core_commerce.categories (
  category_id       SERIAL PRIMARY KEY,
  parent_category_id INT REFERENCES core_commerce.categories (category_id),
  level             SMALLINT NOT NULL CHECK (level BETWEEN 1 AND 3),
  category_name     VARCHAR(255) NOT NULL,
  category_path     VARCHAR(512) NOT NULL
);
CREATE INDEX idx_categories_parent ON core_commerce.categories (parent_category_id);
CREATE INDEX idx_categories_level ON core_commerce.categories (level);

-- users (global_user_id = UUID, unico cross-schema)
CREATE TABLE core_commerce.users (
  global_user_id    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  email_hash        VARCHAR(64),
  phone_hash        VARCHAR(64),
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  country           CHAR(2) NOT NULL DEFAULT 'PL',
  marketing_opt_in  BOOLEAN NOT NULL DEFAULT FALSE,
  sms_opt_in        BOOLEAN NOT NULL DEFAULT FALSE,
  first_purchase_at TIMESTAMPTZ,
  is_employee_flag  BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX idx_users_created ON core_commerce.users (created_at);
CREATE INDEX idx_users_first_purchase ON core_commerce.users (first_purchase_at) WHERE first_purchase_at IS NOT NULL;

-- user_addresses
CREATE TABLE core_commerce.user_addresses (
  address_id     SERIAL PRIMARY KEY,
  global_user_id UUID NOT NULL REFERENCES core_commerce.users (global_user_id) ON DELETE CASCADE,
  city           VARCHAR(100) NOT NULL,
  postal_code    VARCHAR(20) NOT NULL,
  region         VARCHAR(100),
  address_type   core_commerce.address_type_enum NOT NULL DEFAULT 'shipping'
);
CREATE INDEX idx_user_addresses_user ON core_commerce.user_addresses (global_user_id);

-- stores
CREATE TABLE core_commerce.stores (
  store_id   SERIAL PRIMARY KEY,
  city       VARCHAR(100) NOT NULL,
  region     VARCHAR(100),
  format_type VARCHAR(50)
);
CREATE INDEX idx_stores_region ON core_commerce.stores (region);

-- products
CREATE TABLE core_commerce.products (
  product_id     SERIAL PRIMARY KEY,
  sku            VARCHAR(64) NOT NULL UNIQUE,
  brand_id       INT NOT NULL REFERENCES core_commerce.brands (brand_id),
  category_id    INT NOT NULL REFERENCES core_commerce.categories (category_id),
  name           VARCHAR(500) NOT NULL,
  base_price_pln NUMERIC(12,2) NOT NULL CHECK (base_price_pln >= 0),
  vat_rate       NUMERIC(5,2) NOT NULL CHECK (vat_rate >= 0 AND vat_rate <= 100),
  cost_pln       NUMERIC(12,2) CHECK (cost_pln >= 0),
  launch_date    DATE,
  is_outlet_flag BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX idx_products_sku ON core_commerce.products (sku);
CREATE INDEX idx_products_brand ON core_commerce.products (brand_id);
CREATE INDEX idx_products_category ON core_commerce.products (category_id);
CREATE INDEX idx_products_outlet ON core_commerce.products (is_outlet_flag) WHERE is_outlet_flag;

-- orders
CREATE TABLE core_commerce.orders (
  order_id        BIGSERIAL PRIMARY KEY,
  global_user_id  UUID REFERENCES core_commerce.users (global_user_id),
  guest_session_id UUID,
  order_ts        TIMESTAMPTZ NOT NULL,
  channel         core_commerce.channel_type NOT NULL,
  store_id        INT REFERENCES core_commerce.stores (store_id),
  status          core_commerce.order_status_type NOT NULL DEFAULT 'pending',
  gross_pln       NUMERIC(14,2) NOT NULL CHECK (gross_pln >= 0),
  net_pln         NUMERIC(14,2) NOT NULL CHECK (net_pln >= 0),
  vat_pln         NUMERIC(14,2) NOT NULL CHECK (vat_pln >= 0),
  shipping_pln    NUMERIC(10,2) NOT NULL DEFAULT 0 CHECK (shipping_pln >= 0),
  payment_method  VARCHAR(50)
);
CREATE INDEX idx_orders_user ON core_commerce.orders (global_user_id) WHERE global_user_id IS NOT NULL;
CREATE INDEX idx_orders_guest ON core_commerce.orders (guest_session_id) WHERE guest_session_id IS NOT NULL;
CREATE INDEX idx_orders_ts ON core_commerce.orders (order_ts);
CREATE INDEX idx_orders_channel ON core_commerce.orders (channel);
CREATE INDEX idx_orders_status ON core_commerce.orders (status);

-- order_items (promo_applied_id FK to promotions_marketing.promos)
CREATE TABLE core_commerce.order_items (
  order_item_id     BIGSERIAL PRIMARY KEY,
  order_id          BIGINT NOT NULL REFERENCES core_commerce.orders (order_id) ON DELETE CASCADE,
  product_id        INT NOT NULL REFERENCES core_commerce.products (product_id),
  qty               INT NOT NULL CHECK (qty > 0),
  unit_gross_pln    NUMERIC(12,2) NOT NULL CHECK (unit_gross_pln >= 0),
  unit_net_pln      NUMERIC(12,2) NOT NULL CHECK (unit_net_pln >= 0),
  discount_gross_pln NUMERIC(12,2) NOT NULL DEFAULT 0 CHECK (discount_gross_pln >= 0),
  promo_applied_id  INT
  -- FK to promotions_marketing.promos added after that table exists
);
CREATE INDEX idx_order_items_order ON core_commerce.order_items (order_id);
CREATE INDEX idx_order_items_product ON core_commerce.order_items (product_id);
CREATE INDEX idx_order_items_promo ON core_commerce.order_items (promo_applied_id) WHERE promo_applied_id IS NOT NULL;

-- returns
CREATE TABLE core_commerce.returns (
  return_id     BIGSERIAL PRIMARY KEY,
  order_id      BIGINT NOT NULL REFERENCES core_commerce.orders (order_id),
  global_user_id UUID REFERENCES core_commerce.users (global_user_id),
  return_ts     TIMESTAMPTZ NOT NULL,
  reason_code   VARCHAR(50),
  refund_pln    NUMERIC(14,2) NOT NULL CHECK (refund_pln >= 0)
);
CREATE INDEX idx_returns_order ON core_commerce.returns (order_id);
CREATE INDEX idx_returns_user ON core_commerce.returns (global_user_id) WHERE global_user_id IS NOT NULL;

-- -----------------------------------------------------------------------------
-- PROMOTIONS_MARKETING
-- -----------------------------------------------------------------------------

CREATE TABLE promotions_marketing.promos (
  promo_id          SERIAL PRIMARY KEY,
  promo_name        VARCHAR(255) NOT NULL,
  promo_type        promotions_marketing.promo_type_enum NOT NULL,
  start_ts          TIMESTAMPTZ NOT NULL,
  end_ts            TIMESTAMPTZ NOT NULL,
  funding_type      promotions_marketing.funding_type_enum,
  planned_budget_pln NUMERIC(14,2),
  notes             TEXT
);
CREATE INDEX idx_promos_dates ON promotions_marketing.promos (start_ts, end_ts);
CREATE INDEX idx_promos_type ON promotions_marketing.promos (promo_type);

CREATE TABLE promotions_marketing.promo_rules (
  promo_rule_id   SERIAL PRIMARY KEY,
  promo_id        INT NOT NULL REFERENCES promotions_marketing.promos (promo_id) ON DELETE CASCADE,
  scope           promotions_marketing.scope_enum NOT NULL,
  scope_id        INT,
  min_cart_pln    NUMERIC(12,2),
  discount_type   promotions_marketing.discount_type_enum NOT NULL,
  discount_value  NUMERIC(12,2) NOT NULL,
  max_redemptions INT
);
CREATE INDEX idx_promo_rules_promo ON promotions_marketing.promo_rules (promo_id);

CREATE TABLE promotions_marketing.coupons (
  coupon_id       BIGSERIAL PRIMARY KEY,
  promo_id        INT NOT NULL REFERENCES promotions_marketing.promos (promo_id),
  coupon_code     VARCHAR(64) NOT NULL,
  is_personalized BOOLEAN NOT NULL DEFAULT FALSE,
  global_user_id  UUID REFERENCES core_commerce.users (global_user_id),
  issued_ts       TIMESTAMPTZ NOT NULL,
  redeemed_ts     TIMESTAMPTZ
);
CREATE INDEX idx_coupons_promo ON promotions_marketing.coupons (promo_id);
CREATE INDEX idx_coupons_code ON promotions_marketing.coupons (coupon_code);
CREATE INDEX idx_coupons_user ON promotions_marketing.coupons (global_user_id) WHERE global_user_id IS NOT NULL;

CREATE TABLE promotions_marketing.promo_exposures (
  exposure_id     BIGSERIAL PRIMARY KEY,
  promo_id        INT NOT NULL REFERENCES promotions_marketing.promos (promo_id),
  global_user_id  UUID REFERENCES core_commerce.users (global_user_id),
  guest_session_id UUID,
  exposure_ts     TIMESTAMPTZ NOT NULL,
  channel         promotions_marketing.channel_enum NOT NULL,
  placement       promotions_marketing.placement_enum,
  campaign_id     VARCHAR(64)
);
CREATE INDEX idx_promo_exposures_promo ON promotions_marketing.promo_exposures (promo_id);
CREATE INDEX idx_promo_exposures_user ON promotions_marketing.promo_exposures (global_user_id) WHERE global_user_id IS NOT NULL;
CREATE INDEX idx_promo_exposures_ts ON promotions_marketing.promo_exposures (exposure_ts);

CREATE TABLE promotions_marketing.promo_clicks (
  click_id     BIGSERIAL PRIMARY KEY,
  exposure_id  BIGINT NOT NULL REFERENCES promotions_marketing.promo_exposures (exposure_id) ON DELETE CASCADE,
  click_ts     TIMESTAMPTZ NOT NULL,
  landing_type promotions_marketing.landing_type_enum,
  landing_id   VARCHAR(64)
);
CREATE INDEX idx_promo_clicks_exposure ON promotions_marketing.promo_clicks (exposure_id);

CREATE TABLE promotions_marketing.promo_attribution (
  attrib_id     BIGSERIAL PRIMARY KEY,
  order_id      BIGINT NOT NULL REFERENCES core_commerce.orders (order_id),
  promo_id      INT NOT NULL REFERENCES promotions_marketing.promos (promo_id),
  attrib_model  promotions_marketing.attrib_model_enum NOT NULL,
  attrib_weight NUMERIC(5,4) NOT NULL CHECK (attrib_weight >= 0 AND attrib_weight <= 1),
  lookback_days INT NOT NULL CHECK (lookback_days > 0)
);
CREATE INDEX idx_promo_attrib_order ON promotions_marketing.promo_attribution (order_id);
CREATE INDEX idx_promo_attrib_promo ON promotions_marketing.promo_attribution (promo_id);

CREATE TABLE promotions_marketing.promo_costs (
  cost_id   BIGSERIAL PRIMARY KEY,
  promo_id  INT NOT NULL REFERENCES promotions_marketing.promos (promo_id),
  cost_type promotions_marketing.cost_type_enum NOT NULL,
  cost_pln  NUMERIC(14,2) NOT NULL CHECK (cost_pln >= 0),
  cost_ts   TIMESTAMPTZ NOT NULL
);
CREATE INDEX idx_promo_costs_promo ON promotions_marketing.promo_costs (promo_id);

-- Add FK from order_items to promos (deferred)
ALTER TABLE core_commerce.order_items
  ADD CONSTRAINT fk_order_items_promo
  FOREIGN KEY (promo_applied_id) REFERENCES promotions_marketing.promos (promo_id);

-- -----------------------------------------------------------------------------
-- IDENTITY STITCH (guest → registered)
-- -----------------------------------------------------------------------------
CREATE TABLE identity.identity_stitch (
  stitch_id        BIGSERIAL PRIMARY KEY,
  guest_session_id UUID NOT NULL,
  global_user_id   UUID NOT NULL REFERENCES core_commerce.users (global_user_id),
  stitched_ts      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  method           identity.stitch_method_enum NOT NULL
);
CREATE UNIQUE INDEX idx_identity_stitch_guest ON identity.identity_stitch (guest_session_id);
CREATE INDEX idx_identity_stitch_user ON identity.identity_stitch (global_user_id);

-- -----------------------------------------------------------------------------
-- DIGITAL_ANALYTICS
-- -----------------------------------------------------------------------------

CREATE TABLE digital_analytics.sessions (
  session_id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  global_user_id  UUID REFERENCES core_commerce.users (global_user_id),
  guest_session_id UUID,
  session_start_ts TIMESTAMPTZ NOT NULL,
  channel         digital_analytics.session_channel_enum NOT NULL,
  device_type     VARCHAR(50),
  traffic_source  VARCHAR(100),
  traffic_medium  VARCHAR(100),
  campaign_id     VARCHAR(64)
);
CREATE INDEX idx_sessions_user ON digital_analytics.sessions (global_user_id) WHERE global_user_id IS NOT NULL;
CREATE INDEX idx_sessions_guest ON digital_analytics.sessions (guest_session_id) WHERE guest_session_id IS NOT NULL;
CREATE INDEX idx_sessions_start ON digital_analytics.sessions (session_start_ts);

CREATE TABLE digital_analytics.events (
  event_id    BIGSERIAL PRIMARY KEY,
  session_id  UUID NOT NULL REFERENCES digital_analytics.sessions (session_id) ON DELETE CASCADE,
  event_ts    TIMESTAMPTZ NOT NULL,
  event_type  digital_analytics.event_type_enum NOT NULL,
  page_url    TEXT,
  referrer    TEXT,
  product_id  INT REFERENCES core_commerce.products (product_id),
  category_id INT REFERENCES core_commerce.categories (category_id),
  search_query TEXT
);
CREATE INDEX idx_events_session ON digital_analytics.events (session_id);
CREATE INDEX idx_events_ts ON digital_analytics.events (event_ts);
CREATE INDEX idx_events_type ON digital_analytics.events (event_type);
CREATE INDEX idx_events_product ON digital_analytics.events (product_id) WHERE product_id IS NOT NULL;

CREATE TABLE digital_analytics.searches (
  search_id    BIGSERIAL PRIMARY KEY,
  session_id   UUID NOT NULL REFERENCES digital_analytics.sessions (session_id) ON DELETE CASCADE,
  search_ts    TIMESTAMPTZ NOT NULL,
  query_text   TEXT NOT NULL,
  results_count INT
);
CREATE INDEX idx_searches_session ON digital_analytics.searches (session_id);
CREATE INDEX idx_searches_ts ON digital_analytics.searches (search_ts);

CREATE TABLE digital_analytics.cart_events (
  cart_event_id BIGSERIAL PRIMARY KEY,
  session_id    UUID NOT NULL REFERENCES digital_analytics.sessions (session_id) ON DELETE CASCADE,
  event_ts      TIMESTAMPTZ NOT NULL,
  product_id    INT NOT NULL REFERENCES core_commerce.products (product_id),
  qty           INT NOT NULL,
  action        digital_analytics.cart_action_enum NOT NULL
);
CREATE INDEX idx_cart_events_session ON digital_analytics.cart_events (session_id);
CREATE INDEX idx_cart_events_ts ON digital_analytics.cart_events (event_ts);

-- -----------------------------------------------------------------------------
-- COMMENTS (documentation)
-- -----------------------------------------------------------------------------
COMMENT ON SCHEMA core_commerce IS 'OLTP: users, orders, products, stores, returns';
COMMENT ON SCHEMA promotions_marketing IS 'Promos, exposures, clicks, attribution, costs';
COMMENT ON SCHEMA digital_analytics IS 'Sessions, events, searches, cart_events';
COMMENT ON SCHEMA identity IS 'Guest-to-user stitching';
COMMENT ON COLUMN core_commerce.orders.guest_session_id IS 'Set when order is placed by guest; can be stitched later via identity_stitch';
COMMENT ON COLUMN core_commerce.order_items.promo_applied_id IS 'FK to promotions_marketing.promos';
