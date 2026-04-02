-- =============================================================================
-- BigQuery mart – Media Expert Dashboard
-- 10 categories + 72 subcategories, 55 brands, 6 HCG segments, products table
-- Period: 2023-01-01 → 2025-12-31
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS raw  OPTIONS(location = "EU");
CREATE SCHEMA IF NOT EXISTS mart OPTIONS(location = "EU");

-- ─── dim_brand (55 brands; brand_country, brand_category_focus for analytics) ─
CREATE OR REPLACE TABLE mart.dim_brand (
  brand_id INT64 NOT NULL,
  brand_name STRING NOT NULL,
  brand_country STRING,
  brand_category_focus STRING
);
INSERT mart.dim_brand (brand_id, brand_name, brand_country, brand_category_focus) VALUES
  (1,'Samsung','KR','TV, Large Appliances, Smartphones'),
  (2,'LG','KR','TV, Large Appliances'),
  (3,'Sony','JP','TV, Audio, Gaming, Photo'),
  (4,'Philips','NL','TV, Small Appliances, Smart Home, Health'),
  (5,'TCL','CN','TV'),
  (6,'Hisense','CN','TV'),
  (7,'Panasonic','JP','TV'),
  (8,'Apple','US','Smartphones, Computers, Audio'),
  (9,'Xiaomi','CN','Smartphones, Small Appliances, Smart Home'),
  (10,'Oppo','CN','Smartphones'),
  (11,'Realme','CN','Smartphones'),
  (12,'Huawei','CN','Smartphones'),
  (13,'Motorola','US','Smartphones'),
  (14,'OnePlus','CN','Smartphones'),
  (15,'Garmin','CH','Smartphones & wearables'),
  (16,'Dell','US','Computers'),
  (17,'HP','US','Computers'),
  (18,'Lenovo','CN','Computers'),
  (19,'Asus','TW','Computers, Gaming'),
  (20,'Acer','TW','Computers'),
  (21,'MSI','TW','Computers, Gaming'),
  (22,'Logitech','CH','Computers, Gaming'),
  (23,'TP-Link','CN','Computers, Smart Home'),
  (24,'Microsoft','US','Gaming'),
  (25,'Nintendo','JP','Gaming'),
  (26,'Razer','US','Gaming'),
  (27,'SteelSeries','DK','Gaming'),
  (28,'HyperX','US','Gaming'),
  (29,'Bosch','DE','Large Appliances, Small Appliances'),
  (30,'Siemens','DE','Large Appliances'),
  (31,'Whirlpool','US','Large Appliances'),
  (32,'Beko','TR','Large Appliances'),
  (33,'Electrolux','SE','Large Appliances'),
  (34,'Amica','PL','Large Appliances'),
  (35,'Tefal','FR','Small Appliances'),
  (36,'Dyson','UK','Small Appliances, Health'),
  (37,'DeLonghi','IT','Small Appliances'),
  (38,'Krups','DE','Small Appliances'),
  (39,'Google','US','Smart Home'),
  (40,'Amazon','US','Smart Home'),
  (41,'Ring','US','Smart Home'),
  (42,'Bose','US','Audio'),
  (43,'JBL','US','Audio'),
  (44,'Marshall','UK','Audio'),
  (45,'Beats','US','Audio'),
  (46,'Sennheiser','DE','Audio'),
  (47,'Braun','DE','Health & Beauty'),
  (48,'Oral-B','US','Health & Beauty'),
  (49,'Canon','JP','Photo & Video'),
  (50,'Nikon','JP','Photo & Video'),
  (51,'GoPro','US','Photo & Video'),
  (52,'DJI','CN','Photo & Video'),
  (53,'Remington','US','Health & Beauty'),
  (54,'Withings','FR','Health'),
  (55,'Fitbit','US','Smartphones & wearables');

-- ─── dim_category (10 parent + 72 subcategories) ────────────────────────────
CREATE OR REPLACE TABLE mart.dim_category (
  category_id        INT64  NOT NULL,
  category_name      STRING NOT NULL,
  level              INT64  NOT NULL,
  parent_category_id INT64,
  category_path      STRING
);
INSERT mart.dim_category (category_id, category_name, level, parent_category_id, category_path) VALUES
  (1,'TV & Home Entertainment',1,NULL,'TV & Home Entertainment'),
  (2,'Mobile and smartwatches',1,NULL,'Mobile and smartwatches'),
  (3,'Computers & IT',1,NULL,'Computers & IT'),
  (4,'Gaming',1,NULL,'Gaming'),
  (5,'Large Appliances',1,NULL,'Large Appliances'),
  (6,'Small Appliances',1,NULL,'Small Appliances'),
  (7,'Audio',1,NULL,'Audio'),
  (8,'Smart Home',1,NULL,'Smart Home'),
  (9,'Health & Beauty Tech',1,NULL,'Health & Beauty Tech'),
  (10,'Photo & Video',1,NULL,'Photo & Video'),
  (101,'LED TV',2,1,'TV & Home Entertainment > LED TV'),
  (102,'OLED TV',2,1,'TV & Home Entertainment > OLED TV'),
  (103,'QLED TV',2,1,'TV & Home Entertainment > QLED TV'),
  (104,'Mini LED TV',2,1,'TV & Home Entertainment > Mini LED TV'),
  (105,'Soundbars',2,1,'TV & Home Entertainment > Soundbars'),
  (106,'Home cinema systems',2,1,'TV & Home Entertainment > Home cinema systems'),
  (107,'Projectors',2,1,'TV & Home Entertainment > Projectors'),
  (108,'Streaming devices',2,1,'TV & Home Entertainment > Streaming devices'),
  (201,'Smartphones flagship',2,2,'Mobile and smartwatches > Smartphones flagship'),
  (202,'Smartphones mid-range',2,2,'Mobile and smartwatches > Smartphones mid-range'),
  (203,'Smartphones entry',2,2,'Mobile and smartwatches > Smartphones entry'),
  (204,'Foldable smartphones',2,2,'Mobile and smartwatches > Foldable smartphones'),
  (205,'Tablets',2,2,'Mobile and smartwatches > Tablets'),
  (206,'Smartwatches',2,2,'Mobile and smartwatches > Smartwatches'),
  (207,'Fitness trackers',2,2,'Mobile and smartwatches > Fitness trackers'),
  (208,'Phone accessories',2,2,'Mobile and smartwatches > Phone accessories'),
  (301,'Laptops',2,3,'Computers & IT > Laptops'),
  (302,'Gaming laptops',2,3,'Computers & IT > Gaming laptops'),
  (303,'Desktop PCs',2,3,'Computers & IT > Desktop PCs'),
  (304,'Monitors',2,3,'Computers & IT > Monitors'),
  (305,'Keyboards',2,3,'Computers & IT > Keyboards'),
  (306,'Mice',2,3,'Computers & IT > Mice'),
  (307,'Webcams',2,3,'Computers & IT > Webcams'),
  (308,'External storage',2,3,'Computers & IT > External storage'),
  (309,'Routers',2,3,'Computers & IT > Routers'),
  (310,'Mesh WiFi systems',2,3,'Computers & IT > Mesh WiFi systems'),
  (401,'Consoles',2,4,'Gaming > Consoles'),
  (402,'Gaming PCs',2,4,'Gaming > Gaming PCs'),
  (403,'Gaming laptops',2,4,'Gaming > Gaming laptops'),
  (404,'Controllers',2,4,'Gaming > Controllers'),
  (405,'Gaming headsets',2,4,'Gaming > Gaming headsets'),
  (406,'Gaming keyboards',2,4,'Gaming > Gaming keyboards'),
  (407,'Gaming mice',2,4,'Gaming > Gaming mice'),
  (408,'VR headsets',2,4,'Gaming > VR headsets'),
  (501,'Refrigerators',2,5,'Large Appliances > Refrigerators'),
  (502,'Washing machines',2,5,'Large Appliances > Washing machines'),
  (503,'Dryers',2,5,'Large Appliances > Dryers'),
  (504,'Dishwashers',2,5,'Large Appliances > Dishwashers'),
  (505,'Ovens',2,5,'Large Appliances > Ovens'),
  (506,'Induction hobs',2,5,'Large Appliances > Induction hobs'),
  (507,'Built-in appliances',2,5,'Large Appliances > Built-in appliances'),
  (508,'Freezers',2,5,'Large Appliances > Freezers'),
  (601,'Coffee machines',2,6,'Small Appliances > Coffee machines'),
  (602,'Blenders',2,6,'Small Appliances > Blenders'),
  (603,'Air fryers',2,6,'Small Appliances > Air fryers'),
  (604,'Vacuum cleaners',2,6,'Small Appliances > Vacuum cleaners'),
  (605,'Robot vacuums',2,6,'Small Appliances > Robot vacuums'),
  (606,'Kitchen processors',2,6,'Small Appliances > Kitchen processors'),
  (607,'Electric kettles',2,6,'Small Appliances > Electric kettles'),
  (608,'Toasters',2,6,'Small Appliances > Toasters'),
  (701,'Wireless headphones',2,7,'Audio > Wireless headphones'),
  (702,'Noise cancelling headphones',2,7,'Audio > Noise cancelling headphones'),
  (703,'Earbuds',2,7,'Audio > Earbuds'),
  (704,'Portable speakers',2,7,'Audio > Portable speakers'),
  (705,'Hi-Fi systems',2,7,'Audio > Hi-Fi systems'),
  (706,'DJ equipment',2,7,'Audio > DJ equipment'),
  (801,'Smart speakers',2,8,'Smart Home > Smart speakers'),
  (802,'Smart lighting',2,8,'Smart Home > Smart lighting'),
  (803,'Smart thermostats',2,8,'Smart Home > Smart thermostats'),
  (804,'Security cameras',2,8,'Smart Home > Security cameras'),
  (805,'Smart locks',2,8,'Smart Home > Smart locks'),
  (806,'Smart plugs',2,8,'Smart Home > Smart plugs'),
  (901,'Electric toothbrushes',2,9,'Health & Beauty Tech > Electric toothbrushes'),
  (902,'Hair dryers',2,9,'Health & Beauty Tech > Hair dryers'),
  (903,'Hair straighteners',2,9,'Health & Beauty Tech > Hair straighteners'),
  (904,'Grooming kits',2,9,'Health & Beauty Tech > Grooming kits'),
  (905,'Smart scales',2,9,'Health & Beauty Tech > Smart scales'),
  (1001,'Cameras',2,10,'Photo & Video > Cameras'),
  (1002,'Mirrorless cameras',2,10,'Photo & Video > Mirrorless cameras'),
  (1003,'Lenses',2,10,'Photo & Video > Lenses'),
  (1004,'Action cameras',2,10,'Photo & Video > Action cameras'),
  (1005,'Drones',2,10,'Photo & Video > Drones');

-- ─── dim_product (products catalog: brand, category, subcategory, price) ─────
CREATE OR REPLACE TABLE mart.dim_product (
  product_id    INT64 NOT NULL,
  product_name  STRING NOT NULL,
  brand_id      INT64 NOT NULL,
  category_id   INT64 NOT NULL,
  subcategory_id INT64 NOT NULL,
  price_pln     NUMERIC(10,2) NOT NULL,
  launch_year   INT64,
  premium_flag  BOOL NOT NULL
);
-- dim_product: 1200 prodotti. Genera con: python scripts/generate_seed_data.py
-- Lo script run_bigquery_schema.py esegue dim_product_generated.sql dopo CREATE TABLE

-- ─── dim_date (2023-2026) – full retail electronics promo calendar ───────────
CREATE OR REPLACE TABLE mart.dim_date AS
SELECT
  FORMAT_DATE('%Y%m%d', d) AS date_key, d AS date,
  EXTRACT(WEEK FROM d) AS week, EXTRACT(MONTH FROM d) AS month,
  EXTRACT(QUARTER FROM d) AS quarter, EXTRACT(YEAR FROM d) AS year,
  EXTRACT(DAYOFWEEK FROM d) AS day_of_week,
  (EXTRACT(MONTH FROM d) = 11 AND EXTRACT(DAY FROM d) >= 22) AS is_black_friday_week,
  (EXTRACT(MONTH FROM d) = 12) AS is_xmas_period,
  (EXTRACT(MONTH FROM d) = 8 OR (EXTRACT(MONTH FROM d) = 9 AND EXTRACT(DAY FROM d) <= 15)) AS is_back_to_school,
  CASE
    WHEN EXTRACT(MONTH FROM d) = 12 AND EXTRACT(DAY FROM d) <= 2 AND EXTRACT(DAYOFWEEK FROM d) = 2 THEN 'Cyber Monday'
    WHEN EXTRACT(MONTH FROM d) = 11 AND EXTRACT(DAY FROM d) >= 25 AND EXTRACT(DAYOFWEEK FROM d) = 2 THEN 'Cyber Monday'
    WHEN EXTRACT(MONTH FROM d) = 11 AND EXTRACT(DAY FROM d) = 11 THEN 'Singles Day'
    WHEN EXTRACT(MONTH FROM d) = 11 AND EXTRACT(DAY FROM d) >= 22 THEN 'Black Friday'
    WHEN EXTRACT(MONTH FROM d) = 12 THEN 'Christmas'
    WHEN EXTRACT(MONTH FROM d) = 1 AND EXTRACT(DAY FROM d) <= 15 THEN 'New Year Sales'
    WHEN EXTRACT(MONTH FROM d) = 2 AND EXTRACT(DAY FROM d) <= 14 THEN 'Valentines Day'
    WHEN EXTRACT(MONTH FROM d) = 1 AND EXTRACT(DAY FROM d) >= 16 THEN 'Winter Sales'
    WHEN EXTRACT(MONTH FROM d) = 2 AND EXTRACT(DAY FROM d) >= 15 THEN 'Winter Sales'
    WHEN (EXTRACT(MONTH FROM d) = 3 AND EXTRACT(DAY FROM d) >= 25) OR (EXTRACT(MONTH FROM d) = 4 AND EXTRACT(DAY FROM d) <= 25) THEN 'Easter'
    WHEN EXTRACT(MONTH FROM d) = 3 OR EXTRACT(MONTH FROM d) = 4 THEN 'Spring Cleaning'
    WHEN EXTRACT(MONTH FROM d) = 5 AND EXTRACT(DAY FROM d) <= 5 THEN 'May Holiday'
    WHEN EXTRACT(MONTH FROM d) = 6 OR EXTRACT(MONTH FROM d) = 7 THEN 'Summer Sales'
    WHEN EXTRACT(MONTH FROM d) = 8 OR (EXTRACT(MONTH FROM d) = 9 AND EXTRACT(DAY FROM d) <= 15) THEN 'Back to School'
    WHEN EXTRACT(MONTH FROM d) = 9 AND EXTRACT(DAY FROM d) >= 16 THEN 'Tech Launch'
    ELSE 'Regular'
  END AS peak_event
FROM UNNEST(GENERATE_DATE_ARRAY('2023-01-01', '2026-12-31')) AS d;

-- ─── dim_promo (10 types: discounts, bundle, flash, seasonal, etc.) ───────────
CREATE OR REPLACE TABLE mart.dim_promo (
  promo_id INT64 NOT NULL, promo_name STRING NOT NULL, promo_type STRING NOT NULL,
  promo_mechanic STRING, funding_type STRING, start_date DATE, end_date DATE
);
INSERT mart.dim_promo (promo_id, promo_name, promo_type, promo_mechanic, funding_type, start_date, end_date) VALUES
  (1,'Rabat -10%','percentage_discount','Rabat liniowy 10%','retailer',DATE('2023-01-01'),DATE('2026-12-31')),
  (2,'Rabat -20%','percentage_discount','Rabat liniowy 20%','retailer',DATE('2023-01-01'),DATE('2026-12-31')),
  (3,'Rabat -30%','percentage_discount','Rabat liniowy 30%','joint',DATE('2023-01-01'),DATE('2026-12-31')),
  (4,'2+1 Gratis','bundle','Kup 2 + 1 gratis','brand',DATE('2023-01-01'),DATE('2026-12-31')),
  (5,'Cashback 15%','cashback','Zwrot 15% po zakupie','brand',DATE('2023-01-01'),DATE('2026-12-31')),
  (6,'Hit Dnia','flash_sale','Oferta flash 24h','retailer',DATE('2023-01-01'),DATE('2026-12-31')),
  (7,'Tylko w App','app_only','Znizka tylko w app','retailer',DATE('2024-01-01'),DATE('2026-12-31')),
  (8,'Drugi za 1 PLN','bundle','Drugi produkt za 1 PLN','joint',DATE('2023-01-01'),DATE('2026-12-31')),
  (9,'Black Friday','seasonal','Saldi Black Friday','retailer',DATE('2023-11-22'),DATE('2026-11-30')),
  (10,'Swieta','seasonal','Offerte natalizie','retailer',DATE('2023-12-01'),DATE('2026-12-31'));

-- ─── dim_segment (6 segmenti HCG – MediaWorld) ──────────────────────────────
CREATE OR REPLACE TABLE mart.dim_segment (
  segment_id INT64 NOT NULL, segment_name STRING NOT NULL,
  segment_description STRING, age_range STRING, income_level STRING,
  gender_skew STRING, top_driver STRING
);
INSERT mart.dim_segment (segment_id, segment_name, segment_description, age_range, income_level, gender_skew, top_driver) VALUES
  (1,'Liberals','Wellness, knowledge, sustainability, quality of life','45-64','high','57% male','wellness'),
  (2,'Optimistic Doers','Status, image, optimism, work-life balance (ex-Balancers HCG)','35-54','high','balanced','status'),
  (3,'Go-Getters','Performance, productivity, career oriented','25-44','very_high','balanced','performance'),
  (4,'Outcasts','Entertainment, escapism, price sensitive, young','18-24','low','58% male','entertainment'),
  (5,'Contributors','Family, community, practicality, home management','45-54','low','70% female','family'),
  (6,'Floaters','Necessity, stability, low openness to change','45-54','low','balanced','necessity');

-- ─── dim_customer (buyers) – identità, canali, loyalty, demografia, segment_id ─
CREATE OR REPLACE TABLE mart.dim_customer (
  customer_id          INT64 NOT NULL,
  global_user_id       STRING,
  is_registered        BOOL NOT NULL,
  registration_date    DATE,
  first_purchase_date  DATE,
  last_purchase_date   DATE,
  customer_status     STRING NOT NULL,
  has_app              BOOL NOT NULL,
  app_first_seen_date  DATE,
  app_last_seen_date   DATE,
  has_website_account  BOOL NOT NULL,
  preferred_channel    STRING NOT NULL,
  omnichannel_flag     BOOL NOT NULL,
  has_loyalty_card     BOOL NOT NULL,
  loyalty_tier         STRING NOT NULL,
  loyalty_join_date    DATE,
  gender               STRING NOT NULL,
  age_band             STRING,
  birth_year           INT64,
  city                 STRING,
  region               STRING,
  urbanicity           STRING,
  income_band          STRING,
  segment_id           INT64 NOT NULL,
  segment_confidence   FLOAT64,
  marketing_optin_email BOOL,
  marketing_optin_push BOOL,
  marketing_optin_sms  BOOL
);

INSERT mart.dim_customer (
  customer_id, global_user_id, is_registered, registration_date, first_purchase_date, last_purchase_date,
  customer_status, has_app, app_first_seen_date, app_last_seen_date, has_website_account, preferred_channel,
  omnichannel_flag, has_loyalty_card, loyalty_tier, loyalty_join_date, gender, age_band, birth_year,
  city, region, urbanicity, income_band, segment_id, segment_confidence,
  marketing_optin_email, marketing_optin_push, marketing_optin_sms
)
WITH
-- segment_behavior: 1=Liberals 2=Optimistic Doers 3=Go-Getters 4=Outcasts 5=Contributors 6=Floaters
-- channel_pref: 1=web 2=app 3=store. Outcasts: app/web. Contributors: store+web. Go-Getters: omnichannel. Floaters: store
seg_behavior AS (
  SELECT 1 AS seg, 0.35 AS promo_sens, 1 AS ch_web, 1 AS ch_app, 0 AS ch_store, 0.65 AS loyalty_prob, 0.5 AS prem, 'high' AS inc, 0.12 AS churn UNION ALL
  SELECT 2, 0.42, 1, 1, 0, 0.55, 0.7, 'high', 0.10 UNION ALL
  SELECT 3, 0.28, 1, 1, 1, 0.70, 0.75, 'high', 0.05 UNION ALL
  SELECT 4, 0.58, 1, 1, 0, 0.25, 0.2, 'low', 0.28 UNION ALL
  SELECT 5, 0.48, 1, 0, 1, 0.80, 0.35, 'low', 0.08 UNION ALL
  SELECT 6, 0.52, 0, 0, 1, 0.45, 0.25, 'low', 0.15
),
ids AS (SELECT i AS customer_id FROM UNNEST(GENERATE_ARRAY(1, 24000)) AS i),
/* Segmenti: Liberals / Optimistic Doers / Go-Getters = premium & value; Outcasts/Contributors/Floaters = volume & promo. */
seg_assign AS (
  SELECT id.customer_id, sb.seg AS segment_id, sb.promo_sens, sb.ch_web, sb.ch_app, sb.ch_store, sb.loyalty_prob, sb.prem, sb.inc, sb.churn
  FROM ids id
  JOIN seg_behavior sb ON sb.seg = CASE
    WHEN id.customer_id <= 3000 THEN 1
    WHEN id.customer_id <= 6500 THEN 2
    WHEN id.customer_id <= 11800 THEN 3
    WHEN id.customer_id <= 18800 THEN 4
    WHEN id.customer_id <= 22200 THEN 5
    ELSE 6
  END
),
gen AS (
  SELECT
    sa.customer_id,
    'uid_' || sa.customer_id AS global_user_id,
    MOD(ABS(FARM_FINGERPRINT(CAST(sa.customer_id AS STRING))), 100) < 78 AS is_registered,
    DATE_ADD(DATE('2020-01-01'), INTERVAL MOD(ABS(FARM_FINGERPRINT(CAST(sa.customer_id AS STRING))), 1400) DAY) AS registration_date,
    DATE_ADD(DATE('2022-01-01'), INTERVAL MOD(ABS(FARM_FINGERPRINT(CONCAT('fp', CAST(sa.customer_id AS STRING)))), 400) DAY) AS first_purchase_date,
    DATE_ADD(DATE('2023-06-01'), INTERVAL MOD(ABS(FARM_FINGERPRINT(CONCAT('lp', CAST(sa.customer_id AS STRING)))), 900) DAY) AS last_purchase_date,
    CASE WHEN MOD(ABS(FARM_FINGERPRINT(CONCAT('st', CAST(sa.customer_id AS STRING)))), 100) < (100 - CAST(sa.churn * 100 AS INT64)) THEN 'active'
         WHEN MOD(ABS(FARM_FINGERPRINT(CONCAT('st', CAST(sa.customer_id AS STRING)))), 100) < 92 THEN 'dormant' ELSE 'churned' END AS customer_status,
    (sa.ch_app = 1 AND MOD(ABS(FARM_FINGERPRINT(CONCAT('app', CAST(sa.customer_id AS STRING)))), 100) < 72)
      OR (sa.ch_app = 0 AND MOD(ABS(FARM_FINGERPRINT(CONCAT('app', CAST(sa.customer_id AS STRING)))), 100) < 45) AS has_app,
    DATE_ADD(DATE('2022-01-01'), INTERVAL MOD(sa.customer_id, 500) DAY) AS app_first_seen_date,
    DATE_ADD(DATE('2024-01-01'), INTERVAL MOD(sa.customer_id, 400) DAY) AS app_last_seen_date,
    MOD(ABS(FARM_FINGERPRINT(CAST(sa.customer_id AS STRING))), 100) < 75 AS has_website_account,
    CASE
      WHEN sa.ch_web = 1 AND sa.ch_app = 1 AND sa.ch_store = 1 THEN CASE MOD(ABS(FARM_FINGERPRINT(CONCAT('ch', CAST(sa.customer_id AS STRING)))), 3) WHEN 0 THEN 'web' WHEN 1 THEN 'app' ELSE 'store' END
      WHEN sa.ch_store = 1 AND sa.ch_web = 1 THEN CASE MOD(ABS(FARM_FINGERPRINT(CONCAT('ch', CAST(sa.customer_id AS STRING)))), 2) WHEN 0 THEN 'store' ELSE 'web' END
      WHEN sa.ch_app = 1 AND sa.ch_web = 1 THEN CASE MOD(ABS(FARM_FINGERPRINT(CONCAT('ch', CAST(sa.customer_id AS STRING)))), 2) WHEN 0 THEN 'app' ELSE 'web' END
      WHEN sa.ch_store = 1 THEN 'store'
      ELSE CASE MOD(ABS(FARM_FINGERPRINT(CONCAT('ch', CAST(sa.customer_id AS STRING)))), 2) WHEN 0 THEN 'app' ELSE 'web' END
    END AS preferred_channel,
    (sa.segment_id = 3) OR (MOD(sa.customer_id, 7) = 0) AS omnichannel_flag,
    MOD(ABS(FARM_FINGERPRINT(CONCAT('ly', CAST(sa.customer_id AS STRING)))), 100) < CAST(sa.loyalty_prob * 100 AS INT64) AS has_loyalty_card,
    CASE WHEN MOD(ABS(FARM_FINGERPRINT(CONCAT('ly', CAST(sa.customer_id AS STRING)))), 100) >= CAST(sa.loyalty_prob * 100 AS INT64) THEN 'none'
         WHEN MOD(ABS(FARM_FINGERPRINT(CONCAT('tier', CAST(sa.customer_id AS STRING)))), 10) < 2 THEN 'gold'
         WHEN MOD(ABS(FARM_FINGERPRINT(CONCAT('tier', CAST(sa.customer_id AS STRING)))), 10) < 5 THEN 'silver'
         ELSE 'basic' END AS loyalty_tier,
    DATE_ADD(DATE('2021-01-01'), INTERVAL MOD(sa.customer_id, 1200) DAY) AS loyalty_join_date,
    CASE sa.segment_id
      WHEN 1 THEN CASE WHEN MOD(sa.customer_id, 100) < 57 THEN 'male' ELSE 'female' END
      WHEN 5 THEN CASE WHEN MOD(sa.customer_id, 100) < 25 THEN 'male' ELSE 'female' END
      ELSE CASE MOD(sa.customer_id, 2) WHEN 0 THEN 'male' ELSE 'female' END
    END AS gender,
    CASE sa.segment_id
      WHEN 4 THEN '18-24'
      WHEN 3 THEN CASE MOD(sa.customer_id, 3) WHEN 0 THEN '25-34' WHEN 1 THEN '35-44' ELSE '25-34' END
      WHEN 1 THEN CASE MOD(sa.customer_id, 2) WHEN 0 THEN '45-54' ELSE '55-64' END
      WHEN 5 THEN CASE MOD(sa.customer_id, 2) WHEN 0 THEN '45-54' ELSE '35-44' END
      WHEN 6 THEN CASE MOD(sa.customer_id, 2) WHEN 0 THEN '45-54' ELSE '55-64' END
      ELSE CASE MOD(sa.customer_id, 5) WHEN 0 THEN '18-24' WHEN 1 THEN '25-34' WHEN 2 THEN '35-44' WHEN 3 THEN '45-54' ELSE '55-64' END
    END AS age_band,
    CASE sa.inc WHEN 'high' THEN 1985 - MOD(sa.customer_id, 25) WHEN 'low' THEN 1975 + MOD(sa.customer_id, 30) ELSE 1980 + MOD(sa.customer_id, 25) END AS birth_year,
    CONCAT('City_', MOD(sa.customer_id, 50)) AS city,
    CASE MOD(sa.customer_id, 5) WHEN 0 THEN 'Mazowieckie' WHEN 1 THEN 'Malopolskie' WHEN 2 THEN 'Slaskie' WHEN 3 THEN 'Wielkopolskie' ELSE 'Other' END AS region,
    CASE MOD(sa.customer_id, 3) WHEN 0 THEN 'urban' WHEN 1 THEN 'suburban' ELSE 'rural' END AS urbanicity,
    sa.inc AS income_band,
    sa.segment_id,
    0.72 + (MOD(ABS(FARM_FINGERPRINT(CAST(sa.customer_id AS STRING))), 28) / 100.0) AS segment_confidence,
    MOD(sa.customer_id, 2) = 0 AS marketing_optin_email,
    (sa.segment_id IN (3, 4)) OR (MOD(sa.customer_id, 3) = 0) AS marketing_optin_push,
    MOD(sa.customer_id, 5) = 0 AS marketing_optin_sms
  FROM seg_assign sa
)
SELECT
  customer_id, global_user_id, is_registered, registration_date, first_purchase_date, last_purchase_date,
  customer_status, has_app, app_first_seen_date, app_last_seen_date, has_website_account, preferred_channel,
  omnichannel_flag, has_loyalty_card, loyalty_tier, loyalty_join_date, gender, age_band, birth_year,
  city, region, urbanicity, income_band, segment_id, segment_confidence,
  marketing_optin_email, marketing_optin_push, marketing_optin_sms
FROM gen;

-- fact_sales_daily e fact_promo_performance: derivati da derive_sales_from_orders.sql

-- ─── fact_orders (date × customer) – buyer analytics, canale, loyalty, segment ─
-- Partitioning + clustering per ridurre bytes scanned e latenza query
-- DROP: BQ non consente OR REPLACE se cambia PARTITION/CLUSTER rispetto alla tabella esistente
DROP TABLE IF EXISTS mart.fact_orders;

CREATE TABLE mart.fact_orders (
  order_id           INT64 NOT NULL,
  date               DATE NOT NULL,
  customer_id        INT64 NOT NULL,
  channel            STRING NOT NULL,
  gross_pln          NUMERIC(14,2) NOT NULL,
  net_pln            NUMERIC(14,2) NOT NULL,
  units              INT64 NOT NULL,
  promo_flag         BOOL NOT NULL,
  promo_id           INT64,
  discount_depth_pct NUMERIC(5,1)
)
PARTITION BY date
CLUSTER BY channel, customer_id;

INSERT mart.fact_orders (order_id, date, customer_id, channel, gross_pln, net_pln, units, promo_flag, promo_id, discount_depth_pct)
WITH
promo_mech AS (
  SELECT 1 AS pid, 10.0 AS ddp UNION ALL SELECT 2, 20.0 UNION ALL SELECT 3, 30.0 UNION ALL SELECT 4, 15.0 UNION ALL SELECT 5, 15.0 UNION ALL
  SELECT 6, 12.0 UNION ALL SELECT 7, 8.0 UNION ALL SELECT 8, 18.0 UNION ALL SELECT 9, 25.0 UNION ALL SELECT 10, 20.0
),
nums AS (SELECT i FROM UNNEST(GENERATE_ARRAY(1, 380000)) AS i),
order_dates AS (
  SELECT i, DATE_ADD(DATE('2023-01-01'), INTERVAL MOD(ABS(FARM_FINGERPRINT(CAST(i AS STRING))), 1461) DAY) AS dt
  FROM nums
),
order_cust AS (
  SELECT od.i, od.dt, 1 + MOD(ABS(FARM_FINGERPRINT(CONCAT('c', CAST(od.i AS STRING)))), 24000) AS cust_id,
    c.preferred_channel AS ch_pref, c.segment_id
  FROM order_dates od
  JOIN mart.dim_customer c ON c.customer_id = 1 + MOD(ABS(FARM_FINGERPRINT(CONCAT('c', CAST(od.i AS STRING)))), 24000)
),
order_peak AS (
  SELECT oc.i, oc.dt, oc.cust_id, oc.ch_pref, oc.segment_id, d.peak_event
  FROM order_cust oc
  JOIN mart.dim_date d ON d.date = oc.dt
),
gen AS (
  SELECT
    op.i AS order_id,
    op.dt AS date,
    op.cust_id AS customer_id,
    CASE WHEN MOD(ABS(FARM_FINGERPRINT(CONCAT('ch', CAST(op.i AS STRING)))), 100) < 78 THEN op.ch_pref
         WHEN MOD(ABS(FARM_FINGERPRINT(CONCAT('ch', CAST(op.i AS STRING)))), 3) = 0 THEN 'web'
         WHEN MOD(ABS(FARM_FINGERPRINT(CONCAT('ch', CAST(op.i AS STRING)))), 3) = 1 THEN 'app' ELSE 'store' END AS channel,
    ROUND(150 + (MOD(ABS(FARM_FINGERPRINT(CAST(op.i AS STRING))), 6000) / 10.0), 2) AS gross_pln,
    1 + MOD(ABS(FARM_FINGERPRINT(CONCAT('u', CAST(op.i AS STRING)))), 4) AS units,
    /* Promo: soglia da promo_sens (seg_behavior) + picchi + bias cliente [-7,+7]; niente 72% vs 14% fissi */
    (
      MOD(ABS(FARM_FINGERPRINT(CONCAT('p', CAST(op.i AS STRING)))), 100)
      < LEAST(87, GREATEST(22,
        CAST(ROUND(100 * (
          CASE op.segment_id
            WHEN 1 THEN 0.35 WHEN 2 THEN 0.42 WHEN 3 THEN 0.28
            WHEN 4 THEN 0.58 WHEN 5 THEN 0.48 ELSE 0.52
          END * 0.58 + 0.17
        )) AS INT64)
        + CASE
            WHEN op.peak_event IN ('Black Friday','Christmas','Cyber Monday','New Year Sales') THEN 15
            WHEN op.peak_event IN ('Back to School','Summer Sales','Winter Sales','Spring Cleaning') THEN 10
            ELSE 0
          END
        + MOD(ABS(FARM_FINGERPRINT(CONCAT('prcust', CAST(op.cust_id AS STRING)))), 15) - 7
      ))
    ) AS promo_flag,
    CASE
      WHEN op.peak_event = 'Black Friday' THEN 9
      WHEN op.peak_event IN ('Christmas','New Year Sales') THEN 10
      WHEN op.peak_event = 'Back to School' THEN 2
      WHEN op.peak_event = 'Summer Sales' THEN 6
      WHEN op.peak_event = 'Tech Launch' THEN 7
      ELSE 1 + MOD(op.i, 10)
    END AS promo_id_cand,
    op.segment_id,
    op.peak_event
  FROM order_peak op
)
SELECT gen.order_id, gen.date, gen.customer_id, gen.channel,
  CAST(gen.gross_pln AS NUMERIC), CAST(ROUND(gen.gross_pln/1.23, 2) AS NUMERIC),
  gen.units,
  gen.promo_flag,
  IF(gen.promo_flag, LEAST(gen.promo_id_cand, 10), NULL) AS promo_id,
  CAST(IF(gen.promo_flag,
    LEAST(28, GREATEST(6,
      11.0 + MOD(ABS(FARM_FINGERPRINT(CONCAT('dd', CAST(gen.order_id AS STRING)))), 12)
      + CASE gen.segment_id
          WHEN 4 THEN 4.0 WHEN 5 THEN 3.0 WHEN 6 THEN 3.5
          WHEN 1 THEN -1.5 WHEN 2 THEN -1.0 WHEN 3 THEN -0.5
          ELSE 0.0
        END
    )),
  NULL) AS NUMERIC) AS discount_depth_pct
FROM gen;

-- ─── product_pool_seg_channel_gender – pool prodotti preferiti per (segment, channel, gender) ─
CREATE OR REPLACE TABLE mart.product_pool_seg_channel_gender AS
WITH
seg_pref AS (
  SELECT 1 AS segment_id, 8 AS parent_category_id
  UNION ALL SELECT 1, 9 UNION ALL SELECT 1, 3 UNION ALL SELECT 1, 2 UNION ALL SELECT 1, 6
  UNION ALL SELECT 1, 1 UNION ALL SELECT 1, 7
  UNION ALL SELECT 2, 1 UNION ALL SELECT 2, 2 UNION ALL SELECT 2, 7 UNION ALL SELECT 2, 3 UNION ALL SELECT 2, 6 UNION ALL SELECT 2, 8
  UNION ALL SELECT 2, 5 UNION ALL SELECT 2, 9
  UNION ALL SELECT 3, 3 UNION ALL SELECT 3, 2 UNION ALL SELECT 3, 7 UNION ALL SELECT 3, 1 UNION ALL SELECT 3, 6 UNION ALL SELECT 3, 8
  UNION ALL SELECT 3, 4 UNION ALL SELECT 3, 9
  UNION ALL SELECT 4, 2 UNION ALL SELECT 4, 7 UNION ALL SELECT 4, 4 UNION ALL SELECT 4, 1 UNION ALL SELECT 4, 8 UNION ALL SELECT 4, 6
  UNION ALL SELECT 4, 3 UNION ALL SELECT 4, 5
  UNION ALL SELECT 5, 5 UNION ALL SELECT 5, 6 UNION ALL SELECT 5, 1 UNION ALL SELECT 5, 2 UNION ALL SELECT 5, 9 UNION ALL SELECT 5, 8
  UNION ALL SELECT 6, 5 UNION ALL SELECT 6, 6 UNION ALL SELECT 6, 1 UNION ALL SELECT 6, 2 UNION ALL SELECT 6, 9 UNION ALL SELECT 6, 8
  UNION ALL SELECT 6, 7
),
ch_pref AS (
  SELECT 'store' AS channel, 5 AS parent_category_id
  UNION ALL SELECT 'store', 1 UNION ALL SELECT 'store', 6 UNION ALL SELECT 'store', 2 UNION ALL SELECT 'store', 8
  UNION ALL SELECT 'app', 2 UNION ALL SELECT 'app', 4 UNION ALL SELECT 'app', 7 UNION ALL SELECT 'app', 3 UNION ALL SELECT 'app', 8
  UNION ALL SELECT 'web', 3 UNION ALL SELECT 'web', 4 UNION ALL SELECT 'web', 10 UNION ALL SELECT 'web', 2 UNION ALL SELECT 'web', 6 UNION ALL SELECT 'web', 8
),
gender_pref AS (
  SELECT 'male' AS gender, 3 AS parent_category_id
  UNION ALL SELECT 'male', 2 UNION ALL SELECT 'male', 1 UNION ALL SELECT 'male', 5 UNION ALL SELECT 'male', 7 UNION ALL SELECT 'male', 8
  UNION ALL SELECT 'female', 6 UNION ALL SELECT 'female', 9 UNION ALL SELECT 'female', 5 UNION ALL SELECT 'female', 2 UNION ALL SELECT 'female', 1 UNION ALL SELECT 'female', 8
),
combos AS (
  SELECT seg.segment_id, ch.channel, g.gender
  FROM (SELECT 1 AS segment_id UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6) seg
  CROSS JOIN (SELECT 'store' AS channel UNION ALL SELECT 'app' UNION ALL SELECT 'web') ch
  CROSS JOIN (SELECT 'male' AS gender UNION ALL SELECT 'female') g
),
inter_pref AS (
  SELECT c.segment_id, c.channel, c.gender, sp.parent_category_id
  FROM combos c
  JOIN seg_pref sp ON sp.segment_id = c.segment_id
  JOIN ch_pref cp ON cp.channel = c.channel AND cp.parent_category_id = sp.parent_category_id
  JOIN gender_pref gp ON gp.gender = c.gender AND gp.parent_category_id = sp.parent_category_id
),
combos_with_inter AS (
  SELECT DISTINCT segment_id, channel, gender FROM inter_pref
),
union_pref AS (
  SELECT c.segment_id, c.channel, c.gender, sp.parent_category_id
  FROM combos c
  JOIN seg_pref sp ON sp.segment_id = c.segment_id
  UNION DISTINCT
  SELECT c.segment_id, c.channel, c.gender, cp.parent_category_id
  FROM combos c
  JOIN ch_pref cp ON cp.channel = c.channel
  UNION DISTINCT
  SELECT c.segment_id, c.channel, c.gender, gp.parent_category_id
  FROM combos c
  JOIN gender_pref gp ON gp.gender = c.gender
),
all_pref AS (
  SELECT segment_id, channel, gender, parent_category_id FROM inter_pref
  UNION ALL
  SELECT up.segment_id, up.channel, up.gender, up.parent_category_id
  FROM union_pref up
  WHERE NOT EXISTS (SELECT 1 FROM combos_with_inter cw
    WHERE cw.segment_id = up.segment_id AND cw.channel = up.channel AND cw.gender = up.gender)
),
pool AS (
  SELECT segment_id, channel, gender, product_id,
    ROW_NUMBER() OVER (PARTITION BY segment_id, channel, gender ORDER BY ord_k) - 1 AS ix
  FROM (
    /* Volume segmenti (4–6): escluso premium dal blocco base; premium rientrano da UNION flagship/TV sotto */
    SELECT ap.segment_id, ap.channel, ap.gender, p.product_id,
      MOD(ABS(FARM_FINGERPRINT(CONCAT(CAST(ap.segment_id AS STRING), '|', CAST(p.product_id AS STRING), '|', CAST(p.subcategory_id AS STRING)))), 1000003) AS ord_k
    FROM all_pref ap
    JOIN mart.dim_product p ON p.category_id = ap.parent_category_id
    WHERE NOT (ap.segment_id IN (4, 5, 6) AND p.premium_flag)
    UNION ALL
    /* Segmenti 1–3: sovrappeso forte ai premium nel mix cestino */
    SELECT ap.segment_id, ap.channel, ap.gender, p.product_id,
      MOD(ABS(FARM_FINGERPRINT(CONCAT(CAST(ap.segment_id AS STRING), '|', CAST(p.product_id AS STRING), '|', CAST(p.subcategory_id AS STRING), '|', CAST(dup AS STRING)))), 1000003) AS ord_k
    FROM all_pref ap
    JOIN mart.dim_product p ON p.category_id = ap.parent_category_id
    CROSS JOIN UNNEST(GENERATE_ARRAY(1, 10)) AS dup
    WHERE ap.segment_id IN (1, 2, 3) AND p.premium_flag
    UNION ALL
    /* Smartphone/foldable premium: peso extra per segmenti 1–3 (Liberals, Doers, Go-Getters) */
    SELECT ap.segment_id, ap.channel, ap.gender, p.product_id,
      MOD(ABS(FARM_FINGERPRINT(CONCAT('flag', CAST(ap.segment_id AS STRING), '|', CAST(p.product_id AS STRING), '|', CAST(dup AS STRING)))), 1000003) AS ord_k
    FROM all_pref ap
    JOIN mart.dim_product p ON p.category_id = ap.parent_category_id
    CROSS JOIN UNNEST(GENERATE_ARRAY(1, 12)) AS dup
    WHERE ap.segment_id IN (1, 2, 3) AND p.premium_flag AND p.subcategory_id IN (201, 202, 204)
    UNION ALL
    /* Flagship/foldable: peso aumentato su 4–6 (cross-segment su SKU premium mobile) */
    SELECT ap.segment_id, ap.channel, ap.gender, p.product_id,
      MOD(ABS(FARM_FINGERPRINT(CONCAT('fp46', CAST(ap.segment_id AS STRING), '|', CAST(p.product_id AS STRING), '|', CAST(dup AS STRING)))), 1000003) AS ord_k
    FROM all_pref ap
    JOIN mart.dim_product p ON p.category_id = ap.parent_category_id
    CROSS JOIN UNNEST(GENERATE_ARRAY(1, 12)) AS dup
    WHERE ap.segment_id IN (4, 5, 6) AND p.premium_flag AND p.subcategory_id IN (201, 202, 204)
    UNION ALL
    /* TV premium (OLED/QLED/soundbar): copertura 4–6 oltre solo smartphone */
    SELECT ap.segment_id, ap.channel, ap.gender, p.product_id,
      MOD(ABS(FARM_FINGERPRINT(CONCAT('tvp46', CAST(ap.segment_id AS STRING), '|', CAST(p.product_id AS STRING), '|', CAST(dup AS STRING)))), 1000003) AS ord_k
    FROM all_pref ap
    JOIN mart.dim_product p ON p.category_id = ap.parent_category_id
    CROSS JOIN UNNEST(GENERATE_ARRAY(1, 10)) AS dup
    WHERE ap.segment_id IN (4, 5, 6) AND p.premium_flag AND p.subcategory_id IN (102, 103, 105)
  )
)
SELECT segment_id, channel, gender, product_id, ix FROM pool;

-- ─── fact_order_items (order × product) – basket mix differenziato per segment/channel/gender ─
-- Clustering per join con fact_orders e lookup prodotto
DROP TABLE IF EXISTS mart.fact_order_items;

CREATE TABLE mart.fact_order_items (
  order_id   INT64 NOT NULL,
  product_id INT64 NOT NULL,
  quantity   INT64 NOT NULL,
  gross_pln   NUMERIC(14,2) NOT NULL
)
CLUSTER BY order_id, product_id;

INSERT mart.fact_order_items (order_id, product_id, quantity, gross_pln)
WITH
order_enriched AS (
  SELECT o.order_id, o.channel, c.segment_id, c.gender
  FROM mart.fact_orders o
  JOIN mart.dim_customer c ON c.customer_id = o.customer_id
),
pool_agg AS (
  SELECT segment_id, channel, gender,
    ARRAY_AGG(product_id ORDER BY ix) AS products
  FROM mart.product_pool_seg_channel_gender
  GROUP BY segment_id, channel, gender
),
order_lines AS (
  SELECT oe.order_id, oe.segment_id, oe.channel, oe.gender, l AS line_num
  FROM order_enriched oe
  CROSS JOIN UNNEST(GENERATE_ARRAY(1, 1 + MOD(oe.order_id, 4))) AS l
),
lines_with_product AS (
  SELECT
    ol.order_id,
    ol.line_num,
    ol.segment_id,
    ol.channel,
    ol.gender,
    FARM_FINGERPRINT(CONCAT(CAST(ol.order_id AS STRING), CAST(ol.line_num AS STRING))) AS h1,
    FARM_FINGERPRINT(CONCAT(CAST(ol.order_id AS STRING), CAST(ol.line_num AS STRING), 'i')) AS h2,
    pa.products
  FROM order_lines ol
  LEFT JOIN pool_agg pa ON pa.segment_id = ol.segment_id AND pa.channel = ol.channel AND pa.gender = ol.gender
),
lines_pick AS (
  SELECT
    lwp.order_id,
    lwp.line_num,
    lwp.segment_id,
    lwp.h1,
    lwp.h2,
    CASE
      WHEN MOD(ABS(lwp.h1), 100) < (82 + MOD(lwp.segment_id * 7, 15)) AND ARRAY_LENGTH(COALESCE(lwp.products, [])) > 0
      THEN lwp.products[SAFE_OFFSET(MOD(ABS(lwp.h2) + lwp.segment_id * 104729 + lwp.line_num * 7919, ARRAY_LENGTH(lwp.products)))]
    END AS pool_product_id
  FROM lines_with_product lwp
),
lines_base AS (
  SELECT
    lp.order_id,
    lp.line_num,
    lp.segment_id,
    lp.h1,
    COALESCE(
      lp.pool_product_id,
      CASE
        WHEN lp.segment_id IN (4, 5, 6) THEN (
          SELECT q.product_id FROM (
            SELECT p.product_id, ROW_NUMBER() OVER (ORDER BY p.product_id) AS rn
            FROM mart.dim_product p
            WHERE NOT p.premium_flag
          ) q
          WHERE q.rn = 1 + MOD(
            ABS(lp.h1) + lp.segment_id * 19,
            (SELECT COUNT(*) FROM mart.dim_product WHERE NOT premium_flag)
          )
        )
        ELSE 10001 + MOD(ABS(lp.h1) + lp.segment_id * 17, 1200)
      END
    ) AS product_id
  FROM lines_pick lp
),
lines AS (
  SELECT
    lb.order_id,
    lb.line_num,
    lb.product_id,
    CASE
      WHEN p.category_id = 5 THEN 1
      WHEN p.category_id = 6 THEN 1 + MOD(ABS(lb.h1), 2)
      WHEN p.category_id = 3 THEN 1 + MOD(ABS(lb.h1), 3)
      WHEN p.category_id = 4 THEN 1 + MOD(ABS(lb.h1), 3)
      ELSE 1 + MOD(ABS(lb.h1), 2)
    END AS quantity
  FROM lines_base lb
  JOIN mart.dim_product p ON p.product_id = lb.product_id
),
/* Segmenti 4–6: premium non azzerato — swap parziale su premium + allowlist OLED/QLED/soundbar/smartphone flagship */
lines_safe_pre AS (
  SELECT
    l.order_id,
    l.line_num,
    l.product_id AS line_product_id,
    lp.segment_id,
    lp.h1,
    dp0.premium_flag,
    dp0.subcategory_id
  FROM lines l
  JOIN lines_pick lp ON lp.order_id = l.order_id AND lp.line_num = l.line_num
  JOIN mart.dim_product dp0 ON dp0.product_id = l.product_id
),
lines_safe_resolved AS (
  SELECT
    lsp.order_id,
    lsp.line_num,
    lsp.h1,
    CASE
      WHEN lsp.premium_flag AND lsp.segment_id IN (4, 5, 6)
        AND NOT (lsp.subcategory_id IN (201, 202, 204, 102, 103, 105))
        AND MOD(ABS(FARM_FINGERPRINT(CONCAT('premswap', CAST(lsp.order_id AS STRING), '|', CAST(lsp.line_num AS STRING)))), 100) < 55 THEN (
        SELECT q.product_id FROM (
          SELECT p2.product_id, ROW_NUMBER() OVER (ORDER BY p2.product_id) AS rn
          FROM mart.dim_product p2
          WHERE NOT p2.premium_flag
        ) q
        WHERE q.rn = 1 + MOD(
          ABS(lsp.h1) + lsp.segment_id * 19 + 7,
          (SELECT COUNT(*) FROM mart.dim_product WHERE NOT premium_flag)
        )
      )
      ELSE lsp.line_product_id
    END AS product_id
  FROM lines_safe_pre lsp
),
lines_safe AS (
  SELECT
    lsr.order_id,
    lsr.line_num,
    lsr.product_id,
    CASE
      WHEN pfin.category_id = 5 THEN 1
      WHEN pfin.category_id = 6 THEN 1 + MOD(ABS(lsr.h1), 2)
      WHEN pfin.category_id = 3 THEN 1 + MOD(ABS(lsr.h1), 3)
      WHEN pfin.category_id = 4 THEN 1 + MOD(ABS(lsr.h1), 3)
      ELSE 1 + MOD(ABS(lsr.h1), 2)
    END AS quantity
  FROM lines_safe_resolved lsr
  JOIN mart.dim_product pfin ON pfin.product_id = lsr.product_id
),
lines_qty AS (
  SELECT
    ls.order_id,
    ls.product_id,
    CAST(GREATEST(1,
      ROUND(CAST(ls.quantity AS FLOAT64) * CASE
        WHEN p.premium_flag AND c.segment_id IN (1, 2, 3) THEN 1.08
        WHEN p.premium_flag AND c.segment_id IN (4, 5, 6) THEN 0.90
        WHEN NOT p.premium_flag AND c.segment_id IN (4, 5, 6) THEN 1.52
        WHEN NOT p.premium_flag AND c.segment_id IN (1, 2, 3) THEN 0.80
        ELSE 1.0
      END)
    ) AS INT64) AS quantity
  FROM lines_safe ls
  JOIN mart.dim_product p ON p.product_id = ls.product_id
  JOIN mart.fact_orders o ON o.order_id = ls.order_id
  JOIN mart.dim_customer c ON c.customer_id = o.customer_id
),
with_price AS (
  SELECT lq.order_id, lq.product_id, lq.quantity,
    ROUND(CAST(p.price_pln AS FLOAT64) * CAST(lq.quantity AS FLOAT64) *
      IF(o.promo_flag AND o.discount_depth_pct IS NOT NULL, 1 - o.discount_depth_pct/100, 1.0) *
      CASE
        WHEN p.premium_flag AND c.segment_id IN (1, 2, 3) THEN 1.22
        WHEN p.premium_flag AND c.segment_id IN (4, 5, 6) THEN 0.62
        ELSE 1.0
      END *
      (0.68 + MOD(ABS(FARM_FINGERPRINT(CONCAT('br', CAST(p.brand_id AS STRING), '|', CAST(p.category_id AS STRING), '|', CAST(p.subcategory_id AS STRING)))), 55) / 100.0) *
      (0.72 + 0.46 * (MOD(ABS(FARM_FINGERPRINT(CONCAT('evt', CAST(p.brand_id AS STRING), '|', d.peak_event))), 1000) / 1000.0))
    , 2) AS gross_pln
  FROM lines_qty lq
  JOIN mart.dim_product p ON p.product_id = lq.product_id
  JOIN mart.fact_orders o ON o.order_id = lq.order_id
  JOIN mart.dim_customer c ON c.customer_id = o.customer_id
  JOIN mart.dim_date d ON d.date = o.date
)
SELECT order_id, product_id, quantity, CAST(gross_pln AS NUMERIC) FROM with_price;
