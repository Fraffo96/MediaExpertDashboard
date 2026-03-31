-- =============================================================================
-- Config CTE per seed e derive – Media Expert Dashboard
-- Riferimento: le CTE sono embeddate in schema_and_seed.sql e derive_sales_from_orders.sql
-- =============================================================================

-- segment_behavior_profile (usato in dim_customer, fact_orders)
-- seg | promo_sens | ch_web | ch_app | ch_store | loyalty_prob | prem | inc  | churn
-- 1   | 0.35      | 1      | 1      | 0        | 0.65         | 0.5  | high | 0.12  Liberals
-- 2   | 0.42      | 1      | 1      | 0        | 0.55         | 0.7  | high | 0.10  Balancers
-- 3   | 0.28      | 1      | 1      | 1        | 0.70         | 0.75 | high | 0.05  Go-Getters
-- 4   | 0.58      | 1      | 1      | 0        | 0.25         | 0.2  | low  | 0.28  Outcasts
-- 5   | 0.48      | 1      | 0      | 1        | 0.80         | 0.35 | low  | 0.08  Contributors
-- 6   | 0.52      | 0      | 0      | 1        | 0.45         | 0.25 | low  | 0.15  Floaters

-- promo_mechanic_profile (usato in fact_orders, derive fact_promo_performance)
-- promo_id | discount_depth_pct | dr (discount rate) | mr (media rate) | br (base ROI)
-- 1-10: ddp 10,20,30,15,15,12,8,18,25,20

-- event_uplift (implicito in fact_orders: peak_event -> promo_id)
-- Black Friday -> 9, Christmas/New Year -> 10, Back to School -> 2, Summer Sales -> 6, Tech Launch -> 7

-- brand_focus_map: vedi scripts/generate_seed_data.py BRAND_FOCUS, PARENT_TO_SUB
-- subcategory_price_profile: vedi scripts/generate_seed_data.py SUBCAT_PRICE
