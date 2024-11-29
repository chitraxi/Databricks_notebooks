# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists  analytics_selfserve.customer_fraud;
# MAGIC create table analytics_selfserve.customer_fraud AS 
# MAGIC   SELECT
# MAGIC     cards.global_fingerprint,
# MAGIC     sum(rf.payments_base_amount) customer_fraud_gmv
# MAGIC   FROM
# MAGIC     warehouse.risk_fraud rf
# MAGIC     JOIN realtime_hudi_api.cards ON rf.payments_card_id = cards.id
# MAGIC     AND rf.frauds_fraud_date BETWEEN '2024-10-25'
# MAGIC     AND '2024-11-25'
# MAGIC     AND rf.frauds_source IN ('Visa', 'MasterCard')
# MAGIC     AND rf.frauds_type = 'cn'
# MAGIC     AND rf.payments_status = 'captured'
# MAGIC     AND rf.payments_international = 1
# MAGIC   GROUP BY
# MAGIC     1

# COMMAND ----------

# MAGIC %sql
# MAGIC create table analytics_selfserve.CB_payments_cards as
# MAGIC
# MAGIC Select CB_payments.* , authn.enrolled,authz.gateway_error_code,coalesce(mfeat.accept_only_3ds_payments, 0) accept_only_3ds_payments, authorization.*, payment_analytics.*, cards.iin, iins.country,merc.billing_label,merc.name merc, merchants.category2 from
# MAGIC (select * from analytics_selfserve.CB_payments  where created_date between '2024-10-25' and '2024-11-25') as pay
# MAGIC JOIN realtime_hudi_api.cards ON pay.card_id = cards.id
# MAGIC
# MAGIC           LEFT JOIN realtime_pgpayments_card_live.authentication authn ON pay.id = authn.payment_id
# MAGIC           AND authn.created_date BETWEEN '2024-10-01'
# MAGIC           AND '2024-11-25'
# MAGIC           LEFT JOIN realtime_pgpayments_card_live.authorization authz ON pay.id = authz.payment_id
# MAGIC           AND authz.created_date BETWEEN '2024-10-01'
# MAGIC           AND '2024-11-25'
# MAGIC           LEFT JOIN realtime_hudi_api.payment_analytics pan ON pay.id = pan.payment_id
# MAGIC           AND pan.created_date BETWEEN '2024-10-01'
# MAGIC           AND '2024-11-25'
# MAGIC           LEFT JOIN aggregate_pa.merchant_features mfeat ON pay.merchant_id = mfeat.entity_id
# MAGIC           LEFT JOIN realtime_hudi_api.iins ON cards.iin = iins.iin
# MAGIC           LEFT JOIN (
# MAGIC             SELECT
# MAGIC               iin,
# MAGIC               array_agg(country) country,
# MAGIC               COUNT(DISTINCT(country)) country_count
# MAGIC             FROM
# MAGIC               realtime_bin_service.ranges bs
# MAGIC             GROUP BY
# MAGIC               1
# MAGIC             HAVING
# MAGIC               COUNT(DISTINCT(country)) = 1
# MAGIC           ) bs ON cards.iin = bs.iin
# MAGIC           LEFT JOIN realtime_hudi_api.merchants merc ON pay.merchant_id = merc.id

# COMMAND ----------

# MAGIC %sql
# MAGIC create table analytics_selfserve.customer_txn
# MAGIC as 
# MAGIC   SELECT
# MAGIC     cards.global_fingerprint,
# MAGIC     merchants_id,
# MAGIC     payments_id,
# MAGIC     payments_base_amount,
# MAGIC     lag(payments_created_date) OVER (
# MAGIC       PARTITION BY cards.global_fingerprint
# MAGIC       ORDER BY
# MAGIC         payments_created_at
# MAGIC     ) prev_date
# MAGIC   FROM
# MAGIC     (select * from warehouse.payments where payments_created_date BETWEEN '2024-10-25'
# MAGIC     AND '2024-10-26' and payments_method = 'card' and payments_international = 1 and (payments_internal_error_code NOT IN (
# MAGIC         'BAD_REQUEST_PAYMENT_CARD_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_LINKS',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_GATEWAY',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_PAGES',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_INVOICES'
# MAGIC       )
# MAGIC       OR payments_internal_error_code IS NULL)) pay
# MAGIC     JOIN (select id, global_fingerprint from realtime_hudi_api.cards) as cards ON pay.cards_id = cards.id

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct prev_date from  analytics_selfserve.customer_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT into analytics_selfserve.customer_txn
# MAGIC   SELECT
# MAGIC     cards_global_fingerprint ,
# MAGIC     p_merchant_id as merchants_id,
# MAGIC     p_id as payments_id,
# MAGIC     p_base_amount as payments_base_amount ,
# MAGIC     lag(p_created_date) OVER (
# MAGIC       PARTITION BY cards.global_fingerprint
# MAGIC       ORDER BY
# MAGIC         p_created_at
# MAGIC     ) prev_date
# MAGIC   FROM
# MAGIC     (select * from whs_v.cards_payments_flat_fact where p_created_date BETWEEN '2024-11-02'
# MAGIC     AND '2024-11-15' and p_method = 'card' and p_international = 1 and (p_internal_error_code NOT IN (
# MAGIC         'BAD_REQUEST_PAYMENT_CARD_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_LINKS',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_GATEWAY',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_PAGES',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_INVOICES'
# MAGIC       )
# MAGIC       OR p_internal_error_code IS NULL)) pay
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC describe whs_v.cards_payments_flat_fact

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT into analytics_selfserve.customer_txn
# MAGIC   SELECT
# MAGIC     cards.global_fingerprint,
# MAGIC     merchants_id,
# MAGIC     payments_id,
# MAGIC     payments_base_amount,
# MAGIC     lag(payments_created_date) OVER (
# MAGIC       PARTITION BY cards.global_fingerprint
# MAGIC       ORDER BY
# MAGIC         payments_created_at
# MAGIC     ) prev_date
# MAGIC   FROM
# MAGIC     (select * from warehouse.payments where payments_created_date BETWEEN '2024-11-16'
# MAGIC     AND '2024-11-25' and payments_method = 'card' and payments_international = 1 and (payments_internal_error_code NOT IN (
# MAGIC         'BAD_REQUEST_PAYMENT_CARD_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_LINKS',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_GATEWAY',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_PAGES',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_INVOICES'
# MAGIC       )
# MAGIC       OR payments_internal_error_code IS NULL)) pay
# MAGIC     JOIN (select id, global_fingerprint from realtime_hudi_api.cards) as cards ON pay.cards_id = cards.id

# COMMAND ----------

# MAGIC %sql
# MAGIC describe warehouse.payments

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_selfserve.customer_fraud
