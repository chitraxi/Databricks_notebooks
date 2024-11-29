# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists analytics_selfserve.CB_payments_cards;
# MAGIC create table analytics_selfserve.CB_payments_cards as
# MAGIC
# MAGIC Select pay.* , authn.enrolled,authz.gateway_error_code as authz_gateway_error_code,coalesce(mfeat.accept_only_3ds_payments, 0) accept_only_3ds_payments, cards.iin, iins.country,merc.billing_label,merc.name as merc_category2,bs.country as bs_country ,CASE
# MAGIC               WHEN pay.base_amount < 300000 THEN '1. under 3k'
# MAGIC               WHEN pay.base_amount < 500000 THEN '2. 3-5k'
# MAGIC               WHEN pay.base_amount < 1000000 THEN '3. 5-10k'
# MAGIC               WHEN pay.base_amount < 2000000 THEN '4. 10-20k'
# MAGIC               ELSE '20k+'
# MAGIC             END as amount_range, global_fingerprint from
# MAGIC (select * from analytics_selfserve.CB_payments  where created_date between '2024-10-25' and '2024-11-01') as pay
# MAGIC JOIN realtime_hudi_api.cards ON pay.card_id = cards.id
# MAGIC
# MAGIC           LEFT JOIN (select * from realtime_pgpayments_card_live.authentication where created_date BETWEEN '2024-10-25' and '2024-11-01') authn ON pay.id = authn.payment_id
# MAGIC           
# MAGIC           LEFT JOIN (select * from realtime_pgpayments_card_live.authorization where created_date BETWEEN '2024-10-25' and '2024-11-01' )authz ON pay.id = authz.payment_id
# MAGIC           
# MAGIC           LEFT JOIN (select * from realtime_hudi_api.payment_analytics where created_date between '2024-10-25' and '2024-11-01') pan ON pay.id = pan.payment_id
# MAGIC           
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
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into analytics_selfserve.CB_payments_cards
# MAGIC Select pay.* , authn.enrolled,authz.gateway_error_code as authz_gateway_error_code,coalesce(mfeat.accept_only_3ds_payments, 0) accept_only_3ds_payments, cards.iin, iins.country,merc.billing_label,merc.name as merc_category2,bs.country as bs_country ,CASE
# MAGIC               WHEN pay.base_amount < 300000 THEN '1. under 3k'
# MAGIC               WHEN pay.base_amount < 500000 THEN '2. 3-5k'
# MAGIC               WHEN pay.base_amount < 1000000 THEN '3. 5-10k'
# MAGIC               WHEN pay.base_amount < 2000000 THEN '4. 10-20k'
# MAGIC               ELSE '20k+'
# MAGIC             END as amount_range from
# MAGIC (select * from analytics_selfserve.CB_payments  where created_date between '2024-10-02' and '2024-11-15') as pay
# MAGIC JOIN realtime_hudi_api.cards ON pay.card_id = cards.id
# MAGIC
# MAGIC           LEFT JOIN (select * from realtime_pgpayments_card_live.authentication where created_date BETWEEN '2024-10-02' and '2024-11-15') authn ON pay.id = authn.payment_id
# MAGIC           
# MAGIC           LEFT JOIN (select * from realtime_pgpayments_card_live.authorization where created_date BETWEEN '2024-10-02' and '2024-11-15' )authz ON pay.id = authz.payment_id
# MAGIC           
# MAGIC           LEFT JOIN (select * from realtime_hudi_api.payment_analytics where created_date between '2024-10-02' and '2024-11-15') pan ON pay.id = pan.payment_id
# MAGIC           
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
# MAGIC describe whs_v.payments_flat_fact

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cards_pay as (
# MAGIC   select id,
# MAGIC     gateway,
# MAGIC     currency,
# MAGIC     authorized_at
# MAGIC   FROM realtime_hudi_api.payments pay
# MAGIC   WHERE created_date between '2024-10-24' and '2024-11-08'
# MAGIC     and method = 'card'
# MAGIC     and international = 1
# MAGIC     and gateway in ('hitachi','fulcrum','checkout_dot_com')
# MAGIC ),
# MAGIC ev as
# MAGIC (
# MAGIC   SELECT event_name,
# MAGIC      replace(get_json_object(properties,'$.payment.id'),'pay_') payment_id
# MAGIC   FROM events.payment_events_v2 
# MAGIC   WHERE producer_created_date between '2024-10-24' and '2024-11-08'
# MAGIC   and lower(event_name) in (
# MAGIC   'payment.creation.processed',
# MAGIC   'payment.authentication.skipped',
# MAGIC   'payment.authentication.initiated',
# MAGIC   'payment.authentication.enrollment.initiated',
# MAGIC   'payment.authentication.enrollment.processed',
# MAGIC   'payment.authentication.processed',
# MAGIC   'payment.authentication.otp.submit.initiated',
# MAGIC   'payment.authentication.otp.resend.initiated',
# MAGIC   'payment.authentication.otp.submit.processed',
# MAGIC   'payment.authentication.otp.resend.processed',
# MAGIC   'payment.authorization.initiated',
# MAGIC   'payment.authorization.processed')
# MAGIC ),
# MAGIC dcc_render as (
# MAGIC select replace(get_json_object(properties,'$.payment_id'),'pay_') payment_id
# MAGIC   FROM events.lumberjack_intermediate
# MAGIC   WHERE producer_created_date between '2024-10-24' and '2024-11-08'
# MAGIC     and event_name = 'acs_page:render'
# MAGIC     and get_json_object(properties,'$.meta.native_currency') is not null
# MAGIC   GROUP BY 1
# MAGIC )
# MAGIC
# MAGIC
# MAGIC select event_name,
# MAGIC   cards_pay.gateway,
# MAGIC   authn.enrolled,
# MAGIC   (case when pm.gateway_currency <> cards_pay.currency then 'DCC'
# MAGIC           when cards_pay.currency <> 'INR' then 'MCC'
# MAGIC           ELSE 'INR' end) currency_type,
# MAGIC   (case when cards_pay.authorized_at is not null then 1 else 0 end)authorized,
# MAGIC   (case when skip_auth.exp_payment = 'exp' then 'Test'
# MAGIC         when skip_auth.exp_payment = 'non-exp' then 'Control'
# MAGIC         else 'Not in Experiment' end) payment_segment,
# MAGIC   (case when authz.id is not null then 1 else 0 end) authorization_entity_created,
# MAGIC   count(distinct(cards_pay.id))attempts
# MAGIC from cards_pay
# MAGIC   INNER JOIN realtime_hudi_api.payment_analytics pan
# MAGIC     ON cards_pay.id = pan.payment_id
# MAGIC     AND pan.created_date between '2024-10-24' and '2024-11-08'
# MAGIC     and pan.library = 3
# MAGIC   
# MAGIC   LEFT JOIN ev
# MAGIC     ON ev.payment_id  = cards_pay.id  
# MAGIC   
# MAGIC   LEFT JOIN realtime_hudi_api.payment_meta pm
# MAGIC     ON cards_pay.id = pm.payment_id
# MAGIC     AND pm.dcc_offered = 1
# MAGIC     AND pm.created_date between '2024-10-24' and '2024-11-08'
# MAGIC   LEFT JOIN realtime_pgpayments_card_live.authentication authn
# MAGIC     ON cards_pay.id = authn.payment_id
# MAGIC     and authn.created_date between '2024-10-24' and '2024-11-08'
# MAGIC   LEFT JOIN realtime_pgpayments_card_live.authorization authz
# MAGIC     ON cards_pay.id = authz.payment_id
# MAGIC     AND authz.created_date between '2024-10-24' and '2024-11-08'
# MAGIC   LEFT JOIN
# MAGIC     (select *
# MAGIC      from
# MAGIC        aggregate_pa.pa_cb_skip_authn_impact_tracking skip_auth
# MAGIC      WHERE iin in ('401795',
# MAGIC       '408565',
# MAGIC       '410039',
# MAGIC       '414709',
# MAGIC       '416598',
# MAGIC       '418887',
# MAGIC       '420767',
# MAGIC       '423953',
# MAGIC       '434769',
# MAGIC       '440066',
# MAGIC       '440393',
# MAGIC       '443913',
# MAGIC       '450553',
# MAGIC       '450644',
# MAGIC       '451014',
# MAGIC       '451992',
# MAGIC       '452034',
# MAGIC       '453600',
# MAGIC       '453734',
# MAGIC       '453735',
# MAGIC       '456835',
# MAGIC       '459693',
# MAGIC       '460031',
# MAGIC       '462239',
# MAGIC       '472409',
# MAGIC       '512230',
# MAGIC       '515676',
# MAGIC       '516361',
# MAGIC       '517805',
# MAGIC       '521267',
# MAGIC       '521600',
# MAGIC       '521729',
# MAGIC       '524204',
# MAGIC       '525363',
# MAGIC       '526408',
# MAGIC       '532655',
# MAGIC       '535316',
# MAGIC       '540978',
# MAGIC       '542418',
# MAGIC       '542933',
# MAGIC       '546068',
# MAGIC       '548171',
# MAGIC       '552191',
# MAGIC       '552433',
# MAGIC       '554576',
# MAGIC       '557652')
# MAGIC        and created_date between '2024-10-24' and '2024-11-08'
# MAGIC        and merchant_id
# MAGIC          IN
# MAGIC          ('BqAd114c65b2gR',
# MAGIC 'CzSXv4EOwnwowy',
# MAGIC 'DfmWzfghGVGbOO',
# MAGIC 'F3fHJMjKTt9z6k',
# MAGIC 'FA0NAlEhpuEjoC',
# MAGIC 'FQDIB7jeu18Cyh',
# MAGIC 'HehaoblB8Wu8el',
# MAGIC 'IwfJ0M8WARteiV',
# MAGIC 'JG0LJ5mF8MvtGG',
# MAGIC 'JmvgkqIF6MrCxB',
# MAGIC 'JUGgDpl81uP79d',
# MAGIC 'NEbqyYAKiPWLW4')
# MAGIC      ) skip_auth
# MAGIC        ON cards_pay.id = skip_auth.payment_id
# MAGIC GROUP BY 1,2,3,4,5,6,7
# MAGIC UNION
# MAGIC SELECT 'Payments Created' as event_name,
# MAGIC   cards_pay.gateway,
# MAGIC   authn.enrolled,
# MAGIC   null currency_type,
# MAGIC   0 authorized,
# MAGIC   null payment_segment,
# MAGIC   0 authorization_entity_created,
# MAGIC   count(distinct(cards_pay.id)) payments
# MAGIC FROM cards_pay
# MAGIC   LEFT JOIN realtime_pgpayments_card_live.authentication authn
# MAGIC     ON cards_pay.id = authn.payment_id
# MAGIC   INNER JOIN realtime_hudi_api.payment_analytics pan
# MAGIC     ON cards_pay.id = pan.payment_id
# MAGIC     AND pan.created_date between '2024-10-24' and '2024-11-08'
# MAGIC     and pan.library = 3
# MAGIC
# MAGIC GROUP BY 1,2,3,4,5,6,7
# MAGIC UNION 
# MAGIC SELECT 'DCC Page Rendered' as event_name,
# MAGIC   cards_pay.gateway,
# MAGIC   null as enrolled,
# MAGIC   'DCC' as currency_type,
# MAGIC   0 authorized,
# MAGIC   null payment_segment,
# MAGIC   0 authorization_entity_created,
# MAGIC   count(distinct(cards_pay.id)) payments
# MAGIC FROM cards_pay
# MAGIC   JOIN dcc_render
# MAGIC     ON cards_pay.id = dcc_render.payment_id
# MAGIC GROUP BY 1,2,3,4,5,6,7
# MAGIC UNION
# MAGIC SELECT 'Payments Updated to Authorized' as event_name,
# MAGIC   cards_pay.gateway,
# MAGIC   authn.enrolled,
# MAGIC   (case when pm.gateway_currency <> cards_pay.currency then 'DCC'
# MAGIC           when cards_pay.currency <> 'INR' then 'MCC'
# MAGIC           ELSE 'INR' end) currency_type,
# MAGIC   (case when cards_pay.authorized_at is not null then 1 else 0 end)authorized,
# MAGIC   (case when skip_auth.exp_payment = 'exp' then 'Test'
# MAGIC         when skip_auth.exp_payment = 'non-exp' then 'Control'
# MAGIC         else 'Not in Experiment' end) payment_segment,
# MAGIC   (case when authz.id is not null then 1 else 0 end) authorization_entity_created,
# MAGIC   count(distinct(cards_pay.id)) payments
# MAGIC FROM cards_pay
# MAGIC   INNER JOIN realtime_hudi_api.payment_analytics pan
# MAGIC     ON cards_pay.id = pan.payment_id
# MAGIC     AND pan.created_date between '2024-10-24' and '2024-11-08'
# MAGIC     and pan.library = 3
# MAGIC     and cards_pay.authorized_at is not null
# MAGIC   LEFT JOIN realtime_pgpayments_card_live.authentication authn
# MAGIC     ON cards_pay.id = authn.payment_id
# MAGIC     and authn.created_date between '2024-10-24' and '2024-11-08'
# MAGIC   LEFT JOIN
# MAGIC     (select *
# MAGIC      from
# MAGIC        aggregate_pa.pa_cb_skip_authn_impact_tracking skip_auth
# MAGIC      WHERE iin in ('401795',
# MAGIC       '408565',
# MAGIC       '410039',
# MAGIC       '414709',
# MAGIC       '416598',
# MAGIC       '418887',
# MAGIC       '420767',
# MAGIC       '423953',
# MAGIC       '434769',
# MAGIC       '440066',
# MAGIC       '440393',
# MAGIC       '443913',
# MAGIC       '450553',
# MAGIC       '450644',
# MAGIC       '451014',
# MAGIC       '451992',
# MAGIC       '452034',
# MAGIC       '453600',
# MAGIC       '453734',
# MAGIC       '453735',
# MAGIC       '456835',
# MAGIC       '459693',
# MAGIC       '460031',
# MAGIC       '462239',
# MAGIC       '472409',
# MAGIC       '512230',
# MAGIC       '515676',
# MAGIC       '516361',
# MAGIC       '517805',
# MAGIC       '521267',
# MAGIC       '521600',
# MAGIC       '521729',
# MAGIC       '524204',
# MAGIC       '525363',
# MAGIC       '526408',
# MAGIC       '532655',
# MAGIC       '535316',
# MAGIC       '540978',
# MAGIC       '542418',
# MAGIC       '542933',
# MAGIC       '546068',
# MAGIC       '548171',
# MAGIC       '552191',
# MAGIC       '552433',
# MAGIC       '554576',
# MAGIC       '557652')
# MAGIC        and created_date between '2024-10-24' and '2024-11-08'
# MAGIC        and merchant_id
# MAGIC          IN
# MAGIC          ('BqAd114c65b2gR',
# MAGIC 'CzSXv4EOwnwowy',
# MAGIC 'DfmWzfghGVGbOO',
# MAGIC 'F3fHJMjKTt9z6k',
# MAGIC 'FA0NAlEhpuEjoC',
# MAGIC 'FQDIB7jeu18Cyh',
# MAGIC 'HehaoblB8Wu8el',
# MAGIC 'IwfJ0M8WARteiV',
# MAGIC 'JG0LJ5mF8MvtGG',
# MAGIC 'JmvgkqIF6MrCxB',
# MAGIC 'JUGgDpl81uP79d',
# MAGIC 'NEbqyYAKiPWLW4')
# MAGIC      ) skip_auth
# MAGIC        ON cards_pay.id = skip_auth.payment_id
# MAGIC   
# MAGIC   LEFT JOIN realtime_hudi_api.payment_meta pm
# MAGIC     ON cards_pay.id = pm.payment_id
# MAGIC     AND pm.dcc_offered = 1
# MAGIC     AND pm.created_date between '2024-10-24' and '2024-11-08'
# MAGIC   LEFT JOIN realtime_pgpayments_card_live.authorization authz
# MAGIC     ON cards_pay.id = authz.payment_id
# MAGIC     and authz.created_date between '2024-10-24' and '2024-11-08'
# MAGIC GROUP BY 1,2,3,4,5,6,7

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH customer_txns AS (
# MAGIC   SELECT
# MAGIC     cards.global_fingerprint,
# MAGIC     pay.merchant_id,
# MAGIC     pay.id,
# MAGIC     pay.base_amount,
# MAGIC     lag(pay.created_date) OVER (
# MAGIC       PARTITION BY cards.global_fingerprint
# MAGIC       ORDER BY
# MAGIC         pay.created_at
# MAGIC     ) prev_date
# MAGIC   FROM
# MAGIC     realtime_hudi_api.payments pay
# MAGIC     JOIN realtime_hudi_api.cards ON pay.card_id = cards.id
# MAGIC     AND pay.created_date BETWEEN '2024-08-21'
# MAGIC     AND '2024-11-20'
# MAGIC     AND pay.method = 'card'
# MAGIC     AND pay.international = 0
# MAGIC     AND (
# MAGIC       pay.internal_error_code NOT IN (
# MAGIC         'BAD_REQUEST_PAYMENT_CARD_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_LINKS',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_GATEWAY',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_PAGES',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_INVOICES'
# MAGIC       )
# MAGIC       OR pay.internal_error_code IS NULL
# MAGIC     )
# MAGIC ),
# MAGIC customer_fraud AS (
# MAGIC   SELECT
# MAGIC     cards.global_fingerprint,
# MAGIC     sum(rf.payments_base_amount) customer_fraud_gmv
# MAGIC   FROM
# MAGIC     warehouse.risk_fraud rf
# MAGIC     JOIN realtime_hudi_api.cards ON rf.payments_card_id = cards.id
# MAGIC     AND rf.frauds_fraud_date BETWEEN '2024-08-21'
# MAGIC     AND '2024-11-20'
# MAGIC     AND rf.frauds_source IN ('Visa', 'MasterCard')
# MAGIC     AND rf.frauds_type = 'cn'
# MAGIC     AND rf.payments_status = 'captured'
# MAGIC     AND rf.payments_international = 1
# MAGIC   GROUP BY
# MAGIC     1
# MAGIC ),
# MAGIC customer_prev_threeds AS (
# MAGIC   SELECT
# MAGIC     cards.global_fingerprint,
# MAGIC     pay.merchant_id,
# MAGIC     pay.id,
# MAGIC     pay.base_amount,
# MAGIC     authn.status,
# MAGIC     pay.created_at
# MAGIC   FROM
# MAGIC     realtime_hudi_api.payments pay
# MAGIC     JOIN realtime_hudi_api.cards ON pay.card_id = cards.id
# MAGIC     AND pay.created_date BETWEEN '2024-08-21'
# MAGIC     AND '2024-11-20'
# MAGIC     AND pay.method = 'card'
# MAGIC     AND pay.international = 0
# MAGIC     AND (
# MAGIC       pay.internal_error_code NOT IN (
# MAGIC         'BAD_REQUEST_PAYMENT_CARD_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_LINKS',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_GATEWAY',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_PAGES',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_INVOICES'
# MAGIC       )
# MAGIC       OR pay.internal_error_code IS NULL
# MAGIC     )
# MAGIC     JOIN realtime_pgpayments_card_live.authentication authn ON pay.id = authn.payment_id
# MAGIC     AND authn.enrolled IN ('C', 'Y', 'A')
# MAGIC ),
# MAGIC customer_any_threeds AS (
# MAGIC   SELECT
# MAGIC     cards.global_fingerprint,
# MAGIC     pay.merchant_id,
# MAGIC     COUNT(pay.id) authn_success
# MAGIC   FROM
# MAGIC     realtime_hudi_api.payments pay
# MAGIC     JOIN realtime_hudi_api.cards ON pay.card_id = cards.id
# MAGIC     AND pay.created_date BETWEEN '2024-08-21'
# MAGIC     AND '2024-11-20'
# MAGIC     AND pay.method = 'card'
# MAGIC     AND pay.international = 0
# MAGIC     AND (
# MAGIC       pay.internal_error_code NOT IN (
# MAGIC         'BAD_REQUEST_PAYMENT_CARD_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_LINKS',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_GATEWAY',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_PAGES',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_INVOICES'
# MAGIC       )
# MAGIC       OR pay.internal_error_code IS NULL
# MAGIC     )
# MAGIC     JOIN realtime_pgpayments_card_live.authentication authn ON pay.id = authn.payment_id
# MAGIC     AND authn.enrolled IN ('C', 'Y', 'A')
# MAGIC     AND authn.status = 'success'
# MAGIC   GROUP BY
# MAGIC     1,
# MAGIC     2
# MAGIC )
# MAGIC SELECT
# MAGIC   pay.*,
# MAGIC   frauds.fraud_gmv
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       iin,
# MAGIC       bin_country,
# MAGIC       merchant_id,
# MAGIC       merchant_name,
# MAGIC       accept_only_3ds_payments,
# MAGIC       enrolled,
# MAGIC        cooldown_period,
# MAGIC        amount_range,
# MAGIC       authz_gateway_error,
# MAGIC       COUNT(payment_id) attempts,
# MAGIC       COUNT(
# MAGIC         CASE
# MAGIC           WHEN authorized_at IS NOT NULL THEN payment_id
# MAGIC         END
# MAGIC       ) success,
# MAGIC       COUNT(
# MAGIC         CASE
# MAGIC           WHEN internal_error_code IN (
# MAGIC             'GATEWAY_ERROR_AUTHENTICATION_STATUS_FAILED',
# MAGIC             'BAD_REQUEST_PAYMENT_CARD_HOLDER_AUTHENTICATION_FAILED',
# MAGIC             'BAD_REQUEST_PAYMENT_DECLINED_3DSECURE_AUTH_FAILED'
# MAGIC           ) THEN payment_id
# MAGIC         END
# MAGIC       ) authn_failures,
# MAGIC       COUNT(
# MAGIC         CASE
# MAGIC           WHEN internal_error_code IN (
# MAGIC             'BAD_REQUEST_PAYMENT_TIMED_OUT',
# MAGIC             'BAD_REQUEST_PAYMENT_CANCELLED_BY_USER'
# MAGIC           ) THEN payment_id
# MAGIC         END
# MAGIC       ) authn_timeout,
# MAGIC       COUNT(
# MAGIC         CASE
# MAGIC           WHEN internal_error_code IN (
# MAGIC             'BAD_REQUEST_PAYMENT_DECLINED_BY_BANK_DUE_TO_RISK',
# MAGIC             'BAD_REQUEST_PAYMENT_DECLINED_BY_BANK',
# MAGIC             'BAD_REQUEST_PAYMENT_BLOCKED_DUE_TO_FRAUD'
# MAGIC           ) THEN payment_id
# MAGIC         END
# MAGIC       ) authz_failures,
# MAGIC       SUM(
# MAGIC         CASE
# MAGIC           WHEN captured_at IS NOT NULL THEN base_amount * 1.00 / 100
# MAGIC         END
# MAGIC       ) captured_gmv
# MAGIC     FROM
# MAGIC       (
# MAGIC         SELECT
# MAGIC           cards.iin,
# MAGIC           coalesce(iins.country, bs.country [1]) bin_country,
# MAGIC           pay.merchant_id,
# MAGIC           merc.name merchant_name,
# MAGIC           coalesce(mfeat.accept_only_3ds_payments, 0) accept_only_3ds_payments,
# MAGIC           authn.enrolled,
# MAGIC           (
# MAGIC             CASE
# MAGIC               WHEN date_diff(
# MAGIC                 DAY,
# MAGIC                 CAST(customer_txns.prev_date AS date),
# MAGIC                 CAST(pay.created_date AS date)
# MAGIC               ) <= 1 THEN '1. <=1 day'
# MAGIC               WHEN date_diff(
# MAGIC                 DAY,
# MAGIC                 CAST(customer_txns.prev_date AS date),
# MAGIC                 CAST(pay.created_date AS date)
# MAGIC               ) <= 7 THEN '2. 2-7 days'
# MAGIC               WHEN date_diff(
# MAGIC                 DAY,
# MAGIC                 CAST(customer_txns.prev_date AS date),
# MAGIC                 CAST(pay.created_date AS date)
# MAGIC               ) <= 21 THEN '3. 7-21 days'
# MAGIC               ELSE '4. 22 days+'
# MAGIC             END
# MAGIC           ) cooldown_period,
# MAGIC           (
# MAGIC             CASE
# MAGIC               WHEN pay.base_amount < 300000 THEN '1. under 3k'
# MAGIC               WHEN pay.base_amount < 500000 THEN '2. 3-5k'
# MAGIC               WHEN pay.base_amount < 1000000 THEN '3. 5-10k'
# MAGIC               WHEN pay.base_amount < 2000000 THEN '4. 10-20k'
# MAGIC               ELSE '20k+'
# MAGIC             END
# MAGIC           ) amount_range,
# MAGIC           pay.id payment_id,
# MAGIC           (
# MAGIC             CASE
# MAGIC               WHEN authz.gateway_error_code IN ('59', 'C3', 'C5', '05', 'C4', '63', '07') THEN authz.gateway_error_code
# MAGIC               ELSE 'others'
# MAGIC             END
# MAGIC           ) authz_gateway_error,
# MAGIC           pay.authorized_at,
# MAGIC           pay.internal_error_code,
# MAGIC           pay.captured_at,
# MAGIC           pay.base_amount,
# MAGIC           global_rep.status,
# MAGIC           local_rep.status,
# MAGIC           global_any_rep.authn_success,
# MAGIC           local_any_rep.authn_success,
# MAGIC           (
# MAGIC             CASE
# MAGIC               WHEN CAST(rl.score AS double) < 20.0 THEN '1. 0-20'
# MAGIC               WHEN CAST(rl.score AS double) < 40.0 THEN '2. 20-40'
# MAGIC               WHEN CAST(rl.score AS double) < 50.0 THEN '3. 40-50'
# MAGIC               WHEN CAST(rl.score AS double) < 55.0 THEN '4. 50-55'
# MAGIC               WHEN CAST(rl.score AS double) < 60.0 THEN '5. 55-60'
# MAGIC               WHEN CAST(rl.score AS double) < 65.0 THEN '6. 60-65'
# MAGIC               WHEN CAST(rl.score AS double) < 70.0 THEN '7. 65-70'
# MAGIC               WHEN CAST(rl.score AS double) < 75.0 THEN '8. 70-75'
# MAGIC               WHEN CAST(rl.score AS double) < 80.0 THEN '9. 75-80'
# MAGIC               WHEN CAST(rl.score AS double) < 85.0 THEN '10. 80-85'
# MAGIC               WHEN CAST(rl.score AS double) < 90.0 THEN '11. 85-90'
# MAGIC               WHEN CAST(rl.score AS double) < 95.0 THEN '12. 90-100'
# MAGIC             END
# MAGIC           ) cybs_score_range,
# MAGIC           row_number() over (
# MAGIC             partition BY pay.id
# MAGIC             ORDER BY
# MAGIC               global_rep.created_at DESC
# MAGIC           ) rnk,
# MAGIC           row_number() over (
# MAGIC             partition BY pay.id
# MAGIC             ORDER BY
# MAGIC               local_rep.created_at DESC
# MAGIC           ) rnk2
# MAGIC         FROM
# MAGIC           realtime_hudi_api.payments pay
# MAGIC           JOIN realtime_hudi_api.cards ON pay.card_id = cards.id
# MAGIC           AND pay.created_date BETWEEN '2024-08-01'
# MAGIC           AND '2024-10-31'
# MAGIC           AND pay.method = 'card'
# MAGIC           AND pay.international = 1
# MAGIC           AND (
# MAGIC             pay.internal_error_code NOT IN (
# MAGIC               'BAD_REQUEST_PAYMENT_CARD_INTERNATIONAL_NOT_ALLOWED',
# MAGIC               'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
# MAGIC               'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_LINKS',
# MAGIC               'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_GATEWAY',
# MAGIC               'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_PAGES',
# MAGIC               'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_INVOICES'
# MAGIC             )
# MAGIC             OR pay.internal_error_code IS NULL
# MAGIC           )
# MAGIC           AND pay.merchant_id IN (
# MAGIC             '7TGWbqGVhcInG3',
# MAGIC             '6H7N6hlcv29OMG',
# MAGIC             '8S0i1kWYyF2woQ',
# MAGIC             'JI1lNgG0l0rfyH',
# MAGIC             'BqAd114c65b2gR',
# MAGIC             'IwfJ0M8WARteiV',
# MAGIC             'EAbCMkZgT3Umdw',
# MAGIC             'A1RnzlyDZsARZW',
# MAGIC             'AzaPxFRJ5Zz6Sp',
# MAGIC             '80059nUSWl6L5p',
# MAGIC             'FA0NAlEhpuEjoC',
# MAGIC             'B7scQkxfjvSTPy',
# MAGIC             '4uObL8AHBqFNnP',
# MAGIC             'BwStjpJx6XTnrr',
# MAGIC             'F3fHJMjKTt9z6k',
# MAGIC             'JUGgDpl81uP79d',
# MAGIC             'FScE9p0YVuoeC1',
# MAGIC             'HehaoblB8Wu8el',
# MAGIC             'HkcP2poDUofdvZ',
# MAGIC             'CpuH6q8ZovBmrD',
# MAGIC             'HPaVCM0v6FFGGB',
# MAGIC             'JG0LJ5mF8MvtGG',
# MAGIC             'IRmkUvUjMyaIQY',
# MAGIC             'GipZIq3wepkocv',
# MAGIC             'BrY4yPj7zn1qQO',
# MAGIC             'InX9dtoSdpQm7m',
# MAGIC             'FHR7G2XT6giDu8',
# MAGIC             'KBq1JGmZ8N2HJH',
# MAGIC             '87qTXzFTBLFN7i',
# MAGIC             'FQat1DkATb0AeR',
# MAGIC             'G5haSq92fuDyVG',
# MAGIC             'GUioTeT836TrT2',
# MAGIC             'FQDIB7jeu18Cyh',
# MAGIC             'HGg6S2seJ6UGJS',
# MAGIC             'HO46K9pwinLJ46',
# MAGIC             'AHiRqyWhsPiCHs',
# MAGIC             'CqHO5djPrwyFp1',
# MAGIC             'FrPlwrckL9A9yN',
# MAGIC             'D87uVI8QaOyTwh',
# MAGIC             'Kj5DUlG01B3vbW',
# MAGIC             'NsW8M66R2r4sLa',
# MAGIC             'DfmWzfghGVGbOO',
# MAGIC             'NEbqyYAKiPWLW4',
# MAGIC             'GP62WJ43VsAtEj',
# MAGIC             'GGuIdjsIlKDnDB',
# MAGIC             'DpSPD8fe67Frr1',
# MAGIC             'F01cvb5zEvt3ze',
# MAGIC             'Ka0c8kUaZ9ftOk',
# MAGIC             'FMDfr15kqgJcgV',
# MAGIC             'JnbwuDCFKtaU9a',
# MAGIC             'BS1TdpNE93gqFI',
# MAGIC             '9NVPPQuTqF4cYx',
# MAGIC             'EG8TVpqCeLkd9A',
# MAGIC             '8K4v0EqHDl342o',
# MAGIC             'GNxmKSL4lAyXcG',
# MAGIC             'Ndof2z8TPZvcD3',
# MAGIC             'JmvgkqIF6MrCxB',
# MAGIC             'AgeBUR39tqKaYu',
# MAGIC             'Gcap93PZaDMaLZ',
# MAGIC             'ECwRvYqexwWYB0',
# MAGIC             '8vPHCbhhOfDgpO',
# MAGIC             'FeNZUxwkik6goA',
# MAGIC             'GTUm5wan38syAG',
# MAGIC             '3olwQ4VH3c2kZl',
# MAGIC             'Nsaoly7kJnbcdY',
# MAGIC             'DIoOEQ8gOieAue',
# MAGIC             'MmU6dyMicCeS6Q',
# MAGIC             'LgKPzhwHkmMXn0',
# MAGIC             'IqINCea5uPl6oY',
# MAGIC             'HxJpOgJLFxaCdQ',
# MAGIC             'Fx5t9LCT82s5tk',
# MAGIC             'EWvRSacdcQT9vW',
# MAGIC             'Ig02AbVzaOyPjW',
# MAGIC             'F20yGBIK6U2EbM',
# MAGIC             'FVDN32gp1T2yz8',
# MAGIC             'CNnISw5fPzKG8v',
# MAGIC             'JDc0axxftF3EwQ',
# MAGIC             'GxyxoVU71exX0E',
# MAGIC             'KdcF69dDAlpfvm',
# MAGIC             'HDngISyfSQGsLT',
# MAGIC             'C3HD1IeMMX08bb',
# MAGIC             'DhivoY9l0fHNcy',
# MAGIC             'O3FlCgworxPNwi',
# MAGIC             'J1m7B6fw2Uwo7W',
# MAGIC             'NeIXGHaOTfd759',
# MAGIC             'KT9WKdDd77lNHC',
# MAGIC             'MA0ZflcbxXETNj',
# MAGIC             'BhxiTOhtjAu8ed',
# MAGIC             '9IHUoLNekUYtzU',
# MAGIC             'KSVXCO6Gd1Yrcl',
# MAGIC             'GxALpuU22N2O5m',
# MAGIC             'Bz2VyGKL8INUTQ',
# MAGIC             'OfwXBg4IqhG6kf',
# MAGIC             'MA0PmDJyjxEaBQ',
# MAGIC             'I5JQRY4tSejnLC',
# MAGIC             'GUIpIEhuwUfSwN',
# MAGIC             'Ej1nCGKjTJSXoE',
# MAGIC             'BcilQkbZYUFxzA',
# MAGIC             'I5Nqye39jAyujf',
# MAGIC             'DE63vNNo7F8dOa',
# MAGIC             'GiYWTowtHTQbDw',
# MAGIC             'DqUFL16n6np1L3',
# MAGIC             'HswYlma6pmfCY8',
# MAGIC             'EJHfGbW717NLZh',
# MAGIC             'Jvgd80A0t6RG2o',
# MAGIC             'NlsQMH9og9L2zJ',
# MAGIC             'JFZ1cEyElNLMVu',
# MAGIC             'GwiUA5KpS3g3jY',
# MAGIC             'LUT2lBSK84mLR3',
# MAGIC             'CzSXv4EOwnwowy',
# MAGIC             'NwpmicU1ip9WrR',
# MAGIC             'HTdTqQVzyhvYOh'
# MAGIC           )
# MAGIC           LEFT JOIN realtime_pgpayments_card_live.authentication authn ON pay.id = authn.payment_id
# MAGIC           AND authn.created_date BETWEEN '2024-08-01'
# MAGIC           AND '2024-10-31'
# MAGIC           LEFT JOIN realtime_pgpayments_card_live.authorization authz ON pay.id = authz.payment_id
# MAGIC           AND authz.created_date BETWEEN '2024-08-01'
# MAGIC           AND '2024-10-31'
# MAGIC           LEFT JOIN realtime_hudi_api.payment_analytics pan ON pay.id = pan.payment_id
# MAGIC           AND pan.created_date BETWEEN '2024-08-01'
# MAGIC           AND '2024-10-31'
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
# MAGIC           LEFT JOIN customer_txns ON pay.id = customer_txns.id
# MAGIC           LEFT JOIN customer_prev_threeds global_rep ON cards.global_fingerprint = global_rep.global_fingerprint
# MAGIC           AND pay.created_at > global_rep.created_at
# MAGIC           LEFT JOIN customer_prev_threeds local_rep ON cards.global_fingerprint = local_rep.global_fingerprint
# MAGIC           AND pay.merchant_id = local_rep.merchant_id
# MAGIC           AND pay.created_at > global_rep.created_at
# MAGIC           LEFT JOIN (
# MAGIC             SELECT
# MAGIC               global_fingerprint,
# MAGIC               sum(authn_success) authn_success
# MAGIC             FROM
# MAGIC               customer_any_threeds group by 1
# MAGIC           ) global_any_rep ON cards.global_fingerprint = global_any_rep.global_fingerprint
# MAGIC           LEFT JOIN customer_any_threeds local_any_rep ON cards.global_fingerprint = local_any_rep.global_fingerprint
# MAGIC           AND pay.merchant_id = local_any_rep.merchant_id
# MAGIC           LEFT JOIN realtime_shield.risk_logs rl ON pay.id = rl.entity_id
# MAGIC       )
# MAGIC     WHERE
# MAGIC       rnk =

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH customer_txns AS (
# MAGIC   SELECT
# MAGIC     cards.global_fingerprint,
# MAGIC     pay.merchant_id,
# MAGIC     pay.id,
# MAGIC     pay.base_amount,
# MAGIC     lag(pay.created_date) OVER (
# MAGIC       PARTITION BY cards.global_fingerprint
# MAGIC       ORDER BY
# MAGIC         pay.created_at
# MAGIC     ) prev_date
# MAGIC   FROM
# MAGIC     realtime_hudi_api.payments pay
# MAGIC     JOIN realtime_hudi_api.cards ON pay.card_id = cards.id
# MAGIC     AND pay.created_date BETWEEN '2024-08-21'
# MAGIC     AND '2024-11-20'
# MAGIC     AND pay.method = 'card'
# MAGIC     AND pay.international = 0
# MAGIC     AND (
# MAGIC       pay.internal_error_code NOT IN (
# MAGIC         'BAD_REQUEST_PAYMENT_CARD_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_LINKS',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_GATEWAY',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_PAGES',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_INVOICES'
# MAGIC       )
# MAGIC       OR pay.internal_error_code IS NULL
# MAGIC     )
# MAGIC ),
# MAGIC customer_fraud AS (
# MAGIC   SELECT
# MAGIC     cards.global_fingerprint,
# MAGIC     sum(rf.payments_base_amount) customer_fraud_gmv
# MAGIC   FROM
# MAGIC     warehouse.risk_fraud rf
# MAGIC     JOIN realtime_hudi_api.cards ON rf.payments_card_id = cards.id
# MAGIC     AND rf.frauds_fraud_date BETWEEN '2024-08-21'
# MAGIC     AND '2024-11-20'
# MAGIC     AND rf.frauds_source IN ('Visa', 'MasterCard')
# MAGIC     AND rf.frauds_type = 'cn'
# MAGIC     AND rf.payments_status = 'captured'
# MAGIC     AND rf.payments_international = 1
# MAGIC   GROUP BY
# MAGIC     1
# MAGIC ),
# MAGIC customer_prev_threeds AS (
# MAGIC   SELECT
# MAGIC     cards.global_fingerprint,
# MAGIC     pay.merchant_id,
# MAGIC     pay.id,
# MAGIC     pay.base_amount,
# MAGIC     authn.status,
# MAGIC     pay.created_at
# MAGIC   FROM
# MAGIC     realtime_hudi_api.payments pay
# MAGIC     JOIN realtime_hudi_api.cards ON pay.card_id = cards.id
# MAGIC     AND pay.created_date BETWEEN '2024-08-21'
# MAGIC     AND '2024-11-20'
# MAGIC     AND pay.method = 'card'
# MAGIC     AND pay.international = 0
# MAGIC     AND (
# MAGIC       pay.internal_error_code NOT IN (
# MAGIC         'BAD_REQUEST_PAYMENT_CARD_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_LINKS',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_GATEWAY',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_PAGES',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_INVOICES'
# MAGIC       )
# MAGIC       OR pay.internal_error_code IS NULL
# MAGIC     )
# MAGIC     JOIN realtime_pgpayments_card_live.authentication authn ON pay.id = authn.payment_id
# MAGIC     AND authn.enrolled IN ('C', 'Y', 'A')
# MAGIC ),
# MAGIC customer_any_threeds AS (
# MAGIC   SELECT
# MAGIC     cards.global_fingerprint,
# MAGIC     pay.merchant_id,
# MAGIC     COUNT(pay.id) authn_success
# MAGIC   FROM
# MAGIC     realtime_hudi_api.payments pay
# MAGIC     JOIN realtime_hudi_api.cards ON pay.card_id = cards.id
# MAGIC     AND pay.created_date BETWEEN '2024-08-21'
# MAGIC     AND '2024-11-20'
# MAGIC     AND pay.method = 'card'
# MAGIC     AND pay.international = 0
# MAGIC     AND (
# MAGIC       pay.internal_error_code NOT IN (
# MAGIC         'BAD_REQUEST_PAYMENT_CARD_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_LINKS',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_GATEWAY',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_PAGES',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_INVOICES'
# MAGIC       )
# MAGIC       OR pay.internal_error_code IS NULL
# MAGIC     )
# MAGIC     JOIN realtime_pgpayments_card_live.authentication authn ON pay.id = authn.payment_id
# MAGIC     AND authn.enrolled IN ('C', 'Y', 'A')
# MAGIC     AND authn.status = 'success'
# MAGIC   GROUP BY
# MAGIC     1,
# MAGIC     2
# MAGIC )
# MAGIC SELECT
# MAGIC   pay.*,
# MAGIC   frauds.fraud_gmv
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       iin,
# MAGIC       bin_country,
# MAGIC       merchant_id,
# MAGIC       merchant_name,
# MAGIC       accept_only_3ds_payments,
# MAGIC       enrolled,
# MAGIC        cooldown_period,
# MAGIC        amount_range,
# MAGIC       authz_gateway_error,
# MAGIC       COUNT(payment_id) attempts,
# MAGIC       COUNT(
# MAGIC         CASE
# MAGIC           WHEN authorized_at IS NOT NULL THEN payment_id
# MAGIC         END
# MAGIC       ) success,
# MAGIC       COUNT(
# MAGIC         CASE
# MAGIC           WHEN internal_error_code IN (
# MAGIC             'GATEWAY_ERROR_AUTHENTICATION_STATUS_FAILED',
# MAGIC             'BAD_REQUEST_PAYMENT_CARD_HOLDER_AUTHENTICATION_FAILED',
# MAGIC             'BAD_REQUEST_PAYMENT_DECLINED_3DSECURE_AUTH_FAILED'
# MAGIC           ) THEN payment_id
# MAGIC         END
# MAGIC       ) authn_failures,
# MAGIC       COUNT(
# MAGIC         CASE
# MAGIC           WHEN internal_error_code IN (
# MAGIC             'BAD_REQUEST_PAYMENT_TIMED_OUT',
# MAGIC             'BAD_REQUEST_PAYMENT_CANCELLED_BY_USER'
# MAGIC           ) THEN payment_id
# MAGIC         END
# MAGIC       ) authn_timeout,
# MAGIC       COUNT(
# MAGIC         CASE
# MAGIC           WHEN internal_error_code IN (
# MAGIC             'BAD_REQUEST_PAYMENT_DECLINED_BY_BANK_DUE_TO_RISK',
# MAGIC             'BAD_REQUEST_PAYMENT_DECLINED_BY_BANK',
# MAGIC             'BAD_REQUEST_PAYMENT_BLOCKED_DUE_TO_FRAUD'
# MAGIC           ) THEN payment_id
# MAGIC         END
# MAGIC       ) authz_failures,
# MAGIC       SUM(
# MAGIC         CASE
# MAGIC           WHEN captured_at IS NOT NULL THEN base_amount * 1.00 / 100
# MAGIC         END
# MAGIC       ) captured_gmv
# MAGIC     FROM
# MAGIC       (
# MAGIC         SELECT
# MAGIC           cards.iin,
# MAGIC           coalesce(iins.country, bs.country [1]) bin_country,
# MAGIC           pay.merchant_id,
# MAGIC           merc.name merchant_name,
# MAGIC           coalesce(mfeat.accept_only_3ds_payments, 0) accept_only_3ds_payments,
# MAGIC           authn.enrolled,
# MAGIC           (
# MAGIC              CASE
# MAGIC               WHEN date_diff(
# MAGIC                 DAY,
# MAGIC                 CAST(customer_txns.prev_date AS date),
# MAGIC                 CAST(pay.created_date AS date)
# MAGIC               ) <= 1 THEN '1. <=1 day'
# MAGIC               WHEN date_diff(
# MAGIC                 DAY,
# MAGIC                 CAST(customer_txns.prev_date AS date),
# MAGIC                 CAST(pay.created_date AS date)
# MAGIC               ) <= 7 THEN '2. 2-7 days'
# MAGIC               WHEN date_diff(
# MAGIC                 DAY,
# MAGIC                 CAST(customer_txns.prev_date AS date),
# MAGIC                 CAST(pay.created_date AS date)
# MAGIC               ) <= 21 THEN '3. 7-21 days'
# MAGIC               ELSE '4. 22 days+' end
# MAGIC           ) cooldown_period,
# MAGIC           (
# MAGIC             CASE
# MAGIC               WHEN pay.base_amount < 300000 THEN '1. under 3k'
# MAGIC               WHEN pay.base_amount < 500000 THEN '2. 3-5k'
# MAGIC               WHEN pay.base_amount < 1000000 THEN '3. 5-10k'
# MAGIC               WHEN pay.base_amount < 2000000 THEN '4. 10-20k'
# MAGIC               ELSE '20k+'
# MAGIC             END
# MAGIC           ) amount_range,
# MAGIC           pay.id payment_id,
# MAGIC           (
# MAGIC             CASE
# MAGIC               WHEN authz.gateway_error_code IN ('59', 'C3', 'C5', '05', 'C4', '63', '07') THEN authz.gateway_error_code
# MAGIC               ELSE 'others'
# MAGIC             END
# MAGIC           ) authz_gateway_error,
# MAGIC           pay.authorized_at,
# MAGIC           pay.internal_error_code,
# MAGIC           pay.captured_at,
# MAGIC           pay.base_amount,
# MAGIC           global_rep.status,
# MAGIC           local_rep.status,
# MAGIC           global_any_rep.authn_success,
# MAGIC           local_any_rep.authn_success,
# MAGIC           (
# MAGIC             CASE
# MAGIC               WHEN CAST(rl.score AS double) < 20.0 THEN '1. 0-20'
# MAGIC               WHEN CAST(rl.score AS double) < 40.0 THEN '2. 20-40'
# MAGIC               WHEN CAST(rl.score AS double) < 50.0 THEN '3. 40-50'
# MAGIC               WHEN CAST(rl.score AS double) < 55.0 THEN '4. 50-55'
# MAGIC               WHEN CAST(rl.score AS double) < 60.0 THEN '5. 55-60'
# MAGIC               WHEN CAST(rl.score AS double) < 65.0 THEN '6. 60-65'
# MAGIC               WHEN CAST(rl.score AS double) < 70.0 THEN '7. 65-70'
# MAGIC               WHEN CAST(rl.score AS double) < 75.0 THEN '8. 70-75'
# MAGIC               WHEN CAST(rl.score AS double) < 80.0 THEN '9. 75-80'
# MAGIC               WHEN CAST(rl.score AS double) < 85.0 THEN '10. 80-85'
# MAGIC               WHEN CAST(rl.score AS double) < 90.0 THEN '11. 85-90'
# MAGIC               WHEN CAST(rl.score AS double) < 95.0 THEN '12. 90-100'
# MAGIC             END
# MAGIC           ) cybs_score_range,
# MAGIC           row_number() over (
# MAGIC             partition BY pay.id
# MAGIC             ORDER BY
# MAGIC               global_rep.created_at DESC
# MAGIC           ) rnk,
# MAGIC           row_number() over (
# MAGIC             partition BY pay.id
# MAGIC             ORDER BY
# MAGIC               local_rep.created_at DESC
# MAGIC           ) rnk2
# MAGIC         FROM
# MAGIC           realtime_hudi_api.payments pay
# MAGIC           JOIN realtime_hudi_api.cards ON pay.card_id = cards.id
# MAGIC           AND pay.created_date BETWEEN '2024-08-01'
# MAGIC           AND '2024-08-31'
# MAGIC           AND pay.method = 'card'
# MAGIC           AND pay.international = 1
# MAGIC           AND (
# MAGIC             pay.internal_error_code NOT IN (
# MAGIC               'BAD_REQUEST_PAYMENT_CARD_INTERNATIONAL_NOT_ALLOWED',
# MAGIC               'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
# MAGIC               'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_LINKS',
# MAGIC               'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_GATEWAY',
# MAGIC               'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_PAGES',
# MAGIC               'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_INVOICES'
# MAGIC             )
# MAGIC             OR pay.internal_error_code IS NULL
# MAGIC           )
# MAGIC           AND pay.merchant_id IN (
# MAGIC             '7TGWbqGVhcInG3',
# MAGIC             '6H7N6hlcv29OMG',
# MAGIC             '8S0i1kWYyF2woQ',
# MAGIC             'JI1lNgG0l0rfyH',
# MAGIC             'BqAd114c65b2gR',
# MAGIC             'IwfJ0M8WARteiV',
# MAGIC             'EAbCMkZgT3Umdw',
# MAGIC             'A1RnzlyDZsARZW',
# MAGIC             'AzaPxFRJ5Zz6Sp',
# MAGIC             '80059nUSWl6L5p',
# MAGIC             'FA0NAlEhpuEjoC',
# MAGIC             'B7scQkxfjvSTPy',
# MAGIC             '4uObL8AHBqFNnP',
# MAGIC             'BwStjpJx6XTnrr',
# MAGIC             'F3fHJMjKTt9z6k',
# MAGIC             'JUGgDpl81uP79d',
# MAGIC             'FScE9p0YVuoeC1',
# MAGIC             'HehaoblB8Wu8el',
# MAGIC             'HkcP2poDUofdvZ',
# MAGIC             'CpuH6q8ZovBmrD',
# MAGIC             'HPaVCM0v6FFGGB',
# MAGIC             'JG0LJ5mF8MvtGG',
# MAGIC             'IRmkUvUjMyaIQY',
# MAGIC             'GipZIq3wepkocv',
# MAGIC             'BrY4yPj7zn1qQO',
# MAGIC             'InX9dtoSdpQm7m',
# MAGIC             'FHR7G2XT6giDu8',
# MAGIC             'KBq1JGmZ8N2HJH',
# MAGIC             '87qTXzFTBLFN7i',
# MAGIC             'FQat1DkATb0AeR',
# MAGIC             'G5haSq92fuDyVG',
# MAGIC             'GUioTeT836TrT2',
# MAGIC             'FQDIB7jeu18Cyh',
# MAGIC             'HGg6S2seJ6UGJS',
# MAGIC             'HO46K9pwinLJ46',
# MAGIC             'AHiRqyWhsPiCHs',
# MAGIC             'CqHO5djPrwyFp1',
# MAGIC             'FrPlwrckL9A9yN',
# MAGIC             'D87uVI8QaOyTwh',
# MAGIC             'Kj5DUlG01B3vbW',
# MAGIC             'NsW8M66R2r4sLa',
# MAGIC             'DfmWzfghGVGbOO',
# MAGIC             'NEbqyYAKiPWLW4',
# MAGIC             'GP62WJ43VsAtEj',
# MAGIC             'GGuIdjsIlKDnDB',
# MAGIC             'DpSPD8fe67Frr1',
# MAGIC             'F01cvb5zEvt3ze',
# MAGIC             'Ka0c8kUaZ9ftOk',
# MAGIC             'FMDfr15kqgJcgV',
# MAGIC             'JnbwuDCFKtaU9a',
# MAGIC             'BS1TdpNE93gqFI',
# MAGIC             '9NVPPQuTqF4cYx',
# MAGIC             'EG8TVpqCeLkd9A',
# MAGIC             '8K4v0EqHDl342o',
# MAGIC             'GNxmKSL4lAyXcG',
# MAGIC             'Ndof2z8TPZvcD3',
# MAGIC             'JmvgkqIF6MrCxB',
# MAGIC             'AgeBUR39tqKaYu',
# MAGIC             'Gcap93PZaDMaLZ',
# MAGIC             'ECwRvYqexwWYB0',
# MAGIC             '8vPHCbhhOfDgpO',
# MAGIC             'FeNZUxwkik6goA',
# MAGIC             'GTUm5wan38syAG',
# MAGIC             '3olwQ4VH3c2kZl',
# MAGIC             'Nsaoly7kJnbcdY',
# MAGIC             'DIoOEQ8gOieAue',
# MAGIC             'MmU6dyMicCeS6Q',
# MAGIC             'LgKPzhwHkmMXn0',
# MAGIC             'IqINCea5uPl6oY',
# MAGIC             'HxJpOgJLFxaCdQ',
# MAGIC             'Fx5t9LCT82s5tk',
# MAGIC             'EWvRSacdcQT9vW',
# MAGIC             'Ig02AbVzaOyPjW',
# MAGIC             'F20yGBIK6U2EbM',
# MAGIC             'FVDN32gp1T2yz8',
# MAGIC             'CNnISw5fPzKG8v',
# MAGIC             'JDc0axxftF3EwQ',
# MAGIC             'GxyxoVU71exX0E',
# MAGIC             'KdcF69dDAlpfvm',
# MAGIC             'HDngISyfSQGsLT',
# MAGIC             'C3HD1IeMMX08bb',
# MAGIC             'DhivoY9l0fHNcy',
# MAGIC             'O3FlCgworxPNwi',
# MAGIC             'J1m7B6fw2Uwo7W',
# MAGIC             'NeIXGHaOTfd759',
# MAGIC             'KT9WKdDd77lNHC',
# MAGIC             'MA0ZflcbxXETNj',
# MAGIC             'BhxiTOhtjAu8ed',
# MAGIC             '9IHUoLNekUYtzU',
# MAGIC             'KSVXCO6Gd1Yrcl',
# MAGIC             'GxALpuU22N2O5m',
# MAGIC             'Bz2VyGKL8INUTQ',
# MAGIC             'OfwXBg4IqhG6kf',
# MAGIC             'MA0PmDJyjxEaBQ',
# MAGIC             'I5JQRY4tSejnLC',
# MAGIC             'GUIpIEhuwUfSwN',
# MAGIC             'Ej1nCGKjTJSXoE',
# MAGIC             'BcilQkbZYUFxzA',
# MAGIC             'I5Nqye39jAyujf',
# MAGIC             'DE63vNNo7F8dOa',
# MAGIC             'GiYWTowtHTQbDw',
# MAGIC             'DqUFL16n6np1L3',
# MAGIC             'HswYlma6pmfCY8',
# MAGIC             'EJHfGbW717NLZh',
# MAGIC             'Jvgd80A0t6RG2o',
# MAGIC             'NlsQMH9og9L2zJ',
# MAGIC             'JFZ1cEyElNLMVu',
# MAGIC             'GwiUA5KpS3g3jY',
# MAGIC             'LUT2lBSK84mLR3',
# MAGIC             'CzSXv4EOwnwowy',
# MAGIC             'NwpmicU1ip9WrR',
# MAGIC             'HTdTqQVzyhvYOh'
# MAGIC           )
# MAGIC           LEFT JOIN realtime_pgpayments_card_live.authentication authn ON pay.id = authn.payment_id
# MAGIC           AND authn.created_date BETWEEN '2024-08-01'
# MAGIC           AND '2024-08-31'
# MAGIC           LEFT JOIN realtime_pgpayments_card_live.authorization authz ON pay.id = authz.payment_id
# MAGIC           AND authz.created_date BETWEEN '2024-08-01'
# MAGIC           AND '2024-08-31'
# MAGIC           LEFT JOIN realtime_hudi_api.payment_analytics pan ON pay.id = pan.payment_id
# MAGIC           AND pan.created_date BETWEEN '2024-08-01'
# MAGIC           AND '2024-08-31'
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
# MAGIC           LEFT JOIN customer_txns ON pay.id = customer_txns.id
# MAGIC           LEFT JOIN customer_prev_threeds global_rep ON cards.global_fingerprint = global_rep.global_fingerprint
# MAGIC           AND pay.created_at > global_rep.created_at
# MAGIC           LEFT JOIN customer_prev_threeds local_rep ON cards.global_fingerprint = local_rep.global_fingerprint
# MAGIC           AND pay.merchant_id = local_rep.merchant_id
# MAGIC           AND pay.created_at > global_rep.created_at
# MAGIC           LEFT JOIN (
# MAGIC             SELECT
# MAGIC               global_fingerprint,
# MAGIC               sum(authn_success) authn_success
# MAGIC             FROM
# MAGIC               customer_any_threeds group by 1
# MAGIC           ) global_any_rep ON cards.global_fingerprint = global_any_rep.global_fingerprint
# MAGIC           LEFT JOIN customer_any_threeds local_any_rep ON cards.global_fingerprint = local_any_rep.global_fingerprint
# MAGIC           AND pay.merchant_id = local_any_rep.merchant_id
# MAGIC           LEFT JOIN realtime_shield.risk_logs rl ON pay.id = rl.entity_id
# MAGIC       )
# MAGIC     WHERE
# MAGIC       rnk = 1
# MAGIC       AND rnk2 = 1
# MAGIC     GROUP BY
# MAGIC       1,
# MAGIC       2,
# MAGIC       3,
# MAGIC       4,
# MAGIC       5,
# MAGIC       6,
# MAGIC       7,
# MAGIC       8,9
# MAGIC   )
# MAGIC  pay
# MAGIC LEFT JOIN (
# MAGIC   SELECT
# MAGIC     rf.cards_iin,
# MAGIC     rf.payments_merchant_id,
# MAGIC     authn.enrolled,
# MAGIC     (
# MAGIC       CASE
# MAGIC         WHEN rf.payments_base_amount < 300000 THEN '1. under 3k'
# MAGIC         WHEN rf.payments_base_amount < 500000 THEN '2. 3-5k'
# MAGIC         WHEN rf.payments_base_amount < 1000000 THEN '3. 5-10k'
# MAGIC         WHEN rf.payments_base_amount < 2000000 THEN '4. 10-20k'
# MAGIC         ELSE '20k+'
# MAGIC       END
# MAGIC     ) amount_range,
# MAGIC     SUM(rf.payments_base_amount * 1.00 / 100) fraud_gmv
# MAGIC   FROM
# MAGIC     warehouse.risk_fraud rf
# MAGIC     LEFT JOIN realtime_pgpayments_card_live.authentication authn ON rf.payments_id = authn.payment_id
# MAGIC     AND authn.created_date BETWEEN '2024-08-01'
# MAGIC     AND '2024-08-31'
# MAGIC     LEFT JOIN realtime_hudi_api.payment_analytics pan ON rf.payments_id = pan.payment_id
# MAGIC     AND pan.created_date BETWEEN '2024-08-01'
# MAGIC     AND '2024-08-31'
# MAGIC   WHERE
# MAGIC     rf.frauds_fraud_date BETWEEN '2024-08-01'
# MAGIC     AND '2024-08-31'
# MAGIC     AND rf.frauds_type = 'cn'
# MAGIC     AND rf.frauds_source IN ('Visa', 'MasterCard')
# MAGIC     AND rf.payments_method = 'card'
# MAGIC     AND rf.payments_international = 1
# MAGIC     AND rf.payments_merchant_id IN (
# MAGIC       '7TGWbqGVhcInG3',
# MAGIC       '6H7N6hlcv29OMG',
# MAGIC       '8S0i1kWYyF2woQ',
# MAGIC       'JI1lNgG0l0rfyH',
# MAGIC       'BqAd114c65b2gR',
# MAGIC       'IwfJ0M8WARteiV',
# MAGIC       'EAbCMkZgT3Umdw',
# MAGIC       'A1RnzlyDZsARZW',
# MAGIC       'AzaPxFRJ5Zz6Sp',
# MAGIC       '80059nUSWl6L5p',
# MAGIC       'FA0NAlEhpuEjoC',
# MAGIC       'B7scQkxfjvSTPy',
# MAGIC       '4uObL8AHBqFNnP',
# MAGIC       'BwStjpJx6XTnrr',
# MAGIC       'F3fHJMjKTt9z6k',
# MAGIC       'JUGgDpl81uP79d',
# MAGIC       'FScE9p0YVuoeC1',
# MAGIC       'HehaoblB8Wu8el',
# MAGIC       'HkcP2poDUofdvZ',
# MAGIC       'CpuH6q8ZovBmrD',
# MAGIC       'HPaVCM0v6FFGGB',
# MAGIC       'JG0LJ5mF8MvtGG',
# MAGIC       'IRmkUvUjMyaIQY',
# MAGIC       'GipZIq3wepkocv',
# MAGIC       'BrY4yPj7zn1qQO',
# MAGIC       'InX9dtoSdpQm7m',
# MAGIC       'FHR7G2XT6giDu8',
# MAGIC       'KBq1JGmZ8N2HJH',
# MAGIC       '87qTXzFTBLFN7i',
# MAGIC       'FQat1DkATb0AeR',
# MAGIC       'G5haSq92fuDyVG',
# MAGIC       'GUioTeT836TrT2',
# MAGIC       'FQDIB7jeu18Cyh',
# MAGIC       'HGg6S2seJ6UGJS',
# MAGIC       'HO46K9pwinLJ46',
# MAGIC       'AHiRqyWhsPiCHs',
# MAGIC       'CqHO5djPrwyFp1',
# MAGIC       'FrPlwrckL9A9yN',
# MAGIC       'D87uVI8QaOyTwh',
# MAGIC       'Kj5DUlG01B3vbW',
# MAGIC       'NsW8M66R2r4sLa',
# MAGIC       'DfmWzfghGVGbOO',
# MAGIC       'NEbqyYAKiPWLW4',
# MAGIC       'GP62WJ43VsAtEj',
# MAGIC       'GGuIdjsIlKDnDB',
# MAGIC       'DpSPD8fe67Frr1',
# MAGIC       'F01cvb5zEvt3ze',
# MAGIC       'Ka0c8kUaZ9ftOk',
# MAGIC       'FMDfr15kqgJcgV',
# MAGIC       'JnbwuDCFKtaU9a',
# MAGIC       'BS1TdpNE93gqFI',
# MAGIC       '9NVPPQuTqF4cYx',
# MAGIC       'EG8TVpqCeLkd9A',
# MAGIC       '8K4v0EqHDl342o',
# MAGIC       'GNxmKSL4lAyXcG',
# MAGIC       'Ndof2z8TPZvcD3',
# MAGIC       'JmvgkqIF6MrCxB',
# MAGIC       'AgeBUR39tqKaYu',
# MAGIC       'Gcap93PZaDMaLZ',
# MAGIC       'ECwRvYqexwWYB0',
# MAGIC       '8vPHCbhhOfDgpO',
# MAGIC       'FeNZUxwkik6goA',
# MAGIC       'GTUm5wan38syAG',
# MAGIC       '3olwQ4VH3c2kZl',
# MAGIC       'Nsaoly7kJnbcdY',
# MAGIC       'DIoOEQ8gOieAue',
# MAGIC       'MmU6dyMicCeS6Q',
# MAGIC       'LgKPzhwHkmMXn0',
# MAGIC       'IqINCea5uPl6oY',
# MAGIC       'HxJpOgJLFxaCdQ',
# MAGIC       'Fx5t9LCT82s5tk',
# MAGIC       'EWvRSacdcQT9vW',
# MAGIC       'Ig02AbVzaOyPjW',
# MAGIC       'F20yGBIK6U2EbM',
# MAGIC       'FVDN32gp1T2yz8',
# MAGIC       'CNnISw5fPzKG8v',
# MAGIC       'JDc0axxftF3EwQ',
# MAGIC       'GxyxoVU71exX0E',
# MAGIC       'KdcF69dDAlpfvm',
# MAGIC       'HDngISyfSQGsLT',
# MAGIC       'C3HD1IeMMX08bb',
# MAGIC       'DhivoY9l0fHNcy',
# MAGIC       'O3FlCgworxPNwi',
# MAGIC       'J1m7B6fw2Uwo7W',
# MAGIC       'NeIXGHaOTfd759',
# MAGIC       'KT9WKdDd77lNHC',
# MAGIC       'MA0ZflcbxXETNj',
# MAGIC       'BhxiTOhtjAu8ed',
# MAGIC       '9IHUoLNekUYtzU',
# MAGIC       'KSVXCO6Gd1Yrcl',
# MAGIC       'GxALpuU22N2O5m',
# MAGIC       'Bz2VyGKL8INUTQ',
# MAGIC       'OfwXBg4IqhG6kf',
# MAGIC       'MA0PmDJyjxEaBQ',
# MAGIC       'I5JQRY4tSejnLC',
# MAGIC       'GUIpIEhuwUfSwN',
# MAGIC       'Ej1nCGKjTJSXoE',
# MAGIC       'BcilQkbZYUFxzA',
# MAGIC       'I5Nqye39jAyujf',
# MAGIC       'DE63vNNo7F8dOa',
# MAGIC       'GiYWTowtHTQbDw',
# MAGIC       'DqUFL16n6np1L3',
# MAGIC       'HswYlma6pmfCY8',
# MAGIC       'EJHfGbW717NLZh',
# MAGIC       'Jvgd80A0t6RG2o',
# MAGIC       'NlsQMH9og9L2zJ',
# MAGIC       'JFZ1cEyElNLMVu',
# MAGIC       'GwiUA5KpS3g3jY',
# MAGIC       'LUT2lBSK84mLR3',
# MAGIC       'CzSXv4EOwnwowy',
# MAGIC       'NwpmicU1ip9WrR',
# MAGIC       'HTdTqQVzyhvYOh'
# MAGIC     )
# MAGIC   GROUP BY
# MAGIC     1,
# MAGIC     2,
# MAGIC     3,
# MAGIC     4
# MAGIC ) frauds ON pay.iin = frauds.cards_iin
# MAGIC AND pay.merchant_id = frauds.payments_merchant_id
# MAGIC AND pay.enrolled = frauds.enrolled
# MAGIC AND pay.amount_range = frauds.amount_range

# COMMAND ----------

# MAGIC %sql
# MAGIC create table analytics_selfserve.customer_txn
# MAGIC as 
# MAGIC   SELECT
# MAGIC     cards.global_fingerprint,
# MAGIC     pay.merchant_id,
# MAGIC     pay.id,
# MAGIC     pay.base_amount,
# MAGIC     lag(pay.created_date) OVER (
# MAGIC       PARTITION BY cards.global_fingerprint
# MAGIC       ORDER BY
# MAGIC         pay.created_at
# MAGIC     ) prev_date
# MAGIC   FROM
# MAGIC     realtime_hudi_api.payments pay
# MAGIC     JOIN realtime_hudi_api.cards ON pay.card_id = cards.id
# MAGIC     AND pay.created_date BETWEEN '2024-08-25'
# MAGIC     AND '2024-11-25'
# MAGIC     AND pay.method = 'card'
# MAGIC     AND pay.international = 0
# MAGIC     AND (
# MAGIC       pay.internal_error_code NOT IN (
# MAGIC         'BAD_REQUEST_PAYMENT_CARD_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_LINKS',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_GATEWAY',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_PAGES',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_INVOICES'
# MAGIC       )
# MAGIC       OR pay.internal_error_code IS NULL)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists  analytics_selfserve.customer_fraud;
# MAGIC create table analytics_selfserve.customer_fraud AS 
# MAGIC   SELECT
# MAGIC     cards.global_fingerprint,
# MAGIC     sum(rf.payments_base_amount) customer_fraud_gmv
# MAGIC   FROM
# MAGIC     warehouse.risk_fraud rf
# MAGIC     JOIN realtime_hudi_api.cards ON rf.payments_card_id = cards.id
# MAGIC     AND rf.frauds_fraud_date BETWEEN '2024-08-25'
# MAGIC     AND '2024-11-25'
# MAGIC     AND rf.frauds_source IN ('Visa', 'MasterCard')
# MAGIC     AND rf.frauds_type = 'cn'
# MAGIC     AND rf.payments_status = 'captured'
# MAGIC     AND rf.payments_international = 1
# MAGIC   GROUP BY
# MAGIC     1
