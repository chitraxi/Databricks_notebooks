# Databricks notebook source
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
# MAGIC     (select * from realtime_hudi_api.payments where created_date BETWEEN '2024-08-25'
# MAGIC     AND '2024-09-25' and method = 'card' and international = 1 and (internal_error_code NOT IN (
# MAGIC         'BAD_REQUEST_PAYMENT_CARD_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_LINKS',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_GATEWAY',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_PAGES',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_INVOICES'
# MAGIC       )
# MAGIC       OR internal_error_code IS NULL)) pay
# MAGIC     left JOIN realtime_hudi_api.cards ON pay.card_id = cards.id
# MAGIC     
# MAGIC      

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
# MAGIC drop table if exists analytics_selfserve.customer_prev_threeds;
# MAGIC create table analytics_selfserve.customer_prev_threeds AS 
# MAGIC   SELECT
# MAGIC     cards_global_fingerprint as global_fingerprint,
# MAGIC     p_merchant_id as merchants_id,
# MAGIC     p_id as payments_id,
# MAGIC     pay.p_base_amount as payments_base_amount,
# MAGIC     at_status as status,
# MAGIC     p_created_at as payments_created_at
# MAGIC   FROM
# MAGIC     (select * from whs_trino_v.cards_payments_flat_fact as pay where
# MAGIC     p_created_date between '2024-08-25' and '2024-08-26'
# MAGIC       AND p_method = 'card' and p_international = 1 and 
# MAGIC       (p_internal_error_code NOT IN (
# MAGIC         'BAD_REQUEST_PAYMENT_CARD_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_LINKS',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_GATEWAY',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_PAYMENT_PAGES',
# MAGIC         'BAD_REQUEST_CARD_INTERNATIONAL_NOT_ALLOWED_FOR_INVOICES'
# MAGIC       )
# MAGIC       OR p_internal_error_code IS NULL) ) as pay
# MAGIC     where
# MAGIC     at_enrolled IN ('C', 'Y', 'A')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  analytics_selfserve.customer_prev_threeds

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into analytics_selfserve.CB_payments_cards_v1
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
# MAGIC drop table if exists  analytics_selfserve.frauds;
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
# MAGIC     AND '2024-10-31'
# MAGIC     LEFT JOIN realtime_hudi_api.payment_analytics pan ON rf.payments_id = pan.payment_id
# MAGIC     AND pan.created_date BETWEEN '2024-08-01'
# MAGIC     AND '2024-10-31'
# MAGIC   WHERE
# MAGIC     rf.frauds_fraud_date BETWEEN '2024-08-01'
# MAGIC     AND '2024-10-31'
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
# MAGIC ) frauds
