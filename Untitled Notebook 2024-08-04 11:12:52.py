# Databricks notebook source
# MAGIC %sql
# MAGIC with Optimizer_Flag AS
# MAGIC (select distinct payment_id
# MAGIC -- replacing analytics_selfserve table with aggregate_pa.optimizer_terminal_payments as the table has been fixed
# MAGIC from aggregate_pa.optimizer_terminal_payments where producer_created_date between try_cast(CURRENT_DATE + interval '-60' day as varchar) and try_cast(CURRENT_DATE + interval '-31' day as varchar)),
# MAGIC
# MAGIC Optimizer_age as (select a.*,b.created_date as feature_created_date from
# MAGIC                           (select distinct entity_id, min(case when features._is_row_deleted is not null then 999999 else date_diff('day',date(features.created_date),current_date) end) age,
# MAGIC                            max( features._is_row_deleted ) as is_deleted from features where name = 'raas'  group by 1) a
# MAGIC                   left join (select entity_id, created_date from realtime_hudi_api.features where _is_row_deleted is null and name = 'raas') b on a.entity_id = b.entity_id),
# MAGIC
# MAGIC P as (select distinct id, merchant_id, method, gateway, terminal_id, created_at, captured_at, authorized_at,
# MAGIC                     gateway_captured, card_id, internal_error_code, base_amount, created_date, customer_id,
# MAGIC                     global_customer_id, global_token_id, token_id, wallet, bank, error_description, auth_type
# MAGIC       from realtime_hudi_api.payments where created_date >= try_cast(CURRENT_DATE + interval '-15' day as varchar)),
# MAGIC
# MAGIC -- 6. Replacing terminals table with flat table
# MAGIC t as (select distinct terminal_id as id, gateway, procurer, deleted_at from whs_trino_v.terminals_flat_hourly where deleted_at is null),
# MAGIC
# MAGIC -- 7. adding issuer, network tokenization to the table
# MAGIC c as (select distinct type, network, id, issuer, case when trivia is null then '0' else trivia end as network_tokenised_payment from realtime_hudi_api.cards),
# MAGIC
# MAGIC u as (select distinct payment_id, action, type, provider from realtime_hudi_api.upi where created_date between try_cast(CURRENT_DATE + interval '-60' day as varchar) and try_cast(CURRENT_DATE + interval '-31' day as varchar)),
# MAGIC
# MAGIC upi_fiscal as (select distinct payment_id, flow, provider, created_date from realtime_payments_upi_live.fiscal where created_date between try_cast(CURRENT_DATE + interval '-60' day as varchar) and try_cast(CURRENT_DATE + interval '-31' day as varchar)),
# MAGIC
# MAGIC upi_app as (select payment_id, app, created_date from realtime_hudi_api.upi_metadata_new where created_date between try_cast(CURRENT_DATE + interval '-60' day as varchar) and try_cast(CURRENT_DATE + interval '-31' day as varchar)),
# MAGIC
# MAGIC pa as (select distinct payment_id, library from realtime_hudi_api.payment_analytics where created_date between try_cast(CURRENT_DATE + interval '-60' day as varchar) and try_cast(CURRENT_DATE + interval '-31' day as varchar))
# MAGIC
# MAGIC
# MAGIC select id, count(*) from 
# MAGIC
# MAGIC ( select distinct p.id, p.merchant_id, p.created_at, p.captured_at, p.authorized_at, p.method,
# MAGIC   CASE
# MAGIC             WHEN p.method = 'card' and (c.type in ('debit','Debit'))  THEN 'Debit Card'
# MAGIC             WHEN p.method = 'card' and (c.type in ('credit','Credit')) THEN 'Credit Card'
# MAGIC             WHEN p.method = 'card' and (c.type in ('prepaid','Prepaid')) THEN 'Prepaid Card'
# MAGIC             WHEN p.method = 'card' and (c.type IS NULL OR c.type = '')  THEN 'Unknown Card'
# MAGIC
# MAGIC             WHEN p.method = 'emi' and (c.type in ('debit','Debit')) then 'DC emi'
# MAGIC             WHEN p.method = 'emi' and (c.type in ('credit','Credit')) then 'CC emi'
# MAGIC             WHEN p.method = 'emi' and (c.type in ('prepaid','Prepaid')) then 'Prepaid Card emi'
# MAGIC
# MAGIC             WHEN coalesce(u.type,upi_fiscal.flow) = 'collect' THEN 'UPI Collect'
# MAGIC             WHEN coalesce(u.type,upi_fiscal.flow) = 'pay' or coalesce(u.type,upi_fiscal.flow) = 'PAY' or coalesce(u.type,upi_fiscal.flow) = 'intent' THEN 'UPI Intent'
# MAGIC             WHEN coalesce(u.type,upi_fiscal.flow) = 'in_app'  THEN 'UPI Unknown'
# MAGIC
# MAGIC             ELSE p.method
# MAGIC
# MAGIC         END  AS method_advanced,
# MAGIC     p.terminal_id, p.gateway, t.procurer, p.base_amount AS base_amount,
# MAGIC     case when p.id in (o.payment_id) then 1 else 0 end AS "optimizer_Flag",
# MAGIC     case
# MAGIC     when p.gateway = 'optimizer_razorpay' then 'internal'
# MAGIC     when p.id in (o.payment_id) and t.procurer = 'razorpay' then 'internal'
# MAGIC     when p.id in (o.payment_id) and t.procurer is null then null
# MAGIC     when p.id in (o.payment_id) and t.procurer = 'razorpay' then 'internal'
# MAGIC     when p.id in (o.payment_id) and t.procurer is null then null
# MAGIC     when p.id in (o.payment_id) and t.procurer = 'merchant' and p.merchant_id in
# MAGIC     ('HsuMMq5AQBGCIW',
# MAGIC     'Hsup9WpX7ImZKw',
# MAGIC     'HsvZ5lz0dsDEJb',
# MAGIC     'HsvpIn3D8Cc6Zc',
# MAGIC     'GWjTMDeGwcVrr1',
# MAGIC     'HEaGxjXCXZUwSY',
# MAGIC     'GV7M7ZXLNzez0b',
# MAGIC     'IyYh0RU78PkNts',
# MAGIC     'HAdYq8orPiRhEw',
# MAGIC     'HThMRY5d6U3ald',
# MAGIC     'GA4CuzvbXn2kGF',
# MAGIC     'GA6Sj3FCr1X48P',
# MAGIC     'GA6zg8KmCvShIt',
# MAGIC     'GA7CHjv52jbuI6',
# MAGIC     'GA7ek79g40DCy9',
# MAGIC     'GA7Us9F5x4zS9E',
# MAGIC     'GA7ymrtIHoBmJ6',
# MAGIC     'GA8BEH81JBq9eA',
# MAGIC     'GA8Ml3740cOIQg',
# MAGIC     'H9ObHPX7JVb3ET',
# MAGIC     'HZAwyvY5FKzWkP',
# MAGIC     'IPl7WsunideZRw')
# MAGIC     and  t.gateway in ('cybersource','hdfc','axis_migs') then 'internal'
# MAGIC     when p.id in ( o.payment_id) and  t.procurer = 'merchant' then 'external'
# MAGIC     else 'internal' end AS "internal_external_flag",
# MAGIC  CASE WHEN pa.library = 1 then 'CHECKOUTJS'
# MAGIC       WHEN pa.library = 2 then 'RAZORPAYJS'
# MAGIC       WHEN pa.library = 3 then 'S2S'
# MAGIC       WHEN pa.library = 4 then 'CUSTOM'
# MAGIC       WHEN pa.library = 5 then 'DIRECT'
# MAGIC       WHEN pa.library = 6 then 'PUSH'
# MAGIC       WHEN pa.library = 7 then 'LEGACYJS'
# MAGIC       WHEN pa.library = 8 then 'HOSTED'
# MAGIC       WHEN pa.library = 9 then 'EMBEDDED'
# MAGIC       ELSE 'UNKNOWN' end as library,
# MAGIC  CASE
# MAGIC       WHEN p.method = 'card' AND p.global_customer_id IS NOT NULL AND p.global_token_id IS NOT NULL AND p.token_id IS NULL then 'Global saved card'
# MAGIC       WHEN p.method = 'card' AND p.global_customer_id IS NOT NULL AND p.global_token_id IS NULL AND p.token_id IS not NULL then 'Global customer local saved card'
# MAGIC       WHEN p.method = 'card' AND p.global_customer_id IS NULL AND p.global_token_id IS NULL AND p.token_id is not NULL AND p.customer_id is not NULL then 'Local saved card'
# MAGIC       WHEN p.method = 'card' THEN 'Non-Saved card'
# MAGIC       else 'other_method'
# MAGIC       end as saved_card_type,
# MAGIC     Optimizer_age.feature_created_date as feature_created_date, ome.first_successful_external_payment as first_transaction_date,
# MAGIC
# MAGIC --  1. replacing null dba with parent name
# MAGIC     coalesce(optimizer_dba.dba, parent_id_mapping.parent_name) as dba,
# MAGIC
# MAGIC     oted.trial_end_date,Optimizer_age.is_deleted as is_raas_feature_deleted, golive.golive_date,
# MAGIC     c.network,c.type as card_type,
# MAGIC     p.internal_error_code as internal_error_code,
# MAGIC     case when p.id in ( dr.payment_id) then 1 else 0 end AS "priority_based_routing_flag",
# MAGIC     dr.input_provider input_provider,
# MAGIC     dr.input_terminal_id input_terminal_id,
# MAGIC     dr.input_priority input_priority,
# MAGIC     dr.output_provider output_provider,
# MAGIC     dr.output_terminal_id output_terminal_id,
# MAGIC     case when p.id in (sr.payment_id) then 1 else 0 end AS "smart_router_flag",
# MAGIC     sr.smart_router_terminal_id smart_router_terminal_id,
# MAGIC     parent_id_mapping.parent_id parent_id,
# MAGIC     parent_id_mapping.salesforce_parent_id salesforce_parent_id,
# MAGIC     parent_id_mapping.parent_name,
# MAGIC
# MAGIC --     adding issuer and tokenization here
# MAGIC     c.network_tokenised_payment as network_tokenised_payment, c.issuer issuer,
# MAGIC     p.bank as bank, p.wallet as wallet, p.error_description as error_description,
# MAGIC     CASE
# MAGIC         WHEN p.method = 'upi' and (COALESCE(u.type, upi_fiscal.flow) = 'collect') AND (COALESCE(u.provider, upi_fiscal.provider)) IN ('ybl', 'ibl', 'axl') THEN 'Phonepe'
# MAGIC         WHEN p.method = 'upi' and (COALESCE(u.type, upi_fiscal.flow) = 'collect') AND (COALESCE(u.provider, upi_fiscal.provider)) = 'paytm' THEN 'Paytm'
# MAGIC         WHEN p.method = 'upi' and (COALESCE(u.type, upi_fiscal.flow) = 'collect') AND (COALESCE(u.provider, upi_fiscal.provider)) = 'upi' THEN 'BHIM'
# MAGIC         WHEN p.method = 'upi' and (COALESCE(u.type, upi_fiscal.flow) = 'collect') AND (COALESCE(u.provider, upi_fiscal.provider)) = 'apl' THEN 'Amazon Pay'
# MAGIC         WHEN p.method = 'upi' and (COALESCE(u.type, upi_fiscal.flow) = 'collect') AND (COALESCE(u.provider, upi_fiscal.provider)) IN ('oksbi', 'okhdfcbank', 'okicici', 'okaxis') THEN 'GooglePay'
# MAGIC         WHEN p.method = 'upi' and (COALESCE(u.type, upi_fiscal.flow) = 'pay' OR COALESCE(u.type, upi_fiscal.flow) = 'PAY' OR COALESCE(u.type, upi_fiscal.flow) = 'intent') AND pa.library = 1 AND upi_app.app = 'com.phonepe.app' THEN 'Phonepe'
# MAGIC         WHEN p.method = 'upi' and (COALESCE(u.type, upi_fiscal.flow) = 'pay' OR COALESCE(u.type, upi_fiscal.flow) = 'PAY' OR COALESCE(u.type, upi_fiscal.flow) = 'intent') AND pa.library = 1 AND upi_app.app = 'net.one97.paytm' THEN 'Paytm'
# MAGIC         WHEN p.method = 'upi' and (COALESCE(u.type, upi_fiscal.flow) = 'pay' OR COALESCE(u.type, upi_fiscal.flow) = 'PAY' OR COALESCE(u.type, upi_fiscal.flow) = 'intent') AND pa.library = 1 AND upi_app.app = 'in.org.npci.upiapp' THEN 'BHIM'
# MAGIC         WHEN p.method = 'upi' and (COALESCE(u.type, upi_fiscal.flow) = 'pay' OR COALESCE(u.type, upi_fiscal.flow) = 'PAY' OR COALESCE(u.type, upi_fiscal.flow) = 'intent') AND pa.library = 1 AND upi_app.app = 'com.google.android.apps.nbu.paisa.user' THEN 'GooglePay'
# MAGIC         WHEN p.method = 'upi' and (COALESCE(u.provider, upi_fiscal.provider)) IN ('ybl', 'ibl', 'axl') THEN 'Phonepe'
# MAGIC         WHEN p.method = 'upi' and (COALESCE(u.provider, upi_fiscal.provider)) IN ('oksbi', 'okhdfcbank', 'okicici', 'okaxis') THEN 'GooglePay'
# MAGIC         WHEN p.method = 'upi' and (COALESCE(u.provider, upi_fiscal.provider)) = 'Paytm' THEN 'Paytm'
# MAGIC         WHEN p.method = 'upi' and (COALESCE(u.provider, upi_fiscal.provider)) = 'axisb' THEN 'Cred'
# MAGIC         WHEN p.method = 'upi' and (COALESCE(u.provider, upi_fiscal.provider)) IN ('axisbank') THEN 'Axis Bank'
# MAGIC         WHEN p.method = 'upi' and (COALESCE(u.provider, upi_fiscal.provider)) IN ('apl', 'yapl', 'rapl') THEN 'Amazon Pay'
# MAGIC         WHEN p.method = 'upi' and (COALESCE(u.type, upi_fiscal.flow) = 'pay' OR COALESCE(u.type, upi_fiscal.flow) = 'PAY' OR COALESCE(u.type, upi_fiscal.flow) = 'intent') AND pa.library != 1 THEN 'NA'
# MAGIC         WHEN p.method !=  'upi' then 'Not Applicable'
# MAGIC         ELSE 'Others' END AS upi_app,
# MAGIC     gateway_response.pay_verify_error_message,
# MAGIC     gateway_response.verify_error_message,
# MAGIC     gateway_response.pay_verify_response,
# MAGIC     gateway_response.verify_response,
# MAGIC     p.auth_type as auth_type,
# MAGIC     p.created_date as created_date
# MAGIC
# MAGIC     from
# MAGIC     Optimizer_age
# MAGIC
# MAGIC     full outer join P on Optimizer_age.entity_id = p.merchant_id
# MAGIC
# MAGIC     left join t
# MAGIC     on p.terminal_id = t.id and t.deleted_at is null
# MAGIC
# MAGIC     left join Optimizer_Flag AS o
# MAGIC     on p.id = o.payment_id
# MAGIC
# MAGIC     left join c
# MAGIC     ON c.id = p.card_id
# MAGIC
# MAGIC     left join  u
# MAGIC     ON u.payment_id = p.id AND (u.action = 'pre_debit' OR u.action = 'authorize' OR u.action = 'authenticate')
# MAGIC
# MAGIC -- 2. replacing hive.aggregate_pa.ppg_optimizer_trial_end_date_backup_2023_06_26 with analytics_selfserve.ppg_optimizer_trial_end_date
# MAGIC
# MAGIC     left join (select distinct merchant_id, max(trial_end_date) as trial_end_date from analytics_selfserve.ppg_optimizer_trial_end_date group by 1) oted on oted.merchant_id = p.merchant_id
# MAGIC
# MAGIC     left join  hive.aggregate_pa.ppg_optimizer_merchant_enablement_backup_2023_06_26 ome on ome.p_merchant_id = Optimizer_age.entity_id
# MAGIC
# MAGIC     left join pa on p.id = pa.payment_id
# MAGIC
# MAGIC     left join hive.aggregate_pa.ppg_optimizer_dba_backup_2023_06_26 as optimizer_dba on optimizer_dba.merchant_id = p.merchant_id
# MAGIC
# MAGIC -- 3. Replacing aggregate_pa.optimizer_golive_date with analytics_selfserve.optimizer_golive_date
# MAGIC
# MAGIC     left join analytics_selfserve.optimizer_golive_date as golive on golive.merchant_id = p.merchant_id
# MAGIC
# MAGIC     left join hive.aggregate_pa.optimizer_dynamic_routing dr
# MAGIC     on dr.payment_id = o.payment_id
# MAGIC --exclude duplicate events record
# MAGIC     left join (select distinct payment_id,null as smart_router_terminal_id from hive.aggregate_pa.smart_router_terminal_payments  where producer_created_date between try_cast(CURRENT_DATE + interval '-60' day as varchar) and try_cast(CURRENT_DATE + interval '-31' day as varchar)
# MAGIC  ) sr
# MAGIC     on sr.payment_id = o.payment_id
# MAGIC
# MAGIC --  adding parent id & salesforce parent id
# MAGIC --  batch sheet link - https://docs.google.com/spreadsheets/d/14szCAASy6hGkoHh3iDmC8HGDYZhaBo-JABIFWDB_kTo/edit#gid=0
# MAGIC
# MAGIC     left join (select distinct merchant_id, parent_id, parent_name, salesforce_parent_id from batch_sheets.optimizer_parent_id_mapping) parent_id_mapping
# MAGIC     on p.merchant_id = parent_id_mapping.merchant_id
# MAGIC
# MAGIC -- 5. Joining upi_fiscal with payments table
# MAGIC     left join upi_fiscal on p.id = upi_fiscal.payment_id
# MAGIC
# MAGIC     left join upi_app ON p.id = upi_app.payment_id
# MAGIC
# MAGIC     left join (select distinct source_id,pay_verify_error_message, verify_error_message, pay_verify_response, verify_response
# MAGIC   from analytics_selfserve.optimizer_gateway_response where created_date between try_cast(CURRENT_DATE + interval '-60' day as varchar) and try_cast(CURRENT_DATE + interval '-31' day as varchar)) as gateway_response
# MAGIC     on gateway_response.source_id = p.id
# MAGIC
# MAGIC -- 4. Filtering out cod & offline method
# MAGIC     WHERE p.method not in ('transfer','cod','offline')) group by 1 having count(*) > 1 order by count(*) desc
