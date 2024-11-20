# Databricks notebook source
# MAGIC %sql
# MAGIC --delete from  analytics_selfserve.optimizer_payments_agg where  p_created_date between '2024-09-01' and '2024-09-14';
# MAGIC insert into analytics_selfserve.optimizer_payments_agg
# MAGIC with optimizer_age as (select a.*,b.created_date as feature_created_date from
# MAGIC                          (select distinct entity_id, min(case when features._is_row_deleted is not null then 999999 else  DATEDIFF(CURRENT_DATE, DATE(features.created_date)) AS  age,
# MAGIC                           max( features._is_row_deleted ) as is_deleted from features where name = 'raas'  group by 1) a
# MAGIC                  left join (select entity_id, created_date from realtime_hudi_api.features where _is_row_deleted is null and name = 'raas') b on a.entity_id = b.entity_id),
# MAGIC smart_router_2 as  (SELECT
# MAGIC    write_key
# MAGIC  FROM
# MAGIC    events.events_doppler_v2
# MAGIC  WHERE
# MAGIC    producer_created_date between '2024-09-01' and '2024-09-14'
# MAGIC    AND properties.smart_router_provider_sorting = TRUE),
# MAGIC
# MAGIC
# MAGIC total_smart_router_payments AS (
# MAGIC  SELECT
# MAGIC   payment_id
# MAGIC  FROM
# MAGIC    pinot.default.router_worker_execution_events
# MAGIC  WHERE
# MAGIC    producer_created_date between '2024-09-01' and '2024-09-14'
# MAGIC    AND worker_name = 'RaaS'
# MAGIC    AND output_terminal_ids LIKE '["smart_router"%'
# MAGIC )
# MAGIC   
# MAGIC select
# MAGIC p_gateway,
# MAGIC p_base_amount_refunded    ,
# MAGIC p_convert_currency    ,
# MAGIC p_bank    ,
# MAGIC p_recurring_type    ,
# MAGIC p_fee_bearer    ,
# MAGIC p_wallet    ,
# MAGIC p_authentication_gateway    ,
# MAGIC p_auth_type   ,
# MAGIC case when p_authorized_at is not null then 1 else 0 end as is_authorized  ,
# MAGIC p_merchant_id   ,
# MAGIC case when p_captured_at is not null then 1 else 0 end  as is_captured   ,
# MAGIC cards.trivia    ,
# MAGIC p_currency    ,
# MAGIC null p_base_amount    ,
# MAGIC p_method    ,
# MAGIC p_verify_bucket   ,
# MAGIC p_refund_status   ,
# MAGIC p_international   ,
# MAGIC p_receiver_type   ,
# MAGIC p_verified    ,
# MAGIC p_internal_error_code   ,
# MAGIC p_error_code    ,
# MAGIC p_status    ,
# MAGIC p_recurring   ,
# MAGIC p_settled_by    ,
# MAGIC p_on_hold   ,
# MAGIC p_gateway_captured    ,
# MAGIC p_late_authorized   ,
# MAGIC p_terminal_id   ,
# MAGIC pa_library      ,
# MAGIC pa_device     ,
# MAGIC pa_platform   ,
# MAGIC t_merchant_id     ,
# MAGIC t_procurer      ,
# MAGIC t_created_date      ,
# MAGIC merchant_parent_id      ,
# MAGIC category2     ,
# MAGIC merchant_name     ,
# MAGIC website     ,
# MAGIC email     ,
# MAGIC optimizer_payments_flat_fact.merchant_id      ,
# MAGIC contact_email     ,
# MAGIC contact_name      ,
# MAGIC cards_type      ,
# MAGIC cards_network     ,
# MAGIC cards_issuer      ,
# MAGIC upi_type      ,
# MAGIC upi_provider      ,
# MAGIC upi_app     ,
# MAGIC parent_id     ,
# MAGIC parent_name     ,
# MAGIC salesforce_parent_id      ,
# MAGIC golive_date ,
# MAGIC trial_end_date      ,
# MAGIC recon_status      ,
# MAGIC output_provider     ,
# MAGIC input_provider      ,
# MAGIC smart_router_terminal_id      ,
# MAGIC input_priority    ,
# MAGIC is_smart_router_payment   ,
# MAGIC is_dynamic_routing_payment  ,
# MAGIC sum(p_base_amount) as amount_attempted ,
# MAGIC sum(p_base_amount_refunded) as amount_refunded,
# MAGIC count(distinct p_id) as payment_attempts ,
# MAGIC case when golive_date  <= p_created_date then 1 else 0 end as is_live_payment,
# MAGIC optimizer_age.feature_created_date,
# MAGIC team_owner,
# MAGIC poc_name as onwer_name,
# MAGIC owner_email,
# MAGIC billing_label,
# MAGIC owner_role__c,
# MAGIC case when s.write_key is NULL then 0 else 1 end as is_smart_router_2_payment,
# MAGIC case when ts.payment_id is NULL then 0 else 1 end as smart_router_payment_flag,
# MAGIC optimizer_payments_flat_fact.p_token_id,
# MAGIC p_cps_route,
# MAGIC CASE WHEN p_cps_route = 100 THEN 'Optimizer_Service'
# MAGIC WHEN p_cps_route = 7 THEN 'UPS'
# MAGIC ELSE 'API' END
# MAGIC AS `service`,
# MAGIC case when t_procurer='Razorpay' then 'Razorpay' else p_gateway end as p_aggregator,
# MAGIC case when  t_procurer = 'merchant' and p_merchant_id in
# MAGIC    ('HsuMMq5AQBGCIW',
# MAGIC    'Hsup9WpX7ImZKw',
# MAGIC    'HsvZ5lz0dsDEJb',
# MAGIC    'HsvpIn3D8Cc6Zc',
# MAGIC    'GWjTMDeGwcVrr1',
# MAGIC    'HEaGxjXCXZUwSY',
# MAGIC    'GV7M7ZXLNzez0b',
# MAGIC    'IyYh0RU78PkNts',
# MAGIC    'HAdYq8orPiRhEw',
# MAGIC    'HThMRY5d6U3ald',
# MAGIC    'GA4CuzvbXn2kGF',
# MAGIC    'GA6Sj3FCr1X48P',
# MAGIC    'GA6zg8KmCvShIt',
# MAGIC    'GA7CHjv52jbuI6',
# MAGIC    'GA7ek79g40DCy9',
# MAGIC    'GA7Us9F5x4zS9E',
# MAGIC    'GA7ymrtIHoBmJ6',
# MAGIC    'GA8BEH81JBq9eA',
# MAGIC    'GA8Ml3740cOIQg',
# MAGIC    'H9ObHPX7JVb3ET',
# MAGIC    'HZAwyvY5FKzWkP',
# MAGIC    'IPl7WsunideZRw')
# MAGIC    and  p_gateway in ('cybersource','hdfc','axis_migs') then 'internal'
# MAGIC    when    t_procurer = 'merchant' then 'external'
# MAGIC    else 'internal' END as `Internal vs External Flag`,
# MAGIC p_created_date
# MAGIC from (select * from whs_trino_v.optimizer_payments_flat_fact where p_created_date between '2024-09-01' and '2024-09-14') as optimizer_payments_flat_fact
# MAGIC left join realtime_hudi_api.cards on cards.id = optimizer_payments_flat_fact.p_card_id
# MAGIC --left join analytics_selfserve.optimizer_prepaid_live_gmv on optimizer_prepaid_live_gmv.payment_id = optimizer_payments_flat_fact.p_id
# MAGIC full outer join optimizer_age on optimizer_age.entity_id = optimizer_payments_flat_fact.merchant_id
# MAGIC left join  (select merchant_id, owner_role__c, team_owner, name as poc_name,owner_email  from aggregate_ba.final_team_tagging) account_owner_view
# MAGIC on optimizer_payments_flat_fact.merchant_id = account_owner_view.merchant_id
# MAGIC left join smart_router_2 s on optimizer_payments_flat_fact.p_id=s.write_key
# MAGIC left join total_smart_router_payments ts on optimizer_payments_flat_fact.p_id=ts.payment_id
# MAGIC --where
# MAGIC --p_created_date >= '2024-09-01'
# MAGIC --and p_created_date <= '2024-06-30'
# MAGIC GROUP BY
# MAGIC    p_gateway,
# MAGIC    p_base_amount_refunded,
# MAGIC    p_convert_currency,
# MAGIC    p_bank,
# MAGIC    p_recurring_type,
# MAGIC    p_fee_bearer,
# MAGIC    p_wallet,
# MAGIC    p_authentication_gateway,
# MAGIC    p_auth_type,
# MAGIC    case when p_authorized_at is not null then 1 else 0 end ,
# MAGIC    p_merchant_id,
# MAGIC    case when p_captured_at is not null then 1 else 0 end,
# MAGIC    cards.trivia    ,
# MAGIC    p_currency,
# MAGIC    p_method,
# MAGIC    p_verify_bucket,
# MAGIC    p_refund_status,
# MAGIC    p_international,
# MAGIC    p_receiver_type,
# MAGIC    p_verified,
# MAGIC    p_internal_error_code,
# MAGIC    p_error_code,
# MAGIC    p_status,
# MAGIC    p_recurring,
# MAGIC    p_settled_by,
# MAGIC    p_on_hold,
# MAGIC    p_gateway_captured,
# MAGIC    p_late_authorized,
# MAGIC    p_terminal_id,
# MAGIC    pa_library,
# MAGIC    pa_device,
# MAGIC    pa_platform,
# MAGIC    t_merchant_id,
# MAGIC    t_procurer,
# MAGIC    t_created_date,
# MAGIC    merchant_parent_id,
# MAGIC    category2,
# MAGIC    merchant_name,
# MAGIC    website,
# MAGIC    email,
# MAGIC    optimizer_payments_flat_fact.merchant_id,
# MAGIC    contact_email,
# MAGIC    contact_name,
# MAGIC    cards_type,
# MAGIC    cards_network,
# MAGIC    cards_issuer,
# MAGIC    upi_type,
# MAGIC    upi_provider,
# MAGIC    upi_app,
# MAGIC    parent_id,
# MAGIC    parent_name,
# MAGIC    salesforce_parent_id,
# MAGIC    golive_date,
# MAGIC    trial_end_date,
# MAGIC    recon_status,
# MAGIC    output_provider,
# MAGIC    input_provider,
# MAGIC    smart_router_terminal_id,
# MAGIC    input_priority,
# MAGIC    is_smart_router_payment,
# MAGIC    is_dynamic_routing_payment,
# MAGIC    case when golive_date  <= p_created_date then 1 else 0 end  ,
# MAGIC    billing_label,
# MAGIC    optimizer_age.feature_created_date,
# MAGIC optimizer_age.age,
# MAGIC team_owner,
# MAGIC poc_name ,
# MAGIC owner_email,
# MAGIC owner_role__c,
# MAGIC case when s.write_key is NULL then 0 else 1 end,
# MAGIC case when ts.payment_id is NULL then 0 else 1 end,
# MAGIC optimizer_payments_flat_fact.p_token_id,
# MAGIC p_cps_route,
# MAGIC CASE WHEN p_cps_route = 100 THEN 'Optimizer_Service'
# MAGIC WHEN p_cps_route = 7 THEN 'UPS'
# MAGIC ELSE 'API' END,
# MAGIC case when t_procurer='Razorpay' then 'Razorpay' else p_gateway end,
# MAGIC case when  t_procurer = 'merchant' and p_merchant_id in
# MAGIC    ('HsuMMq5AQBGCIW',
# MAGIC    'Hsup9WpX7ImZKw',
# MAGIC    'HsvZ5lz0dsDEJb',
# MAGIC    'HsvpIn3D8Cc6Zc',
# MAGIC    'GWjTMDeGwcVrr1',
# MAGIC    'HEaGxjXCXZUwSY',
# MAGIC    'GV7M7ZXLNzez0b',
# MAGIC    'IyYh0RU78PkNts',
# MAGIC    'HAdYq8orPiRhEw',
# MAGIC    'HThMRY5d6U3ald',
# MAGIC    'GA4CuzvbXn2kGF',
# MAGIC    'GA6Sj3FCr1X48P',
# MAGIC    'GA6zg8KmCvShIt',
# MAGIC    'GA7CHjv52jbuI6',
# MAGIC    'GA7ek79g40DCy9',
# MAGIC    'GA7Us9F5x4zS9E',
# MAGIC    'GA7ymrtIHoBmJ6',
# MAGIC    'GA8BEH81JBq9eA',
# MAGIC    'GA8Ml3740cOIQg',
# MAGIC    'H9ObHPX7JVb3ET',
# MAGIC    'HZAwyvY5FKzWkP',
# MAGIC    'IPl7WsunideZRw')
# MAGIC    and  p_gateway in ('cybersource','hdfc','axis_migs') then 'internal'
# MAGIC    when    t_procurer = 'merchant' then 'external'
# MAGIC    else 'internal' END,
# MAGIC    p_created_date;
# MAGIC
# MAGIC
