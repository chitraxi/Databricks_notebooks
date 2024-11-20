# Databricks notebook source
# MAGIC %sql
# MAGIC with base as (select  a.merchant_id,count(distinct case when authorized_at is not null and authorized_at != 0    and created_date between '2024-07-01' and '2024-07-31' then id end) as payments_T_2,
# MAGIC count(distinct case when authorized_at is not null and authorized_at != 0   and created_date between '2024-08-01' and '2024-08-31' then id end) as payments_T_1,count(case when internal_external_flag = 'external' and authorized_at is not null and authorized_at != 0  then id end) from (  select *   from aggregate_ba.payments_optimizer_flag_sincejan2021
# MAGIC where created_date between  '2024-07-01' and '2024-08-31'   and optimizer_flag = 1) as a  
# MAGIC inner join analytics_selfserve.optimizer_golive_date_base on a.merchant_id = optimizer_golive_date_base.merchant_id where optimizer_golive_date_base.golive_date <= a.created_date
# MAGIC group by 1 )
# MAGIC
# MAGIC select base.merchant_id, coalesce(billing_label,merchants.name) merchant_name, tag.team_owner, payments_T_2,payments_T_1 from base 
# MAGIC left join realtime_hudi_api.merchants on merchants.id = base.merchant_id
# MAGIC left join (
# MAGIC select merchant_id, owner_role__c, team_owner, name as poc_name from aggregate_ba.final_team_tagging) tag on tag.merchant_id = base.merchant_id 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC GMV for prepaid manually marked live

# COMMAND ----------

# MAGIC %sql
# MAGIC with base as (select  a.merchant_id,golive_date,sum(case when (authorized_at is not null and authorized_at != 0  )  and created_date between '2024-07-01' and '2024-07-31' then base_amount end) as GMV_T_2,
# MAGIC sum(case when (authorized_at is not null and authorized_at != 0 )  and created_date between '2024-08-01' and '2024-08-31' then base_amount end) as GMV_T_1,count(case when internal_external_flag = 'external' and authorized_at is not null and authorized_at != 0  then id end) from (  select * ,row_number() over (partition by id order by created_at) rank  from aggregate_ba.payments_optimizer_flag_sincejan2021
# MAGIC where created_date between  '2024-07-01' and '2024-08-31'   and optimizer_flag = 1 and merchant_id in  ('JRU7vMt2lXebz2',
# MAGIC 'KedRp2yHI3MfHo',
# MAGIC 'JVljhbCaidzJRH',
# MAGIC 'BDrq0OGyRMhAf5',
# MAGIC 'HvRpyYp154kz0o',
# MAGIC 'EFSCVQ7vFsCe5W',
# MAGIC '9N2BIh7AF9Ztom',
# MAGIC 'FjWuoIuiBFOfaY',
# MAGIC 'KVAsEdgo8GUkhU',
# MAGIC 'LXG3t5wtpJFkqE',
# MAGIC 'AqrJxrFnyImWwH',
# MAGIC 'Kehx617PnUXJPU',
# MAGIC 'FHZ3Iacdc1BwpC',
# MAGIC 'Ba1YcCCGawI0Ib',
# MAGIC 'BvOv0BXNWgBG1A',
# MAGIC 'JsSer7nviwgYGF',
# MAGIC 'Cx3J7rmA3Ew616',
# MAGIC 'Mk6w94pddx8NQB',
# MAGIC 'JyN10jk2hSjoMV',
# MAGIC 'EhVKfj0Hk2yR8S',
# MAGIC 'OMaY6GWTluPyHm',
# MAGIC 'BMYYnn9IkpH6TQ',
# MAGIC 'FYSORLd1jMqjJv',
# MAGIC 'IKdYkugT5iVQBT',
# MAGIC 'Kj45bZcMVBflzp',
# MAGIC 'GKNe4gss9tYIsx',
# MAGIC 'GojbHV6bQOb7cq',
# MAGIC 'E4m7d8Z275epyQ',
# MAGIC 'KlnrviDJDdOeYy',
# MAGIC 'HZe1xGNRnBPuJ7',
# MAGIC 'GdXKLvLaBU3p0t',
# MAGIC 'NK9nf6FFaFMMf4',
# MAGIC 'Dkwlba66uUF1DZ',
# MAGIC 'JvXCkRAM0YYvkD',
# MAGIC 'NHImd1BjlnnqeZ',
# MAGIC 'MSdgwMRJR4OIoi',
# MAGIC '80oXBj51MHGmwH',
# MAGIC 'Cudzs3Xs5BlhUU',
# MAGIC 'DQJLIFX9rkEzU2',
# MAGIC 'FfeRitz2zvHGRX',
# MAGIC 'AmrrAbWeO74K9q',
# MAGIC 'JxxIHwHNsSrVix',
# MAGIC '6p8ETF3R6hCy5J',
# MAGIC 'JvWY8y09kswqP0',
# MAGIC 'NbYI6rQAbozlKn',
# MAGIC 'AJialN10tzfuf8',
# MAGIC 'HNMcqV3pWnmdkS',
# MAGIC 'EkfzIpXteuDlCl',
# MAGIC 'Icr1hA4jfc5gM7',
# MAGIC 'AXRuIp5uiz5Jsp',
# MAGIC 'KSqGFuk2EdOxjr',
# MAGIC 'BUpnlzETSXDQ8f',
# MAGIC 'HqJO33Wh8pkIQb',
# MAGIC 'H1zVeL2BHor9N1',
# MAGIC '4FDX2N1e0SAnFN',
# MAGIC 'AisofeG4BcQddZ',
# MAGIC 'DNDwnIKxTmfrlR',
# MAGIC 'N4BznsWzLCODvG',
# MAGIC 'G7LH6TmlnLAXCb',
# MAGIC 'HDOqdveL1u2srl',
# MAGIC 'FDyCXUsMWEPgLe',
# MAGIC 'merchant_id',
# MAGIC 'OYo1cPuYXHfNUl',
# MAGIC 'MT4sO0dgAkZ1PS',
# MAGIC 'OhA5OtPUvsZhbr',
# MAGIC 'OCffDlpM993lB1',
# MAGIC 'MT4tTMifX0EZ4L',
# MAGIC 'LyuUmZXVGMXnd6',
# MAGIC '9mr3eFWa79LBay',
# MAGIC 'K8ZhElhXIsYtfu')) as a where 
# MAGIC  golive_date <= a.created_date and rank = 1 
# MAGIC group by 1,2 )
# MAGIC
# MAGIC select base.merchant_id,golive_date, coalesce(billing_label,merchants.name) merchant_name, tag.team_owner, GMV_T_2,GMV_T_1 from base 
# MAGIC left join realtime_hudi_api.merchants on merchants.id = base.merchant_id
# MAGIC left join (
# MAGIC select merchant_id, owner_role__c, team_owner, name as poc_name from aggregate_ba.final_team_tagging) tag on tag.merchant_id = base.merchant_id 

# COMMAND ----------

# MAGIC %md
# MAGIC Razorpay GMV

# COMMAND ----------

# MAGIC %sql
# MAGIC with base as (select  a.merchant_id,sum(case when (authorized_at is not null and authorized_at != 0  )  and created_date between '2024-07-01' and '2024-07-31' then base_amount end) as GMV_T_2,
# MAGIC sum(case when (authorized_at is not null and authorized_at != 0 )  and created_date between '2024-08-01' and '2024-08-31' then base_amount end) as GMV_T_1,count(case when internal_external_flag = 'external' and authorized_at is not null and authorized_at != 0  then id end) from (  select * ,row_number() over (partition by id ) rank  from aggregate_ba.payments_optimizer_flag_sincejan2021
# MAGIC where created_date between  '2024-07-01' and '2024-08-31'   and optimizer_flag = 1) as a  
# MAGIC inner join analytics_selfserve.optimizer_golive_date_base on a.merchant_id = optimizer_golive_date_base.merchant_id where optimizer_golive_date_base.golive_date <= a.created_date and rank = 1 and count(case when internal_external_flag = 'external' and authorized_at is not null and authorized_at != 0  then id end) = 0
# MAGIC group by 1 )
# MAGIC
# MAGIC select base.merchant_id, coalesce(billing_label,merchants.name) merchant_name, tag.team_owner, GMV_T_2,GMV_T_1 from base 
# MAGIC left join realtime_hudi_api.merchants on merchants.id = base.merchant_id
# MAGIC left join (
# MAGIC select merchant_id, owner_role__c, team_owner, name as poc_name from aggregate_ba.final_team_tagging) tag on tag.merchant_id = base.merchant_id 

# COMMAND ----------

# MAGIC %sql
# MAGIC select  merchant_id,try_cast(trim("golive_date") as date),golive_date  from (  select *   from aggregate_ba.payments_optimizer_flag_sincejan2021
# MAGIC where created_date between  '2024-07-01' and '2024-07-31'   and optimizer_flag = 1)  where  merchant_id = 'FDyCXUsMWEPgLe'
# MAGIC
