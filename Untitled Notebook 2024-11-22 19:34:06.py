# Databricks notebook source
# MAGIC %sql
# MAGIC select case when payment_id is not null then 'payment_id Availabel' else 'not available' end, count(*) from whs_v.events_payment_events_v2
# MAGIC where  producer_created_date >= '2024-11-15' and lower(event_name) = 'payment.creation.processed' 
# MAGIC group by 1
