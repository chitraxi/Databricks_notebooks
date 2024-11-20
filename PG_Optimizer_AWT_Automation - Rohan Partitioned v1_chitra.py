# Databricks notebook source
# MAGIC %md
# MAGIC ## List of issues -
# MAGIC 1. link not creating correctly 
# MAGIC 2. doc name incorrect
# MAGIC 5. Formatted and Holistic SR RCA output
# MAGIC 6. Refunds and Settlements coverage

# COMMAND ----------

# MAGIC %pip install --upgrade jinja2
# MAGIC %pip install python-docx
# MAGIC
# MAGIC import pandas as pd
# MAGIC import numpy as np
# MAGIC import shutil
# MAGIC from docx import Document
# MAGIC from docx.shared import Pt
# MAGIC from docx.shared import RGBColor
# MAGIC from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
# MAGIC from IPython.display import display, HTML
# MAGIC from datetime import datetime
# MAGIC from docx.shared import RGBColor
# MAGIC from pyspark.sql import SparkSession
# MAGIC from docx.shared import Inches
# MAGIC import matplotlib.pyplot as plt
# MAGIC from docx import Document
# MAGIC from docx.shared import Pt
# MAGIC import matplotlib.pyplot as plt
# MAGIC from docx.shared import Inches, Pt
# MAGIC from pyspark.sql.window import Window
# MAGIC import shutil
# MAGIC from docx import Document
# MAGIC from docx.shared import Inches
# MAGIC from datetime import datetime
# MAGIC from docx.shared import Pt
# MAGIC import docx.shared
# MAGIC
# MAGIC spark = SparkSession.builder.appName("PG_Optimizer_AWT_Automation") \
# MAGIC     .config("spark.sql.parquet.enableVectorizedReader", "false") \
# MAGIC     .config("spark.sql.parquet.dictionary.encoding.enabled", "false") \
# MAGIC     .getOrCreate()

# COMMAND ----------

start_date_prev_month = '2024-07-01'

t2_start = '2024-07-01'
t2_end = '2024-07-31'

t1_start = '2024-08-01'
t1_end = '2024-08-31'

refund_start_date = '2024-07-25'
refund_end_date = '2024-08-25'

# COMMAND ----------

from datetime import datetime, timedelta

total_days = (datetime.strptime(t1_end,'%Y-%m-%d') - datetime.strptime(start_date_prev_month,'%Y-%m-%d')).days + 1

print(f"Total days: {total_days}")
r=int((total_days / 3))+1

range1_start=start_date_prev_month
range1_end=(datetime.strptime(start_date_prev_month,'%Y-%m-%d')+timedelta(days=r - 1)).strftime('%Y-%m-%d')

range2_start=(datetime.strptime(start_date_prev_month,'%Y-%m-%d')+timedelta(days=r - 1)+ timedelta(days=1)).strftime('%Y-%m-%d')
range2_end=(datetime.strptime((datetime.strptime(start_date_prev_month,'%Y-%m-%d')+timedelta(days=r - 1)+ timedelta(days=1)).strftime('%Y-%m-%d'),'%Y-%m-%d')+timedelta(days=r - 1)).strftime('%Y-%m-%d')

range3_start=(datetime.strptime((datetime.strptime(start_date_prev_month,'%Y-%m-%d')+timedelta(days=r - 1)+ timedelta(days=1)).strftime('%Y-%m-%d'),'%Y-%m-%d')+timedelta(days=r - 1)+ timedelta(days=1)).strftime('%Y-%m-%d')
range3_end=t1_end

print(range1_start,"-",range1_end)
print(range2_start,"-",range2_end)
print(range3_start,"-",range3_end)

# COMMAND ----------

# Execute Spark SQL query with global filters
payments1 = spark.sql(f"""
        SELECT
        a.merchant_id, 
        COALESCE(a.dba,b.billing_label, b.name) AS Name,
        c.team_owner AS `Team Mapping`,
        a.golive_date  as golive_date,   
        a.first_transaction_date,
        CASE 
        WHEN a.internal_external_flag = 'external' THEN gateway
        WHEN a.internal_external_flag = 'internal' THEN 'Razorpay' else null end as gateway,
        a.method_advanced,
        a.network,
        a.network_tokenised_payment,
        cast(d.late_authorized as varchar (10)) late_authorized, 
        IF(a.internal_error_code = '' OR a.internal_error_code IS NULL, 'Success', a.internal_error_code) AS Error_Code,
        CASE
            WHEN a.internal_error_code IN (
                'BAD_REQUEST_CARD_DISABLED_FOR_ONLINE_PAYMENTS',
                'SERVER_ERROR_OTP_ELF_INVALID_ARGUMENT',
                'BAD_REQUEST_PAYMENT_CUSTOMER_DROPPED_OFF',
                'BAD_REQUEST_PAYMENT_CANCELLED',
                'BAD_REQUEST_CARD_CREDIT_LIMIT_REACHED',
                'GATEWAY_ERROR_CREDIT_FAILED',
                'GATEWAY_ERROR_BENEFICIARY_BANK_DEEMED_HIGH_RESPONSE_TIME_CHECK_DECLINE',
                'GATEWAY_ERROR_REMITTER_BANK_DEEMED_HIGH_RESPONSE_TIME_CHECK_DECLINE',
                'GATEWAY_ERROR_MANDATE_CREATION_EXPIRED',
                'GATEWAY_ERROR_REQAUTH_DECLINED',
                'BAD_REQUEST_PAYMENT_DECLINED_BY_CUSTOMER',
                'BAD_REQUEST_TRANSACTION_AMOUNT_LIMIT_EXCEEDED',
                'GATEWAY_ERROR_PAYMENT_FAILED',
                'BAD_REQUEST_TRANSACTION_AMOUNT_LIMIT_AT_REMITTER_EXCEEDED',
                'GATEWAY_ERROR_REMITTER_BANK_NOT_AVAILABLE',
                'GATEWAY_ERROR_PAYER_PSP_NOT_AVAILABLE',
                'BAD_REQUEST_TRANSACTION_FREQUENCY_LIMIT_AT_REMITTER_EXCEEDED',
                'GATEWAY_ERROR_MANDATE_CREATION_TIMEOUT_AT_REMITTER_END',
                'GATEWAY_ERROR_REMITTER_CUTOFF_IN_PROGRESS',
                'BAD_REQUEST_PAYMENT_UPI_MANDATE_REJECTED',
                'BAD_REQUEST_CARD_MANDATE_IS_NOT_ACTIVE',
                'GATEWAY_ERROR_SUPPORT_AUTH_NOT_FOUND',
                'BAD_REQUEST_ERROR_OTP_ELF_INVALID_OTP_LENGTH',
                'BAD_REQUEST_CARD_MANDATE_CUSTOMER_NOT_APPROVED',
                'BAD_REQUEST_USER_NOT_AUTHENTICATED',
                'BAD_REQUEST_CARD_INVALID_DATA',
                'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
                'BAD_REQUEST_CARD_MANDATE_MANDATE_NOT_ACTIVE',
                'GATEWAY_ERROR_MANDATE_CREATION_DECLINED_BY_REMITTER_BANK',
                'GATEWAY_ERROR_PAYMENT_NOT_FOUND',
                'BAD_REQUEST_AUTHENTICATION_FAILED',
                'BAD_REQUEST_EMANDATE_REGISTRATION_ALREADY_DECLINED_BY_BANK',
                'BAD_REQUEST_EMANDATE_REGISTRATION_ALREADY_IN_PROGRESS',
                'BAD_REQUEST_PAYMENT_PENDING',
                'BAD_REQUEST_PAYMENT_CANCELLED_AT_EMANDATE_REGISTRATION',
                'GATEWAY_ERROR_PAYMENT_NOT_FOUND',
                'GATEWAY_ERROR_PAYMENT_FAILED',
                'BAD_REQUEST_ACCOUNT_BLOCKED',
                'BAD_REQUEST_ACCOUNT_CLOSED',
                'BAD_REQUEST_EMANDATE_AMOUNT_LIMIT_EXCEEDED',
                'BAD_REQUEST_EMANDATE_CANCELLED_INACTIVE',
                'BAD_REQUEST_EMANDATE_REGISTRATION_ACTION_NEEDED',
                'BAD_REQUEST_EMANDATE_REGISTRATION_FAILED',
                'BAD_REQUEST_GATEWAY_TOKEN_EMPTY',
                'BAD_REQUEST_INVALID_ACCOUNT_HOLDER_NAME',
                'BAD_REQUEST_INVALID_TRANSACTION_AMOUNT',
                'BAD_REQUEST_INVALID_USER_CREDENTIALS',
                'BAD_REQUEST_NETBANKING_USER_NOT_REGISTERED',
                'BAD_REQUEST_NO_DR_ALLOWED',
                'BAD_REQUEST_PAYMENT_ACCOUNT_INSUFFICIENT_BALANCE',
                'BAD_REQUEST_PAYMENT_ACCOUNT_MAX_LIMIT_EXCEEDED',
                'BAD_REQUEST_PAYMENT_ACCOUNT_WITHDRAWAL_FROZEN',
                'BAD_REQUEST_PAYMENT_CANCELLED_BY_CUSTOMER',
                'BAD_REQUEST_PAYMENT_CANCELLED_BY_USER',
                'BAD_REQUEST_PAYMENT_FAILED',
                'BAD_REQUEST_PAYMENT_INVALID_ACCOUNT',
                'BAD_REQUEST_PAYMENT_INVALID_AMOUNT_OR_CURRENCY',
                'BAD_REQUEST_PAYMENT_KYC_PENDING',
                'BAD_REQUEST_PAYMENT_NETBANKING_CANCELLED_BY_USER',
                'BAD_REQUEST_PAYMENT_POSSIBLE_FRAUD',
                'BAD_REQUEST_PAYMENT_POSSIBLE_FRAUD_WEBSITE_MISMATCH',
                'BAD_REQUEST_PAYMENT_TIMED_OUT',
                'BAD_REQUEST_PAYMENT_VERIFICATION_FAILED',
                'BAD_REQUEST_TRANSACTION_AMOUNT_LIMIT_EXCEEDED',
                'GATEWAY_ERROR_AMOUNT_TAMPERED',
                'GATEWAY_ERROR_CALLBACK_EMPTY_INPUT',
                'GATEWAY_ERROR_DEBIT_BEFORE_MANDATE_START',
                'GATEWAY_ERROR_DEBIT_FAILED',
                'GATEWAY_ERROR_FATAL_ERROR',
                'GATEWAY_ERROR_INVALID_PARAMETERS',
                'GATEWAY_ERROR_INVALID_RESPONSE',
                'GATEWAY_ERROR_ISSUER_DOWN',
                'GATEWAY_ERROR_MANDATE_CREATION_FAILED',
                'GATEWAY_ERROR_MERCHANT_IP_NOT_WHITELISTED',
                'GATEWAY_ERROR_PAYMENT_VERIFICATION_ERROR',
                'GATEWAY_ERROR_REQUEST_ERROR',
                'GATEWAY_ERROR_REQUEST_TIMEOUT',
                'GATEWAY_ERROR_UNKNOWN_ERROR',
                'SERVER_ERROR_AMOUNT_TAMPERED',
                'SERVER_ERROR_ASSERTION_ERROR',
                'BAD_REQUEST_EMANDATE_REGISTRATION_FAILED_JOINT_ACCOUNT',
                'BAD_REQUEST_EMANDATE_REGISTRATION_DUPLICATE_REQUEST',
                'BAD_REQUEST_PAYMENT_OTP_INCORRECT',
                'BAD_REQUEST_PAYMENT_CARD_INVALID_PIN',
                'BAD_REQUEST_PAYMENT_OTP_VALIDATION_ATTEMPT_LIMIT_EXCEEDED',
                'BAD_REQUEST_PAYMENT_CARD_EXPIRED',
                'BAD_REQUEST_PAYMENT_CARD_NUMBER_POSSIBLY_INVALID',
                'SERVER_ERROR_NBPLUS_PAYMENT_SERVICE_FAILURE',
                'GATEWAY_ERROR_COMMUNICATION_ERROR',
                'BAD_REQUEST_PAYMENT_DECLINED_BY_BANK_DUE_TO_BLOCKED_CARD',
                'BAD_REQUEST_EMANDATE_INACTIVE',
                'BAD_REQUEST_CARD_INACTIVE',
                'BAD_REQUEST_PAYMENT_CARD_INVALID_CVV',
                'BAD_REQUEST_VALIDATION_FAILURE',
                'BAD_REQUEST_ACCOUNT_DORMANT',
                'BAD_REQUEST_PAYMENT_CARD_INVALID_EXPIRY_DATE',
                'BAD_REQUEST_PAYMENT_ALREADY_PROCESSED',
                'SERVER_ERROR_RUNTIME_ERROR',
                'GATEWAY_ERROR_CHECKSUM_MATCH_FAILED') THEN 
                CASE
                    WHEN a.internal_error_code IN (
                        'BAD_REQUEST_CARD_DISABLED_FOR_ONLINE_PAYMENTS',
                        'BAD_REQUEST_PAYMENT_CUSTOMER_DROPPED_OFF',
                        'BAD_REQUEST_PAYMENT_CANCELLED',
                        'BAD_REQUEST_CARD_CREDIT_LIMIT_REACHED',
                        'BAD_REQUEST_PAYMENT_DECLINED_BY_CUSTOMER',
                        'BAD_REQUEST_TRANSACTION_AMOUNT_LIMIT_EXCEEDED',
                        'BAD_REQUEST_PAYMENT_UPI_MANDATE_REJECTED',
                        'BAD_REQUEST_CARD_MANDATE_IS_NOT_ACTIVE',
                        'BAD_REQUEST_CARD_INVALID_DATA',
                        'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
                        'BAD_REQUEST_CARD_MANDATE_MANDATE_NOT_ACTIVE',
                        'BAD_REQUEST_AUTHENTICATION_FAILED',
                        'BAD_REQUEST_EMANDATE_REGISTRATION_ALREADY_DECLINED_BY_BANK',
                        'BAD_REQUEST_EMANDATE_REGISTRATION_ALREADY_IN_PROGRESS',
                        'BAD_REQUEST_PAYMENT_PENDING',
                        'BAD_REQUEST_PAYMENT_CANCELLED_AT_EMANDATE_REGISTRATION',
                        'BAD_REQUEST_ACCOUNT_BLOCKED',
                        'BAD_REQUEST_ACCOUNT_CLOSED',
                        'BAD_REQUEST_EMANDATE_CANCELLED_INACTIVE',
                        'BAD_REQUEST_EMANDATE_REGISTRATION_ACTION_NEEDED',
                        'BAD_REQUEST_GATEWAY_TOKEN_EMPTY',
                        'BAD_REQUEST_INVALID_ACCOUNT_HOLDER_NAME',
                        'BAD_REQUEST_INVALID_TRANSACTION_AMOUNT',
                        'BAD_REQUEST_INVALID_USER_CREDENTIALS',
                        'BAD_REQUEST_NETBANKING_USER_NOT_REGISTERED',
                        'BAD_REQUEST_NO_DR_ALLOWED',
                        'BAD_REQUEST_PAYMENT_ACCOUNT_INSUFFICIENT_BALANCE',
                        'BAD_REQUEST_PAYMENT_ACCOUNT_MAX_LIMIT_EXCEEDED',
                        'BAD_REQUEST_PAYMENT_ACCOUNT_WITHDRAWAL_FROZEN',
                        'BAD_REQUEST_PAYMENT_CANCELLED_BY_CUSTOMER',
                        'BAD_REQUEST_PAYMENT_CANCELLED_BY_USER',
                        'BAD_REQUEST_PAYMENT_FAILED',
                        'BAD_REQUEST_PAYMENT_INVALID_ACCOUNT',
                        'BAD_REQUEST_PAYMENT_INVALID_AMOUNT_OR_CURRENCY',
                        'BAD_REQUEST_PAYMENT_KYC_PENDING',
                        'BAD_REQUEST_PAYMENT_NETBANKING_CANCELLED_BY_USER',
                        'BAD_REQUEST_PAYMENT_POSSIBLE_FRAUD',
                        'BAD_REQUEST_PAYMENT_POSSIBLE_FRAUD_WEBSITE_MISMATCH',
                        'BAD_REQUEST_PAYMENT_TIMED_OUT',
                        'BAD_REQUEST_PAYMENT_VERIFICATION_FAILED',
                        'BAD_REQUEST_TRANSACTION_AMOUNT_LIMIT_EXCEEDED',
                        'BAD_REQUEST_EMANDATE_REGISTRATION_FAILED_JOINT_ACCOUNT',
                        'BAD_REQUEST_EMANDATE_REGISTRATION_DUPLICATE_REQUEST',
                        'BAD_REQUEST_PAYMENT_OTP_INCORRECT',
                        'BAD_REQUEST_PAYMENT_CARD_INVALID_PIN',
                        'BAD_REQUEST_PAYMENT_OTP_VALIDATION_ATTEMPT_LIMIT_EXCEEDED',
                        'BAD_REQUEST_PAYMENT_CARD_EXPIRED',
                        'BAD_REQUEST_PAYMENT_CARD_NUMBER_POSSIBLY_INVALID',
                        'BAD_REQUEST_PAYMENT_DECLINED_BY_BANK_DUE_TO_BLOCKED_CARD',
                        'BAD_REQUEST_EMANDATE_INACTIVE',
                        'BAD_REQUEST_CARD_INACTIVE',
                        'BAD_REQUEST_PAYMENT_CARD_INVALID_CVV',
                        'BAD_REQUEST_VALIDATION_FAILURE',
                        'BAD_REQUEST_ACCOUNT_DORMANT',
                        'BAD_REQUEST_PAYMENT_CARD_INVALID_EXPIRY_DATE',
                        'SERVER_ERROR_RUNTIME_ERROR'
                    ) THEN 'customer'
                    WHEN a.internal_error_code IN (
                        'SERVER_ERROR_OTP_ELF_INVALID_ARGUMENT',
                        'GATEWAY_ERROR_REQAUTH_DECLINED',
                        'GATEWAY_ERROR_PAYMENT_VERIFICATION_ERROR',
                        'GATEWAY_ERROR_REQUEST_ERROR',
                        'GATEWAY_ERROR_UNKNOWN_ERROR',
                        'SERVER_ERROR_AMOUNT_TAMPERED',
                        'SERVER_ERROR_ASSERTION_ERROR',
                        'GATEWAY_ERROR_CHECKSUM_MATCH_FAILED'
                    ) THEN 'internal'
                    WHEN a.internal_error_code IN (
                        'GATEWAY_ERROR_CREDIT_FAILED',
                        'GATEWAY_ERROR_BENEFICIARY_BANK_DEEMED_HIGH_RESPONSE_TIME_CHECK_DECLINE',
                        'GATEWAY_ERROR_REMITTER_BANK_DEEMED_HIGH_RESPONSE_TIME_CHECK_DECLINE',
                        'GATEWAY_ERROR_PAYMENT_FAILED',
                        'GATEWAY_ERROR_MANDATE_CREATION_TIMEOUT_AT_REMITTER_END',
                        'GATEWAY_ERROR_REMITTER_CUTOFF_IN_PROGRESS',
                        'GATEWAY_ERROR_SUPPORT_AUTH_NOT_FOUND',
                        'GATEWAY_ERROR_FATAL_ERROR',
                        'GATEWAY_ERROR_INVALID_PARAMETERS',
                        'GATEWAY_ERROR_INVALID_RESPONSE',
                        'GATEWAY_ERROR_ISSUER_DOWN',
                        'GATEWAY_ERROR_MANDATE_CREATION_FAILED',
                        'GATEWAY_ERROR_MERCHANT_IP_NOT_WHITELISTED',
                        'GATEWAY_ERROR_COMMUNICATION_ERROR',
                        'GATEWAY_ERROR_REQUEST_TIMEOUT'
                    ) THEN 'gateway'
                    ELSE 'business'
                END
            ELSE a.internal_error_code
        END AS `Error Source`,
        a.created_date,
        CASE 
            WHEN cast(a.created_date as date) BETWEEN date '{t2_start}' AND date '{t2_end}' THEN 'T-2'
            WHEN cast(a.created_date as date) BETWEEN date '{t1_start}' AND date '{t1_end}' THEN 'T-1'
            ELSE NULL
        END AS Period,
        IF (DAY(date(a.created_date)) < day(current_date),1,0) MTD_check,
        CASE 
            WHEN a.authorized_at is null then 'Unauthorized'
            WHEN a.authorized_at is not null and (a.authorized_at - a.created_at) BETWEEN 0 and 120 then 'a. less than 2 mins'
            WHEN a.authorized_at is not null and (a.authorized_at - a.created_at) BETWEEN 121 and 300 then 'b. 2-5 mins'
            WHEN a.authorized_at is not null and (a.authorized_at - a.created_at) BETWEEN 301 and 480 then 'c. 5-8 mins'
            WHEN a.authorized_at is not null and (a.authorized_at - a.created_at) BETWEEN 481 and 720 then 'd. 8- 12 mins'
            WHEN a.authorized_at is not null and (a.authorized_at - a.created_at) > 720 then 'e. > 12 mins' end as auth_tat,
            d.auth_type,
           
            a.issuer,
            
        sum(a.base_amount/100) AS `GMV`,
        count(distinct CASE WHEN authorized_at IS NOT NULL and authorized_at != 0 THEN a.id ELSE NULL END) AS `Successful payments`,
        count(distinct a.id) AS `Total Payments`
    FROM 
        (select * from aggregate_ba.payments_optimizer_flag_sincejan2021 where created_date between  '{range1_start}' AND  '{range1_end}' and optimizer_flag = 1) a 
        LEFT JOIN (SELECT id, billing_label, name FROM realtime_hudi_api.merchants) b ON a.merchant_id = b.id
        LEFT JOIN (SELECT * FROM aggregate_ba.final_team_tagging) c ON a.merchant_id = c.merchant_id
        INNER JOIN (
        select id , late_authorized, auth_type ,created_date from realtime_hudi_api.payments
        where created_date between  '{range1_start}' AND  '{range1_end}' ) d ON a.id = d.id
        --LEFT JOIN analytics_selfserve.optimizer_native_otp_abtesting_enabled_3ds_fallback_payments e on a.id = e.payment_id
        --LEFT JOIN analytics_selfserve.optimizer_native_otp_abtesting_enabled_payments f on a.id = f.payment_id
    WHERE 
        date_trunc('month', date(a.created_date)) >= date_add(month,-2,date_trunc('month',current_date))
        AND a.optimizer_flag = 1
    GROUP BY all 
""" )

# COMMAND ----------

# Execute Spark SQL query with global filters
payments2 = spark.sql(f"""
        SELECT
        a.merchant_id, 
        COALESCE(a.dba,b.billing_label, b.name) AS Name,
        c.team_owner AS `Team Mapping`,
        min(try_cast(a.golive_date as date)) as golive_date, ,  
        a.first_transaction_date,
        CASE 
        WHEN a.internal_external_flag = 'external' THEN gateway
        WHEN a.internal_external_flag = 'internal' THEN 'Razorpay' else null end as gateway,
        a.method_advanced,
        a.network,
        a.network_tokenised_payment,
        cast(d.late_authorized as varchar (10)) late_authorized, 
        IF(a.internal_error_code = '' OR a.internal_error_code IS NULL, 'Success', a.internal_error_code) AS Error_Code,
        CASE
            WHEN a.internal_error_code IN (
                'BAD_REQUEST_CARD_DISABLED_FOR_ONLINE_PAYMENTS',
                'SERVER_ERROR_OTP_ELF_INVALID_ARGUMENT',
                'BAD_REQUEST_PAYMENT_CUSTOMER_DROPPED_OFF',
                'BAD_REQUEST_PAYMENT_CANCELLED',
                'BAD_REQUEST_CARD_CREDIT_LIMIT_REACHED',
                'GATEWAY_ERROR_CREDIT_FAILED',
                'GATEWAY_ERROR_BENEFICIARY_BANK_DEEMED_HIGH_RESPONSE_TIME_CHECK_DECLINE',
                'GATEWAY_ERROR_REMITTER_BANK_DEEMED_HIGH_RESPONSE_TIME_CHECK_DECLINE',
                'GATEWAY_ERROR_MANDATE_CREATION_EXPIRED',
                'GATEWAY_ERROR_REQAUTH_DECLINED',
                'BAD_REQUEST_PAYMENT_DECLINED_BY_CUSTOMER',
                'BAD_REQUEST_TRANSACTION_AMOUNT_LIMIT_EXCEEDED',
                'GATEWAY_ERROR_PAYMENT_FAILED',
                'BAD_REQUEST_TRANSACTION_AMOUNT_LIMIT_AT_REMITTER_EXCEEDED',
                'GATEWAY_ERROR_REMITTER_BANK_NOT_AVAILABLE',
                'GATEWAY_ERROR_PAYER_PSP_NOT_AVAILABLE',
                'BAD_REQUEST_TRANSACTION_FREQUENCY_LIMIT_AT_REMITTER_EXCEEDED',
                'GATEWAY_ERROR_MANDATE_CREATION_TIMEOUT_AT_REMITTER_END',
                'GATEWAY_ERROR_REMITTER_CUTOFF_IN_PROGRESS',
                'BAD_REQUEST_PAYMENT_UPI_MANDATE_REJECTED',
                'BAD_REQUEST_CARD_MANDATE_IS_NOT_ACTIVE',
                'GATEWAY_ERROR_SUPPORT_AUTH_NOT_FOUND',
                'BAD_REQUEST_ERROR_OTP_ELF_INVALID_OTP_LENGTH',
                'BAD_REQUEST_CARD_MANDATE_CUSTOMER_NOT_APPROVED',
                'BAD_REQUEST_USER_NOT_AUTHENTICATED',
                'BAD_REQUEST_CARD_INVALID_DATA',
                'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
                'BAD_REQUEST_CARD_MANDATE_MANDATE_NOT_ACTIVE',
                'GATEWAY_ERROR_MANDATE_CREATION_DECLINED_BY_REMITTER_BANK',
                'GATEWAY_ERROR_PAYMENT_NOT_FOUND',
                'BAD_REQUEST_AUTHENTICATION_FAILED',
                'BAD_REQUEST_EMANDATE_REGISTRATION_ALREADY_DECLINED_BY_BANK',
                'BAD_REQUEST_EMANDATE_REGISTRATION_ALREADY_IN_PROGRESS',
                'BAD_REQUEST_PAYMENT_PENDING',
                'BAD_REQUEST_PAYMENT_CANCELLED_AT_EMANDATE_REGISTRATION',
                'GATEWAY_ERROR_PAYMENT_NOT_FOUND',
                'GATEWAY_ERROR_PAYMENT_FAILED',
                'BAD_REQUEST_ACCOUNT_BLOCKED',
                'BAD_REQUEST_ACCOUNT_CLOSED',
                'BAD_REQUEST_EMANDATE_AMOUNT_LIMIT_EXCEEDED',
                'BAD_REQUEST_EMANDATE_CANCELLED_INACTIVE',
                'BAD_REQUEST_EMANDATE_REGISTRATION_ACTION_NEEDED',
                'BAD_REQUEST_EMANDATE_REGISTRATION_FAILED',
                'BAD_REQUEST_GATEWAY_TOKEN_EMPTY',
                'BAD_REQUEST_INVALID_ACCOUNT_HOLDER_NAME',
                'BAD_REQUEST_INVALID_TRANSACTION_AMOUNT',
                'BAD_REQUEST_INVALID_USER_CREDENTIALS',
                'BAD_REQUEST_NETBANKING_USER_NOT_REGISTERED',
                'BAD_REQUEST_NO_DR_ALLOWED',
                'BAD_REQUEST_PAYMENT_ACCOUNT_INSUFFICIENT_BALANCE',
                'BAD_REQUEST_PAYMENT_ACCOUNT_MAX_LIMIT_EXCEEDED',
                'BAD_REQUEST_PAYMENT_ACCOUNT_WITHDRAWAL_FROZEN',
                'BAD_REQUEST_PAYMENT_CANCELLED_BY_CUSTOMER',
                'BAD_REQUEST_PAYMENT_CANCELLED_BY_USER',
                'BAD_REQUEST_PAYMENT_FAILED',
                'BAD_REQUEST_PAYMENT_INVALID_ACCOUNT',
                'BAD_REQUEST_PAYMENT_INVALID_AMOUNT_OR_CURRENCY',
                'BAD_REQUEST_PAYMENT_KYC_PENDING',
                'BAD_REQUEST_PAYMENT_NETBANKING_CANCELLED_BY_USER',
                'BAD_REQUEST_PAYMENT_POSSIBLE_FRAUD',
                'BAD_REQUEST_PAYMENT_POSSIBLE_FRAUD_WEBSITE_MISMATCH',
                'BAD_REQUEST_PAYMENT_TIMED_OUT',
                'BAD_REQUEST_PAYMENT_VERIFICATION_FAILED',
                'BAD_REQUEST_TRANSACTION_AMOUNT_LIMIT_EXCEEDED',
                'GATEWAY_ERROR_AMOUNT_TAMPERED',
                'GATEWAY_ERROR_CALLBACK_EMPTY_INPUT',
                'GATEWAY_ERROR_DEBIT_BEFORE_MANDATE_START',
                'GATEWAY_ERROR_DEBIT_FAILED',
                'GATEWAY_ERROR_FATAL_ERROR',
                'GATEWAY_ERROR_INVALID_PARAMETERS',
                'GATEWAY_ERROR_INVALID_RESPONSE',
                'GATEWAY_ERROR_ISSUER_DOWN',
                'GATEWAY_ERROR_MANDATE_CREATION_FAILED',
                'GATEWAY_ERROR_MERCHANT_IP_NOT_WHITELISTED',
                'GATEWAY_ERROR_PAYMENT_VERIFICATION_ERROR',
                'GATEWAY_ERROR_REQUEST_ERROR',
                'GATEWAY_ERROR_REQUEST_TIMEOUT',
                'GATEWAY_ERROR_UNKNOWN_ERROR',
                'SERVER_ERROR_AMOUNT_TAMPERED',
                'SERVER_ERROR_ASSERTION_ERROR',
                'BAD_REQUEST_EMANDATE_REGISTRATION_FAILED_JOINT_ACCOUNT',
                'BAD_REQUEST_EMANDATE_REGISTRATION_DUPLICATE_REQUEST',
                'BAD_REQUEST_PAYMENT_OTP_INCORRECT',
                'BAD_REQUEST_PAYMENT_CARD_INVALID_PIN',
                'BAD_REQUEST_PAYMENT_OTP_VALIDATION_ATTEMPT_LIMIT_EXCEEDED',
                'BAD_REQUEST_PAYMENT_CARD_EXPIRED',
                'BAD_REQUEST_PAYMENT_CARD_NUMBER_POSSIBLY_INVALID',
                'SERVER_ERROR_NBPLUS_PAYMENT_SERVICE_FAILURE',
                'GATEWAY_ERROR_COMMUNICATION_ERROR',
                'BAD_REQUEST_PAYMENT_DECLINED_BY_BANK_DUE_TO_BLOCKED_CARD',
                'BAD_REQUEST_EMANDATE_INACTIVE',
                'BAD_REQUEST_CARD_INACTIVE',
                'BAD_REQUEST_PAYMENT_CARD_INVALID_CVV',
                'BAD_REQUEST_VALIDATION_FAILURE',
                'BAD_REQUEST_ACCOUNT_DORMANT',
                'BAD_REQUEST_PAYMENT_CARD_INVALID_EXPIRY_DATE',
                'BAD_REQUEST_PAYMENT_ALREADY_PROCESSED',
                'SERVER_ERROR_RUNTIME_ERROR',
                'GATEWAY_ERROR_CHECKSUM_MATCH_FAILED') THEN 
                CASE
                    WHEN a.internal_error_code IN (
                        'BAD_REQUEST_CARD_DISABLED_FOR_ONLINE_PAYMENTS',
                        'BAD_REQUEST_PAYMENT_CUSTOMER_DROPPED_OFF',
                        'BAD_REQUEST_PAYMENT_CANCELLED',
                        'BAD_REQUEST_CARD_CREDIT_LIMIT_REACHED',
                        'BAD_REQUEST_PAYMENT_DECLINED_BY_CUSTOMER',
                        'BAD_REQUEST_TRANSACTION_AMOUNT_LIMIT_EXCEEDED',
                        'BAD_REQUEST_PAYMENT_UPI_MANDATE_REJECTED',
                        'BAD_REQUEST_CARD_MANDATE_IS_NOT_ACTIVE',
                        'BAD_REQUEST_CARD_INVALID_DATA',
                        'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
                        'BAD_REQUEST_CARD_MANDATE_MANDATE_NOT_ACTIVE',
                        'BAD_REQUEST_AUTHENTICATION_FAILED',
                        'BAD_REQUEST_EMANDATE_REGISTRATION_ALREADY_DECLINED_BY_BANK',
                        'BAD_REQUEST_EMANDATE_REGISTRATION_ALREADY_IN_PROGRESS',
                        'BAD_REQUEST_PAYMENT_PENDING',
                        'BAD_REQUEST_PAYMENT_CANCELLED_AT_EMANDATE_REGISTRATION',
                        'BAD_REQUEST_ACCOUNT_BLOCKED',
                        'BAD_REQUEST_ACCOUNT_CLOSED',
                        'BAD_REQUEST_EMANDATE_CANCELLED_INACTIVE',
                        'BAD_REQUEST_EMANDATE_REGISTRATION_ACTION_NEEDED',
                        'BAD_REQUEST_GATEWAY_TOKEN_EMPTY',
                        'BAD_REQUEST_INVALID_ACCOUNT_HOLDER_NAME',
                        'BAD_REQUEST_INVALID_TRANSACTION_AMOUNT',
                        'BAD_REQUEST_INVALID_USER_CREDENTIALS',
                        'BAD_REQUEST_NETBANKING_USER_NOT_REGISTERED',
                        'BAD_REQUEST_NO_DR_ALLOWED',
                        'BAD_REQUEST_PAYMENT_ACCOUNT_INSUFFICIENT_BALANCE',
                        'BAD_REQUEST_PAYMENT_ACCOUNT_MAX_LIMIT_EXCEEDED',
                        'BAD_REQUEST_PAYMENT_ACCOUNT_WITHDRAWAL_FROZEN',
                        'BAD_REQUEST_PAYMENT_CANCELLED_BY_CUSTOMER',
                        'BAD_REQUEST_PAYMENT_CANCELLED_BY_USER',
                        'BAD_REQUEST_PAYMENT_FAILED',
                        'BAD_REQUEST_PAYMENT_INVALID_ACCOUNT',
                        'BAD_REQUEST_PAYMENT_INVALID_AMOUNT_OR_CURRENCY',
                        'BAD_REQUEST_PAYMENT_KYC_PENDING',
                        'BAD_REQUEST_PAYMENT_NETBANKING_CANCELLED_BY_USER',
                        'BAD_REQUEST_PAYMENT_POSSIBLE_FRAUD',
                        'BAD_REQUEST_PAYMENT_POSSIBLE_FRAUD_WEBSITE_MISMATCH',
                        'BAD_REQUEST_PAYMENT_TIMED_OUT',
                        'BAD_REQUEST_PAYMENT_VERIFICATION_FAILED',
                        'BAD_REQUEST_TRANSACTION_AMOUNT_LIMIT_EXCEEDED',
                        'BAD_REQUEST_EMANDATE_REGISTRATION_FAILED_JOINT_ACCOUNT',
                        'BAD_REQUEST_EMANDATE_REGISTRATION_DUPLICATE_REQUEST',
                        'BAD_REQUEST_PAYMENT_OTP_INCORRECT',
                        'BAD_REQUEST_PAYMENT_CARD_INVALID_PIN',
                        'BAD_REQUEST_PAYMENT_OTP_VALIDATION_ATTEMPT_LIMIT_EXCEEDED',
                        'BAD_REQUEST_PAYMENT_CARD_EXPIRED',
                        'BAD_REQUEST_PAYMENT_CARD_NUMBER_POSSIBLY_INVALID',
                        'BAD_REQUEST_PAYMENT_DECLINED_BY_BANK_DUE_TO_BLOCKED_CARD',
                        'BAD_REQUEST_EMANDATE_INACTIVE',
                        'BAD_REQUEST_CARD_INACTIVE',
                        'BAD_REQUEST_PAYMENT_CARD_INVALID_CVV',
                        'BAD_REQUEST_VALIDATION_FAILURE',
                        'BAD_REQUEST_ACCOUNT_DORMANT',
                        'BAD_REQUEST_PAYMENT_CARD_INVALID_EXPIRY_DATE',
                        'SERVER_ERROR_RUNTIME_ERROR'
                    ) THEN 'customer'
                    WHEN a.internal_error_code IN (
                        'SERVER_ERROR_OTP_ELF_INVALID_ARGUMENT',
                        'GATEWAY_ERROR_REQAUTH_DECLINED',
                        'GATEWAY_ERROR_PAYMENT_VERIFICATION_ERROR',
                        'GATEWAY_ERROR_REQUEST_ERROR',
                        'GATEWAY_ERROR_UNKNOWN_ERROR',
                        'SERVER_ERROR_AMOUNT_TAMPERED',
                        'SERVER_ERROR_ASSERTION_ERROR',
                        'GATEWAY_ERROR_CHECKSUM_MATCH_FAILED'
                    ) THEN 'internal'
                    WHEN a.internal_error_code IN (
                        'GATEWAY_ERROR_CREDIT_FAILED',
                        'GATEWAY_ERROR_BENEFICIARY_BANK_DEEMED_HIGH_RESPONSE_TIME_CHECK_DECLINE',
                        'GATEWAY_ERROR_REMITTER_BANK_DEEMED_HIGH_RESPONSE_TIME_CHECK_DECLINE',
                        'GATEWAY_ERROR_PAYMENT_FAILED',
                        'GATEWAY_ERROR_MANDATE_CREATION_TIMEOUT_AT_REMITTER_END',
                        'GATEWAY_ERROR_REMITTER_CUTOFF_IN_PROGRESS',
                        'GATEWAY_ERROR_SUPPORT_AUTH_NOT_FOUND',
                        'GATEWAY_ERROR_FATAL_ERROR',
                        'GATEWAY_ERROR_INVALID_PARAMETERS',
                        'GATEWAY_ERROR_INVALID_RESPONSE',
                        'GATEWAY_ERROR_ISSUER_DOWN',
                        'GATEWAY_ERROR_MANDATE_CREATION_FAILED',
                        'GATEWAY_ERROR_MERCHANT_IP_NOT_WHITELISTED',
                        'GATEWAY_ERROR_COMMUNICATION_ERROR',
                        'GATEWAY_ERROR_REQUEST_TIMEOUT'
                    ) THEN 'gateway'
                    ELSE 'business'
                END
            ELSE a.internal_error_code
        END AS `Error Source`,
        a.created_date,
        CASE 
            WHEN cast(a.created_date as date) BETWEEN date '{t2_start}' AND date '{t2_end}' THEN 'T-2'
            WHEN cast(a.created_date as date) BETWEEN date '{t1_start}' AND date '{t1_end}' THEN 'T-1'
            ELSE NULL
        END AS Period,
        IF (DAY(date(a.created_date)) < day(current_date),1,0) MTD_check,
        CASE 
            WHEN a.authorized_at is null then 'Unauthorized'
            WHEN a.authorized_at is not null and (a.authorized_at - a.created_at) BETWEEN 0 and 120 then 'a. less than 2 mins'
            WHEN a.authorized_at is not null and (a.authorized_at - a.created_at) BETWEEN 121 and 300 then 'b. 2-5 mins'
            WHEN a.authorized_at is not null and (a.authorized_at - a.created_at) BETWEEN 301 and 480 then 'c. 5-8 mins'
            WHEN a.authorized_at is not null and (a.authorized_at - a.created_at) BETWEEN 481 and 720 then 'd. 8- 12 mins'
            WHEN a.authorized_at is not null and (a.authorized_at - a.created_at) > 720 then 'e. > 12 mins' end as auth_tat,
            d.auth_type,
            if(a.id = e.payment_id,1,0) fallback_check,
            if(a.id = f.payment_id,1,0) abtesting_check,
            e.initial_auth_type,
            e.final_auth,
            a.issuer,
            concat(initial_auth_type,'_',final_auth) fallback_flag,
        sum(a.base_amount/100) AS `GMV`,
        count(distinct CASE WHEN authorized_at IS NOT NULL and authorized_at != 0  THEN a.id ELSE NULL END) AS `Successful payments`,
        count(distinct a.id) AS `Total Payments`
    FROM 
        (select * from aggregate_ba.payments_optimizer_flag_sincejan2021 where created_date between  '{range2_start}' AND  '{range2_end}' and optimizer_flag = 1) a 
        LEFT JOIN (SELECT id, billing_label, name FROM realtime_hudi_api.merchants) b ON a.merchant_id = b.id
        LEFT JOIN (SELECT * FROM aggregate_ba.final_team_tagging) c ON a.merchant_id = c.merchant_id
        INNER JOIN (
        select id , late_authorized, auth_type ,created_date from realtime_hudi_api.payments
        where created_date between  '{range2_start}' AND  '{range2_end}' ) d ON a.id = d.id
        LEFT JOIN analytics_selfserve.optimizer_native_otp_abtesting_enabled_3ds_fallback_payments e on a.id = e.payment_id
        LEFT JOIN analytics_selfserve.optimizer_native_otp_abtesting_enabled_payments f on a.id = f.payment_id
    WHERE 
        date_trunc('month', date(a.created_date)) >= date_add(month,-2,date_trunc('month',current_date))
        AND a.optimizer_flag = 1
    GROUP BY all 
""" )

# COMMAND ----------

# Execute Spark SQL query with global filters
payments3 = spark.sql(f"""
        SELECT
        a.merchant_id, 
        COALESCE(a.dba,b.billing_label, b.name) AS Name,
        c.team_owner AS `Team Mapping`,
        min(try_cast(a.golive_date as date)) as golive_date, ,  
        a.first_transaction_date,
        CASE 
        WHEN a.internal_external_flag = 'external' THEN gateway
        WHEN a.internal_external_flag = 'internal' THEN 'Razorpay' else null end as gateway,
        a.method_advanced,
        a.network,
        a.network_tokenised_payment,
        cast(d.late_authorized as varchar (10)) late_authorized, 
        IF(a.internal_error_code = '' OR a.internal_error_code IS NULL, 'Success', a.internal_error_code) AS Error_Code,
        CASE
            WHEN a.internal_error_code IN (
                'BAD_REQUEST_CARD_DISABLED_FOR_ONLINE_PAYMENTS',
                'SERVER_ERROR_OTP_ELF_INVALID_ARGUMENT',
                'BAD_REQUEST_PAYMENT_CUSTOMER_DROPPED_OFF',
                'BAD_REQUEST_PAYMENT_CANCELLED',
                'BAD_REQUEST_CARD_CREDIT_LIMIT_REACHED',
                'GATEWAY_ERROR_CREDIT_FAILED',
                'GATEWAY_ERROR_BENEFICIARY_BANK_DEEMED_HIGH_RESPONSE_TIME_CHECK_DECLINE',
                'GATEWAY_ERROR_REMITTER_BANK_DEEMED_HIGH_RESPONSE_TIME_CHECK_DECLINE',
                'GATEWAY_ERROR_MANDATE_CREATION_EXPIRED',
                'GATEWAY_ERROR_REQAUTH_DECLINED',
                'BAD_REQUEST_PAYMENT_DECLINED_BY_CUSTOMER',
                'BAD_REQUEST_TRANSACTION_AMOUNT_LIMIT_EXCEEDED',
                'GATEWAY_ERROR_PAYMENT_FAILED',
                'BAD_REQUEST_TRANSACTION_AMOUNT_LIMIT_AT_REMITTER_EXCEEDED',
                'GATEWAY_ERROR_REMITTER_BANK_NOT_AVAILABLE',
                'GATEWAY_ERROR_PAYER_PSP_NOT_AVAILABLE',
                'BAD_REQUEST_TRANSACTION_FREQUENCY_LIMIT_AT_REMITTER_EXCEEDED',
                'GATEWAY_ERROR_MANDATE_CREATION_TIMEOUT_AT_REMITTER_END',
                'GATEWAY_ERROR_REMITTER_CUTOFF_IN_PROGRESS',
                'BAD_REQUEST_PAYMENT_UPI_MANDATE_REJECTED',
                'BAD_REQUEST_CARD_MANDATE_IS_NOT_ACTIVE',
                'GATEWAY_ERROR_SUPPORT_AUTH_NOT_FOUND',
                'BAD_REQUEST_ERROR_OTP_ELF_INVALID_OTP_LENGTH',
                'BAD_REQUEST_CARD_MANDATE_CUSTOMER_NOT_APPROVED',
                'BAD_REQUEST_USER_NOT_AUTHENTICATED',
                'BAD_REQUEST_CARD_INVALID_DATA',
                'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
                'BAD_REQUEST_CARD_MANDATE_MANDATE_NOT_ACTIVE',
                'GATEWAY_ERROR_MANDATE_CREATION_DECLINED_BY_REMITTER_BANK',
                'GATEWAY_ERROR_PAYMENT_NOT_FOUND',
                'BAD_REQUEST_AUTHENTICATION_FAILED',
                'BAD_REQUEST_EMANDATE_REGISTRATION_ALREADY_DECLINED_BY_BANK',
                'BAD_REQUEST_EMANDATE_REGISTRATION_ALREADY_IN_PROGRESS',
                'BAD_REQUEST_PAYMENT_PENDING',
                'BAD_REQUEST_PAYMENT_CANCELLED_AT_EMANDATE_REGISTRATION',
                'GATEWAY_ERROR_PAYMENT_NOT_FOUND',
                'GATEWAY_ERROR_PAYMENT_FAILED',
                'BAD_REQUEST_ACCOUNT_BLOCKED',
                'BAD_REQUEST_ACCOUNT_CLOSED',
                'BAD_REQUEST_EMANDATE_AMOUNT_LIMIT_EXCEEDED',
                'BAD_REQUEST_EMANDATE_CANCELLED_INACTIVE',
                'BAD_REQUEST_EMANDATE_REGISTRATION_ACTION_NEEDED',
                'BAD_REQUEST_EMANDATE_REGISTRATION_FAILED',
                'BAD_REQUEST_GATEWAY_TOKEN_EMPTY',
                'BAD_REQUEST_INVALID_ACCOUNT_HOLDER_NAME',
                'BAD_REQUEST_INVALID_TRANSACTION_AMOUNT',
                'BAD_REQUEST_INVALID_USER_CREDENTIALS',
                'BAD_REQUEST_NETBANKING_USER_NOT_REGISTERED',
                'BAD_REQUEST_NO_DR_ALLOWED',
                'BAD_REQUEST_PAYMENT_ACCOUNT_INSUFFICIENT_BALANCE',
                'BAD_REQUEST_PAYMENT_ACCOUNT_MAX_LIMIT_EXCEEDED',
                'BAD_REQUEST_PAYMENT_ACCOUNT_WITHDRAWAL_FROZEN',
                'BAD_REQUEST_PAYMENT_CANCELLED_BY_CUSTOMER',
                'BAD_REQUEST_PAYMENT_CANCELLED_BY_USER',
                'BAD_REQUEST_PAYMENT_FAILED',
                'BAD_REQUEST_PAYMENT_INVALID_ACCOUNT',
                'BAD_REQUEST_PAYMENT_INVALID_AMOUNT_OR_CURRENCY',
                'BAD_REQUEST_PAYMENT_KYC_PENDING',
                'BAD_REQUEST_PAYMENT_NETBANKING_CANCELLED_BY_USER',
                'BAD_REQUEST_PAYMENT_POSSIBLE_FRAUD',
                'BAD_REQUEST_PAYMENT_POSSIBLE_FRAUD_WEBSITE_MISMATCH',
                'BAD_REQUEST_PAYMENT_TIMED_OUT',
                'BAD_REQUEST_PAYMENT_VERIFICATION_FAILED',
                'BAD_REQUEST_TRANSACTION_AMOUNT_LIMIT_EXCEEDED',
                'GATEWAY_ERROR_AMOUNT_TAMPERED',
                'GATEWAY_ERROR_CALLBACK_EMPTY_INPUT',
                'GATEWAY_ERROR_DEBIT_BEFORE_MANDATE_START',
                'GATEWAY_ERROR_DEBIT_FAILED',
                'GATEWAY_ERROR_FATAL_ERROR',
                'GATEWAY_ERROR_INVALID_PARAMETERS',
                'GATEWAY_ERROR_INVALID_RESPONSE',
                'GATEWAY_ERROR_ISSUER_DOWN',
                'GATEWAY_ERROR_MANDATE_CREATION_FAILED',
                'GATEWAY_ERROR_MERCHANT_IP_NOT_WHITELISTED',
                'GATEWAY_ERROR_PAYMENT_VERIFICATION_ERROR',
                'GATEWAY_ERROR_REQUEST_ERROR',
                'GATEWAY_ERROR_REQUEST_TIMEOUT',
                'GATEWAY_ERROR_UNKNOWN_ERROR',
                'SERVER_ERROR_AMOUNT_TAMPERED',
                'SERVER_ERROR_ASSERTION_ERROR',
                'BAD_REQUEST_EMANDATE_REGISTRATION_FAILED_JOINT_ACCOUNT',
                'BAD_REQUEST_EMANDATE_REGISTRATION_DUPLICATE_REQUEST',
                'BAD_REQUEST_PAYMENT_OTP_INCORRECT',
                'BAD_REQUEST_PAYMENT_CARD_INVALID_PIN',
                'BAD_REQUEST_PAYMENT_OTP_VALIDATION_ATTEMPT_LIMIT_EXCEEDED',
                'BAD_REQUEST_PAYMENT_CARD_EXPIRED',
                'BAD_REQUEST_PAYMENT_CARD_NUMBER_POSSIBLY_INVALID',
                'SERVER_ERROR_NBPLUS_PAYMENT_SERVICE_FAILURE',
                'GATEWAY_ERROR_COMMUNICATION_ERROR',
                'BAD_REQUEST_PAYMENT_DECLINED_BY_BANK_DUE_TO_BLOCKED_CARD',
                'BAD_REQUEST_EMANDATE_INACTIVE',
                'BAD_REQUEST_CARD_INACTIVE',
                'BAD_REQUEST_PAYMENT_CARD_INVALID_CVV',
                'BAD_REQUEST_VALIDATION_FAILURE',
                'BAD_REQUEST_ACCOUNT_DORMANT',
                'BAD_REQUEST_PAYMENT_CARD_INVALID_EXPIRY_DATE',
                'BAD_REQUEST_PAYMENT_ALREADY_PROCESSED',
                'SERVER_ERROR_RUNTIME_ERROR',
                'GATEWAY_ERROR_CHECKSUM_MATCH_FAILED') THEN 
                CASE
                    WHEN a.internal_error_code IN (
                        'BAD_REQUEST_CARD_DISABLED_FOR_ONLINE_PAYMENTS',
                        'BAD_REQUEST_PAYMENT_CUSTOMER_DROPPED_OFF',
                        'BAD_REQUEST_PAYMENT_CANCELLED',
                        'BAD_REQUEST_CARD_CREDIT_LIMIT_REACHED',
                        'BAD_REQUEST_PAYMENT_DECLINED_BY_CUSTOMER',
                        'BAD_REQUEST_TRANSACTION_AMOUNT_LIMIT_EXCEEDED',
                        'BAD_REQUEST_PAYMENT_UPI_MANDATE_REJECTED',
                        'BAD_REQUEST_CARD_MANDATE_IS_NOT_ACTIVE',
                        'BAD_REQUEST_CARD_INVALID_DATA',
                        'BAD_REQUEST_NON_3DS_INTERNATIONAL_NOT_ALLOWED',
                        'BAD_REQUEST_CARD_MANDATE_MANDATE_NOT_ACTIVE',
                        'BAD_REQUEST_AUTHENTICATION_FAILED',
                        'BAD_REQUEST_EMANDATE_REGISTRATION_ALREADY_DECLINED_BY_BANK',
                        'BAD_REQUEST_EMANDATE_REGISTRATION_ALREADY_IN_PROGRESS',
                        'BAD_REQUEST_PAYMENT_PENDING',
                        'BAD_REQUEST_PAYMENT_CANCELLED_AT_EMANDATE_REGISTRATION',
                        'BAD_REQUEST_ACCOUNT_BLOCKED',
                        'BAD_REQUEST_ACCOUNT_CLOSED',
                        'BAD_REQUEST_EMANDATE_CANCELLED_INACTIVE',
                        'BAD_REQUEST_EMANDATE_REGISTRATION_ACTION_NEEDED',
                        'BAD_REQUEST_GATEWAY_TOKEN_EMPTY',
                        'BAD_REQUEST_INVALID_ACCOUNT_HOLDER_NAME',
                        'BAD_REQUEST_INVALID_TRANSACTION_AMOUNT',
                        'BAD_REQUEST_INVALID_USER_CREDENTIALS',
                        'BAD_REQUEST_NETBANKING_USER_NOT_REGISTERED',
                        'BAD_REQUEST_NO_DR_ALLOWED',
                        'BAD_REQUEST_PAYMENT_ACCOUNT_INSUFFICIENT_BALANCE',
                        'BAD_REQUEST_PAYMENT_ACCOUNT_MAX_LIMIT_EXCEEDED',
                        'BAD_REQUEST_PAYMENT_ACCOUNT_WITHDRAWAL_FROZEN',
                        'BAD_REQUEST_PAYMENT_CANCELLED_BY_CUSTOMER',
                        'BAD_REQUEST_PAYMENT_CANCELLED_BY_USER',
                        'BAD_REQUEST_PAYMENT_FAILED',
                        'BAD_REQUEST_PAYMENT_INVALID_ACCOUNT',
                        'BAD_REQUEST_PAYMENT_INVALID_AMOUNT_OR_CURRENCY',
                        'BAD_REQUEST_PAYMENT_KYC_PENDING',
                        'BAD_REQUEST_PAYMENT_NETBANKING_CANCELLED_BY_USER',
                        'BAD_REQUEST_PAYMENT_POSSIBLE_FRAUD',
                        'BAD_REQUEST_PAYMENT_POSSIBLE_FRAUD_WEBSITE_MISMATCH',
                        'BAD_REQUEST_PAYMENT_TIMED_OUT',
                        'BAD_REQUEST_PAYMENT_VERIFICATION_FAILED',
                        'BAD_REQUEST_TRANSACTION_AMOUNT_LIMIT_EXCEEDED',
                        'BAD_REQUEST_EMANDATE_REGISTRATION_FAILED_JOINT_ACCOUNT',
                        'BAD_REQUEST_EMANDATE_REGISTRATION_DUPLICATE_REQUEST',
                        'BAD_REQUEST_PAYMENT_OTP_INCORRECT',
                        'BAD_REQUEST_PAYMENT_CARD_INVALID_PIN',
                        'BAD_REQUEST_PAYMENT_OTP_VALIDATION_ATTEMPT_LIMIT_EXCEEDED',
                        'BAD_REQUEST_PAYMENT_CARD_EXPIRED',
                        'BAD_REQUEST_PAYMENT_CARD_NUMBER_POSSIBLY_INVALID',
                        'BAD_REQUEST_PAYMENT_DECLINED_BY_BANK_DUE_TO_BLOCKED_CARD',
                        'BAD_REQUEST_EMANDATE_INACTIVE',
                        'BAD_REQUEST_CARD_INACTIVE',
                        'BAD_REQUEST_PAYMENT_CARD_INVALID_CVV',
                        'BAD_REQUEST_VALIDATION_FAILURE',
                        'BAD_REQUEST_ACCOUNT_DORMANT',
                        'BAD_REQUEST_PAYMENT_CARD_INVALID_EXPIRY_DATE',
                        'SERVER_ERROR_RUNTIME_ERROR'
                    ) THEN 'customer'
                    WHEN a.internal_error_code IN (
                        'SERVER_ERROR_OTP_ELF_INVALID_ARGUMENT',
                        'GATEWAY_ERROR_REQAUTH_DECLINED',
                        'GATEWAY_ERROR_PAYMENT_VERIFICATION_ERROR',
                        'GATEWAY_ERROR_REQUEST_ERROR',
                        'GATEWAY_ERROR_UNKNOWN_ERROR',
                        'SERVER_ERROR_AMOUNT_TAMPERED',
                        'SERVER_ERROR_ASSERTION_ERROR',
                        'GATEWAY_ERROR_CHECKSUM_MATCH_FAILED'
                    ) THEN 'internal'
                    WHEN a.internal_error_code IN (
                        'GATEWAY_ERROR_CREDIT_FAILED',
                        'GATEWAY_ERROR_BENEFICIARY_BANK_DEEMED_HIGH_RESPONSE_TIME_CHECK_DECLINE',
                        'GATEWAY_ERROR_REMITTER_BANK_DEEMED_HIGH_RESPONSE_TIME_CHECK_DECLINE',
                        'GATEWAY_ERROR_PAYMENT_FAILED',
                        'GATEWAY_ERROR_MANDATE_CREATION_TIMEOUT_AT_REMITTER_END',
                        'GATEWAY_ERROR_REMITTER_CUTOFF_IN_PROGRESS',
                        'GATEWAY_ERROR_SUPPORT_AUTH_NOT_FOUND',
                        'GATEWAY_ERROR_FATAL_ERROR',
                        'GATEWAY_ERROR_INVALID_PARAMETERS',
                        'GATEWAY_ERROR_INVALID_RESPONSE',
                        'GATEWAY_ERROR_ISSUER_DOWN',
                        'GATEWAY_ERROR_MANDATE_CREATION_FAILED',
                        'GATEWAY_ERROR_MERCHANT_IP_NOT_WHITELISTED',
                        'GATEWAY_ERROR_COMMUNICATION_ERROR',
                        'GATEWAY_ERROR_REQUEST_TIMEOUT'
                    ) THEN 'gateway'
                    ELSE 'business'
                END
            ELSE a.internal_error_code
        END AS `Error Source`,
        a.created_date,
        CASE 
            WHEN cast(a.created_date as date) BETWEEN date '{t2_start}' AND date '{t2_end}' THEN 'T-2'
            WHEN cast(a.created_date as date) BETWEEN date '{t1_start}' AND date '{t1_end}' THEN 'T-1'
            ELSE NULL
        END AS Period,
        IF (DAY(date(a.created_date)) < day(current_date),1,0) MTD_check,
        CASE 
            WHEN a.authorized_at is null then 'Unauthorized'
            WHEN a.authorized_at is not null and (a.authorized_at - a.created_at) BETWEEN 0 and 120 then 'a. less than 2 mins'
            WHEN a.authorized_at is not null and (a.authorized_at - a.created_at) BETWEEN 121 and 300 then 'b. 2-5 mins'
            WHEN a.authorized_at is not null and (a.authorized_at - a.created_at) BETWEEN 301 and 480 then 'c. 5-8 mins'
            WHEN a.authorized_at is not null and (a.authorized_at - a.created_at) BETWEEN 481 and 720 then 'd. 8- 12 mins'
            WHEN a.authorized_at is not null and (a.authorized_at - a.created_at) > 720 then 'e. > 12 mins' end as auth_tat,
            d.auth_type,
            if(a.id = e.payment_id,1,0) fallback_check,
            if(a.id = f.payment_id,1,0) abtesting_check,
            e.initial_auth_type,
            e.final_auth,
            a.issuer,
            concat(initial_auth_type,'_',final_auth) fallback_flag,
        sum(a.base_amount/100) AS `GMV`,
        count(distinct CASE WHEN authorized_at IS NOT NULL and authorized_at != 0 THEN a.id ELSE NULL END) AS `Successful payments`,
        count(distinct a.id) AS `Total Payments`
    FROM 
        (select * from aggregate_ba.payments_optimizer_flag_sincejan2021 where created_date between  '{range3_start}' AND  '{range3_end}' and optimizer_flag = 1) a 
        LEFT JOIN (SELECT id, billing_label, name FROM realtime_hudi_api.merchants) b ON a.merchant_id = b.id
        LEFT JOIN (SELECT * FROM aggregate_ba.final_team_tagging) c ON a.merchant_id = c.merchant_id
        INNER JOIN (
        select id , late_authorized, auth_type ,created_date from realtime_hudi_api.payments
        where created_date between  '{range3_start}' AND  '{range3_end}' ) d ON a.id = d.id
        LEFT JOIN analytics_selfserve.optimizer_native_otp_abtesting_enabled_3ds_fallback_payments e on a.id = e.payment_id
        LEFT JOIN analytics_selfserve.optimizer_native_otp_abtesting_enabled_payments f on a.id = f.payment_id
    WHERE 
        date_trunc('month', date(a.created_date)) >= date_add(month,-2,date_trunc('month',current_date))
        AND a.optimizer_flag = 1
    GROUP BY all 
""" )

# COMMAND ----------

payments_pd_1 = payments1.toPandas()
payments_pd_1.shape

# COMMAND ----------



# COMMAND ----------

payments1_part1 = payments1.limit(int(payments1.count() / 2))
payments1_part2 = payments1.subtract(payments1_part1)

payments_pd_part1 = payments1_part1.toPandas()
payments_pd_part2 = payments1_part2.toPandas()

# COMMAND ----------



# COMMAND ----------

#payments_pd_1['created_date'].value_counts()

# COMMAND ----------

#payments_pd_2 = payments2.toPandas()
#payments_pd_2.shape

payments2_part1 = payments2.limit(int(payments1.count() / 2))
payments2_part2 = payments2.subtract(payments1_part1)

payments2_pd_part1 = payments2_part1.toPandas()
payments2_pd_part2 = payments2_part2.toPandas()

# COMMAND ----------

#payments_pd_3 = payments3.toPandas()
#payments_pd_3.shape

payments3_part1 = payments3.limit(int(payments1.count() / 2))
payments3_part2 = payments3.subtract(payments1_part1)

payments3_pd_part1 = payments3_part1.toPandas()
payments3_pd_part2 = payments3_part2.toPandas()

# COMMAND ----------



# COMMAND ----------

payments_pd = pd.concat([payments3_part1,payments3_part2,payments2_part1,payments2_part2, payments1_part1, payments1_part2], ignore_index=True)
payments_pd.shape

# COMMAND ----------

# MAGIC %md
# MAGIC ##Native OTP

# COMMAND ----------

payments_pd.head(1)

# COMMAND ----------

native_otp_base_100 = payments_pd[
    (
        (payments_pd['method_advanced'] == 'Credit Card') 
        |
        (payments_pd['method_advanced'] == 'Debit Card') 
        |
        (payments_pd['method_advanced'] == 'Prepaid Card')
    ) &
    (
        (payments_pd['gateway'] == 'billdesk_optimizer') 
        |
        (payments_pd['gateway'] == 'payu')
    ) &
    (
        (payments_pd['network'] == 'Visa') |
        (payments_pd['network'] == 'MasterCard')
    ) &
    (
        (payments_pd['auth_type'] == '3ds') |
        (payments_pd['auth_type'] == 'headless_otp')
    ) &
    (payments_pd['merchant_id'] != 'ELi8nocD30pFkb')
    &
    (payments_pd['fallback_flag'] != 'headless_otp_3ds')
    &
    (payments_pd["Period"] ==  'T-1')

    &
    (payments_pd["golive_date"] !=  "None")

    ]

# COMMAND ----------

native_otp_base_non_100 = payments_pd[
    (
        (payments_pd['method_advanced'] == 'Credit Card') 
        |
        (payments_pd['method_advanced'] == 'Debit Card') 
        |
        (payments_pd['method_advanced'] == 'Prepaid Card')
    ) &
    (
        (payments_pd['gateway'] == 'paytm') 
    ) &
    (
        (payments_pd['network'] == 'Visa') |
        (payments_pd['network'] == 'MasterCard')
    ) &
    (
        (payments_pd['auth_type'] == '3ds') |
        (payments_pd['auth_type'] == 'headless_otp')
    ) &
    (payments_pd['merchant_id'] != 'ELi8nocD30pFkb')

    &
    (payments_pd['abtesting_check'] == 1)
    
    &
    (payments_pd["Period"] ==  'T-1')

    &
    (payments_pd["golive_date"] !=  "None")
    ]

# COMMAND ----------

# Gateway	Issuer	Network Tokenised Payment	Network	Merchant ID	Billing Label

# COMMAND ----------

native_otp_base_100

# COMMAND ----------

native_otp_base_non_100

# COMMAND ----------

native_otp_base_100_pivot = native_otp_base_100.pivot_table(
    index=["merchant_id","Name","gateway", "issuer", "network", "network_tokenised_payment"],
    columns="auth_type",
    values=["Successful payments", "Total Payments"],
    aggfunc=sum
).fillna(0).astype(int)

native_otp_base_100_pivot
# Sort the columns in reverse order
native_otp_base_100_pivot = native_otp_base_100_pivot.reindex(columns=sorted(native_otp_base_100_pivot.columns, reverse=True)).astype(int)

native_otp_base_100_pivot['Headless OTP SR'] = ((native_otp_base_100_pivot[('Successful payments', 'headless_otp')]/ native_otp_base_100_pivot[('Total Payments', 'headless_otp')]) * 100).round(2)

native_otp_base_100_pivot['3ds SR'] = ((native_otp_base_100_pivot[('Successful payments', '3ds')]/ native_otp_base_100_pivot[('Total Payments', '3ds')]) * 100).round(2)

native_otp_base_100_pivot['Attempts on both flows'] = (
    (native_otp_base_100_pivot[('Total Payments', 'headless_otp')] > 0 ) & 
    (native_otp_base_100_pivot[('Total Payments', '3ds')] > 0)
).astype(int)


native_otp_base_100_pivot['Attempts > 30 on both flows'] = (
    (native_otp_base_100_pivot[('Total Payments', 'headless_otp')] >= 30) & 
    (native_otp_base_100_pivot[('Total Payments', '3ds')] >= 30)
).astype(int)

# native_otp_base_100_pivot = native_otp_base_100_pivot[native_otp_base_100_pivot[('Total Payments', 'headless_otp')] >= 30]
# native_otp_base_100_pivot = native_otp_base_100_pivot[native_otp_base_100_pivot[('Total Payments', '3ds')] >= 30]

native_otp_base_100_pivot['Delta SR'] = native_otp_base_100_pivot['Headless OTP SR'] - native_otp_base_100_pivot['3ds SR']

native_otp_base_100_pivot['Native OTP SR > 3ds SR'] = ((native_otp_base_100_pivot[('Delta SR', '')] > 0)
).astype(int)

# native_otp_base_100_pivot = native_otp_base_100_pivot[native_otp_base_100_pivot[('Total Payments', 'headless_otp')] >= 30]
# native_otp_base_100_pivot = native_otp_base_100_pivot[native_otp_base_100_pivot[('Total Payments', '3ds')] >= 30]

native_otp_base_100_pivot = native_otp_base_100_pivot.reset_index()
native_otp_base_100_pivot

# median_delta_sr = native_otp_base_100_pivot['Delta SR'].median()
# max_delta_sr = native_otp_base_100_pivot['Delta SR'].max()
# min_delta_sr = native_otp_base_100_pivot['Delta SR'].min()

# COMMAND ----------

native_otp_base_non_100_pivot = native_otp_base_non_100.pivot_table(
    index=["merchant_id","Name","gateway", "issuer", "network", "network_tokenised_payment"],
    columns="auth_type",
    values=["Successful payments", "Total Payments"],
    aggfunc=sum
).fillna(0).astype(int)


# Sort the columns in reverse order
native_otp_base_non_100_pivot = native_otp_base_non_100_pivot.reindex(columns=sorted(native_otp_base_non_100_pivot.columns, reverse=True)).astype(int)

native_otp_base_non_100_pivot['Headless OTP SR'] = ((native_otp_base_non_100_pivot[('Successful payments', 'headless_otp')]/ native_otp_base_non_100_pivot[('Total Payments', 'headless_otp')]) * 100).round(2)

native_otp_base_non_100_pivot['3ds SR'] = ((native_otp_base_non_100_pivot[('Successful payments', '3ds')]/ native_otp_base_non_100_pivot[('Total Payments', '3ds')]) * 100).round(2)

native_otp_base_non_100_pivot['Attempts on both flows'] = (
    (native_otp_base_non_100_pivot[('Total Payments', 'headless_otp')] > 0 ) & 
    (native_otp_base_non_100_pivot[('Total Payments', '3ds')] > 0)
).astype(int)


native_otp_base_non_100_pivot['Attempts > 30 on both flows'] = (
    (native_otp_base_non_100_pivot[('Total Payments', 'headless_otp')] >= 30) & 
    (native_otp_base_non_100_pivot[('Total Payments', '3ds')] >= 30)
).astype(int)

# native_otp_base_100_pivot = native_otp_base_100_pivot[native_otp_base_100_pivot[('Total Payments', 'headless_otp')] >= 30]
# native_otp_base_100_pivot = native_otp_base_100_pivot[native_otp_base_100_pivot[('Total Payments', '3ds')] >= 30]

native_otp_base_non_100_pivot['Delta SR'] = native_otp_base_non_100_pivot['Headless OTP SR'] - native_otp_base_non_100_pivot['3ds SR']

native_otp_base_non_100_pivot['Native OTP SR > 3ds SR'] = ((native_otp_base_non_100_pivot[('Delta SR', '')] > 0)
).astype(int)

# native_otp_base_non_100_pivot = native_otp_base_non_100_pivot[native_otp_base_non_100_pivot[('Total Payments', 'headless_otp')] >= 30]
# native_otp_base_non_100_pivot = native_otp_base_non_100_pivot[native_otp_base_non_100_pivot[('Total Payments', '3ds')] >= 30]

native_otp_base_non_100_pivot = native_otp_base_non_100_pivot.reset_index()
native_otp_base_non_100_pivot

# median_delta_sr = native_otp_base_100_pivot['Delta SR'].median()
# max_delta_sr = native_otp_base_100_pivot['Delta SR'].max()
# min_delta_sr = native_otp_base_100_pivot['Delta SR'].min()

# COMMAND ----------

native_otp_final = pd.merge(native_otp_base_100_pivot, native_otp_base_non_100_pivot, how='outer').drop_duplicates().reset_index(drop=True)
native_otp_final

# COMMAND ----------

#native_otp_final_overall

# COMMAND ----------

native_otp_final_overall = native_otp_final[native_otp_final['Attempts > 30 on both flows'] == 1]
native_otp_final_overall

# COMMAND ----------

median_delta_sr = native_otp_final_overall['Delta SR'].median()
max_delta_sr = native_otp_final_overall['Delta SR'].max()
min_delta_sr = native_otp_final_overall['Delta SR'].min()

delta_sr_stats = pd.DataFrame({
    'Metric': ['Median Delta SR', 'Max Delta SR', 'Min Delta SR'],
    '% Value': [median_delta_sr, max_delta_sr, min_delta_sr]})

# Display the results
delta_sr_stats

# COMMAND ----------

# Calculate the total and successful attempts for both '3ds' and 'headless_otp'
total_attempts_3ds = native_otp_final_overall[('Total Payments', '3ds')].sum()
successful_attempts_3ds = native_otp_final_overall[('Successful payments', '3ds')].sum()

total_attempts_headless = native_otp_final_overall[('Total Payments', 'headless_otp')].sum()
successful_attempts_headless = native_otp_final_overall[('Successful payments', 'headless_otp')].sum()

# Create a DataFrame to display these statistics with metrics as headers
overall_stats = pd.DataFrame({
    'Total Attempts 3ds': [total_attempts_3ds],
    'Successful Attempts 3ds': [successful_attempts_3ds],
    'Total Attempts Headless': [total_attempts_headless],
    'Successful Attempts Headless': [successful_attempts_headless]
})

overall_stats['3ds SR'] = ((overall_stats['Successful Attempts 3ds'] / overall_stats['Total Attempts 3ds']) * 100).round(2)
overall_stats['Headless OTP SR'] = ((overall_stats['Successful Attempts Headless'] / overall_stats['Total Attempts Headless']) * 100).round(2)

overall_stats['Delta SR'] = overall_stats['Headless OTP SR'] - overall_stats['3ds SR']
overall_stats

# COMMAND ----------

# Count the distinct merchant_id
distinct_merchant_count = native_otp_final[native_otp_final[('Total Payments', 'headless_otp')] > 0]['merchant_id'].nunique()
distinct_merchant_count

# Count the distinct merchant_id
eligible_merchant_count = native_otp_final[native_otp_final['Attempts > 30 on both flows'] == 1]['merchant_id'].nunique()
eligible_merchant_count

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# native_otp_final_overall = native_otp_final_overall.reset_index()

# COMMAND ----------

eligible_merchant_summary = native_otp_final_overall.pivot_table(
    index=[('merchant_id', ''), ('Name', ''), ('gateway', '')],
    values=[
        ('Total Payments', 'headless_otp'),
        ('Total Payments', '3ds'),
        ('Successful payments', 'headless_otp'),
        ('Successful payments', '3ds')
    ],
    aggfunc='sum'  # Specify the aggregation function
)

eligible_merchant_summary = eligible_merchant_summary.reindex(columns=sorted(eligible_merchant_summary.columns, reverse=True)).astype(int)

eligible_merchant_summary['Headless OTP SR'] = ((eligible_merchant_summary[('Successful payments', 'headless_otp')]/ eligible_merchant_summary[('Total Payments', 'headless_otp')]) * 100).round(2)

eligible_merchant_summary['3ds SR'] = ((eligible_merchant_summary[('Successful payments', '3ds')]/ eligible_merchant_summary[('Total Payments', '3ds')]) * 100).round(2)

eligible_merchant_summary['Delta SR'] = eligible_merchant_summary['Headless OTP SR']  - eligible_merchant_summary['3ds SR']

eligible_merchant_summary = eligible_merchant_summary.reset_index()

eligible_merchant_summary.columns

new_columns = {
    ('merchant_id', ''): 'Merchant ID',
    ('Name', ''): 'Name',
    ('gateway', ''): 'Gateway',
    ('Total Payments', 'headless_otp'): 'Total Native OTP Attempts',
    ('Total Payments', '3ds'): 'Total 3ds OTP Attempts',
    ('Successful payments', 'headless_otp'): 'Total Native OTP Successful Attempts',
    ('Successful payments', '3ds'): 'Total 3ds Successful Attempts',
    ('Headless OTP SR', ''): 'Native OTP SR',
    ('3ds SR', ''): '3ds SR',
    ('Delta SR', ''): 'Delta SR'
}

# Rename the columns
eligible_merchant_summary.columns = eligible_merchant_summary.columns.map(new_columns)

eligible_merchant_summary

# COMMAND ----------



# COMMAND ----------

paytm_native_mx = native_otp_final[
    (native_otp_final['Attempts > 30 on both flows'] == 1) & 
    (native_otp_final['gateway'] == 'paytm')
]

# Group by 'Name' and calculate the statistics for 'Delta SR'
paytm_native_mx_stats = paytm_native_mx.groupby('Name')['Delta SR'].agg(
    Median='median',
    Max='max',
    Min='min'
).reset_index().round(2)

# Display the results
paytm_native_mx_stats

plt.figure(figsize=(10, 6))

# Set the positions and width for the bars
bar_width = 0.2
names = paytm_native_mx_stats['Name']
print(range(len(names)))
x = range(len(names))

# Create bars for each metric with custom colors
bars1 = plt.bar(x, paytm_native_mx_stats['Median'], width=bar_width, label='Median Delta SR', color='blue', align='center')
bars2 = plt.bar([p + bar_width for p in x], paytm_native_mx_stats['Max'], width=bar_width, label='Max Delta SR', color='green', align='center')
bars3 = plt.bar([p + bar_width * 2 for p in x], paytm_native_mx_stats['Min'], width=bar_width, label='Min Delta SR', color='red', align='center')

# Add text labels above the bars
for bar in bars1:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2, yval, round(yval, 2), ha='center', va='bottom')
for bar in bars2:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2, yval, round(yval, 2), ha='center', va='bottom')
for bar in bars3:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2, yval, round(yval, 2), ha='center', va='bottom')

# Set the x-axis and y-axis labels
plt.xlabel('Name')
plt.ylabel('Delta SR')
plt.title('Delta SR Statistics by Name for Paytm Gateway')

# Set the position of the x ticks
plt.xticks([p + bar_width for p in x], names, rotation=60)

# Add the legend
plt.legend()

# Remove gridlines
plt.grid(False)

# Display the plot
plt.tight_layout()
plt.savefig('paytm_mx_comparison.png')

plt.show()

# COMMAND ----------

payu_native_mx = native_otp_final[
    (native_otp_final['Attempts > 30 on both flows'] == 1) & 
    (native_otp_final['gateway'] == 'payu')
]

# Group by 'Name' and calculate the statistics for 'Delta SR'
payu_native_mx = payu_native_mx.groupby('Name')['Delta SR'].agg(
    Median='median',
    Max='max',
    Min='min'
).reset_index().round(2)

# Display the results
payu_native_mx

plt.figure(figsize=(10, 6))

# Set the positions and width for the bars
bar_width = 0.2
names = payu_native_mx['Name']
x = range(len(names))

# Create bars for each metric with custom colors
bars1 = plt.bar(x, payu_native_mx['Median'], width=bar_width, label='Median Delta SR', color='blue', align='center')
bars2 = plt.bar([p + bar_width for p in x], payu_native_mx['Max'], width=bar_width, label='Max Delta SR', color='green', align='center')
bars3 = plt.bar([p + bar_width * 2 for p in x], payu_native_mx['Min'], width=bar_width, label='Min Delta SR', color='red', align='center')

# Add text labels above the bars
for bar in bars1:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2, yval, round(yval, 2), ha='center', va='bottom')
for bar in bars2:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2, yval, round(yval, 2), ha='center', va='bottom')
for bar in bars3:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2, yval, round(yval, 2), ha='center', va='bottom')

# Set the x-axis and y-axis labels
plt.xlabel('Name')
plt.ylabel('Delta SR')
plt.title('Delta SR Statistics by Name for Payu Gateway')

# Set the position of the x ticks
plt.xticks([p + bar_width for p in x], names, rotation=60)

# Add the legend
plt.legend()

# Remove gridlines
plt.grid(False)

# Display the plot
plt.tight_layout()
plt.savefig('payu_mx_comparison.png')

plt.show()



# COMMAND ----------

billdesk_native_mx = native_otp_final[
    (native_otp_final['Attempts > 30 on both flows'] == 1) & 
    (native_otp_final['gateway'] == 'billdesk_optimizer')
]

# Group by 'Name' and calculate the statistics for 'Delta SR'
billdesk_native_mx = billdesk_native_mx.groupby('Name')['Delta SR'].agg(
    Median='median',
    Max='max',
    Min='min'
).reset_index().round(2)

# Display the results
billdesk_native_mx

plt.figure(figsize=(10, 6))

# Set the positions and width for the bars
bar_width = 0.25
names = billdesk_native_mx['Name']
x = range(len(names))

# Create bars for each metric with custom colors
bars1 = plt.bar(x, billdesk_native_mx['Median'], width=bar_width, label='Median Delta SR', color='blue', align='center')
bars2 = plt.bar([p + bar_width for p in x], billdesk_native_mx['Max'], width=bar_width, label='Max Delta SR', color='green', align='center')
bars3 = plt.bar([p + bar_width * 2 for p in x], billdesk_native_mx['Min'], width=bar_width, label='Min Delta SR', color='red', align='center')

# Add text labels above the bars
for bar in bars1:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2, yval, round(yval, 2), ha='center', va='bottom')
for bar in bars2:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2, yval, round(yval, 2), ha='center', va='bottom')
for bar in bars3:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2, yval, round(yval, 2), ha='center', va='bottom')

# Set the x-axis and y-axis labels
plt.xlabel('Name')
plt.ylabel('Delta SR')
plt.title('Delta SR Statistics by Name for Billdesk Gateway')

# Set the position of the x ticks
plt.xticks([p + bar_width for p in x], names, rotation=60)

# Add the legend
plt.legend()

# Remove gridlines
plt.grid(False)

# Display the plot
plt.tight_layout()
plt.savefig('billdesk_mx_comparison.png')

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##MTU Flag

# COMMAND ----------

mtu_flag = spark.sql(
    f"""
    select * , if(`T-2 Flag` = `T-1 Flag`, 1 , 0) flag_change from 
    (select 
    a.merchant_id, parent_id, merchant_name, team_owner,
    poc, golive_month, if(a.merchant_id = b.merchant_id,1,0) dmt_merchant,
    max(case when date(reporting_month) = date(date_trunc('month', add_months(current_date(), -2))) then flag end) `T-2 Flag`,
    max(case when date(reporting_month) = date(date_trunc('month', add_months(current_date(), -2))) then t_2_attempts end ) `T-2 Attempts`,
    max(case when date(reporting_month) = date(date_trunc('month', add_months(current_date(), -2))) then t_2_total_attempts_external end ) `T-2 External Attempts`,
    max(case when date(reporting_month) = date(date_trunc('month', add_months(current_date(), -1))) then flag end ) `T-1 Flag`,
    max(case when date(reporting_month) = date(date_trunc('month', add_months(current_date(), -1))) then t_2_attempts end ) `T-1 Attempts`,
    max(case when date(reporting_month) = date(date_trunc('month', add_months(current_date(), -1))) then t_1_total_attempts_external end ) `T-1 External Attempts`
    from analytics_selfserve.optimizer_monthly_mtu_flag a
    left join batch_sheets.dmt_merchant_list b on a.merchant_id = b.merchant_id
where date(reporting_month) between date(date_trunc('month', add_months(current_date(), -2))) 
and date(date_trunc('month', add_months(current_date(), -1)))
group by all
)

"""
)

# COMMAND ----------

mtu_flag_pd = mtu_flag.toPandas()
mtu_flag_pd

# COMMAND ----------

mtu_flag_pd.columns

# COMMAND ----------



# Check if 'merchant_id' column exists in the DataFrame
if 'merchant_id' in mtu_flag_pd.columns:
    # Perform operations using merchant_id
    t_1_mtu_flag = mtu_flag_pd.pivot_table(
        index="T-1 Flag",
        values="merchant_id",
        aggfunc=pd.Series.nunique
    ).fillna(0).astype(int)
    #.sort_values(by="merchant_id", ascending=False)

    t_2_mtu_flag = mtu_flag_pd.pivot_table(
        index="T-2 Flag",
        values="merchant_id",
        aggfunc=pd.Series.nunique
    ).fillna(0).astype(int)

    # Display or use t_1_mtu_flag and t_2_mtu_flag
    print("T-1 Flag Pivot Table:\n", t_1_mtu_flag)
    print("\nT-2 Flag Pivot Table:\n", t_2_mtu_flag)
else:
    print("Column 'merchant_id' does not exist in the DataFrame.")

# COMMAND ----------

t_1_mtu_flag = mtu_flag_pd.pivot_table(
    index= "T-1 Flag",
    values="merchant_id",
    aggfunc=pd.Series.nunique,).fillna(0).astype(int).sort_values(by="merchant_id", ascending=False)
    
t_2_mtu_flag = mtu_flag_pd.pivot_table(
    index= "T-2 Flag",
    values= "merchant_id",
    aggfunc=pd.Series.nunique,).fillna(0).astype(int)

t_1_mtu_flag
t_2_mtu_flag

# COMMAND ----------

mtu_flag_pd.columns

# COMMAND ----------

t_1_mtu_flag = t_1_mtu_flag.reset_index()

# COMMAND ----------

flag_changed = mtu_flag_pd[mtu_flag_pd['flag_change'] != 0]

# COMMAND ----------

flag_changed = flag_changed[['merchant_id', 'parent_id', 'merchant_name', 'team_owner', 'poc',
                             'golive_month', 'dmt_merchant', 'T-2 Flag', 'T-2 Attempts', 
                             'T-1 Flag', 'T-1 Attempts']]

flag_changed

# COMMAND ----------



# COMMAND ----------

new_live_pd = mtu_flag_pd[mtu_flag_pd['T-1 Flag'] == 'New Live']
revived_pd = mtu_flag_pd[mtu_flag_pd['T-1 Flag'] == 'Revived']
regular_pd = mtu_flag_pd[mtu_flag_pd['T-1 Flag'] == 'Regular']
at_risk_pd = mtu_flag_pd[mtu_flag_pd['T-1 Flag'] == 'At Risk']
hard_churn_live_pd = mtu_flag_pd[mtu_flag_pd['T-1 Flag'] == 'Churn (Hard)']
soft_churn_live_pd = mtu_flag_pd[mtu_flag_pd['T-1 Flag'] == 'Churn (Soft)']


# COMMAND ----------

new_live_pd = new_live_pd[['merchant_id', 'parent_id', 'merchant_name', 'team_owner', 'poc',
                             'golive_month', 'dmt_merchant', 'T-2 Flag', 'T-2 Attempts', 
                             'T-1 Flag', 'T-1 Attempts']]

revived_pd = revived_pd[['merchant_id', 'parent_id', 'merchant_name', 'team_owner', 'poc',
                             'golive_month', 'dmt_merchant', 'T-2 Flag', 'T-2 Attempts', 
                             'T-1 Flag', 'T-1 Attempts']]

regular_pd = regular_pd[['merchant_id', 'parent_id', 'merchant_name', 'team_owner', 'poc',
                             'golive_month', 'dmt_merchant', 'T-2 Flag', 'T-2 Attempts', 
                             'T-1 Flag', 'T-1 Attempts']]

at_risk_pd = at_risk_pd[['merchant_id', 'parent_id', 'merchant_name', 'team_owner', 'poc',
                             'golive_month', 'dmt_merchant', 'T-2 Flag', 'T-2 Attempts', 
                             'T-1 Flag', 'T-1 Attempts']]

hard_churn_live_pd = hard_churn_live_pd[['merchant_id', 'parent_id', 'merchant_name', 'team_owner', 'poc',
                             'golive_month', 'dmt_merchant', 'T-2 Flag', 'T-2 Attempts', 
                             'T-1 Flag', 'T-1 Attempts']]

soft_churn_live_pd = soft_churn_live_pd[['merchant_id', 'parent_id', 'merchant_name', 'team_owner', 'poc',
                             'golive_month', 'dmt_merchant', 'T-2 Flag', 'T-2 Attempts', 
                             'T-1 Flag', 'T-1 Attempts']]


# COMMAND ----------



new_live_pd

# COMMAND ----------

revived_pd

# COMMAND ----------

regular_pd

# COMMAND ----------

# Assuming df is your DataFrame

# Filter the dataframe based on the given conditions
filtered_df_t1 = mtu_flag_pd[(mtu_flag_pd['T-1 External Attempts'] > 0) | ((mtu_flag_pd['dmt_merchant'] == 1) & (mtu_flag_pd['T-1 Attempts'] > 0))]
filtered_df_t2 = mtu_flag_pd[(mtu_flag_pd['T-2 External Attempts'] > 0) | ((mtu_flag_pd['dmt_merchant'] == 1) & (mtu_flag_pd['T-2 Attempts'] > 0))]

# Group by team_owner and count distinct merchant_id
grouped_counts_t1 = filtered_df_t1.groupby('team_owner')['merchant_id'].nunique().reset_index()
grouped_counts_t2 = filtered_df_t2.groupby('team_owner')['merchant_id'].nunique().reset_index()


# Rename the columns for clarity
grouped_counts_t1.columns = ['team_owner', 'MTU']
grouped_counts_t2.columns = ['team_owner', 'MTU']


merged_df = pd.merge(grouped_counts_t1, grouped_counts_t2, on='team_owner', suffixes=(' T-1', ' T-2'))
live_transacting_externally = merged_df
live_transacting_externally.columns = ['Team', 'MTU T-1', 'MTU T-2']

live_transacting_externally

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##Late Auth

# COMMAND ----------

# Convert string dates to datetime
payments_pd["created_date"] = pd.to_datetime(payments_pd["created_date"])

# Filter the DataFrame based on global dates
payments_filtered = payments_pd[
    (((payments_pd["created_date"] >= t2_start)
        & (payments_pd["created_date"] <= t1_end))
        # & (payments_pd["gateway"] != "Razorpay")
        & (payments_pd["golive_date"] != "None")
        & (payments_pd["method_advanced"] != "UPI Collect")
        & (payments_pd["method_advanced"] != "UPI Intent")
        & (payments_pd["method_advanced"] != "upi")
        & (payments_pd["method_advanced"] != "UPI Unknown")
        & (payments_pd["merchant_id"] != "ELi8nocD30pFkb")
        )
    ]
payments_filtered

# COMMAND ----------

# MAGIC %md
# MAGIC ####Overall Level

# COMMAND ----------

# MAGIC %md
# MAGIC ####Overall Late Auth

# COMMAND ----------

overall_late_auth = payments_filtered.pivot_table(
    columns=['Period','late_authorized'],
    values='Total Payments',
    aggfunc=sum).fillna(0).astype(int)

overall_late_auth['T-2 Total'] = (overall_late_auth['T-2']['1'] + overall_late_auth['T-2']['0'])
overall_late_auth['T-1 Total'] = (overall_late_auth['T-1']['1'] + overall_late_auth['T-1']['0'])
overall_late_auth['T-2 %Late Auth'] = ((overall_late_auth['T-2']['1']/overall_late_auth['T-2 Total']) * 100).round(2)
overall_late_auth['T-1 %Late Auth'] = ((overall_late_auth['T-1']['1']/overall_late_auth['T-1 Total']) * 100).round(2)
overall_late_auth['Delta'] = (overall_late_auth['T-1 %Late Auth'] - overall_late_auth['T-2 %Late Auth']).round(2)
overall_late_auth

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gateway Level

# COMMAND ----------

gateway_late_auth = payments_filtered.pivot_table(
        index=['gateway'],
        columns=['Period','late_authorized'],
        values='Total Payments',
        aggfunc = sum).fillna(0).astype(int)
gateway_late_auth = gateway_late_auth.reindex(columns=sorted(gateway_late_auth.columns, reverse=True))

# Assuming gateway is your DataFrame

# Calculate 'T-1 Total' and 'T-2 Total' rounded to 2 decimals
gateway_late_auth['T-2 Total'] = (gateway_late_auth['T-2']['1'] + gateway_late_auth['T-2']['0']).round(2)
gateway_late_auth['T-1 Total'] = (gateway_late_auth['T-1']['1'] + gateway_late_auth['T-1']['0']).round(2)

# Calculate 'T-1 Late Auth' and 'T-2 Late Auth' rounded to 2 decimals
gateway_late_auth['T-2 Late Auth'] = ((gateway_late_auth['T-2']['1'] / gateway_late_auth['T-2 Total']) * 100).round(2)
gateway_late_auth['T-1 Late Auth'] = ((gateway_late_auth['T-1']['1'] / gateway_late_auth['T-1 Total']) * 100).round(2)

# Calculate 'Delta'
gateway_late_auth['Delta'] = (gateway_late_auth['T-1 Late Auth'] - gateway_late_auth['T-2 Late Auth']).round(2)
# gateway = gateway[(gateway['Delta'] > 0.1)]
print(gateway_late_auth)
gateway_late_auth_all=gateway_late_auth.copy()
gateway_late_auth=gateway_late_auth[(gateway_late_auth["T-1"]['1'] >= 10) & (gateway_late_auth["Delta"] > 0.05)]


# Print the DataFrame with the rounded values

#change rohan


gateway_late_auth['Delta Rank'] = gateway_late_auth['Delta'].rank(method='first',ascending=False)
gateway_late_auth['Delta Rank'].fillna(0)
# gateway_filter = gateway[gateway['gateway'] != 'optimizer_razorpay']
gateway_late_auth = gateway_late_auth.sort_values(by=('T-1 Total', ''), ascending=False)
print('Changed')
print(gateway_late_auth)
print(gateway_late_auth_all)

# COMMAND ----------

gateway_for_which_late_auth_increased = gateway_late_auth

# Columns to check
columns_to_check = [
    ('T-2', '0'),
    ('T-2', '1'),
    ('T-1', '0'),
    ('T-1', '1')]

# Check if any of the columns are missing
missing_columns = [col for col in columns_to_check if col not in gateway_for_which_late_auth_increased.columns]

# If any missing columns are found, add them with zeros
if missing_columns:
    for col in missing_columns:
        gateway_for_which_late_auth_increased[col] = 0
print(gateway_for_which_late_auth_increased)

# COMMAND ----------

print(gateway_for_which_late_auth_increased)
gateway_filterd_1 = gateway_for_which_late_auth_increased[gateway_for_which_late_auth_increased["Delta Rank"]== 1.0].reset_index()
gateway_filterd_2 = gateway_for_which_late_auth_increased[gateway_for_which_late_auth_increased["Delta Rank"]== 2.0].reset_index()

print(gateway_filterd_1['gateway'])
print(gateway_filterd_2['gateway'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gateway X Method

# COMMAND ----------

filtered_df_check_1 = payments_filtered[payments_filtered['gateway'].isin(gateway_filterd_1['gateway'])]
filtered_df_check_2 = payments_filtered[payments_filtered['gateway'].isin(gateway_filterd_2['gateway'])]

print(filtered_df_check_1.shape)
print(filtered_df_check_2.shape)

# COMMAND ----------

gateway_drill_1 = filtered_df_check_1.pivot_table(
        index=['gateway','method_advanced'],
        columns=['Period','late_authorized'],
        values='Total Payments',
        aggfunc = sum).fillna(0).astype(int)


if gateway_drill_1.empty:
    gateway_drill_1

else:

    # Reverse column order
    gateway_drill_1 = gateway_drill_1.reindex(columns=sorted(gateway_drill_1.columns, reverse=True))

    # Check if the column ('T-2', '1') exists in the MultiIndex columns
    if ('T-2', '1') in gateway_drill_1.columns:
        # Calculate 'T-2 Total' and 'T-1 Total' when the column exists
        gateway_drill_1['T-2 Total'] = (gateway_drill_1[('T-2', '1')] + gateway_drill_1[('T-2', '0')]).round(2)
        gateway_drill_1['T-1 Total'] = (gateway_drill_1[('T-1', '1')] + gateway_drill_1[('T-1', '0')]).round(2)
        # Calculate 'T-2 Late Auth' and 'T-1 Late Auth'
        gateway_drill_1['T-2 Late Auth'] = ((gateway_drill_1[('T-2', '1')] / gateway_drill_1['T-2 Total']) * 100).round(2)
        gateway_drill_1['T-1 Late Auth'] = ((gateway_drill_1[('T-1', '1')] / gateway_drill_1['T-1 Total']) * 100).round(2)
        # Calculate 'Delta'
        gateway_drill_1['Delta'] = (gateway_drill_1['T-1 Late Auth'] - gateway_drill_1['T-2 Late Auth']).round(2)
    else:
        # Calculate 'T-1 Total' when 'T-2' doesn't exist
        gateway_drill_1['T-1 Total'] = (gateway_drill_1[('T-1', '1')] + gateway_drill_1[('T-1', '0')]).round(2)
        # Calculate 'T-1 Late Auth'
        gateway_drill_1['T-1 Late Auth'] = ((gateway_drill_1[('T-1', '1')] / gateway_drill_1['T-1 Total']) * 100).round(2)
        # Calculate 'Delta' using 'T-1' data
        gateway_drill_1['Delta'] = ((gateway_drill_1[('T-1', '1')] / gateway_drill_1['T-1 Total']) * 100).round(2)

    if ('T-2', '1') in gateway_drill_1.columns:
        gateway_drill_1 = gateway_drill_1[
            ((gateway_drill_1[('T-2', '1')] >= 30) |
            (gateway_drill_1[('T-1', '1')] >= 30)) &
            (gateway_drill_1['Delta'] >= 0.1)
        ]
    else:
        gateway_drill_1 = gateway_drill_1[
            (gateway_drill_1[('T-1', '1')] >= 30) &
            (gateway_drill_1['Delta'] >= 0.1)
        ]

# Display the DataFrame
gateway_drill_1

# COMMAND ----------

# Columns to check
columns_to_check = [
    ('T-2', '0'),
    ('T-2', '1'),
    ('T-1', '0'),
    ('T-1', '1')]

# Check if any of the columns are missing
missing_columns = [col for col in columns_to_check if col not in gateway_drill_1.columns]

# If any missing columns are found, add them with zeros
if missing_columns:
    for col in missing_columns:
        gateway_drill_1[col] = 0
gateway_drill_1

# COMMAND ----------

gateway_drill_2 = filtered_df_check_2.pivot_table(
        index=['gateway','method_advanced'],
        columns=['Period','late_authorized'],
        values='Total Payments',
        aggfunc = sum).fillna(0).astype(int)

if gateway_drill_2.empty:
    gateway_drill_2
else:
    # Reverse column order
    gateway_drill_2 = gateway_drill_2.reindex(columns=sorted(gateway_drill_2.columns, reverse=True))

# Columns to check
columns_to_check = [
    ('T-2', '0'),
    ('T-2', '1'),
    ('T-1', '0'),
    ('T-1', '1')]

# Check if any of the columns are missing
missing_columns = [col for col in columns_to_check if col not in gateway_drill_2.columns]

# If any missing columns are found, add them with zeros
if missing_columns:
    for col in missing_columns:
        gateway_drill_2[col] = 0
gateway_drill_2

    # Check if the column ('T-2', '1') exists in the MultiIndex columns
if ('T-2', '1') in gateway_drill_2.columns:
        # Calculate 'T-2 Total' and 'T-1 Total' when the column exists
        gateway_drill_2['T-2 Total'] = (gateway_drill_2[('T-2', '1')] + gateway_drill_2[('T-2', '0')]).round(2)
        gateway_drill_2['T-1 Total'] = (gateway_drill_2[('T-1', '1')] + gateway_drill_2[('T-1', '0')]).round(2)
        # Calculate 'T-2 Late Auth' and 'T-1 Late Auth'
        gateway_drill_2['T-2 Late Auth'] = ((gateway_drill_2[('T-2', '1')] / gateway_drill_2['T-2 Total']) * 100).round(2)
        gateway_drill_2['T-1 Late Auth'] = ((gateway_drill_2[('T-1', '1')] / gateway_drill_2['T-1 Total']) * 100).round(2)
        # Calculate 'Delta'
        gateway_drill_2['Delta'] = (gateway_drill_2['T-1 Late Auth'] - gateway_drill_2['T-2 Late Auth']).round(2)
else:
        # Calculate 'T-1 Total' when 'T-2' doesn't exist
        gateway_drill_2['T-1 Total'] = (gateway_drill_2[('T-1', '1')] + gateway_drill_2[('T-1', '0')]).round(2)
        # Calculate 'T-1 Late Auth'
        gateway_drill_2['T-1 Late Auth'] = ((gateway_drill_2[('T-1', '1')] / gateway_drill_2['T-1 Total']) * 100).round(2)
        # Calculate 'Delta' using 'T-1' data
        gateway_drill_2['Delta'] = ((gateway_drill_2[('T-1', '1')] / gateway_drill_2['T-1 Total']) * 100).round(2)

if ('T-2', '1') in gateway_drill_2.columns:
        gateway_drill_2 = gateway_drill_2[
            ((gateway_drill_2[('T-2', '1')] >= 30) |
            (gateway_drill_2[('T-1', '1')] >= 30)) &
            (gateway_drill_2['Delta'] >= 0.1)
        ]
else:
        gateway_drill_2 = gateway_drill_2[
            (gateway_drill_2[('T-1', '1')] >= 30) &
            (gateway_drill_2['Delta'] >= 0.1)]
gateway_drill_2    

# COMMAND ----------

gateway_drill_method_1 = gateway_drill_1.reset_index()
gateway_drill_method_2 = gateway_drill_2.reset_index()

print(gateway_drill_method_1['method_advanced'])
print(gateway_drill_method_2['method_advanced'])

# COMMAND ----------

merchants_df_check_1 = payments_filtered[payments_filtered['gateway'].isin(gateway_filterd_1['gateway'])
                                       & 
                                       payments_filtered['method_advanced'].isin(gateway_drill_method_1['method_advanced'])]

merchants_df_check_2 = payments_filtered[payments_filtered['gateway'].isin(gateway_filterd_2['gateway'])
                                       & 
                                       payments_filtered['method_advanced'].isin(gateway_drill_method_2['method_advanced'])]
print(merchants_df_check_1)
print(merchants_df_check_2)

# COMMAND ----------

# Pivot the DataFrame
merchant_drill_1 = merchants_df_check_1.pivot_table(
    index=['merchant_id', 'Name', 'gateway', 'method_advanced'],
    columns=['Period', 'late_authorized'],
    values='Total Payments',
    aggfunc=sum
).fillna(0).astype(int)

# Columns to check
columns_to_check = [
    ('T-2', '0'),
    ('T-2', '1'),
    ('T-1', '0'),
    ('T-1', '1')]

# Check if any of the columns are missing
missing_columns = [col for col in columns_to_check if col not in merchant_drill_1.columns]

# If any missing columns are found, add them with zeros
if missing_columns:
    for col in missing_columns:
        merchant_drill_1[col] = 0
merchant_drill_1


if merchant_drill_1.empty:
    merchant_drill_1
else:

    # Reverse column order
    merchant_drill_1 = merchant_drill_1.reindex(columns=sorted(merchant_drill_1.columns, reverse=True))

    # Check if the column ('T-2', '1') exists in the MultiIndex columns
    if ('T-2', '1') in merchant_drill_1.columns:
        # Calculate 'T-2 Total' and 'T-1 Total' when the column exists
        merchant_drill_1['T-2 Total'] = (merchant_drill_1[('T-2', '1')] + merchant_drill_1[('T-2', '0')]).round(2)
        merchant_drill_1['T-1 Total'] = (merchant_drill_1[('T-1', '1')] + merchant_drill_1[('T-1', '0')]).round(2)
        # Calculate 'T-2 Late Auth' and 'T-1 Late Auth'
        merchant_drill_1['T-2 Late Auth'] = ((merchant_drill_1[('T-2', '1')] / merchant_drill_1['T-2 Total']) * 100).round(2)
        merchant_drill_1['T-1 Late Auth'] = ((merchant_drill_1[('T-1', '1')] / merchant_drill_1['T-1 Total']) * 100).round(2)
        # Calculate 'Delta'
        merchant_drill_1['Delta'] = (merchant_drill_1['T-1 Late Auth'] - merchant_drill_1['T-2 Late Auth']).round(2)
    else:
        # Calculate 'T-1 Total' when 'T-2' doesn't exist
        merchant_drill_1['T-1 Total'] = (merchant_drill_1[('T-1', '1')] + merchant_drill_1[('T-1', '0')]).round(2)
        # Calculate 'T-1 Late Auth'
        merchant_drill_1['T-1 Late Auth'] = ((merchant_drill_1[('T-1', '1')] / merchant_drill_1['T-1 Total']) * 100).round(2)
        # Calculate 'Delta' using 'T-1' data
        merchant_drill_1['Delta'] = ((merchant_drill_1[('T-1', '1')] / merchant_drill_1['T-1 Total']) * 100).round(2)
    merchant_drill_1

    # if ('T-2', '1') in merchant_drill_1.columns:
    #     merchant_drill_1 = merchant_drill_1[
    #         ((merchant_drill_1[('T-2', '1')] >= 10) |
    #         (merchant_drill_1[('T-1', '1')] >= 10)) &
    #         (merchant_drill_1['Delta'] >= 0.2)
    #     ]
    # else:
    #     merchant_drill_1 = merchant_drill_1[
    #         (merchant_drill_1[('T-1', '1')] >= 10) &
    #         (merchant_drill_1['Delta'] >= 0.2)
    #     ]

# Display the DataFrame
merchant_drill_1

# COMMAND ----------

# Pivot the DataFrame
merchant_drill_2 = merchants_df_check_2.pivot_table(
    index=['merchant_id', 'Name', 'gateway', 'method_advanced'],
    columns=['Period', 'late_authorized'],
    values='Total Payments',
    aggfunc=sum
).fillna(0).astype(int)

# Columns to check
columns_to_check = [
    ('T-2', '0'),
    ('T-2', '1'),
    ('T-1', '0'),
    ('T-1', '1')]

# Check if any of the columns are missing
missing_columns = [col for col in columns_to_check if col not in merchant_drill_2.columns]

# If any missing columns are found, add them with zeros
if missing_columns:
    for col in missing_columns:
        merchant_drill_2[col] = 0
merchant_drill_2

if merchant_drill_2.empty:
    merchant_drill_2
else:

    # Reverse column order
    merchant_drill_2 = merchant_drill_2.reindex(columns=sorted(merchant_drill_2.columns, reverse=True))

    # Check if the column ('T-2', '1') exists in the MultiIndex columns
    if ('T-2', '1') in merchant_drill_2.columns:
        # Calculate 'T-2 Total' and 'T-1 Total' when the column exists
        merchant_drill_2['T-2 Total'] = (merchant_drill_2[('T-2', '1')] + merchant_drill_2[('T-2', '0')]).round(2)
        merchant_drill_2['T-1 Total'] = (merchant_drill_2[('T-1', '1')] + merchant_drill_2[('T-1', '0')]).round(2)
        # Calculate 'T-2 Late Auth' and 'T-1 Late Auth'
        merchant_drill_2['T-2 Late Auth'] = ((merchant_drill_2[('T-2', '1')] / merchant_drill_2['T-2 Total']) * 100).round(2)
        merchant_drill_2['T-1 Late Auth'] = ((merchant_drill_2[('T-1', '1')] / merchant_drill_2['T-1 Total']) * 100).round(2)
        # Calculate 'Delta'
        merchant_drill_2['Delta'] = (merchant_drill_2['T-1 Late Auth'] - merchant_drill_2['T-2 Late Auth']).round(2)
    else:
        # Calculate 'T-1 Total' when 'T-2' doesn't exist
        merchant_drill_2['T-1 Total'] = (merchant_drill_2[('T-1', '1')] + merchant_drill_2[('T-1', '0')]).round(2)
        # Calculate 'T-1 Late Auth'
        merchant_drill_2['T-1 Late Auth'] = ((merchant_drill_2[('T-1', '1')] / merchant_drill_2['T-1 Total']) * 100).round(2)
        # Calculate 'Delta' using 'T-1' data
        merchant_drill_2['Delta'] = ((merchant_drill_2[('T-1', '1')] / merchant_drill_2['T-1 Total']) * 100).round(2)
    merchant_drill_2

    if ('T-2', '1') in merchant_drill_2.columns:
        merchant_drill_2 = merchant_drill_2[
            ((merchant_drill_2[('T-2', '1')] >= 30) |
            (merchant_drill_2[('T-1', '1')] >= 30)) &
            (merchant_drill_2['Delta'] >= 0.1)
        ]
    else:
        merchant_drill_2 = merchant_drill_2[
            (merchant_drill_2[('T-1', '1')] >= 30) &
            (merchant_drill_2['Delta'] >= 0.1)
        ]

# Display the DataFrame
merchant_drill_2

# COMMAND ----------

late_auth_last_week = payments_pd[
    (((payments_pd['created_date'] >= t1_start)
      & (payments_pd['created_date'] <= t1_end)) 
     &(payments_pd['gateway'] != 'Razorpay') 
     &(payments_pd['golive_date'] != 'None')
    & (payments_pd['merchant_id'] != 'ELi8nocD30pFkb')
    & (payments_pd["method_advanced"] != "UPI Collect")
    & (payments_pd["method_advanced"] != "UPI Intent")
    & (payments_pd["method_advanced"] != "upi")
    & (payments_pd["method_advanced"] != "UPI Unknown")
    )]
late_auth_last_week

# COMMAND ----------

# MAGIC %md
# MAGIC #### Top 5 Merchants

# COMMAND ----------

top_5_merchants = late_auth_last_week.pivot_table(
        index=['merchant_id','Name','gateway','method_advanced'],
        columns=['Period','late_authorized'],
        values='Total Payments',
        aggfunc = sum).fillna(0).astype(int)
top_5_merchants = top_5_merchants.reindex(columns=sorted(top_5_merchants.columns, reverse=True))

# Columns to check
columns_to_check = [
    ('T-1', '0'),
    ('T-1', '1')]

# Check if any of the columns are missing
missing_columns = [col for col in columns_to_check if col not in top_5_merchants.columns]

# If any missing columns are found, add them with zeros
if missing_columns:
    for col in missing_columns:
        top_5_merchants[col] = 0
top_5_merchants

# Calculate 'T-1 Total' and 'T-2 Total' rounded to 2 decimals
top_5_merchants['T-1 Total'] = (top_5_merchants['T-1']['1'] + top_5_merchants['T-1']['0']).round(2)

# Calculate 'T-1 Late Auth' and 'T-2 Late Auth' rounded to 2 decimals
top_5_merchants['T-1 Late Auth'] = ((top_5_merchants['T-1']['1'] / top_5_merchants['T-1 Total']) * 100).round(2)
print("First:")
print(top_5_merchants.sort_values(by=['T-1 Late Auth'],ascending=False))

# top_5_merchants
top_5_merchants = top_5_merchants[(top_5_merchants['T-1']['1'] >= 30)]
top_5_merchants = top_5_merchants[(top_5_merchants['T-1 Late Auth'] >= 5)]

top_5_merchants_late_auth = top_5_merchants.sort_values(by = ['T-1 Late Auth'],ascending=False).head(5)
top_5_merchants_late_auth

# COMMAND ----------

# MAGIC %md
# MAGIC ###UPI 

# COMMAND ----------

# Convert string dates to datetime
payments_pd["created_date"] = pd.to_datetime(payments_pd["created_date"])

# Filter the DataFrame based on global dates
upi_late_auth = payments_pd[
    (((payments_pd["created_date"] >= t2_start)
        & (payments_pd["created_date"] <= t1_end))
        & (payments_pd["gateway"] != "Razorpay")
        & (payments_pd["golive_date"] != "None")
        & 
        (
        (payments_pd["method_advanced"] == "UPI Collect")| 
        (payments_pd["method_advanced"] == "UPI Intent")| 
        (payments_pd["method_advanced"] == "upi")
        |(payments_pd["method_advanced"] == "UPI Unknown")
        )
        & (payments_pd["merchant_id"] != "ELi8nocD30pFkb")
        & (payments_pd["auth_tat"] != "Unauthorized"))
    ]
upi_late_auth.head(10)

# COMMAND ----------

upi_gateway_late_auth = upi_late_auth.pivot_table(
        index=['gateway', 'auth_tat'],
        columns=['Period'],
        values='Total Payments',
        aggfunc = sum).fillna(0).astype(int)
upi_gateway_late_auth = upi_gateway_late_auth.reindex(columns=sorted(upi_gateway_late_auth.columns, reverse=True))

columns_to_check = [
    ('T-2'),
    ('T-1')]

# Check if any of the columns are missing
missing_columns = [col for col in columns_to_check if col not in upi_gateway_late_auth.columns]

# If any missing columns are found, add them with zeros
if missing_columns:
    for col in missing_columns:
        upi_gateway_late_auth[col] = 0

upi_gateway_late_auth

# COMMAND ----------

# Pivot the DataFrame
upi_gateway_late_auth = upi_late_auth.pivot_table(
    index=['auth_tat'],
    columns=['Period'],
    values='Total Payments',
    aggfunc=sum
).fillna(0).astype(int)

# Columns to check
columns_to_check = [
    ('T-2'),
    ('T-1')]

# Check if any of the columns are missing
missing_columns = [col for col in columns_to_check if col not in upi_gateway_late_auth.columns]

# If any missing columns are found, add them with zeros
if missing_columns:
    for col in missing_columns:
        upi_gateway_late_auth[col] = 0
upi_gateway_late_auth

# Reorder columns
upi_gateway_late_auth = upi_gateway_late_auth.reindex(columns=sorted(upi_gateway_late_auth.columns, reverse=True))

# Calculate the sum of each column
column_sums = upi_gateway_late_auth.sum()

# Calculate the percentage of each column and create a new DataFrame
percentage_df = (upi_gateway_late_auth.div(column_sums, axis=1) * 100).astype(float).round(2)

# Rename the columns to indicate they represent percentages
percentage_df.columns = [f"{col} %" for col in percentage_df.columns]

# Concatenate the original DataFrame and the percentage DataFrame along the columns axis
upi_gateway_late_auth_with_percentage = pd.concat([upi_gateway_late_auth, percentage_df], axis=1)

# Display the DataFrame with the added percentage columns
upi_gateway_late_auth_with_percentage

# COMMAND ----------

# Pivot the DataFrame
upi_gateway_late_auth = upi_late_auth.pivot_table(
    index=['gateway', 'auth_tat'],
    columns=['Period'],
    values='Total Payments',
    aggfunc=sum
).fillna(0).astype(int)

# Columns to check
columns_to_check = [
    ('T-2'),
    ('T-1')]

# Check if any of the columns are missing
missing_columns = [col for col in columns_to_check if col not in upi_gateway_late_auth.columns]

# If any missing columns are found, add them with zeros
if missing_columns:
    for col in missing_columns:
        upi_gateway_late_auth[col] = 0
upi_gateway_late_auth

# Reorder columns
upi_gateway_late_auth = upi_gateway_late_auth.reindex(columns=sorted(upi_gateway_late_auth.columns, reverse=True))

# Add new columns for % payment split
for period in upi_gateway_late_auth.columns:
    # Calculate the total payments for each gateway in the period
    total_payments_period = upi_gateway_late_auth[period].groupby(level=0).transform('sum')
    
    # Calculate the percentage split for each auth_tat
    upi_gateway_late_auth[f'{period} % Split'] = (upi_gateway_late_auth[period] / total_payments_period) * 100

# Round % split columns to 2 decimals
upi_gateway_late_auth[['T-2 % Split', 'T-1 % Split']] = upi_gateway_late_auth[['T-2 % Split', 'T-1 % Split']].round(2)

# Replace NaN values with 0
upi_gateway_late_auth = upi_gateway_late_auth.fillna(0)

# Ensure that % split columns sum up to 100 for each period
for period in upi_gateway_late_auth.columns:
    if '% Split' in period:
        # Adjust the percentage split to ensure it sums up to 100
        upi_gateway_late_auth[period] = upi_gateway_late_auth[period].div(upi_gateway_late_auth[period].groupby(level=0).transform('sum')).mul(100)
upi_gateway_late_auth[['T-2 % Split', 'T-1 % Split']] = upi_gateway_late_auth[['T-2 % Split', 'T-1 % Split']].round(2)

# Replace NaN values with 0
upi_gateway_late_auth = upi_gateway_late_auth.fillna(0)

upi_gateway_late_auth['Delta'] = (upi_gateway_late_auth['T-1 % Split'] - upi_gateway_late_auth['T-2 % Split']).round(2)

# Display the DataFrame
upi_gateway_late_auth

# COMMAND ----------

# Filter the result for auth_tat 'e. > 12 mins'
upi_gateway_late_auth_filtered_check = upi_gateway_late_auth.loc[(slice(None), 'e. > 12 mins'),:]

# Display the filtered DataFrame
upi_gateway_late_auth_filtered_check

# COMMAND ----------

# Pivot the DataFrame
upi_gateway_late_auth_drill = upi_late_auth.pivot_table(
    index=['gateway','method_advanced' ,'auth_tat'],
    columns=['Period'],
    values='Total Payments',
    aggfunc=sum
).fillna(0).astype(int)

# Columns to check
columns_to_check = [
    ('T-2'),
    ('T-1')]

# Check if any of the columns are missing
missing_columns = [col for col in columns_to_check if col not in upi_gateway_late_auth_drill.columns]

# If any missing columns are found, add them with zeros
if missing_columns:
    for col in missing_columns:
        upi_gateway_late_auth_drill[col] = 0
upi_gateway_late_auth_drill

# Reorder columns
upi_gateway_late_auth_drill = upi_gateway_late_auth_drill.reindex(columns=sorted(upi_gateway_late_auth_drill.columns, reverse=True))

# Add new columns for % payment split
for period in upi_gateway_late_auth_drill.columns:
    # Calculate the total payments for each gateway in the period
    total_payments_period = upi_gateway_late_auth_drill[period].groupby(level=0).transform('sum')
    
    # Calculate the percentage split for each auth_tat
    upi_gateway_late_auth_drill[f'{period} % Split'] = (upi_gateway_late_auth_drill[period] / total_payments_period) * 100

# Round % split columns to 2 decimals
upi_gateway_late_auth_drill[['T-2 % Split', 'T-1 % Split']] = upi_gateway_late_auth_drill[['T-2 % Split', 'T-1 % Split']].round(2)

# Replace NaN values with 0
upi_gateway_late_auth_drill = upi_gateway_late_auth_drill.fillna(0)

# Ensure that % split columns sum up to 100 for each period
for period in upi_gateway_late_auth_drill.columns:
    if '% Split' in period:
        # Adjust the percentage split to ensure it sums up to 100
        upi_gateway_late_auth_drill[period] = upi_gateway_late_auth_drill[period].div(upi_gateway_late_auth_drill[period].groupby(level=0).transform('sum')).mul(100)

upi_gateway_late_auth_drill[['T-2 % Split', 'T-1 % Split']] = upi_gateway_late_auth_drill[['T-2 % Split', 'T-1 % Split']].round(2)

# Replace NaN values with 0
upi_gateway_late_auth_drill = upi_gateway_late_auth_drill.fillna(0)

upi_gateway_late_auth_drill['Delta'] = (upi_gateway_late_auth_drill['T-1 % Split'] - upi_gateway_late_auth_drill['T-2 % Split']).round(2)

# Display the DataFrame
upi_gateway_late_auth_drill = upi_gateway_late_auth_drill[upi_gateway_late_auth_drill['T-1'] >= 10]
upi_gateway_late_auth_drill

# Filter the DataFrame for auth_tat 'e. > 12 mins'
upi_gateway_late_auth_drill_df = upi_gateway_late_auth_drill[upi_gateway_late_auth_drill.index.get_level_values('auth_tat') == 'e. > 12 mins']

# Display the filtered DataFrame
upi_gateway_late_auth_drill_df

# COMMAND ----------

# MAGIC %md
# MAGIC To be checked

# COMMAND ----------

upi_gateway_late_auth_drill_inc = upi_gateway_late_auth_drill_df[upi_gateway_late_auth_drill_df['Delta']>=0.1]
upi_gateway_late_auth_drill_inc['Delta Rank'] = upi_gateway_late_auth_drill_inc['Delta'].rank(method='first',ascending=False)

# Columns to check
columns_to_check = [
    ('T-2'),
    ('T-1')]

# Check if any of the columns are missing
missing_columns = [col for col in columns_to_check if col not in upi_gateway_late_auth_drill_inc.columns]

# If any missing columns are found, add them with zeros
if missing_columns:
    for col in missing_columns:
        upi_gateway_late_auth_drill_inc[col] = 0
upi_gateway_late_auth_drill_inc

# COMMAND ----------

#To be checked

# COMMAND ----------

# Check if Delta Rank = 1.0 and 2.0 exist in the column

if upi_gateway_late_auth_drill_inc.empty:
    upi_gateway_late_auth_drill_inc
else:
    # Reverse column order
    upi_gateway_late_auth_drill_inc = upi_gateway_late_auth_drill_inc.reindex(columns=sorted(upi_gateway_late_auth_drill_inc.columns, reverse=True))

upi_gateway_late_auth_drill_inc.columns

# Columns to check
columns_to_check = ['T-2', 'T-1', 'T-2 % Split', 'T-1 % Split']

# Check if any of the columns are missing
missing_columns = [col for col in columns_to_check if col not in upi_gateway_late_auth_drill_inc.columns]

# If any missing columns are found, add them with zeros
if missing_columns:
    for col in missing_columns:
        upi_gateway_late_auth_drill_inc[col] = 0
upi_gateway_late_auth_drill_inc

if 1.0 in upi_gateway_late_auth_drill_inc['Delta Rank'].values:
   df_rank_1 = upi_gateway_late_auth_drill_inc[upi_gateway_late_auth_drill_inc['Delta Rank'] == 1.0]
else : 
    df_rank_1 = upi_gateway_late_auth_drill_inc
df_rank_1  

# COMMAND ----------

df_rank_1_reset = df_rank_1.reset_index()

# Filter gateways and method advanced available in upi_late_auth based on df_rank_1_reset
gateways_available = df_rank_1_reset['gateway'].unique()
methods_available = df_rank_1_reset['method_advanced'].unique()

# Filter upi_late_auth based on available gateways and method advanced
upi_merchants_df_check_1 = upi_late_auth[
    upi_late_auth['gateway'].isin(gateways_available) &
    upi_late_auth['method_advanced'].isin(methods_available)
]

# Print or use upi_merchants_df_check_1 as needed
upi_merchants_df_check_1

# Pivot the DataFrame
upi_merchants_df_check_1 = upi_merchants_df_check_1.pivot_table(
    index=['merchant_id','Name','gateway','method_advanced' ,'auth_tat'],
    columns=['Period'],
    values='Total Payments',
    aggfunc=sum
).fillna(0).astype(int)

upi_merchants_df_check_1

# Columns to check
columns_to_check = [
    ('T-2'),
    ('T-1')]

# Check if any of the columns are missing
missing_columns = [col for col in columns_to_check if col not in upi_merchants_df_check_1.columns]

# If any missing columns are found, add them with zeros
if missing_columns:
    for col in missing_columns:
        upi_merchants_df_check_1[col] = 0
upi_merchants_df_check_1

# Reorder columns
upi_merchants_df_check_1 = upi_merchants_df_check_1.reindex(columns=sorted(upi_merchants_df_check_1.columns, reverse=True))

# Add new columns for % payment split
for period in upi_merchants_df_check_1.columns:
    # Calculate the total payments for each gateway in the period
    total_payments_period = upi_merchants_df_check_1[period].groupby(level=0).transform('sum')
    
    # Calculate the percentage split for each auth_tat
    upi_merchants_df_check_1[f'{period} % Split'] = (upi_merchants_df_check_1[period] / total_payments_period) * 100

# Round % split columns to 2 decimals
upi_merchants_df_check_1[['T-2 % Split', 'T-1 % Split']] = upi_merchants_df_check_1[['T-2 % Split', 'T-1 % Split']].round(2)

# Replace NaN values with 0
upi_merchants_df_check_1 = upi_merchants_df_check_1.fillna(0)

# Ensure that % split columns sum up to 100 for each period
for period in upi_merchants_df_check_1.columns:
    if '% Split' in period:
        # Adjust the percentage split to ensure it sums up to 100
        upi_merchants_df_check_1[period] = upi_merchants_df_check_1[period].div(upi_merchants_df_check_1[period].groupby(level=0).transform('sum')).mul(100)

upi_merchants_df_check_1[['T-2 % Split', 'T-1 % Split']] = upi_merchants_df_check_1[['T-2 % Split', 'T-1 % Split']].round(2)

# Replace NaN values with 0
upi_merchants_df_check_1 = upi_merchants_df_check_1.fillna(0)

upi_merchants_df_check_1['Delta'] = (upi_merchants_df_check_1['T-1 % Split'] - upi_merchants_df_check_1['T-2 % Split']).round(2)

# Display the DataFrame
upi_merchants_df_check_1 = upi_merchants_df_check_1[upi_merchants_df_check_1['T-1'] >= 10]
upi_merchants_df_check_1

# Filter the DataFrame for auth_tat 'e. > 12 mins'
upi_merchants_df_check_1 = upi_merchants_df_check_1[upi_merchants_df_check_1.index.get_level_values('auth_tat') == 'e. > 12 mins']

# Display the filtered DataFrame
upi_merchants_df_check_1

# COMMAND ----------

# Pivot the DataFrame
upi_gateway_late_auth_drill_mx = upi_late_auth.pivot_table(
    index=['merchant_id','Name', 'gateway','method_advanced'],
    columns=['Period'],
    values='Total Payments',
    aggfunc=sum
).fillna(0).astype(int)

# Reorder columns
upi_gateway_late_auth_drill_mx_1 = upi_gateway_late_auth_drill_mx.reindex(columns=sorted(upi_gateway_late_auth_drill_mx.columns, reverse=True))
upi_gateway_late_auth_drill_mx_1.drop(columns='T-2', inplace=True)
upi_gateway_late_auth_drill_mx_1

# COMMAND ----------

# Pivot the DataFrame
upi_gateway_late_auth_drill_mx = upi_late_auth.pivot_table(
    index=['merchant_id','Name','gateway','method_advanced' ,'auth_tat'],
    columns=['Period'],
    values='Total Payments',
    aggfunc=sum
).fillna(0).astype(int)

# Reorder columns
upi_gateway_late_auth_drill_mx = upi_gateway_late_auth_drill_mx.reindex(columns=sorted(upi_gateway_late_auth_drill_mx.columns, reverse=True))

upi_gateway_late_auth_drill_mx = upi_gateway_late_auth_drill_mx.loc[(slice(None), slice(None), slice(None), slice(None), 'e. > 12 mins'), :]
upi_gateway_late_auth_drill_mx.drop(columns='T-2', inplace=True)
upi_gateway_late_auth_drill_mx = upi_gateway_late_auth_drill_mx[upi_gateway_late_auth_drill_mx['T-1']>=10]
upi_gateway_late_auth_drill_mx_check = upi_gateway_late_auth_drill_mx
upi_gateway_late_auth_drill_mx_check

# COMMAND ----------

# Assuming upi_gateway_late_auth_drill_mx_1 and upi_gateway_late_auth_drill_mx_check are your dataframes
merged_df = upi_gateway_late_auth_drill_mx_1.merge(upi_gateway_late_auth_drill_mx_check, 
                                                  on=['merchant_id' , 'Name', 'gateway', 'method_advanced'], 
                                                  how='inner')
merged_df.rename(columns={'T-1_x': 'All Payments', 'T-1_y': 'Payments with auth TAT > 12 mins'}, inplace=True)

# Verify the updated column names
merged_df['% Late Auth'] = ((merged_df['Payments with auth TAT > 12 mins']/ merged_df['All Payments']) * 100).round(2)

merged_df                            

# COMMAND ----------

# MAGIC %md
# MAGIC ## GMV

# COMMAND ----------

# MAGIC %md
# MAGIC ##### WoW GMV

# COMMAND ----------

wow_gmv = payments_pd[
        ((payments_pd["created_date"] >= t2_start)
        & (payments_pd["created_date"] <= t1_end)
        & (payments_pd["golive_date"] != "None")
        & (payments_pd["created_date"] >= payments_pd["golive_date"])
        & (payments_pd["merchant_id"] != "ELi8nocD30pFkb")
        & (payments_pd["Error_Code"] == "Success"))]

wow_gmv_pivot = wow_gmv.pivot_table(
    columns=['Period'],
    values='GMV',
    aggfunc=sum).fillna(0).astype(int)

wow_gmv_pivot = wow_gmv_pivot[['T-2', 'T-1']]
wow_gmv_pivot['T-2 GMV in Cr'] = (wow_gmv_pivot['T-2'] / 10000000).round(2)
wow_gmv_pivot['T-1 GMV in Cr'] = (wow_gmv_pivot['T-1'] / 10000000).round(2)
wow_gmv_pivot['Delta Cr'] = (wow_gmv_pivot['T-1 GMV in Cr'] - wow_gmv_pivot['T-2 GMV in Cr']).round(2)

selected_columns_wow_gmv = ['T-2 GMV in Cr','T-1 GMV in Cr','Delta Cr']
wow_gmv_pivot = wow_gmv_pivot[selected_columns_wow_gmv]
wow_gmv_pivot

# COMMAND ----------

wow_gmv_top_promoters = payments_pd[
    ((payments_pd["created_date"] >= t2_start)
     & (payments_pd["created_date"] <= t1_end)
        & (payments_pd["golive_date"] != "None")
        & (payments_pd["created_date"] >= payments_pd["golive_date"])
        & (payments_pd["merchant_id"] != "ELi8nocD30pFkb")
        & (payments_pd["Error_Code"] == "Success"))]

wow_gmv_top_promoters_pivot = wow_gmv_top_promoters.pivot_table(
    index = ['Name'],
    columns=['Period'],
    values='GMV',
    aggfunc=sum).fillna(0).astype(int)

wow_gmv_top_promoters_pivot = wow_gmv_top_promoters_pivot[['T-2', 'T-1']]
wow_gmv_top_promoters_pivot['T-2 GMV in Cr'] = (wow_gmv_top_promoters_pivot['T-2'] / 10000000).round(2)
wow_gmv_top_promoters_pivot['T-1 GMV in Cr'] = (wow_gmv_top_promoters_pivot['T-1'] / 10000000).round(2)
wow_gmv_top_promoters_pivot['Delta Cr'] = (wow_gmv_top_promoters_pivot['T-1 GMV in Cr'] - wow_gmv_top_promoters_pivot['T-2 GMV in Cr']).round(2)

selected_columns_wow_gmv = ['T-2 GMV in Cr','T-1 GMV in Cr','Delta Cr']
wow_gmv_top_promoters_pivot = wow_gmv_top_promoters_pivot[selected_columns_wow_gmv]
wow_gmv_top_promoters_pivot = wow_gmv_top_promoters_pivot.sort_values(by = "Delta Cr", ascending= False).head(5)
wow_gmv_top_promoters_pivot

# COMMAND ----------

wow_gmv_top_detractors = payments_pd[
    (
        (payments_pd["created_date"] >= t2_start)
        & (payments_pd["created_date"] <= t1_end)
        & (payments_pd["golive_date"] != "None")
        & (payments_pd["created_date"] >= payments_pd["golive_date"])
        & (payments_pd["merchant_id"] != "ELi8nocD30pFkb")
        & (payments_pd["Error_Code"] == "Success"))]

wow_gmv= wow_gmv_top_detractors.pivot_table(
    index = ['Name'],
    columns=['Period'],
    values='GMV',
    aggfunc=sum
).fillna(0).astype(int)

wow_gmv = wow_gmv[['T-2', 'T-1']]
wow_gmv['T-2 GMV in Cr'] = (wow_gmv['T-2'] / 10000000).round(2)
wow_gmv['T-1 GMV in Cr'] = (wow_gmv['T-1'] / 10000000).round(2)
wow_gmv['Delta Cr'] = (wow_gmv['T-1 GMV in Cr'] - wow_gmv['T-2 GMV in Cr']).round(2)

selected_columns_wow_gmv = ['T-2 GMV in Cr','T-1 GMV in Cr','Delta Cr']
wow_gmv = wow_gmv[selected_columns_wow_gmv]
wow_gmv_detractors_pivot = wow_gmv.sort_values(by = "Delta Cr", ascending= True).head(5)
wow_gmv_detractors_pivot

# COMMAND ----------

wow_gmv_detractors_wow = wow_gmv_detractors_pivot.reset_index()
wow_gmv_detractors_wow['Name']

# # Set the option 'mode.use_inf_as_null' to False
# pd.set_option('mode.use_inf_as_null', False)

payments_pd['created_date'] = pd.to_datetime(payments_pd['created_date'])

payments_pd['week'] = payments_pd['created_date'].dt.to_period('W').dt.start_time

wow_gmv_detractors_wow_last_3_months = payments_pd[
    payments_pd['Name'].isin(wow_gmv_detractors_wow['Name']) &
    (payments_pd['created_date'] >= payments_pd['golive_date']) &
    (payments_pd['created_date'] <= t1_end) &
    (payments_pd['Error_Code'] == 'Success')]

# wow_gmv_detractors_wow_last_3_months.head(1)

# Convert GMV to crores
wow_gmv_detractors_wow_last_3_months['GMV in Cr'] = (wow_gmv_detractors_wow_last_3_months['GMV'] / 10**7).round(2)

wow_gmv_detractors_wow_last_3_months_pivot = wow_gmv_detractors_wow_last_3_months.pivot_table(
index = 'week', 
columns = 'Name',
values = 'GMV in Cr',
aggfunc = sum).fillna(0)

wow_gmv_detractors_wow_last_3_months_pivot_interpolated = wow_gmv_detractors_wow_last_3_months_pivot.interpolate(method='spline', order=3)

fig, ax = plt.subplots(figsize=(17.5, 7 ))  # Adjust the figure size
wow_gmv_detractors_wow_last_3_months_pivot.plot(
    ax=ax,
    xticks=wow_gmv_detractors_wow_last_3_months_pivot_interpolated.index,
    marker='o',  # Add markers to data points
    linestyle='dashed',  # Define line style
    linewidth=2,  # Adjust line width
)

# Add values to data points
for date, row in wow_gmv_detractors_wow_last_3_months_pivot.iterrows():
    for column, value in row.items():
        ax.text(date, value, f'{float(value):.0f} Cr', ha='center', va='bottom', fontsize=8, color='black')

# Customize labels and title
plt.title('GMV detractors last 3 month WoW Trend')
plt.xlabel('Week')
plt.ylabel('GMV in Cr')

# Customize legend
plt.legend(loc='center')

ax.set_xticklabels([date.strftime('%Y-%m-%d') for date in wow_gmv_detractors_wow_last_3_months_pivot.index], rotation=60)

# Show plot
plt.tight_layout()  # Adjust layout to prevent cropping
plt.savefig('detractors_gmv_comparison.png')
plt.show()

# COMMAND ----------

# Filter DataFrame based on conditions
wow_gmv_mtd_check = payments_pd[
        (payments_pd['golive_date'] != "None") &
        (payments_pd['created_date'] >= start_date_prev_month) &
        (payments_pd['created_date'] >= payments_pd['golive_date']) &
        (payments_pd['merchant_id'] != "ELi8nocD30pFkb") &
        (payments_pd['Error_Code'] == "Success") &
        (payments_pd['MTD_check'] == 1)]

# COMMAND ----------

wow_gmv_mtd_check['created_date'] = pd.to_datetime(wow_gmv_mtd_check['created_date'])

# Extract month from 'date' column
wow_gmv_mtd_check['month'] = wow_gmv_mtd_check['created_date'].dt.strftime('%Y-%m')

# COMMAND ----------

mtd_pivot = wow_gmv_mtd_check.pivot_table(columns='month', values='GMV', aggfunc=lambda x: sum(x)/10**7).round(2)
print(wow_gmv_mtd_check['month'].value_counts())

mtd_pivot['Delta in cr'] = (mtd_pivot.iloc[:, 1] - mtd_pivot.iloc[:, 0]).round(2)
mtd_pivot['Delta%'] = (((mtd_pivot.iloc[:, 1] / mtd_pivot.iloc[:, 0]) - 1) * 100).round(2)

mtd_pivot_reset = mtd_pivot.reset_index()

# Print the DataFrame with selected columns
print(mtd_pivot_reset)

# Print the first column values
print(mtd_pivot_reset.iloc[:, 0])

# Print the second column values
print(mtd_pivot_reset.iloc[:, 1])

# COMMAND ----------

mtd_pivot = wow_gmv_mtd_check.pivot_table(columns='month', values='GMV', aggfunc=lambda x: sum(x)/10**7).round(2)

mtd_pivot
mtd_pivot.iloc[:, 0]
mtd_pivot.iloc[:, 1]
mtd_pivot['Delta in cr'] = (mtd_pivot.iloc[:, 1] - mtd_pivot.iloc[:, 0]).round(2)
mtd_pivot['Delta%'] = (((mtd_pivot.iloc[:, 1] / mtd_pivot.iloc[:, 0]) - 1) * 100).round(2)
mtd_pivot
mtd_pivot_reset = mtd_pivot.reset_index()

# Print the DataFrame with selected columns
mtd_pivot_reset

# COMMAND ----------

mtd_pivot_promoter = wow_gmv_mtd_check.pivot_table(index = 'Name',columns='month', values='GMV', aggfunc=lambda x: sum(x)/10**7).round(2)
mtd_pivot_promoter
mtd_pivot_promoter.iloc[:, 0]
mtd_pivot_promoter.iloc[:, 1]
mtd_pivot_promoter['Delta in cr'] = (mtd_pivot_promoter.iloc[:, 1] - mtd_pivot_promoter.iloc[:, 0]).round(2)
mtd_pivot_promoter['Delta%'] =  (((mtd_pivot_promoter.iloc[:, 1] / mtd_pivot_promoter.iloc[:, 0]) - 1) * 100).round(2)
top_5_promoters_mtd = mtd_pivot_promoter.sort_values(by = ['Delta in cr'],ascending=False).head(5)
top_5_detractors_mtd = mtd_pivot_promoter.sort_values(by = ['Delta in cr'],ascending=True).head(5)

top_5_promoters_mtd
top_5_detractors_mtd

# COMMAND ----------

# Filter DataFrame based on conditions
mom_gmv_check = payments_pd[
        (payments_pd['golive_date'] != "None") &
        (payments_pd['created_date'] >= start_date_prev_month) &
        (payments_pd['created_date'] >= payments_pd['golive_date']) &
        (payments_pd['merchant_id'] != "ELi8nocD30pFkb") &
        (payments_pd['Error_Code'] == "Success")]
mom_gmv_check['created_date'] = pd.to_datetime(mom_gmv_check['created_date'])

# Extract month from 'date' column
mom_gmv_check['month'] = mom_gmv_check['created_date'].dt.strftime('%Y-%m')

mom_pivot = mom_gmv_check.pivot_table(columns='month', values='GMV', aggfunc=lambda x: sum(x)/10**7).round(2)
mom_pivot.iloc[:, 0]
mom_pivot.iloc[:, 1]
mom_pivot['% of GMV from previous month'] = ((mom_pivot.iloc[:, 1] / mom_pivot.iloc[:, 0])*100).round(2)

mom_pivot_reset = mom_pivot.reset_index()

# Print the DataFrame with selected columns
mom_pivot_reset

# COMMAND ----------

# MAGIC %md
# MAGIC ##MTU

# COMMAND ----------

# Convert golive_date and created_date columns to datetime
payments_pd["golive_date"] = pd.to_datetime(payments_pd["golive_date"])
payments_pd["created_date"] = pd.to_datetime(payments_pd["created_date"])

# Filter the DataFrame based on conditions
mtu_filtered = payments_pd[
    (
        (payments_pd["created_date"] >= t2_start) &
        (payments_pd["created_date"] <= t1_end) &
        (payments_pd["created_date"] >= payments_pd["golive_date"]) &  # Compare datetime objects
        (payments_pd["golive_date"] != "None") 
        & (payments_pd["merchant_id"] != "ELi8nocD30pFkb")
        & (payments_pd["gateway"] != "None" ))]

# Create the pivot table without total column and row
mtu_pivot = mtu_filtered.pivot_table(
    index="Team Mapping",
    columns="Period",
    values="merchant_id",
    aggfunc=pd.Series.nunique,
    margins=True,
    margins_name='Total'
).fillna(0).astype(int)

# mtu_pivot['Total'] = mtu_pivot.sum(axis=0)
mtu_pivot
# Sort the columns in reverse order

mtu_pivot = mtu_pivot.reindex(columns=sorted(mtu_pivot.columns, reverse=True)).astype(int)
mtu_pivot

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transacting this week 

# COMMAND ----------

mtu_filtered

# COMMAND ----------

# Filter out rows with non-finite values in the 'merchant_id' column
mtu_filtered = mtu_filtered[mtu_filtered['merchant_id'].notna()]

# Create the pivot table without total column and row
mtu_pivot_1 = mtu_filtered.pivot_table(
    index=["merchant_id","Name", "Team Mapping"],
    columns="Period",
    values="method_advanced",  # Change this to another column if you want to aggregate
    aggfunc=pd.Series.nunique,  # Count unique merchant_id within each group
    margins=True,
    margins_name='Total'
).fillna(0).astype(int)


columns_to_check = [
    ('T-2'),
    ('T-1')]

# Check if any of the columns are missing
missing_columns = [col for col in columns_to_check if col not in mtu_pivot_1.columns]

# If any missing columns are found, add them with zeros
if missing_columns:
    for col in missing_columns:
        mtu_pivot_1[col] = 0

# Sort columns in reverse order
mtu_pivot_transacting = mtu_pivot_1.reindex(columns=sorted(mtu_pivot_1.columns, reverse=True)).astype(int)

# Compute Delta Mx
mtu_pivot_transacting["Delta Mx"] = mtu_pivot_transacting["T-1"] - mtu_pivot_transacting["T-2"]

# Print the pivot table

mtu_pivot_transacting = mtu_pivot_transacting[mtu_pivot_transacting["T-2"] == 0]
mtu_pivot_transacting

# COMMAND ----------

mtu_pivot_1

# COMMAND ----------

# MAGIC %md
# MAGIC ##Not Transacting This Week

# COMMAND ----------

# Filter out rows with non-finite values in the 'merchant_id' column
mtu_filtered = mtu_filtered[mtu_filtered['merchant_id'].notna()]

# Create the pivot table without total column and row
mtu_pivot_2 = mtu_filtered.pivot_table(
    index=["merchant_id","Name", "Team Mapping"],
    columns="Period",
    values="method_advanced",  # Change this to another column if you want to aggregate
    aggfunc=pd.Series.nunique,  # Count unique merchant_id within each group
    margins=True,
    margins_name='Total'
).fillna(0).astype(int)

columns_to_check = [
    ('T-2'),
    ('T-1')]

# Check if any of the columns are missing
missing_columns = [col for col in columns_to_check if col not in mtu_pivot_2.columns]

# If any missing columns are found, add them with zeros
if missing_columns:
    for col in missing_columns:
        mtu_pivot_2[col] = 0
mtu_pivot_2

# Sort columns in reverse order
mtu_pivot_not_transacting = mtu_pivot_2.reindex(columns=sorted(mtu_pivot_2.columns, reverse=True)).astype(int)

# Compute Delta Mx
mtu_pivot_not_transacting["Delta Mx"] = mtu_pivot_not_transacting["T-1"] - mtu_pivot_not_transacting["T-2"]

# Print the pivot table

mtu_pivot_not_transacting = mtu_pivot_2[(mtu_pivot_2["T-1"] == 0) & (mtu_pivot_2["T-2"] == 1)]
mtu_pivot_not_transacting

# COMMAND ----------

# MAGIC %md
# MAGIC ## Last 15 Days Low Txn (at Risk)
# MAGIC to change the logic for apollo not capturing current month data

# COMMAND ----------

# Apply your additional filter condition here
mtu_filtered = mtu_filtered[
    # Add your filtering conditions here
    # Example: 
    (mtu_filtered["created_date"] >= t2_start) &
    (mtu_filtered["created_date"] <= t1_end) &
    (mtu_filtered["created_date"] >= mtu_filtered["golive_date"]) &  # Compare datetime objects
    (mtu_filtered["golive_date"] != "None") 
    & (mtu_filtered["merchant_id"] != "ELi8nocD30pFkb")
    & (mtu_filtered["gateway"] != "None")]

# Group by merchant_id, Name, and Team Mapping, and calculate the sum of Total Payments and count of unique merchant_id
mtu_risk = mtu_filtered.groupby(["merchant_id", "Name", "Team Mapping"]).agg({"Total Payments": "sum", "merchant_id": "nunique"})

# Filter out rows with Total Payments less than or equal to 10
mtu_risk = mtu_risk[mtu_risk["Total Payments"] <= 10]
mtu_risk

# COMMAND ----------

# MAGIC %md
# MAGIC ###New Live
# MAGIC

# COMMAND ----------

new_live = payments_pd[
        ((payments_pd["golive_date"] >= start_date_prev_month)
        & (payments_pd["merchant_id"] != "ELi8nocD30pFkb"))]
selected_columns_new_live = ['merchant_id', 'Name', 'Team Mapping','golive_date']

new_live = new_live[selected_columns_new_live]
new_live = new_live.drop_duplicates()

new_live_reset = new_live.reset_index()
new_live_reset.columns = ['index', 'MID', 'Name', 'Team Mapping', 'Golive Date']
selected_columns_newLive = ['MID', 'Name', 'Team Mapping', 'Golive Date']
new_live_reset = new_live_reset[selected_columns_newLive]
new_live_reset['Golive Date'] = new_live_reset['Golive Date'].dt.date
new_live_reset = new_live_reset.sort_values(by = 'Golive Date', ascending = False)
new_live_reset

# COMMAND ----------

# MAGIC %md
# MAGIC ##Success Rate

# COMMAND ----------

# Convert golive_date and created_date columns to datetime
payments_pd["golive_date"] = pd.to_datetime(payments_pd["golive_date"])
payments_pd["created_date"] = pd.to_datetime(payments_pd["created_date"])

# Filter the DataFrame based on conditions
SR_filtered = payments_pd[
    (
        (payments_pd["created_date"] >= t2_start) &
        (payments_pd["created_date"] <= t1_end) &
        (payments_pd["created_date"] >= payments_pd["golive_date"]) &  # Compare datetime objects
        (payments_pd["golive_date"] != "None") 
        & (payments_pd["merchant_id"] != "ELi8nocD30pFkb")
        & (payments_pd["gateway"] != "None" ))]

# Create the pivot table without total column and row
SR = SR_filtered.pivot_table(
    index="gateway",
    columns="Period",
    values=["Successful payments", "Total Payments"],
    aggfunc=sum
).fillna(0).astype(int)

# Sort the columns in reverse order
SR = SR.reindex(columns=sorted(SR.columns, reverse=True)).astype(int)
SR

# COMMAND ----------

SR_filtered = payments_pd[
    (
        (payments_pd["created_date"] >= t2_start) &
        (payments_pd["created_date"] <= t1_end) &
        (payments_pd["created_date"] >= payments_pd["golive_date"]) &  # Compare datetime objects
        (payments_pd["golive_date"] != "None") 
        & (payments_pd["merchant_id"] != "ELi8nocD30pFkb")
        & (payments_pd["gateway"] != "None" ))]

# Create the pivot table without total column and row
SR = SR_filtered.pivot_table(
    index="gateway",
    columns="Period",
    values=["Successful payments", "Total Payments"],
    aggfunc=sum
).fillna(0).astype(int)

# Sort the columns in reverse order
SR = SR.reindex(columns=sorted(SR.columns, reverse=True)).astype(int)

# Calculate overall success rates, avoiding division by zero
overall_SR = SR.sum().astype(int)
overall_SR["T-2 SR%"] = round((overall_SR["Successful payments"]["T-2"] / overall_SR["Total Payments"]["T-2"]) * 100, 2)
overall_SR["T-1 SR%"] = round((overall_SR["Successful payments"]["T-1"] / overall_SR["Total Payments"]["T-1"]) * 100, 2)

# Convert the calculated values into a DataFrame
overall_SR_df = pd.DataFrame(overall_SR).T

overall_SR_df[('Total Payments', 'T-2')] = overall_SR_df[('Total Payments', 'T-2')].astype(int)
overall_SR_df[('Total Payments', 'T-1')] = overall_SR_df[('Total Payments', 'T-1')].astype(int)
overall_SR_df[('Successful payments', 'T-2')] = overall_SR_df[('Successful payments', 'T-2')].astype(int)
overall_SR_df[('Successful payments', 'T-1')] = overall_SR_df[('Successful payments', 'T-1')].astype(int)
overall_SR_df['Delta SR'] = (overall_SR_df[('T-1 SR%')] - overall_SR_df[('T-2 SR%')]).round(2)
overall_SR_df

# COMMAND ----------

# Define a custom function to count unique merchant_ids
def count_unique_merchant_ids(x):
    return len(np.unique(x))

# Update the pivot_table function to include the count of unique merchant_ids
SR = SR_filtered.pivot_table(
    index="gateway",
    columns="Period",
    values=["Successful payments", "Total Payments", "merchant_id"],  # Include merchant_id in values
    aggfunc={"Successful payments": sum,
             "Total Payments": sum,
             "merchant_id": count_unique_merchant_ids},  # Aggregate function for each column
    fill_value=0,  # Fill NaN values with 0
    margins=False  # Do not include margins
).astype(int)  # Convert the values to integers

# Rename the count column to "MTU"
SR.rename(columns={('merchant_id', 'MTU'): 'MTU'}, inplace=True)
SR

# COMMAND ----------

SR_filtered = payments_pd[
    (
        (payments_pd["created_date"] >= t2_start) &
        (payments_pd["created_date"] <= t1_end) &
        (payments_pd["created_date"] >= payments_pd["golive_date"]) &  # Compare datetime objects
        (payments_pd["golive_date"] != "None") 
        & (payments_pd["merchant_id"] != "ELi8nocD30pFkb")
        & (payments_pd["gateway"] != "None" ))]

# # Create the pivot table without total column and row
# SR = SR_filtered.pivot_table(
#     index="gateway",
#     columns="Period",
#     values=["Successful payments", "Total Payments"],
#     aggfunc=sum
# ).fillna(0).astype(int)

SR["T-2 SR"] = ((SR["Successful payments"]["T-2"] / SR["Total Payments"]["T-2"]) * 100).round(2)
SR["T-1 SR"] = ((SR["Successful payments"]["T-1"] / SR["Total Payments"]["T-1"]) * 100).round(2)
SR["Delta SR"] = (SR["T-1 SR"] - SR["T-2 SR"]).round(2)
SR["Delta Attempts % Change"] = (((SR["Total Payments"]["T-1"] / SR["Total Payments"]["T-2"]) - 1) * 100).round(2)
SR = SR.sort_values(by=("Total Payments", "T-1"), ascending=False)  # Corrected sort_values function
SR

# COMMAND ----------

# MAGIC %md
# MAGIC ##Gateways That Saw Dip in SR

# COMMAND ----------

SR_negative = SR[(SR['Delta SR'] < 0)]
SR_negative = SR_negative.sort_values(by=[('Total Payments', 'T-2')], ascending=False)
SR_negative['Delta Rank'] = SR_negative['Delta SR'].rank(method='first',ascending=True).astype(int)
SR_negative = SR_negative[SR_negative['Delta Rank'] <= 3]
SR_negative

# COMMAND ----------

# Columns to check
columns_to_check = [
    ('Successful payments', 'T-1'),
    ('Successful payments', 'T-2'),
    ('Total Payments', 'T-1'),
    ('Total Payments', 'T-2')]

# Check if any of the columns are missing
missing_columns = [col for col in columns_to_check if col not in SR_negative.columns]

# If any missing columns are found, add them with zeros
if missing_columns:
    for col in missing_columns:
        SR_negative[col] = 0
SR_negative        

# COMMAND ----------

# if SR_negative_reset_1.empty:
#     SR_negative_reset_1
# else:
#     # Reverse column order
#     SR_negative_reset_1 = SR_negative_reset_1.reindex(columns=sorted(SR_negative_reset_1.columns, reverse=True))

# SR_negative_reset_1.columns

# # Columns to check
# columns_to_check = ['T-2', 'T-1', 'T-2 % Split', 'T-1 % Split']

# # Check if any of the columns are missing
# missing_columns = [col for col in columns_to_check if col not in upi_gateway_late_auth_drill_inc.columns]

# # If any missing columns are found, add them with zeros
# if missing_columns:
#     for col in missing_columns:
#         upi_gateway_late_auth_drill_inc[col] = 0
# upi_gateway_late_auth_drill_inc

# if 1.0 in upi_gateway_late_auth_drill_inc['Delta Rank'].values:
# #    df_rank_1 = upi_gateway_late_auth_drill_inc[upi_gateway_late_auth_drill_inc['Delta Rank'] == 1.0]
# else : 
#     df_rank_1 = upi_gateway_late_auth_drill_inc
# df_rank_1  

# COMMAND ----------

SR_negative_reset_1 = SR_negative[SR_negative["Delta Rank"] == 1].reset_index()

if SR_negative_reset_1.empty:
    SR_negative_reset_1
else:
    # Reverse column order
    SR_negative_reset_1 = SR_negative_reset_1.reindex(columns=sorted(SR_negative_reset_1.columns, reverse=True))

SR_negative_reset_df_1 = payments_pd[
    (
        (payments_pd["created_date"] >= t2_start) &
        (payments_pd["created_date"] <= t1_end) &
        (payments_pd["created_date"] >= payments_pd["golive_date"]) &  # Compare datetime objects
        (payments_pd["golive_date"] != "None") 
        & (payments_pd["merchant_id"] != "ELi8nocD30pFkb")
        & (payments_pd["gateway"].isin(SR_negative_reset_1["gateway"])))]

SR_negative_reset_df_1_pivot = SR_negative_reset_df_1.pivot_table(
    index=["gateway","method_advanced"],
    columns="Period",
    values=["Successful payments", "Total Payments"],
    aggfunc=sum).fillna(0).astype(int)

if SR_negative_reset_df_1_pivot.empty:
    SR_negative_resret_df_1_pivot
else:
    # Reverse column order
    SR_negative_reset_df_1_pivot = SR_negative_reset_df_1_pivot.reindex(columns=sorted(SR_negative_reset_df_1_pivot.columns, reverse=True))

# Columns to check
columns_to_check = [
    ('Successful payments', 'T-1'),
    ('Successful payments', 'T-2'),
    ('Total Payments', 'T-1'),
    ('Total Payments', 'T-2')
]

# Check if any of the columns are missing
missing_columns = [col for col in columns_to_check if col not in SR_negative_reset_df_1_pivot.columns]

# If any missing columns are found, add them with zeros
if missing_columns:
    for col in missing_columns:
        SR_negative_reset_df_1_pivot[col] = 0

if SR_negative_reset_df_1_pivot.empty:
    SR_negative_reset_df_1_pivot
else:    
    SR_negative_reset_df_1_pivot = SR_negative_reset_df_1_pivot.reindex(columns=sorted(SR_negative_reset_df_1_pivot.columns, reverse=True))
    SR_negative_reset_df_1_pivot["T-2 SR"] = ((SR_negative_reset_df_1_pivot["Successful payments"]["T-2"] / SR_negative_reset_df_1_pivot["Total Payments"]["T-2"]) * 100).round(2)
    SR_negative_reset_df_1_pivot["T-1 SR"] = ((SR_negative_reset_df_1_pivot["Successful payments"]["T-1"] / SR_negative_reset_df_1_pivot["Total Payments"]["T-1"]) * 100).round(2)
    SR_negative_reset_df_1_pivot["Delta SR"] = (SR_negative_reset_df_1_pivot["T-1 SR"] - SR_negative_reset_df_1_pivot["T-2 SR"]).round(2)
    SR_negative_reset_df_1_pivot["Delta Attempts % Change"] = (((SR_negative_reset_df_1_pivot["Total Payments"]["T-1"] - SR_negative_reset_df_1_pivot["Total Payments"]["T-2"]) / SR_negative_reset_df_1_pivot["Total Payments"]["T-2"]) * 100).round(2)
    # SR_negative_reset_df_1_pivot = SR_negative_reset_df_1_pivot[SR_negative_reset_df_1_pivot["Total Payments"]["T-2"] >=50]
    SR_negative_reset_df_1_pivot = SR_negative_reset_df_1_pivot[SR_negative_reset_df_1_pivot["Delta SR"] < 0]
    SR_negative_reset_df_1_pivot = SR_negative_reset_df_1_pivot.sort_values(by=('Total Payments', 'T-1'), ascending=False)

SR_negative_reset_df_1_pivot

# COMMAND ----------

#SR_negative_reset_2 = SR_negative[SR_negative["Delta Rank"] == 2].reset_index()


SR_negative_reset_1_method = SR_negative_reset_df_1_pivot.reset_index()

if SR_negative_reset_1_method.empty:
    SR_negative_reset_1_method
else:
    # Reverse column order
    SR_negative_reset_1_method = SR_negative_reset_1_method.reindex(columns=sorted(SR_negative_reset_1_method.columns, reverse=True))

SR_negative_reset_df_1 = payments_pd[
    (
        (payments_pd["created_date"] >= t2_start) &
        (payments_pd["created_date"] <= t1_end) &
        (payments_pd["created_date"] >= payments_pd["golive_date"]) &  # Compare datetime objects
        (payments_pd["golive_date"] != "None") 
        & (payments_pd["merchant_id"] != "ELi8nocD30pFkb")
        & (payments_pd["gateway"].isin(SR_negative_reset_1["gateway"]))
        & (payments_pd["method_advanced"].isin(SR_negative_reset_1_method["method_advanced"])))]

SR_negative_reset_df_1_mx_pivot = SR_negative_reset_df_1.pivot_table(
    index=["merchant_id","Name","gateway","method_advanced"],
    columns="Period",
    values=["Successful payments", "Total Payments"],
    aggfunc=sum).fillna(0).astype(int)

SR_negative_reset_df_1_mx_pivot

if SR_negative_reset_df_1_mx_pivot.empty:
    SR_negative_reset_df_1_mx_pivot
else:
    # Reverse column order
    SR_negative_reset_df_1_mx_pivot = SR_negative_reset_df_1_mx_pivot.reindex(columns=sorted(SR_negative_reset_df_1_mx_pivot.columns, reverse=True))

# Columns to check
columns_to_check = [
    ('Successful payments', 'T-1'),
    ('Successful payments', 'T-2'),
    ('Total Payments', 'T-1'),
    ('Total Payments', 'T-2')
]

# Check if any of the columns are missing
missing_columns = [col for col in columns_to_check if col not in SR_negative_reset_df_1_mx_pivot.columns]

# If any missing columns are found, add them with zeros
if missing_columns:
    for col in missing_columns:
        SR_negative_reset_df_1_mx_pivot[col] = 0

SR_negative_reset_df_1_mx_pivot

if SR_negative_reset_df_1_mx_pivot.empty:
    SR_negative_reset_df_1_mx_pivot

else:

    SR_negative_reset_df_1_mx_pivot.reindex(columns=sorted(SR_negative_reset_df_1_mx_pivot.columns, reverse=True))
    SR_negative_reset_df_1_mx_pivot["T-2 SR"] = ((SR_negative_reset_df_1_mx_pivot["Successful payments"]["T-2"] / SR_negative_reset_df_1_mx_pivot["Total Payments"]["T-2"]) * 100).round(2)
    SR_negative_reset_df_1_mx_pivot["T-1 SR"] = ((SR_negative_reset_df_1_mx_pivot["Successful payments"]["T-1"] / SR_negative_reset_df_1_mx_pivot["Total Payments"]["T-1"]) * 100).round(2)
    SR_negative_reset_df_1_mx_pivot["Delta SR"] = (SR_negative_reset_df_1_mx_pivot["T-1 SR"] - SR_negative_reset_df_1_mx_pivot["T-2 SR"]).round(2)
    SR_negative_reset_df_1_mx_pivot["Delta Attempts % Change"] = (((SR_negative_reset_df_1_mx_pivot["Total Payments"]["T-1"] - SR_negative_reset_df_1_mx_pivot["Total Payments"]["T-2"]) / SR_negative_reset_df_1_mx_pivot["Total Payments"]["T-2"]) * 100).round(2)
    SR_negative_reset_df_1_mx_pivot = SR_negative_reset_df_1_mx_pivot[SR_negative_reset_df_1_mx_pivot["Delta SR"] < 0]
    SR_negative_reset_df_1_mx_pivot = SR_negative_reset_df_1_mx_pivot[SR_negative_reset_df_1_mx_pivot["Total Payments"]["T-2"] >=50]
    SR_negative_reset_df_1_mx_pivot = SR_negative_reset_df_1_mx_pivot[SR_negative_reset_df_1_mx_pivot["Delta SR"] < 0]
    SR_negative_reset_df_1_mx_pivot = SR_negative_reset_df_1_mx_pivot.sort_values(by=('Total Payments', 'T-1'), ascending=False)
SR_negative_reset_df_1_mx_pivot

# COMMAND ----------

SR_negative_reset_2 = SR_negative[SR_negative["Delta Rank"] == 2].reset_index()

if SR_negative_reset_2.empty:
    SR_negative_reset_2
else:
    # Reverse column order
    SR_negative_reset_2 = SR_negative_reset_2.reindex(columns=sorted(SR_negative_reset_2.columns, reverse=True))


SR_negative_reset_df_2 = payments_pd[
    (
        (payments_pd["created_date"] >= t2_start) &
        (payments_pd["created_date"] <= t1_end) &
        (payments_pd["created_date"] >= payments_pd["golive_date"]) &  # Compare datetime objects
        (payments_pd["golive_date"] != "None") 
        & (payments_pd["merchant_id"] != "ELi8nocD30pFkb")
        & (payments_pd["gateway"].isin(SR_negative_reset_2["gateway"])))]


SR_negative_reset_df_2_pivot = SR_negative_reset_df_2.pivot_table(
    index=["gateway","method_advanced"],
    columns="Period",
    values=["Successful payments", "Total Payments"],
    aggfunc=sum).fillna(0).astype(int)

if SR_negative_reset_df_2_pivot.empty:
    SR_negative_reset_df_2_pivot
else:
    # Reverse column order
    SR_negative_reset_df_2_pivot = SR_negative_reset_df_2_pivot.reindex(columns=sorted(SR_negative_reset_df_2_pivot.columns, reverse=True))

# Columns to check
columns_to_check = [
    ('Successful payments', 'T-1'),
    ('Successful payments', 'T-2'),
    ('Total Payments', 'T-1'),
    ('Total Payments', 'T-2')
]

# Check if any of the columns are missing
missing_columns = [col for col in columns_to_check if col not in SR_negative_reset_df_2_pivot.columns]

# If any missing columns are found, add them with zeros
if missing_columns:
    for col in missing_columns:
        SR_negative_reset_df_2_pivot[col] = 0

if SR_negative_reset_df_2_pivot.empty:
    SR_negative_reset_df_2_pivot
else:

    SR_negative_reset_df_2_pivot["T-2 SR"] = ((SR_negative_reset_df_2_pivot["Successful payments"]["T-2"] / SR_negative_reset_df_2_pivot["Total Payments"]["T-2"]) * 100).round(2)
    SR_negative_reset_df_2_pivot["T-1 SR"] = ((SR_negative_reset_df_2_pivot["Successful payments"]["T-1"] / SR_negative_reset_df_2_pivot["Total Payments"]["T-1"]) * 100).round(2)
    SR_negative_reset_df_2_pivot["Delta SR"] = (SR_negative_reset_df_2_pivot["T-1 SR"] - SR_negative_reset_df_2_pivot["T-2 SR"]).round(2)
    SR_negative_reset_df_2_pivot["Delta Attempts % Change"] = (((SR_negative_reset_df_2_pivot["Total Payments"]["T-1"] - SR_negative_reset_df_2_pivot["Total Payments"]["T-2"]) / SR_negative_reset_df_2_pivot["Total Payments"]["T-2"]) * 100).round(2)
    SR_negative_reset_df_2_pivot = SR_negative_reset_df_2_pivot[SR_negative_reset_df_2_pivot["Delta SR"] < 0]
    SR_negative_reset_df_2_pivot = SR_negative_reset_df_2_pivot.sort_values(by=('Total Payments', 'T-1'), ascending=False)
SR_negative_reset_df_2_pivot

# COMMAND ----------

SR_negative_reset_2_method = SR_negative_reset_df_2_pivot.reset_index()

SR_negative_reset_df_2 = payments_pd[
    (
        (payments_pd["created_date"] >= t2_start) &
        (payments_pd["created_date"] <= t1_end) &
        (payments_pd["created_date"] >= payments_pd["golive_date"]) &  # Compare datetime objects
        (payments_pd["golive_date"] != "None") 
        & (payments_pd["merchant_id"] != "ELi8nocD30pFkb")
        & (payments_pd["gateway"].isin(SR_negative_reset_2["gateway"]))
        & (payments_pd["method_advanced"].isin(SR_negative_reset_2_method["method_advanced"])))]

SR_negative_reset_df_2_mx_pivot = SR_negative_reset_df_2.pivot_table(
    index=["merchant_id","Name","gateway","method_advanced"],
    columns="Period",
    values=["Successful payments", "Total Payments"],
    aggfunc=sum).fillna(0).astype(int)

if SR_negative_reset_df_2_mx_pivot.empty:
    SR_negative_reset_df_2_mx_pivot

else:
    SR_negative_reset_df_2_mx_pivot.reindex(columns=sorted(SR_negative_reset_df_2_mx_pivot.columns, reverse=True))
    SR_negative_reset_df_2_mx_pivot["T-2 SR"] = ((SR_negative_reset_df_2_mx_pivot["Successful payments"]["T-2"] / SR_negative_reset_df_2_mx_pivot["Total Payments"]["T-2"]) * 100).round(2)
    SR_negative_reset_df_2_mx_pivot["T-1 SR"] = ((SR_negative_reset_df_2_mx_pivot["Successful payments"]["T-1"] / SR_negative_reset_df_2_mx_pivot["Total Payments"]["T-1"]) * 100).round(2)
    SR_negative_reset_df_2_mx_pivot["Delta SR"] = (SR_negative_reset_df_2_mx_pivot["T-1 SR"] - SR_negative_reset_df_2_mx_pivot["T-2 SR"]).round(2)

    SR_negative_reset_df_2_mx_pivot["Delta Attempts % Change"] = (((SR_negative_reset_df_2_mx_pivot["Total Payments"]["T-1"] - SR_negative_reset_df_2_mx_pivot["Total Payments"]["T-2"]) / SR_negative_reset_df_2_mx_pivot["Total Payments"]["T-2"]) * 100).round(2)
    SR_negative_reset_df_2_mx_pivot = SR_negative_reset_df_2_mx_pivot[SR_negative_reset_df_2_mx_pivot["Delta SR"] < 0]
    SR_negative_reset_df_2_mx_pivot = SR_negative_reset_df_2_mx_pivot[SR_negative_reset_df_2_mx_pivot["Total Payments"]["T-1"] >=50]
    SR_negative_reset_df_2_mx_pivot = SR_negative_reset_df_2_mx_pivot[SR_negative_reset_df_2_mx_pivot["Total Payments"]["T-2"] >=50]
    SR_negative_reset_df_2_mx_pivot = SR_negative_reset_df_2_mx_pivot[SR_negative_reset_df_2_mx_pivot["Delta SR"] < -1]
    SR_negative_reset_df_2_pivot_mx = SR_negative_reset_df_2_mx_pivot.sort_values(by=('Total Payments', 'T-1'), ascending=False)
SR_negative_reset_df_2_pivot_mx

# COMMAND ----------

# MAGIC %md
# MAGIC ##Gateways That Saw Increase in SR

# COMMAND ----------

SR_positive = SR[(SR['Delta SR'] > 0)]
SR_positive = SR_positive.sort_values(by=[('Total Payments', 'T-2')], ascending=False)
SR_positive['Delta Rank'] = SR_positive['Delta SR'].rank(method='first',ascending=False).astype(int)
SR_positive = SR_positive[SR_positive['Delta Rank'] <= 3]
SR_positive.sort_values(by = 'Delta Rank', ascending=True)

# COMMAND ----------

# Columns to check
columns_to_check = [
    ('Successful payments', 'T-1'),
    ('Successful payments', 'T-2'),
    ('Total Payments', 'T-1'),
    ('Total Payments', 'T-2')]

# Check if any of the columns are missing
missing_columns = [col for col in columns_to_check if col not in SR_positive.columns]

# If any missing columns are found, add them with zeros
if missing_columns:
    for col in missing_columns:
        SR_positive[col] = 0
SR_positive        

# COMMAND ----------

# MAGIC %md
# MAGIC ##Method Level SR

# COMMAND ----------

SR_filtered = payments_pd[
    (
        (payments_pd["created_date"] >= t2_start) &
        (payments_pd["created_date"] <= t1_end) &
        (payments_pd["created_date"] >= payments_pd["golive_date"]) &  # Compare datetime objects
        (payments_pd["golive_date"] != "None") 
        & (payments_pd["gateway"] != None ))]

# COMMAND ----------

SR_filtered = SR_filtered[SR_filtered['gateway']!= None]

# Create the pivot table
SR_method = SR_filtered.pivot_table(
    index='method_advanced',
    columns='Period',
    values=['Successful payments', 'Total Payments'],
    aggfunc=sum)

# Fill NaN values with 0 and convert to integer type
SR_method = SR_method.fillna(0).astype(int)

# Sort the columns in reverse order
SR_method = SR_method.reindex(columns=sorted(SR_method.columns, reverse=True))

# Calculate success rates, avoiding division by zero
SR_method['T-2 SR'] = ((SR_method['Successful payments']['T-2'] / SR_method['Total Payments']['T-2']) * 100).round(2)
SR_method['T-1 SR'] = ((SR_method['Successful payments']['T-1'] / SR_method['Total Payments']['T-1']) * 100).round(2)
SR_method['Delta SR'] = (SR_method['T-1 SR'] - SR_method['T-2 SR']).round(2)

# Print the DataFrame
SR_method = SR_method.sort_values(by=("Total Payments", "T-1"), ascending=False)
SR_method

# COMMAND ----------

# MAGIC %md
# MAGIC ##WoW SR

# COMMAND ----------

payments_pd['created_date'] = pd.to_datetime(payments_pd['created_date'])

payments_pd['week'] = payments_pd['created_date'].dt.to_period('W').dt.start_time

wow_sr = payments_pd[
        (payments_pd["created_date"] >= start_date_prev_month) &
        (payments_pd["created_date"] <= t1_end) &
        (payments_pd["created_date"] >= payments_pd["golive_date"]) &  # Compare datetime objects
        (payments_pd["golive_date"] != "None") 
        & (payments_pd["merchant_id"] != "ELi8nocD30pFkb")
        & (payments_pd["gateway"] != "None" )]

# Pivot the DataFrame to calculate the total successful payments and total payments for each week and gateway
wow_sr_pivot = wow_sr.pivot_table(index=['gateway','week'], values=['Successful payments', 'Total Payments'], aggfunc='sum')

wow_sr_pivot['SR'] = ((wow_sr_pivot['Successful payments'] / wow_sr_pivot['Total Payments']) * 100).round(2)
wow_sr_pivot_reset = wow_sr_pivot.reset_index()
wow_sr_pivot_reset

# COMMAND ----------

# Reset index for pivot table
wow_sr_pivot_reset = wow_sr_pivot.reset_index()

# Set figure size for the first chart
plt.figure(figsize=(17.5, 7))

# Define the width of each line
line_width = 1

# Get the first six gateways
gateways_first = wow_sr_pivot_reset['gateway'].unique()[:6]

# Iterate over each unique gateway and plot its SR over time for the first chart
for gateway in gateways_first:
    gateway_data = wow_sr_pivot_reset[wow_sr_pivot_reset['gateway'] == gateway]
    plt.plot(gateway_data['week'], gateway_data['SR'], marker='o', linestyle='--', linewidth=line_width, label=gateway)

    # Add values to the chart with slight offsets
    for idx, val in enumerate(gateway_data['SR']):
        formatted_val = '{:.1f}%'.format(val)  # Round SR to 1 decimal place
        plt.text(gateway_data['week'].iloc[idx], val, formatted_val, ha='center', va='bottom', fontsize=10, color='black', rotation=45)

# Set labels and title for the first chart
plt.xlabel('Week')
plt.ylabel('SR%')
plt.title('Gateway-Level SR Over Time (First 6 Gateways)')
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
plt.legend()  # Show legend with gateway names
plt.grid(False)  # Remove gridlines
plt.tight_layout()  # Adjust layout to prevent clipping of labels

# Save the first plot as an image file
plt.savefig('wow_sr_pivot_first.png')

# Show the first plot
plt.show()

# Set figure size for the second chart
plt.figure(figsize=(17.5, 7))

# Get the remaining gateways
gateways_remaining = wow_sr_pivot_reset['gateway'].unique()[6:]

# Iterate over each unique gateway and plot its SR over time for the second chart
for gateway in gateways_remaining:
    gateway_data = wow_sr_pivot_reset[wow_sr_pivot_reset['gateway'] == gateway]
    plt.plot(gateway_data['week'], gateway_data['SR'], marker='o', linestyle='--', linewidth=line_width, label=gateway)

    # Add values to the chart with slight offsets
    for idx, val in enumerate(gateway_data['SR']):
        formatted_val = '{:.1f}%'.format(val)  # Round SR to 1 decimal place
        plt.text(gateway_data['week'].iloc[idx], val, formatted_val, ha='center', va='bottom', fontsize=10, color='black', rotation=45)

# Set labels and title for the second chart
plt.xlabel('Week')
plt.ylabel('SR%')
plt.title('Gateway-Level SR Over Time (Remaining Gateways)')
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
plt.legend()  # Show legend with gateway names
plt.grid(False)  # Remove gridlines
plt.tight_layout()  # Adjust layout to prevent clipping of labels

# Save the second plot as an image file
plt.savefig('wow_sr_pivot_second.png')

# Show the second plot
plt.show()


# COMMAND ----------


wow_sr_pivot_token = payments_pd[
        (payments_pd["created_date"] >= start_date_prev_month) &
        (payments_pd["created_date"] <= t1_end) &
        (payments_pd["created_date"] >= payments_pd["golive_date"]) &  # Compare datetime objects
        (payments_pd["golive_date"] != "None") 
        & (payments_pd["merchant_id"] != "ELi8nocD30pFkb")
        & (payments_pd["gateway"] != "None")
        & (
        (payments_pd["method_advanced"] == "Credit Card")
        | (payments_pd["method_advanced"] == "Debit Card")
        | (payments_pd["method_advanced"] == "Prepaid Card")
        | (payments_pd["method_advanced"] == "Unknown Card" )
        | (payments_pd["method_advanced"] == "card" )
        )
        & (payments_pd["network_tokenised_payment"] == '1' )
        ]

wow_sr_pivot_token

# Pivot the DataFrame to calculate the total successful payments and total payments for each week and gateway
wow_sr_pivot_token = wow_sr_pivot_token.pivot_table(index=['gateway','week'], values=['Successful payments', 'Total Payments'], aggfunc='sum')

wow_sr_pivot_token['SR'] = ((wow_sr_pivot_token['Successful payments'] / wow_sr_pivot_token['Total Payments']) * 100).round(2)
wow_sr_pivot_token = wow_sr_pivot_token.reset_index()
wow_sr_pivot_token

# COMMAND ----------

# Set figure size for the first chart
plt.figure(figsize=(17.5, 7))

# Define the width of each line
line_width = 1

# Get the first six gateways
gateways_first = wow_sr_pivot_token['gateway'].unique()[:6]

# Iterate over each unique gateway and plot its SR over time for the first chart
for gateway in gateways_first:
    gateway_data = wow_sr_pivot_token[wow_sr_pivot_token['gateway'] == gateway]
    plt.plot(gateway_data['week'], gateway_data['SR'], marker='o', linestyle='--', linewidth=line_width, label=gateway)

    # Add values to the chart with slight offsets
    for idx, val in enumerate(gateway_data['SR']):
        formatted_val = '{:.1f}%'.format(val)  # Round SR to 1 decimal place
        plt.text(gateway_data['week'].iloc[idx], val, formatted_val, ha='center', va='bottom', fontsize=10, color='black', rotation=45)

# Set labels and title for the first chart
plt.xlabel('Week')
plt.ylabel('SR%')
plt.title('Gateway-Level Tokenization SR (First 6 Gateways)')
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
plt.legend()  # Show legend with gateway names
plt.grid(False)  # Remove gridlines
plt.tight_layout()  # Adjust layout to prevent clipping of labels

# Save the first plot as an image file
plt.savefig('wow_sr_tokenization.png')

# Show the first plot
plt.show()

# Set figure size for the second chart
plt.figure(figsize=(17.5, 7))

# Get the remaining gateways
gateways_remaining = wow_sr_pivot_token['gateway'].unique()[6:]

# Iterate over each unique gateway and plot its SR over time for the second chart
for gateway in gateways_remaining:
    gateway_data = wow_sr_pivot_token[wow_sr_pivot_token['gateway'] == gateway]
    plt.plot(gateway_data['week'], gateway_data['SR'], marker='o', linestyle='--', linewidth=line_width, label=gateway)

    # Add values to the chart with slight offsets
    for idx, val in enumerate(gateway_data['SR']):
        formatted_val = '{:.1f}%'.format(val)  # Round SR to 1 decimal place
        plt.text(gateway_data['week'].iloc[idx], val, formatted_val, ha='center', va='bottom', fontsize=10, color='black', rotation=45)

# Set labels and title for the second chart
plt.xlabel('Week')
plt.ylabel('SR%')
plt.title('Gateway-Level Tokenization SR (Remaining Gateways)')
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
plt.legend()  # Show legend with gateway names
plt.grid(False)  # Remove gridlines
plt.tight_layout()  # Adjust layout to prevent clipping of labels

# Save the second plot as an image file
plt.savefig('wow_sr_tokenization_2.png')

# Show the second plot
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##WoW Attempts

# COMMAND ----------

# Group by week and gateway and sum total payments
total_payments_weekly = wow_sr_pivot.groupby(['week', 'gateway'])['Total Payments'].sum().unstack()

# Fill missing values with zeros
total_payments_weekly.fillna(0, inplace=True)

# Sort gateways by total attempts
total_attempts = total_payments_weekly.sum().sort_values(ascending=False).index

# Define the width of each bar
bar_width = 0.1

# Calculate the x-coordinate for each bar
x = np.arange(len(total_payments_weekly.index))

# Plotting for the first 6 gateways
plt.figure(figsize=(17.5, 7))
for i, gateway in enumerate(total_attempts[:6]):
    plt.bar(x + i * bar_width, 
            total_payments_weekly[gateway], 
            width=bar_width, label=gateway)

    # Add formatted values to the chart with slight offsets
    for idx, val in enumerate(total_payments_weekly[gateway]):
        formatted_val = '{:,.0f}'.format(val)  # Format value as comma-separated integer
        plt.text(x[idx] + i * bar_width, val, formatted_val, ha='center', va='bottom', fontsize=10, color='black', rotation=60)

plt.title('WoW Attempts by Gateway (First 6 Gateways)')
plt.xlabel('Week')
plt.ylabel('Total Attempts')
plt.legend()
plt.grid(False)

# Extracting only the date part from the 'week' column for x-axis labels
x_labels = [week.date() for week in total_payments_weekly.index]
plt.xticks(ticks=x + (len(total_attempts[:6]) / 2 - 0.5) * bar_width, labels=x_labels, rotation=60)
plt.tight_layout()
plt.savefig('wow_attempts_first_6.png')
plt.show()

# Plotting for the remaining gateways
plt.figure(figsize=(17.5, 7))
for i, gateway in enumerate(total_attempts[6:]):
    plt.bar(x + i * bar_width, 
            total_payments_weekly[gateway], 
            width=bar_width, label=gateway)

    # Add formatted values to the chart with slight offsets
    for idx, val in enumerate(total_payments_weekly[gateway]):
        formatted_val = '{:,.0f}'.format(val)  # Format value as comma-separated integer
        plt.text(x[idx] + i * bar_width, val, formatted_val, ha='center', va='bottom', fontsize=10, color='black', rotation=60)

plt.title('WoW Attempts by Gateway (Remaining Gateways)')
plt.xlabel('Week')
plt.ylabel('Total Attempts')
plt.legend()
plt.grid(False)

# Extracting only the date part from the 'week' column for x-axis labels
plt.xticks(ticks=x + (len(total_attempts[6:]) / 2 - 0.5) * bar_width, labels=x_labels, rotation=60)
plt.tight_layout()
plt.savefig('wow_attempts_remaining.png')
plt.show()

# COMMAND ----------

# Group by week and gateway and sum total payments
total_payments_weekly = wow_sr_pivot_token.groupby(['week', 'gateway'])['Total Payments'].sum().unstack()

# Fill missing values with zeros
total_payments_weekly.fillna(0, inplace=True)

# Sort gateways by total attempts
total_attempts = total_payments_weekly.sum().sort_values(ascending=False).index

# Define the width of each bar
bar_width = 0.1

# Calculate the x-coordinate for each bar
x = np.arange(len(total_payments_weekly.index))

# Plotting for the first 6 gateways
plt.figure(figsize=(17.5, 7))
for i, gateway in enumerate(total_attempts[:6]):
    plt.bar(x + i * bar_width, 
            total_payments_weekly[gateway], 
            width=bar_width, label=gateway)

    # Add formatted values to the chart with slight offsets
    for idx, val in enumerate(total_payments_weekly[gateway]):
        formatted_val = '{:,.0f}'.format(val)  # Format value as comma-separated integer
        plt.text(x[idx] + i * bar_width, val, formatted_val, ha='center', va='bottom', fontsize=10, color='black', rotation=60)

plt.title('WoW Tokenized Attempts by Gateway (First 6 Gateways)')
plt.xlabel('Week')
plt.ylabel('Total Attempts')
plt.legend()
plt.grid(False)

# Extracting only the date part from the 'week' column for x-axis labels
x_labels = [week.date() for week in total_payments_weekly.index]
plt.xticks(ticks=x + (len(total_attempts[:6]) / 2 - 0.5) * bar_width, labels=x_labels, rotation=60)
plt.tight_layout()
plt.savefig('wow_tokenized_attempts_excluding_razorpay_first_6.png')
plt.show()

# Plotting for the remaining gateways
plt.figure(figsize=(17.5, 7))
for i, gateway in enumerate(total_attempts[6:]):
    plt.bar(x + i * bar_width, 
            total_payments_weekly[gateway], 
            width=bar_width, label=gateway)

    # Add formatted values to the chart with slight offsets
    for idx, val in enumerate(total_payments_weekly[gateway]):
        formatted_val = '{:,.0f}'.format(val)  # Format value as comma-separated integer
        plt.text(x[idx] + i * bar_width, val, formatted_val, ha='center', va='bottom', fontsize=10, color='black', rotation=60)

plt.title('WoW Tokenized Attempts by Gateway (Remaining Gateways)')
plt.xlabel('Week')
plt.ylabel('Total Attempts')
plt.legend()
plt.grid(False)

# Extracting only the date part from the 'week' column for x-axis labels
plt.xticks(ticks=x + (len(total_attempts[6:]) / 2 - 0.5) * bar_width, labels=x_labels, rotation=60)
plt.tight_layout()
plt.savefig('wow_tokenized_attempts_excluding_razorpay_remaining.png')
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ##Top 10 merchants with significant dip in SR

# COMMAND ----------

# Convert golive_date and created_date columns to datetime
payments_pd["golive_date"] = pd.to_datetime(payments_pd["golive_date"])
payments_pd["created_date"] = pd.to_datetime(payments_pd["created_date"])

# Filter the DataFrame based on conditions
mx_filtered = payments_pd[
    (
        (payments_pd["created_date"] >= t2_start) &
        (payments_pd["created_date"] <= t1_end) &
        (payments_pd["gateway"] != "Razorpay")
        & (payments_pd["gateway"] != None)
        & (payments_pd["Error Source"] != 'customer')
        )]

# Create the pivot table with total column and row
mx_filtered_pivot = mx_filtered.pivot_table(
    index=["merchant_id","Name","gateway","method_advanced"],
    columns="Period",
    values=["Successful payments", "Total Payments"],
    aggfunc=sum
).fillna(0).astype(int)

mx_filtered_pivot.reindex(columns=sorted(mx_filtered_pivot.columns, reverse=True))

# Calculate success rates, avoiding division by zero
mx_filtered_pivot['T-2 SR'] = ((mx_filtered_pivot['Successful payments']['T-2'] / mx_filtered_pivot['Total Payments']['T-2']) * 100).round(2)
mx_filtered_pivot['T-1 SR'] = ((mx_filtered_pivot['Successful payments']['T-1'] / mx_filtered_pivot['Total Payments']['T-1']) * 100).round(2)
mx_filtered_pivot['Delta SR'] = (mx_filtered_pivot['T-1 SR'] - mx_filtered_pivot['T-2 SR']).round(2)
sr_mx_filtered_pivot = mx_filtered_pivot[(mx_filtered_pivot["Delta SR"] < -0.05)].sort_values(by= 'Delta SR', ascending=True)
sr_mx_filtered_pivot = mx_filtered_pivot[(mx_filtered_pivot["Delta SR"] < -5)]
sr_mx_filtered_pivot = sr_mx_filtered_pivot [sr_mx_filtered_pivot['Total Payments']['T-2'] >= 30]
sr_mx_filtered_pivot = sr_mx_filtered_pivot [sr_mx_filtered_pivot['Total Payments']['T-1'] >= 30]
sr_mx_filtered_pivot = sr_mx_filtered_pivot.sort_values(by="Delta SR", ascending=True)
sr_mx_filtered_pivot = sr_mx_filtered_pivot.head(10)
sr_mx_filtered_pivot

# COMMAND ----------

# MAGIC %md
# MAGIC ## Settlements

# COMMAND ----------

settlements = spark.sql(f"""
select 
merchant_id, coalesce(billing_label,name) name ,gateway, recon_status , rzp_entity_type, transaction_date, count(distinct payment_id) as total_payments
from analytics_selfserve.optimizer_settlement_single_recon a
inner join (select id, billing_label, name from realtime_hudi_api.merchants) b 
on a.merchant_id = b.id
where date_trunc('month', date(transaction_date)) >= date_add(month,-2,date_trunc('month',current_date))
group by 1,2,3,4,5,6 """)
recon_pd = settlements.toPandas()

# COMMAND ----------

recon_pd

# COMMAND ----------

# Convert transaction_date to datetime and extract month
recon_pd['transaction_date'] = pd.to_datetime(recon_pd['transaction_date'])
recon_pd['month'] = recon_pd['transaction_date'].dt.to_period('M')

# Group by month and gateway, then calculate aggregations
monthly_aggregate = recon_pd.groupby(['month', 'gateway']).apply(
    lambda x: pd.Series({
        'total_payments': x['total_payments'].sum(),
        'reconciled_payments': x.loc[x['recon_status'] == 'Reconciled', 'total_payments'].sum()
    })
).reset_index()

# Calculate settlement coverage percentage
monthly_aggregate['settlement_coverage'] = (monthly_aggregate['reconciled_payments'] / monthly_aggregate['total_payments']) * 100
monthly_aggregate

# COMMAND ----------

# Group by month and gateway, then calculate aggregations
monthly_aggregate = recon_pd.groupby(['month', 'gateway']).apply(
    lambda x: pd.Series({
        'total_payments': x['total_payments'].sum(),
        'reconciled_payments': x.loc[x['recon_status'] == 'Reconciled', 'total_payments'].sum()
    })
).reset_index()

# Calculate settlement coverage percentage
monthly_aggregate['settlement_coverage'] = ((monthly_aggregate['reconciled_payments'] / monthly_aggregate['total_payments']) * 100).round(2)

# Pivot the DataFrame to have gateway as columns and only settlement coverage
pivot_table = monthly_aggregate.pivot(index='month', columns='gateway', values='settlement_coverage')

# Rename columns to make it clear that these are settlement coverage values
pivot_table.columns = ['Settlement Coverage ' + str(col) for col in pivot_table.columns]
monthyly_coverage_settlement = pivot_table
monthyly_coverage_settlement = monthyly_coverage_settlement.reset_index()
monthyly_coverage_settlement

# COMMAND ----------

# # Pivot the DataFrame for settlement coverage
settlement_coverage_pivot = monthly_aggregate.pivot_table(index='month', columns='gateway', values='settlement_coverage')

# # Pivot the DataFrame for total payments
total_payments_pivot = monthly_aggregate.pivot_table(index='month', columns='gateway', values='total_payments')

# # Pivot the DataFrame for reconciled payments
# # Calculate settlement coverage percentage
monthly_aggregate['settlement_coverage'] = (monthly_aggregate['reconciled_payments'] / monthly_aggregate['total_payments']) * 100

# # Pivot the DataFrame to have gateway as columns and only settlement coverage
pivot_table = monthly_aggregate.pivot(index='month', columns='gateway', values='settlement_coverage')

monthly_settlement_aggregate = pivot_table 
monthly_settlement_aggregate_reset = monthly_settlement_aggregate.reset_index()
monthly_settlement_aggregate_reset = monthly_settlement_aggregate_reset.reset_index()
monthly_settlement_aggregate_reset

# COMMAND ----------

monthly_settlement_aggregate_reset.columns

# COMMAND ----------

nia_pd = recon_pd[(recon_pd["merchant_id"] == "If9Z0dDl6Vht65") &
                  (recon_pd["gateway"] == "billdesk_optimizer")]
nia_pd                  

# COMMAND ----------

from datetime import datetime, timedelta

# Convert transaction_date to datetime
recon_pd['transaction_date'] = pd.to_datetime(recon_pd['transaction_date'])
recon_pd

end_date_settlement = pd.Timestamp(datetime.now().date() - timedelta(days=1))  # Yesterday's date
start_date_settlement = pd.Timestamp(end_date_settlement - timedelta(days=30))  # 30 days before yesterday

# Ensure that both end_date and start_date are in the same format as transaction_date
recon_pd_pivot = recon_pd[(recon_pd['transaction_date'] >= start_date_settlement) & (recon_pd['transaction_date'] <= end_date_settlement)]

# Create pivot table
recon_pd_pivot = recon_pd_pivot.pivot_table(index='transaction_date', columns=['gateway', 'recon_status'], values='total_payments', aggfunc='sum', fill_value=0)

# Calculate the total for the 'Billdesk' gateway
recon_pd_pivot['Billdesk Total'] = recon_pd_pivot[('billdesk_optimizer', 'Reconciled')] + recon_pd_pivot[('billdesk_optimizer', 'Unreconciled')]
recon_pd_pivot['Cashfree Total'] = recon_pd_pivot[('cashfree', 'Reconciled')] + recon_pd_pivot[('cashfree', 'Unreconciled')]
#recon_pd_pivot['PayTM Total'] = recon_pd_pivot[('paytm', 'Reconciled')] + recon_pd_pivot[('paytm', 'Unreconciled')]
recon_pd_pivot['Payu Total'] = recon_pd_pivot[('payu', 'Reconciled')] + recon_pd_pivot[('payu', 'Unreconciled')]

recon_pd_pivot['Billdesk Coverage%'] = ((recon_pd_pivot[('billdesk_optimizer', 'Reconciled')] / recon_pd_pivot['Billdesk Total']) * 100).round(2)
recon_pd_pivot['Cashfree Coverage%'] = ((recon_pd_pivot[('cashfree', 'Reconciled')] / recon_pd_pivot['Cashfree Total']) * 100).round(2)
#recon_pd_pivot['PayTM Coverage%'] = ((recon_pd_pivot[('paytm', 'Reconciled')] / recon_pd_pivot['PayTM Total']) * 100).round(2)
recon_pd_pivot['Payu Coverage%'] = ((recon_pd_pivot[('payu', 'Reconciled')] / recon_pd_pivot['Payu Total']) * 100).round(2)

# Display the pivot table
recon_pd_pivot

# COMMAND ----------

recon_pd_pivot_reset = recon_pd_pivot.reset_index()
recon_pd_pivot_reset

# COMMAND ----------

from datetime import datetime, timedelta

# Convert transaction_date to datetime
nia_pd['transaction_date'] = pd.to_datetime(nia_pd['transaction_date'])
nia_pd

end_date_settlement = pd.Timestamp(datetime.now().date() - timedelta(days=1))  # Yesterday's date
start_date_settlement = pd.Timestamp(end_date_settlement - timedelta(days=30))  # 30 days before yesterday

# Ensure that both end_date and start_date are in the same format as transaction_date
nia_pd = nia_pd[(nia_pd['transaction_date'] >= start_date_settlement) & (nia_pd['transaction_date'] <= end_date_settlement)]

# Create pivot table
nia_pd_pivot = nia_pd.pivot_table(index='transaction_date', columns=['gateway', 'recon_status'], values='total_payments', aggfunc='sum', fill_value=0)

# Calculate the total for the 'Billdesk' gateway
nia_pd_pivot['Billdesk Total'] = nia_pd_pivot[('billdesk_optimizer', 'Reconciled')] + nia_pd_pivot[('billdesk_optimizer', 'Unreconciled')]

nia_pd_pivot['Billdesk Coverage%'] = ((nia_pd_pivot[('billdesk_optimizer', 'Reconciled')] / nia_pd_pivot['Billdesk Total']) * 100).round(2)

# Display the pivot table
nia_pd_pivot

# COMMAND ----------

nia_pd_pivot_reset = nia_pd_pivot.reset_index()
nia_pd_pivot_reset

# COMMAND ----------

# Rename columns
recon_pd_pivot_reset.rename(columns={'transaction_date': 'Transaction Date'}, inplace=True)

# Select desired columns
settlement_selected_2 = ['Transaction Date', 'Billdesk Coverage%']
nia_pd_pivot_reset = recon_pd_pivot_reset[settlement_selected_2]
nia_pd_pivot_reset

# COMMAND ----------

recon_pd_pivot_reset.columns

# COMMAND ----------



# COMMAND ----------

# Rename columns
recon_pd_pivot_reset.columns = ['Transaction Date', 'Billdesk Reconciled', 'Billdesk Unreconciled','Cashfree Reconciled', 'Cashfree Unreconciled','PayTM Unreconciloed','Payu Reconciled', 'Payu Unreconciled','Billdesk Total', 'Cashfree Total', 'Payu Total','Billdesk Coverage%', 'Cashfree Coverage%', 'Payu Coverage%']
'''recon_pd_pivot_reset.columns = ['Transaction Date', 'Billdesk Reconciled', 'Billdesk Unreconciled',
               'Cashfree Reconciled', 'Cashfree Unreconciled',
               'PayTM Reconciled', 'PayTM Unreconciled',
               'Payu Reconciled', 'Payu Unreconciled',
               'Billdesk Total', 'Cashfree Total', 'PayTM Total', 'Payu Total',
               'Billdesk Coverage%', 'Cashfree Coverage%', 'PayTM Coverage%', 'Payu Coverage%']'''
settlement_selected_1 = ['Transaction Date', 'Billdesk Coverage%', 'Cashfree Coverage%', 'Payu Coverage%']
recon_pd_pivot_reset = recon_pd_pivot_reset[settlement_selected_1]

# COMMAND ----------

recon_pd_pivot_reset

# COMMAND ----------

# Assuming nia_pd_pivot_reset is defined

# Convert 'Transaction Date' column to datetime
nia_pd_pivot_reset['Transaction Date'] = pd.to_datetime(nia_pd_pivot_reset['Transaction Date'])

# Plot the line plot
plt.figure(figsize=(17.5, 7))
for column in nia_pd_pivot_reset.columns[1:]:
    plt.plot(nia_pd_pivot_reset['Transaction Date'], nia_pd_pivot_reset[column], linestyle='dashed', label=column[0])

    # Add values to the plot
    for i, value in enumerate(nia_pd_pivot_reset[column]):
        plt.text(nia_pd_pivot_reset['Transaction Date'][i], value, str(value), ha='center', va='bottom')

plt.title('Settlement Coverage in last 30 days')
plt.xlabel('Transaction Date')
plt.ylabel('NIA Settlement Percentage')
plt.legend()
plt.grid(False)
plt.xticks(rotation=45)  # Rotate x-axis labels by 45 degrees
plt.tight_layout()

# Set x-axis ticks to include all dates
plt.xticks(nia_pd_pivot_reset['Transaction Date'])
plt.savefig('nia_coverage.png')

# Show the plot
plt.show()

# COMMAND ----------

recon_pd_pivot_reset['Transaction Date'] = pd.to_datetime(recon_pd_pivot_reset['Transaction Date'])

# Get the list of gateways
gateways = recon_pd_pivot_reset.columns[1:]

# Create subplots
fig, axes = plt.subplots(len(gateways), 1, figsize=(17.5, 4 * len(gateways)), sharex=True)

# Define a colormap
colors = plt.cm.tab10(np.linspace(0, 1, len(gateways)))

# Plot each gateway on a separate subplot with a different color
for i, (gateway, color) in enumerate(zip(gateways, colors)):
    axes[i].plot(recon_pd_pivot_reset['Transaction Date'], recon_pd_pivot_reset[gateway], linestyle='dashed', color=color, label=gateway)

    # Add values to the plot
    for j, value in enumerate(recon_pd_pivot_reset[gateway]):
        axes[i].text(recon_pd_pivot_reset['Transaction Date'][j], value, str(value), ha='center', va='bottom')

    axes[i].set_title(f'{gateway}')
    axes[i].set_ylabel('Coverage Percentage')
    axes[i].legend()
    axes[i].grid(False)
    axes[i].tick_params(axis='x', rotation=45)  # Rotate x-axis labels by 45 degrees

# Set x-axis ticks to include all dates for the last subplot
axes[-1].set_xlabel('Transaction Date')
plt.tight_layout()

plt.savefig('coverage_percentage_over_time_subplots.png')

# Show the plot
plt.show()


# COMMAND ----------

low_coverage = recon_pd_pivot_reset[(recon_pd_pivot_reset['Billdesk Coverage%'] <=99.00)
                                             |(recon_pd_pivot_reset['Cashfree Coverage%'] <=99.00)
                                             |(recon_pd_pivot_reset['Payu Coverage%'] <=99.00)]
'''low_coverage = recon_pd_pivot_reset[(recon_pd_pivot_reset['Billdesk Coverage%'] <=99.00)
                                             |(recon_pd_pivot_reset['Cashfree Coverage%'] <=99.00)
                                             |(recon_pd_pivot_reset['Payu Coverage%'] <=99.00)
                                             |(recon_pd_pivot_reset['PayTM Coverage%'] <=99.00)]'''
low_coverage['Transaction Date'] = low_coverage['Transaction Date'].dt.date
low_coverage

# COMMAND ----------

# MAGIC %md
# MAGIC ##Refunds

# COMMAND ----------

refunds_sr_df = spark.sql("""
with optimizer_payments as (select payment_id from aggregate_pa.optimizer_terminal_payments where producer_created_date >= cast(CURRENT_DATE + interval '-6' month as varchar(20))),

refund as (select payment_id, merchant_id, id, created_date, gateway, status, terminal_id, created_at
           from realtime_scrooge_live.refunds
           where status in ( 'file_init','processed','init','on_hold')
           and created_date >= date_add(month, -2,date_trunc('month',current_date))
           and payment_id in (select payment_id from optimizer_payments)
           and merchant_id in (select distinct merchant_id from analytics_selfserve.optimizer_golive_date_base where golive_date is not null)
           and method != 'transfer'
           and payment_gateway_captured = 1),
           
terminals as (select terminal_id, procurer from realtime_terminalslive.terminals where deleted_at is null),           

final as
(select refund.*, terminals.procurer from refund inner join terminals on refund.terminal_id = terminals.terminal_id),

final_base as
(select final.merchant_id, id, final.payment_id , created_date, status,
case when procurer = 'razorpay' then 'razorpay'
        when procurer = 'merchant' and merchant_id in ('HsuMMq5AQBGCIW',
        'Hsup9WpX7ImZKw',
        'HsvZ5lz0dsDEJb',
        'HsvpIn3D8Cc6Zc',
        'GWjTMDeGwcVrr1',
        'HEaGxjXCXZUwSY',
        'GV7M7ZXLNzez0b',
        'IyYh0RU78PkNts',
        'HAdYq8orPiRhEw',
        'HThMRY5d6U3ald',
        'GA4CuzvbXn2kGF',
        'GA6Sj3FCr1X48P',
        'GA6zg8KmCvShIt',
        'GA7CHjv52jbuI6',
        'GA7ek79g40DCy9',
        'GA7Us9F5x4zS9E',
        'GA7ymrtIHoBmJ6',
        'GA8BEH81JBq9eA',
        'GA8Ml3740cOIQg',
        'H9ObHPX7JVb3ET',
        'HZAwyvY5FKzWkP',
        'IPl7WsunideZRw') and gateway in ('cybersource','hdfc','axis_migs') then 'razorpay'
        when procurer = 'merchant'  then gateway end gateway from final)

   select 
   merchant_id,
   created_date,
   gateway, 
   status,
   id as refund_id
   from final_base where gateway != 'razorpay' """)

refunds_sr_df = refunds_sr_df[(refunds_sr_df['created_date'] >= refund_start_date) & (refunds_sr_df['created_date'] <= refund_end_date)]
refunds_sr_pd = refunds_sr_df.toPandas()
refunds_sr_pd.head(5)

# COMMAND ----------

pending_refunds_df = refunds_sr_df[
    (refunds_sr_df['created_date'] >= refund_start_date) &
    (refunds_sr_df['created_date'] <= refund_end_date) & 
    ((refunds_sr_df['gateway'] == 'paytm') | (refunds_sr_df['gateway'] == 'payu')) &
    ((refunds_sr_df['status'] == 'file_init') | (refunds_sr_df['status'] == 'init'))]
pending_refunds_pd = pending_refunds_df.toPandas()  
pending_refunds_pd

# COMMAND ----------

pending_reason = spark.sql("""
                          select * from
                          (select 
                          merchant_id, refund_id, action, gateway_response, dense_rank() over (partition by refund_id, action order by attempts.created_at desc) as rank
                          from realtime_scrooge_live.attempts 
                          where action = 'refund'
                          and created_date >= date_add(month, -2,date_trunc('month',current_date)))
                          where rank = 1 """)
pending_reason                          

# COMMAND ----------

merchant_name = spark.sql("""
                          select id as mx_mid, billing_label, name from realtime_hudi_api.merchants
                          where id in (select distinct merchant_id from analytics_selfserve.optimizer_golive_date_base where golive_date is not null)
                           """)
merchant_name                          

# COMMAND ----------

pending_refunds_df.createOrReplaceTempView('temp1')
pending_reason.createOrReplaceTempView('temp2')
merchant_name.createOrReplaceTempView('temp3')

# COMMAND ----------

pending_refuds_reason_df = spark.sql("""
          select mid,coalesce(name,billing_label) as name ,gateway, status, 
          case 
          when gateway = 'paytm' then
          coalesce(get_json_object(gateway_response, '$.body.resultInfo.resultCode'),
          get_json_object(gateway_response, '$.resultInfo.resultCode')) 
          when gateway = 'payu' then get_json_object(gateway_response, '$.error_code') end `error code`,

          case 
          when gateway = 'paytm' then
          coalesce(get_json_object(gateway_response, '$.body.resultInfo.resultMsg'),
          get_json_object(gateway_response, '$.resultInfo.resultMsg')) 
          when gateway = 'payu' then get_json_object(gateway_response, '$.msg') end `error message`,

          count(distinct rid) as `total pending refunds`
          from
          (select a.merchant_id as mid, a.refund_id as rid, 
          c.name, c.billing_label, * from temp1 a 
          inner join temp2 b on a.refund_id = b.refund_id
          inner join temp3 c on a.merchant_id = c.mx_mid
          )
          group by all
           """)
pending_refuds_reason_pd = pending_refuds_reason_df.toPandas() 
pending_refuds_reason_pd

# COMMAND ----------

refunds_sr_df = refunds_sr_df[(refunds_sr_df['created_date'] >= refund_start_date) & (refunds_sr_df['created_date'] <= refund_end_date)]
refunds_sr_pd = refunds_sr_df.toPandas()
refunds_sr_pd

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

a = refunds_sr_pd[(refunds_sr_pd["gateway"] == "hdfc") & (refunds_sr_pd["status"] == "file_init")]
a = a['refund_id'].head(100).tolist()
a

# COMMAND ----------

overall_SR = (
    refunds_sr_pd.pivot_table(
        columns='status',
        values='refund_id',
        aggfunc=pd.Series.nunique).round(0).sort_values(by=['processed'], ascending=False)).astype(int)

overall_SR

if 'init' in overall_SR.columns and 'on_hold' in overall_SR.columns:
    overall_SR['Total Refunds'] = overall_SR[['file_init', 'init', 'processed', 'on_hold']].sum(axis=1).astype(int)
    overall_SR['processed %'] = (overall_SR['processed'] / overall_SR['Total Refunds'] * 100).round(2)
    overall_SR['init %'] = (overall_SR['init'] / overall_SR['Total Refunds'] * 100).round(2)
    overall_SR['file_init %'] = (overall_SR['file_init'] / overall_SR['Total Refunds'] * 100).round(2)
    overall_SR['on_hold %'] = (overall_SR['on_hold'] / overall_SR['Total Refunds'] * 100).round(2)
elif 'init' in overall_SR.columns:
    overall_SR['Total Refunds'] = overall_SR[['file_init', 'init', 'processed']].sum(axis=1).astype(int)
    overall_SR['processed %'] = (overall_SR['processed'] / overall_SR['Total Refunds'] * 100).round(2)
    overall_SR['init %'] = (overall_SR['init'] / overall_SR['Total Refunds'] * 100).round(2)
    overall_SR['file_init %'] = (overall_SR['file_init'] / overall_SR['Total Refunds'] * 100).round(2)
    overall_SR['on_hold'] = 0  # Setting 'on_hold %' to 0 as 'on_hold' does not exist
    overall_SR['on_hold %'] = 0  # Setting 'on_hold %' to 0 as 'on_hold' does not exist
elif 'on_hold' in overall_SR.columns:
    overall_SR['Total Refunds'] = overall_SR[['file_init', 'processed', 'on_hold']].sum(axis=1).astype(int)
    overall_SR['processed %'] = (overall_SR['processed'] / overall_SR['Total Refunds'] * 100).round(2)
    overall_SR['file_init %'] = (overall_SR['file_init'] / overall_SR['Total Refunds'] * 100).round(2)
    overall_SR['init'] = 0  # Setting 'on_hold %' to 0 as 'on_hold' does not exist
    overall_SR['init %'] = 0  # Setting 'init %' to 0 as 'init' does not exist
    overall_SR['on_hold %'] = (overall_SR['on_hold'] / overall_SR['Total Refunds'] * 100).round(2)
else:
    overall_SR['Total Refunds'] = overall_SR[['file_init', 'processed']].sum(axis=1).astype(int)
    overall_SR['processed %'] = (overall_SR['processed'] / overall_SR['Total Refunds'] * 100).round(2)
    overall_SR['file_init %'] = (overall_SR['file_init'] / overall_SR['Total Refunds'] * 100).round(2)
    overall_SR['init'] = 0
    overall_SR['init %'] = 0  # Setting 'init %' and 'on_hold %' to 0 as 'init' and 'on_hold' do not exist
    overall_SR['on_hold'] = 0
    overall_SR['on_hold %'] = 0

overall_SR

# COMMAND ----------

# Create a DataFrame without NaN or infinite values
refunds_sr_final_pd = refunds_sr_pd.pivot_table(
    index='gateway',
    columns='status',
    values='refund_id',
    aggfunc=pd.Series.nunique  # Corrected typo
).fillna(0).round(0).sort_values(by='processed', ascending=False)

if 'init' in refunds_sr_final_pd.columns and 'on_hold' in refunds_sr_final_pd.columns:
    refunds_sr_final_pd['Total Refunds'] = refunds_sr_final_pd[['file_init', 'init', 'processed', 'on_hold']].sum(axis=1).astype(int)
    refunds_sr_final_pd['processed %'] = (refunds_sr_final_pd['processed'] / refunds_sr_final_pd['Total Refunds'] * 100).round(2)
    refunds_sr_final_pd['init %'] = (refunds_sr_final_pd['init'] / refunds_sr_final_pd['Total Refunds'] * 100).round(2)
    refunds_sr_final_pd['file_init %'] = (refunds_sr_final_pd['file_init'] / refunds_sr_final_pd['Total Refunds'] * 100).round(2)
    refunds_sr_final_pd['on_hold %'] = (refunds_sr_final_pd['on_hold'] / refunds_sr_final_pd['Total Refunds'] * 100).round(2)
elif 'init' in refunds_sr_final_pd.columns:
    refunds_sr_final_pd['Total Refunds'] = refunds_sr_final_pd[['file_init', 'init', 'processed']].sum(axis=1).astype(int)
    refunds_sr_final_pd['processed %'] = (refunds_sr_final_pd['processed'] / refunds_sr_final_pd['Total Refunds'] * 100).round(2)
    refunds_sr_final_pd['init %'] = (refunds_sr_final_pd['init'] / refunds_sr_final_pd['Total Refunds'] * 100).round(2)
    refunds_sr_final_pd['file_init %'] = (refunds_sr_final_pd['file_init'] / refunds_sr_final_pd['Total Refunds'] * 100).round(2)
    refunds_sr_final_pd['on_hold %'] = 0  # Setting 'on_hold %' to 0 as 'on_hold' does not exist
    refunds_sr_final_pd['on_hold'] = 0  # Setting 'on_hold %' to 0 as 'on_hold' does not exist

elif 'on_hold' in refunds_sr_final_pd.columns:
    refunds_sr_final_pd['Total Refunds'] = refunds_sr_final_pd[['file_init', 'processed', 'on_hold']].sum(axis=1).astype(int)
    refunds_sr_final_pd['processed %'] = (refunds_sr_final_pd['processed'] / refunds_sr_final_pd['Total Refunds'] * 100).round(2)
    refunds_sr_final_pd['file_init %'] = (refunds_sr_final_pd['file_init'] / refunds_sr_final_pd['Total Refunds'] * 100).round(2)
    refunds_sr_final_pd['init'] = 0  # Setting 'init %' to 0 as 'init' does not exist
    refunds_sr_final_pd['init %'] = 0  # Setting 'init %' to 0 as 'init' does not exist
    refunds_sr_final_pd['on_hold %'] = (refunds_sr_final_pd['on_hold'] / refunds_sr_final_pd['Total Refunds'] * 100).round(2)
else:
    refunds_sr_final_pd['Total Refunds'] = refunds_sr_final_pd[['file_init', 'processed']].sum(axis=1).astype(int)
    refunds_sr_final_pd['processed %'] = (refunds_sr_final_pd['processed'] / refunds_sr_final_pd['Total Refunds'] * 100).round(2)
    refunds_sr_final_pd['file_init %'] = (refunds_sr_final_pd['file_init'] / refunds_sr_final_pd['Total Refunds'] * 100).round(2)
    refunds_sr_final_pd['init %'] = 0  # Setting 'init %' and 'on_hold %' to 0 as 'init' and 'on_hold' do not exist
    refunds_sr_final_pd['init'] = 0  
    refunds_sr_final_pd['on_hold'] = 0
    refunds_sr_final_pd['on_hold %'] = 0

# Ensure other columns are casted to integer type
non_percentage_cols = [col for col in refunds_sr_final_pd.columns if '%' not in col]
refunds_sr_final_pd[non_percentage_cols] = refunds_sr_final_pd[non_percentage_cols].astype(int)

# Replace any remaining non-finite values with 0
refunds_sr_final_pd.replace([np.nan, np.inf, -np.inf], 0, inplace=True)

refunds_sr_final_pd


# COMMAND ----------

pending_refuds_paytm_file_init_pd = pending_refuds_reason_pd[(pending_refuds_reason_pd['gateway'] == 'paytm') & (pending_refuds_reason_pd['status'] == 'file_init')]

pending_refuds_payu_file_init_pd = pending_refuds_reason_pd[(pending_refuds_reason_pd['gateway'] == 'payu') & (pending_refuds_reason_pd['status'] == 'file_init')]

pending_refuds_paytm_file_init_pd.sort_values(by=['total pending refunds'], ascending=False)
pending_refuds_payu_file_init_pd.sort_values(by=['total pending refunds'], ascending=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #Resetting Everything

# COMMAND ----------

#Refunds SR
overall_SR_reset = overall_SR.reset_index()
refunds_sr_final_pd_reset = refunds_sr_final_pd.reset_index()
pending_refuds_paytm_file_init_pd_reset = pending_refuds_paytm_file_init_pd.reset_index()
pending_refuds_payu_file_init_pd_reset = pending_refuds_payu_file_init_pd.reset_index()

# COMMAND ----------

SR_negative_reset_df_2_pivot_mx

# COMMAND ----------

#SR
overall_SR_df_reset = overall_SR_df.reset_index()
SR_reset = SR.reset_index()
SR_positive_reset = SR_positive.reset_index()
SR_negative_reset = SR_negative.reset_index()
SR_negative_reset_df_1_pivot_reset = SR_negative_reset_df_1_pivot.reset_index()
SR_negative_reset_df_1_mx_pivot_reset = SR_negative_reset_df_1_mx_pivot.reset_index()
SR_negative_reset_df_2_pivot_reset = SR_negative_reset_df_2_pivot.reset_index()
SR_negative_reset_df_2_mx_pivot_reset = SR_negative_reset_df_2_pivot_mx.reset_index()

# COMMAND ----------

# GMV
wow_gmv_pivot_reset = wow_gmv_pivot.reset_index()
wow_gmv_top_promoters_pivot_reset = wow_gmv_top_promoters_pivot.reset_index()
wow_gmv_detractors_reset = wow_gmv_detractors_pivot.reset_index()
mtd_pivot_reset = mtd_pivot.reset_index()
top_5_promoters_mtd_reset = top_5_promoters_mtd.reset_index()
top_5_detractors_mtd_reset = top_5_detractors_mtd.reset_index()
mom_pivot_reset

# COMMAND ----------

##MTU 
mtu_pivot_reset = mtu_pivot.reset_index()
mtu_pivot_transacting_reset = mtu_pivot_transacting.reset_index()
mtu_pivot_not_transacting_reset = mtu_pivot_not_transacting.reset_index()
df_drop_merchant_id = mtu_risk.drop(columns=['merchant_id'])
mtu_risk_reset = df_drop_merchant_id.reset_index()

# COMMAND ----------

#Late Auth

overall_late_auth_reset = overall_late_auth.reset_index()
gateway_late_auth_reset = gateway_late_auth_all.reset_index()
gateway_for_which_late_auth_increased_reset = gateway_for_which_late_auth_increased.reset_index()
gateway_drill_1_reset = gateway_drill_1.reset_index()
gateway_drill_2_reset = gateway_drill_2.reset_index()
merchant_drill_1_reset = merchant_drill_1.reset_index()
merchant_drill_2_reset = merchant_drill_2.reset_index()
top_5_merchants_late_auth_reset = top_5_merchants_late_auth.reset_index()
upi_gateway_late_auth_reset = upi_gateway_late_auth.reset_index()
upi_gateway_late_auth_filtered_check_reset = upi_gateway_late_auth_filtered_check.reset_index()
upi_gateway_late_auth_drill_df_reset = upi_gateway_late_auth_drill_df.reset_index()
df_rank_1_reset = df_rank_1.reset_index()
upi_merchants_df_check_1_reset = upi_merchants_df_check_1.reset_index()
upi_gateway_late_auth_drill_mx_reset = merged_df.reset_index()
upi_gateway_late_auth_with_percentage_reset = upi_gateway_late_auth_with_percentage.reset_index()


# COMMAND ----------

# merged_df

# COMMAND ----------

# upi_gateway_late_auth_with_percentage_reset

# COMMAND ----------

# upi_gateway_late_auth_drill_df_reset

# COMMAND ----------

# MAGIC %md
# MAGIC #Renaming The Columns & Select the columns

# COMMAND ----------

# MAGIC %md
# MAGIC ###Refunds

# COMMAND ----------

def rename_columns_in_dataframes(dataframes, column_mapping):
    for df_name, df in dataframes.items():
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df.rename(columns={old_name: new_name}, inplace=True)
            else:
                print(f"Column '{old_name}' not found in DataFrame '{df_name}'.")
    return dataframes

# Example of column mapping
column_mapping = {
    'index': 'Index',
    'mid': 'MID',
    'name': 'Name',
    'gateway': 'Gateway',
    'status': 'Status',
    'error code': 'Error Code',
    'error message': 'Error Message',
    'total pending refunds': 'Total Pending Refunds',
    'file_init': 'File Init',
    'init': 'Init',
    'processed': 'Processed',
    'Total Refunds': 'Total Refunds',
    'processed %': 'Processed %',
    'init %': 'Init %',
    'file_init %': 'File Init %',
    'on_hold' : 'On hold',
    'on_hold %' : 'On hold %'
}

# Sample DataFrames
dataframes = {
    'overall_SR_reset': overall_SR_reset,
    'refunds_sr_final_pd_reset': refunds_sr_final_pd_reset,
    'pending_refuds_paytm_file_init_pd_reset': pending_refuds_paytm_file_init_pd_reset,
    'pending_refuds_paytm_file_init_pd_reset': pending_refuds_paytm_file_init_pd_reset
}

# Call the function to rename columns in all DataFrames
updated_dataframes = rename_columns_in_dataframes(dataframes, column_mapping)

# COMMAND ----------

overall_SR_reset

# COMMAND ----------

columns_to_exclude = ['Index', 'status']
overall_SR_reset = overall_SR_reset.reset_index(drop=True)[[col for col in overall_SR_reset.columns if col not in columns_to_exclude]]

new_column_order = ['Total Refunds', 'Processed', 'Processed %','File Init', 'File Init %', 'Init', 'Init %', 'On hold', 'On hold %']
overall_SR_reset = overall_SR_reset[new_column_order]

columns_to_exclude = ['Index', 'status']
refunds_sr_final_pd_reset = refunds_sr_final_pd_reset.reset_index(drop=True)[[col for col in refunds_sr_final_pd_reset.columns if col not in columns_to_exclude]]

new_column_order = ['Gateway','Total Refunds', 'Processed', 'Processed %','File Init', 'File Init %', 'Init', 'Init %', 'On hold', 'On hold %']
refunds_sr_final_pd_reset = refunds_sr_final_pd_reset[new_column_order]

columns_to_exclude = ['Index', 'status']
pending_refuds_paytm_file_init_pd_reset = pending_refuds_paytm_file_init_pd_reset.reset_index(drop=True)[[col for col in pending_refuds_paytm_file_init_pd_reset.columns if col not in columns_to_exclude]]

new_column_order = ['MID','Name', 'Gateway', 'Status','Error Code', 'Error Message', 'Total Pending Refunds']
pending_refuds_paytm_file_init_pd_reset = pending_refuds_paytm_file_init_pd_reset[new_column_order]

columns_to_exclude = ['Index', 'status']
pending_refuds_paytm_file_init_pd_reset = pending_refuds_paytm_file_init_pd_reset.reset_index(drop=True)[[col for col in pending_refuds_paytm_file_init_pd_reset.columns if col not in columns_to_exclude]]

new_column_order = ['MID','Name', 'Gateway', 'Status','Error Code', 'Error Message', 'Total Pending Refunds']
pending_refuds_paytm_file_init_pd_reset = pending_refuds_paytm_file_init_pd_reset[new_column_order]

pending_refuds_paytm_file_init_pd_reset

# COMMAND ----------

columns_to_exclude = ['Index', 'status']
overall_SR_reset = overall_SR_reset[[col for col in overall_SR_reset.columns if col not in columns_to_exclude]]

new_column_order = [ 'Total Refunds', 'Processed', 'Processed %','File Init',  'File Init %', 'Init',  'Init %', 'On hold', 'On hold %']
overall_SR_reset = overall_SR_reset[new_column_order]

overall_SR_reset

columns_to_exclude_2 = ['Index', 'status']
refunds_sr_final_pd_reset = refunds_sr_final_pd_reset[[col for col in refunds_sr_final_pd_reset.columns if col not in columns_to_exclude_2]]

new_column_order_2 = ['Gateway','Total Refunds', 'Processed', 'Processed %','File Init',  'File Init %', 'Init',  'Init %', 'On hold', 'On hold %']
refunds_sr_final_pd_reset = refunds_sr_final_pd_reset[new_column_order_2]
refunds_sr_final_pd_reset

columns_to_exclude_3 = ['Index', 'status']
pending_refuds_paytm_file_init_pd_reset = pending_refuds_paytm_file_init_pd_reset[[col for col in pending_refuds_paytm_file_init_pd_reset.columns if col not in columns_to_exclude_3]]

new_column_order_3 = ['MID','Name', 'Gateway', 'Status','Error Code',  'Error Message', 'Total Pending Refunds']
pending_refuds_paytm_file_init_pd_reset = pending_refuds_paytm_file_init_pd_reset[new_column_order_3]

columns_to_exclude_4 = ['Index', 'status']
pending_refuds_paytm_file_init_pd_reset = pending_refuds_paytm_file_init_pd_reset[[col for col in pending_refuds_paytm_file_init_pd_reset.columns if col not in columns_to_exclude_4]]

new_column_order_4 = ['MID','Name', 'Gateway', 'Status','Error Code',  'Error Message', 'Total Pending Refunds']
pending_refuds_paytm_file_init_pd_reset = pending_refuds_paytm_file_init_pd_reset[new_column_order_4]

# COMMAND ----------

overall_SR_reset

# COMMAND ----------

# MAGIC %md
# MAGIC ### GMV

# COMMAND ----------

wow_gmv_pivot_reset_reset = wow_gmv_pivot_reset.reset_index()

# Select specific columns
selected_columns = [
    # 'index',
    'T-2 GMV in Cr',
    'T-1 GMV in Cr',
    'Delta Cr']

wow_gmv_pivot_reset_selected_df = wow_gmv_pivot_reset[selected_columns]

wow_gmv_top_promoters_pivot_reset_reset = wow_gmv_top_promoters_pivot_reset.reset_index(drop=True)

# Select specific columns
selected_columns_1 = [
    'Name',
    'T-2 GMV in Cr',
    'T-1 GMV in Cr',
    'Delta Cr'
]

wow_gmv_top_promoters_pivot_reset_selected_df = wow_gmv_top_promoters_pivot_reset[selected_columns_1]

wow_gmv_top_promoters_pivot_reset.index.name = None

# Select specific columns
selected_columns_3 = [
    'Name',
    'T-2 GMV in Cr',
    'T-1 GMV in Cr',
    'Delta Cr'
]
wow_gmv_top_promoters_pivot_reset_selected_df = wow_gmv_top_promoters_pivot_reset[selected_columns_3]

wow_gmv_detractors_reset.index.name = ''

# Select specific columns
selected_columns_4 = [
    'Name',
    'T-2 GMV in Cr',
    'T-1 GMV in Cr',
    'Delta Cr'
]

wow_gmv_detractors_reset_selected_df = wow_gmv_detractors_reset[selected_columns_4]

mtd_pivot_reset 
top_5_promoters_mtd_reset
top_5_detractors_mtd_reset
mom_pivot_reset

# COMMAND ----------

# MAGIC %md
# MAGIC ###SR

# COMMAND ----------

SR_reset.columns

# COMMAND ----------

# # Define the column mapping for renaming
# column_mapping = {
#     'Gateway': 'Gateway',
#     'Method Advanced': 'Method Advanced',
#     'Total Payments T-2': 'Total Payments T-2',
#     'Total Payments T-1': 'Total Payments T-1',
#     'Successful Payments T-2': 'Successful Payments T-2',
#     'Successful Payments T-1': 'Successful Payments T-1',
#     'T-2 SR': 'T-2 SR',
#     'T-1 SR': 'T-1 SR',
#     'Delta SR': 'Delta SR',
#     'Delta Attempts % Change': 'Delta Attempts % Change'
# }

# # Check if the DataFrame is not empty
# if not SR_negative_reset_df_2_pivot_reset.empty:
#     # Get the available columns in the DataFrame
#     available_columns = SR_negative_reset_df_2_pivot_reset.columns
    
#     # Iterate over the available columns and rename them if they exist in the column_mapping
#     for old_col_name, new_col_name in column_mapping.items():
#         if old_col_name in available_columns:
#             print(f"Renaming column '{old_col_name}' to '{new_col_name}'")
#             SR_negative_reset_df_2_pivot_reset.rename(columns={old_col_name: new_col_name}, inplace=True)
#         else:
#             print(f"Column '{old_col_name}' not found in DataFrame")
# else:
#     print("DataFrame SR_negative_reset_df_2_pivot_reset is empty.")

# if SR_negative_reset_df_2_pivot_reset.empty:
#     SR_negative_reset_df_2_pivot_reset
# else:
#     # Define the new column names
#     new_column_names = [
#     'Gateway',
#     'Method Advanced',
#     'Total Payments T-2',
#     'Total Payments T-1',
#     'Successful Payments T-2',
#     'Successful Payments T-1',
#     'T-2 SR',
#     'T-1 SR',
#     'Delta SR',
#     'Delta Attempts % Change'    ]

# # Rename the columns
#     SR_negative_reset_df_2_mx_pivot_reset.columns = new_column_names
# SR_negative_reset_df_2_mx_pivot_reset            

# COMMAND ----------

# Fetch column names of the DataFrame
# columns = overall_SR_df_reset.columns

# # Check if the DataFrame is empty
# if overall_SR_df_reset.empty:
#     selected_df = overall_SR_df_reset
# else:
#     # Define the selected columns
#     selected_columns = [
#         ('Total Payments', 'T-1'),
#         ('Total Payments', 'T-2'),
#         ('T-2 SR%', ''),
#         ('T-1 SR%', ''),
#         ('Delta SR', '')
#     ]

#     try:
#         # Attempt to select the specified columns
#         selected_df = overall_SR_df_reset[selected_columns]
#         selected_df
#     except KeyError as e:
#         # Handle KeyError exception
#         print("Error:", e)

# # Fetch column names of the DataFrame
# columns = SR_reset.columns

# # Check if the DataFrame is empty
# if SR_reset.empty:
#     selected_df_1 = SR_reset
# else:
#     # Define the selected columns
#     selected_columns = [
#         ('gateway', ''),
#         ('Total Payments', 'T-2'),
#         ('Total Payments', 'T-1'),
#         ('T-2 SR', ''),
#         ('T-1 SR', ''),
#         ('Delta SR', ''),
#         ('Delta Attempts % Change', '')
#     ]

#     try:
#         # Attempt to select the specified columns
#         selected_df_1 = SR_reset[selected_columns]
#         selected_df_1
#     except KeyError as e:
#         # Handle KeyError exception
#         print("Error:", e)


# # Fetch column names of the DataFrame
# columns = sr_mx_filtered_pivot_reset.columns

# # Check if the DataFrame is empty
# if sr_mx_filtered_pivot_reset.empty:
#     selected_df_3 = sr_mx_filtered_pivot_reset
# else:
#     # Define the selected columns
#     selected_columns = [
#         ('merchant_id', ''),
#         ('Name', ''),
#         ('gateway', ''),
#         ('method_advanced', ''),
#         ('Total Payments', 'T-2'),
#         ('Total Payments', 'T-1'),
#         ('T-2 SR', ''),
#         ('T-1 SR', ''),
#         ('Delta SR', '')
#     ]

#     try:
#         # Attempt to select the specified columns
#         selected_df_3 = sr_mx_filtered_pivot_reset[selected_columns]
#         selected_df_3
#     except KeyError as e:
#         # Handle KeyError exception
#         print("Error:", e)

# # Fetch column names of the DataFrame
# columns = SR_positive_reset.columns

# # Check if the DataFrame is empty
# if SR_positive_reset.empty:
#     selected_df_4 = SR_positive_reset
# else:
#     # Define the selected columns
#     selected_columns = [
#         ('gateway', ''),
#         ('Total Payments', 'T-2'),
#         ('Total Payments', 'T-1'),
#         ('T-2 SR', ''),
#         ('T-1 SR', ''),
#         ('Delta SR', ''),
#         ('Delta Attempts % Change', ''),
#     ]

#     try:
#         # Attempt to select the specified columns
#         selected_df_4 = SR_positive_reset[selected_columns]
#         selected_df_4
#     except KeyError as e:
#         # Handle KeyError exception
#         print("Error:", e)

# # Fetch column names of the DataFrame
# columns = SR_negative_reset.columns

# # Check if the DataFrame is empty
# if SR_negative_reset.empty:
#     selected_df_5 = SR_negative_reset
# else:
#     # Define the selected columns
#     selected_columns = [
#         ('gateway', ''),
#         ('Total Payments', 'T-2'),
#         ('Total Payments', 'T-1'),
#         ('T-2 SR', ''),
#         ('T-1 SR', ''),
#         ('Delta SR', ''),
#         ('Delta Attempts % Change', '')
#             ]

#     try:
#         # Attempt to select the specified columns
#         selected_df_5 = SR_negative_reset[selected_columns]
#         selected_df_5
#     except KeyError as e:
#         # Handle KeyError exception
#         print("Error:", e)

# # Fetch column names of the DataFrame
# columns_sr_negative_reset_df_2_pivot_reset = SR_negative_reset_df_2_pivot_reset.columns

# # Check if the DataFrame is empty
# if SR_negative_reset_df_2_pivot_reset.empty:
#     selected_df_6 = SR_negative_reset_df_2_pivot_reset
# else:
#     # Define the selected columns
#     selected_columns_sr_negative_reset_df_2_pivot_reset = [
#         ('gateway', ''),
#         ('method_advanced', ''),
#         ('Total Payments', 'T-2'),
#         ('Total Payments', 'T-1'),
#         ('T-2 SR', ''),
#         ('T-1 SR', ''),
#         ('Delta SR', ''),
#         ('Delta Attempts % Change', '')
#     ]

#     try:
#         # Attempt to select the specified columns
#         selected_df_6 = SR_negative_reset_df_2_pivot_reset[selected_columns_sr_negative_reset_df_2_pivot_reset]
#         selected_df_6
#     except KeyError as e:
#         # Handle KeyError exception
#         print("Error:", e)

# # Fetch column names of the DataFrame
# columns_sr_negative_reset_df_1_pivot_reset = SR_negative_reset_df_1_pivot_reset.columns

# # Check if the DataFrame is empty
# if SR_negative_reset_df_1_pivot_reset.empty:
#     selected_df_7 = SR_negative_reset_df_1_pivot_reset
# else:
#     # Define the selected columns
#     selected_columns_sr_negative_reset_df_1_pivot_reset = [
#         ('gateway', ''),
#         ('method_advanced', ''),
#         ('Total Payments', 'T-2'),
#         ('Total Payments', 'T-1'),
#         ('T-2 SR', ''),
#         ('T-1 SR', ''),
#         ('Delta SR', ''),
#         ('Delta Attempts % Change', '')
#     ]

#     try:
#         # Attempt to select the specified columns
#         selected_df_7 = SR_negative_reset_df_1_pivot_reset[selected_columns_sr_negative_reset_df_1_pivot_reset]
#         selected_df_7
#     except KeyError as e:
#         # Handle KeyError exception
#         print("Error:", e)

# # Fetch column names of the DataFrame
# columns_sr_negative_reset_df_2_pivot_reset = SR_negative_reset_df_2_pivot_reset.columns

# # Check if the DataFrame is empty
# if SR_negative_reset_df_2_pivot_reset.empty:
#     selected_df_10 = SR_negative_reset_df_2_pivot_reset
# else:
#     # Define the selected columns
#     selected_columns_sr_negative_reset_df_2_pivot_reset = [
#         ('gateway', ''),
#         ('method_advanced', ''),
#         ('Total Payments', 'T-2'),
#         ('Total Payments', 'T-1'),
#         ('T-2 SR', ''),
#         ('T-1 SR', ''),
#         ('Delta SR', ''),
#         ('Delta Attempts % Change', '')
#     ]

#     try:
#         # Attempt to select the specified columns
#         selected_df_10 = SR_negative_reset_df_2_pivot_reset[selected_columns_sr_negative_reset_df_2_pivot_reset]
#         selected_df_10
#     except KeyError as e:
#         # Handle KeyError exception
#         print("Error:", e)
# selected_df_10


# # Fetch column names of the DataFrame
# columns_sr_negative_reset_df_1_mx_pivot_reset = SR_negative_reset_df_1_mx_pivot_reset.columns

# # Check if the DataFrame is empty
# if SR_negative_reset_df_1_mx_pivot_reset.empty:
#     selected_df_8 = SR_negative_reset_df_1_mx_pivot_reset
# else:
#     # Define the selected columns
#     selected_columns_sr_negative_reset_df_1_mx_pivot_reset = [
#         ('Name', ''),
#         ('gateway', ''),
#         ('method_advanced', ''),
#         ('Total Payments', 'T-2'),
#         ('Total Payments', 'T-1'),
#         ('T-2 SR', ''),
#         ('T-1 SR', ''),
#         ('Delta SR', ''),
#         ('Delta Attempts % Change', '')
#     ]

#     try:
#         # Attempt to select the specified columns
#         selected_df_8 = SR_negative_reset_df_1_mx_pivot_reset[selected_columns_sr_negative_reset_df_1_mx_pivot_reset]
#         selected_df_8
#     except KeyError as e:
#         # Handle KeyError exception
#         print("Error:", e)

# # Fetch column names of the DataFrame
# columns_sr_negative_reset_df_2_mx_pivot_reset = SR_negative_reset_df_2_mx_pivot_reset.columns

# # Check if the DataFrame is empty
# if SR_negative_reset_df_2_mx_pivot_reset.empty:
#     selected_df_9 = SR_negative_reset_df_2_mx_pivot_reset
# else:
#     # Define the selected columns
#     selected_columns_sr_negative_reset_df_2_mx_pivot_reset = [
#         ('Name', ''),
#         ('gateway', ''),
#         ('method_advanced', ''),
#         ('Total Payments', 'T-2'),
#         ('Total Payments', 'T-1'),
#         ('T-2 SR', ''),
#         ('T-1 SR', ''),
#         ('Delta SR', ''),
#         ('Delta Attempts % Change', '')
#     ]

#     try:
#         # Attempt to select the specified columns
#         selected_df_9 = SR_negative_reset_df_2_mx_pivot_reset[selected_columns_sr_negative_reset_df_2_mx_pivot_reset]
#         selected_df_9
#     except KeyError as e:
#         # Handle KeyError exception
#         print("Error:", e)

# COMMAND ----------



# COMMAND ----------

SR_negative_reset.columns

# COMMAND ----------

SR_negative_reset_df_2_pivot_reset

# COMMAND ----------

# Fetch column names of the DataFrame
columns = overall_SR_df_reset.columns

# Check if the DataFrame is empty
if overall_SR_df_reset.empty:
    selected_df = overall_SR_df_reset
else:
    # Define the selected columns
    selected_columns = [
            (     'Total Payments', 'T-2'),
            (     'Total Payments', 'T-1'),
            ('Successful payments', 'T-2'),
            ('Successful payments', 'T-1'),
            (            'T-2 SR%',    ''),
            (            'T-1 SR%',    ''),
            (           'Delta SR',    '')
    ]

    try:
        # Attempt to select the specified columns
        selected_df = overall_SR_df_reset[selected_columns]
    except KeyError as e:
        # Handle KeyError exception
        print("Error:", e)
        selected_df = None

selected_df

# Fetch column names of the DataFrame
columns = SR_reset.columns

# Check if the DataFrame is empty
if SR_reset.empty:
    selected_df_1 = SR_reset
else:
    # Define the selected columns
    selected_columns = [
        
            (                'gateway',    ''),
            (         'Total Payments', 'T-1'),
            (         'Total Payments', 'T-2'),
            (            'merchant_id', 'T-1'),
            (            'merchant_id', 'T-2'),
            (                 'T-2 SR',    ''),
            (                 'T-1 SR',    ''),
            (               'Delta SR',    ''),
            ('Delta Attempts % Change',    '')
        
        ]
    try:
        # Attempt to select the specified columns
        selected_df_1 = SR_reset[selected_columns]
        selected_df_1
    except KeyError as e:
        # Handle KeyError exception
        print("Error:", e)
selected_df_1

# Fetch column names of the DataFrame
columns = SR_negative_reset.columns

# Check if the DataFrame is empty
# Fetch column names of the DataFrame
columns = SR_negative_reset.columns

if SR_negative_reset.empty:
    selected_df_5 = SR_negative_reset
else:
    # Define the selected columns
    selected_columns_sr_negative_reset_df_2_mx_pivot_reset = [
            (                'gateway',    ''),
            (         'Total Payments', 'T-1'),
            (         'Total Payments', 'T-2'),
            (            'merchant_id', 'T-1'),
            (            'merchant_id', 'T-2'),
            (                 'T-2 SR',    ''),
            (                 'T-1 SR',    ''),
            (               'Delta SR',    ''),
            ('Delta Attempts % Change',    ''),
            ]

    try:
        # Attempt to select the specified columns
        selected_df_5 = SR_negative_reset[selected_columns_sr_negative_reset_df_2_mx_pivot_reset]
        selected_df_5
    except KeyError as e:
        # Handle KeyError exception
        print("Error:", e)

# Check if the DataFrame is empty
#Change Rohan For Positive side, change all negative df copies
if SR_positive.empty:
    selected_df_4 = SR_positive
else:
    # Define the selected columns
    selected_columns_sr_positive_reset_df_2_mx_pivot_reset = [
            (                'gateway',    ''),
            (         'Total Payments', 'T-1'),
            (         'Total Payments', 'T-2'),
            (            'merchant_id', 'T-1'),
            (            'merchant_id', 'T-2'),
            (                 'T-2 SR',    ''),
            (                 'T-1 SR',    ''),
            (               'Delta SR',    ''),
            ('Delta Attempts % Change',    ''),
            ]

    try:
        # Attempt to select the specified columns
        selected_df_4 = SR_positive_reset[selected_columns_sr_positive_reset_df_2_mx_pivot_reset]
        selected_df_4
    except KeyError as e:
        # Handle KeyError exception
        print("Error:", e)

# Fetch column names of the DataFrame
columns = SR_negative_reset.columns

# Check if the DataFrame is empty
if SR_negative_reset.empty:
    selected_df_5 = SR_negative_reset
else:
    # Define the selected columns
    selected_columns = [

            (                'gateway',    ''),
            (         'Total Payments', 'T-2'),
            (         'Total Payments', 'T-1'),
            (                 'T-2 SR',    ''),
            (                 'T-1 SR',    ''),
            (               'Delta SR',    ''),
            ('Delta Attempts % Change',    '')

            ]

    try:
        # Attempt to select the specified columns
        selected_df_5 = SR_negative_reset[selected_columns]
        selected_df_5
    except KeyError as e:
        # Handle KeyError exception
        print("Error:", e)

# Fetch column names of the DataFrame
columns_sr_negative_reset_df_2_pivot_reset = SR_negative_reset_df_2_pivot_reset.columns

# Check if the DataFrame is empty
if SR_negative_reset_df_2_pivot_reset.empty:
    selected_df_6 = SR_negative_reset_df_2_pivot_reset
else:
    # Define the selected columns
    selected_columns_sr_negative_reset_df_2_pivot_reset = [
        ('gateway', ''),
        ('method_advanced', ''),
        ('Total Payments', 'T-2'),
        ('Total Payments', 'T-1'),
        ('T-2 SR', ''),
        ('T-1 SR', ''),
        ('Delta SR', ''),
        ('Delta Attempts % Change', '')
    ]

    try:
        # Attempt to select the specified columns
        selected_df_6 = SR_negative_reset_df_2_pivot_reset[selected_columns_sr_negative_reset_df_2_pivot_reset]
        selected_df_6
    except KeyError as e:
        # Handle KeyError exception
        print("Error:", e)

# Fetch column names of the DataFrame
columns_sr_negative_reset_df_1_pivot_reset = SR_negative_reset_df_1_pivot_reset.columns

# Check if the DataFrame is empty
if SR_negative_reset_df_1_pivot_reset.empty:
    selected_df_7 = SR_negative_reset_df_1_pivot_reset
else:
    # Define the selected columns
    selected_columns_sr_negative_reset_df_1_pivot_reset = [

            (                'gateway',    ''),
            (        'method_advanced',    ''),
            (         'Total Payments', 'T-2'),
            (         'Total Payments', 'T-1'),
            (                 'T-2 SR',    ''),
            (                 'T-1 SR',    ''),
            (               'Delta SR',    ''),

    ]

    try:
        # Attempt to select the specified columns
        selected_df_7 = SR_negative_reset_df_1_pivot_reset[selected_columns_sr_negative_reset_df_1_pivot_reset]
        selected_df_7
    except KeyError as e:
        # Handle KeyError exception
        print("Error:", e)

# Fetch column names of the DataFrame
columns_sr_negative_reset_df_2_pivot_reset = SR_negative_reset_df_2_pivot_reset.columns

# Check if the DataFrame is empty
if SR_negative_reset_df_2_pivot_reset.empty:
    selected_df_10 = SR_negative_reset_df_2_pivot_reset
else:
    # Define the selected columns
    selected_columns_sr_negative_reset_df_2_pivot_reset = [
        ('gateway', ''),
        ('method_advanced', ''),
        ('Total Payments', 'T-2'),
        ('Total Payments', 'T-1'),
        ('T-2 SR', ''),
        ('T-1 SR', ''),
        ('Delta SR', ''),
        ('Delta Attempts % Change', '')
    ]

    try:
        # Attempt to select the specified columns
        selected_df_10 = SR_negative_reset_df_2_pivot_reset[selected_columns_sr_negative_reset_df_2_pivot_reset]
        selected_df_10
    except KeyError as e:
        # Handle KeyError exception
        print("Error:", e)
selected_df_10

# Fetch column names of the DataFrame
columns_sr_negative_reset_df_1_mx_pivot_reset = SR_negative_reset_df_1_mx_pivot_reset.columns

# Check if the DataFrame is empty
if SR_negative_reset_df_1_mx_pivot_reset.empty:
    selected_df_8 = SR_negative_reset_df_1_mx_pivot_reset
else:
    # Define the selected columns
    selected_columns_sr_negative_reset_df_1_mx_pivot_reset = [
            (            'merchant_id',    ''),
            (                   'Name',    ''),
            (                'gateway',    ''),
            (        'method_advanced',    ''),
            (         'Total Payments', 'T-2'),
            (         'Total Payments', 'T-1'),
            (    'Successful payments', 'T-2'),
            (    'Successful payments', 'T-1'),
            (                 'T-2 SR',    ''),
            (                 'T-1 SR',    ''),
            (               'Delta SR',    ''),
            ('Delta Attempts % Change',    '')
    ]

    try:
        # Attempt to select the specified columns
        selected_df_8 = SR_negative_reset_df_1_mx_pivot_reset[selected_columns_sr_negative_reset_df_1_mx_pivot_reset]
        selected_df_8
    except KeyError as e:
        # Handle KeyError exception
        print("Error:", e)

# Fetch column names of the DataFrame
columns_sr_negative_reset_df_2_mx_pivot_reset = SR_negative_reset_df_2_mx_pivot_reset.columns

# Check if the DataFrame is empty
if SR_negative_reset_df_2_mx_pivot_reset.empty:
    selected_df_9 = SR_negative_reset_df_2_mx_pivot_reset
else:
    # Define the selected columns
    selected_columns_sr_negative_reset_df_2_mx_pivot_reset = [
        ('merchant_id',''),
        ('Name', ''),
        ('gateway', ''),
        ('method_advanced', ''),
        ('Total Payments', 'T-2'),
        ('Total Payments', 'T-1'),
        ('T-2 SR', ''),
        ('T-1 SR', ''),
        ('Delta SR', ''),
        ('Delta Attempts % Change', '')
    ]

    try:
        # Attempt to select the specified columns
        selected_df_9 = SR_negative_reset_df_2_mx_pivot_reset[selected_columns_sr_negative_reset_df_2_mx_pivot_reset]
        selected_df_9
    except KeyError as e:
        # Handle KeyError exception
        print("Error:", e)

# COMMAND ----------

#1st rank
#Change Rohan -- change in final assignment only selected_df_6 and selected_df_7 needs change, to be interchanged
selected_df_6 #2nd Rank Gateway X Method
selected_df_7 #1st Rank Gateway X Method
selected_df_8 #1st Rank Gateway X Merchant X Method 
selected_df_9 #2nd Rank Gateway X Merchant X Method

#SR_negative_reset_df_2_pivot_reset
#SR_negative

# COMMAND ----------

# MAGIC %md
# MAGIC ###MTU

# COMMAND ----------

# MTU
# Rename the columns based on the provided index
mtu_pivot_reset.columns = ['Team Mapping', 'Total', 'T-2', 'T-1']

# Select specific columns
selected_columns_1 = ['Team Mapping','T-2', 'T-1']  # Select the columns you want to keep
mtu_df1_selected = mtu_pivot_reset[selected_columns_1]

#change rohan
mtu_pay_attempts = mtu_filtered.pivot_table(
    index=["merchant_id","Name", "Team Mapping"],
    columns="Period",
    values="Successful payments",
    aggfunc=sum
).fillna(0).astype(int)

# DataFrame 2: mtu_pivot_transacting_reset
mtu_pivot_transacting_reset.columns = ['merchant_id','Name', 'Team Mapping', 'Total', 'T-2', 'T-1', 'Delta Mx']
selected_columns_2 = ['merchant_id','Name', 'Team Mapping']  # Select the columns you want to keep
mtu_df2_selected = mtu_pivot_transacting_reset[selected_columns_2]

# DataFrame 3: mtu_pivot_not_transacting_reset
mtu_pivot_not_transacting_reset.columns =["merchant_id",'Name', 'Team Mapping', 'T-1', 'T-2', 'Total']
selected_columns_3 = ["merchant_id",'Name', 'Team Mapping']
mtu_df3_selected = mtu_pivot_not_transacting_reset[selected_columns_3]

# DataFrame 4: mtu_risk_reset
new_column_names = {
    'merchant_id': 'Merchant ID',
    'Name': 'Name',
    'Team Mapping': 'Team',
    'Total Payments': 'Total Payments'}
mtu_df4_selected = mtu_risk_reset.rename(columns=new_column_names)

# Check if the expected columns are not present, retain the original DataFrame

mtu_df1_selected = mtu_pivot_reset.loc[:, ['Team Mapping', 'T-1', 'T-2']]

if not set(['Name', 'Team Mapping']).issubset(mtu_df2_selected.columns):
    mtu_df2_selected = mtu_pivot_transacting_reset[['Name', 'Team Mapping']]
else:
    mtu_df2_selected = mtu_pivot_transacting_reset[["merchant_id","Name", "Team Mapping", "T-1", "T-2"]]

if not set(['Name', 'Team Mapping']).issubset(mtu_df3_selected.columns):
    mtu_df3_selected = mtu_pivot_not_transacting_reset[['Name', 'Team Mapping']]
else:
    mtu_df3_selected = mtu_pivot_not_transacting_reset[["merchant_id","Name", "Team Mapping", "T-1", "T-2"]]

if not set(['Name', 'Team Mapping', 'Merchant ID', 'Total Payments']).issubset(mtu_df4_selected.columns):
    mtu_df4_selected = mtu_risk_reset
else:
    mtu_df4_selected = mtu_risk_reset[["Name", "Team Mapping", "Merchant ID", "Total Payments"]]


#mtu_df2_selected= mtu_pay_attempts[mtu_pay_attempts['Name'].isin(mtu_df2_selected['Name'])]
#mtu_df3_selected = mtu_pay_attempts[mtu_pay_attempts['Name'].isin(mtu_df3_final['Name'])]
#Change Rohan
mtu_df2_selected = mtu_pay_attempts[(mtu_pay_attempts['T-2'] == 0) & (mtu_pay_attempts['T-1'] != 0)]
mtu_df3_selected = mtu_pay_attempts[(mtu_pay_attempts['T-1'] == 0) & (mtu_pay_attempts['T-2'] != 0)]

# COMMAND ----------

mtu_df2_selected

# COMMAND ----------

# MAGIC %md
# MAGIC ##Late Auth

# COMMAND ----------

merchant_drill_1_reset

# COMMAND ----------

expected_columns = ['merchant_id', 'Name', 'gateway', 'method_advanced',
                    'T-1 Late Auth', 'T-1 Total', 'T-1 Late Auth%']

# Check the columns that are available in the DataFrame
available_columns = list(set(expected_columns).intersection(merchant_drill_1_reset.columns))

# If none of the expected columns are available, select an empty DataFrame
if not available_columns:
    late_auth_df_selected_3 = pd.DataFrame(columns=merchant_drill_1_reset.columns)
else:
    # Rename the columns based on the available columns
    renamed_columns = dict(zip(available_columns, expected_columns))
    merchant_drill_1_reset.rename(columns=renamed_columns, inplace=True)

    # Select the available columns
    late_auth_df_selected_3 = merchant_drill_1_reset[expected_columns]
late_auth_df_selected_3

# COMMAND ----------

gateway_for_which_late_auth_increased_reset

# COMMAND ----------

gateway_drill_1_reset

# COMMAND ----------

gateway_drill_1_reset.columns

# COMMAND ----------

if gateway_drill_1_reset.empty:
    late_auth_df_selected_2 = (
        "% Late auth either not greater than 0.1% or attempts lesser than 10"
    )
else:
    gateway_drill_1_reset.columns = [
        "Gateway",
        "Method Advanced",
        "T-2 Non Late Auth",
        "T-1 Non Late Auth",
        "T-1 Late Auth",
        "T-1 Non Late Auth",
        "T-2 Total",
        "T-1 Late Auth%",
        "T-2 Late Auth%",
        "Delta",
        "T-2 Late Auth",
    ]
    selected_columns_2 = [
        "Gateway",
        "Method Advanced",
        "T-1 Late Auth",
        "T-2 Late Auth",
        "T-1 Late Auth%",
        "T-2 Late Auth%",
        "Delta"
    ]
    late_auth_df_selected_2 = gateway_drill_1_reset[selected_columns_2]

# COMMAND ----------

gateway_drill_1_reset.columns

# COMMAND ----------


if overall_late_auth_reset.empty:
    late_auth_df_selected_0 = overall_late_auth_reset
else:   
    overall_late_auth_reset.columns = ['Index', 'T-1 Non Late Auth', 'T-1 Late Auth', 'T-2 Non Late Auth', 'T-2 Late Auth', 'T-2 Total', 'T-1 Total', 'T-2 Late Auth%', 'T-1 Late Auth%', 'Delta']
    selected_columns_0 = ['Index','T-2 Late Auth', 'T-1 Late Auth', 'T-2 Total', 'T-1 Total', 'T-2 Late Auth%', 'T-1 Late Auth%', 'Delta']
    late_auth_df_selected_0 = overall_late_auth_reset[selected_columns_0]


if gateway_late_auth_reset.empty:
    late_auth_df_selected_1 = gateway_late_auth_reset
else:   
    gateway_late_auth_reset.columns = ['Gateway', 'T-2 Late Auth', 'T-2 Non Late Auth', 'T-1 Late Auth', 'T-1 Non Late Auth', 'T-2 Total', 'T-1 Total', 'T-2 Late Auth%', 'T-1 Late Auth%', 'Delta']
    selected_columns_1 = ['Gateway', 'T-2 Late Auth', 'T-1 Late Auth', 'T-2 Total', 'T-1 Total','T-2 Late Auth%', 'T-1 Late Auth%', 'Delta']
    late_auth_df_selected_1 = gateway_late_auth_reset[selected_columns_1]
late_auth_df_selected_1

if gateway_for_which_late_auth_increased_reset.empty:
    late_auth_df_selected_01 = '% Late auth either not greater than 0.1% or attempts lesser than 10'
else:   
    gateway_for_which_late_auth_increased_reset.columns = ['Gateway', 'T-2 Late Auth', 'T-2 Non Late Auth', 'T-1 Late Auth', 'T-1 Non Late Auth', 'T-2 Total', 'T-1 Total', 'T-2 Late Auth%', 'T-1 Late Auth%', 'Delta', 'Delta Rank']
    selected_columns_1 = ['Gateway', 'T-2 Late Auth', 'T-1 Late Auth', 'T-2 Total', 'T-1 Total','T-2 Late Auth%', 'T-1 Late Auth%', 'Delta']
    late_auth_df_selected_01 = gateway_for_which_late_auth_increased_reset[selected_columns_1]
late_auth_df_selected_01

print("Columns")
print(gateway_drill_1_reset)
print(gateway_drill_1_reset.columns)

if gateway_drill_1_reset.empty:
    late_auth_df_selected_2 = (
        "% Late auth either not greater than 0.1% or attempts lesser than 10"
    )
else:
    gateway_drill_1_reset.columns = ['Gateway', 'Method', 'T-2 Late Auth', 'T-2 Non Late Auth', 'T-1 Late Auth', 'T-1 Non Late Auth', 'T-2 Total', 'T-1 Total', 'T-2 Late Auth%', 'T-1 Late Auth%', 'Delta']
    selected_columns_2 = ['Gateway', 'Method','T-2 Late Auth', 'T-1 Late Auth', 'T-2 Total', 'T-1 Total','T-2 Late Auth%', 'T-1 Late Auth%', 'Delta']
    late_auth_df_selected_2 = gateway_drill_1_reset[selected_columns_2]

print(late_auth_df_selected_2)
if gateway_drill_2_reset.empty:
    late_auth_df_selected_20 = '% Late auth either not greater than 0.1% or attempts lesser than 10'
else:   
    gateway_drill_2_reset.columns = ['Gateway', 'Method', 'T-2 Late Auth', 'T-2 Non Late Auth', 'T-1 Late Auth', 'T-1 Non Late Auth', 'T-2 Total', 'T-1 Total', 'T-2 Late Auth%', 'T-1 Late Auth%', 'Delta']
    selected_columns_20 = ['Gateway', 'Method','T-2 Late Auth', 'T-1 Late Auth', 'T-2 Total', 'T-1 Total','T-2 Late Auth%', 'T-1 Late Auth%', 'Delta']
    late_auth_df_selected_20 = gateway_drill_2_reset[selected_columns_20]

if merchant_drill_1_reset.empty:
    late_auth_df_selected_3 = '% Late auth either not greater than 0.1% or attempts lesser than 10'
else:   
    merchant_drill_1_reset.columns = ['merchant_id','Name','Gateway', 'Method','T-2 Late Auth', 'T-2 Non Late Auth' , 'T-1 Late Auth', 'T-1 Non Late Auth', 'T-2 Total' ,'T-1 Total', 'T-2 Late Auth%','T-1 Late Auth%', 'Delta']
    selected_columns_3 = ['merchant_id','Name','Gateway', 'Method', 'T-2 Late Auth', 'T-2 Total', 'T-2 Late Auth%', 'T-1 Late Auth', 'T-1 Total', 'T-1 Late Auth%', 'Delta']
    late_auth_df_selected_3 = merchant_drill_1_reset[selected_columns_3]

if merchant_drill_2_reset.empty:
    late_auth_df_selected_30 = '% Late auth either not greater than 0.1% or attempts lesser than 10'
else:   
    merchant_drill_2_reset.columns = ['merchant_id','Name','Gateway', 'Method', 'T-2 Late Auth', 'T-2 Non Late Auth', 'T-1 Late Auth', 'T-1 Non Late Auth', 'T-2 Total', 'T-1 Total', 'T-2 Late Auth%', 'T-1 Late Auth%', 'Delta']
    selected_columns_30 = ['merchant_id','Name','Gateway', 'Method', 'T-2 Late Auth', 'T-1 Late Auth', 'T-1 Non Late Auth', 'T-2 Total', 'T-1 Total', 'T-2 Late Auth%', 'T-1 Late Auth%', 'Delta']
    late_auth_df_selected_30 = merchant_drill_2_reset[selected_columns_30]

current_columns = top_5_merchants_late_auth_reset.columns

# Define the new column names
new_columns = ['merchant_id', 'Name', 'gateway', 'method_advanced', 'T-1 Late Auth', 'T-1 Non Late Auth', 'T-1 Total', 'T-1 Late Auth']

# Rename the columns using a list comprehension
top_5_merchants_late_auth_reset.columns = [col[0] if col[1] == '' else col[1] for col in top_5_merchants_late_auth_reset.columns]

# Assign the new column names
top_5_merchants_late_auth_reset.columns = new_columns

if top_5_merchants_late_auth_reset.empty:
    late_auth_df_selected_4 = '% Late auth either not greater than 0.1% or attempts lesser than 10'
else:   
    top_5_merchants_late_auth_reset.columns = ['merchant_id', 'Name', 'gateway', 'method_advanced', 'T-1 Late Auth', 'T-1 Non Late Auth', 'T-1 Total', 'T-1 Late Auth']
    selected_columns_30 = ['merchant_id', 'Name', 'gateway', 'method_advanced', 'T-1 Late Auth', 'T-1 Total', 'T-1 Late Auth']
    late_auth_df_selected_4 = top_5_merchants_late_auth_reset[selected_columns_30]

late_auth_df_selected_4

# COMMAND ----------

gateway_drill_1_reset

# COMMAND ----------

current_columns = top_5_merchants_late_auth_reset.columns

# Define the new column names
new_columns = ['merchant_id', 'Name', 'gateway', 'method_advanced', 'T-1 Late Auth', 'T-1 Non Late Auth', 'T-1 Total', 'T-1 Late Auth']

# Rename the columns using a list comprehension
top_5_merchants_late_auth_reset.columns = [col[0] if col[1] == '' else col[1] for col in top_5_merchants_late_auth_reset.columns]

# Assign the new column names
top_5_merchants_late_auth_reset.columns = new_columns

if top_5_merchants_late_auth_reset.empty:
    late_auth_df_selected_30 = '% Late auth either not greater than 0.1% or attempts lesser than 10'
else:   
    top_5_merchants_late_auth_reset.columns = ['merchant_id', 'Name', 'gateway', 'method_advanced', 'T-1 Late Auth', 'T-1 Non Late Auth', 'T-1 Total', 'T-1 Late Auth']
    selected_columns_30 = ['merchant_id', 'Name', 'gateway', 'method_advanced', 'T-1 Late Auth', 'T-1 Total', 'T-1 Late Auth']
    late_auth_df_selected_4 = top_5_merchants_late_auth_reset[selected_columns_30]

late_auth_df_selected_4

# COMMAND ----------

# upi_gateway_late_auth_drill_mx_reset

# COMMAND ----------

if upi_gateway_late_auth_reset.empty:
    upi_late_auth_df1 = upi_gateway_late_auth_reset
else:   
    upi_gateway_late_auth_reset.columns = ['Gateway', 'Auth TAT', 'T-2 Attempts', 'T-1 Attempts', 'T-2 % Attempts Split', 'T-1 % Attempts Split','Delta']
    selected_columns_0 = ['Gateway', 'Auth TAT','T-2 % Attempts Split', 'T-1 % Attempts Split','Delta']
    upi_late_auth_df1 = upi_gateway_late_auth_reset[selected_columns_0]

if upi_gateway_late_auth_filtered_check_reset.empty:
    upi_late_auth_df2 = upi_gateway_late_auth_filtered_check_reset
else:   
    upi_gateway_late_auth_filtered_check_reset.columns = ['Gateway', 'Auth TAT', 'T-2 Attempts', 'T-1 Attempts', 'T-2 % Attempts Split', 'T-1 % Attempts Split','Delta']
    selected_columns_1 = ['Gateway', 'Auth TAT', 'T-2 Attempts', 'T-1 Attempts', 'T-2 % Attempts Split', 'T-1 % Attempts Split','Delta']
    upi_late_auth_df2 = upi_gateway_late_auth_filtered_check_reset[selected_columns_1]

if upi_gateway_late_auth_drill_df_reset.empty:
    upi_late_auth_df3 = upi_gateway_late_auth_drill_df_reset
else:   
    upi_gateway_late_auth_drill_df_reset.columns = ['Gateway', 'method_advanced' ,'Auth TAT', 'T-2 Attempts', 'T-1 Attempts', 'T-2 % Attempts Split', 'T-1 % Attempts Split','Delta']
    selected_columns_2 = ['Gateway', 'method_advanced' ,'Auth TAT', 'T-2 Attempts', 'T-1 Attempts', 'T-2 % Attempts Split', 'T-1 % Attempts Split','Delta']
    upi_late_auth_df3 = upi_gateway_late_auth_drill_df_reset[selected_columns_2]

if df_rank_1_reset.empty:
    upi_late_auth_df4 = df_rank_1_reset
else:   
    df_rank_1_reset.columns = ['Gateway', 'method_advanced', 'Auth TAT', 'T-2 Attempts', 'T-1 Attempts', 'T-2 % Attempts Split',
       'T-1 % Attempts Split', 'Delta', 'Delta Rank']
    selected_columns_3 = ['Gateway', 'method_advanced', 'Auth TAT', 'T-2 Attempts', 'T-1 Attempts', 'T-2 % Attempts Split',
       'T-1 % Attempts Split', 'Delta']
    upi_late_auth_df4 = df_rank_1_reset[selected_columns_3]    

if upi_merchants_df_check_1_reset.empty:
    upi_late_auth_df5 = upi_merchants_df_check_1_reset
else:   
    upi_merchants_df_check_1_reset.columns = ['Merchant ID', 'Name', 'Gateway', 'Method Advanced', 'Auth TAT', 'T-2 Attempts', 'T-1 Attempts', 'T-2 % Attempts Split', 'T-1 % Attempts Split', 'Delta']
    selected_columns_4 = ['Merchant ID', 'Name', 'Gateway', 'Method Advanced', 'Auth TAT', 'T-2 Attempts', 'T-1 Attempts', 'T-2 % Attempts Split', 'T-1 % Attempts Split', 'Delta']
    upi_late_auth_df5 = upi_merchants_df_check_1_reset[selected_columns_4]    
upi_late_auth_df5


if upi_gateway_late_auth_drill_mx_reset.empty:
    upi_late_auth_df6 = upi_gateway_late_auth_drill_mx_reset
else:   
    upi_gateway_late_auth_drill_mx_reset.columns = ['Merchant ID', 'Name', 'Gateway', 'Method Advance', 'All Payments',
       'Payments with auth TAT > 12 mins', '% Late Auth']
    selected_columns_5 = ['Merchant ID', 'Name', 'Gateway', 'Method Advance', 'All Payments',
       'Payments with auth TAT > 12 mins', '% Late Auth']
    upi_late_auth_df6 = upi_gateway_late_auth_drill_mx_reset[selected_columns_5]    

# COMMAND ----------

# upi_gateway_late_auth_drill_mx_reset.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #Doc Creation

# COMMAND ----------

distinct_merchant_count = new_live_reset['MID'].nunique()
print(distinct_merchant_count)

transacting = mtu_pivot_transacting_reset['Name'].nunique()
print(transacting)

not_transacting = mtu_pivot_not_transacting_reset["Name"].nunique()
print(not_transacting)

# COMMAND ----------



# COMMAND ----------

unique_counts = new_live_reset.groupby('Team Mapping')['MID'].nunique()

# Display the counts
print("Unique MID counts:")
unique_counts

# COMMAND ----------

t_1_mtu_flag_reset = t_1_mtu_flag.reset_index()
t_1_mtu_flag_reset

# COMMAND ----------

t_1_mtu_flag_reset = t_1_mtu_flag_reset.drop('index', axis=1)

wow_gmv_pivot_reset_reset = wow_gmv_pivot_reset_reset.drop(['level_0', 'index'], axis=1)

mom_pivot_reset = mom_pivot_reset.drop(['index'], axis=1)

mtd_pivot_reset = mtd_pivot_reset.drop(['index'], axis=1)


# COMMAND ----------

selected_df.columns

# COMMAND ----------

selected_df_6

# COMMAND ----------

late_auth_df_selected_2
#change rohan
late_auth_df_selected_3=late_auth_df_selected_3.sort_values(by=['Delta'], ascending=False)
late_auth_df_selected_3


# COMMAND ----------


def add_summary_bullet_points(doc, summary_text):
    # Add a heading for the summary section

    # Add bullet points for the summary text
    for text in summary_text:
        # Set font to Arial
        paragraph = doc.add_paragraph(text, style='ListBullet')
        for run in paragraph.runs:
            run.font.name = "Arial"
            run.font.size = Pt(11)  # Set font size to 10

def add_dataframe_as_table(doc, df, name):
    # Add a heading for the DataFrame
    doc.add_heading(name, level=2)

    # Add table
    table = doc.add_table(rows=df.shape[0] + 1, cols=df.shape[1])

    # Set column widths
    table.width = Inches(15)
    column_widths = [1.75] * df.shape[1]  # Set each column width to 1.5 inches
    for column, width in zip(table.columns, column_widths):
        column.width = Inches(width)

    # Set table style
    table.style = "Table Grid"

    # Set borders to black
    for row in table.rows:
        for cell in row.cells:
            for paragraph in cell.paragraphs:
                for run in paragraph.runs:
                    run.font.color.rgb = docx.shared.RGBColor(0, 0, 0)  # Black

    for j in range(df.shape[1]):
        table.cell(0, j).text = df.columns[j]
        # Access paragraph in the cell
        paragraph = table.cell(0, j).paragraphs[0]
        paragraph.runs[0].font.bold = True  # Set first row bold
        paragraph.runs[0].font.name = "Arial"  # Set font to Arial
        paragraph.runs[0].font.size = Pt(10)  # Set font size to 10

    # Add data
    for i in range(df.shape[0]):
        for j in range(df.shape[1]):
            table.cell(i + 1, j).text = str(df.values[i, j])
            # Access paragraph in the cell
            paragraph = table.cell(i + 1, j).paragraphs[0]
            paragraph.runs[0].font.name = "Arial"  # Set font to Arial
            paragraph.runs[0].font.size = Pt(10)  # Set font size to 10

# Create a new Word document
doc = Document()

for paragraph in doc.paragraphs:
    for run in paragraph.runs:
        run.font.name = "Arial"

    for row in table.rows:
        for cell in row.cells:
            for paragraph in cell.paragraphs:
                for run in paragraph.runs:
                    run.font.name = "Arial"

# Add heading with font Lato and size 30
heading_text = "Optimizer - Core & Intelligence - "
current_date = datetime.now().strftime("%Y-%m-%d")
heading_text += current_date
heading = doc.add_heading(heading_text, level=1)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(30)

doc.add_paragraph(f"Note:")
intro_bullet = [f"T1 period starts on {t1_start} & T1 period ends on {t1_end}",
f"T2 period starts on {t2_start} & T2 period ends on {t2_end}",
f"Refunds summary are considered between the date {refund_start_date} and {refund_end_date}"]
add_summary_bullet_points(doc, intro_bullet)

heading_text_summary = "Summary"
heading = doc.add_heading(heading_text_summary, level=2)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(25)

# Define the summary text
# Initialize the summary text
summary_text = [
    f"Total MTU as of T-1 Period: {mtu_df1_selected.iloc[3,2]}",
    f"Total merchants that went live in the last 2 months: {distinct_merchant_count}",
    f"Count of Mx transacting in T-1 & not T-2: {transacting}",
    f"Count of Mx transacting in T-2 & not T-1: {not_transacting}",
    f"Gateways for which SR dipped are: {selected_df_5.iloc[0, 0]} (by {selected_df_5.iloc[0, 5]})",
    f"T-1 Period GMV change in Cr from T-2 Period: {wow_gmv_pivot_reset_reset.iloc[0,2]}",
    f"% GMV of achieved of T-2 month GMV attained in T-1 month: {mom_pivot_reset.iloc[0,2]}",
    f"% MTD change in GMV for this month as compared to previous month: {mtd_pivot_reset.iloc[0,3]}",
    f"% change in %late auth from T-2 period than in T-1 period: {late_auth_df_selected_0.iloc[0,7]}",
    f"Refunds SR between {refund_start_date} & {refund_end_date}: {overall_SR_reset.iloc[0,2]}%"
]

# Add the summary section to the document
add_summary_bullet_points(doc, summary_text)

# Define a function to add a dataframe as a table with a specified header
def add_dataframe_as_table_with_header(doc, dataframe, header_text):
    # Add header
    header = doc.add_paragraph()
    header_run = header.add_run(header_text)
    header_run.bold = True
    header_run.font.size = Pt(14)
    header.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
    
    # Add table
    table = doc.add_table(rows=dataframe.shape[0] + 1, cols=dataframe.shape[1])
    
    # Add column headers
    for j, col in enumerate(dataframe.columns):
        table.cell(0, j).text = col
    
    # Add data rows
    for i in range(dataframe.shape[0]):
        for j in range(dataframe.shape[1]):
            table.cell(i + 1, j).text = str(dataframe.iloc[i, j])

heading_text_3 = "MTU"
heading = doc.add_heading(heading_text_3, level=2)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(25)

# Add DataFrames to the document
heading_text_3 = "WoW MTU Count"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, mtu_df1_selected,"")

heading_text_3 = "Merchants that went live in last 2 month"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, new_live_reset, "")

heading_text_3 = "Transacting in T-1 but not in T-2"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, mtu_df2_selected.reset_index(), "")

heading_text_3 = "Transacting in T-2 but not in T-1"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, mtu_df3_selected.reset_index(), "")

heading_text_3 = "MTU count as per revised definition (for last month only)"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, t_1_mtu_flag_reset,"")


# GMV
heading_text_2 = "GMV"
heading = doc.add_heading(heading_text_2, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(25)

# Add DataFrames to the document

heading_text_3 = "GMV changes in the last 2 weeks"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, wow_gmv_pivot_reset_reset, "")

heading_text_3 = "% of T-1 Period Live GMV reached"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, mom_pivot_reset, "")

heading_text_3 = "Top Promoters from the last week"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, wow_gmv_top_promoters_pivot_reset_selected_df,"",)

heading_text_3 = "Top Detractors from the last week"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, wow_gmv_detractors_reset_selected_df, "")

doc.add_picture('detractors_gmv_comparison.png', width=Inches(10))

heading_text_3 = "LMTD vs MTD GMV Comparison"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc,mtd_pivot_reset,"",)

heading_text_3 = "LMTD vs MTD Promoters"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, top_5_promoters_mtd_reset, "")

heading_text_3 = "LMTD vs MTD Detractors"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, top_5_detractors_mtd_reset, "")

# Success Rate
heading_text_1 = "Success Rate"
heading = doc.add_heading(heading_text_1, level=2)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(25)

# Add DataFrames to the document
heading_text_3 = "SR% Change Last 2 Weeks"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, selected_df, "")

heading_text_3 = "Gateway Level SR% Change Last 2 Weeks"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, selected_df_1, "")

heading_text_3 = "Gateway for which SR has increased"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, selected_df_4, "")

heading_text_3 = "Gateways for which SR has decreased"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, selected_df_5, "")

heading_text_3 = "Gateway 1 X Method for which SR has decreased"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc,selected_df_7,"",)

heading_text_3 = "Gateway 1 X Merchant X Method for which SR has decreased"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc,selected_df_8,"",)

heading_text_3 = "Gateway 2 X Method for which SR has decreased"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc,selected_df_6,"",)

heading_text_3 = "Gateway 2 X Merchant X Method for which SR has decreased"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc,selected_df_9,"",)

doc.add_picture('wow_sr_pivot_first.png')  
doc.add_picture('wow_sr_pivot_second.png') 
doc.add_picture('wow_attempts_first_6.png')  
doc.add_picture('wow_attempts_remaining.png')  
doc.add_picture('wow_sr_tokenization.png')  
doc.add_picture('wow_sr_tokenization_2.png')  
doc.add_picture('wow_tokenized_attempts_excluding_razorpay_first_6.png')  
doc.add_picture('wow_tokenized_attempts_excluding_razorpay_remaining.png')  

# Late Auth
heading_text_2 = "Late Auth"
heading = doc.add_heading(heading_text_2, level=2)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(25)

# Add DataFrames to the document

heading_text_3 = "Overall %late auth change in the last 2 weeks (Excluding method UPI)"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, late_auth_df_selected_0, "",)

heading_text_3 = "Gateway level %late auth changes in the last 2 weeks (Excluding method UPI)"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, late_auth_df_selected_1, "",)


heading_text_3 = "Gateways for which %late auth has increased (Excluding method UPI)"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
if isinstance(late_auth_df_selected_01, pd.DataFrame):
    add_dataframe_as_table(doc, late_auth_df_selected_01, "")
else:
    late_auth_df_selected_01

heading_text_3 = "Gateway 1 that saw increase in  %late auth changes in the last 2 weeks (Excluding method UPI)"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
if isinstance(late_auth_df_selected_2, pd.DataFrame):
    add_dataframe_as_table(doc, late_auth_df_selected_2, "")
else:
    late_auth_df_selected_2

heading_text_3 = "Merchants that saw increase in  %late auth changes in the last 2 weeks from the above gateway (Excluding method UPI)"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
if isinstance(late_auth_df_selected_3, pd.DataFrame):
#Change rohan : Below 2 lines
    late_auth_df_selected_3.fillna(0,inplace = True)
    late_auth_df_selected_3.sort_values(by="Delta",ascending=False)
    add_dataframe_as_table(doc, late_auth_df_selected_3, "")
else:
    late_auth_df_selected_3    


heading_text_3 = "Gateway 2 that saw increase in  %late auth changes in the last 2 weeks (Excluding method UPI)"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
if isinstance(late_auth_df_selected_20, pd.DataFrame):
    add_dataframe_as_table(doc, late_auth_df_selected_20, "")
else:
    late_auth_df_selected_20    


heading_text_3 = "Merchants that saw increase in  %late auth changes in the last 2 weeks from the above gateway (Excluding method UPI)"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
#ChangeRohan
#late_auth_df_selected_30=late_auth_df_selected_30.sort_values(by=[‘Delta’], ascending=False)
if isinstance(late_auth_df_selected_30, pd.DataFrame):
    #Change rohan : Below 3 lines
    late_auth_df_selected_30.fillna(0,inplace = True)
    late_auth_df_selected_30.sort_values(by="Delta",ascending=False)
    add_dataframe_as_table(doc, late_auth_df_selected_30, "")
else:
    late_auth_df_selected_30    

heading_text_3 = "Top 5 merchants with highest %late auth in the last week (Excluding method UPI)"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
if isinstance(late_auth_df_selected_4, pd.DataFrame):
    add_dataframe_as_table(doc, late_auth_df_selected_4, "")
else:
    late_auth_df_selected_4    

heading_text_3 = "% of Payments with Auth TAT > than 12 mins method UPI "
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
if isinstance(upi_gateway_late_auth_with_percentage_reset, pd.DataFrame):
    add_dataframe_as_table(doc, upi_gateway_late_auth_with_percentage_reset, "")
else:
    upi_gateway_late_auth_with_percentage_reset    

heading_text_3 = "Gateway level payments attempt across different Auth TAT bucket for method UPI"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
if isinstance(upi_late_auth_df1, pd.DataFrame):
    add_dataframe_as_table(doc, upi_late_auth_df1, "")
else:
    upi_late_auth_df1    

heading_text_3 = "Gateway level payment attempts for which Auth TAT is greater than 12 mins for method UPI"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
if isinstance(upi_late_auth_df2, pd.DataFrame):
    add_dataframe_as_table(doc, upi_late_auth_df2, "")
else:
    upi_late_auth_df2    


heading_text_3 = "Gateway & method level payment attempts for which Auth TAT is greater than 12 mins for method UPI"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
if isinstance(upi_late_auth_df3, pd.DataFrame):
    add_dataframe_as_table(doc, upi_late_auth_df3, "")
else:
    upi_late_auth_df3    


heading_text_3 = "Gateway & method level payment attempts where %delta increased from previous week where Auth TAT is greater than 12 mins for method UPI"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
if isinstance(upi_late_auth_df4, pd.DataFrame):
    add_dataframe_as_table(doc, upi_late_auth_df4, "")
else:
    upi_late_auth_df4    


heading_text_3 = "Merchants for which % delta has increased from previous week where Auth TAT is greater than 12 mins for method UPI"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
if isinstance(upi_late_auth_df5, pd.DataFrame):
    add_dataframe_as_table(doc, upi_late_auth_df5, "")
else:
    upi_late_auth_df5    

heading_text_3 = "Top merchants having high payments in Auth TAT bucket greater than 12 mins for method UPI"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
if isinstance(upi_late_auth_df6, pd.DataFrame):
    add_dataframe_as_table(doc, upi_late_auth_df6, "")
else:
    upi_late_auth_df6    


# Settlement Coverage
heading_text_2 = "Settlement Coverage"
heading = doc.add_heading(heading_text_2, level=2)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(25)

# Add DataFrames to the document

heading_text_3 = "Monthyl Settlement Coverage across gateway"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, monthyly_coverage_settlement, "",)

doc.add_picture('coverage_percentage_over_time_subplots.png', width=Inches(10))  # Adjust width as needed
doc.add_picture('nia_coverage.png', width=Inches(10))  # Adjust width as needed

heading_text_3 = "Days on which gateway settlement coverage was lower than 99% in last 30 days"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, low_coverage, "",)

# Refunds
heading_text_2 = "Refunds Summary"
heading = doc.add_heading(heading_text_2, level=2)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(25)

# Add DataFrames to the document

heading_text_3 = "Overall Refunds SR for T-2 Week"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, overall_SR_reset, "",)

heading_text_3 = "Gateway Level Refunds SR for T-2 Week"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, refunds_sr_final_pd_reset, "",)

heading_text_3 = "PayTM File Init Pending Refunds Reasons"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, pending_refuds_paytm_file_init_pd_reset, "",)

heading_text_3 = "PayU File Init Pending Refunds Reasons"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, pending_refuds_payu_file_init_pd_reset, "",)

heading_text_2 = "Native OTP Summary"
heading = doc.add_heading(heading_text_2, level=2)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(25)

heading_text_3 = "Overall Native OTP Performance Across Comparable Cohorts"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, overall_stats, "",)

heading_text_3 = "Overall Native OTP Delta SR Against 3ds Comparison"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, delta_sr_stats, "",)

summary_text = [
    f"Total merchants having atleast 1 attempts on Native OTP: {distinct_merchant_count}",
    f"Total merchants having atleast 30 attempts on Native OTP & 3ds: {eligible_merchant_count}"
]

# Add the summary section to the document
add_summary_bullet_points(doc, summary_text)

heading_text_3 = "Eligible Merchants Native OTP Performance"
heading = doc.add_heading(heading_text_3, level=3)
heading.runs[0].font.name = "Arial"
heading.runs[0].font.size = Pt(14)
add_dataframe_as_table(doc, eligible_merchant_summary, "",)

doc.add_picture('payu_mx_comparison.png') 
doc.add_picture('billdesk_mx_comparison.png')  
doc.add_picture('paytm_mx_comparison.png')  


# Save Word document to local file system with timestamp appended to the file name
current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
local_output_file_path = f"/tmp/output_{current_time}.docx"
# local_output_file_path = f"/tmp/Post_Pod_AWT_{current_time}.docx"  # Updated file name
doc.save(local_output_file_path)

# Move file to DBFS location
dbfs_output_file_path = f"/dbfs/FileStore/rohan/output_AWT.docx"
shutil.move(local_output_file_path, dbfs_output_file_path)
display(HTML(f'<a href="{dbfs_output_file_path}" download>Post Pod AWT_{current_time}</a>'))
#  instead of dbfs/FileStore , add files or file

# COMMAND ----------

late_auth_df_selected_1
#late_auth_df_selected_01
#print(gateway_late_auth.shape)
#print(gateway_late_auth_all.shape)
#gateway_late_auth_all

# COMMAND ----------

print(mtu_df2_selected.reset_index())
print(mtu_df3_selected.reset_index())

# COMMAND ----------

#late_auth_df_selected_30
#mtu_df2_selected.reset_index()
selected_df_10.sort_values(by='Delta SR', ascending=True)
#print(mtu_df2_selected.reset_index().columns)

# COMMAND ----------

#Gateway for which SR has increased
#selected_df_5

late_auth_df_selected_3.fillna(0,inplace = True)
late_auth_df_selected_3.sort_values(by="Delta",ascending=False)

# COMMAND ----------



# COMMAND ----------

# while True:
#     user_input = int(input("Enter a number (enter 0 to quit): "))
#     if user_input == 0:
#         print("Exiting the loop.")
#         break
#     else:

#         print(f"The square of {user_input} is: {user_input ** 2}")

# COMMAND ----------


