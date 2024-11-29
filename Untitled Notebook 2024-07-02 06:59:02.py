# Databricks notebook source
WITH bins AS (
    SELECT
        producer_created_date,
        payment_id,
        mode,
        iin
    FROM hive.events.events_payments_card_meta_v1
    WHERE iin IS NOT NULL
)

SELECT
    CASE WHEN optimizer_cvv_less_payments_consolidated.id = payments_optimizer_flag_sincejan2021.id THEN 'cvv less' ELSE 'non cvv less' END AS cvv_less_payments_flag,
    CASE
        WHEN FROM_UNIXTIME(payments_optimizer_flag_sincejan2021.created_at + 19800) < TIMESTAMP '2024-04-09' THEN 'Pre'
        WHEN FROM_UNIXTIME(payments_optimizer_flag_sincejan2021.created_at + 19800) >= TIMESTAMP '2024-04-10' THEN 'Post'
        ELSE 'Other'
    END AS time_period,
    payments_optimizer_flag_sincejan2021.network AS network,
    payments_optimizer_flag_sincejan2021.merchant_id AS merchant_id,
    merchants.billing_label AS merchants_billing_label,
    CASE
        WHEN payments_optimizer_flag_sincejan2021.internal_external_flag = 'internal' THEN 'razorpay'
        WHEN payments_optimizer_flag_sincejan2021.internal_external_flag IS NULL THEN NULL
        ELSE payments_optimizer_flag_sincejan2021.gateway
    END AS payments_optimizer_flag_sincejan2021_payment_aggregator,
    payments_optimizer_flag_sincejan2021.method_advanced AS payments_optimizer_flag_sincejan2021_method_drilled,
    IF(payments.auth_type IS NULL, '3ds', payments.auth_type) AS payments_auth_type,
    payments_optimizer_flag_sincejan2021.issuer AS payments_optimizer_flag_sincejan2021_issuer,
    bins.iin AS iin,
    COUNT(DISTINCT payments_optimizer_flag_sincejan2021.id) AS payments_optimizer_flag_sincejan2021_payments_total,
    ROUND(COUNT(DISTINCT CASE WHEN payments_optimizer_flag_sincejan2021.authorized_at IS NOT NULL THEN payments_optimizer_flag_sincejan2021.id END) * 1.000000 / COUNT(DISTINCT payments_optimizer_flag_sincejan2021.id), 4) AS payments_optimizer_flag_sincejan2021_success_rate_1,
    CASE
        WHEN COUNT(CASE WHEN payments_optimizer_flag_sincejan2021.authorized_at IS NOT NULL THEN payments_optimizer_flag_sincejan2021.id END) <= 2 THEN AVG(CASE WHEN payments_optimizer_flag_sincejan2021.authorized_at IS NOT NULL THEN payments_optimizer_flag_sincejan2021.authorized_at - payments_optimizer_flag_sincejan2021.created_at END)
        ELSE APPROX_PERCENTILE(CASE WHEN payments_optimizer_flag_sincejan2021.authorized_at IS NOT NULL THEN payments_optimizer_flag_sincejan2021.authorized_at - payments_optimizer_flag_sincejan2021.created_at END, 1, 0.5, 0.001)
    END AS median_of_time_to_authorize,
    COUNT(DISTINCT CASE WHEN payments_optimizer_flag_sincejan2021.authorized_at IS NOT NULL THEN payments_optimizer_flag_sincejan2021.id END) AS payments_optimizer_flag_sincejan2021_payments_success
FROM 
    aggregate_ba.payments_optimizer_flag_sincejan2021 AS payments_optimizer_flag_sincejan2021
INNER JOIN 
    realtime_hudi_api.merchants AS merchants ON payments_optimizer_flag_sincejan2021.merchant_id = merchants.id
LEFT JOIN 
    payment_analytics ON payments_optimizer_flag_sincejan2021.id = payment_analytics.payment_id
LEFT JOIN 
    realtime_hudi_api.payments AS payments ON payments_optimizer_flag_sincejan2021.id = payments.id
LEFT JOIN 
    analytics_selfserve.optimizer_cvv_less_payments_consolidated AS optimizer_cvv_less_payments_consolidated ON payments_optimizer_flag_sincejan2021.id = optimizer_cvv_less_payments_consolidated.id
LEFT JOIN 
    bins ON bins.payment_id = payments_optimizer_flag_sincejan2021.id
WHERE 
    FROM_UNIXTIME(payments_optimizer_flag_sincejan2021.created_at + 19800) >= DATE_ADD('month', -1, DATE_TRUNC('MONTH', CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP))) 
    AND FROM_UNIXTIME(payments_optimizer_flag_sincejan2021.created_at + 19800) < DATE_ADD('month', 1, DATE_ADD('month', -1, DATE_TRUNC('MONTH', CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP))))
    AND payments_optimizer_flag_sincejan2021.merchant_id = 'JCswjQPtZfw2i3' 
    AND payments_optimizer_flag_sincejan2021.method LIKE '%card%' 
    AND payments_optimizer_flag_sincejan2021.optimizer_flag = 1 
    AND DATE(payments_optimizer_flag_sincejan2021.golive_date) IS NOT NULL 
    AND payments_optimizer_flag_sincejan2021.network_tokenised_payment = '1' 
    AND payments_optimizer_flag_sincejan2021.network = 'Visa' 
    AND DATE(payment_analytics.created_date) >= DATE_ADD('month', -1, DATE_TRUNC('MONTH', CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP))) 
    AND DATE(payment_analytics.created_date) < DATE_ADD('month', 1, DATE_ADD('month', -1, DATE_TRUNC('MONTH', CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP))))
    AND payments_optimizer_flag_sincejan2021.gateway IS NOT NULL
GROUP BY 
    cvv_less_payments_flag,
    time_period,
    payments_optimizer_flag_sincejan2021.network,
    payments_optimizer_flag_sincejan2021.merchant_id,
    merchants.billing_label,
    payments_optimizer_flag_sincejan2021_payment_aggregator,
    payments_optimizer_flag_sincejan2021_method_drilled,
    payments_auth_type,
    payments_optimizer_flag_sincejan2021_issuer,
    bins.iin;
This corrected query assumes that bins is supposed to be joined with other tables like payments_optimizer_flag_sincejan2021 using LEFT JOIN. Adjust the join condition (bins.payment_id = ...) as per your data model. Also, ensure all column names and conditions fit your specific database schema and requirements.



Don't share sensitive info. Chats may be reviewed and used to train our models.
Learn more


Message ChatGPT

ChatGPT can make mistakes. Check important info.
?

