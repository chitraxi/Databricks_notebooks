# Databricks notebook source
# MAGIC %md
# MAGIC Base 

# COMMAND ----------

# MAGIC %pip install --upgrade jinja2
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import jinja2
from datetime import datetime, timedelta

# COMMAND ----------

permenant_downtimes_base = spark.sql(f"""
 
          select downtime_id,method,terminal_id, from_unixtime(min(begin_time)) as downtime_start_time,max(case when event_rank = 1 then status end) latest_status,count(status) total_state_change,max(downtimes_count) max_downtime_count  from 
(select event_name, 
event_timestamp,
 properties,
get_json_object(properties,'$.status') status,
get_json_object(properties,'$.downtime_entities[0].method') method,
cast(get_json_object(properties,'$.downtime_entities[0].begin') as int) begin_time,
cast(get_json_object(properties,'$.downtime_entities[0].end') as int) end_time,
get_json_object(properties,'$.downtime_entities[0].id') downtime_id,
cast (get_json_object(properties,'$.downtime_entities[0].info.downtimes_count') as int) downtimes_count,
cast (get_json_object(properties,'$.downtime_entities[0].info.sr') as int) downtime_sr,
get_json_object(properties,'$.downtime_entities[0].merchant_id') merchant_id,
get_json_object(properties,'$.downtime_entities[0].instrument.terminal_id') terminal_id,
 rank() over (partition by get_json_object(properties,'$.downtime_entities[0].id') order by event_timestamp desc) as event_rank,
-- Adding downtime begin time and end time for various downtimes
cast(from_unixtime(min(event_timestamp)
              over
              (partition by get_json_object(properties,'$.downtime_entities[0].id'), try_cast
               (get_json_object(properties,'$.downtime_entities[0].info.downtimes_count') as int) order by
               event_timestamp)+19800) as varchar(200))  as downtime_stage_begin_time,

cast(from_unixtime(lead(event_timestamp)
              over
              (partition by get_json_object(properties,'$.downtime_entities[0].id') order by event_timestamp)+19800)                as varchar(200))
              as downtime_end_time,
created_date
from events.events_downtime_v2
where created_date >= try_cast(current_date + interval '-7' day as varchar(30)))
group by 1,2,3  
--order by 7 desc
having case when max(downtimes_count) >= 5 
and  max(case when event_rank = 1 then status end) != 'resolved' then true end order by 3 desc 
                    """)

# COMMAND ----------

downtime_pd = permenant_downtimes_base.toPandas()

# COMMAND ----------

merchant_base = spark.sql(f""" select id, coalesce(billing_label, name ) as merchant_name from realtime_hudi_api.merchants where id in (select entity_id from realtime_hudi_api.features where name = 'raas' and _is_row_deleted is null) """)

# Register merchant_base as a temporary view to use in the next query
merchant_base.createOrReplaceTempView("merchant_base")

# COMMAND ----------

merchant_name = merchant_base.toPandas()

# COMMAND ----------

terminal_base = spark.sql(f""" select distinct terminal_id,gateway,merchant_id from realtime_terminalslive.terminals where merchant_id in (select id from merchant_base) and `_is_row_deleted` is  null """)

# COMMAND ----------

terminal_base_pd = terminal_base.toPandas()

# COMMAND ----------

terminal_base_pd

# COMMAND ----------

downtime_with_tid = pd.merge(terminal_base_pd, downtime_pd, on='terminal_id', how='left')


# COMMAND ----------

downtime_details = pd.merge(downtime_with_tid,merchant_name,left_on='merchant_id',right_on='id',how='left')

# COMMAND ----------

downtime_details

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# Assuming you have the 'cards_dod' DataFrame ready

if not downtime_details.empty:

  # Convert DataFrame to HTML table
  html_table = downtime_details.to_html(index=False, border=1, classes='styled-table')

  # Email details
  sender_email = "chitraxi.raj@razorpay.com"
  receiver_email = "chitraxi.raj@razorpay.com"
  cc_email = ['chitraxi.raj@razorpay.com']  # Add your CC email addresses here
  subject = "Permenant downtime alerts"

  # Create email message
  message = MIMEMultipart("alternative")
  message["To"] = receiver_email  # No need to convert to string for a single recipient
  message["Cc"] = ', '.join(cc_email)
  message["Subject"] = subject

  # Create HTML email content with CSS styling
  html_content = f"""
  <html>
    <head>
      <style>
        .styled-table {{
          border-collapse: collapse;
          width: 100%;
        }}
        .styled-table th, .styled-table td {{
          border: 1px solid black;
          padding: 8px;
          text-align: center;
          white-space: nowrap;
          ;
        }}
        .styled-table th {{
          font-weight: bold;
        }}
      </style>
    </head>
    <body>
      <p>Hello team,<br><br>
        Please find below the details on permenant downtimes created in past 2 days <br><br>
        {html_table}<br><br>
        Kindly check these downtimes.
        <br><br>
        Kind Regards,<br>
        Chitra<br>
      </p>
    </body>
  </html>
  """

  # Attach HTML content to email
  email_body = MIMEText(html_content, "html")
  message.attach(email_body)

  # Connect to SMTP server and send email
  with smtplib.SMTP('smtp.gmail.com', 587) as server:
      server.starttls()
      server.login("chitraxi.raj@razorpay.com", "lzus crbk nefm tsdd")
      server.sendmail(sender_email, receiver_email, message.as_string())

  print("Email sent successfully!")

else:
    print("DataFrame is empty. Email not sent.")

# COMMAND ----------

while True:
    user_input = int(input("Enter a number (enter 0 to quit): "))
    if user_input == 0:
        print("Exiting the loop.")
        break
    else:
        print(f"The square of {user_input} is: {user_input ** 2}")

# COMMAND ----------


