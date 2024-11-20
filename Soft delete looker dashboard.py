# Databricks notebook source
base_url=https://razorpay.cloud.looker.com
# API 3 client id TO BE ADDED BY THE USER
client_id= ''             
# API 3 client secret TO BE ADDED BY THE USER
client_secret=          
# Set to false if testing locally against self-signed certs. Otherwise, leave True
verify_ssl=True

# COMMAND ----------

import looker_sdk
from looker_sdk import models40

sdk = looker_sdk.init40()

def delete_dashboards(dashboard_ids):
    """Soft delete dashboards"""
    for dashboard_id in dashboard_ids:
        try:
            sdk.update_dashboard(dashboard_id, models40.WriteDashboard(deleted=False))
        except:
            print("Failed to delete dashboard with id {}".format(dashboard_id))
        else:
            print("Dashboard id {} has been moved to trash".format(dashboard_id))

# List of Dashboard IDs to be added in the below function call
delete_dashboards(['5541'])
