"""Alerts: Airflow calls these functions by itself (callbacks) when a task retries or fails.

Every message is printed in the task log and added to data/alerts.log.
If SLACK_WEBHOOK_URL is set in .env, the message is also sent to Slack.
"""
import os
from datetime import datetime
from pathlib import Path

import requests

ALERT_FILE = Path(__file__).resolve().parents[1] / "data" / "alerts.log"


def write_alert(message):
    """Print the message, add it to data/alerts.log, and send it to Slack if a webhook is set."""
    print(message)

    # "a" = append: each new alert is added at the end, old alerts are kept (a history)
    with open(ALERT_FILE, "a") as file:
        file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " " + message + "\n")

    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if webhook:
        try:
            requests.post(webhook, json={"text": message}, timeout=10)
        except requests.RequestException as error:
            print("Could not send the Slack message:", error)  # the alert is still in alerts.log
    else:
        print("No SLACK_WEBHOOK_URL in .env - the alert is only in data/alerts.log")


def notify_retry(context):
    """Called by Airflow when a task failed but will try again."""
    task = context["task_instance"]
    message = ("RETRY: task " + task.task_id + " failed and will try again. Error: "
               + str(context.get("exception")))
    write_alert(message)


def alert_on_failure(context):
    """Called by Airflow once, when a task failed after all its retries."""
    task = context["task_instance"]
    message = ("ALERT: task " + task.task_id + " in DAG " + task.dag_id
               + " failed after all retries. Error: " + str(context.get("exception"))
               + " | Logs: " + task.log_url)
    write_alert(message)
