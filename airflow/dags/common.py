"""
common.py — Shared DAG configuration for the stock-market pipeline.

Import default_args into every DAG so alert settings stay consistent
across the project.
"""

from datetime import timedelta

ALERT_EMAIL = "charlesisaac266@gmail.com"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    # Send an email when a task fails or retries after all retries are exhausted.
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
}

# Lighter settings for the backfill DAG — fewer retries, still alerts on failure.
backfill_default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
}
