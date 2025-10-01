import os
from airflow.operators.email import EmailOperator
from html import escape

DBT_LOG_PATH = os.getenv("DBT_LOG_PATH")

def send_failure_email(context):
    """
    Sends an email alert on task failure and attaches the tail of the dbt log file if available.
    """
    ti = context["task_instance"]
    dag_run = context["dag_run"]
    
    dbt_log_content = ""
    try:
        # Try to read the last 100 lines of the dbt log file
        with open(DBT_LOG_PATH, "r") as f:
            lines = f.readlines()
            last_100_lines = lines[-100:]
            # Escape HTML to prevent rendering issues in the email client
            dbt_log_content = escape("".join(last_100_lines))
    except FileNotFoundError:
        dbt_log_content = "dbt.log file not found."
    except Exception as e:
        dbt_log_content = f"Error reading dbt.log file: {e}"

    subject = f"Airflow Task Failed: {ti.task_id}"
    html_content = f"""
    <h3>Task Failed</h3>
    <p><b>Task:</b> {ti.task_id}</p>
    <p><b>DAG:</b> {dag_run.dag_id}</p>
    <p><b>Execution Time:</b> {dag_run.execution_date}</p>
    <p><b>Log URL:</b> <a href="{ti.log_url}">View Full Airflow Log</a></p>
    <hr>
    <h3>dbt Log Tail (Last 100 Lines):</h3>
    <pre><code>{dbt_log_content}</code></pre>
    """
    
    email_alert = EmailOperator(
        task_id="send_failure_email_alert",
        to=os.getenv('YOUR_EMAIL'), # Make sure this environment variable is set
        subject=subject,
        html_content=html_content,
    )
    email_alert.execute(context=context)
