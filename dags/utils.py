import os
from airflow.operators.email import EmailOperator

def send_failure_email(context):
    """
    Sends an email alert on task failure.
    """
    ti = context["task_instance"]
    dag_run = context["dag_run"]
    
    subject = f"Airflow Task Failed: {ti.task_id}"
    html_content = f"""
    <h3>Task Failed</h3>
    <p><b>Task:</b> {ti.task_id}</p>
    <p><b>DAG:</b> {dag_run.dag_id}</p>
    <p><b>Execution Time:</b> {dag_run.execution_date}</p>
    <p><b>Log URL:</b> <a href="{ti.log_url}">View Log</a></p>
    """
    
    email_alert = EmailOperator(
        task_id="send_failure_email_alert",
        to=os.getenv('YOUR_EMAIL'),
        subject=subject,
        html_content=html_content,
    )
    email_alert.execute(context=context)
