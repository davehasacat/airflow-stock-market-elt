FROM quay.io/astronomer/astro-runtime:10.9.0

# Switch to the root user for privileged operations.
USER root

# Create a virtual environment for dbt to avoid dependency conflicts with Airflow.
RUN python -m venv dbt_venv

# Install dbt and your database adapter inside the virtual environment.
# Replace dbt-postgres with your specific adapter (e.g., dbt-snowflake, dbt-bigquery).
RUN /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-postgres==1.8.2

# Copy your local dbt project into the container.
# The Astro CLI automatically mounts project files, but this ensures they are available
# inside the Docker image for local development.
# This copies everything from your local 'dbt' folder into the container's '/usr/local/airflow/dbt' folder.
COPY dbt /usr/local/airflow/dbt

# Switch back to the default Airflow user for running the application.
# This is a security best practice.
USER astro
