FROM quay.io/astronomer/astro-runtime:13.2.0

# Switch to the root user for privileged operations.
USER root

# Create a virtual environment for dbt to avoid dependency conflicts with Airflow.
RUN python -m venv dbt_venv

# Install dbt and your database adapter inside the virtual environment.
RUN /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-postgres==1.9.0

# Create the target dbt directory
RUN mkdir -p /usr/local/airflow/dbt

# --- Optimized Dependency Installation ---
# Copy only the files needed to install dependencies
COPY dbt/packages.yml /usr/local/airflow/dbt/packages.yml
COPY dbt/dbt_project.yml /usr/local/airflow/dbt/dbt_project.yml

# Install dbt packages. This layer is only rebuilt when packages.yml changes.
RUN /usr/local/airflow/dbt_venv/bin/dbt deps --project-dir /usr/local/airflow/dbt

# --- Copy and Parse the Full Project ---
# Now copy the rest of the dbt project files
COPY dbt /usr/local/airflow/dbt

# Parse the project to generate manifest.json without a database connection.
RUN /usr/local/airflow/dbt_venv/bin/dbt parse \
    --project-dir /usr/local/airflow/dbt \
    --profiles-dir /usr/local/airflow/dbt \
    --target ci

# Switch back to the default Airflow user for running the application.
USER astro
