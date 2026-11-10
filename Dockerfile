FROM apache/airflow:3.4.0-python3.12

# dbt lives in its own virtualenv so its dependencies never conflict with Airflow's.
USER root
RUN python -m venv /opt/dbt_venv \
    && /opt/dbt_venv/bin/pip install --no-cache-dir dbt-postgres==1.11.0 \
    && chown -R airflow:0 /opt/dbt_venv
USER airflow

ENV PYTHONPATH=/opt/airflow/src \
    DBT_BIN=/opt/dbt_venv/bin/dbt \
    DBT_PROJECT_DIR=/opt/airflow/dbt
