#!/bin/bash
# Creates the warehouse role and database next to the Airflow metadata database.
# Runs once, the first time the Postgres volume is initialised.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
    CREATE ROLE ${WAREHOUSE_USER} LOGIN PASSWORD '${WAREHOUSE_PASSWORD}';
    CREATE DATABASE ${WAREHOUSE_DB} OWNER ${WAREHOUSE_USER};
SQL
