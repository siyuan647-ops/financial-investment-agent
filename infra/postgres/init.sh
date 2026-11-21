#!/bin/sh
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  --set=app_password="$APP_DB_PASSWORD" --set=prefect_password="$PREFECT_DB_PASSWORD" <<'SQL'
CREATE ROLE a_share LOGIN PASSWORD :'app_password';
CREATE ROLE prefect LOGIN PASSWORD :'prefect_password';
CREATE DATABASE a_share OWNER a_share;
CREATE DATABASE prefect OWNER prefect TEMPLATE template0;
REVOKE CONNECT ON DATABASE a_share FROM PUBLIC;
REVOKE CONNECT ON DATABASE prefect FROM PUBLIC;
GRANT CONNECT ON DATABASE a_share TO a_share;
GRANT CONNECT ON DATABASE prefect TO prefect;
\connect a_share
CREATE EXTENSION IF NOT EXISTS timescaledb;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO a_share;
SQL

