#!/bin/bash
set -e

# Create separate databases for each test project to avoid conflicts
# when running tests in parallel.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE cloudfabric_es_test_pg;
    CREATE DATABASE cloudfabric_es_test_es;
    CREATE DATABASE cloudfabric_es_test_os;
EOSQL
