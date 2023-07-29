#!/bin/bash
set -e

cat >> "$PGDATA/postgresql.conf" <<-EOCONF
wal_level=logical
EOCONF
