# pg2parquet

A simple, straightforward code to do a CDC and dump in a parquet

## Design

The main idea of this project is to create a very concise code, without unnecessary branches, to do just one thing.

Since there is so few parameters, I didn't bother to create a config file, just parameters.

This code is tested only in postgres 15.

## Compiling

Assuming a fedora with pgdg:

```
sudo dnf install postgresql16-devel
PATH=/usr/pgsql-16/bin:${PATH} cargo build

```

## Using

The project opted to not create postgres infra, you'll need to do it yourself:

```
psql -U postgres database
CREATE PUBLICATION pg2parquet FOR ALL TABLES;
SELECT pg_create_logical_replication_slot('pg2parquet', 'pgoutput');
\q
export PG2PARQUET_PUBLICATION=pg2parquet
export PG2PARQUET_SLOT=pg2parquet
export PG2PARQUET_OUTPUT=output_directory
export PG2PARQUET_CONNINFO='host=127.0.0.1 port=5433 user=postgres password=postgres replication=database'
pg2parquet
```
