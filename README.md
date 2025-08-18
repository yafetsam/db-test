# PostgreSQL Logical Replication with Go and sqlc

This project demonstrates setting up **PostgreSQL logical replication** between a master and a replica database using Docker Compose. It also shows how to interact with the database in **Go** using **sqlc** for type-safe queries.

---

## Features

- Docker Compose setup with two PostgreSQL instances:
  - `pg_master` (publisher)
  - `pg_replica` (subscriber)
- `orders` table in `testDB` database
- Logical replication from `pg_master` → `pg_replica`
- Go code to:
  - Insert orders into the master database
  - List orders from both master and replica
- Queries written in SQL and exposed in Go via **sqlc**

---

## Start PostgreSQL with Docker Compose
```bash
docker compose up -d
```

## Initialize the databases
```bash
make migrate-main
make migrate-replica
```

## Configure Logical Replication
### Note: You need to run the following commands inside the Postgres REPL (psql)
```bash
CREATE PUBLICATION orders_pub FOR TABLE orders;
```
```bash
CREATE SUBSCRIPTION orders_sub
CONNECTION 'host=pg_master port=5432 dbname=testDB user=postgres password=postgres'
PUBLICATION orders_pub;
```
