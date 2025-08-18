.PHONY: migrate-main
migrate-main:
	cat db/schema.sql | docker exec -i pg_master psql -U postgres -d testDB

.PHONY: migrate-replica
migrate-replica:
	cat db/schema.sql | docker exec -i pg_replica psql -U postgres -d testDB
