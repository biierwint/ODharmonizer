gunzip -c database-setup.sql.gz | docker exec -i odharmonizer-postgres psql -U postgres
gunzip -c odharmonizer_database_dump.sql.gz | docker exec -i odharmonizer-postgres psql -U postgres -d odmapper
