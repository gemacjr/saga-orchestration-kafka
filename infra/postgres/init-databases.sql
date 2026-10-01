-- Local only: one Postgres instance, one database + owner per service (database-per-service).
-- In prod each service gets its own RDS instance/cluster or at least its own database and credentials.
create user order_svc with password 'order_pw';
create database orders owner order_svc;
create user payment_svc with password 'payment_pw';
create database payments owner payment_svc;
create user inventory_svc with password 'inventory_pw';
create database inventory owner inventory_svc;
create user saga_svc with password 'saga_pw';
create database sagas owner saga_svc;
