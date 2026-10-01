create table orders
(
    id              uuid primary key,
    saga_id         uuid           not null unique,
    product_id      varchar(100)   not null,
    quantity        int            not null check (quantity > 0),
    amount          numeric(19, 2) not null check (amount > 0),
    status          varchar(20)    not null,
    failure_reason  varchar(500),
    idempotency_key varchar(100) unique,
    version         bigint         not null,
    created_at      timestamptz    not null,
    updated_at      timestamptz    not null
);

create index ix_orders_created_at on orders (created_at desc);
