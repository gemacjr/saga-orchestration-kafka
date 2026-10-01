create table payments
(
    id         uuid primary key,
    saga_id    uuid        not null unique,
    order_id   uuid        not null,
    amount     numeric(19, 2),
    status     varchar(20) not null,
    reason     varchar(500),
    version    bigint      not null,
    created_at timestamptz not null,
    updated_at timestamptz not null
);

create index ix_payments_created_at on payments (created_at desc);
