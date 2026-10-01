create table saga_instance
(
    saga_id        uuid primary key,
    order_id       uuid           not null unique,
    product_id     varchar(100)   not null,
    quantity       int            not null,
    amount         numeric(19, 2) not null,
    status         varchar(30)    not null,
    payment_id     uuid,
    reservation_id uuid,
    failure_reason varchar(500),
    compensating   boolean        not null default false,
    step_timeouts  int            not null default 0,
    version        bigint         not null,
    created_at     timestamptz    not null,
    updated_at     timestamptz    not null
);

-- Supports the timeout watcher scanning for stuck, non-terminal sagas.
create index ix_saga_active_updated on saga_instance (updated_at)
    where status not in ('COMPLETED', 'COMPENSATED', 'FAILED');
create index ix_saga_created_at on saga_instance (created_at desc);
