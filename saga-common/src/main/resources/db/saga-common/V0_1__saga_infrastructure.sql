-- Transactional outbox: rows are written in the same DB transaction as the business change
-- and relayed to Kafka asynchronously by OutboxRelay.
create table outbox_message
(
    id           uuid primary key,
    seq          bigserial    not null,
    aggregate_id uuid         not null,
    topic        varchar(255) not null,
    message_key  varchar(255) not null,
    message_type varchar(255) not null,
    payload      jsonb        not null,
    created_at   timestamptz  not null default now(),
    published_at timestamptz
);

create index ix_outbox_unpublished on outbox_message (seq) where published_at is null;
create index ix_outbox_published_at on outbox_message (published_at) where published_at is not null;

-- Idempotent inbox: one row per (consumer, message id) already handled.
create table processed_message
(
    consumer     varchar(100) not null,
    message_id   varchar(64)  not null,
    processed_at timestamptz  not null default now(),
    primary key (consumer, message_id)
);
