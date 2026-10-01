create table product_stock
(
    product_id varchar(100) primary key,
    available  int not null check (available >= 0)
);

create table reservations
(
    id         uuid primary key,
    saga_id    uuid         not null unique,
    order_id   uuid         not null,
    product_id varchar(100) not null,
    quantity   int          not null,
    status     varchar(20)  not null,
    reason     varchar(500),
    version    bigint       not null,
    created_at timestamptz  not null,
    updated_at timestamptz  not null
);

create index ix_reservations_created_at on reservations (created_at desc);

insert into product_stock (product_id, available)
values ('product-100', 1000),
       ('product-200', 1000),
       ('product-300', 1000);
