create table if not exists `integration_test`.`order`
(
    `id`          bigint auto_increment
        primary key,
    `using_col1`  varchar(128) null,
    `using_col2`  varchar(128) null
);

create table if not exists `integration_test`.`order_detail`
(
    `order_id`          bigint auto_increment
        primary key,
    `item_id`  bigint null,
    `using_col1`  varchar(128) null,
    `using_col2`  varchar(128) null
);

create table if not exists `integration_test`.`item`
(
    `id`          bigint auto_increment
        primary key
);
