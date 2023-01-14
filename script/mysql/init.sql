create database if not exists `integration_test`;
create table if not exists `integration_test`.`simple_struct`
(
    `id` bigint auto_increment,
    bool smallint not null,
    bool_ptr smallint,
    `int` int not null,
    int_ptr int,
    `int8` smallint not null,
    int8_ptr smallint,
    int16 int not null,
    int16_ptr int,
    int32 int not null,
    int32_ptr int,
    int64 bigint not null,
    int64_ptr bigint,
    uint int not null,
    uint_ptr int,
    uint8 int not null,
    uint8_ptr int,
    uint16 int not null,
    uint16_ptr int,
    uint32 int not null,
    uint32_ptr int,
    uint64 bigint not null,
    uint64_ptr bigint,
    float32 float not null,
    float32_ptr float,
    float64 float not null,
    float64_ptr float,
    byte_array varchar(1024),
    string varchar(1024) not null,
    null_string_ptr varchar(1024),
    null_int16_ptr int,
    null_int32_ptr int,
    null_int64_ptr int,
    null_bool_ptr smallint,
    null_time_ptr datetime,
    null_float64_ptr float,
    json_column varchar(2048),
    primary key (`id`)
);

create table if not exists `integration_test`.`combined_model`
(
    `id`          bigint auto_increment
    primary key,
    `first_name`  varchar(128) null,
    `age`         int          null,
    `last_name`   varchar(128) null,
    `create_time` bigint       null,
    `update_time` bigint       null
    );
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
