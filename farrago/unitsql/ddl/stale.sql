-- $Id$
-- Test for correct cache invalidation of plans on stale object definitions

create schema overstock;

set schema overstock;

create table tomato_sauce(sku bigint not null primary key);

insert into tomato_sauce values (20);

select sku from tomato_sauce;

select * from tomato_sauce;

drop table tomato_sauce;

create table tomato_sauce(
    sku int not null primary key,
    qty int default 5 not null);

select sku from tomato_sauce;

insert into tomato_sauce(sku) values (30);

select sku from tomato_sauce;

select * from tomato_sauce;

