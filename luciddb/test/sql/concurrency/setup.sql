-- create a single-row table for use in various locking scenarios

create schema concurrency;

create table concurrency.test(i int);
insert into concurrency.test values(42);

create table concurrency.t1(c integer);
create table concurrency.t2(c integer);


create schema concurrency2;
create table concurrency2.t1(c integer);
create table concurrency2.t2(c integer);