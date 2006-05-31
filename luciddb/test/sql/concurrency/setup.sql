-- create a single-row table for use in various locking scenarios

create schema concurrency;

create table concurrency.test(i int);
insert into concurrency.test values(42);
