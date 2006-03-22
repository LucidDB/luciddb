-- Setup the data for the inline tests

set schema 's';

drop table bill;
create table bill(n numeric(9,0));

insert into bill(n) values(null);
insert into bill(n) values(10);
insert into bill(n) values(100);
insert into bill(n) values(1000);
insert into bill(n) values(10000);
insert into bill(n) values(100000);
insert into bill(n) values(1000000);
insert into bill(n) values(10000000);
insert into bill(n) values(100000000);
insert into bill(n) values(-10);
insert into bill(n) values(-100);
insert into bill(n) values(-1000);
insert into bill(n) values(-10000);
insert into bill(n) values(-100000);
insert into bill(n) values(-1000000);
insert into bill(n) values(-10000000);
insert into bill(n) values(-100000000);

drop table bill2;
create table bill2(n numeric(15,0));

insert into bill2(n) values(null);
insert into bill2(n) values(10);
insert into bill2(n) values(100);
insert into bill2(n) values(1000);
insert into bill2(n) values(10000);
insert into bill2(n) values(100000);
insert into bill2(n) values(1000000);
insert into bill2(n) values(10000000);
insert into bill2(n) values(100000000);
insert into bill2(n) values(1000000000);
insert into bill2(n) values(10000000000);
insert into bill2(n) values(100000000000);
insert into bill2(n) values(1000000000000);
insert into bill2(n) values(10000000000000);
insert into bill2(n) values(100000000000000);
insert into bill2(n) values(-10);
insert into bill2(n) values(-100);
insert into bill2(n) values(-1000);
insert into bill2(n) values(-10000);
insert into bill2(n) values(-100000);
insert into bill2(n) values(-1000000);
insert into bill2(n) values(-10000000);
insert into bill2(n) values(-100000000);
insert into bill2(n) values(-1000000000);
insert into bill2(n) values(-10000000000);
insert into bill2(n) values(-100000000000);
insert into bill2(n) values(-1000000000000);
insert into bill2(n) values(-10000000000000);
insert into bill2(n) values(-100000000000000);

