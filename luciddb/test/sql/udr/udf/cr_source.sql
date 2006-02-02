-- $Id$
create schema udftest;

set schema 'udftest';

set path 'udftest';

create table customers(
fname varchar(20), 
lname varchar(20), 
age integer, 
sex char(1),
phone varchar(25))
server sys_column_store_data_server;

insert into customers values
('Mark', 'Wyatt', 34, 'M', '1234567890'),
('Mary', 'O Brian', 12, 'F', '234-456-7843'),
('Gregory', 'Packery', 55, 'M', '(342) 234-2355'),
('Ephram', 'Vestrit', 62, 'M', '(321)3454321'),
('Lilah', 'Lowe', 27, 'F', '   234    412   344  2'),
('Dirk the 3rd', 'Treethorn', 39, 'M', '8622399175'),
('2Tito1', '', 17, 'F', '888 888 8888');

select * from customers
order by 1;

create table data_source(
fm integer,
timecol time,
datecol date,
tscol timestamp)
server sys_column_store_data_server;

insert into data_source values
(2, TIME'12:50:31', DATE'2006-1-29', TIMESTAMP'2001-6-11 12:54:01.1'),
(1, TIME'6:00:59.99', DATE'1845-09-25', TIMESTAMP'2010-11-11 01:18:21.7'),
(10, TIME'5:15:15', DATE'1993-3-21', TIMESTAMP'2164-10-18 7:17:12');

select * from data_source
order by 1;

create table inttable(
colname varchar(10),
coltiny tinyint,
colsmall smallint,
colint integer,
colbig bigint);

insert into inttable values
('r1', 127, 32767, 2147483647, 4294967296),
('r2', 0, -32767, -2147483647, -4294967296),
('r3', -127, 0, 45678921, 12121212121212);


select * from inttable
order by 1;