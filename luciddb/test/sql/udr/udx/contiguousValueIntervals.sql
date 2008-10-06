-- tests for ContiguousValueIntervals UDX
create schema cvi;
set schema 'cvi';

create table OptyHist(
  id varchar(20),
  changeTimestamp timestamp,
  stage varchar(50),
  amount decimal(10,2),
  expectedRevenue decimal(10,2),
  probability int,
  isDeleted boolean);

-- empty table
select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage  from OptyHist
        order by id, changeTimestamp),
    row(id),
    row(changeTimestamp)));

-- table with 1 row
insert into OptyHist values
  ('AA100', timestamp'2007-10-14 00:00:00', '0 - Prospect', 150, 100.0, 10,
   false);

select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage  from OptyHist
        order by id, changeTimestamp),
    row(id),
    row(changeTimestamp)));

-- table with one partition and one clump
insert into OptyHist values
  ('AA100', timestamp'2007-10-14 01:00:00', '0 - Prospect', 150, 100.0, 10,
   false);

select * from OptyHist order by id, changeTimestamp;

select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage  from OptyHist
        order by id, changeTimestamp),
    row(id),
    row(changeTimestamp)));

-- table with one partition and multiple clumps
delete from OptyHist where changeTimestamp = timestamp'2007-10-14 01:00:00';
insert into OptyHist values
  ('AA100', timestamp'2007-10-14 01:00:00', '1 - Qualified', 150, 100.0, 10,
   false);

select * from OptyHist order by id, changeTimestamp;

select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage  from OptyHist
        order by id, changeTimestamp),
    row(id),
    row(changeTimestamp)));

insert into OptyHist values
  ('AA100', timestamp'2007-10-14 02:00:00', '2 - Mtg Schd / NDA Signed', 150, 100.0, 10,
   false);

select * from OptyHist order by id, changeTimestamp;

select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage  from OptyHist
        order by id, changeTimestamp),
    row(id),
    row(changeTimestamp)));

insert into OptyHist values
  ('AA100', timestamp'2007-10-14 00:30:00', '0 - Prospect', 150, 100.0, 10,
   false);

select * from OptyHist order by id, changeTimestamp;

select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage  from OptyHist
        order by id, changeTimestamp),
    row(id),
    row(changeTimestamp)));

insert into OptyHist values
  ('AA100', timestamp'2007-10-14 01:30:00', '1 - Qualified', 150, 100.0, 10,
   false);

select * from OptyHist order by id, changeTimestamp;

select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage  from OptyHist
        order by id, changeTimestamp),
    row(id),
    row(changeTimestamp)));

insert into OptyHist values
  ('AA100', timestamp'2007-10-14 02:30:00', '2 - Mtg Schd / NDA Signed', 150, 100.0, 10,
   false);

select * from OptyHist order by id, changeTimestamp;

select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage  from OptyHist
        order by id, changeTimestamp),
    row(id),
    row(changeTimestamp)));

-- table with multiple partitions each with one clump
truncate table OptyHist;
insert into OptyHist values
  ('AA100', timestamp'2007-10-14 00:00:00', '0 - Prospect', 150, 100.0, 10,
   false),
  ('BB100', timestamp'2007-09-09 05:00:23', '0 - Prospect', 10, 90, 40, true),
  ('DD400', timestamp'2007-12-04 16:56:09', '1 - Qualified', 10, 15, 99, false);

select * from OptyHist order by id, changeTimestamp;

select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage  from OptyHist
        order by id, changeTimestamp),
    row(id),
    row(changeTimestamp)));

-- basic table
truncate table OptyHist;
insert into OptyHist values
  ('AA100', timestamp'2007-10-14 00:00:00', '0 - Prospect', 150, 100.0, 10,
   false),
  ('AA100', timestamp'2007-12-01 09:00:00', '1 - Qualified', 150.0, 200, 20,
   false),
  ('AA100', timestamp'2007-12-29 11:45:00', '2 - Mtg Schd / NDA Signed', 
   150.05, 100, 20, false),
  ('BB100', timestamp'2007-09-09 05:00:23', '0 - Prospect', 10, 90, 40, true),
  ('BB200', timestamp'2008-01-01 07:27:00', '1 - Qualified', 900.99, 2000, 20,
   false),
  ('DD400', timestamp'2007-12-04 16:56:09', '0 - Prospect', 10, 15, 99, false),
  ('DD400', timestamp'2007-12-05 00:00:00', '1 - Qualified', 10, 15, 80,
   false),
  ('DD400', timestamp'2007-12-05 00:00:04', '2 - Mtg Schd / NDA Signed', 15,
   15, 90, false),
  ('DD400', timestamp'2007-12-05 00:02:00', '2 - Mtg Schd / NDA Signed', 20,
   20, 95, true),
  ('AA100', timestamp'2007-10-12 12:23:41', '0 - Prospect', 20,
   20, 95, false),
  ('AA100', timestamp'2007-10-13 00:00:00', '0 - Prospect', 20,
   20, 95, false),
  ('AA100', timestamp'2007-12-01 15:00:00', '0 - Prospect', 20,
   20, 95, true);

select * from OptyHist order by id, changeTimestamp;

select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage  from OptyHist
        order by id, changeTimestamp),
    row(id),
    row(changeTimestamp)));

-- multi-column key

select * from OptyHist order by id, isDeleted, changeTimestamp;

select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage, isDeleted  from OptyHist
        order by id, isDeleted, changeTimestamp),
    row(id, isDeleted),
    row(changeTimestamp)));

-- with empty strings and spaces
insert into OptyHist values
  ('  ', timestamp'1999-10-10 04:29:09', '0 - Prospect', 34229.8, 67, 8,
   false),
  ('', timestamp'2020-01-01 00:59:59', '1 - Qualified', 10, 67, 9, true),
  ('AA100  ', timestamp'2007-12-29 22:45:00', '3 - Data received / Analyzed',
   150.00, 30000, 90, true),
  ('BB100', timestamp'2007-09-09 05:00:21', '', 20, 90, 50, null),
  ('DD400', timestamp'2007-12-05 00:01:02', '2 - Mtg Schd / NDA Signed     ',
   755.05, 20, 99, true);

select * from OptyHist order by id, changeTimestamp;

select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage  from OptyHist
        order by id, changeTimestamp),
    row(id),
    row(changeTimestamp)));

-- with nulls
truncate table OptyHist;
insert into OptyHist values
  (null, timestamp'2007-10-14 00:00:00', null, 150, 100.0, 10,
   false),
  (null, timestamp'2007-12-01 09:00:00', null, 150.0, 200, 20,
   false);

select * from OptyHist order by id, changeTimestamp;
  
select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage  from OptyHist
        order by id, changeTimestamp),
    row(id),
    row(changeTimestamp)));

truncate table OptyHist;
insert into OptyHist values
  (null, timestamp'2007-10-14 00:00:00', null, 150, 100.0, 10,
   false),
  (null, timestamp'2007-12-01 09:00:00', null, 150.0, 200, 20,
   false),
  (null, timestamp'2007-12-29 11:45:00', '0 - Prospect', 150.05, 100, 20, false),
  ('BB100', timestamp'2007-09-09 05:00:23', null, 10, 90, 40, true),
  ('BB200', timestamp'2008-01-01 07:27:00', '1 - Qualified', 900.99, 2000, 20,
   false),
  ('DD400', timestamp'2007-12-04 16:56:09', '0 - Prospect', 10, 15, 99, false),
  ('DD400', timestamp'2007-12-05 00:00:00', '1 - Qualified', 10, 15, 80,
   false),
  ('DD400', timestamp'2007-12-05 00:00:04', '2 - Mtg Schd / NDA Signed', 15,
   15, 90, false),
  ('DD400', timestamp'2007-12-05 00:02:00', null, 20, 20, 95, true),
  ('DD400', timestamp'2007-12-05 00:03:00', '2 - Mtg Schd / NDA Signed', 20, 20, 95, true),
  ('DD400', timestamp'2007-12-05 00:04:00', null, 20, 20, 95, true),
  ('DD400', timestamp'2007-12-05 00:05:00', null, 20, 20, 95, true),
  ('DD400', timestamp'2007-12-05 00:06:00', '2 - Mtg Schd / NDA Signed', 20, 20, 95, true),
  ('DD400', timestamp'2007-12-05 00:07:00', null, 20, 20, 95, true),
  ('DD400', timestamp'2007-12-05 00:08:00', null, 20, 20, 95, true),
  ('DD400', timestamp'2007-12-05 00:09:00', null, 20, 20, 95, true),
  ('DD400', timestamp'2007-12-05 00:10:00', '2 - Mtg Schd / NDA Signed', 20, 20, 95, true),
  ('AA100', timestamp'2007-10-12 12:23:41', '0 - Prospect', 20,
   20, 95, false),
  ('AA100', timestamp'2007-10-13 00:00:00', '0 - Prospect', 20,
   20, 95, false),
  ('AA100', timestamp'2007-12-01 15:00:00', '0 - Prospect', 20,
   20, 95, true);

select * from OptyHist order by id, changeTimestamp;
  
select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage  from OptyHist
        order by id, changeTimestamp),
    row(id),
    row(changeTimestamp)));

-- same timestamp same clumping values
truncate table OptyHist;
insert into OptyHist values
  ('AA100', timestamp'2007-10-14 00:00:00', '0 - Prospect', 150, 100.0, 10,
   false),
  ('AA100', timestamp'2007-12-01 09:00:00', '1 - Qualified', 150.0, 200, 20,
   false),
  ('AA100', timestamp'2007-12-01 09:00:00', '1 - Qualified', 150.0, 200, 20,
   false),
  ('AA100', timestamp'2007-12-01 09:00:00', '1 - Qualified', 150.0, 200, 20,
   false),
  ('AA100', timestamp'2007-12-29 11:45:00', '2 - Mtg Schd / NDA Signed', 
   150.05, 100, 20, false),
  ('BB100', timestamp'2007-09-09 05:00:23', '0 - Prospect', 10, 90, 40, true),
  ('BB100', timestamp'2007-09-09 05:00:23', '0 - Prospect', 10, 90, 40, true),
  ('BB100', timestamp'2007-09-09 05:00:23', '0 - Prospect', 10, 90, 40, true),
  ('BB200', timestamp'2008-01-01 07:27:00', '1 - Qualified', 900.99, 2000, 20,
   false),
  ('DD400', timestamp'2007-12-04 16:56:09', '0 - Prospect', 10, 15, 99, false),
  ('DD400', timestamp'2007-12-05 00:00:00', '1 - Qualified', 10, 15, 80,
   false),
  ('DD400', timestamp'2007-12-05 00:00:04', '2 - Mtg Schd / NDA Signed', 15,
   15, 90, false),
  ('DD400', timestamp'2007-12-05 00:02:00', '2 - Mtg Schd / NDA Signed', 20,
   20, 95, true);

select * from OptyHist order by id, changeTimestamp;
  
select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage  from OptyHist
        order by id, changeTimestamp),
    row(id),
    row(changeTimestamp)));

-- same timestamp different clumping values
truncate table OptyHist;
insert into OptyHist values
  ('AA100', timestamp'2007-10-14 00:00:00', '0 - Prospect', 150, 100.0, 10,
   false),
  ('AA100', timestamp'2007-12-01 09:00:00', '1 - Qualified', 150.0, 200, 20,
   false),
  ('AA100', timestamp'2007-12-01 09:00:00', '1a - Qualified', 150.0, 200, 20,
   false),
  ('AA100', timestamp'2007-12-01 09:00:00', '1 - Qualified', 150.0, 200, 20,
   false),
  ('AA100', timestamp'2007-12-29 11:45:00', '2 - Mtg Schd / NDA Signed', 
   150.05, 100, 20, false),
  ('BB100', timestamp'2007-09-09 05:00:23', '0 - Prospect', 10, 90, 40, true),
  ('BB100', timestamp'2007-09-09 05:00:23', '0 - Prospect', 10, 90, 40, true),
  ('BB100', timestamp'2007-09-09 05:00:23', '0a - Prospect', 10, 90, 40, true),
  ('BB200', timestamp'2008-01-01 07:27:00', '1 - Qualified', 900.99, 2000, 20,
   false),
  ('DD400', timestamp'2007-12-04 16:56:09', '0 - Prospect', 10, 15, 99, false),
  ('DD400', timestamp'2007-12-05 00:00:00', '1 - Qualified', 10, 15, 80,
   false),
  ('DD400', timestamp'2007-12-05 00:00:04', '2 - Mtg Schd / NDA Signed', 15,
   15, 90, false),
  ('DD400', timestamp'2007-12-05 00:02:00', '2 - Mtg Schd / NDA Signed', 20,
   20, 95, true);

select * from OptyHist order by id, changeTimestamp;
  
select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage  from OptyHist
        order by id, changeTimestamp),
    row(id),
    row(changeTimestamp)));

-- input table has more than one columns of type timestamp
-- notes that only one of those can be served as timestamp column
create table OptyHistMTS(
  id varchar(20),
  changeTimestamp timestamp,
  moreTimeStamp timestamp,
  stage varchar(50),
  amount decimal(10,2),
  expectedRevenue decimal(10,2),
  probability int,
  isDeleted boolean);

insert into OptyHistMTS values
  ('AA100', timestamp'2007-10-14 00:00:00', timestamp'2007-10-14 00:00:00', '0 - Prospect', 150, 100.0, 10,
   false),
  ('AA100', timestamp'2007-12-01 09:00:00', timestamp'2007-10-14 00:00:00', '1 - Qualified', 150.0, 200, 20,
   false),
  ('AA100', timestamp'2007-12-29 11:45:00', timestamp'2007-10-14 00:00:00', '2 - Mtg Schd / NDA Signed', 
   150.05, 100, 20, false),
  ('BB100', timestamp'2007-09-09 05:00:23', timestamp'2007-09-09 05:00:23', '0 - Prospect', 10, 90, 40, true),
  ('BB200', timestamp'2008-01-01 07:27:00', timestamp'2008-01-01 07:27:00', '1 - Qualified', 900.99, 2000, 20,
   false),
  ('DD400', timestamp'2007-12-04 16:56:09', timestamp'2007-12-04 16:56:09', '0 - Prospect', 10, 15, 99, false);

select * from OptyHistMTS order by id, changeTimestamp;
  
select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, moreTimestamp, changeTimestamp, stage  from OptyHistMTS
        order by id, changeTimestamp),
    row(id, moreTimestamp),
    row(changeTimestamp)));


--
-- Negative tests
--

truncate table OptyHist;
insert into OptyHist values
  (null, timestamp'2007-10-14 00:00:00', null, 150, 100.0, 10,
   false),
  (null, timestamp'2007-12-01 09:00:00', null, 150.0, 200, 20,
   false),
  (null, timestamp'2007-12-29 11:45:00', '0 - Prospect', 150.05, 100, 20, false),
  ('BB100', timestamp'2007-09-09 05:00:23', null, 10, 90, 40, true),
  ('BB200', timestamp'2008-01-01 07:27:00', '1 - Qualified', 900.99, 2000, 20,
   false),
  ('DD400', timestamp'2007-12-04 16:56:09', '0 - Prospect', 10, 15, 99, false),
  ('DD400', timestamp'2007-12-05 00:00:00', '1 - Qualified', 10, 15, 80,
   false),
  ('DD400', timestamp'2007-12-05 00:00:04', '2 - Mtg Schd / NDA Signed', 15,
   15, 90, false),
  ('DD400', timestamp'2007-12-05 00:02:00', null, 20, 20, 95, true),
  ('AA100', timestamp'2007-10-12 12:23:41', '0 - Prospect', 20,
   20, 95, false),
  ('AA100', timestamp'2007-10-13 00:00:00', '0 - Prospect', 20,
   20, 95, false),
  ('AA100', timestamp'2007-12-01 15:00:00', '0 - Prospect', 20,
   20, 95, true);

-- timestamp column is not one column
select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage from OptyHist
        order by id, changeTimestamp),
    row(id),
    row(changeTimestamp, stage)));

-- timestamp column is not of type timestamp
select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, amount, stage from OptyHist
        order by id, amount),
    row(id),
    row(amount)));

-- timestamp input column contains a null value
insert into OptyHist values
  ('AA100', null, '3 - Data received / Analyzed', 200, 200, 100, false);

select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage  from OptyHist
        order by id, changeTimestamp),
    row(id),
    row(changeTimestamp)));

delete from OptyHist where changeTimestamp is null;

-- timestamp column is in partition columns
select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage from OptyHist
        order by id, changeTimestamp),
    row(changeTimestamp),
    row(changeTimestamp)));

-- column not in input table
select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, stage, isDeleted from OptyHist
        order by id, changeTimestamp),
    row(id),
    row(changeTimestamp)));

-- input table does not have exactly one clumping column
select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage, amount from OptyHist
        order by id, changeTimestamp),
    row(id),
    row(changeTimestamp)));
select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage from OptyHist
        order by id, changeTimestamp),
    row(id, stage),
    row(changeTimestamp)));

-- clumping column is not of type char or varchar
select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, amount from OptyHist
        order by id, changeTimestamp),
    row(id),
    row(changeTimestamp)));

-- input table not presorted correctly
-- partitioning column not sorted
select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, isDeleted, changeTimestamp, stage from OptyHist),
    row(id, isDeleted),
    row(changeTimestamp)));

-- timestamp column not sorted
truncate table OptyHist;
insert into OptyHist values
  ('AA100', timestamp'2007-10-13 12:23:41', '0 - Prospect', 20,
   20, 95, false),
  ('AA100', timestamp'2007-10-13 00:00:00', '0 - Prospect', 20,
   20, 95, false),
  ('AA100', timestamp'2007-12-01 15:00:00', '0 - Prospect', 20,
   20, 95, true);

select * from table(
  applib.contiguous_value_intervals(
    cursor(
        select id, changeTimestamp, stage from OptyHist),
    row(id),
    row(changeTimestamp)));

-- cleanup
drop table OptyHist cascade;
drop schema cvi cascade;
