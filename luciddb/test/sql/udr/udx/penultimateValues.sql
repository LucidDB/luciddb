-- tests for Penultimate_values UDX
create schema pv;
set schema 'pv';

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
  applib.penultimate_values(
    cursor(select * from OptyHist order by id, changeTimestamp),
    row(id),
    row(stage, changeTimestamp)));

-- basic table
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
   20, 95, true);

select * from table(
  applib.penultimate_values(
    cursor(select * from OptyHist order by id, changeTimestamp),
    row(id),
    row(stage, changeTimestamp)))
order by id;

-- with nulls
insert into OptyHist values
  (null, timestamp'1800-01-01 00:00:00', null, 0, null, 0, true),
  (null, timestamp'6666-06-16 00:59:59', null, null, 0, -1, false),
  ('AA100', timestamp'2007-10-12 12:23:41', '0 - Prospect', 100, null, null,
   null),
  ('AA100', timestamp'2007-12-01 15:00:00', '0 - Prospect', null, 100, 60,
   false),
  ('BB100', timestamp'2007-09-09 05:00:20', null, 10, null, 50, null);
  
select * from table(
  applib.penultimate_values(
    cursor(select * from OptyHist order by id, changeTimestamp),
    row(id),
    row(stage, changeTimestamp)))
order by id;

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

select * from table(
  applib.penultimate_values(
    cursor(select * from OptyHist order by id, changeTimestamp),
    row(id),
    row(stage, changeTimestamp)))
order by id;

select id, stage, applib.days_diff(until_timestamp, changeTimestamp)
  from table(
    applib.penultimate_values(
      cursor(select id, stage, changeTimestamp from OptyHist
        order by id, changeTimestamp),
      row(id),
      row(stage, changeTimestamp)))
order by id;

-- multi-column key
select * from table(
  applib.penultimate_values(
    cursor(select * from OptyHist
      order by id, expectedRevenue, changeTimestamp),
    row(id, expectedRevenue),
    row(stage, changeTimestamp)))
order by id, expectedRevenue;

--
-- Negative tests
--

-- designated_value_and_timestamp is not 2 columns
select * from table(
  applib.penultimate_values(
    cursor(select * from OptyHist order by id, changeTimestamp),
    row(id),
    row(stage, changeTimestamp, amount)));

-- second column of designated_value_and_timestamp is not a timestamp
select * from table(
  applib.penultimate_values(
    cursor(select * from OptyHist order by id, changeTimestamp),
    row(id),
    row(stage, amount)));

-- input table not presorted correctly
select * from table(
  applib.penultimate_values(
    cursor(select * from OptyHist order by id desc, changeTimestamp),
    row(id),
    row(stage, changeTimestamp)));

select * from table(
  applib.penultimate_values(
    cursor(select * from OptyHist order by id, changeTimestamp desc),
    row(id),
    row(stage, changeTimestamp)));

-- columns in designated_value_and_timestamp are in grouping_columns
select * from table(
  applib.penultimate_values(
    cursor(select * from OptyHist order by id, changeTimestamp),
    row(amount,id),
    row(amount, changeTimestamp)));

-- timestamp input column contains a null value
insert into OptyHist values
  ('AA100', null, '3 - Data received / Analyzed', 200, 200, 100, false);

select * from table(
  applib.penultimate_values(
    cursor(select * from OptyHist order by id, changeTimestamp),
    row(id),
    row(stage, changeTimestamp)));

-- cleanup
drop table OptyHist cascade;
drop schema pv cascade;