-- test the create_table_from_source_table UDP

-- setup a schema and couples of tables for testing.
create schema "S";
set schema 'S';

create table T1 (col1 varchar(50));
create view T2 
description 'test for create_table_from_source_table UDP'
as select * from "S"."T1";

-- test to make sure datatypes, precision match
call applib.create_table_from_source_table ( '"S"."T1"', null, 'T1_TARGET', null);
select TABLE_NAME,COLUMN_NAME,DATATYPE,"PRECISION",IS_NULLABLE from sys_root.dba_columns where table_name like 'T1%' and schema_name = 'S' order by TABLE_NAME,COLUMN_NAME;

-- test to make sure it works with tables and views
call applib.create_table_from_source_table ( '"S"."T2"', null, 'T2_TARGET', null);
select TABLE_NAME,COLUMN_NAME,DATATYPE,"PRECISION",IS_NULLABLE from sys_root.dba_columns where table_name like 'T2%' and schema_name = 'S' order by TABLE_NAME,COLUMN_NAME;

-- test adding additional columns
call applib.create_table_from_source_table ( '"S"."T1"', null, 'T3_TARGET', 'desc varchar(255)');
select TABLE_NAME,COLUMN_NAME,DATATYPE,"PRECISION",IS_NULLABLE from sys_root.dba_columns where table_name in ('T1','T3_TARGET')  and schema_name = 'S' order by TABLE_NAME,COLUMN_NAME;
--The source table input, if a simple table name, should try and access tables in the default schema
drop table "T3_TARGET";
call applib.create_table_from_source_table ( '"T1"', null, 'T3_TARGET', 'desc varchar(255)');
select TABLE_NAME,COLUMN_NAME,DATATYPE,"PRECISION",IS_NULLABLE from sys_root.dba_columns where table_name in ('T1','T3_TARGET')  and schema_name = 'S' order by TABLE_NAME,COLUMN_NAME;

--Test datatype decimal(precision, scale)
CREATE TABLE "DecimalTb"(
col decimal(5,4)
);
insert into "DecimalTb" values(5.0015);
call applib.create_table_from_source_table ( '"DecimalTb"', null, 'DDB26_DecimalTb', null);
select TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,DATATYPE,"PRECISION",DEC_DIGITS 
from SYS_ROOT.DBA_COLUMNS where SCHEMA_NAME='S' and TABLE_NAME like '%DecimalTb' order by TABLE_NAME,ORDINAL_POSITION;

insert into "DDB26_DecimalTb" values(5.0015);
select * from "DecimalTb" order by col;
select * from "DDB26_DecimalTb" order by col;

drop table "DecimalTb";
drop table "DDB26_DecimalTb";
-- test to make sure that it works with remote tables
--TODO: NOT UNDERSTAND REMOTE TABLES.

-- test to make sure errors are thrown when:
--table already exists 
call applib.create_table_from_source_table ( '"S"."T1"', null, 'T1_TARGET', null);

--additional columns clause contains another column already in the table
call applib.create_table_from_source_table ( '"S"."T1"', null, 'T4_TARGET', 'col1 varchar(25)');

--additional columns clause is invalid 
call applib.create_table_from_source_table ( '"S"."T1"', null, 'T5_TARGET', 'desc vachar(50)');

-- do cleanup
drop view "T2";
drop table "T1";
drop table "T1_TARGET";
drop table "T2_TARGET";
drop table "T3_TARGET";
drop schema "S";
