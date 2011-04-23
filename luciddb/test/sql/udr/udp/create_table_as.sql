-- test the create_table_as UDP

-- null schema without set schema first, no null exception
call applib.create_table_as(null, 'ddb26_t1', 'select * from nothing', true);
-- setup a schema and couples of tables for testing.
create or replace schema "SS";
set schema 'SS';
create table DDB26(COL int);
insert into DDB26 values(1);
insert into DDB26 values(2);

-- Test to make sure datatypes/precisions match
call applib.create_table_as(null,'DDB26_T1','select * from "SS"."DDB26"',true);
select TABLE_NAME,COLUMN_NAME,DATATYPE,"PRECISION",IS_NULLABLE from sys_root.dba_columns where table_name like 'DDB26%' and schema_name = 'SS' order by TABLE_NAME,COLUMN_NAME;

call applib.create_table_as('APPLIB','DDB26_T1','select * from "SS"."DDB26"',false);
select TABLE_NAME,COLUMN_NAME,DATATYPE,"PRECISION",IS_NULLABLE from sys_root.dba_columns where table_name = 'DDB26_T1'  and schema_name = 'APPLIB' order by TABLE_NAME,COLUMN_NAME;
select TABLE_NAME,COLUMN_NAME,DATATYPE,"PRECISION",IS_NULLABLE from sys_root.dba_columns where table_name = 'DDB26_T1'  and schema_name = 'SS' order by TABLE_NAME,COLUMN_NAME;

select * from DDB26_T1;
select * from SS.DDB26_T1;
SELECT * FROM APPLIB.DDB26_T1;
-- Test for select statements from tables, UDXs, and remote tables 
--UDXs
call applib.create_table_as('SS','DDB26_UDX','select TIME_KEY_SEQ,TIME_KEY from table(APPLIB.FISCAL_TIME_DIMENSION(2010,1,1,2010,1,10,1))',true);
select TIME_KEY_SEQ,TIME_KEY from table(APPLIB.FISCAL_TIME_DIMENSION(2010,1,1,2010,1,10,1)) order by TIME_KEY_SEQ;
select * from SS.DDB26_UDX order by TIME_KEY_SEQ;
-- Test datatype decimal
CREATE TABLE DecimalTb(
col decimal(5,4)
);
insert into DecimalTb values(5.0015);
call applib.create_table_as(null,'DDB26_DecimalTb','select * from SS.DecimalTb',true);

select * from DecimalTb order by col;
select * from "DDB26_DecimalTb" order by col;

drop table DecimalTb;
drop table "DDB26_DecimalTb";
--remote tables
--TODO: Need to undertand the concept remote table.

-- invalid input parameters.
call applib.create_table_as('SS','','select * from "SS"."DDB26"',true);
call applib.create_table_as('SS','DDB26_E','',true);
-- target table is exsiting in specific schema.
call applib.create_table_as(null,'DDB26_T1','select * from "SS"."DDB26"',true);
-- illegal select statement.
call applib.create_table_as('SS','DDB26_E','Selet * from "SS"."DDB26"',true);

-- do cleanup
drop table DDB26_T1;
drop table APPLIB.DDB26_T1;
drop table DDB26_UDX;
drop schema SS CASCADE;
