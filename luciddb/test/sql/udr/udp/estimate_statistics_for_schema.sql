-- test the estimate_statistics_for_schema UDP
create schema TESTSCHEMA;
set schema 'TESTSCHEMA';
set path 'TESTSCHEMA';

create function ramp(n int) 
    returns table(i int)
    language java
    parameter style system defined java
    no sql
    external name 'class net.sf.farrago.test.FarragoTestUDR.nullableRamp(java.lang.Integer, java.sql.PreparedStatement)';

create table T1(col1 integer, col2 integer);
create table T2(col3 varchar(255), col4 integer);

-- should be empty
select COLUMN_NAME, PERCENT_SAMPLED, SAMPLE_SIZE from SYS_ROOT.DBA_COLUMN_STATS where TABLE_NAME = 'T1' order by COLUMN_NAME;
select COLUMN_NAME, PERCENT_SAMPLED, SAMPLE_SIZE from SYS_ROOT.DBA_COLUMN_STATS where TABLE_NAME = 'T2' order by COLUMN_NAME;

-- create some data
insert into T1 (col1, col2) select I, I from table(ramp(10000));
insert into T2 (col3, col4) 
    select 'row: ' || cast(I as varchar(5)), I from table(ramp(1000));

-- analyze them
call applib.estimate_statistics_for_schema('TESTSCHEMA');

-- should be non-empty
select COLUMN_NAME, PERCENT_SAMPLED, SAMPLE_SIZE from SYS_ROOT.DBA_COLUMN_STATS where TABLE_NAME = 'T1' order by COLUMN_NAME;
select COLUMN_NAME, PERCENT_SAMPLED, SAMPLE_SIZE from SYS_ROOT.DBA_COLUMN_STATS where TABLE_NAME = 'T2' order by COLUMN_NAME;

-- re-analyze them with fixed sampling rate
call applib.estimate_statistics_for_schema('TESTSCHEMA', 25.0);

-- should be non-empty
select COLUMN_NAME, PERCENT_SAMPLED, SAMPLE_SIZE from SYS_ROOT.DBA_COLUMN_STATS where TABLE_NAME = 'T1' order by COLUMN_NAME;
select COLUMN_NAME, PERCENT_SAMPLED, SAMPLE_SIZE from SYS_ROOT.DBA_COLUMN_STATS where TABLE_NAME = 'T2' order by COLUMN_NAME;

-- verify that no read lock lingered from ANALYZE (FRG-141)
insert into T1 values (3, 3);

-- try using invalid sampling rates
call applib.estimate_statistics_for_schema('TESTSCHEMA', 0.0);
call applib.estimate_statistics_for_schema('TESTSCHEMA', 101.0);
call applib.estimate_statistics_for_schema('TESTSCHEMA', -1.0);

drop schema TESTSCHEMA cascade;

-- try using on a nonexisting schemas, should get error (LER-2608)
call applib.estimate_statistics_for_schema('IMAGINARY_SCHEMA');
call applib.estimate_statistics_for_schema('IMAGINARY_SCHEMA', 50.0);
