-- test the compute_statistics_for_schema UDP
create schema TESTSCHEMA;
set schema 'TESTSCHEMA';
create table T1(col1 integer, col2 integer);
create table T2(col3 varchar(255), col4 integer);

-- should be null
select LAST_ANALYZE_ROW_COUNT from SYS_ROOT.DBA_STORED_TABLES where TABLE_NAME = 'T1';
select LAST_ANALYZE_ROW_COUNT from SYS_ROOT.DBA_STORED_TABLES where TABLE_NAME = 'T2';

-- analyze them
call applib.compute_statistics_for_schema('TESTSCHEMA');

-- should be zero
select LAST_ANALYZE_ROW_COUNT from SYS_ROOT.DBA_STORED_TABLES where TABLE_NAME = 'T1';
select LAST_ANALYZE_ROW_COUNT from SYS_ROOT.DBA_STORED_TABLES where TABLE_NAME = 'T2';
drop schema TESTSCHEMA cascade;

-- try using on a nonexisting schema, should get error (LER-2608)
call applib.compute_statistics_for_schema('IMAGINARY_SCHEMA');
