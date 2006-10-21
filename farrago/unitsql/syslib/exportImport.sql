-- $Id$
-- Test export/import functionality

-- export query results as tab-delimited .txt
call sys_boot.mgmt.export_query_to_file(
'select * from sales.depts order by name',
'${FARRAGO_HOME}/unitsql/syslib/depts_files/DEPTS',
true,
true,
false,
'\t',
'.txt',
null,
null,
null);

-- set up a flat file reader to verify export
create server flatfile_server
foreign data wrapper sys_file_wrapper
options (
    directory 'unitsql/syslib/depts_files',
    file_extension 'txt',
    with_header 'yes', 
    log_directory 'testlog/',
    field_delimiter '\t',
    lenient 'no');

-- run import query
select * from flatfile_server.bcp.depts order by name;


-- Again, but this time as .csv
call sys_boot.mgmt.export_query_to_file(
'select * from sales.depts order by name',
'${FARRAGO_HOME}/unitsql/syslib/depts_files/DEPTS_CSV',
true,
true,
false,
',',
'.csv',
null,
null,
null);

-- Once more, this time omitting data
call sys_boot.mgmt.export_query_to_file(
'select * from sales.depts order by name',
'${FARRAGO_HOME}/unitsql/syslib/depts_files/DEPTS_NO_CSV',
true,
false,
false,
',',
'.csv',
null,
null,
null);

create server csv_flatfile_server
foreign data wrapper sys_file_wrapper
options (
    directory 'unitsql/syslib/depts_files',
    file_extension 'csv',
    with_header 'yes', 
    log_directory 'testlog/',
    field_delimiter ',',
    lenient 'no');

-- run import query
select * from csv_flatfile_server.bcp.depts_csv order by name;

-- attempt import without data:  should fail
select * from csv_flatfile_server.bcp.depts_no_csv order by name;

-- set up table with datetimes
create schema dt;
set schema 'dt';

create table dates (d date primary key, t time, ts timestamp);
insert into dates values
(date'2001-4-14', time'3:34:08', timestamp'2002-12-20 19:10:10'),
(date'1970-11-2', time'1:59:59', timestamp'1999-2-19 21:17:00');

-- export query with date formatting
call sys_boot.mgmt.export_query_to_file(
'select * from dt.dates order by d',
'${FARRAGO_HOME}/unitsql/syslib/dates_files/DATES',
true,
true,
false,
null,
null,
'EEE, MMM dd, yyyy',
'hh:mm:ss a',
'EEE, MMM dd, yyyy hh:mm:ss a');

create or replace server flatfile_server
foreign data wrapper sys_file_wrapper
options (
    directory 'unitsql/syslib/dates_files',
    file_extension 'txt',
    with_header 'yes', 
    log_directory 'testlog/',
    field_delimiter '\t',
    lenient 'no',
    date_format 'EEE, MMM dd, yyyy',
    time_format 'hh:mm:ss a',
    timestamp_format 'EEE, MMM dd, yyyy hh:mm:ss a');

select * from flatfile_server.bcp.dates order by d;
select * from flatfile_server.sample.dates order by 1,2,3;
