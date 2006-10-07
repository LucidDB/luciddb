-- $Id$
-- Test export/import functionality

-- export query results as tab-delimited .txt
call sys_boot.mgmt.export_query_to_file(
'select * from sales.depts order by name',
'${FARRAGO_HOME}/unitsql/syslib/depts_files/DEPTS',
true,
false);

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
