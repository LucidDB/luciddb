-- $Id$
-- Test export/import functionality

-- export query results
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
