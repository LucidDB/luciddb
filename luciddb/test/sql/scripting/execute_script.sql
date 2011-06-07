-- tests ability to run code from file or from a string
select schema_name from sys_root.dba_schemas order by schema_name;
call applib.execute_script('js', 'importPackage(java.sql) /* There is also an importClass() function. */
  var conn = DriverManager.getConnection("jdbc:default:connection")
  var ps = conn.prepareStatement("create or replace schema boohoo")
  ps.execute()
  conn.close()
');
select schema_name from sys_root.dba_schemas order by schema_name;
call applib.execute_script('js', '${FARRAGO_HOME}/test/sql/scripting/execute_script.js');
select schema_name from sys_root.dba_schemas order by schema_name;
drop schema boohoo;
drop schema boohoo2;
