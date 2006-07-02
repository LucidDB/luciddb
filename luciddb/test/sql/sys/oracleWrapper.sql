-- test built-in oracle jdbc wrapper

-- check that it's been tagged for browse connect
select * from sys_boot.mgmt.browse_connect_foreign_wrappers 
where foreign_wrapper_name = 'ORACLE'
order by 2;

-- browse server options
select * from table(
  sys_boot.mgmt.browse_connect_foreign_server(
    'ORACLE', 
    cursor(
      select '' as option_name, '' as option_value 
      from sys_boot.jdbc_metadata.empty_view)))
order by option_ordinal, option_choice_ordinal;

-- see extended options available
select * from table(
sys_boot.mgmt.browse_connect_foreign_server(
'ORACLE', 
cursor(
 values('URL', 'jdbc:oracle:thin:@akela.lucidera.com:1521:XE'),
       ('EXTENDED_OPTIONS', 'TRUE'))))
order by option_ordinal, option_choice_ordinal;

-- create oracle server
create server my_orcl
foreign data wrapper ORACLE
options(
  url 'jdbc:oracle:thin:@akela.lucidera.com:1521:XE',
  user_name 'schoi',
  password 'schoi'
);

-- browse foreign schemas
select * from table( sys_boot.mgmt.browse_foreign_schemas('MY_ORCL'))
order by schema_name;

-- cleanup
drop server my_orcl cascade;