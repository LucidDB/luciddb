-- Test split out from sqlserverWrapper for FRG-162

-- create sqlserver server with qualifying catalog
create server my_mssql
foreign data wrapper "SQL SERVER"
options(
  url 'jdbc:jtds:sqlserver://akela.lucidera.com:1433',
  user_name 'ldbtest',
  password 'ldbtest',
  qualifying_catalog_name 'BENCHMARK',
  table_types 'TABLE'
);

-- TODO: FRG-162 incorrectly returns no schemas, qualifying_catalog_name
-- doesn't match any schemas since catalog info isn't returned
-- browse foreign schemas should show schemas for BENCHMARK catalog/database
select * from table( sys_boot.mgmt.browse_foreign_schemas('MY_MSSQL'))
order by schema_name;

-- cleanup
drop server my_mssql cascade;
