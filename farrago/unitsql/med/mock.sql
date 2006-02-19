-- $Id$
-- Test mock namespace plugin

create foreign data wrapper mock_foreign_wrapper
library 'class net.sf.farrago.namespace.mock.MedMockForeignDataWrapper'
language java;

create local data wrapper mock_local_wrapper
library 'class net.sf.farrago.namespace.mock.MedMockLocalDataWrapper'
language java;

create server mock_foreign_server
foreign data wrapper mock_foreign_wrapper;

create server mock_local_server
local data wrapper mock_local_wrapper;

create server mock_foreign_metadata_server
foreign data wrapper mock_foreign_wrapper
options (
foreign_schema_name 'MOCK_SCHEMA', 
foreign_table_name 'MOCK_TABLE',
executor_impl 'JAVA',
row_count '3');

create server mock_foreign_dynamic_server
foreign data wrapper mock_foreign_wrapper
options (
foreign_schema_name 'MOCK_SCHEMA', 
foreign_table_name 'MOCK_TABLE',
row_count_sql 'select current_row_count from mock_schema.dynamic_row_count');

create schema mock_schema;

set schema 'mock_schema';

create foreign table mock_fennel_table(
    id int not null)
server mock_foreign_server
options (executor_impl 'FENNEL', row_count '3');

create foreign table mock_java_table(
    id int not null)
server mock_foreign_server
options (executor_impl 'JAVA', row_count '3');

create function ramp(n int)
returns table(i int)
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.ramp';

create foreign table mock_ramp_udx_table(
    id int not null)
server mock_foreign_server
options (
    executor_impl 'JAVA', 
    udx_specific_name 'mock_schema.ramp',
    row_count '3');

create table mock_empty_table(
    id int not null primary key)
server mock_local_server;

create foreign table mock_implicit_metadata_table
server mock_foreign_metadata_server;

create foreign table mock_explicit_metadata_table(
    id int not null)
server mock_foreign_metadata_server
options (foreign_schema_name 'MOCK_SCHEMA', foreign_table_name 'MOCK_TABLE');

-- should fail:  schema name mismatch
create foreign table bad_schema_name
server mock_foreign_metadata_server
options (foreign_schema_name 'BACH_SCHEMA');

-- should fail:  table name mismatch
create foreign table bad_table_name
server mock_foreign_metadata_server
options (foreign_table_name 'BACH_TABLE');

-- test create index on mock table

create index mock_index on mock_empty_table(id);

insert into mock_empty_table values (5);

select * from mock_fennel_table;

select * from mock_java_table;

select * from mock_ramp_udx_table;

select * from mock_empty_table;

select * from mock_implicit_metadata_table;

select * from mock_explicit_metadata_table;

select * from mock_foreign_metadata_server.mock_schema.mock_table;

-- should fail:  unknown schema name
select * from mock_foreign_metadata_server.bach_schema.mock_table;

-- should fail:  unknown table name
select * from mock_foreign_metadata_server.mock_schema.bach_table;

explain plan for select * from mock_fennel_table;

explain plan for select * from mock_java_table;

explain plan for select * from mock_empty_table;

explain plan for insert into mock_empty_table values (5);

create schema mock_local_schema;

set schema 'mock_local_schema';

import foreign schema mock_schema
from server mock_foreign_metadata_server
into mock_local_schema;

select * from mock_local_schema.mock_table;

-- should fail:  no metadata
import foreign schema mock_schema
from server mock_foreign_server
into mock_local_schema;

-- should fail:  no such schema
import foreign schema bach_schema
from server mock_foreign_metadata_server
into mock_local_schema;

-- should fail:  duplicate table name
import foreign schema mock_schema
from server mock_foreign_metadata_server
into mock_local_schema;

create schema mock_limit_schema;

set schema 'mock_limit_schema';

import foreign schema mock_schema
limit to (mock_table)
from server mock_foreign_metadata_server
into mock_limit_schema;

select * from mock_limit_schema.mock_table;

-- should fail:  no such table
import foreign schema mock_schema
limit to (bach_table)
from server mock_foreign_metadata_server
into mock_limit_schema;

create schema mock_exclude_schema;

set schema 'mock_exclude_schema';

import foreign schema mock_schema
except (mock_table)
from server mock_foreign_metadata_server
into mock_exclude_schema;

-- should fail:  excluded
select * from mock_exclude_schema.mock_table;

create table mock_schema.dynamic_row_count(
    current_row_count int not null primary key);
insert into mock_schema.dynamic_row_count values(7);

select count(*) from mock_foreign_dynamic_server.mock_schema.mock_table;

-- now change the current_row_count value and verify that we get
-- a corresponding number of rows back from the mock
update mock_schema.dynamic_row_count set current_row_count=21;

-- have to flush the plan cache, because the value 7 is still burned
-- into the old plan and the optimizer doesn't know that it's supposed
-- to read the new value
alter system set "codeCacheMaxBytes" = min;

select count(*) from mock_foreign_dynamic_server.mock_schema.mock_table;
