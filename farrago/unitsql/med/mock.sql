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

create table mock_empty_table(
    id int not null primary key)
server mock_local_server;

insert into mock_empty_table values (5);

select * from mock_fennel_table;

select * from mock_java_table;

select * from mock_empty_table;

explain plan for select * from mock_fennel_table;

explain plan for select * from mock_java_table;

explain plan for select * from mock_empty_table;

explain plan for insert into mock_empty_table values (5);
