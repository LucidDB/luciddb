-- $Id$
-- Test mock namespace plugin

create foreign data wrapper mock_wrapper
library 'class net.sf.farrago.namespace.mock.MedMockForeignDataWrapper'
language java;

create server mock_server
foreign data wrapper mock_wrapper;

create schema mock_schema;

set schema mock_schema;

create foreign table mock_fennel_table(
    id int not null)
server mock_server
options (executor_impl 'FENNEL', row_count '3');

create foreign table mock_java_table(
    id int not null)
server mock_server
options (executor_impl 'JAVA', row_count '3');

select * from mock_fennel_table;

select * from mock_java_table;

explain plan for select * from mock_fennel_table;

explain plan for select * from mock_java_table;
