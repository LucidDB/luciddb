-- $Id$
-- Test create or replace rename to DDL

--
-- View
--
create schema foo1;
set schema 'foo1';
create view orange as select * from sales.depts;

-- should succeed
create or replace rename to apple view orange as select * from sales.depts;
select * from apple;

-- should fail 'cause orange is now apple 
select * from orange;

-- should succeed, usage of rename is redundant 
create or replace rename to apple view apple as select * from sales.depts;

create view plum as select * from sales.emps;
-- should fail, duplicate name
create or replace rename to plum view apple as select * from sales.depts;

--
-- Schema
--
create schema foo2;
set schema 'foo2';
create view pineapple as select * from foo1.apple;

-- should fail 'cause foo2.pineapple depends on foo1.apple
create or replace rename to foo3 schema foo1;

-- should fail, same error as above
create or replace rename to lemon view foo1.apple as select * from sales.depts;

--
-- Server
--
create foreign data wrapper foo_wrapper
 library 'class net.sf.farrago.namespace.mock.MedMockForeignDataWrapper'
 language java;
                                                                                
create server foo_server
 foreign data wrapper foo_wrapper;
                                                                                
create foreign table foo_table(
    id int not null)
server foo_server
options (executor_impl 'JAVA', row_count '3');
                                                                                
create or replace rename to foo_server_new server foo_server
 foreign data wrapper foo_wrapper
 description 'blah';

select "description" from sys_fem.med."DataServer" where "name" = 'FOO_SERVER_NEW';
                                                                                
select * from foo_table;

-- 
-- Wrapper
--
create or replace rename to foo_wrapper_new foreign data wrapper foo_wrapper
 library 'class net.sf.farrago.namespace.mock.MedMockForeignDataWrapper'
 language java
 description 'blah2';

select "description" from sys_fem.med."DataWrapper" where "name" = 'FOO_WRAPPER_NEW';
                                                                                
select * from foo_table;

