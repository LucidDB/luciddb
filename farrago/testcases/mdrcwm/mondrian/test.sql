create or replace schema test;
set schema 'test';
set path 'test';

create or replace function lurql_query(
    foreign_server_name varchar(128),
    lurql varchar(65535))
returns table(
    class_name varchar(128),
    obj_name varchar(128),
    mof_id varchar(128),
    obj_attrs varchar(65535)
)
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.test.LurqlQueryUdx.queryMedMdr';

create or replace server olap_package
foreign data wrapper sys_mdr
options(
    extent_name 'Mondrian', 
    schema_name 'Mondrian',
    "org.netbeans.mdr.persistence.Dir" 'testcases/mdrcwm/catalog/mdr')
description 'Virtual catalog for CWM OLAP metadata imported from Mondrian XML';

select * from olap_package."Mondrian"."Level";

select * from table(lurql_query(
'OLAP_PACKAGE', 
'select * from class Level;'
)) order by class_name, obj_name;
