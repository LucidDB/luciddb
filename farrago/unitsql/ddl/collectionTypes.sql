-- $id: //open/dt/dev/farrago/unitsql/ddl/udf.sql#1 $
-- Test DDL for collection types

create schema collectionsTest;
set schema 'collectionsTest';

-- MULTISET 
create table multisetTable(i integer primary key, ii integer multiset);
