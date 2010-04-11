-- $Id$
-- Test metadata calls, and varying combinations of arguments

!set outputformat csv

create schema m;
set schema 'm';
create table MYTABLE(i int not null primary key, j int);
create table "Mixed case" ("i" int not null primary key, j int);
create table "lower case" ("i" int not null primary key, "j" int);
create table "with""quote" ("i" int not null primary key, "j" int);
create table "paint" ("i" int not null primary key);
create table "point" ("i" int not null primary key);
create table "p%nt" ("i" int not null primary key);

-- DESCRIBE
--
-- with arg 'TABLES', shows all tables
!describe tables
-- likewise
!describe TaBlES
-- describe particular table, one arg
!describe mytable
-- describe particular table, one arg quoted
!describe "MYTABLE"
-- describe particular table, two args
!describe m.mytable
-- describe particular table, quoted wildcard (expect 2)
!describe m."% case"
-- describe, unquoted wildcard (expect 2)
!describe m.M%
-- if missing close quote ", be lenient and assume it is there
!describe "% case
!describe m."% case

-- TABLES
--
!tables LOCALDB
-- spurious args ignored
!tables t foo bar
-- with no args, shows all tables (in all schemas)
!tables
-- containing quote
!tables "with""quote"
-- wildcard (expect 3)
!tables "p%nt"
-- escaped wildcard (expect 1)
!tables "p\%nt"
-- line ends with spaces; these should be ignored (expect 1)
!tables mytable    
-- line ends with semicolon; should be ignored (expect 1)
!tables mytable;
-- line ends with spaces, tab and semicolon; should be ignored (expect 1)
!tables mytable ;   	   

-- COLUMNS
--
-- one arg
!columns EMPS
-- one wildcard arg
!columns M%
-- two args
!columns M.EMPS
-- two args, bad schema (expect 0)
!columns XXX.EMPS

-- INDEXES
--
-- one arg
!indexes mytable
-- two args
!indexes m.mytable
-- three args
!indexes localdb.m.mytable

-- TYPEINFO
--
!typeinfo
-- spurious args ignored
!typeinfo foo bar

-- PROCEDURES
!procedures

-- METADATA
--
-- no args, error
!metadata
-- bad arg
-- TODO jvs 14-Mar-2010:  re-enable this once we drop JDK 1.5 support
-- (for now it gives a diff between 1.5 and 1.6)
-- !metadata getIcecream
-- ok
!metadata getSchemas
-- 3 args
!metadata getTables LOCALDB % M%

!quit
-- End metadata.sql
