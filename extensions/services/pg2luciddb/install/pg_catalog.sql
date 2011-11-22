-- create schema:
create schema pg_catalog;

-- set schema:
set schema 'pg_catalog';

-- pg_version table:
create table pg_catalog.pg_version (version int);
insert into pg_catalog.pg_version (version) values (1);

-- grant permission to pg_version:
GRANT SELECT ON pg_catalog.pg_version TO PUBLIC;

-- dual table:
create table pg_catalog.dual (id int);
insert into pg_catalog.dual (id) values (0);

-- grant permission to dual:
GRANT SELECT ON pg_catalog.dual TO PUBLIC;

-- register udf functions:
call sqlj.install_jar('file:${FARRAGO_HOME}/plugin/luciddb-postgres-adapter-catalog.jar','pg_catalog_jar',0);

-- mofid to integer:
create function mofIdToInteger(input varchar(1024))
returns int
language java
no sql
external name 'pg_catalog.pg_catalog_jar:org.luciddb.pg2luciddb.pg_catalog_plugin.mofIdToInteger';

-- grant permission to mofIdToInteger function:
GRANT EXECUTE ON SPECIFIC FUNCTION pg_catalog.mofIdToInteger TO PUBLIC;

-- pg_namespace view:
create view pg_catalog.pg_namespace -- (oid, nspname)
as
select
  pg_catalog.mofIdToInteger(mof_id) as oid, 
  schema_name as nspname 
from sys_root.dba_schemas where schema_name not in ('SYS_BOOT', 'SQLJ', 'JDBC_METADATA', 'INFORMATION_SCHEMA', 'PG_CATALOG', 'SYS_ROOT', 'MGMT', 'APPLIB'); 

-- grant permission to dual:
GRANT SELECT ON pg_catalog.pg_namespace TO PUBLIC;

-- pg_roles view:
create view pg_catalog.pg_roles -- (oid, rolname, rolcreaterole, rolcreatedb)
as
select 
   pg_catalog.mofIdToInteger("mofId") as oid,
   "name" as rolname,
   case when "name" = '_SYSTEM' then 't' else 'f' end as rolecreaterole,
   case when "name" = '_SYSTEM' then 't' else 'f' end as rolecreatedb
from sys_fem."Security"."User";

-- grant permission to pg_roles:
GRANT SELECT ON pg_catalog.pg_roles TO PUBLIC;

-- pg_database table:
create table pg_catalog.pg_database
(
    oid int,
    datname varchar(128),
    encoding int,
    datlastsysoid int,
    datallowconn boolean,
    datconfig varchar(128), -- text[]
    datacl varbinary(128), -- aclitem[]
    datdba int,
    dattablespace int
);

insert into pg_catalog.pg_database values(
    0, -- oid
    'postgres', -- datname
    6, -- encoding, UTF8
    100000, -- datlastsysoid
    true, -- datallowconn
    null, -- datconfig
    null, -- datacl
    (select min(oid) from pg_catalog.pg_roles where rolecreatedb = 't'), -- datdba
    0 -- dattablespace
);

-- grant permission to pg_database:
GRANT SELECT ON pg_catalog.pg_database TO PUBLIC;

-- pg_tablespace table:
create table pg_catalog.pg_tablespace
(
    oid int,
    spcname varchar(128),
    spclocation varchar(128),
    spcowner int,
    spcacl varchar(128) -- aclitem[]
);

insert into pg_catalog.pg_tablespace values
(
    0,
    'main', -- spcname
    '?', -- spclocation
    0, -- spcowner,
    null -- spcacl
);

-- grant permission to pg_tablespace:
GRANT SELECT ON pg_catalog.pg_tablespace TO PUBLIC;

-- pg_settings table:
create table pg_catalog.pg_settings
(
    oid int,
    name varchar(128),
    setting varchar(128)
);

insert into pg_catalog.pg_settings values
(0, 'autovacuum', 'on'),
(1, 'stats_start_collector', 'on'),
(2, 'stats_row_level', 'on');

-- grant permission to pg_settings:
GRANT SELECT ON pg_catalog.pg_settings TO PUBLIC;

-- pg_user view:
create or replace view pg_catalog.pg_user -- oid, usename, usecreatedb, usesuper
as
select 
   pg_catalog.mofIdToInteger("mofId") as oid,
   "name" as usename,
    true usecreatedb,
    true usesuper
from sys_fem."Security"."User";

-- grant permission to pg_user:
GRANT SELECT ON pg_catalog.pg_user TO PUBLIC;

-- pg_authid table:
create table pg_catalog.pg_authid
(
    oid int,
    rolname varchar(128),          
    rolsuper boolean,
    rolinherit boolean,
    rolcreaterole boolean,
    rolcreatedb boolean,
    rolcatupdate boolean,
    rolcanlogin boolean,
    rolconnlimit boolean,
    rolpassword boolean,
    rolvaliduntil timestamp, -- timestamptz
    rolconfig varchar(128) -- text[]
);

-- grant permission to pg_authid:
GRANT SELECT ON pg_catalog.pg_authid TO PUBLIC;

-- pg_am table:
create table pg_catalog.pg_am(oid int, amname varchar(128));
insert into pg_catalog.pg_am values(0, 'btree');
insert into pg_catalog.pg_am values(1, 'hash');

-- grant permission to pg_am:
GRANT SELECT ON pg_catalog.pg_am TO PUBLIC;

-- pg_description view:
create view pg_catalog.pg_description -- (objoid, objsubid, classoid, description)
as
select
    oid objoid,
    0 objsubid,
    -1 classoid,
    datname description
from pg_catalog.pg_database;

-- grant permission to pg_description:
GRANT SELECT ON pg_catalog.pg_description TO PUBLIC;

-- pg_group view:
create view pg_catalog.pg_group -- oid, groname
as
select
    0 oid,
    '' groname
from pg_catalog.pg_database where oid = -1;

-- grant permission to pg_group:
GRANT SELECT ON pg_catalog.pg_group TO PUBLIC;

-- pg_type table:
create table pg_catalog.pg_type
(
    oid int,
    typname varchar(255),
    pgtypname varchar(255),      
    typnamespace int,
    typlen int,
    typtype varchar(128),
    typbasetype int,
    typtypmod int,
    haslength int,
    typalign char(1)
);

insert into pg_catalog.pg_type (oid, typname, pgtypname, typnamespace, typlen, typtype, typbasetype, typtypmod, haslength, typalign) values
(1043, 'VARCHAR', 'varchar', (select oid from pg_catalog.pg_namespace where nspname = 'PG_CATALOG'), -1, 'c', 0, -1, 1, 'i'), 
-- (18,  'CHAR', 'char', (select oid from pg_catalog.pg_namespace where nspname = 'PG_CATALOG'), -1, 'c', 0, -1, 1, 'i'),   
(1043, 'CHAR', 'varchar', (select oid from pg_catalog.pg_namespace where nspname = 'PG_CATALOG'), -1, 'c', 0, -1, 1, 'i'), 
(16, 'BOOLEAN', 'bool', (select oid from pg_catalog.pg_namespace where nspname = 'PG_CATALOG'), 1, 'c', 0, -1, 0, 'i'),    
(21, 'TINYINT', 'int2', (select oid from pg_catalog.pg_namespace where nspname = 'PG_CATALOG'), 2, 'c', 0, -1, 0, 'i'),     
(21,  'SMALLINT', 'int2', (select oid from pg_catalog.pg_namespace where nspname = 'PG_CATALOG'), 2, 'c', 0, -1, 0, 'i'),      
(23,  'INTEGER', 'int4', (select oid from pg_catalog.pg_namespace where nspname = 'PG_CATALOG'), 4, 'c', 0, -1, 0, 'i'),       
(20, 'BIGINT', 'int8', (select oid from pg_catalog.pg_namespace where nspname = 'PG_CATALOG'), 8, 'c', 0, -1, 0, 'i'),        
(1700,  'DECIMAL', 'numeric', (select oid from pg_catalog.pg_namespace where nspname = 'PG_CATALOG'), -1, 'c', 0, -1, 1, 'i'),         
(1700,  'NUMERIC', 'numeric', (select oid from pg_catalog.pg_namespace where nspname = 'PG_CATALOG'), -1, 'c', 0, -1, 1, 'i'),          
(700,  'REAL', 'float4', (select oid from pg_catalog.pg_namespace where nspname = 'PG_CATALOG'), 4, 'c', 0, -1, 0, 'i'),           
(701,  'DOUBLE', 'float8', (select oid from pg_catalog.pg_namespace where nspname = 'PG_CATALOG'), 8, 'c', 0, -1, 0, 'i'),            
(701,  'FLOAT', 'float8', (select oid from pg_catalog.pg_namespace where nspname = 'PG_CATALOG'), 8, 'c', 0, -1, 0, 'i'),             
(1083, 'TIME', 'time', (select oid from pg_catalog.pg_namespace where nspname = 'PG_CATALOG'), 8, 'c', 0, -1, 0, 'i'),             
(1082, 'DATE', 'date', (select oid from pg_catalog.pg_namespace where nspname = 'PG_CATALOG'), 4, 'c', 0, -1, 0, 'i'),             
(1114, 'TIMESTAMP', 'timestamp', (select oid from pg_catalog.pg_namespace where nspname = 'PG_CATALOG'), 8, 'c', 0, -1, 0, 'i'),              
(17, 'VARBINARY', 'bytea', (select oid from pg_catalog.pg_namespace where nspname = 'PG_CATALOG'), -1, 'c', 0, -1, 1, 'i'),               
(17, 'BINARY', 'bytea', (select oid from pg_catalog.pg_namespace where nspname = 'PG_CATALOG'), -1, 'c', 0, -1, 1, 'i'),                
(1111, 'NAME', 'name', (select oid from pg_catalog.pg_namespace where nspname = 'PG_CATALOG'), 64, 'c', 0, -1, 0, 'i'),                 
(0, 'NULL', 'null', (select oid from pg_catalog.pg_namespace where nspname = 'PG_CATALOG'), -1, 'c', 0, -1, 0, 'i');                

-- grant permission to pg_type:
GRANT SELECT ON pg_catalog.pg_type TO PUBLIC;

-- pg_class view:
create view pg_catalog.pg_class -- (oid, relname, relnamespace, relkind, relam, reltuples, relpages, relhasrules, relhasoids)
as
select
    pg_catalog.mofIdToInteger(mof_id) as oid,
    table_name as relname,
    (select oid from pg_catalog.pg_namespace where nspname = schema_name) relnamespace,
    case table_type when 'LOCAL TABLE' then 'r' else 'v' end relkind,
    0 relam,
    cast(0 as float) reltuples,
    0 relpages,
    false relhasrules,
    false relhasoids
from sys_root.dba_tables where table_type in ('LOCAL VIEW', 'LOCAL TABLE') and schema_name not in ('SYS_BOOT', 'SQLJ', 'JDBC_METADATA', 'INFORMATION_SCHEMA', 'PG_CATALOG', 'SYS_ROOT', 'MGMT', 'APPLIB');

-- grant permission to pg_class:
GRANT SELECT ON pg_catalog.pg_class TO PUBLIC;

-- pg_proc table:
create table pg_catalog.pg_proc
(
    oid int,
    proname varchar(128),
    prorettype int,
    pronamespace int
);

-- grant permission to pg_proc:
GRANT SELECT ON pg_catalog.pg_proc TO PUBLIC;

-- pg_trigger table:
create table pg_catalog.pg_trigger
(
    oid int,
    tgconstrrelid int,
    tgfoid int,
    tgargs int,
    tgnargs int,
    tgdeferrable boolean,
    tginitdeferred boolean,
    tgconstrname varchar(128),
    tgrelid int
);

-- grant permission to pg_trigger:
GRANT SELECT ON pg_catalog.pg_trigger TO PUBLIC;

-- pg_attrdef view:
create view pg_catalog.pg_attrdef -- (oid, adsrc, adrelid, adnum)
as
select
    oid oid,
    0 adsrc,
    0 adrelid,
    0 adnum
from pg_catalog.pg_namespace where oid = -1;   

-- grant permission to pg_attrdef:
GRANT SELECT ON pg_catalog.pg_attrdef TO PUBLIC;

-- pg_attribute view:
create view pg_catalog.pg_attribute -- (oid, attrelid, attname, atttypid, attlen, attnum, atttypmod, attnotnull, attisdropped, atthasdef)
as
select 
  pg_catalog.mofIdToInteger(t.mof_id) as oid,
  (select pg_catalog.mofIdToInteger(t1.mof_id) from sys_root.dba_tables as t1 where t1.catalog_name = t.catalog_name and t1.schema_name = t.schema_name and t1.table_name = t.table_name) as attrelid,
  t.column_name as attname,
  (select oid from pg_catalog.pg_type where typname = t.datatype) as atttypid,
  -1 attlen,
  t.ordinal_position attnum,
  -1 atttypmod,
  false attnotnull,
  false attisdropped,
  false atthasdef
from sys_root.dba_columns t where t.schema_name not in ('SYS_BOOT', 'SQLJ', 'JDBC_METADATA', 'INFORMATION_SCHEMA', 'PG_CATALOG', 'SYS_ROOT', 'MGMT', 'APPLIB');

-- grant permission to pg_attribute:
GRANT SELECT ON pg_catalog.pg_attribute TO PUBLIC;

-- pg_index view:
create view pg_catalog.pg_index -- (oid, indexrelid, indrelid, indisclustered, indisunique, indisprimary, indexprs, indkey)
as
select
    0 oid,
    0 indexrelid,
    0 indrelid,
    false indisclustered,
    false indisunique,
    false indisprimary,
    '' indexprs,
    0 indkey
from pg_catalog.pg_namespace where oid = -1;

-- grant permission to pg_index:
GRANT SELECT ON pg_catalog.pg_index TO PUBLIC;

-- pg_get_indexdef function:
create function pg_catalog.pg_get_indexdef(indexId integer, ordinalPosition integer, pretty boolean)
returns varchar(128)
contains sql
return cast(NULL as varchar(128));

-- grant permission to pg_get_indexdef function:
GRANT EXECUTE ON SPECIFIC FUNCTION pg_catalog.pg_get_indexdef TO PUBLIC;

-- version function:
create function pg_catalog.version() returns varchar(128)
contains sql
return 'PostgreSQL 8.1.4 server protocol using LucidDB';

-- grant permission to version function:
GRANT EXECUTE ON SPECIFIC FUNCTION pg_catalog.version TO PUBLIC;

-- pg_encoding_to_char function:
create function pg_catalog.pg_encoding_to_char(code int) returns varchar(128)
contains sql
return
case code when 0 then 'SQL_ASCII'
when 6 then 'UTF8'
when 8 then 'LATIN1'
else case when code < 40 then 'UTF8' else '' end end; 

-- grant permission to pg_encoding_to_char function:
GRANT EXECUTE ON SPECIFIC FUNCTION pg_catalog.pg_encoding_to_char TO PUBLIC;

-- pg_postmaster_start_time function:
create function pg_catalog.pg_postmaster_start_time() returns timestamp
contains sql
return current_timestamp;
                                                                                             
-- grant permission to pg_postmaster_start_time function:
GRANT EXECUTE ON SPECIFIC FUNCTION pg_catalog.pg_postmaster_start_time TO PUBLIC;

-- has_database_privilege function:
create function has_database_privilege(id int, privilege varchar(255)) returns boolean
contains sql
return true;

-- grant execute permission to has_database_privilege function:
GRANT EXECUTE ON SPECIFIC FUNCTION pg_catalog.has_database_privilege TO PUBLIC;

-- has_table_privilege function:
create function has_table_privilege(tableName varchar(255), privilege varchar(255)) returns boolean
contains sql
return true;

-- grant execute permission to has_table_privilege function:
GRANT EXECUTE ON SPECIFIC FUNCTION pg_catalog.has_table_privilege TO PUBLIC;

-- currtid2 function:
create function currtid2(tableName varchar(255), id varchar(255)) returns int
contains sql
return 1;

-- grant execute permission to currtid2 function:
GRANT EXECUTE ON SPECIFIC FUNCTION pg_catalog.currtid2 TO PUBLIC;

-- get user by id:
create function pg_get_userbyid(input int)
returns varchar(255)
language java
reads sql data
external name 'pg_catalog.pg_catalog_jar:org.luciddb.pg2luciddb.pg_catalog_plugin.getUserById';

-- grant permission to pg_get_userbyid function:
GRANT EXECUTE ON SPECIFIC FUNCTION pg_catalog.pg_get_userbyid TO PUBLIC;

-- dummy procedure:
create procedure dummy_procedure(input varchar(255))
language java
no sql
external name 'pg_catalog.pg_catalog_jar:org.luciddb.pg2luciddb.pg_catalog_plugin.dummyProcedure';

-- grant permission to pg_get_userbyid function:
GRANT EXECUTE ON SPECIFIC PROCEDURE pg_catalog.dummy_procedure TO PUBLIC;

-- internal_columns_view:
create view internal_columns_view as
select 
  t1.table_schem as nspname, t1.table_name as relname, c."name" as attname, t2.oid as atttypeid, t2.pgtypname as typname,
  c."ordinal" + 1 as attnum, -1 as attlen, 
  (case dt."name" 
    --when 'VARCHAR' then coalesce(c."length",c."precision") + 4
    --when 'CHAR' then coalesce(c."length",c."precision") + 4
    when 'BINARY' then coalesce(c."length",c."precision")
    when 'VARBINARY' then coalesce(c."length",c."precision")
    when 'NUMERIC' then (coalesce(c."length",c."precision") * 65536 + (case when c."scale" IS NULL then 0 else c."scale" end) + 4)
    when 'DECIMAL' then (coalesce(c."length",c."precision") * 65536 + (case when c."scale" IS NULL then 0 else c."scale" end) + 4)
    else -1
  end) as atttypemode,
  --(case c."isNullable" when 'columnNullable' then false else true end) as attnotnull, 
  false as attnotnull,
  false as relhasrules, 
  case t1."mofClassName" when 'LocalTable' then 'r' else 'v' end relkind,
  pg_catalog.mofIdToInteger(t1."mofId") as oid,
  cast(null as char(1)) as adsrc, 0, t2.typtypmod     
from sys_boot.jdbc_metadata.tables_view_internal t1
inner join sys_fem."SQL2003"."AbstractColumn" c on t1."mofId" = c."owner"
inner join sys_cwm."Relational"."SQLDataType" dt on c."type" = dt."mofId"
inner join pg_catalog.pg_type t2 on dt."name" = t2.typname
where t1.table_schem not in ('SYS_BOOT', 'SQLJ', 'JDBC_METADATA', 'INFORMATION_SCHEMA', 'PG_CATALOG', 'SYS_ROOT', 'MGMT', 'APPLIB');

-- grant rights:
GRANT SELECT ON pg_catalog.internal_columns_view TO PUBLIC;

-- internal_tables_view:
create view internal_tables_view as
select t1.table_name as relname, t1.table_schem as nspname, (case t1."mofClassName" when 'LocalTable' then 'r' else 'v' end) relkind, pg_catalog.mofIdToInteger(t1."mofId") as oid
from sys_boot.jdbc_metadata.tables_view_internal t1
where t1.table_schem not in ('SYS_BOOT', 'SQLJ', 'JDBC_METADATA', 'INFORMATION_SCHEMA', 'PG_CATALOG', 'SYS_ROOT', 'MGMT', 'APPLIB');

-- grant:
GRANT SELECT ON pg_catalog.internal_tables_view TO PUBLIC;
