-- This script creates a view schema used for database management 
                                                                                
!set verbose true
                                                                                
-- create views in system-owned schema sys_boot.mgmt
create schema sys_boot.mgmt;
set schema 'sys_boot.mgmt';
set path 'sys_boot.mgmt';

create function statements()
returns table(id int, session_id int, sql_stmt varchar(1024), create_time timestamp, parameters varchar(1024))
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.syslib.FarragoManagementUDR.statements';

create view statements_view as
  select * from table(statements());

-- todo:  grant this only to a privileged user
grant select on statements_view to public;

create function sessions()
returns table(id int, url varchar(128), current_user_name varchar(128), current_role_name varchar(128), session_user_name varchar(128), system_user_name varchar(128), catalog_name varchar(128), schema_name varchar(128), is_closed boolean, is_auto_commit boolean, is_txn_in_progress boolean)

language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.syslib.FarragoManagementUDR.sessions';

create view sessions_view as
  select * from table(sessions());

-- todo:  grant this only to a privileged user
grant select on sessions_view to public;

create function objectsInUse()
returns table(stmt_id int, mof_id varchar(32))

language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.syslib.FarragoManagementUDR.objectsInUse';

create view objects_in_use_view as
  select * from table(objectsInUse());

-- todo:  grant this only to a privileged user
grant select on objects_in_use_view to public;

