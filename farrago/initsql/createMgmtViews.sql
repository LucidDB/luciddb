-- This script creates a view schema used for database management 
                                                                                
!set verbose true
                                                                                
-- create views in system-owned schema sys_boot.mgmt
create schema sys_boot.mgmt;
set schema 'sys_boot.mgmt';
set path 'sys_boot.mgmt';

create function statements()
returns table(id int, sqlStmt varchar(1024), createTime timestamp, parameters varchar(1024))
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.syslib.FarragoManagementUDR.statements';

create view statements_view as
  select * from table(statements());

-- todo:  grant this only to a privileged user
grant select on statements_view to public;

create function sessions()
returns table(id int, url varchar(256), currentUserName varchar(256), currentRoleName varchar(256), sessionUserName varchar(256), systemUserName varchar(256), catalogName varchar(256), schemaName varchar(256), isClosed boolean, isAutoCommit boolean, isTxnInProgress boolean)

language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.syslib.FarragoManagementUDR.sessions';

create view sessions_view as
  select * from table(sessions());

-- todo:  grant this only to a privileged user
grant select on sessions_view to public;

create function objectsInUse()
returns table(stmtId int, mofId varchar(32))

language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.syslib.FarragoManagementUDR.objectsInUse';

create view objectsInUse_view as
  select * from table(objectsInUse());

-- todo:  grant this only to a privileged user
grant select on objectsInUse_view to public;

