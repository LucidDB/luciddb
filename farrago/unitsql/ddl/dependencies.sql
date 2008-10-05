-- $Id$
-- Test that dependency suppliers are not re-validated when a new dependency
-- client is added.

create schema igor;
set path 'sys_boot.mgmt';

create table igor.t(i int not null primary key);

create table igor.audit_trail(j int not null primary key, ts varchar(128));

-- Copy initial timestamp into audit trail
insert into igor.audit_trail
select 1, "modificationTimestamp" from sys_fem."MED"."StoredTable" 
where "name"='T';

-- Make sure at least one second elapses or else we might not notice if T's
-- modificationTimestamp is modified.
values(sleep(1001));

-- V becomes dependent on T, with T as dependency supplier
create view igor.v as select * from igor.t;

-- Copy latest timestamp into audit trail
insert into igor.audit_trail
select 2, "modificationTimestamp" from sys_fem."MED"."StoredTable" 
where "name"='T';

-- Verify only a single timestamp (which indicates T was NOT revalidated)
select count(distinct ts) from igor.audit_trail;
