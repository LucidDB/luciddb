-- $Id$
-- Test catalog access

create schema "S";

create table "S"."T" (i int not null primary key);

!metadata getCatalogs

-- okay
select * from "LOCALDB"."S"."T";

-- nonexistent catalog
select * from "NONEXISTENT"."S"."T";

-- nonexistent catalog
create table "NONEXISTENT"."S"."T" (i int not null primary key);

create catalog universe description 'all kinds of stuff';

-- should fail:  duplicate name
create catalog universe description 'all kinds of stuff';

-- should fail:  clashes with a data server name
create catalog hsqldb_demo;

!metadata getCatalogs

create schema universe.s;

create table universe.s.t(i int not null primary key);

insert into s.t values (5);

insert into universe.s.t values (6);

select * from s.t;

select * from universe.s.t;

set catalog 'universe';

select * from s.t;

set catalog 'localdb';

-- should fail
drop catalog universe;

drop catalog universe cascade;

!metadata getCatalogs
