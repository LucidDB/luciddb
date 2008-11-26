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
