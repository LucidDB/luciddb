-- $Id$
-- Test effect on view's stored original text when a dependency is re-created.

create schema x;
create schema y;
create table x.t(i int not null primary key);
create table y.t(i int not null primary key);
insert into x.t values(100);
insert into y.t values(200);
set schema 'x';
create view y.q1 as select * from t;
create view y.q2 as select * from t union all select * from y.q1;
select * from y.q2;
set schema 'y';
create or replace view y.q1 as select * from t;
select * from y.q2;

!set outputformat csv
!set force true
CREATE OR REPLACE SCHEMA dtbug1173;
SET SCHEMA 'dtbug1173';
CREATE OR REPLACE VIEW v AS SELECT 1 AS i FROM (VALUES (TRUE));
CREATE OR REPLACE VIEW v2 AS SELECT *, 999 AS k FROM v;
CREATE OR REPLACE VIEW v AS SELECT 1 AS i FROM (VALUES (TRUE));
-- expect original view text to be unchanged
SELECT "originalDefinition" FROM sys_fem."SQL2003"."LocalView" WHERE "name" = 'V2';
SELECT * FROM v2;
CREATE OR REPLACE VIEW v AS SELECT 1 AS i, 2 AS j FROM (VALUES (TRUE));
-- must change V2's text because V's columns have changed
SELECT "originalDefinition" FROM sys_fem."SQL2003"."LocalView" WHERE "name" = 'V2';
SELECT * FROM v2;

-- repeat, this time with LucidDB personality, which eschews
-- the view text preservation since it causes spurious exceptions
-- in the log

alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

CREATE OR REPLACE SCHEMA ler_7331;
SET SCHEMA 'ler_7331';
CREATE OR REPLACE VIEW v AS SELECT 1 AS i FROM (VALUES (TRUE));
CREATE OR REPLACE VIEW v3 AS SELECT *, 999 AS k FROM v;
CREATE OR REPLACE VIEW v AS SELECT 1 AS i FROM (VALUES (TRUE));
-- expect original view text to be changed
SELECT "originalDefinition" FROM sys_fem."SQL2003"."LocalView" WHERE "name" = 'V3';

-- End replaceView.sql
