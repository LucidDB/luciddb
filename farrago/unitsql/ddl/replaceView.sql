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
CREATE OR REPLACE VIEW v AS SELECT 1 AS i, 2 AS j FROM (VALUES (TRUE));
CREATE OR REPLACE VIEW v2 AS SELECT *, 999 AS k   FROM v;
CREATE OR REPLACE VIEW v2x AS SELECT k FROM v2;

-- expect "*" to have been expanded, rest of view (including spacing) lexically unchanged
SELECT "originalDefinition" FROM sys_fem."SQL2003"."LocalView" WHERE "name" = 'V2';
SELECT * FROM v2;
CREATE OR REPLACE VIEW v AS SELECT 3 AS i, 2 AS j FROM (VALUES (TRUE));
-- V2 has same definition, but reflects new data
SELECT "originalDefinition" FROM sys_fem."SQL2003"."LocalView" WHERE "name" = 'V2';
SELECT * FROM v2;

-- add a column to underlying view V, and V2 is still valid, but does not gain column
CREATE OR REPLACE VIEW v AS SELECT 1 AS j, 5 AS k, 2 AS i FROM (VALUES (FALSE));
SELECT "originalDefinition" FROM sys_fem."SQL2003"."LocalView" WHERE "name" = 'V2';
SELECT * FROM v2;

-- incompatable change to underlying view V, and queries to V2 (and dependent view V2X) fail
CREATE OR REPLACE VIEW v AS SELECT 1 AS x, 5 AS k FROM (VALUES (FALSE));
SELECT "originalDefinition" FROM sys_fem."SQL2003"."LocalView" WHERE "name" = 'V2';
SELECT * FROM v2;
SELECT * FROM v2x;

-- select at start of line, star at end of line
CREATE OR REPLACE VIEW v2b AS
SELECT 1 as i, *
, 'abc' AS k FROM (VALUES ('xxx')) AS t(q);
SELECT "originalDefinition" FROM sys_fem."SQL2003"."LocalView" WHERE "name" = 'V2B';

-- should expand "relation.*" in select clause, but not change qualified and
-- unqualified column refs
CREATE OR REPLACE VIEW v3 AS SELECT t.*, 999 AS k, t.i as z, j AS y FROM v AS t;
SELECT "originalDefinition" FROM sys_fem."SQL2003"."LocalView" WHERE "name" = 'V3';
SELECT * FROM v3;

-- should not expand "*" in subquery
CREATE OR REPLACE VIEW v4 AS SELECT j, i FROM (select * FROM v);
SELECT "originalDefinition" FROM sys_fem."SQL2003"."LocalView" WHERE "name" = 'V4';
SELECT * FROM v4;

-- should not expand "relation.*" in subquery
CREATE OR REPLACE VIEW v5 AS SELECT j, i FROM (select t.* FROM v AS t);
SELECT "originalDefinition" FROM sys_fem."SQL2003"."LocalView" WHERE "name" = 'V5';
SELECT * FROM v5;

-- "*" in all branches of union are expanded
CREATE OR REPLACE VIEW v6 AS
  SELECT * FROM v
  UNION ALL
  SELECT * FROM v AS t WHERE i < 3
  UNION ALL
  SELECT * FROM dtbug1173.v  WHERE v.i + j < 4;
SELECT "originalDefinition" FROM sys_fem."SQL2003"."LocalView" WHERE "name" = 'V6';
SELECT * FROM v6;

-- Change V in a way that will break multiple dependent views; expect an error,
-- and V's definition should be unchanged. (Behavior is different in SQLstream:
-- SQLstream accepts the change, and marks dependents as invalid.)
CREATE OR REPLACE VIEW v AS SELECT * FROM (VALUES (1)) AS t(aaa);
SELECT "originalDefinition" FROM sys_fem."SQL2003"."LocalView" WHERE "name" = 'V';
SELECT * FROM v;
SELECT * FROM v2;
SELECT * FROM v3;
SELECT * FROM v4;
SELECT * FROM v5;
SELECT * FROM v6;

-- Create view on top of two relations, then alter the relations so that the view
-- would now be invalid due to an ambiguous column.
CREATE VIEW va AS SELECT * FROM (VALUES (1, 2)) AS t(i, j);
CREATE VIEW vb AS SELECT * FROM (VALUES (2, 3)) AS t(x, y);
CREATE VIEW vc AS
  SELECT i, x FROM (
    SELECT * FROM va, vb WHERE j = x) AS z;
SELECT "originalDefinition" FROM sys_fem."SQL2003"."LocalView" WHERE "name" = 'VC';
SELECT * FROM vc;
-- expect error 'column ... is ambiguous'
CREATE OR REPLACE VIEW vb AS SELECT * FROM (VALUES (2, 4)) AS t(x, j);
-- vc def should be unchanged
SELECT "originalDefinition" FROM sys_fem."SQL2003"."LocalView" WHERE "name" = 'VC';




-- repeat, this time with LucidDB personality, which eschews
-- the view text preservation since it causes spurious exceptions
-- in the log

alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

CREATE OR REPLACE SCHEMA ler_7331;
SET SCHEMA 'ler_7331';
CREATE OR REPLACE VIEW v AS SELECT 1 AS i FROM (VALUES (TRUE));
CREATE OR REPLACE VIEW v9 AS SELECT *, 999 AS k FROM v;
-- original text not changed yet
SELECT "originalDefinition" FROM sys_fem."SQL2003"."LocalView" WHERE "name" = 'V9';
CREATE OR REPLACE VIEW v AS SELECT 1 AS i FROM (VALUES (TRUE));
-- expect original view text to be changed
SELECT "originalDefinition" FROM sys_fem."SQL2003"."LocalView" WHERE "name" = 'V9';

-- End replaceView.sql
