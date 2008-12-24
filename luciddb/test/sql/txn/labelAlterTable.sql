-- $Id$
-- Tests LucidDB warehouse labels combined with ALTER TABLE ADD COLUMN

create schema la;
set schema 'la';

create table t(a int);
insert into t values(1);
create label l;

alter table t add b int;
insert into t values(2,2);

alter session set "label" = 'L';
-- should only see column a
select * from t;

-- negative test:  should not be able to reference column b
select b from t;
