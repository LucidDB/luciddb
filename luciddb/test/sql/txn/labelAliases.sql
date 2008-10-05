-- $Id$
-- Tests LucidDB support for label aliases

---------------------------------------------------------------------------------- Setup labels and their aliases for different points-in-time
--------------------------------------------------------------------------------

create schema "labelAlias";
set schema '"labelAlias"';

create table t(a int);
create label l0 description 'empty table';

insert into t values(1);
create label l1 description 'one row';

insert into t values(2);
create label l2 description 'two rows';

insert into t values(3);
create label l3 description 'three rows';

insert into t values(4);

create label l0alias from label l0 description 'L0 alias';
create label l1alias from label l1 description 'L1 alias';
create label l2alias from label l2 description 'L2 alias';
create label l3alias from label l3;
create label chainedAlias from label l0alias description 'chained alias';
create label doublyChainedAlias from label chainedAlias;

select label_name, parent_label_name, remarks from sys_root.dba_labels
    order by label_name;
-- make sure the aliases have null csn's
select label_name, csn from sys_root.dba_labels
    where parent_label_name is not null order by label_name;

--------------------------------------------------------------------------------
-- Set the various label aliases and validate the data returned
--------------------------------------------------------------------------------

-- default case
select * from t order by a;

alter session set "label" = 'L0ALIAS';
select * from t order by a;

alter session set "label" = 'L3ALIAS';
select * from t order by a;

alter session set "label" = 'L2ALIAS';
select * from t order by a;

alter session set "label" = 'L1ALIAS';
select * from t order by a;

alter session set "label" = 'CHAINEDALIAS';
select * from t order by a;

alter session set "label" = 'DOUBLYCHAINEDALIAS';
select * from t order by a;

--------------------------------------------------------------------------------
-- Recreate labels and make sure their aliases "follow" the new labels
--------------------------------------------------------------------------------

alter session set "label" = null;
create or replace label l0;
select label_name, parent_label_name, remarks from sys_root.dba_labels
    order by label_name;

-- the following selects should now return 4 rows
alter session set "label" = 'L0';
select * from t order by a;
alter session set "label" = 'L0ALIAS';
select * from t order by a;
alter session set "label" = 'CHAINEDALIAS';
select * from t order by a;
alter session set "label" = 'DOUBLYCHAINEDALIAS';
select * from t order by a;

alter session set "label" = null;
create or replace label chainedAlias from label l2alias
    description 'new chained label alias';
select label_name, parent_label_name, remarks from sys_root.dba_labels
    order by label_name;

-- the following selects should now return 2 rows
alter session set "label" = 'CHAINEDALIAS';
select * from t order by a;
alter session set "label" = 'DOUBLYCHAINEDALIAS';
select * from t order by a;

-- make sure the other labels are unchanged
alter session set "label" = 'L1ALIAS';
select * from t order by a;
alter session set "label" = 'L2ALIAS';
select * from t order by a;
alter session set "label" = 'L3ALIAS';
select * from t order by a;
