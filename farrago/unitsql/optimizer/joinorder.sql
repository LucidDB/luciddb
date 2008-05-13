-- $Id$
-- Tests for ConvertMultiJoinRule and OptimizeJoinRule

create schema jo;
set schema 'jo';

-- set session personality to LucidDB so all tables
-- will be column-store by default and LucidDB-specific optimization rules
-- are picked up
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

create table f(f int, f_d1 int, f_d2 int, f_d3 int);
create table d1(d1 int, d1_f int, d1_d2 int, d1_d3 int);
create table d2(d2 int, d2_f int, d2_d1 int, d2_d3 int);
create table d3(d3 int, d3_f int, d3_d1 int, d3_d2 int);

create index if_d1 on f(f_d1);
create index if_d2 on f(f_d2);
create index if_d3 on f(f_d3);
create index id1_f on d1(d1_f);
create index id1_d2 on d1(d1_d2);
create index id1_d3 on d1(d1_d3);
create index id2_f on d2(d2_f);
create index id2_d1 on d2(d2_d1);
create index id2_d3 on d2(d2_d3);
create index id3_f on d3(d3_f);
create index id3_d1 on d3(d3_d1);
create index id3_d2 on d3(d3_d2);

insert into f values(0, 0, 0, 0);
insert into d1 values(1, 0, 1, 1);
insert into d2 values(2, 0, 1, 2);
insert into d3 values(3, 0, 1, 2);

-- fake stats
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'JO', 'F', 10000000);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'JO', 'D1', 100000);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'JO', 'D2', 1000);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'JO', 'D3', 10);

call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'JO', 'F', 'F_D1', 100000, 100, 100000, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'JO', 'F', 'F_D2', 1000, 100, 1000, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'JO', 'F', 'F_D3', 10, 100, 10, 0, '0123456789');

call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'JO', 'D1', 'D1_F', 100000, 100, 100000, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'JO', 'D1', 'D1_D2', 1000, 100, 1000, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'JO', 'D1', 'D1_D3', 10, 100, 10, 0, '0123456789');

call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'JO', 'D2', 'D2_F', 1000, 100, 1000, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'JO', 'D2', 'D2_D1', 1000, 100, 1000, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'JO', 'D2', 'D2_D3', 10, 100, 10, 0, '0123456789');

call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'JO', 'D3', 'D3_F', 10, 100, 10, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'JO', 'D3', 'D3_D1', 10, 100, 10, 0, '0123456789');
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB', 'JO', 'D3', 'D3_D2', 10, 100, 10, 0, '0123456789');

!set outputformat csv

--------------------------------------------------------------------
-- Test different combinations of patterns into ConvertMultiJoinRule
--------------------------------------------------------------------

-- MJ/MJ
explain plan for select f, d1, d2, d3
    from (select * from f, d1 where f.f_d1 = d1.d1_f and d1.d1 >= 0) j1,
        (select * from d2, d3 where d2.d2_d3 = d3.d3_d2 and d3.d3 >= 0) j2
    where j1.f_d2 = j2.d2_f and j2.d2 >= 0;

-- MJ/RS
explain plan for select f, d1, d2
    from (select * from f, d1 where f.f_d1 = d1.d1_f and d1.d1 >= 0) j, d2
    where j.f_d2 = d2.d2_f and d2.d2 >= 0;

-- MJ/FRS
explain plan for select f, d1, d2
    from (select * from f, d1 where f.f_d1 = d1.d1_f and d1.d1 >= 0) j, d2
    where j.f_d2 = d2.d2_f and d2.d2 + 0 = 2;

-- RS/MJ
explain plan for select f, d1, d2
    from f, (select * from d1, d2 where d1.d1_d2 = d2.d2_d1 and d2.d2 >= 0) j
    where f.f_d1 = j.d1_f and j.d1 >= 0;

-- RS/RS
explain plan for select f, d1 from f, d1 where f.f_d1 = d1.d1_f and d1.d1 >= 0;

-- RS/FRS
explain plan for select f, d2 from f, d2
    where f.f_d2 = d2.d2_f and d2.d2 + 0 = 2;

-- FRS/MJ
explain plan for select f, d1, d3
    from f, (select * from d1, d3 where d1.d1_d3 = d3.d3_d1 and d3.d3 >= 0) j
    where f.f_d1 = j.d1_f and j.d1 + 0 = 1;

-- FRS/RS
explain plan for select f, d1 from f, d1
    where f.f_d1 = d1.d1_f and f.f + 0 = 0 and d1.d1 >= 0;

-- FRS/FRS
explain plan for select f, d2 from f, d2
    where f.f_d2 = d2.d2_f and f.f + 0 = 0 and d2.d2 + 0 = 2;

------------------------------------------------------
-- different join combinations, including corner cases
------------------------------------------------------
-- no join filters
explain plan for select f, d1, d2, d3 
    from f, d1, d2, d3;

-- non-comparsion expression
explain plan for select f, d1 from f, d1
    where f.f_d1 = d1.d1_f or f.f_d2 = d1.d1_f;

-- filter involving > 2 tables
explain plan for select f, d1, d2 from f, d1, d2
    where f.f_d1 + d1.d1_f = d2.d2_f;

-- filter with 2 tables but both on one side of the comparison operator
explain plan for select f, d1 from f, d1
    where f.f_d1 + d1.d1_f = 0;

-- non-equality operator
explain plan for select f, d1 from f, d1
    where f.f_d1 >= d1.d1_f;

-- all possible join combinations
explain plan for select f, d1, d2, d3
    from d3, d1, d2, f
    where
        d1.d1_f = 1 and d2.d2_f = 2 and d3.d3_f = 3 and
        f.f_d1 = d1.d1_f and f.f_d2 = d2.d2_f and f.f_d3 = d3.d3_f and
        d1.d1_d2 = d2.d2_d1 and d1.d1_d3 = d3.d3_d1 and
        d2.d2_d3 = d3.d3_d2;

------------------------
-- run the queries above
------------------------
!set outputformat table

select f, d1, d2, d3
    from (select * from f, d1 where f.f_d1 = d1.d1_f and d1.d1 >= 0) j1,
        (select * from d2, d3 where d2.d2_d3 = d3.d3_d2 and d3.d3 >= 0) j2
    where j1.f_d2 = j2.d2_f and j2.d2 >= 0;

select f, d1, d2
    from (select * from f, d1 where f.f_d1 = d1.d1_f and d1.d1 >= 0) j, d2
    where j.f_d2 = d2.d2_f and d2.d2 >= 0;

select f, d1, d2
    from (select * from f, d1 where f.f_d1 = d1.d1_f and d1.d1 >= 0) j, d2
    where j.f_d2 = d2.d2_f and d2.d2 + 0 = 2;

select f, d1, d2
    from f, (select * from d1, d2 where d1.d1_d2 = d2.d2_d1 and d2.d2 >= 0) j
    where f.f_d1 = j.d1_f and j.d1 >= 0;

select f, d1 from f, d1 where f.f_d1 = d1.d1_f and d1.d1 >= 0;

select f, d2 from f, d2
    where f.f_d2 = d2.d2_f and d2.d2 + 0 = 2;

select f, d1, d3
    from f, (select * from d1, d3 where d1.d1_d3 = d3.d3_d1 and d3.d3 >= 0) j
    where f.f_d1 = j.d1_f and j.d1 + 0 = 1;

select f, d1 from f, d1
    where f.f_d1 = d1.d1_f and f.f + 0 = 0 and d1.d1 >= 0;

select f, d2 from f, d2
    where f.f_d2 = d2.d2_f and f.f + 0 = 0 and d2.d2 + 0 = 2;

select f, d1, d2, d3 
    from f, d1, d2, d3;

select f, d1 from f, d1
    where f.f_d1 = d1.d1_f or f.f_d2 = d1.d1_f;

select f, d1, d2 from f, d1, d2
    where f.f_d1 + d1.d1_f = d2.d2_f;

select f, d1 from f, d1
    where f.f_d1 + d1.d1_f = 0;

select f, d1 from f, d1
    where f.f_d1 >= d1.d1_f;

select f, d1, d2, d3
    from f, d1, d2, d3
    where
        f.f_d1 = d1.d1_f and f.f_d2 = d2.d2_f and f.f_d3 = d3.d3_f and
        d1.d1_d2 = d2.d2_d1 and d1.d1_d3 = d3.d3_d1 and
        d2.d2_d3 = d3.d3_d2;

------------
-- Misc Bugs
------------
-- LDB-65 -- need to handle self-joins
-- no need for any actual data, as the bug appears during optimization time
create table EMP (
  EMPNO numeric(5,0),
  FNAME varchar(20),
  LNAME varchar(20),
  SEX char(1),
  DEPTNO integer,
  MANAGER numeric(5,0),
  LOCID CHAR(2),
  SAL integer,
  COMMISSION integer,
  HOBBY varchar(20)
);

create table DEPT (
  DEPTNO integer,
  DNAME varchar(20),
  LOCID CHAR(2)
);

create table LOCATION(
  LOCID char(2),
  STREET varchar(50),
  CITY varchar(20),
  STATE char(2),
  ZIP numeric(5,0)
);

!set outputformat csv

explain plan for
select EMP.LNAME, DEPT.DNAME
    from EMP, DEPT, LOCATION EL
    where EL.LOCID = EMP.LOCID and EL.LOCID=DEPT.LOCID;

explain plan for
select EMP.LNAME, DEPT.DNAME
    from EMP, DEPT, LOCATION EL, LOCATION DL
    where EL.LOCID = EMP.LOCID and DL.LOCID=DEPT.LOCID;

-- tables needed for complex select below; taken from pge query 1

create table CUST_SERV_ACCT(CUST_SERV_ACCT_KEY DECIMAL(10,0),
 CU_ID DECIMAL(10,0),
 CU_TYP_IND CHAR(1),
 CUOR_CUST_NM VARCHAR(50),
 ORGDTL_STR_ID_TXT VARCHAR(10),
 ORGDTL_NM VARCHAR(50),
 ORGDTL_DBA_NM VARCHAR(18),
 CUIN_LST_NM VARCHAR(25),
 CUIN_FST_NM VARCHAR(25),
 CUIN_MID_INIT_TXT CHAR(1),
 CA_ID DECIMAL(10,0),
 MAIL_ADDR_ID DECIMAL(10,0),
 SITE_ID DECIMAL(10,0),
 SERV_ADDR VARCHAR(40),
 MNCPLT_NM VARCHAR(25),
 ST_CD CHAR(2),
 ZIP_CD_NUM CHAR(5),
 CO_CD CHAR(3),
 GEOG_KEY DECIMAL(10,0),
 SITUSE_CD CHAR(2),
 SAA_ID DECIMAL(10,0),
 SAA_SEQ_NUM DECIMAL(5,0),
 SRVPLN_KEY DECIMAL(10,0),
 CYC_FREQY_CD CHAR(2),
 ACYC_NUM CHAR(2),
 CNTLUT_NUM DECIMAL(10,0),
 IS_DWLGS_VAL DECIMAL(5,0),
 SERV_SIC_CD CHAR(4),
 BU_CD CHAR(3),
 UTIL_TYP_CD CHAR(2),
 SAA_SPLINSTL_INDCR CHAR(1),
 SAA_CLRG_ACCT_ID DECIMAL(10,0),
 SAA_ST_TX_EXM_INDC CHAR(1),
 IS_SERV_INSTLD_DT TIMESTAMP,
 RCD_EFF_DT TIMESTAMP,
 RCD_END_DT TIMESTAMP,
 PROC_BAT_ID DECIMAL(10,0)) 
;
create table GEOG(GEOG_KEY DECIMAL(10,0),
 DPT_CD CHAR(3),
 RVTWN_CD CHAR(3),
 RVTWN_DESC VARCHAR(30),
 GEOG_CNTY_CD CHAR(2),
 GEOG_CNTY_DESC VARCHAR(30),
 ST_CD CHAR(2),
 ST_DESC VARCHAR(30),
 PROC_BATCH_ID VARCHAR(10)) 
;
create table REVN_DTL_RAND(CUST_SERV_ACCT_KEY DECIMAL(10,0),
 GEOG_KEY DECIMAL(10,0),
 SRVPLN_KEY DECIMAL(10,0),
 REVN_YR_MO DECIMAL(6,0),
 USG_VAL DOUBLE,
 USG_BILL_THERM DOUBLE,
 USG_BILL_KWH DOUBLE,
 USGC_BILL_KW_VAL DOUBLE,
 RSD_SVC_BILG_AMT DOUBLE,
 RSD_ST_SLSTX_AMT DOUBLE,
 RSD_TRSPT_CHRG_AMT DOUBLE,
 RSD_BALG_CHRG_AMT DOUBLE,
 RSD_TOP_SRCHRG_AMT DOUBLE,
 RSD_CCOGA_AMT DOUBLE,
 RSD_CCOGC_AMT DOUBLE,
 RSD_CUST_CHRG_AMT DOUBLE,
 RSD_DMND_CHRG_AMT DOUBLE,
 RSD_CU_CNT DECIMAL(5,0),
 SAS_FNL_BIL_INDCR CHAR(1),
 SAS_TYP_CD CHAR(1),
 USG_DAYS_NUM DECIMAL(5,0),
 USG_STRT_DT TIMESTAMP,
 USG_END_DT TIMESTAMP,
 CISPD_DT TIMESTAMP,
 PROC_BAT_ID DECIMAL(10,0)) 
;
create table REVN_PRD(REVN_YR_MO DECIMAL(6,0),
 REVN_YR_MO_DESC VARCHAR(14),
 REVN_MO DECIMAL(2,0),
 REVN_MO_DESC VARCHAR(9),
 REVN_YR DECIMAL(4,0),
 REVN_QTR CHAR(1),
 REVN_QTR_YR CHAR(7),
 REVN_QTR_YR_DESC VARCHAR(20),
 USER_ID CHAR(4)) 
;
create table SERV_PLAN(SRVPLN_KEY DECIMAL(10,0),
 SRVPLN_ID DECIMAL(10,0),
 SRVPLN_NM VARCHAR(25),
 SPO_ID DECIMAL(10,0),
 SPO_NM VARCHAR(25),
 BU_CD CHAR(3),
 BU_DESC VARCHAR(30),
 RC_CD CHAR(2),
 RC_DESC VARCHAR(30),
 PROC_BAT_ID DECIMAL(10,0)) 
;

-- this query exercises the case where merge projections are required as
-- joins are being converted to MultiJoinRels and projections are pulled up;
-- the resulting query plan should NOT contain cartesian product joins
explain plan for
SELECT DISTINCT AL1.CUST_SERV_ACCT_KEY
   FROM
    CUST_SERV_ACCT AL1,
    GEOG AL2,
    SERV_PLAN AL3,
    REVN_PRD AL4,
    REVN_DTL_RAND AL5
   WHERE AL5.REVN_YR_MO=AL4.REVN_YR_MO
    AND AL2.GEOG_KEY=AL5.GEOG_KEY
    AND AL3.SRVPLN_KEY=AL5.SRVPLN_KEY
    AND AL1.CUST_SERV_ACCT_KEY=AL5.CUST_SERV_ACCT_KEY
    AND AL5.SAS_FNL_BIL_INDCR='Y' AND AL4.REVN_YR=1995;

-- this query exercises ensuring that all projections are merged before
-- converting joins to MultiJoinRels; the resulting query plan
-- should NOT contain cartesian product joins
explain plan for
SELECT AL1.CU_ID, AL4.REVN_YR_MO, SUM(AL5.USG_VAL),
 SUM(AL5.USG_BILL_THERM), SUM(AL5.USGC_BILL_KW_VAL),
 SUM(AL5.RSD_SVC_BILG_AMT), AL5.RSD_CU_CNT
FROM
 CUST_SERV_ACCT AL1,
 GEOG AL2,
 SERV_PLAN AL3,
 REVN_PRD AL4,
 REVN_DTL_RAND AL5
WHERE (AL5.REVN_YR_MO=AL4.REVN_YR_MO
 AND AL2.GEOG_KEY=AL5.GEOG_KEY AND AL3.SRVPLN_KEY=AL5.SRVPLN_KEY
 AND AL1.CUST_SERV_ACCT_KEY=AL5.CUST_SERV_ACCT_KEY)
 AND (AL2.ST_CD='IN' AND AL2.GEOG_CNTY_DESC='LAPORTE'
 AND AL4.REVN_YR=1995 AND AL3.SRVPLN_NM LIKE '%DUSK%'
 AND AL3.SPO_NM LIKE '%SODIUM%'
 AND AL3.RC_DESC='RESIDENTIAL - GENERAL SERVICE')
GROUP BY AL1.CU_ID, AL4.REVN_YR_MO, AL5.RSD_CU_CNT;


-- LER-3639 -- If this query is not properly optimized, a cartesian join is
-- incorrectly chosen.
create table t1(t1a char(10));
create table t2(t2a char(10));
create table t3(t3a char(10));
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'JO', 'T1', 4841);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'JO', 'T2', 25199);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'JO', 'T3', 25199);
explain plan for
select * from (select t1a||t2a as a from t1, t2 where t1a = t2a) as x, t3
where t3a = a;

-- same query as above except without the subquery in the from clause
explain plan for
select * from t1, t2, t3 where t1a = t2a and t1a||t2a = t3a;

-- LER-7778 -- Likewise for this query.  It should not result in a cartesian
-- join.
create table tab1(c1 char(1), c2 char(2), c3 char(3));
create table tab2(c1 char(1), c2 char(2), c3 char(3));
create table tab3(c1 char(1), c2 char(2), c3 char(3));
create view vtab1 as
    select cast(c1 as char(5)) as c1, cast(c2 as char(5)), cast(c3 as char(5))
    from tab1;
explain plan for
select vtab1.c1 from vtab1, tab2, tab3 where
    vtab1.c1 = tab3.c2 and tab2.c2 = tab3.c3;

-- LER-7807 -- Tables A and B should be joined together before joining with C.
-- This allows the joins to be completely processed using hash joins without
-- post-processing of the filter referencing all 3 tables.
create table A(a int, b int, c int);
create table B(a int, b int, c int);
create table C(a int, b int, c int);

call sys_boot.mgmt.stat_set_row_count('LOCALDB','JO','A',1000);
call sys_boot.mgmt.stat_set_row_count('LOCALDB','JO','B',500);
call sys_boot.mgmt.stat_set_row_count('LOCALDB','JO','C',400);

explain plan for
select * from A, B, C
where A.a = B.a and
A.b + B.b = C.b and
A.c = C.c;

