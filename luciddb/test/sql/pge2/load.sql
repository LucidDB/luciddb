--
-- load.sql - load PG&E data
--

insert into PGE.CUST_SERV_ACCT
select * from PGE.CUST_SERV_ACCT_SRC;
select count(*) from PGE.CUST_SERV_ACCT;

insert into PGE.GEOG
select * from PGE.GEOG_SRC;
select count(*) from PGE.GEOG;

insert into PGE.REVN_DTL_RAND
select * from PGE.REVN_DTL_RAND_SRC;
select count(*) from PGE.REVN_DTL_RAND;

insert into PGE.REVN_PRD
select * from PGE.REVN_PRD_SRC;
select count(*) from PGE.REVN_PRD;

insert into PGE.SERV_PLAN
select * from PGE.SERV_PLAN_SRC;
select count(*) from PGE.SERV_PLAN;

-- End load.sql
