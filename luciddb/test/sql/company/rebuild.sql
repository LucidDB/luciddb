-- $Id$
-- 
-- rebuild.sql -- test rebuild
--

set schema 's';

create view index_page_counts as
select "name", "pageCount" from sys_fem."MED"."LocalIndex"
where "name" like '%RSALES%';

create table RSALES(
  CUSTID integer
  ,EMPNO integer
  ,PRODID integer
  ,PRICE numeric(3,2)
);

create index RSALES_PRICE on RSALES(PRICE);
create index RSALES_EMPNO on RSALES(EMPNO);
create index RSALES_PRODID on RSALES(PRODID);
create index RSALES_CUSTID on RSALES(CUSTID);

INSERT INTO RSALES SELECT * FROM ff_server."BCP".SALES;

-- a few self-inserts to make sure they don't break rebuild and to
-- make the table a bit larger
INSERT INTO RSALES SELECT * FROM RSALES;
INSERT INTO RSALES SELECT * FROM RSALES;
INSERT INTO RSALES SELECT * FROM RSALES;
INSERT INTO RSALES SELECT * FROM RSALES;

-- NOTE: commenting this out happens to check whether index only scan 
-- reopens dependent stream correctly
-- INSERT INTO RSALES SELECT * FROM RSALES;

analyze table rsales compute statistics for all columns;
select * from index_page_counts order by 1;

-- basic rebuild on a table without deleted entries
alter table s.rsales rebuild;

analyze table rsales compute statistics for all columns;
select * from index_page_counts order by 1;

-- deleted entries
delete from rsales 
where 
  mod(lcs_rid(custid), 2) = 0 or mod(lcs_rid(custid), 4) = 3
  or (lcs_rid(custid) > 500 and lcs_rid(custid) < 5000);

select count(*) from rsales;
select max(lcs_rid(custid)) from rsales;

analyze table rsales compute statistics for all columns;
select * from index_page_counts order by 1;

-- repeated rebuild, rebuild deleted entries
alter table s.rsales rebuild;

select max(lcs_rid(custid)) from rsales;

analyze table rsales compute statistics for all columns;
select * from index_page_counts order by 1;

-- make sure rebuild doesn't change a table's contents by altering sales
-- and running through the rest of the tests with a rebuilt table

-- insert duplicates and remove them
insert into sales select * from sales;
delete from sales where lcs_rid(custid) < 500 or lcs_rid(custid) >= 1500;
alter table sales rebuild;

select count(*) from sales;
select max(lcs_rid(custid)) from sales;
