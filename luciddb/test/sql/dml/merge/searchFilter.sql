set schema 'mergetest';


-- Create and populate tables with auto increment columns

create table EMPTEMP (
  EMPNO integer not null,
  FNAME varchar(15),
  LNAME varchar(15),
  SEX char(1),
  DEPTNO integer,
  DNAME varchar(15),
  MANAGER integer,
  MFNAME varchar(15),
  MLNAME varchar(15),
  LOCID char(2),
  CITY varchar(8),
  SAL decimal(10, 2),
  COMMISION decimal(10, 2),
  HOBBY varchar(25)
);

--
-- functions in search condition
--

-- f(t1.key) = g(t2.key)
-- restriction:f and g must accept only 1 argument
insert into emptemp (empno, locid, sal)
  (select empno, locid, sal from emp where empno<=105);

select empno, locid, city, sal from emptemp;

merge into emptemp es
  using
    (select * from emp e, location l
       where e.locid = l.locid) as temp
    on
-->>> functions accepting more than 1 argument not supported as of 6/27/06
--    es.empno*10 = temp.empno*10 and 
--    (substring(cast(es.empno as varchar(10)) from 1 for 3)
--     = 
--     substring(cast(temp.empno as varchar(11)) from 1 for 3))
--    substring(es.locid from 1 for 2) = substring(temp.locid from 1 for 2)
--    e.empno > temp.empno
      cast(es.empno as varchar(10)) = cast (temp.empno as varchar(10))
  when matched then 
    update set sal = es.sal + 1
  when not matched then
    insert (empno, locid, city, sal)
    values (temp.empno, temp.locid, temp.city, temp.sal + 2);

select empno, locid, city, sal from emptemp order by empno;
delete from emptemp;


-- test date related function
create table t1 (n1 date, n2 integer);
insert into t1 values (date'1999-01-01',10),(date'1999-01-02',20),
                      (date'1999-01-03',30),(date'1999-01-04',40);
create table t2 (m1 date, m2 integer);
insert into t2 values (date'1999-01-02',200),(date'1999-01-05',500);

select * from t1;

merge into t1
using t2 on
            applib.day_in_year(n1) = applib.day_in_year(m1) and
            applib.day_number_overall(n1) = applib.day_number_overall(m1) and
            n1 = m1
when matched then 
  update set n2 = m2 + 1
when not matched then
  insert (n1,n2) values (m1,m2+2);

select * from t1;
drop table t1;
drop table t2;


--
-- multiple-column keys
--
create table t1 (n1 integer, n2 integer, n3 integer);
insert into t1 values (1,1,11), (1,2,12), (2,2,20), (3,3,30);
create table t2 (m1 integer, m2 integer, m3 integer);
insert into t2 values (1,1,100), (2,2,200), (3,3,300), (4,4,400);

-- as of 6/27/06 this search condition is still buggy
-- Ex. search "on n1=m1 and n2=m2" and "on n1=m1 and not(n2<>m2)"
-- return different results

--merge into t1 using t2 on n1=m1 and not(n2<>m2)
--when matched then update set n3=0
--when not matched then insert (n1,n2,n3) values (m1,m2,m3);

--select * from t1;

-- join condition has 2 keys
merge into t1 using t2 on n1=m1 and n2=m2
when matched then update set n3=0
when not matched then insert (n1,n2,n3) values (m1,m2,m3);

select * from t1;


-- join condition has 1 key and a filter
-- note that the filter is applied on the left outer join of t2 and t1
delete from t1;
insert into t1 values (1,1,10), (2,2,20), (3,3,30);
merge into t1 using t2 on n1=m1 and n2=1
when matched then update set n3=0
when not matched then insert (n1,n2,n3) values (m1,m2,m3);

select * from t1;

-- other filters 
delete from t1;
insert into t1 values (1,1,10), (2,2,20), (3,3,30);
merge into t1 using t2 on n1=m1 and n2<>2
when matched then update set n3=0
when not matched then insert (n1,n2,n3) values (m1,m2,m3);

select * from t1;

delete from t1;
insert into t1 values (1,1,10), (2,2,20), (3,3,30);
merge into t1 using t2 on n1=m1 and m2=1
when matched then update set n3=0
when not matched then insert (n1,n2,n3) values (m1,m2,m3);

select * from t1;

delete from t1;
insert into t1 values (1,1,10), (2,2,20), (3,3,30);
merge into t1 using t2 on n1=m1 and m2<>2
when matched then update set n3=0
when not matched then insert (n1,n2,n3) values (m1,m2,m3);

select * from t1;

delete from t1;
insert into t1 values (1,1,10), (2,2,20), (3,3,30);
merge into t1 using t2 on n1=m1 and n2>1 and m2<>2
when matched then update set n3=0
when not matched then insert (n1,n2,n3) values (m1,m2,m3);

select * from t1;

delete from t1;
insert into t1 values (1,1,10), (2,2,20), (3,3,30);
merge into t1 using t2 on n1=m1 and m2>1 and m2<>2
when matched then update set n3=0
when not matched then insert (n1,n2,n3) values (m1,m2,m3);

select * from t1;

delete from t1;
insert into t1 values (1,1,10), (2,2,20), (3,3,30);
merge into t1 using t2 on n1=m1 and n2=m2 and m2>2
when matched then update set n3=0
when not matched then insert (n1,n2,n3) values (m1,m2,m3);

select * from t1;

delete from t1;
insert into t1 values (1,1,10), (2,2,20), (3,3,30);
merge into t1 using t2 on n1=m1 and m2>2 and m2=n2
when matched then update set n3=0
when not matched then insert (n1,n2,n3) values (m1,m2,m3);

select * from t1;


--
-- more than 1 result in search condition
--

-- ambiguity, should throw an exception (not yet handled as of 6/27/06)

--delete from t1;
--delete from t2;
--insert into t1 values (1,1,10),(2,2,20),(4,4,40);
--insert into t2 values (1,1,11),(1,2,12),(2,2,20),(3,3,30);
--merge into t1 using t2 on n1=m1
--when matched then update set n3=0
--when not matched then insert (n1,n2,n3) values (m1,m2,m3);

--select * from t1;

delete from t1;
delete from t2;
insert into t2 values (1,1,10),(2,2,20),(4,4,40);
insert into t1 values (1,1,11),(1,2,12),(2,2,20),(3,3,30);
merge into t1 using t2 on n1=m1
when matched then update set n3=0
when not matched then insert (n1,n2,n3) values (m1,m2,m3);

select * from t1;



drop table t1;
drop table t2;

drop table emptemp;