set schema 'mergetest';


create table t1 (n1 integer, n2 integer);
insert into t1 values (1,10), (2,20), (3,30);
create table t2 (m1 integer, m2 integer);
insert into t2 values (1,100), (2,200), (4,400);

--
-- positive tests
--
-- insert column list ommitted
merge into t1 using t2 on n1 = m1
when matched then update set n2=m2
when not matched then insert values (m1, pow(m2,2));
select * from t1;
delete from t1;
insert into t1 values (1,10), (2,20), (3,30);

-- one when clause ommitted
merge into t1
using t2 on n1 = m1
when matched then update set n2=m2;
select * from t1;

merge into t1
using t2 on n1 = m1
when not matched then
  insert (n1,n2) values (m1,m2);
select * from t1;

-- merge on self
merge into t1 as tr
using t1 as rf on tr.n1=rf.n1
when matched then update set n1 = tr.n1 + 10;
select * from t1;

merge into t1 as tr
using t1 as rf on tr.n1=rf.n1
when matched then update set n1=rf.n1 - 10;
select * from t1;

merge into t1 as tr
using (select * from t1 rf1, t1 rf2 where rf1.n1=rf2.n1) as rf
on rf.n1=tr.n1
when matched then update set n2=rf.n2 + 1;
select * from t1;


--
-- negative tests
--
-- both when clauses ommitted
merge into t1 using t2 on n1 = m1;

-- more than 1 matched / not matched
merge into t1 using t2 on n1 = m1
when matched then update set n2=m2
when matched then update set n2=n2+1;

merge into t1 using t2 on n1 = m1
when not matched then insert (n1,n2) values (m1,m2)
when not matched then insert (n1,n2) values (m1,m2);

-- repeated insert column name  >>> JIRA FRG-156 filed
--merge into t1 using t2 on n1 = m1
--when matched then update set n2=m2
--when not matched then insert (n1,n1,n1) values (101,102,103);

-- diff number of insert values
merge into t1 using t2 on n1 = m1
when matched then update set n2=m2
when not matched then insert (n1,n2) values (101,102,103);
merge into t1 using t2 on n1 = m1
when matched then update set n2=m2
when not matched then insert (n1,n2) values (101);

-- should throw exception due to multiple matches
-- not yet handled
--merge into t1 as tr
--using (select n1 as n1, n2 as n2 from t1 union all
--       select m1 as n1, m2 as n2 from t2) as rf
--on rf.n1 = tr.n1
--when matched then update set n2 = tr.n2 
--when not matched then insert (n1,n2) values (rf.n1, rf.n2 + 2);
--select * from t1;



-- clean up
drop table t1;
drop table t2;