set schema 'mergetest';


create table t1 (n1 integer, n2 integer, n3 integer);
insert into t1 values (1,1,10), (2,2,20), (null,3,30);
create table t2 (m1 integer, m2 integer, m3 integer);

-- ref table has no rows
merge into t1 using t2 on n1=m1
when matched then update set n3=0
when not matched then insert (n1,n2,n3) values (m1,m2,m3);

select * from t1;

-- target table has no rows
merge into t2 using t1 on n1=m1
when matched then update set m3=0
when not matched then insert (m1,m2,m3) values (n1,n2,n3);

select * from t2;

delete from t2;

insert into t2 values (1,1,100), (2,2,200), (3,3,300), (null,4,400);

merge into t1 using t2 on n1=m1 
when matched then update set n3=0
when not matched then insert (n1,n2,n3) values (m1,m2,m3);

select * from t1;

delete from t1;
insert into t1 values (1,1,10), (2,2,20), (3,null,30);
merge into t1 using t2 on n1=m1
when matched then update set n3=0
when not matched then insert (n1,n2,n3) values (m1,m2,m3);

select * from t1;

delete from t1;
insert into t1 values (1,1,10), (null,2,20), (3,null,30);
merge into t1 using t2 on n1=m1 and n2=m2
when matched then update set n3=0
when not matched then insert (n1,n2,n3) values (m1,m2,m3);

select * from t1;

delete from t1;
insert into t1 values (null,1,10), (null,2,20), (null,3,30);
merge into t1 using t2 on n1=m1
when matched then update set n3=0
when not matched then insert (n1,n2,n3) values (m1,m2,m3);

select * from t1;

delete from t1;
insert into t1 values (null,1,10), (null,2,20), (null,3,30);
delete from t2 where m1 is null;
merge into t2 using t1 on n1=m1
when matched then update set m3=0
when not matched then insert (m1,m2,m3) values (n1,n2,n3);

select * from t2;

drop table t1;
drop table t2;
