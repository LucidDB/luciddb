set schema 'stkmkt';


--
-- rows with null and unknown values
-- null is treated similar to other values
-- unknown is treated similar to other values
-- null is treated as unknown
--
create table t1 (b1 boolean, n1 integer);
create table t2 (b2 boolean, n2 integer);
create table t3 (b3 boolean, n3 integer);

insert into t1 values(null,1),(unknown,1),(null,2),(null,3);
insert into t2 values(null,1),(null,3),(null,4);
insert into t3 values(null,5),(unknown,6);

--
-- basics, intersect/except all not supported as of 6/28
--
select * from t1 union all select * from t2;
select * from t1 union select * from t2;
select * from t1 union all select * from t3;
select * from t1 union select * from t3;
select * from t1 intersect all select * from t2;
select * from t1 intersect select * from t2;
select * from t1 intersect all select * from t3;
select * from t1 intersect select * from t3;
select * from t1 except all select * from t2;
select * from t1 except select * from t2;
select * from t1 except all select * from t3;
select * from t1 except select * from t3;

drop table t1;
drop table t2;
drop table t3;


create table t1(n1 integer);
create table t2(n2 integer);
create table t3(n3 integer);
create table t4(n4 integer);
insert into t1 values (1),(2),(3);
insert into t2 values (2),(3),(4);

--
-- set ops with empty set(s)
--
select * from t3 union all select * from t4;
select * from t3 union select * from t4;
select * from t3 intersect select * from t4;
select * from t3 except select * from t4;
select * from t2 union all select * from t4;
select * from t2 union select * from t4;
select * from t2 intersect select * from t4;
select * from t2 except select * from t4;

--
-- set ops on self
--
select * from t1 union all select * from t1;
select * from t1 union select * from t1;
select * from t1 intersect select * from t1;
select * from t1 except select * from t1;

--
-- nested 
-- (redundant select)
--
select * from
  (select * from t1 union select * from t2)
union all 
select * from t1;

select * from t1
except
select * from
  (select * from t1 except select * from t2);


--
-- precedence: intersect has higher precedence, union and except have same
--
select * from t1 union select * from t1 except select * from t1; 
select * from t1 except select * from t1 union select * from t1;
select * from t1 union all select * from t1 intersect select * from t2;
select * from t2 intersect select * from t1 union all select * from t1;
select * from t1 intersect select * from t2 union all select * from t1;

drop table t1;
drop table t2;

--
-- compatibility (implicit conversion)
--
create table t1(n1 double, n2 integer);
create table t2(m1 integer, m2 integer);

insert into t1 values (1,10),(2,20);
insert into t2 values (1,10),(2,21);

select * from t1 UNION select * from t2;

select * from t1 UNION ALL select * from t2;

select * from t2 UNION select * from t1;

select * from t2 UNION ALL select * from t1;

select * from t1 INTERSECT select * from t2;

select * from t2 INTERSECT select * from t1;


drop table t1;
drop table t2;


--
-- incompatible columns or diff number of cols
-- should all error out
--
create table t1(n1 boolean);
create table t2(m1 integer);

insert into t1 values (true);
insert into t2 values (null);

select * from t1 UNION select * from t2;
select * from t1 UNION ALL select * from t2;
select * from t1 INTERSECT select * from t2;
select * from t1 EXCEPT select * from t2;

drop table t1;
drop table t2;

create table t1(n1 char(1));
create table t2(m1 integer);

insert into t1 values ('a');
insert into t2 values (null);

select * from t1 UNION select * from t2;
select * from t1 UNION ALL select * from t2;
select * from t1 INTERSECT select * from t2;
select * from t1 EXCEPT select * from t2;


delete from t1;
delete from t2;

select * from t1 UNION select * from t2;
select * from t1 UNION ALLselect * from t2;
select * from t1 INTERSECT select * from t2;
select * from t1 EXCEPT select * from t2;

drop table t1;
drop table t2;

create table t1(n1 integer, n2 integer);
create table t2(m1 integer);

insert into t1 values (null,null),(1,1);
insert into t2 values (null),(1);

select * from t1 UNION select * from t2;
select * from t1 UNION ALL select * from t2;
select * from t1 INTERSECT select * from t2;
select * from t1 EXCEPT select * from t2;

drop table t1;
drop table t2;

