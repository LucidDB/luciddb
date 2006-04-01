create schema s1;
set schema 's1';

create table t (i int)
;
insert into t values(1)
;
--delete from t
--;
create index idx1 on t(i)
;
insert into t values(2)
;
explain plan for select * from t where i=1
;
explain plan for select * from t where i=2
;
select * from t where i = 2
;
select * from t where i = 1
;
