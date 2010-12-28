-- TEsts on the like operator

set schema 's';

drop table foo
;
create table foo (x varchar(30))
;
insert into foo values ('TESTER')
;
insert into foo values ('MONSTER')
;
insert into foo values ('RICH')
;
insert into foo values ('CREATURE')
;
select * from foo where x like '%ER%' order by 1
;
!set shownestederrs true
select * from foo where x like '%*E*R%' escape '*'
;
!set shownestederrs false
select * from foo where x like '%ER*%' escape '*'
;
select * from foo where x like '%ERR%' escape 'R'
;
select * from foo where x like '%ER%' escape 'R'
;
drop table foo
;

