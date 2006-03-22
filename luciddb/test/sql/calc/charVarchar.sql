-- bug fixed  (bug: )

set schema 's';

create table bugs (x varchar(30), y char(30));
insert into bugs values ('testme', 'testme');
select * from bugs where x=y;
select * from bugs where x='testme   ';
select * from bugs where x=x;
select * from bugs where y=y;
select * from bugs where y='testme   ';
drop table bugs;
