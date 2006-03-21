--
-- this used to crash the server (bug 12836)
--

set schema 's';

drop table nullLike;
create table nullLike(v varchar(30));
insert into nullLike 
values(null);

select * from nullLike where v like '1%';
