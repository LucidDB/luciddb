--
-- Bug 2533
-- Owner: Boris
-- Abstract: A nested POWER() built-in {POWER(POWER(n1,2), 2)) } returns 0..
--

set schema 's';

create table bug2533tab ( n1 integer)
;
insert into bug2533tab values(2)
;
select POWER(POWER(n1,2), 2) from bug2533tab
;
select POWER(POWER(n1,2), 0.5) from bug2533tab
;
select POWER(n1,0.5) from bug2533tab
;
