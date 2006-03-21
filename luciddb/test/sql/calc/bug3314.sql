--
-- Bug 3314
-- Owner: Boris
-- Abstract: CALC, PARSER, and CONVERSIONS need to round instead of trunc
--

set schema 's';

create table high (x numeric(19,12));
create table low (x numeric(19,10));

insert into high values (25.12345678909876);
insert into low select * from high;
insert into low values (123456789.555555555555555);

select * from high;
select * from low;

drop table high;
drop table low;

