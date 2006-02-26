-- $Id$
set schema 'udftest';
set path 'udftest';

create table international(ph varchar(128));

insert into international values
('5133870'),
('513-3870'),
('6505133870'),
('650-513-3870'),
('415-744-9026 ext123'),
('1-650-513-3870'),
('1415-744-9026'),
('14157449026'),
('(+886) 2-9876-5432'),
('011-886-2-9876-5432'),
('01185223456789');

values applib.clean_phone_international('23890123809214382109432809', true);
values applib.clean_phone_international('fdjk3242478932hfdskf832498', true);
values applib.clean_phone_international('23', true);
values applib.clean_phone_international('sdf', true);

values applib.clean_phone_international('dsf', false);

-- create view with reference to applib.clean_phone_international
create view internationalphone(before, after) as
select ph, applib.clean_phone_international(ph, true)
from international;

select * from internationalphone
order by 1;

-- in expressions
select ph || applib.clean_phone_international(ph, false)
from international
order by 1;

-- cleanup
drop view internationalphone;
