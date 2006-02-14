-- $Id$
set schema 'udftest';
set path 'udftest';

create table intl(ph varchar(128));

insert into intl values
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

-- define functions
create function clean_phone_intl(str varchar(128), b boolean)
returns varchar(128)
language java
no sql
external name 'class com.lucidera.luciddb.applib.CleanPhoneInternational.FunctionExecute';

values clean_phone_intl('23890123809214382109432809', true);
values clean_phone_intl('fdjk3242478932hfdskf832498', true);
values clean_phone_intl('23', true);
values clean_phone_intl('sdf', true);

values clean_phone_intl('dsf', false);

-- create view with reference to clean_phone_intl
create view intlphone(before, after) as
select ph, clean_phone_intl(ph, true)
from intl;

select * from intlphone
order by 1;

-- in expressions
select ph || clean_phone_intl(ph, false)
from intl
order by 1;

-- cleanup
drop view intlphone;
drop routine clean_phone_intl;