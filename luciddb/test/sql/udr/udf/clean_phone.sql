-- $Id$
-- Test queries for CleanPhone UDF
set schema 'udftest';
set path 'udftest';

values applib.clean_phone('1 2 3 4 5 6 7 8 9  0');
values applib.clean_phone('123456789012');

values applib.clean_phone('123 456 789 012', 1);
values applib.clean_phone('123.456.7890', 0);
values applib.clean_phone('1234567890', 1, true);
values applib.clean_phone('123456789012', 1, false);
values applib.clean_phone('aBcDeFgHiJkLm', '(999) 999 999 999 9', true);
values applib.clean_phone('1800TESTING', '9-999-999-9999', true);

-- negative tests
values applib.clean_phone('1234567890', -1);
values applib.clean_phone('123456789012', 1, true);
values applib.clean_phone('123-4567-890123', '(999)9999999', true);
values applib.clean_phone('  fd   1 2 3 4 5 6 7 8 9 0          ', 0, true);
values applib.clean_phone('sdfjl92378sflkds', 'ph:999-999-9999', true);

-- null parameters
values applib.clean_phone(cast (null as varchar(10)), '999-999-9999', true);
values applib.clean_phone('123-232-2222', cast(null as varchar(8)), true);
values applib.clean_phone('222-323-1111', cast(null as integer));

-- create view with references to applib.clean_phone
create view fmtphone(fname, lname, phone1, phone2, phone3, phone4) as
select fname, lname, applib.clean_phone(phone), applib.clean_phone(phone, 0), applib.clean_phone(phone, 1, true), applib.clean_phone(phone, 'ph:999=& 99 !!99999', true)
from customers;

select * from fmtphone
order by 1;

-- applib.clean_phone in expressions
select phone1 || applib.clean_phone(phone2), phone4 || applib.clean_phone(phone3, 0, true)
from fmtphone
order by 1;

-- nested applib.clean_phone
values applib.clean_phone(applib.clean_phone('5558459190', 1, false), 'My Phone Number:(999 999 9999)', true); 

values applib.clean_phone(' df        ' || applib.clean_phone('1 2 3 4 5 6 7 8 9 0', 0, true) || '          ');
