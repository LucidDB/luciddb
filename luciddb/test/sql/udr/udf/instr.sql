-- $Id$
-- Tests for INSTR UDF
set schema 'udftest';
set path 'udftest';

values applib.instr('this, is, a , test', ' ');
values applib.instr('hello there. what''s your name? henny''s ', '''');
values applib.instr('this, is, a , test', ' ', -1, 3);
values applib.instr('AAABBBCCCDDDAABBBCCCEEE', 'AB', 1, 1);
values applib.instr('AAABBBCCCDDDAABBBCCCEEE', 'ABB', -8, 1);
values applib.instr('AAABBBCCCDDDAABBBCCCEEE', 'ABB', -10, 1);
values applib.instr('hello there. what''s your name? henny''s ', 'he', 8, 2);
values applib.instr('hello there. what''s your name? henny''s ', '''', 1, 2);
values applib.instr('n1b2n1h4jn1;', 'n1', -1, 4);

-- invalid argument failures
values applib.instr('nononon', 'no', 0, 1);
values applib.instr('nono', 'no', 1, 0);
values applib.instr('nonon', 'no', 6, 1);
values applib.instr('fds', 'sdfkdsl');
values applib.instr('hello there!', '');

-- null input
values applib.instr(cast(null as varchar(20)), 'null');
values applib.instr('this is a pen', cast(null as varchar(2)), -1, 3);
values applib.instr('this is a pen', 'is', cast(null as integer), 1);
values applib.instr('this is a isthmus', 'is', 2, cast(null as integer));

-- view test
create view flpview(flp, instr_flp) as 
select fname||lname||phone, applib.instr(fname||lname||phone, '23')
from customers;

select * from flpview
order by 1;

select flp, applib.instr(flp, 'a', -3, 2)
from flpview
order by 1;

-- cleanup
drop view flpview;
