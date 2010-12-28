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
values applib.instr('fds', 'sdfkdsl');
values applib.instr('rock 0 paper 0 scissors 0 rock 1 paper 1 scissors 1 rock 2 paper 2 scissors 2 rock 3 paper 3 scissors 3 rock 4 paper 4 scissors 4 rock 5 paper 5 scissors 5 rock 6 paper 6 scissors 6 rock 7 paper 7 scissors 7 rock 8 paper 8 scissors 8 rock 9 paper 9 scissors 9 rock 10 paper 10 scissors 10, rock 11 paper 11 scissors 11 rock 12 paper 12 scissors 12 rock 13 paper 13 scissors 13 rock 14 paper 14 scissors 14 rock 15 paper 15 scissors 15 rock 16 paper 16 scissors 16 rock 17 paper 17 scissors 17 rock 18 paper 18 scissors 18 rock 19 paper 19 scissors 19 rock 20 paper 20 scissors 20', '15 sc');
values applib.instr('hello there!', '');

-- invalid argument failures
values applib.instr('nononon', 'no', 0, 1);
values applib.instr('nono', 'no', 1, 0);
values applib.instr('nonon', 'no', 6, 1);

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
