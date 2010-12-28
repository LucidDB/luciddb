-- $Id$
set schema 'udftest';
set path 'udftest';

values applib.padweeknumber(2);
values applib.padweeknumber(30);

-- failures
values applib.padweeknumber(-3);
values applib.padweeknumber(3423);
values applib.padweeknumber(01234567890123456789);
values applib.padweeknumber(-40);

-- null input
values applib.padweeknumber(cast(null as integer));

-- view test
select TIME_KEY_SEQ, WEEK_NUMBER_IN_YEAR, applib.padweeknumber("WEEK_NUMBER_IN_YEAR") 
from table(applib.time_dimension(2006, 1, 14, 2006, 2, 15));

select applib.padweeknumber( cast (applib.padweeknumber("DAY_NUMBER_IN_MONTH") as integer)) 
from table(applib.time_dimension(1690, 4, 29, 1690, 5, 13));

-- in expressions

values 'Padded value:' || applib.padweeknumber(2) || 'finish';
