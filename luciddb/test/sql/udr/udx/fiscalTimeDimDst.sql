-- tests for Fiscal Time Dimension UDX around daylight savings time

create schema dst;
set schema 'dst';

-- weeks when DST ends
select time_key, 
  week_start_date as week_start, 
  week_end_date as week_end, 
  fiscal_week_start_date as fweek_start,
  fiscal_week_end_date as fweek_end,
  week_number_in_year as wiy,
  fiscal_week_number_in_year as fwiy
from table(applib.fiscal_time_dimension(1995, 10, 20, 1995, 11, 12, 1));

select time_key, 
  week_start_date as week_start, 
  week_end_date as week_end, 
  fiscal_week_start_date as fweek_start,
  fiscal_week_end_date as fweek_end,
  week_number_in_year as wiy,
  fiscal_week_number_in_year as fwiy
from table(applib.fiscal_time_dimension(1998, 10, 20, 1998, 11, 12, 5));

select time_key, 
  week_start_date as week_start, 
  week_end_date as week_end, 
  fiscal_week_start_date as fweek_start,
  fiscal_week_end_date as fweek_end,
  week_number_in_year as wiy,
  fiscal_week_number_in_year as fwiy
from table(applib.fiscal_time_dimension(2003, 10, 15, 2003, 11, 12, 3));

select time_key, 
  week_start_date as week_start, 
  week_end_date as week_end, 
  fiscal_week_start_date as fweek_start,
  fiscal_week_end_date as fweek_end,
  week_number_in_year as wiy,
  fiscal_week_number_in_year as fwiy
from table(applib.fiscal_time_dimension(2007, 10, 20, 2007, 11, 12, 10));

select time_key, 
  week_start_date as week_start, 
  week_end_date as week_end, 
  fiscal_week_start_date as fweek_start,
  fiscal_week_end_date as fweek_end,
  week_number_in_year as wiy,
  fiscal_week_number_in_year as fwiy
from table(applib.fiscal_time_dimension(2010, 10, 25, 2010, 11, 20, 7));

-- weeks when DST starts

select time_key, 
  week_start_date as week_start, 
  week_end_date as week_end, 
  fiscal_week_start_date as fweek_start,
  fiscal_week_end_date as fweek_end,
  week_number_in_year as wiy,
  fiscal_week_number_in_year as fwiy
from table(applib.fiscal_time_dimension(2004, 3, 25, 2004, 4, 20, 1));

select time_key, 
  week_start_date as week_start, 
  week_end_date as week_end, 
  fiscal_week_start_date as fweek_start,
  fiscal_week_end_date as fweek_end,
  week_number_in_year as wiy,
  fiscal_week_number_in_year as fwiy
from table(applib.fiscal_time_dimension(2007, 3, 1, 2007, 4, 5, 7));

select time_key, 
  week_start_date as week_start, 
  week_end_date as week_end, 
  fiscal_week_start_date as fweek_start,
  fiscal_week_end_date as fweek_end,
  week_number_in_year as wiy,
  fiscal_week_number_in_year as fwiy
from table(applib.fiscal_time_dimension(2010, 3, 1, 2010, 4, 5, 10));
