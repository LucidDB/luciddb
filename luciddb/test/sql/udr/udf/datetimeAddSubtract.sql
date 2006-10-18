-- datetime addition and subtraction functions

create schema dtas;
set schema 'dtas';

-- basic positive tests
values(applib.days_diff(date'2010-2-20', date'2010-3-10'));
values(applib.days_diff(timestamp'2010-2-28 23:59:00',
  timestamp'2010-3-1 4:50:00'));
values(applib.hours_diff(timestamp'2010-2-28 20:20:00',
  timestamp'2010-3-1 1:00:00'));
values(applib.add_days(date'2010-12-29', 5));
values(applib.add_days(timestamp'2010-3-2 12:11:11', -10));
values(applib.add_hours(timestamp'2010-12-30 1:59:04', 60));


-- nested
values(applib.days_diff(applib.add_days(current_date, 180), current_date));

values(applib.days_diff(applib.add_days(current_timestamp, 180), 
  current_timestamp));

-- TODO: unresolved issue LER-1909
values(applib.hours_diff(applib.add_hours(current_timestamp, 91),
  current_timestamp));

-- null input
values(applib.days_diff(current_date, cast(null as DATE)));
values(applib.days_diff(cast(null as TIMESTAMP), current_timestamp));
values(applib.hours_diff(current_timestamp, cast(null as TIMESTAMP)));
values(applib.add_days(cast(null as timestamp), 3));
values(applib.add_days(current_timestamp, cast(null as INTEGER)));
values(applib.add_hours(cast(null as TIMESTAMP), 5));

import foreign schema BCP 
limit to ("dates")
from server flatfile_server
into dtas;

-- multiple function calls 
select 
  start_date, 
  applib.add_days(start_date, -3) as sd_minus_3d,
  applib.add_days(start_date, 9) as sd_plus_9d,
  applib.days_diff(start_date, date'2000-11-29') as sd_dd,
  activity_time,
  applib.add_days(activity_time, 5) as at_plus_5d,
  applib.days_diff(activity_time, timestamp'2000-11-29 12:00:01') as at_dd,
  applib.add_hours(activity_time, 9) as at_plus_9h,
  applib.hours_diff(activity_time, timestamp'2000-11-29 12:00:01') as at_hd
from
  dtas."dates"
order by start_date;

-- from UDX
select
  time_key,
  week_start_date as wsd,
  fiscal_quarter_start_date as fqsd,
  fiscal_year_start_date as fysd,
  applib.add_days(week_start_date, -10) as wsd_minus_10,
  applib.days_diff(
    week_start_date, fiscal_quarter_start_date) as wsd_minus_fqsd,
  applib.days_diff(
    fiscal_year_start_date, fiscal_quarter_start_date) as fysd_minus_fqsd,
  applib.days_diff(
    fiscal_quarter_start_date, time_key) as fqsd_minus_tk
from
  table(applib.time_dimension(2001, 2, 20, 2001, 3, 11, 3))
order by time_key;