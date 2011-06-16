-- $Id$
-- Tests for TimestampUtilUdf

-- truncate_timestamp tests

values(applib.truncate_timestamp(timestamp'2011-6-15 16:52:13', 'HOURLY'));
values(applib.truncate_timestamp(timestamp'2011-6-15 16:52:13', 'DAILY'));
values(applib.truncate_timestamp(timestamp'2011-6-15 16:52:13', 'WEEKLY'));
values(applib.truncate_timestamp(timestamp'2011-6-15 16:52:13', 'MONTHLY'));
values(applib.truncate_timestamp(timestamp'2011-6-15 16:52:13', 'YEARLY'));
values(applib.truncate_timestamp(cast(null as timestamp), 'HOURLY'));

-- extract_timestamp tests

values(applib.extract_timestamp(timestamp'2011-6-15 16:52:13', 'SECOND'));
values(applib.extract_timestamp(timestamp'2011-6-15 16:52:13', 'MINUTE'));
values(applib.extract_timestamp(timestamp'2011-6-15 16:52:13', 'HOUR'));
values(applib.extract_timestamp(timestamp'2011-6-15 16:52:13', 'DAY'));
values(applib.extract_timestamp(timestamp'2011-6-15 16:52:13', 'DOW'));
values(applib.extract_timestamp(timestamp'2011-6-13 16:52:13', 'DOW'));
values(applib.extract_timestamp(timestamp'2011-6-12 16:52:13', 'DOW'));
values(applib.extract_timestamp(timestamp'2011-6-15 16:52:13', 'WEEK'));
values(applib.extract_timestamp(timestamp'2011-6-15 16:52:13', 'MONTH'));
values(applib.extract_timestamp(timestamp'2011-6-15 16:52:13', 'YEAR'));
values(applib.extract_timestamp(timestamp'2011-6-15 16:52:13', 'HOW'));

-- adjust_timestamp tests

values(applib.adjust_timestamp(timestamp'2011-6-15 22:15:00', '1:3', 2)); 
values(applib.adjust_timestamp(timestamp'2011-6-15 22:15:00', '+1:3', 2)); 
values(applib.adjust_timestamp(timestamp'2011-6-15 22:15:00', '-1:3', 2)); 
values(applib.adjust_timestamp(timestamp'2011-6-15 22:15:00', '+-1:3', 2)); 
values(applib.adjust_timestamp(timestamp'2011-6-15 22:15:00', '-+1:3', 2)); 
values(applib.adjust_timestamp(timestamp'2011-6-15 22:15:00', '', 2)); 
values(applib.adjust_timestamp(timestamp'2011-6-15 22:15:00', '1', 2)); 
values(applib.adjust_timestamp(timestamp'2011-6-15 22:15:00', '1:', 2)); 
values(applib.adjust_timestamp(timestamp'2011-6-15 22:15:00', '0110', 2)); 

