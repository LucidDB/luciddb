-- $Id$
-- Test various sqlline display options

-- with full error stack
!set shownestederrs true

drop table cylon;

-- back to default:  just most significant error stack entry
!set shownestederrs false

drop table cylon;

--  default:  show warnings
!set showwarnings true
!closeall
!connect jdbc:farrago:;clientProcessId=bogus sa tiger

-- suppress warnings
!set showwarnings false
!closeall
!connect jdbc:farrago:;clientProcessId=bogus sa tiger

-- display numbers with rounding to limited scale
!set numberformat #.###

values (6.666666);

-- display numbers the usual way
!set numberformat default
values (6.666666);

-- stop after two rows
!set rowlimit 2
select * from sales.depts order by name;

-- revert to fetching all rows
!set rowlimit 0
select * from sales.depts order by name;

-- get rowcounts without times
!set silent off
!set showtime off
create schema rowcounts;
create table rowcounts.t(i int not null primary key);
insert into rowcounts.t values (1);
select * from rowcounts.t;

-- revert to default
!set showtime on
!set silent on
select * from rowcounts.t;
