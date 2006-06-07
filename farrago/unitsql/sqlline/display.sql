-- $Id$
-- Test various sqlline display options

-- with full error stack
!set shownestederrs true

drop table cylon;

-- back to default:  just most significant error stack entry
!set shownestederrs false

drop table cylon;

-- display numbers with rounding to limited scale
!set numberformat #.###

values (6.666666);

-- display numbers the usual way
!set numberformat default
values (6.666666);

