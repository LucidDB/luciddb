-- $Id$
-- Full vertical system testing of the cast function

-- NOTE: This script is run twice. Once with the "calcVirtualMachine" set to use fennel
-- and another time to use java. The caller of this script is setting the flag so no need
-- to do it directly unless you need to do acrobatics.

select cast(null as tinyint) from values(1);
select cast(null as smallint) from values(1);
select cast(null as integer) from values(1);
select cast(null as bigint) from values(1);
select cast(null as real) from values(1);
select cast(null as double) from values(1);
--select cast(null as bit) from values(1);
select cast(null as boolean) from values(1);
select cast(null as char(1)) from values(1);
select cast(null as varchar(1)) from values(1);
select cast(null as binary(1)) from values(1);
select cast(null as date) from values(1);
select cast(null as time) from values(1);
select cast(null as timestamp) from values(1);
select cast(null as varbinary(1)) from values(1);
select cast(null as float) from values(1);
--select cast(null as decimal) from values(1);


values cast(1.8 as integer);
values cast(-1.8 as tinyint);
values cast(-1.1 as smallint);
values cast(1e-4 as integer);
values cast(1e2 as integer);

values cast(1 as double);
values cast(cast(1 as real) as double);
values cast(1.1 as double);
values cast(cast(1.0 as double) as real);

values cast(127 as tinyint);
values cast(128 as tinyint);
values cast(255 as tinyint);
values cast(256 as tinyint);

