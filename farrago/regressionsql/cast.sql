-- $Id$
-- Full vertical system testing of the cast function

-- NOTE: This script is run twice. Once with the "calcVirtualMachine" set to use fennel
-- and another time to use java. The caller of this script is setting the flag so no need
-- to do it directly unless you need to do acrobatics.

values cast(null as tinyint);
values cast(null as smallint);
values cast(null as integer);
values cast(null as bigint);
values cast(null as real);
values cast(null as double);
values cast(null as boolean);
values cast(null as char(1));
values cast(null as varchar(1));
values cast(null as binary(1));
values cast(null as date);
values cast(null as time);
values cast(null as timestamp);
values cast(null as varbinary(1));
values cast(null as float);
--values cast(null as decimal);


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

