

-- Should fail: interval should only have a single datetime field.
values cast(interval '1:02' minute to second as decimal(6,1));

-- Should succeed (but fails because interval constant folding is broken)
-- values cast(cast(interval '1:02.1' minute to second(2,1) as interval second(4,1)) as decimal(6,1));

values cast(interval '1' second as bigint);

values cast(interval '1.1' second(1,1) as bigint);

values cast(interval '1.1' second(1,1) as decimal(2,1));

values cast(interval '1' minute as bigint);

values cast(interval '5' hour as bigint);

values cast((interval '5.001' second(1,3)) * 1000 as bigint);

values cast(1 as interval second);

values cast(1.1 as interval second(1,1));

values cast(1 as interval minute);

values cast(5 as interval hour);