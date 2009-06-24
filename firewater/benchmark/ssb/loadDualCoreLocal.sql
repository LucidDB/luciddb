insert into ssb_part1.ssb.lineorder
select * from ssb_files.lineorder_1;

insert into ssb_part2.ssb.lineorder
select * from ssb_files.lineorder_2;

