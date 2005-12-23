set schema 's';

-- overflows. change from INTEGER to NUMERIC later.
select sum("k500k") from bench100K group by "k2" order by 1;
select sum("k500k") from bench100K group by "k5" order by 1;
select sum("k500k") from bench100K group by "k10" order by 1;
select sum("k500k") from bench100K group by "k25" order by 1;

select sum("k10k") from bench100K group by "k2" order by 1;
select sum("k10k") from bench100K group by "k5" order by 1;
select sum("k10k") from bench100K group by "k10" order by 1;
select sum("k10k") from bench100K group by "k25" order by 1;

select max("k10k") from bench100K group by "k2" order by 1;
select max("k10k") from bench100K group by "k5" order by 1;
select max("k10k") from bench100K group by "k10" order by 1;
select max("k10k") from bench100K group by "k25" order by 1;

select min("k10k") from bench100K group by "k2" order by 1;
select min("k10k") from bench100K group by "k5" order by 1;
select min("k10k") from bench100K group by "k10" order by 1;
select min("k10k") from bench100K group by "k25" order by 1;

select count("k10k") from bench100K group by "k2" order by 1;
select count("k10k") from bench100K group by "k5" order by 1;
select count("k10k") from bench100K group by "k10" order by 1;
select count("k10k") from bench100K group by "k25" order by 1;
