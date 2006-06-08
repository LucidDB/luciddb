set schema 's';

delete from bench100k where "k5" = 1;
select count(*) from bench100k;

delete from bench100k where "k5" = 2;
select count(*) from bench100k;

delete from bench100k where "k5" = 3;
select count(*) from bench100k;

delete from bench100k where "k5" = 4;
select count(*) from bench100k;

delete from bench100k where "k5" = 5;
select count(*) from bench100k;

delete from bench10k where
"k2" between 1 and 2 and
"k4" between 1 and 4 and
"k5" between 1 and 5 and
"k10" between 1 and 10 and
"k25" between 1 and 25 and
"k100" between 1 and 100 and
"k1k" between 1 and 1000 and
"k10k" between 1 and 10000 and
"k40k" between 1 and 40000 and
"k100k" between 1 and 100000 and
"k250k" between 1 and 250000 and
"k500k" between 1 and 500000 and
"kseq" between 1 and 10000
;

select count(*) from bench10k;

delete from bench1m where "k100" = 49 and "k250k" = "k500k";
select count(*) from bench1m;

delete from bench1m;
select * from bench1m;