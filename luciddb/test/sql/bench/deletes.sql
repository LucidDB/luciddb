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

-- LER-5790
truncate table bench10k;
INSERT INTO bench10k
SELECT "kseq","k2","k4","k5","k10","k25","k100","k1k","k10k","k40k","k100k","k250k","k500k"
FROM ff_schema."bench10K";
delete from bench10k where "kseq" <= 3824;
delete from bench10k where "kseq" = 5008;
delete from bench10k where "kseq" = 4000;
delete from bench10k where "kseq" = 5001;
delete from bench10k where "kseq" = 5002;
-- following select should return no rows since those were deleted
select * from bench10K where "kseq" in (3824, 4000, 5001, 5002, 5008);

truncate table bench10k;
INSERT INTO bench10k
SELECT "kseq","k2","k4","k5","k10","k25","k100","k1k","k10k","k40k","k100k","k250k","k500k"
FROM ff_schema."bench10K";
delete from bench10k where "kseq" <= 3823;
delete from bench10k where "kseq" = 5008;
delete from bench10k where "kseq" in (3824, 5001, 5002);
delete from bench10k where "kseq" in (4000, 4001);
-- following select should return no rows since those were deleted
select * from bench10K where "kseq" in (3824, 4000, 4001, 5001, 5002, 5008);

delete from bench1m where "kseq" in 
(select b1."kseq" from bench1m b1, bench1m b2
 where b1."k100" = 49 and b1."k250k" = b2."k500k");
select count(*) from bench1m;

delete from bench1m;
select * from bench1m;
