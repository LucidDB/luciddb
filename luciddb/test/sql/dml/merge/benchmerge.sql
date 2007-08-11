set schema 's';

-- merge entails a cartesian join
merge into bench1 tr
using bench1M as rf
on tr."k2" = 0
when matched then
  update set "k2" = 3
when not matched then
  insert ("kseq","k4") values (rf."kseq",5);
-- should be 1M + 1
select count(*) from bench1;
truncate table bench1;
insert into bench1 select * from bench10k where "kseq" = 1;

--
-- 100K updates, no insert
--
-- should be 1M
select count(*) from bench1M;

merge into bench1M tr
using bench100K as rf
on tr."kseq" = rf."kseq"
when matched then
  update set "k2" = 3
when not matched then
  insert ("kseq","k4") values (rf."kseq",5);

-- should be 100K
select count(*) from bench1M where "k2" = 3;
select count(*) from bench1M
where ("kseq">0 and "kseq"<100001);
-- should be 0
select count(*) from bench1M where "k4" = 5;
-- should be 1M
select count(*) from bench1M;

  
-- restore
truncate table bench1M;
insert into bench1M
select "kseq","k2","k4","k5","k10","k25","k100","k1k","k10k","k40k","k100k","k250k","k500k"
from ff_server."BCP"."bench1M";


--
-- 10K updates and 990K inserts
--

-- should be 10K
select count(*) from bench10K;

merge into bench10K as tr
using bench1M as rf
on tr."kseq" = rf."kseq"
when matched then
  update set "k2" = 3
when not matched then
  insert ("kseq","k4") values (rf."kseq",5);

-- should be 10K
select count(*) from bench10K where "k2" = 3;
-- should be 990K
select count(*) from bench10K where "k4" = 5;
-- should be 1M
select count(*) from bench10K;

-- restore
truncate table bench10K;
insert into bench10K
select "kseq","k2","k4","k5","k10","k25","k100","k1k","k10k","k40k","k100k","k250k","k500k"
from ff_server."BCP"."bench10K";


--
-- update only on bench1M, where k2 = 1
--

merge into bench1M as tr
using (select * from bench1M where "k2" = 1) as rf
on tr."kseq" = rf."kseq"
when matched then
  update set "k4" = 5
when not matched then
  insert ("kseq","k4") values (rf."kseq",6);

--
select count(*) from bench1M where "k2" = 1;
-- should be the above
select count(*) from bench1M where "k4" = 5;
-- should be 0
select count(*) from bench1M where "k4" = 6;
-- should be 1M - count-of-k4=5
select count(*) from bench1M where "k4" < 5;

-- restore
truncate table bench1M;
insert into bench1M
select "kseq","k2","k4","k5","k10","k25","k100","k1k","k10k","k40k","k100k","k250k","k500k"
from ff_server."BCP"."bench1M";


--
-- update only on bench1M, where k1k = 1
--

merge into bench1M as tr
using (select * from bench1M where "k1k" = 1) as rf
on tr."kseq" = rf."kseq"
when matched then
  update set "k4" = 5
when not matched then
  insert ("kseq","k4") values (rf."kseq",6);

--
select count(*) from bench1M where "k1k" = 1;
-- should be the above
select count(*) from bench1M where "k4" = 5;
-- should be 0
select count(*) from bench1M where "k4" = 6;
-- should be 1M - count-of-k4=5
select count(*) from bench1M where "k4" < 5;

-- restore
truncate table bench1M;
insert into bench1M
select "kseq","k2","k4","k5","k10","k25","k100","k1k","k10k","k40k","k100k","k250k","k500k"
from ff_server."BCP"."bench1M";


--
-- update only on bench1M, where k100k = 1
--

merge into bench1M as tr
using (select * from bench1M where "k100k" = 1) as rf
on tr."kseq" = rf."kseq"
when matched then
  update set "k4" = 5
when not matched then
  insert ("kseq","k4") values (rf."kseq",6);

--
select count(*) from bench1M where "k100k" = 1;
-- should be the above
select count(*) from bench1M where "k4" = 5;
-- should be 0
select count(*) from bench1M where "k4" = 6;
-- should be 1M - count-of-k4=5
select count(*) from bench1M where "k4" < 5;

-- restore
truncate table bench1M;
insert into bench1M
select "kseq","k2","k4","k5","k10","k25","k100","k1k","k10k","k40k","k100k","k250k","k500k"
from ff_server."BCP"."bench1M";


--
-- few updates & few inserts on bench100K
--

merge into bench100K as tr
using (select * from bench1M where "k10k" = 1) as rf
on tr."kseq" = rf."kseq"
when matched then
  update set "k500k" = 500001
when not matched then
  insert ("kseq","k500k") values (rf."kseq",500002);

--
select count(*) from bench1M where "k10k" = 1;
select count(*) from bench1M where "k10k" = 1 and "kseq"<=100000;
-- the following 2 counts should add up to the top count
select count(*) from bench100K where "k500k" = 500001;
select count(*) from bench100K where "k500k" = 500002;
-- should be 100K + above
select count(*) from bench100K;

-- restore
truncate table bench100K;
insert into bench100K
select "kseq","k2","k4","k5","k10","k25","k100","k1k","k10k","k40k","k100k","k250k","k500k"
from ff_server."BCP"."bench100K";


--
-- few updates & many inserts on bench100K
--

merge into bench100K as tr
using (select * from bench1M 
       where ("kseq"<100000 and "k100k"=1) or 
             ("kseq">=100000 and "k2"=1)) as rf
on tr."kseq" = rf."kseq"
when matched then
  update set "k500k" = 500001
when not matched then
  insert ("kseq","k500k") values (rf."kseq",500002);

--
select count(*) from bench1M 
                where ("kseq"<100000 and "k100k"=1) or 
                      ("kseq">=100000 and "k2"=1);
-- the following 2 counts should add up to the above
select count(*) from bench100K where "k500k" = 500001;
select count(*) from bench100K where "k500k" = 500002;
-- should be 100K + above
select count(*) from bench100K;

-- restore
truncate table bench100K;
insert into bench100K
select "kseq","k2","k4","k5","k10","k25","k100","k1k","k10k","k40k","k100k","k250k","k500k"
from ff_server."BCP"."bench100K";


--
-- many updates and few inserts on bench100K
--

merge into bench100K as tr
using (select * from bench1M 
       where ("kseq"<100000 and "k2"=1) or 
             ("kseq">=100000 and "k100k"=1)) as rf
on tr."kseq" = rf."kseq"
when matched then
  update set "k500k" = 500001
when not matched then
  insert ("kseq","k500k") values (rf."kseq",500002);

--
select count(*) from bench1M 
                where ("kseq"<100000 and "k2"=1) or 
                      ("kseq">=100000 and "k100k"=1);
-- the following 2 counts should add up to the above
select count(*) from bench100K where "k500k" = 500001;
select count(*) from bench100K where "k500k" = 500002;
-- should be 100K + above
select count(*) from bench100K;

-- restore
truncate table bench100K;
insert into bench100K
select "kseq","k2","k4","k5","k10","k25","k100","k1k","k10k","k40k","k100k","k250k","k500k"
from ff_server."BCP"."bench100K";

           
--
-- update on self
--

merge into bench1M as tr
using bench1M as rf
on tr."kseq" = rf."kseq"
when matched then
  update set "kseq" = rf."kseq" + 1
when not matched then
  insert ("kseq") values (rf."kseq");

select count(*) from bench1M;
select count(*) from bench1M where "kseq">1 and "kseq"<1000002;

--
-- update on self
--

merge into bench1M as tr
using bench1M as rf
on tr."kseq" = rf."kseq"
when matched then
  update set "kseq" = rf."kseq" - 1 
when not matched then
  insert ("kseq") values rf."kseq";

select count(*) from bench1M;
select count(*) from bench1M where "kseq">0 and "kseq"<1000001;
-- do an analyze table, which implicitly verifies the btree pages
analyze table bench1M compute statistics for all columns;
