set schema 's';

select count(distinct crc_value) 
from table(applib.generate_crc(
  cursor(select * from bench1m)));

-- should be the same as the query above because all rows are specified
select count(distinct crc_value)
from table(applib.generate_crc(
  cursor(select * from bench1m),
  row(kseq,k2,k4,k5,k10,k25,k100,k1k,k10k,k40k,k100k,k250k,k500k,s1,s2,s3,s4,
    s5,s6,s7,s8),
  true));

select count(crc_value) 
from table(applib.generate_crc(
  cursor(select kseq, k500k 
    from bench1m 
    where k2 = 1 
      and k100 > 80 
      and k10k between 200 and 3000)));

select count(distinct crc_value)
from table(applib.generate_crc(
  cursor(select kseq, k500k 
    from bench1m 
    where k2 = 1 
      and k100 > 80 
      and k10k between 200 and 3000)));

select count(distinct crc_value) 
from table(applib.generate_crc(
  cursor(select * from bench1m),
  row(k100),
  false));

select count(distinct crc_value)
from table(applib.generate_crc(
  cursor(select * from bench1m),
  row(s1, s2, s3, s4, s5, s6, s7, s8),
  true));
