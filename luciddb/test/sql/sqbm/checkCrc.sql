set schema 's';

select count(distinct crc_value) 
from table(applib.generate_crc(
  cursor(select * from bench1m)));

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
