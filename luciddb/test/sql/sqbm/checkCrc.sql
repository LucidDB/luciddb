set schema 's';

select count(distinct crc_value) 
from table(applib.generate_crc(
  cursor(select * from bench1m)));

select count(distinct crc_value)
from table(applib.generate_crc(
  cursor(select * from bench_source)));