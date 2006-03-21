set schema 's';

!set outputformat csv

explain plan for 
select KSEQ, K2, K4 from bench100 
where K2=1 
order by kseq;

select KSEQ, K2, K4 from bench100 
where K2=1 
order by kseq;

explain plan for select kseq, k2, k4 from bench100
where k2=1 and k4=3
order by kseq;

select kseq, k2, k4 from bench100
where k2=1 and k4=3
order by kseq;

explain plan for select kseq, k500k, k2 from bench100
where k500k=626 or k500k=45323
order by kseq;

select kseq, k500k, k2 from bench100
where k500k=626 or k500k=45323
order by kseq;

!set outputformat tables