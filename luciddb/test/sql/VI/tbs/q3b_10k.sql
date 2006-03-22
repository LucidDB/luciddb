set schema 's';

select KSEQ, K1K from bench10K
where
      (Kseq between 4000 and 4100
  OR   Kseq between 4200 and 4300
  OR   Kseq between 4400 and 4500
  OR   Kseq between 4600 and 4700
  OR   Kseq between 4800 and 5000)
  AND  K100K = 3
order by 1;

select KSEQ, K1K from bench10K
where
      (Kseq between 4000 and 4100
  OR   Kseq between 4200 and 4300
  OR   Kseq between 4400 and 4500
  OR   Kseq between 4600 and 4700
  OR   Kseq between 4800 and 5000)
  AND  K10K = 3
order by 1;

select KSEQ, K1K from bench10K
where
      (Kseq between 4000 and 4100
  OR   Kseq between 4200 and 4300
  OR   Kseq between 4400 and 4500
  OR   Kseq between 4600 and 4700
  OR   Kseq between 4800 and 5000)
  AND  K1K = 3
order by 1;

select KSEQ, K1K from bench10K
where
      (Kseq between 4000 and 4100
  OR   Kseq between 4200 and 4300
  OR   Kseq between 4400 and 4500
  OR   Kseq between 4600 and 4700
  OR   Kseq between 4800 and 5000)
  AND  K100 = 3
order by 1;

select KSEQ, K1K from bench10K
where
      (Kseq between 4000 and 4100
  OR   Kseq between 4200 and 4300
  OR   Kseq between 4400 and 4500
  OR   Kseq between 4600 and 4700
  OR   Kseq between 4800 and 5000)
  AND  K10 = 3
order by 1;

select KSEQ, K1K from bench10K
where
      (Kseq between 4000 and 4100
  OR   Kseq between 4200 and 4300
  OR   Kseq between 4400 and 4500
  OR   Kseq between 4600 and 4700
  OR   Kseq between 4800 and 5000)
  AND  K5 = 3
order by 1;

select KSEQ, K1K from bench10K
where
      (Kseq between 4000 and 4100
  OR   Kseq between 4200 and 4300
  OR   Kseq between 4400 and 4500
  OR   Kseq between 4600 and 4700
  OR   Kseq between 4800 and 5000)
  AND  K4 = 3
order by 1;
