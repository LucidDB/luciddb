set schema 's';

select KSEQ, K1K from bench100
where
      (Kseq between 40 and 50
  OR   Kseq between 60 and 70
  OR   Kseq between 70 and 80
  OR   Kseq between 90 and 100
  OR   Kseq between 110 and 120)
  AND  K100K = 3
order by 1;

select KSEQ, K1K from bench100
where
      (Kseq between 40 and 50
  OR   Kseq between 60 and 70
  OR   Kseq between 70 and 80
  OR   Kseq between 90 and 100
  OR   Kseq between 110 and 120)
  AND  K10K = 3
order by 1;

select KSEQ, K1K from bench100
where
      (Kseq between 40 and 50
  OR   Kseq between 60 and 70
  OR   Kseq between 70 and 80
  OR   Kseq between 90 and 100
  OR   Kseq between 110 and 120)
  AND  K1K = 3
order by 1;

select KSEQ, K1K from bench100
where
      (Kseq between 40 and 50
  OR   Kseq between 60 and 70
  OR   Kseq between 70 and 80
  OR   Kseq between 90 and 100
  OR   Kseq between 110 and 120)
  AND  K100 = 3
order by 1;

select KSEQ, K1K from bench100
where
      (Kseq between 40 and 50
  OR   Kseq between 60 and 70
  OR   Kseq between 70 and 80
  OR   Kseq between 90 and 100
  OR   Kseq between 110 and 120)
  AND  K10 = 3
order by 1;

select KSEQ, K1K from bench100
where
      (Kseq between 40 and 50
  OR   Kseq between 60 and 70
  OR   Kseq between 70 and 80
  OR   Kseq between 90 and 100
  OR   Kseq between 110 and 120)
  AND  K5 = 3
order by 1;

select KSEQ, K1K from bench100
where
      (Kseq between 40 and 50
  OR   Kseq between 60 and 70
  OR   Kseq between 70 and 80
  OR   Kseq between 90 and 100
  OR   Kseq between 110 and 120)
  AND  K4 = 3
order by 1;
