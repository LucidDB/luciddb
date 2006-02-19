set schema 's';

set plan=both
;
select * from t
;
select count(*) from t where c1 > 0
;
select count(*) from t where c2 > 0
;
select count(*) from t where c3 > 0
;
select count(*) from t where c4 > 0
;
select count(*) from t where c5 > 0
;
select count(*) from t where c6 > 0
;
select count(*) from t where c7 > 0
;
