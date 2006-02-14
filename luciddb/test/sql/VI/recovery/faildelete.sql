-- set trap to force failure in Flip operation
-- trap: passCountIndexFlip = 3 (fail at the 3rd index in Flip operation)

set schema 's';

insert into t values (9, 9, 9, 9, 9, 9, 9)
;
insert into t values (10, 10, 10, 10, 10, 10, 10)
;
insert into t values (11, 11, 11, 11, 11, 11, 11)
;
insert into t values (12, 12, 12, 12, 12, 12, 12)
;
delete from t where c1 = 7
;
delete from t where c1=9
;
alter system set "trap 44" = 3
;
-- trap: faultIndexFlip = 1 (assert when hitting the trap)
alter system set "trap 10" = 1
;
delete from t where c1=11
;
-- delete from RI and flip VI
select index_name,status,num_blocks_vi from indexes where table_name='T'
;
