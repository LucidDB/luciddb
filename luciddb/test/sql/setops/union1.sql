set schema 'stkmkt';

-- set numberFormat since floating point differs based on VM
!set numberFormat 0.0000

select * from Investor1View
order by 1, 2, 3, 4
;
select * from Investor2View
order by 1, 2, 3, 4
;
select * from Investor1View UNION 
select * from Investor2View 
order by 1, 2, 3, 4
;
select * from Investor3View
order by 1, 2, 3, 4
;
select * from Investor1View UNION 
select * from Investor2View UNION 
select * from Investor3View 
order by 1, 2, 3, 4
;
select * from Investor4View
order by 1, 2, 3, 4
;
select * from Investor1View UNION 
select * from Investor2View UNION 
select * from Investor3View UNION
select * from Investor4View 
order by 1, 2, 3, 4
;
select * from Investor5View
order by 1, 2, 3, 4
;
select * from Investor1View UNION 
select * from Investor2View UNION 
select * from Investor3View UNION
select * from Investor4View UNION
select * from Investor5View 
order by 1, 2, 3, 4
;
select * from Investor6View
order by 1, 2, 3, 4
;
select * from Investor1View UNION 
select * from Investor2View UNION 
select * from Investor3View UNION
select * from Investor4View UNION
select * from Investor5View UNION
select * from Investor6View
order by 1, 2, 3, 4
;
select * from Investor7View
order by 1, 2, 3, 4
;
select * from Investor1View UNION 
select * from Investor2View UNION 
select * from Investor3View UNION
select * from Investor4View UNION
select * from Investor5View UNION
select * from Investor6View UNION
select * from Investor7View
order by 1, 2, 3, 4
;
select * from Investor8View
order by 1, 2, 3, 4
;
select * from Investor1View UNION 
select * from Investor2View UNION 
select * from Investor3View UNION
select * from Investor4View UNION
select * from Investor5View UNION
select * from Investor6View UNION
select * from Investor7View UNION
select * from Investor8View
order by 1, 2, 3, 4
;
select * from Investor9View
order by 1, 2, 3, 4
;
select * from Investor1View UNION 
select * from Investor2View UNION 
select * from Investor3View UNION
select * from Investor4View UNION
select * from Investor5View UNION
select * from Investor6View UNION
select * from Investor7View UNION
select * from Investor8View UNION
select * from Investor9View
order by 1, 2, 3, 4
;
select * from Investor10View
order by 1, 2, 3, 4
;
select * from Investor1View UNION 
select * from Investor2View UNION 
select * from Investor3View UNION
select * from Investor4View UNION
select * from Investor5View UNION
select * from Investor6View UNION
select * from Investor7View UNION
select * from Investor8View UNION
select * from Investor9View UNION
select * from Investor10View
order by 1, 2, 3, 4
;

!set numberFormat default
