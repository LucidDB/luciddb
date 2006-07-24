set schema 'stkmkt';

insert into accounts values(1,'Investor1','conservative')
;
insert into accounts values(2,'Investor2','aggressive')
;
insert into accounts values(3,'Investor3','gambler')
;
insert into accounts values(4,'Investor4','conservative')
;
insert into accounts values(5,'Investor5','conservative')
;
insert into accounts values(6,'Investor6','risk taker')
;
insert into accounts values(7,'Investor7','risk taker')
;
insert into accounts values(8,'Investor8','aggressive')
;
insert into accounts values(9,'Investor9','conservative')
;
insert into accounts values(10,'Investor10','conservative')
;
--
--
insert into exchange values( 'PSX', 'Pacific Stock Exchange', 'San Francisco, CA')
;
insert into exchange values( 'NYSE', 'New York Stock Exchange', 'New York, NY')
;
insert into exchange values( 'NASDAQ', 'Nasdaq Stock Exchange', 'San Francisco, CA')
;
insert into exchange values( 'AMEX', 'American Stock Exchange', 'New York, NY')
;
--
--
insert into tickers values('COM1','Company No. 1', 'NASDAQ')
;
insert into tickers values('COM2','Company No. 2','NASDAQ')
;
insert into tickers values('COM3','Company No. 3','AMEX')
;
insert into tickers values('COM4','Company No. 4','NASDAQ')
;
insert into tickers values('COM5','Company No. 5','PSX')
;
insert into tickers values('COM6','Company No. 6','PSX')
;
insert into tickers values('COM7','Company No. 7','AMEX')
;
insert into tickers values('COM8','Company No. 8','NYSE')
;
insert into tickers values('COM9','Company No. 9','NYSE')
;
insert into tickers values('COM10','Company No. 10','NYSE')
;

--
--JAN Transactions
--
insert into jantran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares)
  values('010001', 5, 'COM3', timestamp'1997-01-01 10:00:00', 50.00, null, null,  100)
;
insert into jantran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('010002', 3, 'COM1', timestamp'1997-01-02 10:03:00', 37.875, null, null,  100)
;
insert into jantran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('010003', 3, 'COM6', null, null,timestamp'1997-01-03 11:15:00', 17.375,  500)
;
insert into jantran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('010004', 4, 'COM9', timestamp'1997-01-07 12:55:10', 57.5, null, null,  200)
;
insert into jantran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('010005', 2, 'COM4', null, null, timestamp'1997-01-08 9:33:00', 7.25,  1000)
;
insert into jantran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('010006', 3, 'COM1', timestamp'1997-01-10 14:15:00', 35.25, null, null,  100)
;
insert into jantran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('010007', 1, 'COM8', timestamp'1997-01-13 13:27:23', 150.00, null, null,  50)
;
insert into jantran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('010008', 7, 'COM10', null, null, timestamp'1997-01-18 12:10:33', 27.625,  700)
;
insert into jantran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('010009', 10, 'COM4', timestamp'1997-01-23 11:08:17', 27.625, null, null,  700)
;
merge into jantran t1
using jantran t2 on t1.tid = t2.tid
when matched then
  update set commission =
    (CASE 
     WHEN t1.purchase_time IS NOT NULL
     THEN CASE
          WHEN t1.numshares <=100 THEN  0.3 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 100 AND 500 THEN 0.2 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 501 AND 1000 THEN 0.125 * t1.purchase_price * t1.numshares
          WHEN t1.numshares > 1000 THEN 0.0075 * t1.purchase_price * t1.numshares
          ELSE null
          END
     WHEN t1.sale_time IS NOT NULL
     THEN CASE 
          WHEN t1.numshares <=100 THEN 0.3 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 100 AND 500 THEN 0.2 * t1.sale_price * t1.numshares
          WHEN t1.numshares BETWEEN 501 AND 1000 THEN 0.125 * t1.sale_price * t1.numshares
          WHEN t1.numshares > 1000 THEN 0.0075 * t1.sale_price * t1.numshares
          ELSE null
          END
     ELSE null
     END)
;


--
--FEB Transactions
--
insert into febtran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('020001', 8, 'COM5', timestamp'1997-02-02 9:47:12', 15.00, null, null,  1200)
;
insert into febtran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('020002', 7, 'COM1', timestamp'1997-02-07 10:03:00', 37.875, null, null,  300)
;
insert into febtran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('020003', 3, 'COM6', null, null,timestamp'1997-02-13 11:15:00', 14.375,  200)
;
insert into febtran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('020004', 4, 'COM9', timestamp'1997-02-17 12:50:11', 52.25, null, null,  100)
;
insert into febtran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('020005', 10, 'COM4', null, null, timestamp'1997-01-08 9:33:00', 7.25,  700)
;
merge into febtran t1
using febtran t2 on t1.tid = t2.tid
when matched then
  update set commission =
    (CASE 
     WHEN t1.purchase_time IS NOT NULL
     THEN CASE
          WHEN t1.numshares <=100 THEN  0.3 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 100 AND 500 THEN 0.2 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 501 AND 1000 THEN 0.125 * t1.purchase_price * t1.numshares
          WHEN t1.numshares > 1000 THEN 0.0075 * t1.purchase_price * t1.numshares
          ELSE null
          END
     WHEN t1.sale_time IS NOT NULL
     THEN CASE 
          WHEN t1.numshares <=100 THEN 0.3 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 100 AND 500 THEN 0.2 * t1.sale_price * t1.numshares
          WHEN t1.numshares BETWEEN 501 AND 1000 THEN 0.125 * t1.sale_price * t1.numshares
          WHEN t1.numshares > 1000 THEN 0.0075 * t1.sale_price * t1.numshares
          ELSE null
          END
     ELSE null
     END)
;


--
--MAR Transactions
--
insert into martran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('030001', 9, 'COM3', timestamp'1997-03-03 9:47:12', 15.00, null, null,  1100)
;
insert into martran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('030002', 5, 'COM7', timestamp'1997-03-03 10:03:00', 12.875, null, null,  300)
;
insert into martran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('030003', 3, 'COM6', null, null,timestamp'1997-03-13 11:15:00', 6.375,  1200)
;
insert into martran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('030004', 4, 'COM9', timestamp'1997-03-27 12:50:11', 53.25, null, null,  100)
;
insert into martran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('030005', 10, 'COM8', null, null, timestamp'1997-03-29 9:33:00', 177.25,  300)
;
merge into martran t1
using martran t2 on t1.tid = t2.tid
when matched then
  update set commission =
    (CASE 
     WHEN t1.purchase_time IS NOT NULL
     THEN CASE
          WHEN t1.numshares <=100 THEN  0.3 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 100 AND 500 THEN 0.2 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 501 AND 1000 THEN 0.125 * t1.purchase_price * t1.numshares
          WHEN t1.numshares > 1000 THEN 0.0075 * t1.purchase_price * t1.numshares
          ELSE null
          END
     WHEN t1.sale_time IS NOT NULL
     THEN CASE 
          WHEN t1.numshares <=100 THEN 0.3 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 100 AND 500 THEN 0.2 * t1.sale_price * t1.numshares
          WHEN t1.numshares BETWEEN 501 AND 1000 THEN 0.125 * t1.sale_price * t1.numshares
          WHEN t1.numshares > 1000 THEN 0.0075 * t1.sale_price * t1.numshares
          ELSE null
          END
     ELSE null
     END)
;


--
--APR Transactions
--
insert into aprtran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('040001', 2, 'COM8', timestamp'1997-04-04 9:47:12', 135.00, null, null,  1100)
;
insert into aprtran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('040002', 7, 'COM10', null, null, timestamp'1997-04-13 11:09:14', 22.25, 300)
;
insert into aprtran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('040003', 7, 'COM6', null, null,timestamp'1997-04-14 9:43:06', 6.375,  1200)
;
merge into aprtran t1
using aprtran t2 on t1.tid = t2.tid
when matched then
  update set commission =
    (CASE 
     WHEN t1.purchase_time IS NOT NULL
     THEN CASE
          WHEN t1.numshares <=100 THEN  0.3 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 100 AND 500 THEN 0.2 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 501 AND 1000 THEN 0.125 * t1.purchase_price * t1.numshares
          WHEN t1.numshares > 1000 THEN 0.0075 * t1.purchase_price * t1.numshares
          ELSE null
          END
     WHEN t1.sale_time IS NOT NULL
     THEN CASE 
          WHEN t1.numshares <=100 THEN 0.3 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 100 AND 500 THEN 0.2 * t1.sale_price * t1.numshares
          WHEN t1.numshares BETWEEN 501 AND 1000 THEN 0.125 * t1.sale_price * t1.numshares
          WHEN t1.numshares > 1000 THEN 0.0075 * t1.sale_price * t1.numshares
          ELSE null
          END
     ELSE null
     END)
;


--
--MAY Transactions
--
insert into maytran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('050001', 3, 'COM3', timestamp'1997-05-05 9:47:12', 15.00, null, null,  600)
;
insert into maytran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('050002', 8, 'COM7', timestamp'1997-05-07 10:04:31', 12.875, null, null,  300)
;
insert into maytran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('050003', 3, 'COM6', null, null,timestamp'1997-05-11 11:15:20', 6.375,  200)
;
insert into maytran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('050004', 4, 'COM9', timestamp'1997-05-19 12:50:11', 57.25, null, null,  100)
;
insert into maytran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('050005', 10, 'COM5', null, null, timestamp'1997-05-28 9:23:40', 177.25,  300)
;
merge into maytran t1
using maytran t2 on t1.tid = t2.tid
when matched then
  update set commission =
    (CASE 
     WHEN t1.purchase_time IS NOT NULL
     THEN CASE
          WHEN t1.numshares <=100 THEN  0.3 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 100 AND 500 THEN 0.2 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 501 AND 1000 THEN 0.125 * t1.purchase_price * t1.numshares
          WHEN t1.numshares > 1000 THEN 0.0075 * t1.purchase_price * t1.numshares
          ELSE null
          END
     WHEN t1.sale_time IS NOT NULL
     THEN CASE 
          WHEN t1.numshares <=100 THEN 0.3 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 100 AND 500 THEN 0.2 * t1.sale_price * t1.numshares
          WHEN t1.numshares BETWEEN 501 AND 1000 THEN 0.125 * t1.sale_price * t1.numshares
          WHEN t1.numshares > 1000 THEN 0.0075 * t1.sale_price * t1.numshares
          ELSE null
          END
     ELSE null
     END)
;


--
--JUN Transactions
--
insert into juntran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('060001', 9, 'COM3', timestamp'1997-06-06 9:30:12', 15.00, null, null,  1000)
;
insert into juntran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('060002', 3, 'COM7', timestamp'1997-06-07 10:03:00', 12.875, null, null,  200)
;
insert into juntran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('060003', 4, 'COM6', null, null,timestamp'1997-06-12 11:15:06', 6.375,  200)
;
insert into juntran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('060004', 1, 'COM9', timestamp'1997-06-17 12:05:13', 54.25, null, null,  200)
;
insert into juntran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('060005', 1, 'COM10', null, null, timestamp'1997-06-18 10:03:03', 27.25,  300)
;
merge into juntran t1
using juntran t2 on t1.tid = t2.tid
when matched then
  update set commission =
    (CASE 
     WHEN t1.purchase_time IS NOT NULL
     THEN CASE
          WHEN t1.numshares <=100 THEN  0.3 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 100 AND 500 THEN 0.2 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 501 AND 1000 THEN 0.125 * t1.purchase_price * t1.numshares
          WHEN t1.numshares > 1000 THEN 0.0075 * t1.purchase_price * t1.numshares
          ELSE null
          END
     WHEN t1.sale_time IS NOT NULL
     THEN CASE 
          WHEN t1.numshares <=100 THEN 0.3 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 100 AND 500 THEN 0.2 * t1.sale_price * t1.numshares
          WHEN t1.numshares BETWEEN 501 AND 1000 THEN 0.125 * t1.sale_price * t1.numshares
          WHEN t1.numshares > 1000 THEN 0.0075 * t1.sale_price * t1.numshares
          ELSE null
          END
     ELSE null
     END)
;

--
--JUL Transactions
--
insert into jultran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('070001', 9, 'COM3', timestamp'1997-07-06 9:30:12', 15.00, null, null,  1000)
;
insert into jultran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('070002', 3, 'COM7', timestamp'1997-07-07 10:03:00', 12.875, null, null,  200)
;
insert into jultran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('070003', 4, 'COM6', null, null,timestamp'1997-07-12 11:15:06', 6.375,  200)
;
insert into jultran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('070004', 1, 'COM9', timestamp'1997-07-17 12:05:13', 54.25, null, null,  200)
;
insert into jultran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('070005', 1, 'COM10', null, null, timestamp'1997-07-18 10:03:03', 27.25,  300)
;
merge into jultran t1
using jultran t2 on t1.tid = t2.tid
when matched then
  update set commission =
    (CASE 
     WHEN t1.purchase_time IS NOT NULL
     THEN CASE
          WHEN t1.numshares <=100 THEN  0.3 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 100 AND 500 THEN 0.2 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 501 AND 1000 THEN 0.125 * t1.purchase_price * t1.numshares
          WHEN t1.numshares > 1000 THEN 0.0075 * t1.purchase_price * t1.numshares
          ELSE null
          END
     WHEN t1.sale_time IS NOT NULL
     THEN CASE 
          WHEN t1.numshares <=100 THEN 0.3 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 100 AND 500 THEN 0.2 * t1.sale_price * t1.numshares
          WHEN t1.numshares BETWEEN 501 AND 1000 THEN 0.125 * t1.sale_price * t1.numshares
          WHEN t1.numshares > 1000 THEN 0.0075 * t1.sale_price * t1.numshares
          ELSE null
          END
     ELSE null
     END)
;

--
--OCT Transactions
--
insert into octtran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('100001', 9, 'COM3', timestamp'1997-10-06 9:30:12', 15.00, null, null,  1000)
;
insert into octtran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('100002', 3, 'COM7', timestamp'1997-10-07 10:03:00', 12.875, null, null,  200)
;
insert into octtran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('100003', 4, 'COM6', null, null,timestamp'1997-10-12 11:15:06', 6.375,  200)
;
insert into octtran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('100004', 1, 'COM9', timestamp'1997-10-17 12:05:13', 54.25, null, null,  200)
;
insert into octtran(tid, account, security, purchase_time, purchase_price, sale_time, sale_price, numshares) 
  values('100005', 1, 'COM10', null, null, timestamp'1997-10-18 10:03:03', 27.25,  300)
;
merge into octtran t1
using octtran t2 on t1.tid = t2.tid
when matched then
  update set commission =
    (CASE 
     WHEN t1.purchase_time IS NOT NULL
     THEN CASE
          WHEN t1.numshares <=100 THEN  0.3 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 100 AND 500 THEN 0.2 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 501 AND 1000 THEN 0.125 * t1.purchase_price * t1.numshares
          WHEN t1.numshares > 1000 THEN 0.0075 * t1.purchase_price * t1.numshares
          ELSE null
          END
     WHEN t1.sale_time IS NOT NULL
     THEN CASE 
          WHEN t1.numshares <=100 THEN 0.3 * t1.purchase_price * t1.numshares
          WHEN t1.numshares BETWEEN 100 AND 500 THEN 0.2 * t1.sale_price * t1.numshares
          WHEN t1.numshares BETWEEN 501 AND 1000 THEN 0.125 * t1.sale_price * t1.numshares
          WHEN t1.numshares > 1000 THEN 0.0075 * t1.sale_price * t1.numshares
          ELSE null
          END
     ELSE null
     END)
;


--
-- ANALYZE
--
ANALYZE TABLE accounts ESTIMATE STATISTICS FOR ALL COLUMNS SAMPLE 100 PERCENT;
ANALYZE TABLE exchange ESTIMATE STATISTICS FOR ALL COLUMNS SAMPLE 100 PERCENT;
ANALYZE TABLE tickers ESTIMATE STATISTICS FOR ALL COLUMNS SAMPLE 100 PERCENT;
ANALYZE TABLE jantran ESTIMATE STATISTICS FOR ALL COLUMNS SAMPLE 100 PERCENT;
ANALYZE TABLE febtran ESTIMATE STATISTICS FOR ALL COLUMNS SAMPLE 100 PERCENT;
ANALYZE TABLE martran ESTIMATE STATISTICS FOR ALL COLUMNS SAMPLE 100 PERCENT;
ANALYZE TABLE aprtran ESTIMATE STATISTICS FOR ALL COLUMNS SAMPLE 100 PERCENT;
ANALYZE TABLE maytran ESTIMATE STATISTICS FOR ALL COLUMNS SAMPLE 100 PERCENT;
ANALYZE TABLE juntran ESTIMATE STATISTICS FOR ALL COLUMNS SAMPLE 100 PERCENT;
ANALYZE TABLE jultran ESTIMATE STATISTICS FOR ALL COLUMNS SAMPLE 100 PERCENT;
ANALYZE TABLE augtran ESTIMATE STATISTICS FOR ALL COLUMNS SAMPLE 100 PERCENT;
ANALYZE TABLE septran ESTIMATE STATISTICS FOR ALL COLUMNS SAMPLE 100 PERCENT;
ANALYZE TABLE octtran ESTIMATE STATISTICS FOR ALL COLUMNS SAMPLE 100 PERCENT;
ANALYZE TABLE novtran ESTIMATE STATISTICS FOR ALL COLUMNS SAMPLE 100 PERCENT;
ANALYZE TABLE dectran ESTIMATE STATISTICS FOR ALL COLUMNS SAMPLE 100 PERCENT;
