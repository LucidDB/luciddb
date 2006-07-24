create schema stkmkt
;
set schema 'stkmkt'
;


create table accounts(acct_no integer primary key, 
                      acct_name varchar(50), 
                      acct_remarks varchar(100))
;
create table exchange(symbol varchar(10) primary key,
                      name   varchar(50), 
                      location varchar(100))
;
create table tickers(symbol char(5) primary key, 
                     company varchar(100),
                     exchange varchar(10))
--                     exchange varchar(10) references exchange(symbol))
;

create table jantran(tid char(10) UNIQUE, 
                     account integer, 
                     security char(5), 
--                     account integer references accounts(acct_no), 
--                     security char(5) references tickers(symbol), 
                     purchase_time timestamp, 
                     purchase_price double, 
                     sale_time timestamp,
                     sale_price double,
                     numshares integer, 
                     commission double)
;

create table febtran(tid char(10) UNIQUE, 
                     account integer, 
                     security char(5), 
--                     account integer references accounts(acct_no), 
--                     security char(5) references tickers(symbol), 
                     purchase_time timestamp, 
                     purchase_price double, 
                     sale_time timestamp,
                     sale_price double,
                     numshares integer, 
                     commission double)
;


create table martran(tid char(10) UNIQUE, 
                     account integer, 
                     security char(5), 
--                     account integer references accounts(acct_no), 
--                     security char(5) references tickers(symbol), 
                     purchase_time timestamp, 
                     purchase_price double, 
                     sale_time timestamp,
                     sale_price double,
                     numshares integer, 
                     commission double)
;

create table aprtran(tid char(10) UNIQUE, 
                     account integer, 
                     security char(5), 
--                     account integer references accounts(acct_no), 
--                     security char(5) references tickers(symbol), 
                     purchase_time timestamp, 
                     purchase_price double, 
                     sale_time timestamp,
                     sale_price double,
                     numshares integer, 
                     commission double)
;

create table maytran(tid char(10) UNIQUE, 
                     account integer, 
                     security char(5), 
--                     account integer references accounts(acct_no), 
--                     security char(5) references tickers(symbol), 
                     purchase_time timestamp, 
                     purchase_price double, 
                     sale_time timestamp,
                     sale_price double,
                     numshares integer, 
                     commission double)
;

create table juntran(tid char(10) UNIQUE, 
                     account integer, 
                     security char(5), 
--                     account integer references accounts(acct_no), 
--                     security char(5) references tickers(symbol), 
                     purchase_time timestamp, 
                     purchase_price double, 
                     sale_time timestamp,
                     sale_price double,
                     numshares integer, 
                     commission double)
;

create table jultran(tid char(10) UNIQUE, 
                     account integer, 
                     security char(5), 
--                     account integer references accounts(acct_no), 
--                     security char(5) references tickers(symbol), 
                     purchase_time timestamp, 
                     purchase_price double, 
                     sale_time timestamp,
                     sale_price double,
                     numshares integer, 
                     commission double)
;

create table augtran(tid char(10) UNIQUE, 
                     account integer, 
                     security char(5), 
--                     account integer references accounts(acct_no), 
--                     security char(5) references tickers(symbol), 
                     purchase_time timestamp, 
                     purchase_price double, 
                     sale_time timestamp,
                     sale_price double,
                     numshares integer, 
                     commission double)
;

create table septran(tid char(10) UNIQUE, 
                     account integer, 
                     security char(5), 
--                     account integer references accounts(acct_no), 
--                     security char(5) references tickers(symbol), 
                     purchase_time timestamp, 
                     purchase_price double, 
                     sale_time timestamp,
                     sale_price double,
                     numshares integer, 
                     commission double)
;

create table octtran(tid char(10) UNIQUE, 
                     account integer, 
                     security char(5), 
--                     account integer references accounts(acct_no), 
--                     security char(5) references tickers(symbol), 
                     purchase_time timestamp, 
                     purchase_price double, 
                     sale_time timestamp,
                     sale_price double,
                     numshares integer, 
                     commission double)
;

create table novtran(tid char(10) UNIQUE, 
                     account integer, 
                     security char(5), 
--                     account integer references accounts(acct_no), 
--                     security char(5) references tickers(symbol), 
                     purchase_time timestamp, 
                     purchase_price double, 
                     sale_time timestamp,
                     sale_price double,
                     numshares integer, 
                     commission double)
;

create table dectran(tid char(10) UNIQUE, 
                     account integer, 
                     security char(5), 
--                     account integer references accounts(acct_no), 
--                     security char(5) references tickers(symbol), 
                     purchase_time timestamp, 
                     purchase_price double, 
                     sale_time timestamp,
                     sale_price double,
                     numshares integer, 
                     commission double)
;
