-- $Id$

-- Test nested loop joins

create schema nlj;
set schema 'nlj';

alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

create table convRates(
    fromCurrency varchar(5) not null,
    toCurrency varchar(5) not null,
    rate float,
    fromDate date,
    toDate date not null);
create table xacts(
    xid int not null,
    amount decimal(10,2) not null,
    currency char(3),
    xactDate date);
create view v as 
    select xid, xactDate, currency, amount as origAmount,
            toCurrency, cast(amount * rate as decimal(10,2)) as convertedAmount
        from xacts left outer join convRates
        on currency = fromCurrency and xactDate between fromDate and toDate;

insert into convRates values('USD', 'EUR', .76, null, date '2006-12-31');
insert into convRates values('USD', 'EUR', .7483, date '2007-1-1',
    date '2007-3-31');
insert into convRates values('USD', 'EUR', .75, date '2007-4-1',
    date '2007-6-30');
insert into convRates values('USD', 'GBP', .5059, date '2007-1-1',
    date '2007-3-31');
insert into convRates values('USD', 'GBP', .5, date '2007-4-1',
    date '2007-6-30');
insert into convRates values('EUR', 'GBP', .6762, date '2007-1-1',
    date '2007-3-31');
insert into convRates values('EUR', 'GBP', .68, date '2007-4-1',
    date '2007-6-30');
insert into convRates values('EUR', 'JPY', 165.12, date '2007-1-1',
    date '2007-3-31');
insert into convRates values('EUR', 'JPY', 165, date '2007-4-1',
    date '2007-6-30');
insert into convRates values('GBP', 'JPY', 244.22, date '2007-1-1',
    date '2007-3-31');
insert into convRates values('GBP', 'JPY', 244, date '2007-4-1',
    date '2007-6-30');
insert into convRates values('GBP', 'USD', 1.9766, date '2007-1-1',
    date '2007-3-31');
insert into convRates values('GBP', 'USD', 2, date '2007-4-1',
    date '2007-6-30');
insert into convRates values('JPY', 'USD', .008095, date '2007-1-1',
    date '2007-3-31');
insert into convRates values('JPY', 'USD', .0081, date '2007-4-1',
    date '2007-6-30');
insert into convRates values('JPY', 'EUR', .006056, date '2007-1-1',
    date '2007-3-31');
insert into convRates values('JPY', 'EUR', .0061, date '2007-4-1',
    date '2007-6-30');

insert into xacts values(1, 100, 'USD', date '2006-12-31');
insert into xacts values(2, 200, 'EUR', date '2007-1-1');
insert into xacts values(3, 300, 'GBP', date '2007-2-1');
insert into xacts values(4, 50000, 'JPY', date '2007-3-31');
insert into xacts values(5, 500, 'USD', date '2007-4-1');
insert into xacts values(6, 600, 'EUR', date '2007-5-1');
insert into xacts values(7, 700, 'GBP', date '2007-6-30');
insert into xacts values(8, 80000, 'JPY', date '2007-7-1');
insert into xacts values(9, 900, 'USD', null);
insert into xacts values(10, 1000, null, date '2007-3-1');
insert into xacts values(11, 1100, null, null);

---------------------------
-- first run with Java calc
---------------------------
alter system set "calcVirtualMachine" = 'CALCVM_JAVA';

-- Only index lookup needed
select xid, xactDate, currency, amount as origAmount,
        toCurrency, toDate,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate >= fromDate
    order by currency, toCurrency, xactDate, toDate;
select xid, xactDate, currency, amount as origAmount,
        toCurrency, toDate,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on xactDate > fromDate and currency = fromCurrency
    order by currency, toCurrency, xactDate, toDate;
select xid, xactDate, currency, amount as origAmount,
        toCurrency, fromDate,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on fromCurrency = currency and toDate >= xactDate
    order by currency, toCurrency, xactDate, fromDate;
select xid, xactDate, currency, amount as origAmount,
        toCurrency, fromDate,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on toDate > xactDate and fromCurrency = currency
    order by currency, toCurrency, xactDate, fromDate;

-- Index lookup + reshape
select xid, xactDate, currency, amount as origAmount,
        toCurrency,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate between fromDate and toDate
    order by currency, toCurrency, xactDate;
select xid, xactDate, currency, amount as origAmount,
        toCurrency, 
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and fromDate < xactDate and toDate > xactDate
    order by currency, toCurrency, xactDate;
select xid, xactDate, currency, amount as origAmount,
        toCurrency,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate <= toDate and xactDate >= fromDate
    order by currency, toCurrency, xactDate;
select xid, xactDate, currency, amount as origAmount,
        toCurrency,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and toDate > xactDate and fromDate < xactDate
    order by currency, toCurrency, xactDate;

-- Index lookup with functional expressions as join keys
select xid, xactDate, currency, amount as origAmount,
        toCurrency, toDate,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on trim(currency) = trim(fromCurrency) and xactDate >= fromDate
    order by currency, toCurrency, xactDate, toDate;
select xid, xactDate, currency, amount as origAmount,
        toCurrency,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on xactDate between fromDate and toDate and
        trim(currency) = trim(fromCurrency)
    order by currency, toCurrency, xactDate;
select xid, xactDate, currency, amount as origAmount, rate, toCurrency,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on trim(currency) = trim(fromCurrency) and abs(amount) < abs(rate*2)
    order by currency, toCurrency, xactDate;

-- Index lookup + calc
select xid, xactDate, currency, amount as origAmount,
        toCurrency, toDate,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate >= fromDate and
    trim(currency) = 'EUR'
    order by currency, toCurrency, xactDate, toDate;

-- Index lookup + reshape + calc
select xid, xactDate, currency, amount as origAmount, rate, toCurrency,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate between fromDate and toDate
        and amount < rate*2
    order by currency, toCurrency, xactDate;
-- rerun the query
select xid, xactDate, currency, amount as origAmount, rate, toCurrency,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate between fromDate and toDate
        and amount < rate*2
    order by currency, toCurrency, xactDate;

-- Calc only
select xid, xactDate, currency, amount as origAmount,
        fromCurrency, toCurrency, fromDate, toDate,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts, convRates
    where currency in (fromCurrency, toCurrency)
    order by currency, fromCurrency, toCurrency, xactDate, fromDate, toDate;

-- RHS of NLJ should be buffered
select xid, xactDate, currency, amount as origAmount,
        fromCurrency, toCurrency, fromDate, toDate,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts, convRates
    where currency in (fromCurrency, toCurrency) and toCurrency = 'USD'
        and fromCurrency = 'JPY'
    order by currency, fromCurrency, toCurrency, xactDate, fromDate, toDate;

-- Right outer join
select xid, xactDate, currency, amount as origAmount,
        toCurrency, 
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from convRates right outer join xacts
    on currency = fromCurrency and xactDate between fromDate and toDate
    order by currency, toCurrency, xactDate;

-- Table level filters applied on tables
select xid, xactDate, currency, amount as origAmount,
        toCurrency, 
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate between fromDate and toDate
        and toCurrency = 'JPY'
    order by currency, toCurrency, xactDate;
select xid, xactDate, currency, amount as origAmount,
        toCurrency, 
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate between fromDate and toDate
        and currency = 'USD'
    order by currency, toCurrency, xactDate;
select xid, xactDate, currency, amount as origAmount,
        toCurrency,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate between fromDate and toDate
        and currency = 'EUR' and toCurrency = 'GBP'
    order by currency, toCurrency, xactDate;
select xid, xactDate, currency, amount as origAmount,
        toCurrency, 
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate between fromDate and toDate
    where toCurrency = 'JPY'
    order by currency, toCurrency, xactDate;
select xid, xactDate, currency, amount as origAmount,
        toCurrency, 
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate between fromDate and toDate
    where currency = 'USD'
    order by currency, toCurrency, xactDate;
select xid, xactDate, currency, amount as origAmount,
        toCurrency,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate between fromDate and toDate
    where currency = 'EUR' and toCurrency = 'GBP'
    order by currency, toCurrency, xactDate;

-- Multi-way joins
select v1.xid, v1.xactDate, v1.currency, v1.origAmount, v1.toCurrency,
        v1.convertedAmount, v2.toCurrency, v2.convertedAmount
    from v v1, v v2
    where v1.currency = 'EUR' and v1.toCurrency = 'GBP' and
        v2.toCurrency = 'JPY' and v1.xid = v2.xid
    order by xactDate;
select v1.xid, v1.xactDate, v2.xactDate as xactDate2, v1.currency,
        v1.origAmount, v1.toCurrency, v1.convertedAmount, v2.toCurrency,
        v2.convertedAmount
    from v v1 left outer join v v2
    on v1.xactDate > v2.xactDate
    where v1.currency = 'EUR' and v1.toCurrency = 'GBP' and
        v2.toCurrency = 'JPY'
    order by xactDate2;

---------
-- Merges
---------

create table target(
    xid int not null, xactDate date, currency varchar(5), toCurrency varchar(5),
    amount decimal(10,2), convertedAmount decimal(10,2));
insert into target
    select xid, xactDate, currency, toCurrency, origAmount,
        convertedAmount
        from v
        where currency is not null and xactDate is not null and
            toCurrency is not null;
select * from target order by currency, toCurrency, xactDate;
        
-- NLJ used in view that is the source for the merge
merge into target t
    using v on t.xid = v.xid and t.currency = v.currency and
        t.toCurrency = v.toCurrency and t.xactDate = v.xactDate
    when matched then
        update set convertedAmount = v.convertedAmount + 10.56
    when not matched then
        insert values(
        v.xid, v.xactDate, v.currency, v.toCurrency, v.origAmount,
            v.convertedAmount);
select * from target order by currency, toCurrency, xactDate;

-- NLJ in the using clause of the MERGE statement
merge into target t
    using convRates cv on t.currency = cv.fromCurrency and
        t.toCurrency = cv.toCurrency and
        t.xactDate between cv.fromDate and cv.toDate
    when matched then
        update set convertedAmount =
            cast((t.convertedAmount - 10.56)/cv.rate  as decimal(10,2))
    when not matched then
        insert values(-1, cv.fromDate, cv.fromCurrency, cv.toCurrency,
            null, null);
-- the convertedAmount should now be equal to the original amount
select * from target order by currency, toCurrency, xactDate;

----------------------------------------------
-- Additional testcases to test misc codepaths
----------------------------------------------

create table ints_notnullable(
    id int not null, a bigint not null, b int not null, c smallint not null);
create table ints_nullable(
    id int not null, a bigint, b int, c smallint);

insert into ints_notnullable values(1, 1, 1, 1);
insert into ints_notnullable values(2, 2, 2, 2);
insert into ints_notnullable values(2, 5000000000, 2, 2);
insert into ints_notnullable values(3, 3, 3, 3);
insert into ints_notnullable values(3, 3, 100000, 3);
insert into ints_nullable select * from ints_notnullable;

-- casting feasible
select * from ints_notnullable i_nn left outer join ints_nullable i_n
    on i_nn.id = i_n.id and i_nn.a > i_n.a
    order by 1, 2, 3, 4, 5, 6, 7, 8;

-- casting should be feasible because RHS can be cast to LHS's type
select * from ints_notnullable i_nn left outer join ints_nullable i_n
    on i_nn.id = i_n.id and i_nn.a > i_n.b
    order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from ints_notnullable i_nn left outer join ints_nullable i_n
    on i_nn.a = i_n.b and i_nn.id >= i_n.id
    order by 1, 2, 3, 4, 5, 6, 7, 8;

-- LHS is cast to two different types
select * from ints_notnullable i_nn left outer join ints_nullable i_n
    on i_nn.id = i_n.id and i_nn.c >= i_n.a and i_nn.c < i_n.b
    order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from ints_notnullable i_nn left outer join ints_nullable i_n
    on i_nn.c = i_n.a and i_nn.c < i_n.b
    order by 1, 2, 3, 4, 5, 6, 7, 8;

-- column filtered by both reshape and calc
select * from ints_notnullable i_nn left outer join ints_nullable i_n
    on i_nn.id > i_n.id and i_nn.a >= i_n.a and i_nn.a > i_n.b
    order by 1, 2, 3, 4, 5, 6, 7, 8;

-- more than one filter to be processed by calc
select * from ints_notnullable i_nn left outer join ints_nullable i_n
    on i_nn.id > i_n.id and i_nn.a > i_n.a and i_nn.b > i_n.b and
        i_nn.c > i_n.c
    order by 1, 2, 3, 4, 5, 6, 7, 8;

-- LHS of NLJ is empty
select xid, xactDate, currency, amount as origAmount,
    toCurrency, toDate,
    cast(amount * rate as decimal(10,2)) as convertedAmount
    from (select * from xacts where currency = 'CNY') left outer join convRates
    on currency = fromCurrency and xactDate >= fromDate;

----------------------------------------
-- explain outputs for the above queries
----------------------------------------
!set outputformat csv

-- Only index lookup needed
explain plan for
select xid, xactDate, currency, amount as origAmount,
        toCurrency, toDate,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate >= fromDate;
explain plan for
select xid, xactDate, currency, amount as origAmount,
        toCurrency, toDate,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on xactDate > fromDate and currency = fromCurrency;
explain plan for
select xid, xactDate, currency, amount as origAmount,
        toCurrency, fromDate,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on fromCurrency = currency and toDate >= xactDate;
explain plan for
select xid, xactDate, currency, amount as origAmount,
        toCurrency, fromDate,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on toDate > xactDate and fromCurrency = currency;
explain plan for
select xid, xactDate, currency, amount as origAmount,
    toCurrency, toDate,
    cast(amount * rate as decimal(10,2)) as convertedAmount
    from (select * from xacts where currency = 'CNY') left outer join convRates
    on currency = fromCurrency and xactDate >= fromDate;

-- Index lookup + reshape
explain plan for
select xid, xactDate, currency, amount as origAmount,
        toCurrency,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate between fromDate and toDate;
explain plan for
select xid, xactDate, currency, amount as origAmount,
        toCurrency, 
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and fromDate < xactDate and toDate > xactDate;
explain plan for
select xid, xactDate, currency, amount as origAmount,
        toCurrency,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate <= toDate and xactDate >= fromDate;
explain plan for
select xid, xactDate, currency, amount as origAmount,
        toCurrency,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and toDate > xactDate and fromDate < xactDate;

-- Index lookup with functional expressions as join keys
explain plan for
select xid, xactDate, currency, amount as origAmount,
        toCurrency, toDate,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on trim(currency) = trim(fromCurrency) and xactDate >= fromDate;
explain plan for
select xid, xactDate, currency, amount as origAmount,
        toCurrency,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on xactDate between fromDate and toDate and
        trim(currency) = trim(fromCurrency);
explain plan for
select xid, xactDate, currency, amount as origAmount, rate, toCurrency,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on trim(currency) = trim(fromCurrency) and abs(amount) < abs(rate*2);

-- Index lookup + calc
explain plan for
select xid, xactDate, currency, amount as origAmount,
        toCurrency, toDate,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate >= fromDate and
    trim(currency) = 'EUR';

-- Index lookup + reshape + calc
explain plan for
select xid, xactDate, currency, amount as origAmount, rate, toCurrency,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate between fromDate and toDate
        and amount < rate*2;

-- Calc only
explain plan for
select xid, xactDate, currency, amount as origAmount,
        fromCurrency, toCurrency, fromDate, toDate,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts, convRates
    where currency in (fromCurrency, toCurrency);

-- RHS of join should be buffered
explain plan for
select xid, xactDate, currency, amount as origAmount,
        fromCurrency, toCurrency, fromDate, toDate,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts, convRates
    where currency in (fromCurrency, toCurrency) and toCurrency = 'USD'
        and fromCurrency = 'JPY';

-- Right outer join
explain plan for
select xid, xactDate, currency, amount as origAmount,
        toCurrency, 
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from convRates right outer join xacts
    on currency = fromCurrency and xactDate between fromDate and toDate;

-- Table level filters applied on tables
explain plan for
select xid, xactDate, currency, amount as origAmount,
        toCurrency, 
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate between fromDate and toDate
        and toCurrency = 'JPY';
explain plan for
select xid, xactDate, currency, amount as origAmount,
        toCurrency, 
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate between fromDate and toDate
        and currency = 'USD';
explain plan for
select xid, xactDate, currency, amount as origAmount,
        toCurrency,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate between fromDate and toDate
        and currency = 'EUR' and toCurrency = 'GBP';
explain plan for
select xid, xactDate, currency, amount as origAmount,
        toCurrency, 
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate between fromDate and toDate
    where toCurrency = 'JPY';
explain plan for
select xid, xactDate, currency, amount as origAmount,
        toCurrency, 
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate between fromDate and toDate
    where currency = 'USD';
explain plan for
select xid, xactDate, currency, amount as origAmount,
        toCurrency,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate between fromDate and toDate
    where currency = 'EUR' and toCurrency = 'GBP';

-- Multi-way joins
explain plan for
select v1.xid, v1.xactDate, v1.currency, v1.origAmount, v1.toCurrency,
        v1.convertedAmount, v2.toCurrency, v2.convertedAmount
    from v v1, v v2
    where v1.currency = 'EUR' and v1.toCurrency = 'GBP' and
        v2.toCurrency = 'JPY' and v1.xid = v2.xid;
explain plan for
select v1.xid, v1.xactDate, v2.xactDate as xactDate2, v1.currency,
        v1.origAmount, v1.toCurrency, v1.convertedAmount, v2.toCurrency,
        v2.convertedAmount
    from v v1 left outer join v v2
    on v1.xactDate > v2.xactDate
    where v1.currency = 'EUR' and v1.toCurrency = 'GBP' and
        v2.toCurrency = 'JPY';

-- Merge
explain plan for
merge into target t
    using v on t.xid = v.xid and t.currency = v.currency and
        t.toCurrency = v.toCurrency and t.xactDate = v.xactDate
    when matched then
        update set convertedAmount = v.convertedAmount + 10.56
    when not matched then
        insert values(
        v.xid, v.xactDate, v.currency, v.toCurrency, v.origAmount,
            v.convertedAmount);
explain plan for
merge into target t
    using convRates cv on t.currency = cv.fromCurrency and
        t.toCurrency = cv.toCurrency and
        t.xactDate between cv.fromDate and cv.toDate
    when matched then
        update set convertedAmount =
            cast((t.convertedAmount - 10.56)/cv.rate  as decimal(10,2))
    when not matched then
        insert values(-1, cv.fromDate, cv.fromCurrency, cv.toCurrency,
            null, null);

-- casting feasible
explain plan for
select * from ints_notnullable i_nn left outer join ints_nullable i_n
    on i_nn.id = i_n.id and i_nn.a > i_n.a;

-- casting should be feasible because RHS can be cast to LHS's type
explain plan for
select * from ints_notnullable i_nn left outer join ints_nullable i_n
    on i_nn.id = i_n.id and i_nn.a > i_n.b;
explain plan for
select * from ints_notnullable i_nn left outer join ints_nullable i_n
    on i_nn.a = i_n.b and i_nn.id >= i_n.id;

-- LHS is cast to two different types
explain plan for
select * from ints_notnullable i_nn left outer join ints_nullable i_n
    on i_nn.id = i_n.id and i_nn.c >= i_n.a and i_nn.c < i_n.b;
explain plan for
select * from ints_notnullable i_nn left outer join ints_nullable i_n
    on i_nn.c = i_n.a and i_nn.c < i_n.b;

-- column filtered by both reshape and calc
explain plan for
select * from ints_notnullable i_nn left outer join ints_nullable i_n
    on i_nn.id > i_n.id and i_nn.a >= i_n.a and i_nn.a > i_n.b;

-- more than one filter to be processed by calc
explain plan for
select * from ints_notnullable i_nn left outer join ints_nullable i_n
    on i_nn.id > i_n.id and i_nn.a > i_n.a and i_nn.b > i_n.b and
        i_nn.c > i_n.c;

------------------------------------------------
-- Rerun queries that use calcs with Fennel Calc
------------------------------------------------

!set outputformat table
alter system set "calcVirtualMachine" = 'CALCVM_FENNEL';

select xid, xactDate, currency, amount as origAmount,
        toCurrency, toDate,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on trim(currency) = trim(fromCurrency) and xactDate >= fromDate
    order by currency, toCurrency, xactDate, toDate;
select xid, xactDate, currency, amount as origAmount,
        toCurrency,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on xactDate between fromDate and toDate and
        trim(currency) = trim(fromCurrency)
    order by currency, toCurrency, xactDate;
select xid, xactDate, currency, amount as origAmount,
        fromCurrency, toCurrency, fromDate, toDate,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts, convRates
    where currency in (fromCurrency, toCurrency)
    order by currency, fromCurrency, toCurrency, xactDate, fromDate, toDate;
select * from ints_notnullable i_nn left outer join ints_nullable i_n
    on i_nn.id = i_n.id and i_nn.a > i_n.b
    order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from ints_notnullable i_nn left outer join ints_nullable i_n
    on i_nn.a = i_n.b and i_nn.id >= i_n.id
    order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from ints_notnullable i_nn left outer join ints_nullable i_n
    on i_nn.id = i_n.id and i_nn.c >= i_n.a and i_nn.c < i_n.b
    order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from ints_notnullable i_nn left outer join ints_nullable i_n
    on i_nn.c = i_n.a and i_nn.c < i_n.b
    order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from ints_notnullable i_nn left outer join ints_nullable i_n
    on i_nn.id > i_n.id and i_nn.a >= i_n.a and i_nn.a > i_n.b
    order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from ints_notnullable i_nn left outer join ints_nullable i_n
    on i_nn.id > i_n.id and i_nn.a > i_n.a and i_nn.b > i_n.b and
        i_nn.c > i_n.c
    order by 1, 2, 3, 4, 5, 6, 7, 8;

----------------
-- Misc tests --
----------------

-- LER-6753 -- Make sure TRIM expression in the ON clause is converted into
-- a constant. The EXPLAIN PLAN should *not* contain a TRIM.
!set outputformat csv
explain plan for 
select xid, xactDate, currency, amount as origAmount,
        toCurrency, toDate,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate >= fromDate and
    currency = trim('EUR   ');
!set outputformat table
select xid, xactDate, currency, amount as origAmount,
        toCurrency, toDate,
        cast(amount * rate as decimal(10,2)) as convertedAmount
    from xacts left outer join convRates
    on currency = fromCurrency and xactDate >= fromDate and
    currency = trim('EUR   ')
    order by currency, toCurrency, xactDate, toDate;

-- LER-10967 -- Nested loop join on large keys
-- Temporarily bump up expectedConcurrentStatements to reduce the amount of
-- memory available to queries.  This assumes the default Farrago cache setting 
-- of 1000 pages.  Verify this by ensuring that the first alter returns an
-- error.
alter system set "expectedConcurrentStatements" = 201;
alter system set "expectedConcurrentStatements" = 200;
!set outputformat csv
explain plan for 
select x1.currency, x2.currency from xacts x1 left outer join xacts x2
    on cast(x1.currency as varchar(32768)) > 
        cast(x2.currency as varchar(32768))
    group by x1.currency, x2.currency;
!set outputformat table
select x1.currency, x2.currency from xacts x1 left outer join xacts x2
    on cast(x1.currency as varchar(32768)) > 
        cast(x2.currency as varchar(32768))
    group by x1.currency, x2.currency
    order by 1, 2;
alter system set "expectedConcurrentStatements" = 4;
