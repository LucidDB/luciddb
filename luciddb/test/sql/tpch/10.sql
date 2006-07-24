-- TPC-D Parameter Substitution (Version 1.1.0D)
-- using default substitutions

set schema 'tpch';

SELECT
        C_CUSTKEY,
        C_NAME,
        SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
        C_ACCTBAL,
        N_NAME,
        C_ADDRESS,
        C_PHONE,
        C_COMMENT
FROM
        CUSTOMER,
        ORDERS,
        LINEITEM,
        NATION
WHERE
        C_CUSTKEY = O_CUSTKEY AND
        L_ORDERKEY = O_ORDERKEY AND
        O_ORDERDATE >= date '1993-10-01' AND
        O_ORDERDATE < DATE '1994-01-01' AND 
        L_RETURNFLAG = 'R' AND
        C_NATIONKEY = N_NATIONKEY
GROUP BY
        C_CUSTKEY,
        C_NAME,
        C_ACCTBAL,
        C_PHONE,
        N_NAME,
        C_ADDRESS,
        C_COMMENT
ORDER BY REVENUE
;