-- Q21 (tpch2.6.1)

-- do NOT add to test; still have cartesian product

!set rowlimit 100

SELECT
    S_NAME,
    COUNT(*) AS NUMWAIT
FROM
    TPCH.SUPPLIER,
    TPCH.LINEITEM L1,
    TPCH.ORDERS,
    TPCH.NATION
WHERE
    S_SUPPKEY = L1.L_SUPPKEY
    AND O_ORDERKEY = L1.L_ORDERKEY
    AND O_ORDERSTATUS = 'F'
    AND L1.L_RECEIPTDATE > L1.L_COMMITDATE
    AND EXISTS (
         SELECT
            *
         FROM
            TPCH.LINEITEM L2
         WHERE
            L2.L_ORDERKEY = L1.L_ORDERKEY
            AND L2.L_SUPPKEY <> L1.L_SUPPKEY
    )
    AND NOT EXISTS (
         SELECT
            *
         FROM
            TPCH.LINEITEM L3
         WHERE
            L3.L_ORDERKEY = L1.L_ORDERKEY
            AND L3.L_SUPPKEY <> L1.L_SUPPKEY
            AND L3.L_RECEIPTDATE > L3.L_COMMITDATE
    )
    AND S_NATIONKEY = N_NATIONKEY
    AND N_NAME = 'SAUDI ARABIA'
GROUP BY
    S_NAME
ORDER BY
    NUMWAIT DESC,
    S_NAME;
