-- Q20 (tpch2.6.1)

SELECT
    S_NAME,
    S_ADDRESS
FROM
    TPCH.SUPPLIER,
    TPCH.NATION
WHERE
    S_SUPPKEY IN (
        SELECT
            PS_SUPPKEY
        FROM
            TPCH.PARTSUPP
        WHERE
            PS_PARTKEY IN (
                SELECT
                    P_PARTKEY
                FROM
                    TPCH.PART
                WHERE
                    P_NAME LIKE 'forest%'
            )
        AND PS_AVAILQTY > (
            SELECT
                0.5 * SUM(L_QUANTITY)
            FROM
                TPCH.LINEITEM
            WHERE
                L_PARTKEY = PS_PARTKEY
                AND L_SUPPKEY = PS_SUPPKEY
                AND L_SHIPDATE >= DATE '1994-01-01'
                AND L_SHIPDATE < DATE '1995-01-01'
            )
        )
        AND S_NATIONKEY = N_NATIONKEY
        AND N_NAME = 'CANADA'
ORDER BY
    S_NAME;
