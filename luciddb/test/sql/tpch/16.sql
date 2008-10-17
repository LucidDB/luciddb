-- Q16 (tpch2.6.1)

!set incremental on

--
-- rewritten query before subquery support
--
--SELECT P_BRAND, P_TYPE, P_SIZE, COUNT(DISTINCT PS_SUPPKEY) AS SUPPLIER_CNT
--FROM PARTSUPP, PART, SUPPLIER
--WHERE
--        P_PARTKEY = PS_PARTKEY AND
--        P_BRAND <> 'Brand#45' AND
--        P_TYPE NOT LIKE 'MEDIUM POLISHED%' AND
--        P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9) AND
--        PS_SUPPKEY = S_SUPPKEY AND
--        S_COMMENT NOT LIKE  '%Better Business Bureau%Complaints%'
--GROUP BY P_BRAND, P_TYPE, P_SIZE
--ORDER BY SUPPLIER_CNT, P_BRAND, P_TYPE, P_SIZE;

SELECT
    P_BRAND,
    P_TYPE,
    P_SIZE,
    COUNT(DISTINCT PS_SUPPKEY) AS SUPPLIER_CNT
FROM
    TPCH.PARTSUPP,
    TPCH.PART
WHERE
    P_PARTKEY = PS_PARTKEY
    AND P_BRAND <> 'Brand#45'
    AND P_TYPE NOT LIKE 'MEDIUM POLISHED%'
    AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND PS_SUPPKEY NOT IN (
        SELECT
            S_SUPPKEY
        FROM
            TPCH.SUPPLIER
        WHERE
            S_COMMENT LIKE '%Customer%Complaints%'
    )
GROUP BY
    P_BRAND,
    P_TYPE,
    P_SIZE
ORDER BY
    SUPPLIER_CNT DESC,
    P_BRAND,
    P_TYPE,
    P_SIZE;
