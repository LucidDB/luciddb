-- Q17 (tpch2.6.1)

--
-- rewritten query before subquery support
--
SELECT SUM(L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY
FROM TPCH.LINEITEM L, TPCH.PART,
     (SELECT L1.L_PARTKEY, (0.2 * AVG(L_QUANTITY)) AS AVGQTY 
      FROM TPCH.LINEITEM L1
      GROUP BY L1.L_PARTKEY) AS TEMP
WHERE
    P_PARTKEY = L.L_PARTKEY AND
    P_BRAND = 'Brand#23' AND
    P_CONTAINER = 'MED BOX' AND
    P_PARTKEY = TEMP.L_PARTKEY AND
    L.L_QUANTITY < TEMP.AVGQTY;

--
-- original tpch query
-- still has cartesian product
--
--SELECT
--    SUM(L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY
--FROM
--    TPCH.LINEITEM,
--    TPCH.PART
--WHERE
--    P_PARTKEY = L_PARTKEY
--    AND P_BRAND = 'Brand#23'
--    AND P_CONTAINER = 'MED BOX'
--    AND L_QUANTITY < (
--        SELECT
--            0.2 * AVG(L_QUANTITY)
--        FROM
--            TPCH.LINEITEM
--        WHERE
--            L_PARTKEY = P_PARTKEY
--   );
