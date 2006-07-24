set schema 'tpch';

SELECT S.s_acctbal, S.s_name, n_name, PS.ps_partkey, P.p_mfgr,
       S.s_address, S.s_phone, S.s_comment
FROM Part P, Supplier S, Partsupp PS, Nation, Region,
     (SELECT PS1.ps_partkey, min(PS1.ps_supplycost) as mincost
      FROM Partsupp PS1, Supplier S1, Nation N1, Region R1
      WHERE PS1.ps_suppkey = S1.s_suppkey AND
            S1.s_nationkey = N1.n_nationkey AND
            N1.n_regionkey = R1.r_regionkey AND
            R1.r_name = 'EUROPE'
      GROUP BY PS1.ps_partkey) AS Temp
WHERE P.p_partkey = PS.ps_partkey AND
      S.s_suppkey = PS.ps_suppkey AND
      P.p_size = 15 AND
      P.p_type LIKE '%BRASS' AND
      S.s_nationkey = n_nationkey AND
      n_regionkey = r_regionkey AND
      PS.ps_partkey = Temp.ps_partkey AND
      PS.ps_supplycost = Temp.mincost
ORDER BY S.s_acctbal, n_name, S.s_name, PS.ps_partkey;