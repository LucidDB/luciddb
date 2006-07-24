
CREATE TABLE TPCH.NATION  ( N_NATIONKEY  INTEGER PRIMARY KEY,
                            N_NAME       VARCHAR(25) NOT NULL,
                            N_REGIONKEY  INTEGER NOT NULL,
                            N_COMMENT    VARCHAR(152));


CREATE TABLE TPCH.REGION  ( R_REGIONKEY  INTEGER PRIMARY KEY,
                            R_NAME       VARCHAR(25) NOT NULL,
                            R_COMMENT    VARCHAR(152));

CREATE TABLE TPCH.PART  ( P_PARTKEY     INTEGER PRIMARY KEY,
                          P_NAME        VARCHAR(55) NOT NULL,
                          P_MFGR        VARCHAR(25) NOT NULL,
                          P_BRAND       VARCHAR(10) NOT NULL,
                          P_TYPE        VARCHAR(25) NOT NULL,
                          P_SIZE        INTEGER NOT NULL,
                          P_CONTAINER   VARCHAR(10) NOT NULL,
                          P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                          P_COMMENT     VARCHAR(23) NOT NULL );

CREATE TABLE TPCH.SUPPLIER ( S_SUPPKEY     INTEGER PRIMARY KEY,
                             S_NAME        VARCHAR(25) NOT NULL,
                             S_ADDRESS     VARCHAR(40) NOT NULL,
                             S_NATIONKEY   INTEGER NOT NULL,
                             S_PHONE       VARCHAR(15) NOT NULL,
                             S_ACCTBAL     DECIMAL(15,2) NOT NULL,
                             S_COMMENT     VARCHAR(101) NOT NULL);

CREATE TABLE TPCH.PARTSUPP ( PS_PARTKEY     INTEGER,
                             PS_SUPPKEY     INTEGER,
                             PS_AVAILQTY    INTEGER NOT NULL,
                             PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                             PS_COMMENT     VARCHAR(199) NOT NULL,
                             PRIMARY KEY(PS_PARTKEY, PS_SUPPKEY) );

CREATE TABLE TPCH.CUSTOMER ( C_CUSTKEY     INTEGER PRIMARY KEY,
                             C_NAME        VARCHAR(25) NOT NULL,
                             C_ADDRESS     VARCHAR(40) NOT NULL,
                             C_NATIONKEY   INTEGER NOT NULL,
                             C_PHONE       VARCHAR(15) NOT NULL,
                             C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                             C_MKTSEGMENT  VARCHAR(10) NOT NULL,
                             C_COMMENT     VARCHAR(117) NOT NULL);


CREATE TABLE TPCH.ORDERS  ( O_ORDERKEY       INTEGER PRIMARY KEY,
                           O_CUSTKEY        INTEGER NOT NULL,
                           O_ORDERSTATUS    VARCHAR(1) NOT NULL,
                           O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  VARCHAR(15) NOT NULL,
                           O_CLERK          VARCHAR(15) NOT NULL,
                           O_SHIPPRIORITY   INTEGER NOT NULL,
                           O_COMMENT        VARCHAR(79) NOT NULL);


CREATE TABLE TPCH.LINEITEM (
L_ORDERKEY    INTEGER,
L_PARTKEY     INTEGER NOT NULL,
L_SUPPKEY     INTEGER NOT NULL,
L_LINENUMBER  INTEGER,
L_QUANTITY    DECIMAL(15,2) NOT NULL,
L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
L_DISCOUNT    DECIMAL(15,2) NOT NULL,
L_TAX         DECIMAL(15,2) NOT NULL,
L_RETURNFLAG  VARCHAR(1) NOT NULL,
L_LINESTATUS  VARCHAR(1) NOT NULL,
L_SHIPDATE    DATE NOT NULL,
L_COMMITDATE  DATE NOT NULL,
L_RECEIPTDATE DATE NOT NULL,
L_SHIPINSTRUCT VARCHAR(25) NOT NULL, 
L_SHIPMODE    VARCHAR(10) NOT NULL,
L_COMMENT      VARCHAR(44) NOT NULL,
PRIMARY KEY(L_ORDERKEY, L_LINENUMBER)
);
