#SUPPLIER
insert into table supplier select * from tpchhive.supplier sort by S_SUPPKEY
#ORDERS
insert into table ORDERS select      O_ORDERDATE,      O_ORDERKEY,      O_CUSTKEY,      O_ORDERSTATUS,      O_TOTALPRICE,      O_ORDERPRIORITY,      O_CLERK,      O_SHIPPRIORITY,      O_COMMENT from tpchhive.ORDERS sort by O_ORDERDATE,O_ORDERKEY,O_CUSTKEY,O_ORDERSTATUS
#LINEITEM
insert into table lineitem select L_SHIPDATE,L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT from tpchhive.lineitem sort BY l_shipdate,L_ORDERKEY,L_PARTKEY,L_SUPPKEY
#PARTSUPP
insert into table PARTSUPP select * from tpchhive.partsupp sort by PS_PARTKEY
#NATION
insert into table nation select N_NAME, N_NATIONKEY,N_REGIONKEY,N_COMMENT from tpchhive.nation sort by N_NAME
#REGION
insert into table REGION select   r_name, R_REGIONKEY,    R_COMMENT    from tpchhive.region sort by r_name
#PART
insert into TABLE PART    select    P_BRAND,    P_PARTKEY,    P_NAME,    P_MFGR,    P_TYPE,    P_SIZE,    P_CONTAINER,    P_RETAILPRICE,    P_COMMENT    from tpchhive.part sort by P_BRAND,P_PARTKEY,P_NAME 
#CUSTOMER
insert into table CUSTOMER select    C_MKTSEGMENT,    C_CUSTKEY,    C_NAME,    C_ADDRESS,    C_NATIONKEY,    C_PHONE,    C_ACCTBAL,    C_COMMENT    from tpchhive.customer sort by C_MKTSEGMENT