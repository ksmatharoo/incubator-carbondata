#LINEITEM
load data inpath "$warehouse/tpchhive.db/lineitem/lineitem.tbl" into table lineitem options('DELIMITER'='|','FILEHEADER'='L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT', 'SORT_SCOPE'='GLOBAL_SORT')
#SUPPLIER
load data inpath "$warehouse/tpchhive.db/supplier/supplier.tbl" into table SUPPLIER options('DELIMITER'='|','FILEHEADER'='S_SUPPKEY,S_NAME,S_ADDRESS,S_NATIONKEY,S_PHONE,S_ACCTBAL,S_COMMENT', 'SORT_SCOPE'='GLOBAL_SORT')
#PARTSUPP
load data inpath "$warehouse/tpchhive.db/partsupp/partsupp.tbl" into table PARTSUPP options('DELIMITER'='|','FILEHEADER'='PS_PARTKEY,PS_SUPPKEY,PS_AVAILQTY,PS_SUPPLYCOST,PS_COMMENT', 'SORT_SCOPE'='GLOBAL_SORT')
#CUSTOMER
load data inpath "$warehouse/tpchhive.db/customer/customer.tbl" into  table CUSTOMER options('DELIMITER'='|','FILEHEADER'='C_CUSTKEY,C_NAME,C_ADDRESS,C_NATIONKEY,C_PHONE,C_ACCTBAL,C_MKTSEGMENT,C_COMMENT', 'SORT_SCOPE'='GLOBAL_SORT')
#NATION
load data inpath "$warehouse/tpchhive.db/nation/nation.tbl" into table NATION options('DELIMITER'='|','FILEHEADER'='N_NATIONKEY,N_NAME,N_REGIONKEY,N_COMMENT', 'SORT_SCOPE'='GLOBAL_SORT')
#REGION
load data inpath "$warehouse/tpchhive.db/region/region.tbl" into table REGION options('DELIMITER'='|','FILEHEADER'='R_REGIONKEY,R_NAME,R_COMMENT', 'SORT_SCOPE'='GLOBAL_SORT')
#PART
load data inpath "$warehouse/tpchhive.db/part/part.tbl" into table PART options('DELIMITER'='|','FILEHEADER'='P_PARTKEY,P_NAME,P_MFGR,P_BRAND,P_TYPE,P_SIZE,P_CONTAINER,P_RETAILPRICE,P_COMMENT', 'SORT_SCOPE'='GLOBAL_SORT')
#ORDERS
load data inpath "$warehouse/tpchhive.db/orders/orders.tbl" into table ORDERS options('DELIMITER'='|','FILEHEADER'='O_ORDERKEY,O_CUSTKEY,O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY,O_COMMENT', 'SORT_SCOPE'='GLOBAL_SORT')