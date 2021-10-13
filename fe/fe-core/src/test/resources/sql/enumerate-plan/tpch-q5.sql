[sql]
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
where
        c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and l_suppkey = s_suppkey
  and c_nationkey = s_nationkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'AFRICA'
  and o_orderdate >= date '1995-01-01'
  and o_orderdate < date '1996-01-01'
group by
    n_name
order by
    revenue desc ;
192
[plan-1]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-2]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-3]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-4]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-5]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-6]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-7]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-8]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-9]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-10]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-11]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-12]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-13]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-14]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[1]
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-15]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[1]
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[20]
                                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE SHUFFLE[10]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-16]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[1]
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE SHUFFLE[10]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-17]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[1]
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-18]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[1]
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[20]
                                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE SHUFFLE[10]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-19]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[1]
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE SHUFFLE[10]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-20]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-21]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[20]
                                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE SHUFFLE[10]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-22]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE SHUFFLE[10]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-23]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-24]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[20]
                                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE SHUFFLE[10]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-25]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE SHUFFLE[10]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-26]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[20]
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE SHUFFLE[10]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE BROADCAST
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-27]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE SHUFFLE[10]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE BROADCAST
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-28]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE BROADCAST
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-29]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-30]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-31]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-32]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-33]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-34]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-35]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-36]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-37]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-38]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-39]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-40]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-41]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-42]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-43]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-44]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-45]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-46]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-47]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-48]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-49]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-50]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-51]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-52]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-53]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-54]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-55]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-56]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-57]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-58]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-59]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-60]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-61]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-62]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-63]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-64]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-65]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-66]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-67]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-68]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-69]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-70]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-71]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-72]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-73]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-74]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-75]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-76]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-77]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[1]
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-78]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[1]
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[20]
                                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE SHUFFLE[10]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-79]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[1]
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE SHUFFLE[10]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-80]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[1]
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-81]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[1]
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[20]
                                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE SHUFFLE[10]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-82]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[1]
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE SHUFFLE[10]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-83]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-84]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[20]
                                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE SHUFFLE[10]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-85]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE SHUFFLE[10]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-86]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-87]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[20]
                                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE SHUFFLE[10]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-88]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE SHUFFLE[10]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-89]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[20]
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE SHUFFLE[10]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE BROADCAST
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-90]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE SHUFFLE[10]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE BROADCAST
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-91]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE BROADCAST
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-92]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-93]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-94]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-95]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-96]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-97]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-98]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-99]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-100]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-101]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-102]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-103]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-104]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-105]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-106]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-107]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-108]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-109]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-110]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-111]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-112]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-113]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-114]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-115]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-116]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-117]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-118]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-119]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-120]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-121]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-122]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-123]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-124]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-125]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY AND 4: C_NATIONKEY = 40: S_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[11]
                                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                        EXCHANGE SHUFFLE[1]
                                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-126]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-127]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-128]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-129]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-130]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-131]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-132]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-133]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-134]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-135]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-136]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-137]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-138]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-139]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-140]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-141]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[1]
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[11]
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-142]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[1]
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[11]
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[20]
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE SHUFFLE[10]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-143]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[1]
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[11]
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE SHUFFLE[10]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-144]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[1]
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[11]
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-145]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[1]
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[11]
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[20]
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE SHUFFLE[10]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-146]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[1]
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[11]
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE SHUFFLE[10]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-147]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[11]
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-148]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[11]
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[20]
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE SHUFFLE[10]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-149]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[11]
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE SHUFFLE[10]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-150]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[11]
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-151]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[11]
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[20]
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE SHUFFLE[10]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-152]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[11]
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE SHUFFLE[10]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-153]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-154]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-155]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-156]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-157]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-158]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-159]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-160]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-161]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-162]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-163]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-164]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-165]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-166]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-167]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-168]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-169]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-170]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-171]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-172]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-173]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-174]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-175]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-176]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-177]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-178]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-179]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-180]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-181]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-182]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-183]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-184]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-185]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-186]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-187]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-188]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-189]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[40]
                                    SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[45]
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-190]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[40]
                                    SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[45]
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-191]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[40]
                                    SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[45]
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]
[plan-192]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 40: S_NATIONKEY AND 22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[40]
                                    SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[45]
                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
[end]