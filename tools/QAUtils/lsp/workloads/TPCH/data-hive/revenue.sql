use ${DB};

DROP VIEW IF EXISTS revenue;

CREATE VIEW revenue_${FILE} (supplier_no, total_revenue) AS
    SELECT l_suppkey, sum(l_extendedprice * (1 - l_discount))
    FROM lineitem_${FILE}
    WHERE l_shipdate >= date '1997-04-01' AND l_shipdate < date '1997-04-01' + interval '3' month
    GROUP BY l_suppkey;
