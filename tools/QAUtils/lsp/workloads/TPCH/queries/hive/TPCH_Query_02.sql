drop view q2_min_ps_supplycost
create view q2_min_ps_supplycost as
select
	p_partkey as min_p_partkey,
	min(ps_supplycost) as min_ps_supplycost
from
	part_TABLESUFFIX,
	partsupp_TABLESUFFIX,
	supplier_TABLESUFFIX,
	nation_TABLESUFFIX,
	region_TABLESUFFIX
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'MIDDLE EAST'
group by
	p_partkey;

select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	part_TABLESUFFIX,
	supplier_TABLESUFFIX,
	partsupp_TABLESUFFIX,
	nation_TABLESUFFIX,
	region_TABLESUFFIX,
	q2_min_ps_supplycost
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and p_size = 50 
	and p_type like '%COPPER'
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'MIDDLE EAST'
	and ps_supplycost = min_ps_supplycost
	and p_partkey = min_p_partkey
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
limit 100;
