drop view q17_lineitem_tmp_cached;

create view q17_lineitem_tmp_cached as
select
	l_partkey as t_partkey,
	0.2 * avg(l_quantity) as t_avg_quantity
from
	lineitem_TABLESUFFIX
group by l_partkey;

select
	sum(l_extendedprice) / 7.0 as avg_yearly
from (
	select
		l_quantity,
		l_extendedprice,
		t_avg_quantity
	from
		q17_lineitem_tmp_cached join
		(select
			l_quantity,
			l_partkey,
			l_extendedprice
		from
			part_TABLESUFFIX,
			lineitem_TABLESUFFIX
		where
			p_partkey = l_partkey
			and p_brand = 'Brand#54'
			and p_container = 'JUMBO CASE'
		) l1 on l1.l_partkey = t_partkey
) a 
where l_quantity < t_avg_quantity;
