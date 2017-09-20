drop view q22_customer_tmp_cached;
drop view q22_customer_tmp1_cached;
drop view q22_orders_tmp_cached;

create view if not exists q22_customer_tmp_cached as
select
	c_acctbal,
	c_custkey,
	substr(c_phone, 1, 2) as cntrycode
from
	customer_TABLESUFFIX
where
	substr(c_phone, 1, 2) = '15' or
	substr(c_phone, 1, 2) = '29' or
	substr(c_phone, 1, 2) = '27' or
	substr(c_phone, 1, 2) = '17' or
	substr(c_phone, 1, 2) = '31' or
	substr(c_phone, 1, 2) = '22' or
	substr(c_phone, 1, 2) = '19';
 
create view if not exists q22_customer_tmp1_cached as
select
	avg(c_acctbal) as avg_acctbal
from
	q22_customer_tmp_cached
where
	c_acctbal > 0.00;

create view if not exists q22_orders_tmp_cached as
select
	o_custkey
from
	orders_TABLESUFFIX
group by
	o_custkey;

select
	cntrycode,
	count(1) as numcust,
	sum(c_acctbal) as totacctbal
from (
	select
		cntrycode,
		c_acctbal,
		avg_acctbal
	from
		q22_customer_tmp1_cached ct1 join (
			select
				cntrycode,
				c_acctbal
			from
				q22_orders_tmp_cached ot
				right outer join q22_customer_tmp_cached ct
				on ct.c_custkey = ot.o_custkey
			where
				o_custkey is null
		) ct2
) a
where
	c_acctbal > avg_acctbal
group by
	cntrycode
order by
	cntrycode;
