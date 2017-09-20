-- Number of orders by shipment status for Jan2010
SELECT to_char(order_datetime,'YYYY/MM') as ship_month
,      item_shipment_status_code
,      COUNT(DISTINCT order_id) AS num_orders
FROM   retail_demo.order_lineitems
WHERE  order_datetime BETWEEN timestamp '2010-01-01' AND date '2010-01-31'
GROUP BY to_char(order_datetime,'YYYY/MM')
,      item_shipment_status_code
ORDER BY to_char(order_datetime,'YYYY/MM') 
,      item_shipment_status_code
;
