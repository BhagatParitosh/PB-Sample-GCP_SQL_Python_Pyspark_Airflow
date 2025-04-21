SELECT
c.customer_id, 
c.customer_name, 
o.order_id, 
o.order_date, 
o.total_amount
FROM `sample_customer_table` c 
JOIN
`sample_orders_table` o
ON
c.customer_id = o.customer_id;


SELECT
c.customer_id, 
c.customer_name,
ARRAY_AGG(STRUCT(o.order_id, o.order_date, o.total_amount) ORDER BY o.order_date DESC) AS order_details
FROM `sample_customer_table` c 
LEFT JOIN
`sample_orders_table` o
ON
c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name;













