SELECT
c.customer_id, 
c.customer_name, 
o.order_id, 
o.order_date, 
o.total_amount
FROM `gcp-actual-pb.test.customers` c 
JOIN
`gcp-actual-pb.test.orders` o
ON
c.customer_id = o.customer_id;


SELECT
c.customer_id, 
c.customer_name,
ARRAY_AGG(STRUCT(o.order_id, o.order_date, o.total_amount) ORDER BY o.order_date DESC) AS order_details
FROM `gcp-actual-pb.test.customers` c 
LEFT JOIN
`gcp-actual-pb.test.orders` o
ON
c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name;













