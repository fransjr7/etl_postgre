select 
	current_date as date_key,
	b.seller_id,
	count(distinct a.customer_id) as count_distinct_customer,
	count(distinct a.order_id) as count_distinct_order,
	max(b.price) as highest_order_item_price,
	min(b.price) as lowest_order_item_price,
	array_agg(distinct c.payment_type) as payment_type_ever_used,
	array_agg(distinct c.payment_installments) as payment_installment_ever_used
from orders_dataset a
left join order_items_dataset b 
	using(order_id)
left join order_payments_dataset c
	using(order_id)
group by 1,2