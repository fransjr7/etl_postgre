with 
purchased_order as (
	select 
		to_date(a.order_purchase_timestamp,'YYYY-MM-DD') as purchased_date,
		b.product_id, 
		a.order_id, 
		b.price, 
		b.freight_value
	from orders_dataset a
	left join order_items_dataset b 
		using(order_id)
)

select 
	a.purchased_date,
	a.product_id,
	AVG(a.price) as avg_price,
	count(distinct review_id) as count_review,
	ROUND(coalesce(AVG(b.review_score),0),2) as avg_review_score,
	sum(
	case 
		when b.review_comment_message != '.' 
		then 1 else 0
	end) as proper_comment_count,
	count( distinct
	case 
		when b.review_comment_message != '.' and review_comment_title != '.'
		then b.review_id else null
	end) as complete_review_count
from purchased_order a
left join order_reviews_dataset b
	on a.order_id = b.order_id
group by 1,2
order by 1,2
