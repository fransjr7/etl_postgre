with
orders as (
	select order_id, customer_id
	from orders_dataset
),
order_items as (
	select order_id, seller_id, price, product_id
	from order_items_dataset
),
customer_seller_stats as(
	select 
		a.customer_id,
		b.seller_id,
		count(distinct a.order_id) as count_order,
		sum(b.price) as total_spent,
		count(distinct b.product_id) as count_product
	from orders a
	left join order_items b
		using(order_id)
	group by 1,2
),
customer_product_stats as(
	select 
		a.customer_id,
		b.product_id,
		count(distinct a.order_id) as count_order,
		sum(b.price) as total_spent,
		count(distinct b.seller_id) as count_seller,
		count(distinct c.product_category_name) as count_category,
		array_agg(distinct c.product_category_name) as category_ordered
	from orders a
	left join order_items b
		using(order_id)
	left join products_dataset c
		using(product_id)
	group by 1,2
),
top_seller as (
	select 
		customer_id,
		seller_id,
		count_product,
		row_number() over(partition by customer_id, seller_id order by count_order desc) as order_rn,
		row_number() over(partition by customer_id, seller_id order by total_spent desc) as spent_rn
	from customer_seller_stats
),
most_order_seller as (
	select 
		customer_id,
		seller_id,
		count_product
	from top_seller
	where order_rn = 1
),
most_spent_seller as (
	select 
		customer_id,
		seller_id,
		count_product
	from top_seller
	where spent_rn = 1
),
top_product as (
	select 
		customer_id,
		product_id,
		count_seller,
		count_category,
		category_ordered,
		row_number() over(partition by customer_id, product_id order by count_order desc) as order_rn,
		row_number() over(partition by customer_id, product_id order by total_spent desc) as spent_rn
	from customer_product_stats
),
most_order_product as (
	select 
		customer_id,
		product_id,
		count_seller,
		count_category,
		category_ordered
	from top_product
	where order_rn = 1
),
most_spent_product as (
	select 
		customer_id,
		product_id,
		count_seller,
		count_category,
		category_ordered
	from top_product
	where spent_rn = 1
)

select 
	current_date as date_key,
	a.customer_id,
	b.seller_id as most_order_seller_id,
	b.count_product as most_order_seller_product_count,
	c.seller_id as most_spent_seller_id,
	c.count_product as most_spent_seller_product_count,
	d.product_id as most_order_product_id,
	d.count_seller as most_order_product_seller_count,
	d.count_category as most_order_product_category_count,
	d.category_ordered as most_order_productcategory,
	d.product_id as most_sepnt_product_id,
	d.count_seller as most_sepnt_product_seller_count,
	d.count_category as most_sepnt_product_category_count,
	d.category_ordered as most_sepnt_product_category
from orders a
left join most_order_seller b 
	using(customer_id)
left join most_spent_seller c
	using(customer_id)
left join most_order_product d 
	using(customer_id)
left join most_spent_product e
	using(customer_id)
