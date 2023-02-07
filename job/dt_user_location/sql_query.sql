with 
user_seller as (
	select 
		coalesce(seller_id,customer_id) as user_id,
		case when 
			seller_id is not null then true else false
		end as is_seller,
		case when 
			customer_id is not null then true else false
		end as is_customer,
		coalesce(a.seller_zip_code_prefix,b.customer_zip_code_prefix) as zip_code_prefix,
		coalesce(a.seller_city,b.customer_city) as city,
		coalesce(a.seller_state,b.customer_state) as state
	from sellers_dataset a
	full join customers_dataset b 
		on a.seller_id = b.customer_id
)

select 
	user_id,
	is_seller,
	is_customer,
	a.zip_code_prefix,
	a.city,
	a.state,
	b.geolocation_lat as lat,
	b.geolocation_lng as lng
from user_seller a
left join geolocation_dataset b
	on a.zip_code_prefix = b.geolocation_zip_code_prefix
	and a.city = b.geolocation_city
	and a.state = b.geolocation_state