{% set import_customers = select_table(source('jaffle_shop', 'customers'), ref('seed_input_customers')) %}
{% set import_orders = select_table(source('jaffle_shop', 'orders'), ref('seeds_input_orders')) %}
with
    customers as (select * from {{ import_customers }}),
    orders as (select * from {{ import_orders }}),
    customers_orders as (
        select
            user_id as customer_id,
            min(order_date) as first_order_date,
            max(order_date) as most_recent_order_date,
            count(id) as number_of_orders
        from orders
        group by 1
    ),
    final as (
        select
            customers.id as customer_id,
            customers.first_name,
            customers.last_name,
            customers_orders.first_order_date,
            customers_orders.most_recent_order_date,
            coalesce(customers_orders.number_of_orders, 0) as number_of_orders
        from customers
        left join customers_orders on customers.id = customers_orders.customer_id
    )

select *
from final
