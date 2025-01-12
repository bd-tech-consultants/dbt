with customers as (
    select * from {{ ref('stg_customers')}}
),
orders as(
    select * from {{ ref('stg_orders')}}
),
customers_orders AS (
    SELECT 
        customer_id,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS most_recent_order_date,
        COUNT(order_id) AS number_of_orders
    FROM orders
    GROUP BY 1
),
final AS (
    SELECT
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customers_orders.first_order_date,
        customers_orders.most_recent_order_date,
        COALESCE(customers_orders.number_of_orders, 0) AS number_of_orders
    FROM customers
    LEFT JOIN customers_orders USING (customer_id)
)

SELECT * FROM final