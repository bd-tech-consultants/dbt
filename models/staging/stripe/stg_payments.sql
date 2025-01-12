with payments as (
    SELECT
        id as payment_id,
        order_id,
        paymentmethod,
        status,
        amount,
        created
    FROM {{ source('stripe', 'payment') }}
)

SELECT * FROM payments
