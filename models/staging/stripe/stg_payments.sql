with payments as (
    SELECT
        id as payment_id,
        orderid,
        paymentmethod,
        status,
        amount,
        created
    FROM {{ source('stripe', 'payment') }}
)

SELECT * FROM payments
