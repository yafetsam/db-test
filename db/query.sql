-- name: InsertOrder :one
INSERT INTO orders(product_name, quantity, order_date)
VALUES ($1, $2, $3)
RETURNING id, product_name, quantity, order_date;

-- name: ListOrder :many
SELECT id, product_name, quantity, order_date
FROM orders
ORDER BY Id;
