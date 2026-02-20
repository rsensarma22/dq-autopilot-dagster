CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.customers;
CREATE TABLE raw.customers (
  customer_id INT,
  email TEXT,
  country TEXT
);

INSERT INTO raw.customers VALUES
(1, 'a@test.com', 'US'),
(2, NULL, 'US'),
(3, 'c@test.com', NULL),
(3, 'c@test.com', 'IN');

DROP TABLE IF EXISTS raw.orders;
CREATE TABLE raw.orders (
  order_id INT,
  customer_id INT,
  amount NUMERIC(10,2),
  created_at TIMESTAMP
);

INSERT INTO raw.orders VALUES
(10, 1, 120.00, NOW()),
(11, 2, NULL, NOW()),
(12, 999, 40.00, NOW());