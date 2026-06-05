CREATE TABLE IF NOT EXISTS sales_orders (
    order_id        SERIAL PRIMARY KEY,
    customer_id     INT NOT NULL,
    customer_name   VARCHAR(100),
    product_sku     VARCHAR(20),
    product_name    VARCHAR(100),
    quantity        INT,
    unit_price      NUMERIC(10,2),
    order_date      DATE,
    region          VARCHAR(50),
    status          VARCHAR(20),
    created_at      TIMESTAMP DEFAULT NOW()
);

INSERT INTO sales_orders
  (customer_id, customer_name, product_sku, product_name,
   quantity, unit_price, order_date, region, status)
SELECT
    (random() * 200 + 1)::int,
    (ARRAY['Arjun Mehta','Priya Nair','Vikram Singh','Divya Rao','Kiran Iyer',
           'Sanjay Rajan','Meena Pillai','Rohit Verma'])[ceil(random()*8)::int],
    'SKU-' || lpad((random()*100)::int::text, 4, '0'),
    (ARRAY['Conveyor Belt','Bucket Elevator','Screw Conveyor',
           'Belt Feeder','Vibrating Screen','Rotary Valve',
           'Drag Chain Conveyor'])[ceil(random()*7)::int],
    (random() * 10 + 1)::int,
    (random() * 50000 + 5000)::numeric(10,2),
    current_date - (random() * 365)::int,
    (ARRAY['Tamil Nadu','Karnataka','Maharashtra','Gujarat',
           'Telangana','Rajasthan'])[ceil(random()*6)::int],
    (ARRAY['pending','confirmed','shipped','delivered',
           'cancelled'])[ceil(random()*5)::int]
FROM generate_series(1, 500);