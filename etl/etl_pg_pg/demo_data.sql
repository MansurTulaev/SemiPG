CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    full_name VARCHAR(100),
    email VARCHAR(100),
    status VARCHAR(20) DEFAULT 'active'
);

INSERT INTO customers (full_name, email) VALUES 
    ('Иван Петров', 'ivan@test.com'),
    ('Мария Сидорова', 'maria@test.com'),
    ('Алексей Козлов', 'alex@test.com');

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    amount DECIMAL(10,2),
    status VARCHAR(20)
);

INSERT INTO orders (customer_id, amount, status) VALUES 
    (1, 100.50, 'completed'),
    (1, 75.25, 'pending'),
    (2, 200.00, 'completed');