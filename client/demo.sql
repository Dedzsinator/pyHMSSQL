CREATE DATABASE demo;

USE demo;

CREATE TABLE products(
    id INT IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2),
    stock INT DEFAULT 0
);

INSERT INTO products (name, price, stock) VALUES 
('Laptop', 999.99, 10),
('Phone', 499.99, 20),
('Tablet', 299.99, 15),
('Monitor', 249.99, 25),
('Keyboard', 59.99, 50),
('Keyboard', 678.99, 100),
('Keyboard', 123.99, 3),
('Keyboard', 333.99, 45),
('Keyboard', 222.99, 9);

SELECT * FROM products;

CREATE INDEX idx_product_name ON products(name);

CREATE TABLE order_details (order_id INT IDENTITY(1,1) PRIMARY KEY, product_id INT, quantity INT, status VARCHAR(20), FOREIGN KEY (product_id) REFERENCES products(id));

INSERT INTO order_details (product_id, quantity, status) VALUES 
(1, 2, 'Shipped'),
(2, 1, 'Pending'),
(3, 5, 'Delivered'),
(4, 3, 'Cancelled'),
(5, 4, 'Shipped'),
(6, 2, 'Pending'),
(7, 1, 'Delivered'),
(8, 3, 'Cancelled'),
(9, 5, 'Shipped');

SELECT * FROM order_details;

SELECT products.id, products.name, order_details.quantity, order_details.status FROM products INNER JOIN order_details ON products.id = order_details.product_id WHERE products.name = 'Keyboard' AND order_details.quantity > 2 GROUP BY;

SELECT * FROM products WHERE price > 300;

SELECT * FROM products ORDER BY price DESC;

SELECT * FROM products ORDER BY price DESC LIMIT 3;

SELECT SUM(price) FROM products;

SELECT name, AVG(price), SUM(stock)
FROM products
GROUP BY name;

DELETE FROM products WHERE price < 100;

UPDATE products SET stock = 99 WHERE stock <= 15;

CREATE TABLE departments (id INT PRIMARY KEY, name VARCHAR(100));
CREATE TABLE employees (id INT PRIMARY KEY,name VARCHAR(100),dept_id INT,salary DECIMAL(10,2),FOREIGN KEY (dept_id) REFERENCES departments(id));

INSERT INTO departments (id, name) VALUES (1, 'Engineering'), (2, 'Marketing'), (3, 'Finance');

INSERT INTO employees (id, name, dept_id, salary) VALUES (1, 'Alice', 1, 75000), (2, 'Bob', 2, 60000), (3, 'Charlie', 3, 50000),
(4, 'Eve', 1, 80000), (5, 'Frank', 2, 55000), (6, 'Grace', 3, 70000);

DELETE FROM departments WHERE id = 1;

UPDATE departments SET id = 10 WHERE id = 1;

INSERT INTO employees (id, name, dept_id) VALUES (7, 'Dave', 4);

DROP TABLE deparments;

DROP TABLE employees;

DROP TABLE order_details;

DROP INDEX idx_product_name ON products;

DROP TABLE products;

DROP DATABASE demo;