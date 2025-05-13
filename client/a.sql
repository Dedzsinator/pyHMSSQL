CREATE TABLE products (id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(100) NOT NULL, price DECIMAL(10,2), stock INT DEFAULT 0)
INSERT INTO products (name, price, stock) VALUES ('Laptop', 999.99, 10)
INSERT INTO products (name, price, stock) VALUES ('Phone', 499.99, 20)
INSERT INTO products (name, price, stock) VALUES ('Tablet', 299.99, 15)