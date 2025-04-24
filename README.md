# pyHMSSQL

![Database](https://img.shields.io/badge/Database-Engine-blue)
![Python](https://img.shields.io/badge/Python-3.7+-yellow)
![Status](https://img.shields.io/badge/Status-Development-green)
[![GitHub release (latest by date)](https://img.shields.io/github/v/release/dedzsinator/pyhmssql?label=Release&cache=0)](https://github.com/dedzsinator/pyhmssql/releases)
[![GitHub issues](https://img.shields.io/github/issues/dedzsinator/pyhmssql)](https://github.com/dedzsinator/pyhmssql/issues)
[![Last Commit](https://img.shields.io/github/last-commit/dedzsinator/pyhmssql/main)](https://github.com/dedzsinator/pyhmssql/commits/main)
[![Lines of Code](https://img.shields.io/github/languages/code-size/dedzsinator/pyhmssql)](https://github.com/dedzsinator/pyhmssql)
[![License](https://img.shields.io/github/license/dedzsinator/pyhmssql)](https://github.com/dedzsinator/pyhmssql/blob/main/LICENSE)
[![Tests](https://img.shields.io/github/actions/workflow/status/dedzsinator/pyhmssql/tests.yml?branch=main&label=Tests)](https://github.com/dedzsinator/pyhmssql/actions/workflows/tests.yml)
[![Lines of Code](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/Dedzsinator/pyHMSSQL/loc-badge/loc-badge.json)](https://github.com/Dedzsinator/pyHMSSQL)

A lightweight, powerful database management system built in Python. pyHMSSQL implements a client-server architecture with B+ tree indexing for efficient data retrieval operations.

## 🚀 Features

- [x] **Client-Server Architecture**
  - [x] Socket-based communication
  - [x] JSON message protocol
  - [x] Error handling and connection management
  - [x] Multi-request handling

- [x] **Multiple Client Interfaces**
  - [x] Command Line Interface (CLI)
  - [x] Graphical User Interface (GUI)
  - [x] Batch processing via script files
  - [ ] REST API (planned)

- [ ] **Database Operations**
  - [x] Create/Drop Databases
  - [x] Create/Drop Tables
  - [x] SQL Query Support (SELECT, INSERT, DELETE, UPDATE)
  - [ ] Views (CREATE VIEW, DROP VIEW)
  - [ ] Temporary tables
  - [ ] Basic joins and subqueries
  - [ ] Transaction support (begin, commit, rollback)

- [x] **Indexing**
  - [x] Custom B+ Tree Implementation
  - [x] Serialization for persistence
  - [x] Index-based lookups
  - [x] Range queries
  - [] Index-based optimizations

- [ ] **Query Optimization**
  - [ ] Index-based query planning
  - [ ] Join optimization (hash join, index join, sort-merge join)
  - [ ] Filter pushdown
  - [ ] Plan rewriting
  - [ ] Join reordering

- [x] **Storage**
  - [x] Binary file storage
  - [x] Schema management
  - [x] Index management

- [ ] **Aggregation Functions**
  - [x] AVG, MIN, MAX, SUM, COUNT
  - [ ] TOP N queries
  - [ ] DISTINCT operations

- [x] **Security Features**
  - [x] User authentication
  - [x] Role-based access control
  - [x] Session management
  - [ ] Encryption (planned)

- [x] **Set Operations**
  - [x] UNION, INTERSECT, EXCEPT
  - [x] Logical operations (AND, OR, NOT)

- [ ] **User Preferences**
  - [ ] Configurable result limits
  - [ ] Pretty printing options
  - [ ] Per-user preference storage

- [ ] **Stored Procedures and Functions**
  - [ ] Create/Drop Stored Procedures
  - [ ] Create/Drop Functions
  - [ ] Procedure execution
  - [ ] Function calls in queries

- [ ] **Triggers**
  - [ ] Create/Drop Triggers
  - [ ] Event-based execution (INSERT, UPDATE, DELETE)
  - [ ] Table-specific triggers

- [ ] **Advanced Query Features**
  - [ ] Views for logical data abstraction
  - [ ] Temporary tables for intermediate results
  - [ ] Complex subquery support
  - [ ] Multiple aggregation functions

## 📋 Requirements

- Python 3.7+
- sqlparse
- graphviz
- networkx
- matplotlib

See requirements.txt for the full and updated list of dependencies.

## 🔧 Installation

- Clone the repository:

```bash
git clone https://github.com/yourusername/pyHMSSQL.git
cd pyHMSSQL
```

- Install dependencies

```bash
pip install -r requirements.txt
```

## 🚀 Quick Start

Start the server:

```bash
cd server
python server.py
```

Start the CLI client:

```bash
cd client
python cli_client.py
```

Start the CLI client in discover mode:

```bash
python client/cli_client.py discover
```

Or in regular mode:

```bash
python client/cli_client.py localhost 9999
```

Start the server (NEW):

```bash
python server.py --name "My Custom HMSSQL Server"
```

Start the GUI client:

```bash
cd client
python gui_client.py
```

Start the java client (experimental):

```bash
cd e:\Programming\pyHMSSQL\client\java_client
mvn clean compile
mvn javafx:run
```

Then run it with:

```bash
java --module-path "path\to\javafx-sdk\lib" --add-modules javafx.controls,javafx.fxml,javafx.web,javafx.swing,javafx.graphics -jar target\java-client-1.0-SNAPSHOT.jar
```

Creating executable JAR:

```bash
cd e:\Programming\pyHMSSQL\client\java_client
mvn clean package
```

## 📝 Example Commands

```sql
-- Create a regular user
register user1 user

-- Create an admin user
register admin admin

-- Login as user
login user1
Password: *****

-- Login as admin
login admin
Password: *****

-- Check current status
status

-- Logout
logout

-- Show all indexes in the current database
query VISUALIZE BPTREE

-- Show all indexes on a specific table
query VISUALIZE BPTREE ON customers

-- Show a specific index
query VISUALIZE BPTREE idx_customer_email ON customers

-- Create a database (admin only)
query CREATE DATABASE test_db ✅

-- List all databases
query SHOW DATABASES ✅

-- Drop a database (admin only)
query DROP DATABASE test_db ✅

-- Create database for testing
query CREATE DATABASE test_db ✅

---

query DROP INDEX name ON customers ✅

-- Use the test database
query USE test_db ✅

query CREATE TABLE products (id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(100) NOT NULL, price DECIMAL(10,2), stock INT DEFAULT 0) ✅

-- ✅
query INSERT INTO products (name, price, stock) VALUES ('Laptop', 999.99, 10)
query INSERT INTO products (name, price, stock) VALUES ('Phone', 499.99, 20)
query INSERT INTO products (name, price, stock) VALUES ('Tablet', 299.99, 15)

-- Create a simple table
query CREATE TABLE customers (id INT PRIMARY KEY, name String, email VARCHAR(100), age INT) ✅

-- Create a table with constraints
query CREATE TABLE orders (id INT PRIMARY KEY,customer_id INT,order_date DATETIME,total DECIMAL(10,2),status VARCHAR(20),FOREIGN KEY (customer_id) REFERENCES customers(id)) ✅

-- Create an index on the customers table
CREATE INDEX idx_customer_email ON customers (email) ✅

-- Create an index on the orders table
CREATE INDEX idx_order_date ON orders (order_date) ✅

-- Show all tables
query SHOW TABLES ✅

-- Drop a table
query DROP TABLE orders ✅

-- Create a non-unique index
query CREATE INDEX idx_customer_name ON customers (name) ✅

-- Create a unique index
query CREATE UNIQUE INDEX idx_customer_email ON customers (email) ✅
query CREATE UNIQUE INDEX customer_id ON customers(id)

-- Show all indexes
query SHOW INDEXES ✅

-- Show indexes for a specific table
query SHOW INDEXES FOR customers ✅

-- Drop an index
query DROP INDEX name ON customers ✅

-- Insert single record
query INSERT INTO customers (id, name, email, age) VALUES (1, 'John Doe', 'john@example.com', 30)

-- Insert multiple records
query INSERT INTO customers (id, name, email, age) VALUES (2, 'Jane Smith', 'jane@example.com', 25)
query INSERT INTO customers (id, name, email, age) VALUES (3, 'Bob Johnson', 'bob@example.com', 45)
query INSERT INTO customers (id, name, email, age) VALUES (4, 'Alice Brown', 'alice@example.com', 35)
query INSERT INTO customers (id, name, email, age) VALUES (5, 'Test Test', 'test@example.com', 50)

-- Simple SELECT
query SELECT * FROM customers ✅

try this with appropiate table and dataset: WHERE age > 25 AND (department = 'Engineering' OR salary >= 80000) AND hire_date BETWEEN '2020-01-01' AND '2023-12-31'

-- SELECT with column projection
query SELECT id, age FROM customers WHERE age > 30 ✅

-- SELECT with WHERE condition
query SELECT * FROM customers WHERE age > 30 ✅

-- SELECT with sorting
query SELECT * FROM customers ORDER BY age DESC ✅

-- SELECT with LIMIT
query SELECT * FROM customers LIMIT 2 ✅

-- Update records
query UPDATE customers SET age = 31 WHERE id = 2 ✅

-- Delete a record
query DELETE FROM customers WHERE id = 4 ✅

-- Delete all records
query DELETE FROM customers ✅

-- Insert test data
query INSERT INTO customers (id, name, email, age) VALUES (5, 'Gipsz Jakab', 'gipsz@example.com', 50)
query INSERT INTO customers (id, name, email, age) VALUES (2, 'Jane Smith', 'jane@example.com', 25)
query INSERT INTO customers (id, name, email, age) VALUES (3, 'Bob Johnson', 'bob@example.com', 45)
query INSERT INTO customers (id, name, email, age) VALUES (4, 'Alice Brown', 'alice@example.com', 35)
query INSERT INTO customers (id, name, email, age) VALUES (5, 'Test Test', 'test@example.com', 50)

-- AVG function
query SELECT AVG(age) FROM customers ✅

-- COUNT function
query SELECT COUNT(*) FROM customers ✅

-- MAX function
query SELECT MAX(age) FROM customers ✅

-- MIN function
query SELECT MIN(age) FROM customers ✅

-- SUM function
query SELECT SUM(age) FROM customers ✅

-- Test WHERE with aggregate
query SELECT COUNT(*) FROM employees WHERE salary > 70000 ✅

-- Test GCD with WHERE
query SELECT GCD(salary) FROM employees WHERE dept_id = 1 ✅

-- Test RAND with WHERE
query SELECT RAND(2) FROM employees WHERE dept_id = 2 -- Check i dont thing this is correct

-- Get 5 random records
query SELECT RAND(5) FROM employees ✅ (returns JSON currently)

-- Get average of 10 random values between 1 and 100
query SELECT RAND(10,1,100) FROM dual

-- Calculate the GCD of all values in the salary column
query SELECT GCD(salary) FROM employees ✅

-- TOP N query
query SELECT TOP 2 * FROM customers ORDER BY age DESC ~

-- Test TOP with aggregate
SELECT TOP 5 AVG(salary) FROM employees WHERE department = 'Engineering'

-- Test LIMIT with aggregate
SELECT SUM(salary) FROM employees WHERE hire_date > '2020-01-01' LIMIT 10

---

-- Create tables for join testing
query CREATE TABLE departments (id INT PRIMARY KEY, name VARCHAR(100))
query CREATE TABLE employees (id INT PRIMARY KEY,name VARCHAR(100),dept_id INT,salary DECIMAL(10,2),FOREIGN KEY (dept_id) REFERENCES departments(id))

-- Insert test data
query INSERT INTO departments (id, name) VALUES (1, 'Engineering')
query INSERT INTO departments (id, name) VALUES (2, 'Marketing')
query INSERT INTO departments (id, name) VALUES (3, 'Finance')

query INSERT INTO employees (id, name, dept_id, salary) VALUES (1, 'Alice', 1, 75000)
query INSERT INTO employees (id, name, dept_id, salary) VALUES (2, 'Bob', 1, 70000)
query INSERT INTO employees (id, name, dept_id, salary) VALUES (3, 'Charlie', 2, 65000)
query INSERT INTO employees (id, name, dept_id, salary) VALUES (4, 'Dave', 2, 68000)
query INSERT INTO employees (id, name, dept_id, salary) VALUES (5, 'Eve', 3, 78000)

---
-- JOIN SPECIFIC TESTS

-- INNER JOIN (default)
query SELECT e.name, d.name FROM employees e INNER JOIN departments d ON e.dept_id = d.id ✅

-- LEFT OUTER JOIN
query SELECT e.name, d.name FROM employees e LEFT JOIN departments d ON e.dept_id = d.id ✅

-- RIGHT OUTER JOIN
query SELECT e.name, d.name FROM employees e RIGHT JOIN departments d ON e.dept_id = d.id ✅

-- FULL OUTER JOIN
query SELECT e.name, d.name FROM employees e FULL JOIN departments d ON e.dept_id = d.id ✅

-- CROSS JOIN (no condition needed)
query SELECT e.name, d.name FROM employees e CROSS JOIN departments d ✅

-- Specifying join algorithm with hints
query SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id WITH (JOIN_TYPE='HASH') ✅
query SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id WITH (JOIN_TYPE='NESTED_LOOP') ✅
query SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id WITH (JOIN_TYPE='MERGE') ✅
query SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id WITH (JOIN_TYPE='INDEX')

--- ✅

-- Hash Join (default)
query SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id

-- Sort-Merge Join
query SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id WITH (JOIN_TYPE='MERGE')

-- Index Join (needs an index)
query CREATE INDEX idx_dept_id ON employees(dept_id)
query SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id WITH (JOIN_TYPE='INDEX')

---

-- Subquery
query SELECT * FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE name = 'Engineering')

-- UNION
query SELECT id, name FROM employees UNION SELECT id, name FROM departments

-- INTERSECT
query SELECT dept_id FROM employees INTERSECT SELECT id FROM departments

-- EXCEPT
query SELECT id FROM departments EXCEPT SELECT dept_id FROM employees

-- AND operation
query SELECT * FROM employees WHERE salary > 70000 AND dept_id = 1

-- OR operation
query SELECT * FROM employees WHERE salary > 75000 OR dept_id = 3

-- NOT operation
query SELECT * FROM departments WHERE NOT id IN (SELECT dept_id FROM employees)

-- Start a transaction
query BEGIN TRANSACTION

-- Make changes within transaction
query INSERT INTO departments (id, name) VALUES (4, 'HR')
query UPDATE employees SET salary = 80000 WHERE id = 1

-- Commit transaction
query COMMIT TRANSACTION

-- Test rollback
query BEGIN TRANSACTION
query DELETE FROM departments WHERE id = 3
query ROLLBACK TRANSACTION

-- Create a view
query CREATE VIEW engineering_staff AS SELECT * FROM employees WHERE dept_id = 1

-- Query the view
query SELECT * FROM engineering_staff

-- Drop the view
query DROP VIEW engineering_staff

-- Get distinct departments
query SELECT DISTINCT dept_id FROM employees

-- Set display preferences
query SET PREFERENCE max_results 50
query SET PREFERENCE pretty_print true

-- Test preferences are applied
query SELECT * FROM employees

-- Show databases
query SHOW DATABASES

-- Show tables
query SHOW TABLES

-- Show indexes
query SHOW INDEXES

-- Show indexes for a specific table
query SHOW INDEXES FOR employees

-- Query with multiple joins, conditions and sorting
query SELECT e.name, d.name, e.salary
FROM employees e
JOIN departments d ON e.dept_id = d.id
WHERE e.salary > 65000
ORDER BY e.salary DESC
LIMIT 3

-- Query with aggregation and grouping
query SELECT dept_id, AVG(salary) as avg_salary, COUNT(*) as emp_count
FROM employees
GROUP BY dept_id

-- Nested subqueries
query SELECT * FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees)

---
-- index tests

-- Create a test database
CREATE DATABASE index_test
USE index_test

-- Create a test table with data
query CREATE TABLE products (id INT,name VARCHAR(100),price DECIMAL(10,2),category VARCHAR(50),in_stock INT);

-- Insert sample data
query INSERT INTO products VALUES (1, 'Laptop XPS', 1299.99, 'Electronics', 10);
query INSERT INTO products VALUES (2, 'Gaming Mouse', 59.99, 'Electronics', 25);
query INSERT INTO products VALUES (3, 'Coffee Maker', 89.99, 'Appliances', 15);
query INSERT INTO products VALUES (4, 'Desk Chair', 199.99, 'Furniture', 8);
query INSERT INTO products VALUES (5, 'Bluetooth Speaker', 79.99, 'Electronics', 30);

-- Time this query - should do a full table scan
query SELECT * FROM products WHERE category = 'Electronics';
2
-- Time this query - should do a full table scan
query SELECT * FROM products WHERE id = 3;

-- Create a regular index on category
query CREATE INDEX idx_category ON products (category);

-- Create a unique index on id
query CREATE INDEX idx_id ON products (id) UNIQUE;

-- Verify indexes were created
query SHOW INDEXES;

-- This should now use the category index
query SELECT * FROM products WHERE category = 'Electronics';

-- This should use the id index
query SELECT * FROM products WHERE id = 3;

-- Enable debug/execution plan mode if available
query SET PREFERENCE debug_mode true;

-- Run again with debug info
query SELECT * FROM products WHERE category = 'Electronics';
query SELECT * FROM products WHERE id = 3;

-- This should leverage the category index for both filtering and sorting
query SELECT * FROM products WHERE category = 'Electronics' ORDER BY category;

-- This should use the id index
query SELECT * FROM products WHERE id > 2 ORDER BY id;

-- This should NOT use any index (no index on price)
query SELECT * FROM products WHERE price > 100;

-- Create a related table
query CREATE TABLE suppliers (
  id INT,
  name VARCHAR(100),
  product_id INT
);

-- Insert some data
query INSERT INTO suppliers VALUES (1, 'Dell', 1);
query INSERT INTO suppliers VALUES (2, 'Logitech', 2);
query INSERT INTO suppliers VALUES (3, 'Breville', 3);

-- Create index on suppliers
query CREATE INDEX idx_product_id ON suppliers (product_id);

-- This join should use the id index on products and product_id index on suppliers
query SELECT p.name, s.name FROM products p JOIN suppliers s ON p.id = s.product_id;

-- Drop an index
query DROP INDEX category ON products

-- Verify it's gone
query SHOW INDEXES;

-- Run the query again - should now use full table scan
query SELECT * FROM products WHERE category = 'Electronics';

-- Visualize an index
query VISUALIZE BPTREE idx_id ON products;
```

## 📊 Architecture

pyHMSSQL follows a modular architecture with clear separation of concerns:

-Client Layer: Handles user interaction through CLI or GUI
-Server Layer: Processes requests and manages database operations
-Storage Layer: Persists data and metadata using MongoDB
-Index Layer: Optimizes data retrieval using B+ trees

For more details, see ARCHITECTURE.md.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 💡 Future Enhancements

1. Transaction Support:

  -Implementing ACID properties
  -Concurrency control mechanisms
  -Rollback and recovery
2. Advanced Query Processing:

  -More complex JOIN operations
  -Advanced aggregation (GROUP BY, HAVING)
  -Window functions
  -Common Table Expressions (CTEs)
3. Security Enhancements:

  -User authentication
  -Access control lists
  -Data encryption
4. Performance Optimizations:

  -Query caching
  -Cost-based query optimization
  -Connection pooling
5. Custom Storage Engine:

  -Page-based storage
  -Write-ahead logging
  -Buffer management
