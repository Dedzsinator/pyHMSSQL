# pyHMSSQL

![Database](https://img.shields.io/badge/Database-Engine-blue)
![Python](https://img.shields.io/badge/Python-3.7+-yellow)
![Status](https://img.shields.io/badge/Status-Development-green)
[![Last Commit](https://img.shields.io/github/last-commit/dedzsinator/pyhmssql/main)](https://github.com/dedzsinator/pyhmssql/commits/main)
[![Lines of Code](https://img.shields.io/github/languages/code-size/dedzsinator/pyhmssql)](https://github.com/dedzsinator/pyhmssql)
[![Lines of Code](https://sloc.xyz/github/dedzsinator/pyhmssql?category=code)](https://github.com/dedzsinator/pyhmssql)

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

- [x] **Database Operations**
  - [x] Create/Drop Databases
  - [x] Create/Drop Tables
  - [x] SQL Query Support (SELECT, INSERT, DELETE, UPDATE)
  - [x] Views (CREATE VIEW, DROP VIEW)
  - [x] Temporary tables
  - [x] Basic joins and subqueries
  - [x] Transaction support (begin, commit, rollback)

- [x] **Indexing**
  - [x] Custom B+ Tree Implementation
  - [x] Serialization for persistence
  - [x] Index-based lookups
  - [x] Range queries
  - [x] Index-based optimizations

- [x] **Query Optimization**
  - [x] Index-based query planning
  - [x] Join optimization (hash join, index join, sort-merge join)
  - [x] Filter pushdown
  - [x] Plan rewriting
  - [x] Join reordering

- [x] **Storage**
  - [x] MongoDB Integration
  - [x] Schema management
  - [x] Index management
  - [ ] Custom page-based storage engine (planned)

- [x] **Aggregation Functions**
  - [x] AVG, MIN, MAX, SUM, COUNT
  - [x] TOP N queries
  - [x] DISTINCT operations

- [x] **Security Features**
  - [x] User authentication
  - [x] Role-based access control
  - [x] Session management
  - [ ] Encryption (planned)

- [x] **Set Operations**
  - [x] UNION, INTERSECT, EXCEPT
  - [x] Logical operations (AND, OR, NOT)

- [x] **User Preferences**
  - [x] Configurable result limits
  - [x] Pretty printing options
  - [x] Per-user preference storage

- [x] **Stored Procedures and Functions**
  - [x] Create/Drop Stored Procedures
  - [x] Create/Drop Functions
  - [x] Procedure execution
  - [x] Function calls in queries

- [x] **Triggers**
  - [x] Create/Drop Triggers
  - [x] Event-based execution (INSERT, UPDATE, DELETE)
  - [x] Table-specific triggers

- [x] **Advanced Query Features**
  - [x] Views for logical data abstraction
  - [x] Temporary tables for intermediate results
  - [x] Complex subquery support
  - [x] Multiple aggregation functions

## 📋 Requirements

- Python 3.7+
- MongoDB
- pymongo

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

- On Windows:
  - Ensure MongoDB is running

- On Linux:
  - Start MongoDB (sudo systemctl start mongodb)

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

-- Use the test database
query USE test_db ✅

-- Create a simple table
query CREATE TABLE customers (id INT PRIMARY KEY, name String, email VARCHAR(100), age INT) ✅

-- Create a table with constraints
query CREATE TABLE orders (id INT PRIMARY KEY,customer_id INT,order_date DATETIME,total DECIMAL(10,2),status VARCHAR(20),FOREIGN KEY (customer_id) REFERENCES customers(id)) ✅

-- Create an index on the customers table
CREATE INDEX idx_customer_email ON customers (email)

-- Create an index on the orders table  
CREATE INDEX idx_order_date ON orders (order_date)

-- Show all tables
query SHOW TABLES ✅

-- Drop a table
query DROP TABLE orders ✅

-- Create a non-unique index
query CREATE INDEX idx_customer_name ON customers (name) ✅

-- Create a unique index
query CREATE UNIQUE INDEX idx_customer_email ON customers (email) ✅

-- Show all indexes
query SHOW INDEXES ✅

-- Show indexes for a specific table
query SHOW INDEXES FOR customers ✅

-- Drop an index
query DROP INDEX idx_customer_name ON customers ✅

-- Insert single record
query INSERT INTO customers (id, name, email, age) VALUES (1, 'John Doe', 'john@example.com', 30)

-- Insert multiple records
query INSERT INTO customers (id, name, email, age) VALUES (2, 'Jane Smith', 'jane@example.com', 25)
query INSERT INTO customers (id, name, email, age) VALUES (3, 'Bob Johnson', 'bob@example.com', 45)
query INSERT INTO customers (id, name, email, age) VALUES (4, 'Alice Brown', 'alice@example.com', 35)

-- Simple SELECT
query SELECT * FROM customers ✅

try this with appropiate table and dataset: WHERE age > 25 AND (department = 'Engineering' OR salary >= 80000) AND hire_date BETWEEN '2020-01-01' AND '2023-12-31'

-- SELECT with column projection
query SELECT id, name FROM customers ✅

-- SELECT with WHERE condition
query SELECT * FROM customers WHERE age > 30 ✅

-- SELECT with sorting
query SELECT * FROM customers ORDER BY age DESC ✅

-- SELECT with LIMIT
query SELECT * FROM customers LIMIT 2 ✅

-- Update records
query UPDATE customers SET age = 31 WHERE id = 2 ✅

-- Delete a record
query DELETE FROM customers WHERE id = 4

-- Delete all records
query DELETE FROM customers

-- Insert test data
query INSERT INTO customers (id, name, email, age) VALUES (1, 'John Doe', 'john@example.com', 30)
query INSERT INTO customers (id, name, email, age) VALUES (2, 'Jane Smith', 'jane@example.com', 25)
query INSERT INTO customers (id, name, email, age) VALUES (3, 'Bob Johnson', 'bob@example.com', 45)
query INSERT INTO customers (id, name, email, age) VALUES (4, 'Alice Brown', 'alice@example.com', 35)

-- AVG function
query SELECT AVG(age) FROM customers ~

-- COUNT function
query SELECT COUNT(*) FROM customers ~

-- MAX function
query SELECT MAX(age) FROM customers ~

-- MIN function
query SELECT MIN(age) FROM customers ~

-- SUM function
query SELECT SUM(age) FROM customers ~

-- TOP N query
query SELECT TOP 2 * FROM customers ORDER BY age DESC ~

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

-- Join Operations
query SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id !!!!

-- Hash Join
query SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id

-- Sort-Merge Join
query SELECT e.name, d.name FROM employees e JOIN departments d ON e.dept_id = d.id

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
