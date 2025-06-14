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
[![Lines of Code](https://tokei.rs/b1/github/Dedzsinator/pyHMSSQL?category=code)](https://github.com/Dedzsinator/pyHMSSQL/)

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
  - [x] REST API (planned)

- [ ] **Database Operations**
  - [x] Create/Drop Databases
  - [x] Create/Drop Tables
  - [x] SQL Query Support (SELECT, INSERT, DELETE, UPDATE)
  - [ ] Views (CREATE VIEW, DROP VIEW)
  - [ ] Temporary tables
  - [x] Basic joins and subqueries
  - [x] Transaction support (begin, commit, rollback)
  - [ ] Batch insertion

- [x] **Indexing**
  - [x] Custom B+ Tree Implementation
  - [x] Serialization for persistence
  - [x] Index-based lookups
  - [x] Range queries
  - [x] Index-based optimizations

- [ ] **Query Optimization**
  - [x] Index-based query planning
  - [x] Join optimization (hash join, index join, sort-merge join)
  - [ ] Filter pushdown
  - [ ] Plan rewriting
  - [ ] Join reordering

- [x] **Storage**
  - [x] Binary file storage
  - [x] Schema management
  - [x] Index management

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
  - [x] Multiple aggregation functions
  - [x] Autocomplete

## Some Stats about execution speed and performance

| Query Type                | Execution Time (s)   | Notes                                      |
|---------------------------|----------------------|--------------------------------------------|
| Insertion of 10k elements | 1.8888               |                                            |
| Two tables joined together| 0.1070               | 10k x 10k elements + WHERE condition       |
| Two tables joined together| 0.2300               | 100k x 100k elements + not all columns     |

## 📱 REST API

pyHMSSQL now provides a REST API for applications to connect and interact with the database server. This allows you to integrate with the database using HTTP requests rather than direct socket connections.

### REST API Endpoints

| Endpoint                  | Method | Description                      |
|---------------------------|--------|----------------------------------|
| /api/login                | POST   | Log in to the database           |
| /api/register             | POST   | Register a new user              |
| /api/logout               | POST   | Log out from the database        |
| /api/query                | POST   | Execute a SQL query              |
| /api/databases            | GET    | Get list of databases            |
| /api/tables               | GET    | Get list of tables               |
| /api/indexes              | GET    | Get index information            |
| /api/table/<table_name>   | GET    | Get table schema information     |
| /api/status               | GET    | Get server status information    |
| /api/use_database         | POST   | Set the current database         |

Example usage:

```bash
from client.rest_client import HMSSQLRestClient

client = HMSSQLRestClient()
client.login('admin', 'admin')
result = client.execute_query('SELECT * FROM users')
print(result)
```

## 📋 Requirements

- Python 3.7+
- sqlparse
- graphviz
- networkx
- matplotlib
- flask
- flask-cors

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

Or run in rest api mode:

```bash
python server.py --use-api --api-port 5000
```

Start the GUI client:

```bash
cd client
python gui_client.py
```

Start the java client (experimental):

```bash
cd pyHMSSQL\client\java_client
mvn clean compile
mvn javafx:run
```

Then run it with:

```bash
java --module-path "path\to\javafx-sdk\lib" --add-modules javafx.controls,javafx.fxml,javafx.web,javafx.swing,javafx.graphics -jar target\java-client-1.0-SNAPSHOT.jar
```

## 🧪 Testing

To run the tests, use:

```bash
pytest tests/ --tb=no -q > results.log 2>&1 && echo "✅ Tests passed" || echo "❌ Tests failed"
```

Creating executable JAR:

```bash
cd e:\Programming\pyHMSSQL\client\java_client
mvn clean package
```

## 🧩 Server Configuration Options

| **Option**        | **Default**                  | **Description**                                                                      |
| ----------------- | ---------------------------- | ------------------------------------------------------------------------------------ |
| `--name`          | Auto-generated from hostname | Sets a custom name for the server instance. Useful for identifying specific servers. |
| `--use-api`       | `False`                      | Starts the REST API server instead of the socket server, enabling HTTP-based access. |
| `--api-host`      | `0.0.0.0`                    | Host address for REST API server. Use `127.0.0.1` for local-only access.             |
| `--api-port`      | `5000`                       | Port number for the REST API server to listen on.                                    |
| `--api-debug`     | `False`                      | Enables debug mode with extra logging and auto-reload for the REST API server.       |
| `--replica-of`    | `None`                       | Configures this server as a replica. Format: `host:port` (e.g., `localhost:9999`).   |
| `--sync-mode`     | `semi-sync`                  | Replication mode: `sync`, `semi-sync`, or `async`.                                   |
| `--sync-replicas` | `1`                          | Number of replicas that must acknowledge writes in `semi-sync` mode.                 |

## 🔁 Replication Modes Explained

| **Mode**    | **Description**                                                                                    | **Use Case**                                        |
| ----------- | -------------------------------------------------------------------------------------------------- | --------------------------------------------------- |
| `sync`      | Fully synchronous replication. All replicas must confirm write operations before primary responds. | Highest consistency where data safety is critical.  |
| `semi-sync` | Primary waits for a set number of replicas to acknowledge before responding.                       | Balanced tradeoff between performance and safety.   |
| `async`     | Primary doesn't wait for any acknowledgment from replicas.                                         | Max performance where some data loss is acceptable. |

### All of the start opions as commands

#### Start a primary server with a custom name

```bash
python server.py --name "Primary HMSSQL Server"
```

#### Start a replica server that replicates from a primary

```bash
python server.py --name "Replica Server" --replica-of localhost:9999
```

#### Start a server with REST API enabled

```bash
python server.py --use-api --api-port 8080
```

#### Start a primary with fully synchronous replication

```bash
python server.py --sync-mode sync
```

#### Start a primary that waits for at least 2 replicas to acknowledge writes

```bash
python server.py --sync-mode semi-sync --sync-replicas 2
```

## ⚙️ Default Server Configuration (No Flags)

### When started without any flags, the server will

- Run as a primary server
- Use semi-synchronous replication waiting for 1 replica
- Listen for socket connections on port 9999
- Enable automatic node discovery
- Use optimized B+ tree, buffer pool, and cost-based optimization

## 🔄 Horizontal Scaling and Clustering Setup

pyHMSSQL supports horizontal scaling through a sophisticated replication system that allows you to distribute your database workload across multiple server instances. The system supports both read replicas and primary-replica configurations with various consistency levels.

### 🚀 Quick Start: Setting Up a Cluster

#### 1. Start the Primary Server

```bash
# Start primary server (default configuration)
python server.py --name "Primary-DB"

# Or with specific replication settings
python server.py --name "Primary-DB" --sync-mode semi-sync --sync-replicas 2
```

#### 2. Start Replica Servers

```bash
# Start first replica
python server.py --name "Replica-1" --replica-of localhost:9999

# Start second replica (on different port if same machine)
python server.py --name "Replica-2" --replica-of localhost:9999 --port 9998

# Start third replica
python server.py --name "Replica-3" --replica-of localhost:9999 --port 9997
```

### 📊 Horizontal Scaling Architecture

The pyHMSSQL horizontal scaler uses an **EnhancedReplicationManager** that provides:

- **Automatic Node Discovery**: Replicas automatically register with the primary
- **Health Monitoring**: Continuous health checks and failure detection
- **Automatic Failover**: Promotes replicas to primary on failures
- **Load Balancing**: Distributes read queries across healthy replicas
- **Conflict Resolution**: Handles write conflicts in distributed scenarios

### 🔧 Scaling Configuration Options

| **Parameter**     | **Description**                                           | **Default**  | **Example**                    |
|-------------------|-----------------------------------------------------------|--------------|--------------------------------|
| `--replica-of`    | Connect as replica to specified primary                   | None         | `localhost:9999`               |
| `--sync-mode`     | Replication synchronization mode                          | `semi-sync`  | `sync`, `semi-sync`, `async`   |
| `--sync-replicas` | Number of replicas to wait for (semi-sync mode)          | `1`          | `2`                            |
| `--port`          | Server port (useful for multiple instances)              | `9999`       | `9998`                         |
| `--name`          | Server instance identifier                                | Auto         | `"Primary-Production"`         |

### 🎯 Scaling Scenarios and Examples

#### Scenario 1: High Availability Setup (2 Replicas)

```bash
# Terminal 1 - Primary server
python server.py --name "HA-Primary" --sync-mode semi-sync --sync-replicas 1

# Terminal 2 - First replica
python server.py --name "HA-Replica-1" --replica-of localhost:9999 --port 9998

# Terminal 3 - Second replica  
python server.py --name "HA-Replica-2" --replica-of localhost:9999 --port 9997
```

**Result**: Writes require acknowledgment from 1 replica, providing good balance of performance and data safety.

#### Scenario 2: Maximum Performance (Async Replication)

```bash
# Primary with async replication
python server.py --name "Async-Primary" --sync-mode async

# Multiple async replicas
python server.py --name "Async-Replica-1" --replica-of localhost:9999 --port 9998
python server.py --name "Async-Replica-2" --replica-of localhost:9999 --port 9997
python server.py --name "Async-Replica-3" --replica-of localhost:9999 --port 9996
```

**Result**: Maximum write performance, replicas catch up asynchronously.

#### Scenario 3: Maximum Consistency (Sync Replication)

```bash
# Primary requiring all replicas to acknowledge
python server.py --name "Sync-Primary" --sync-mode sync

# Replicas (all must be healthy for writes to succeed)
python server.py --name "Sync-Replica-1" --replica-of localhost:9999 --port 9998
python server.py --name "Sync-Replica-2" --replica-of localhost:9999 --port 9997
```

**Result**: Highest consistency, but slower writes and requires all replicas to be available.

#### Scenario 4: Load Balanced Read Scaling

```bash
# Primary with multiple read replicas
python server.py --name "Read-Primary" --sync-mode semi-sync --sync-replicas 1

# Multiple read replicas for scaling read operations
python server.py --name "Read-Replica-1" --replica-of localhost:9999 --port 9998
python server.py --name "Read-Replica-2" --replica-of localhost:9999 --port 9997
python server.py --name "Read-Replica-3" --replica-of localhost:9999 --port 9996
python server.py --name "Read-Replica-4" --replica-of localhost:9999 --port 9995
```

**Result**: Reads can be distributed across 5 servers (1 primary + 4 replicas).

### 🔍 How the Horizontal Scaler Works

#### 1. **EnhancedReplicationManager Components**

- **NodeRegistry**: Tracks all replicas and their health status
- **HealthMonitor**: Performs periodic health checks on all nodes
- **FailoverCoordinator**: Handles primary failures and replica promotion
- **ReplicationCoordinator**: Manages data synchronization between nodes

#### 2. **Replication Process Flow**

```
1. Client sends WRITE to Primary
2. Primary validates and applies changes
3. Primary forwards changes to replicas based on sync-mode:
   - sync: Wait for ALL replicas
   - semi-sync: Wait for specified number (--sync-replicas)
   - async: Don't wait, send in background
4. Primary responds to client when condition is met
5. Remaining replicas catch up asynchronously
```

#### 3. **Automatic Failover Process**

```
1. HealthMonitor detects primary failure
2. FailoverCoordinator selects best replica (most up-to-date)
3. Selected replica promotes itself to primary
4. Other replicas redirect to new primary
5. Clients automatically reconnect to new primary
```

### 🌐 Multi-Machine Cluster Setup

For production environments spanning multiple machines:

```bash
# Machine 1 (192.168.1.10) - Primary
python server.py --name "Prod-Primary" --sync-mode semi-sync --sync-replicas 2

# Machine 2 (192.168.1.11) - Replica 1
python server.py --name "Prod-Replica-1" --replica-of 192.168.1.10:9999

# Machine 3 (192.168.1.12) - Replica 2
python server.py --name "Prod-Replica-2" --replica-of 192.168.1.10:9999

# Machine 4 (192.168.1.13) - Replica 3
python server.py --name "Prod-Replica-3" --replica-of 192.168.1.10:9999
```

### 🔧 Monitoring and Management

#### Check Cluster Status

Connect to any server and use administrative commands:

```sql
-- Show cluster topology
SHOW CLUSTER STATUS;

-- Show replication lag
SHOW REPLICA STATUS;

-- Show node health
SHOW NODES;
```

#### Monitoring Logs

Each server logs replication events:

```bash
# Primary server logs
tail -f server.log | grep "Replication"

# Replica server logs  
tail -f replica.log | grep "Sync"
```

### ⚡ Performance Tuning

#### Optimize for Write-Heavy Workloads

```bash
# Use async replication for maximum write throughput
python server.py --sync-mode async
```

#### Optimize for Read-Heavy Workloads

```bash
# Start many read replicas
for i in {1..5}; do
    python server.py --name "Read-Replica-$i" --replica-of localhost:9999 --port $((9998+i)) &
done
```

#### Balance Performance and Consistency

```bash
# Semi-sync with reasonable replica count
python server.py --sync-mode semi-sync --sync-replicas 2
```

### 🚨 Best Practices

1. **Start Primary First**: Always start the primary server before replicas
2. **Health Monitoring**: Monitor replica lag and health status regularly
3. **Network Considerations**: Ensure stable network connections between nodes
4. **Resource Planning**: Allocate sufficient CPU and memory for each instance
5. **Backup Strategy**: Regular backups even with replication
6. **Testing Failover**: Regularly test failover scenarios in staging environments

### 🔄 Dynamic Scaling

Add or remove replicas dynamically:

```bash
# Add a new replica to existing cluster
python server.py --name "Dynamic-Replica" --replica-of localhost:9999 --port 9994

# Remove replica (stop the process, primary will detect and update cluster)
# pkill -f "Dynamic-Replica"
```

The horizontal scaler automatically adjusts to cluster changes and maintains optimal performance.

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

query SCRIPT a.sql ✅ (no need for `query` keyword in the script file)

---
-- Get detailed cache statistics
CACHE STATS

-- Clear all caches (query cache and buffer pool)
CACHE CLEAR ALL

-- Clear cache entries for a specific table
CACHE CLEAR TABLE customers

-- After altering a table, clear its cache entries
CACHE CLEAR TABLE products

-- Clear all caches before benchmarking for consistent results
CACHE CLEAR ALL

-- When checking if query results are cached or not
CACHE STATS

-- Then after running a query, check again
SELECT * FROM orders WHERE customer_id = 1234;
CACHE STATS
---

-- Create table with compound primary key
query CREATE TABLE order_items (
    order_id INT,
    product_id INT,
    quantity INT,
    price DECIMAL(10,2),
    PRIMARY KEY (order_id, product_id)
);

-- Insert records
query INSERT INTO order_items (order_id, product_id, quantity, price) 
VALUES (1, 101, 2, 19.99), (1, 102, 1, 29.99), (2, 101, 3, 19.99);

-- Create compound index
query CREATE INDEX idx_order_product ON order_items (order_id, product_id);

query CREATE UNIQUE INDEX idx_customer_email ON customers (company_id, email);

query SELECT * FROM order_items WHERE order_id = 1 AND product_id = 101;

query DROP INDEX name ON customers ✅

-- Use the test database
query USE test_db ✅

query INSERT INTO products (name, price, stock) VALUES 
('Laptop', 999.99, 10),
('Phone', 499.99, 20),
('Tablet', 299.99, 15),
('Monitor', 249.99, 25),
('Keyboard', 59.99, 50);

query CREATE TABLE products (id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(100) NOT NULL, price DECIMAL(10,2), stock INT DEFAULT 0) ✅

query CREATE TABLE order_details (
    order_id INT IDENTITY(1,1) PRIMARY KEY,
    product_id INT,
    quantity INT,
    status VARCHAR(20),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Test for the joins

query SELECT products.product_id, products.category, order_details.quantity, order_details.status FROM products INNER JOIN order_details ON products.product_id = order_details.product_id WHERE products.category = 'Electronics' AND order_details.quantity > 2;

-- ✅
query INSERT INTO products (name, price, stock) VALUES ('Laptop', 999.99, 10)
query INSERT INTO products (name, price, stock) VALUES ('Phone', 499.99, 20)
query INSERT INTO products (name, price, stock) VALUES ('Tablet', 299.99, 15)

-- Create a simple table
query CREATE TABLE customers (id INT PRIMARY KEY, name String, email VARCHAR(100), age INT) ✅

-- Create a tabqueryle with constraints
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
query INSERT INTO customers (id, name, email, age) VALUES (5, 'Gipsz Jakab', 'gipsz@example.com', 50);
query INSERT INTO customers (id, name, email, age) VALUES (2, 'Jane Smith', 'jane@example.com', 25);
query INSERT INTO customers (id, name, email, age) VALUES (3, 'Bob Johnson', 'bob@example.com', 45);
query INSERT INTO customers (id, name, email, age) VALUES (4, 'Alice Brown', 'alice@example.com', 35);
query INSERT INTO customers (id, name, email, age) VALUES (5, 'Test Test', 'test@example.com', 50);

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

query INSERT INTO employees (id, name, dept_id, salary) VALUES (1, 'Alice', 1, 75000);
query INSERT INTO employees (id, name, dept_id, salary) VALUES (2, 'Bob', 1, 70000);
query INSERT INTO employees (id, name, dept_id, salary) VALUES (3, 'Charlie', 2, 65000);
query INSERT INTO employees (id, name, dept_id, salary) VALUES (4, 'Dave', 2, 68000);
query INSERT INTO employees (id, name, dept_id, salary) VALUES (5, 'Eve', 3, 78000);

--- testing FK Constraints

-- Test 1: Try to delete a department that has employees (should fail)
query DELETE FROM departments WHERE id = 1 ✅

-- Test 2: Try to update a department ID that is referenced by employees (should fail)
query UPDATE departments SET id = 10 WHERE id = 1 ✅

-- Test 3: Try to insert an employee with a non-existent department (should fail)
query INSERT INTO employees (id, name, dept_id) VALUES (7, 'Dave', 4) ✅

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
query SELECT id, name FROM employees UNION SELECT id, name FROM departments ✅

-- INTERSECT
query SELECT dept_id FROM employees INTERSECT SELECT id FROM departments ✅

-- EXCEPT
query SELECT id FROM departments EXCEPT SELECT dept_id FROM employees ✅

-- AND operation
query SELECT * FROM employees WHERE salary > 70000 AND dept_id = 1 ✅

-- OR operation
query SELECT * FROM employees WHERE salary > 75000 OR dept_id = 3

-- NOT operation
query SELECT * FROM departments WHERE NOT id IN (SELECT dept_id FROM employees)

-- Start a transaction
query BEGIN TRANSACTION ✅

-- Make changes within transaction
query INSERT INTO departments (id, name) VALUES (4, 'HR') ✅
query UPDATE employees SET salary = 80000 WHERE id = 1 ✅

-- Commit transaction
query COMMIT TRANSACTION ✅

-- Test rollback
query BEGIN TRANSACTION ✅
query DELETE FROM customers WHERE id = 3 ✅
query ROLLBACK TRANSACTION ✅

-- Create a view
query CREATE VIEW engineering_staff AS SELECT * FROM employees WHERE dept_id = 1

-- Query the view
query SELECT * FROM engineering_staff

-- Drop the view
query DROP VIEW engineering_staff

-- Get distinct departments
query SELECT DISTINCT dept_id FROM employees ✅

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
query SELECT e.name, d.name, e.salary FROM employees e JOIN departments d ON e.dept_id = d.id WHERE e.salary > 65000 ORDER BY e.salary DESC LIMIT 3

-- Query with aggregation and grouping
query SELECT dept_id, AVG (salary) as avg_salary, COUNT(*) as emp_count FROM employees GROUP BY dept_id

-- Nested subqueries
query SELECT * FROM employees WHERE salary > (SELECT AVG(salary) FROM employees)

---
-- index tests ✅

-- Create a test database
CREATE DATABASE index_test
USE index_test

-- Create a test table with data
query CREATE TABLE products (id INT,name VARCHAR(100),price DECIMAL(10,2),category VARCHAR(50),in_stock INT);

query DROP INDEX idx_id ON products;
query CREATE INDEX idx_id ON products (id);
query VISUALIZE BPTREE idx_id ON products;

-- Insert sample data
query INSERT INTO products VALUES ('Laptop XPS', 1299.99, 'Electronics', 10);
query INSERT INTO products VALUES ('Gaming Mouse', 59.99, 'Electronics', 25);
query INSERT INTO products VALUES ('Coffee Maker', 89.99, 'Appliances', 15);
query INSERT INTO products VALUES ('Desk Chair', 199.99, 'Furniture', 8);
query INSERT INTO products VALUES ('Bluetooth Speaker', 79.99, 'Electronics', 30);

-- Time this query - should do a full table scan
query SELECT * FROM products WHERE category = 'Electronics';

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

---
-- ===================================
-- MULTIMODEL DATABASE TESTING
-- Testing Graph, NoSQL, and Object-Relational capabilities
-- ===================================

-- ===================================
-- GRAPH DATABASE OPERATIONS
-- ===================================

-- Create nodes for a social network graph
query CREATE GRAPH social_network;
query USE GRAPH social_network;

-- Create person nodes
query CREATE NODE person (id: 1, name: 'Alice', age: 28, city: 'New York');
query CREATE NODE person (id: 2, name: 'Bob', age: 32, city: 'San Francisco');
query CREATE NODE person (id: 3, name: 'Charlie', age: 25, city: 'Chicago');
query CREATE NODE person (id: 4, name: 'Diana', age: 30, city: 'Seattle');
query CREATE NODE person (id: 5, name: 'Eve', age: 27, city: 'Boston');

-- Create company nodes
query CREATE NODE company (id: 101, name: 'TechCorp', industry: 'Technology', size: 5000);
query CREATE NODE company (id: 102, name: 'DataInc', industry: 'Analytics', size: 1200);
query CREATE NODE company (id: 103, name: 'CloudSys', industry: 'Cloud Computing', size: 800);

-- Create location nodes
query CREATE NODE location (id: 201, name: 'New York', country: 'USA', population: 8000000);
query CREATE NODE location (id: 202, name: 'San Francisco', country: 'USA', population: 875000);
query CREATE NODE location (id: 203, name: 'Chicago', country: 'USA', population: 2700000);

-- Create relationships (edges)
query CREATE EDGE friendship FROM person(1) TO person(2) (since: '2020-01-15', strength: 0.8);
query CREATE EDGE friendship FROM person(2) TO person(3) (since: '2019-05-10', strength: 0.9);
query CREATE EDGE friendship FROM person(1) TO person(3) (since: '2021-03-20', strength: 0.7);
query CREATE EDGE friendship FROM person(3) TO person(4) (since: '2018-11-05', strength: 0.85);
query CREATE EDGE friendship FROM person(4) TO person(5) (since: '2022-02-14', strength: 0.6);

-- Create employment relationships
query CREATE EDGE works_at FROM person(1) TO company(101) (position: 'Software Engineer', salary: 95000, start_date: '2021-01-01');
query CREATE EDGE works_at FROM person(2) TO company(102) (position: 'Data Scientist', salary: 110000, start_date: '2020-06-15');
query CREATE EDGE works_at FROM person(3) TO company(101) (position: 'Product Manager', salary: 120000, start_date: '2019-03-01');
query CREATE EDGE works_at FROM person(4) TO company(103) (position: 'DevOps Engineer', salary: 105000, start_date: '2020-09-01');

-- Create lives_in relationships
query CREATE EDGE lives_in FROM person(1) TO location(201) (since: '2018-01-01');
query CREATE EDGE lives_in FROM person(2) TO location(202) (since: '2019-01-01');
query CREATE EDGE lives_in FROM person(3) TO location(203) (since: '2017-01-01');

-- Graph traversal queries
-- Find all friends of Alice
query MATCH (p1:person {name: 'Alice'})-[f:friendship]-(p2:person) RETURN p2.name, f.strength;

-- Find friends of friends (2-hop traversal)
query MATCH (p1:person {name: 'Alice'})-[f1:friendship]-(p2:person)-[f2:friendship]-(p3:person) WHERE p1.id != p3.id RETURN p3.name, p2.name as mutual_friend;

-- Find shortest path between two people
query MATCH path = SHORTEST_PATH((p1:person {name: 'Alice'})-[*]-(p2:person {name: 'Eve'})) RETURN path;

-- Find all employees at TechCorp and their friends
query MATCH (p:person)-[:works_at]->(c:company {name: 'TechCorp'}), (p)-[:friendship]-(friend:person) RETURN p.name, friend.name;

-- Complex graph analytics
-- Find influential people (those with many connections)
query MATCH (p:person)-[f:friendship]-() RETURN p.name, COUNT(f) as connection_count ORDER BY connection_count DESC;

-- Find average salary by city
query MATCH (p:person)-[:works_at]->(c:company), (p)-[:lives_in]->(l:location) RETURN l.name, AVG(w.salary) as avg_salary;

-- PageRank algorithm simulation
query GRAPH PAGERANK social_network ITERATIONS 10 DAMPING 0.85;

-- Community detection
query GRAPH COMMUNITY_DETECTION social_network ALGORITHM 'louvain';

-- Centrality measures
query GRAPH CENTRALITY social_network TYPE 'betweenness';
query GRAPH CENTRALITY social_network TYPE 'closeness';
query GRAPH CENTRALITY social_network TYPE 'degree';

-- ===================================
-- NOSQL DOCUMENT OPERATIONS
-- ===================================

-- Create document collections
query CREATE COLLECTION users;
query CREATE COLLECTION orders;
query CREATE COLLECTION products;
query CREATE COLLECTION reviews;

-- Insert JSON documents into users collection
query INSERT INTO users DOCUMENT {
  "_id": "user_001",
  "name": "John Doe",
  "email": "john.doe@email.com",
  "age": 29,
  "address": {
    "street": "123 Main St",
    "city": "New York",
    "state": "NY",
    "zipcode": "10001"
  },
  "preferences": {
    "theme": "dark",
    "notifications": true,
    "newsletter": false
  },
  "tags": ["premium", "early_adopter"],
  "registration_date": "2023-01-15T10:30:00Z",
  "last_login": "2024-06-14T08:45:00Z",
  "purchase_history": [
    {"product_id": "prod_001", "amount": 99.99, "date": "2023-02-01"},
    {"product_id": "prod_003", "amount": 149.99, "date": "2023-05-15"}
  ]
};

query INSERT INTO users DOCUMENT {
  "_id": "user_002",
  "name": "Jane Smith",
  "email": "jane.smith@email.com",
  "age": 34,
  "address": {
    "street": "456 Oak Ave",
    "city": "San Francisco",
    "state": "CA",
    "zipcode": "94102"
  },
  "preferences": {
    "theme": "light",
    "notifications": false,
    "newsletter": true
  },
  "tags": ["vip", "frequent_buyer"],
  "registration_date": "2022-11-20T14:22:00Z",
  "last_login": "2024-06-13T19:30:00Z",
  "purchase_history": [
    {"product_id": "prod_002", "amount": 79.99, "date": "2022-12-01"},
    {"product_id": "prod_001", "amount": 99.99, "date": "2023-01-10"},
    {"product_id": "prod_004", "amount": 199.99, "date": "2023-08-22"}
  ]
};

-- Insert products with complex nested structures
query INSERT INTO products DOCUMENT {
  "_id": "prod_001",
  "name": "Wireless Headphones",
  "category": "Electronics",
  "price": 99.99,
  "specifications": {
    "battery_life": "20 hours",
    "connectivity": ["Bluetooth 5.0", "USB-C"],
    "features": ["Noise Cancellation", "Quick Charge", "Voice Assistant"],
    "dimensions": {
      "width": "18cm",
      "height": "20cm",
      "depth": "8cm",
      "weight": "250g"
    }
  },
  "availability": {
    "in_stock": true,
    "quantity": 150,
    "warehouses": [
      {"location": "New York", "quantity": 50},
      {"location": "California", "quantity": 75},
      {"location": "Texas", "quantity": 25}
    ]
  },
  "ratings": {
    "average": 4.5,
    "count": 1247,
    "distribution": {
      "5": 623,
      "4": 374,
      "3": 186,
      "2": 43,
      "1": 21
    }
  },
  "tags": ["wireless", "premium", "noise-cancelling"],
  "created_at": "2023-01-01T00:00:00Z",
  "updated_at": "2024-06-01T12:00:00Z"
};

-- NoSQL queries with complex conditions
-- Find users by nested field
query SELECT * FROM users WHERE address.city = 'New York';

-- Find users with specific tags
query SELECT * FROM users WHERE 'premium' IN tags;

-- Find products with rating above 4.0
query SELECT * FROM products WHERE ratings.average > 4.0;

-- Find users who made purchases after a certain date
query SELECT * FROM users WHERE purchase_history[*].date > '2023-01-01';

-- Aggregate operations on documents
-- Count users by city
query SELECT address.city, COUNT(*) as user_count FROM users GROUP BY address.city;

-- Average product rating by category
query SELECT category, AVG(ratings.average) as avg_rating FROM products GROUP BY category;

-- Array operations
-- Find products with specific features
query SELECT * FROM products WHERE 'Noise Cancellation' IN specifications.features;

-- Update nested fields
query UPDATE users SET preferences.theme = 'dark' WHERE _id = 'user_002';

-- Add to array
query UPDATE users SET tags = ARRAY_APPEND(tags, 'loyal_customer') WHERE _id = 'user_001';

-- Remove from array
query UPDATE users SET tags = ARRAY_REMOVE(tags, 'early_adopter') WHERE _id = 'user_001';

-- Complex document joins
query SELECT u.name, p.name as product_name, ph.amount 
FROM users u 
CROSS JOIN JSON_TABLE(u.purchase_history, '$[*]' COLUMNS (
  product_id VARCHAR(50) PATH '$.product_id',
  amount DECIMAL(10,2) PATH '$.amount',
  date DATETIME PATH '$.date'
)) ph
JOIN products p ON p._id = ph.product_id;

-- Text search on documents
query SELECT * FROM products WHERE FULL_TEXT_SEARCH(name, 'wireless headphones');

-- GeoSpatial queries (if coordinates are stored)
query INSERT INTO users DOCUMENT {
  "_id": "user_003",
  "name": "Mike Johnson",
  "location": {
    "type": "Point",
    "coordinates": [-74.006, 40.7128]  // NYC coordinates
  }
};

query SELECT * FROM users WHERE ST_DISTANCE(location, ST_POINT(-74.006, 40.7128)) < 1000; // Within 1km

-- ===================================
-- OBJECT-RELATIONAL OPERATIONS
-- ===================================

-- Create custom object types
query CREATE TYPE address_type AS (
  street VARCHAR(100),
  city VARCHAR(50),
  state VARCHAR(2),
  zipcode VARCHAR(10),
  country VARCHAR(50) DEFAULT 'USA'
);

query CREATE TYPE contact_info_type AS (
  email VARCHAR(100),
  phone VARCHAR(20),
  website VARCHAR(100)
);

query CREATE TYPE money_type AS (
  amount DECIMAL(15,2),
  currency VARCHAR(3) DEFAULT 'USD'
);

-- Create tables with object types
query CREATE TABLE companies (
  id INT PRIMARY KEY,
  name VARCHAR(100),
  headquarters address_type,
  contact contact_info_type,
  revenue money_type,
  established_date DATE,
  employees INT[]  -- Array type
);

query CREATE TABLE projects (
  id INT PRIMARY KEY,
  name VARCHAR(100),
  company_id INT,
  budget money_type,
  team_members INT[],  -- Array of employee IDs
  milestones JSON,     -- JSON type for complex milestone data
  metadata JSON,       -- Additional metadata
  FOREIGN KEY (company_id) REFERENCES companies(id)
);

-- Insert data using object constructors
query INSERT INTO companies VALUES (
  1, 
  'TechInnovate Corp',
  ROW('100 Tech Plaza', 'San Francisco', 'CA', '94105', 'USA')::address_type,
  ROW('contact@techinnovate.com', '+1-555-0123', 'www.techinnovate.com')::contact_info_type,
  ROW(50000000.00, 'USD')::money_type,
  '2010-03-15',
  ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
);

query INSERT INTO companies VALUES (
  2,
  'DataDynamics LLC',
  ROW('200 Data Street', 'Seattle', 'WA', '98101', 'USA')::address_type,
  ROW('info@datadynamics.com', '+1-555-0456', 'www.datadynamics.com')::contact_info_type,
  ROW(25000000.00, 'USD')::money_type,
  '2015-07-22',
  ARRAY[11, 12, 13, 14, 15]
);

-- Insert projects with JSON data
query INSERT INTO projects VALUES (
  101,
  'AI Platform Development',
  1,
  ROW(5000000.00, 'USD')::money_type,
  ARRAY[1, 3, 5, 7],
  JSON '{
    "phase1": {"name": "Research", "deadline": "2024-09-01", "status": "completed"},
    "phase2": {"name": "Development", "deadline": "2024-12-15", "status": "in_progress"},
    "phase3": {"name": "Testing", "deadline": "2025-03-01", "status": "planned"}
  }',
  JSON '{
    "priority": "high",
    "confidentiality": "internal",
    "technologies": ["Python", "TensorFlow", "Kubernetes"],
    "client_requirements": {
      "performance": "99.9% uptime",
      "scalability": "1M+ concurrent users",
      "security": "SOC2 compliant"
    }
  }'
);

query INSERT INTO projects VALUES (
  102,
  'Data Analytics Dashboard',
  2,
  ROW(1500000.00, 'USD')::money_type,
  ARRAY[11, 13, 15],
  JSON '{
    "phase1": {"name": "Design", "deadline": "2024-08-15", "status": "completed"},
    "phase2": {"name": "Implementation", "deadline": "2024-11-30", "status": "in_progress"},
    "phase3": {"name": "Deployment", "deadline": "2025-01-15", "status": "planned"}
  }',
  JSON '{
    "priority": "medium",
    "confidentiality": "public",
    "technologies": ["React", "D3.js", "PostgreSQL"],
    "target_metrics": {
      "user_engagement": "75%",
      "load_time": "<2 seconds",
      "data_accuracy": "99.5%"
    }
  }'
);

-- Query object attributes
query SELECT name, (headquarters).city, (headquarters).state FROM companies;

-- Query nested object attributes
query SELECT name, (contact).email, (revenue).amount FROM companies WHERE (revenue).amount > 30000000;

-- Array operations
query SELECT name FROM companies WHERE 5 = ANY(employees);  -- Check if employee 5 exists

query SELECT name FROM companies WHERE ARRAY_LENGTH(employees) > 7;  -- Companies with more than 7 employees

-- JSON operations
query SELECT name, JSON_EXTRACT(metadata, '$.priority') as priority FROM projects;

query SELECT name, JSON_EXTRACT(milestones, '$.phase2.status') as phase2_status FROM projects;

-- Complex JSON queries
query SELECT 
  name,
  JSON_EXTRACT(metadata, '$.technologies') as tech_stack,
  JSON_EXTRACT(metadata, '$.client_requirements.performance') as performance_req
FROM projects 
WHERE JSON_EXTRACT(metadata, '$.priority') = 'high';

-- Update object attributes
query UPDATE companies SET (headquarters).zipcode = '94106' WHERE id = 1;

-- Update array elements
query UPDATE companies SET employees = ARRAY_APPEND(employees, 20) WHERE id = 1;

-- Update JSON fields
query UPDATE projects SET metadata = JSON_SET(metadata, '$.priority', 'critical') WHERE id = 101;

-- Complex joins with object types
query SELECT 
  c.name as company_name,
  (c.headquarters).city as city,
  p.name as project_name,
  (p.budget).amount as budget_amount,
  JSON_EXTRACT(p.metadata, '$.priority') as priority
FROM companies c
JOIN projects p ON c.id = p.company_id
WHERE (c.revenue).amount > 20000000
ORDER BY (p.budget).amount DESC;

-- Aggregate functions with object types
query SELECT 
  (headquarters).state,
  COUNT(*) as company_count,
  AVG((revenue).amount) as avg_revenue
FROM companies 
GROUP BY (headquarters).state;

-- Window functions with object types
query SELECT 
  name,
  (revenue).amount,
  RANK() OVER (ORDER BY (revenue).amount DESC) as revenue_rank
FROM companies;

-- Array aggregation
query SELECT 
  company_id,
  ARRAY_AGG(name) as project_names,
  SUM((budget).amount) as total_budget
FROM projects 
GROUP BY company_id;

-- JSON aggregation
query SELECT 
  JSON_EXTRACT(metadata, '$.priority') as priority_level,
  COUNT(*) as project_count,
  AVG((budget).amount) as avg_budget
FROM projects 
GROUP BY JSON_EXTRACT(metadata, '$.priority');

-- ===================================
-- MULTIMODEL INTEGRATION QUERIES
-- ===================================

-- Cross-model operations: Graph + Relational
query SELECT 
  p.name as person_name,
  c.name as company_name,
  (c.headquarters).city as company_city
FROM GRAPH social_network 
MATCH (p:person)-[:works_at]->(comp:company)
JOIN companies c ON c.name = comp.name;

-- Cross-model operations: NoSQL + Object-Relational
query SELECT 
  u.name as user_name,
  c.name as company_name,
  JSON_EXTRACT(u.document, '$.purchase_history[0].amount') as first_purchase
FROM users u
CROSS JOIN companies c
WHERE JSON_EXTRACT(u.document, '$.address.city') = (c.headquarters).city;

-- Complex multimodel analytics
-- Find graph relationships that correspond to business relationships
query WITH company_employees AS (
  SELECT 
    p.name as person_name,
    comp.name as company_name
  FROM GRAPH social_network 
  MATCH (p:person)-[:works_at]->(comp:company)
),
user_purchases AS (
  SELECT 
    JSON_EXTRACT(document, '$.name') as user_name,
    JSON_EXTRACT(document, '$.purchase_history[*].amount') as purchase_amounts
  FROM users
)
SELECT 
  ce.person_name,
  ce.company_name,
  up.purchase_amounts
FROM company_employees ce
LEFT JOIN user_purchases up ON ce.person_name = up.user_name;

-- Recommendation engine using multiple models
-- Find product recommendations based on graph connections and purchase history
query WITH friend_purchases AS (
  SELECT DISTINCT
    p1.name as user_name,
    JSON_EXTRACT(purchases.document, '$.purchase_history[*].product_id') as friend_products
  FROM GRAPH social_network 
  MATCH (p1:person)-[:friendship]-(p2:person)
  JOIN users purchases ON JSON_EXTRACT(purchases.document, '$.name') = p2.name
)
SELECT 
  fp.user_name,
  p.name as recommended_product,
  p.ratings.average as product_rating
FROM friend_purchases fp
CROSS JOIN products p
WHERE p._id = ANY(fp.friend_products)
AND p.ratings.average > 4.0;

-- Performance testing multimodel queries
query EXPLAIN SELECT 
  g.person_name,
  d.user_preferences,
  o.project_involvement
FROM (
  SELECT p.name as person_name, p.age
  FROM GRAPH social_network MATCH (p:person)
) g
LEFT JOIN (
  SELECT 
    JSON_EXTRACT(document, '$.name') as name,
    JSON_EXTRACT(document, '$.preferences') as user_preferences
  FROM users
) d ON g.person_name = d.name
LEFT JOIN (
  SELECT 
    JSON_EXTRACT(metadata, '$.team_lead') as lead_name,
    COUNT(*) as project_involvement
  FROM projects
  GROUP BY JSON_EXTRACT(metadata, '$.team_lead')
) o ON g.person_name = o.lead_name;
```

## 📊 Architecture

pyHMSSQL follows a modular architecture with clear separation of concerns:

-Client Layer: Handles user interaction through CLI or GUI
-Server Layer: Processes requests and manages database operations
-Storage Layer: Persists data and metadata using MongoDBMongoDB
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

