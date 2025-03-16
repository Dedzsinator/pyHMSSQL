# pyHMSSQL

![Database](https://img.shields.io/badge/Database-Engine-blue)
![Python](https://img.shields.io/badge/Python-3.7+-yellow)
![Status](https://img.shields.io/badge/Status-Development-green)

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
-- Login to the system
DBMS> login username password

-- Create a new database
DBMS> create database mydb

-- Create a table
DBMS> create table mydb users id:int name:varchar age:int

-- Create an index on the name column
DBMS> create index mydb users idx_name name

-- Insert data
DBMS> query INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)

-- Query data
DBMS> query SELECT * FROM users WHERE name = 'Alice'

-- Join example
DBMS> query SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id

-- Aggregation example
DBMS> query SELECT AVG(age) FROM users

-- Delete data
DBMS> query DELETE FROM users WHERE id = 1

-- Run SQL script from file
DBMS> files path/to/script.sql

-- Set user preferences
DBMS> query SET PREFERENCE pretty_print true

-- Begin a transaction
DBMS> query BEGIN TRANSACTION

-- Commit a transaction
DBMS> query COMMIT

-- Logout
DBMS> logout

-- Create a view
DBMS> query CREATE VIEW active_users AS SELECT * FROM users WHERE active = true

-- Drop a view
DBMS> query DROP VIEW active_users

-- Create a stored procedure
DBMS> query CREATE PROCEDURE add_user(name VARCHAR, age INT) 
      BEGIN 
        INSERT INTO users (name, age) VALUES (name, age); 
      END

-- Execute a stored procedure
DBMS> query CALL add_user('John', 25)

-- Create a function
DBMS> query CREATE FUNCTION get_age(user_id INT) 
      RETURNS INT 
      BEGIN 
        RETURN (SELECT age FROM users WHERE id = user_id); 
      END

-- Create a trigger
DBMS> query CREATE TRIGGER update_log 
      AFTER UPDATE ON users 
      FOR EACH ROW 
      BEGIN 
        INSERT INTO logs VALUES (NEW.id, 'updated'); 
      END
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
