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

- [x] **Multiple Client Interfaces**
  - [x] Command Line Interface (CLI)
  - [x] Graphical User Interface (GUI)
  - [ ] REST API (planned)

- [x] **Database Operations**
  - [x] Create/Drop Databases
  - [x] Create/Drop Tables
  - [ ] SQL Query Support (SELECT, INSERT, DELETE)
  - [ ] Transactions (planned)
  - [ ] Stored Procedures (planned)

- [x] **Indexing**
  - [x] Custom B+ Tree Implementation
  - [x] Serialization for persistence
  - [x] Index-based lookups
  - [ ] Range queries

- [x] **Storage**
  - [x] MongoDB Integration
  - [x] Schema management
  - [ ] Custom page-based storage engine (planned)

- [ ] **Security Features**
  - [ ] User authentication
  - [ ] Access control
  - [ ] Encryption

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
  - W.I.P

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

## 📝 Example Commands

```bash
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

-- Delete data
DBMS> query DELETE FROM users WHERE id = 1
```

## 📊 Architecture

pyHMSSQL follows a modular architecture with clear separation of concerns:

Client Layer: Handles user interaction through CLI or GUI
Server Layer: Processes requests and manages database operations
Storage Layer: Persists data and metadata using MongoDB
Index Layer: Optimizes data retrieval using B+ trees
For more details, see ARCHITECTURE.md.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 💡 Future Enhancements

1. Transaction Support:

    *Implementing ACID properties
    *Concurrency control mechanisms
    *Rollback and recovery
    *Advanced Query Processing:

2. JOIN operations
    *Aggregation (GROUP BY, SUM, AVG)
    *Subqueries
    *Security Enhancements:

3. User authentication
    *Access control lists
    *Data encryption
    *Performance Optimizations:

4. Query caching
    *Cost-based query optimization
    *Connection pooling
    *Custom Storage Engine:

5. Custom Storage Engine:
    *Page-based storage
    *Write-ahead logging
    *Buffer management
