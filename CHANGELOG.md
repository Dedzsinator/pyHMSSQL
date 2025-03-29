# CHANGELOG

## Version 1.5.0 (2025-03-29)

### Major Changes

- **Storage Engine Overhaul**: Replaced MongoDB-based storage with custom binary file storage system
- **Architecture Simplification**: Removed CommandHandler and integrated its functionality into SQLParser
- **Enhanced SQLParser**: Refactored to directly handle special commands (USE DATABASE, CREATE INDEX)
- **Execution Flow**: Improved command execution pipeline from Parser to Execution Engine

### Features Implemented

#### Storage System

- Binary file-based storage for tables and indexes
- Schema management through catalog files
- Record-level operations with optimization

#### SQL Operations

- ✅ CREATE/DROP DATABASE
- ✅ USE DATABASE
- ✅ CREATE/DROP TABLE
- ✅ SHOW TABLES/DATABASES
- ✅ CREATE INDEX (regular and UNIQUE)
- ✅ DROP INDEX
- ✅ SHOW INDEXES (all or table-specific)
- ✅ Full CRUD operations (SELECT, INSERT, UPDATE, DELETE)
- ✅ SELECT with WHERE conditions, ORDER BY, and LIMIT clauses

#### B+ Tree Indexing

- ✅ Custom implementation with serialization
- ✅ Index-based lookups
- ✅ Range queries
- ✅ Visual index inspection through VISUALIZE BPTREE command

#### Security

- ✅ User authentication
- ✅ Role-based access control
- ✅ Session management

#### Client Applications

- Command Line Interface (CLI)
- Graphical User Interface (GUI)
- Java client with query history tracking

### Internal Improvements

#### Code Organization

- Improved separation of concerns between components
- Enhanced error handling and logging
- Better parameterization of SQL commands

#### Performance

- Optimized index lookups for common operations
- Improved query parsing logic
- Enhanced binary serialization for records

### Known Limitations

- Advanced query features (JOINs, subqueries, aggregations) still in development
- Transaction support not yet implemented
- Cost-based query optimization not fully implemented

### Future Plans

-Implement transaction support (BEGIN, COMMIT, ROLLBACK)

- Add support for views and temporary tables
- Enhance query optimization with cost-based decisions
- Implement advanced join operations (hash join, merge join)
- Add support for stored procedures and functions
