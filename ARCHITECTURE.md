# pyHMSSQL Architecture

## System Overview

pyHMSSQL is a database management system built with a client-server architecture. The system consists of several components that work together to provide database functionality.

![Architecture Diagram](https://via.placeholder.com/800x400?text=pyHMSSQL+Architecture)

## Core Components

### Client Side

1. **CLI Client** (`cli_client.py`)
   - Provides a command-line interface for interacting with the database
   - Parses user commands and sends them to the server
   - Displays results from the server
   - Handles connection management
   - Supports batch execution from files

2. **GUI Client** (`gui_client.py` & `main_window.py`)
   - Provides a graphical user interface for database operations
   - Offers form-based input for common database operations
   - Displays results and notifications to the user
   - Uses Tkinter for the UI components

3. **Shared Utilities** (`shared/` directory)
   - Contains common code used by both client and server
   - Implements network communication protocol (JSON over sockets)
   - Defines constants and utility functions
   - Handles JSON serialization/deserialization

### Server Side

1. **Server** (`server.py`)
   - Listens for client connections on a predefined port
   - Routes requests to appropriate components
   - Returns results to clients
   - Handles multiple concurrent client connections
   - Provides error handling and logging

2. **Catalog Manager** (`catalog_manager.py`)
   - Manages database metadata (schemas, tables, columns)
   - Stores information about indexes
   - Provides CRUD operations for database objects
   - Uses MongoDB for persistent storage of metadata

3. **Index Manager** (`index_manager.py`)
   - Creates and maintains indexes for fast data retrieval
   - Implements B+ Tree data structure for efficient lookups
   - Provides methods for updating and querying indexes
   - Handles serialization and deserialization of index structures

4. **Planner** (`planner.py`)
   - Parses SQL queries (SELECT, INSERT, UPDATE, DELETE)
   - Generates execution plans for queries
   - Handles complex query structures (joins, subqueries)
   - Transforms SQL statements into executable operations

5. **Optimizer** (`optimizer.py`)
   - Analyzes execution plans to improve performance
   - Uses indexes for efficient data access
   - Implements join optimizations (hash joins, index joins)
   - Applies techniques like filter pushdown, expression rewriting
   - Handles join reordering and index selection

6. **Execution Engine** (`execution_engine.py`)
   - Executes query plans
   - Interacts with storage engine (MongoDB)
   - Returns results to the server
   - Performs CRUD operations on actual data
   - Implements join algorithms and aggregation functions
   - Handles set operations and logical operations

## Data Flow

1. **Client Request Processing**:
   - Client submits a command or query
   - Command is serialized to JSON and sent to the server
   - Server receives and deserializes the request
   - Server determines the appropriate action

2. **Query Execution Pipeline**:
   - For schema operations (CREATE/DROP), the Catalog Manager handles the request
   - For data operations (SELECT/INSERT/DELETE/UPDATE):
     - The Planner parses and creates an execution plan
     - The Optimizer improves the plan for efficiency
     - The Execution Engine executes the optimized plan
     - Results are returned to the client

3. **Index Usage**:
   - When a query involves a field with an index, the index is used for lookups
   - B+ Tree indexes provide efficient key-based and range-based access
   - When data is modified, indexes are updated accordingly
   - The optimizer selects appropriate indexes for query conditions

## B+ Tree Implementation

The system uses a custom B+ Tree implementation for indexing:

- **Tree Structure**:
  - Keys are column values, values are record identifiers
  - Leaf nodes contain actual data entries
  - Non-leaf nodes contain routing information
  - Leaves are linked for efficient range scans

- **Key Operations**:
  - Insert: Add a new key-value pair
  - Search: Lookup a specific key
  - Range Query: Find all keys in a given range
  
- **Optimizations**:
  - Node splitting for balanced tree structure
  - Leaf node linking for efficient sequential access
  - Key-value storage in leaf nodes for direct data access
  
- **Persistence**:
  - Trees are serialized using pickle
  - Each index is stored in a separate file
  - Loaded on-demand to minimize memory usage

## Query Optimization Techniques

The Optimizer implements several strategies:

1. **Index-Based Access**:
   - Uses indexes for efficient data retrieval
   - Avoids full table scans when possible

2. **Join Optimization**:
   - Hash join for equality conditions
   - Sort-merge join for sorted data
   - Index join when indexes are available
   - Nested loop join as fallback

3. **Predicate Pushdown**:
   - Pushes filter conditions closer to data sources
   - Reduces intermediate result sizes

4. **Join Reordering**:
   - Reorders join operations to minimize intermediate results
   - Prioritizes joins with available indexes

5. **Expression Rewriting**:
   - Simplifies and normalizes expressions
   - Eliminates redundant conditions

## Storage Layer

pyHMSSQL uses MongoDB as its storage engine:

- Database metadata stored in MongoDB collections
- Table definitions stored as documents
- Index information maintained in separate collections
- Actual table data stored as documents in MongoDB collections
- B+ Tree index files stored in the filesystem

## Communication Protocol

The client and server communicate using a simple JSON-based protocol:

**Client Request Format:**

```json
{
  "action": "create_database|drop_database|create_table|drop_table|create_index|query",
  "db_name": "database_name",
  "table_name": "table_name",
  "columns": { "column_name": {"type": "data_type"} },
  "index_name": "index_name",
  "column": "column_name",
  "query": "SQL_QUERY"
}
```

**Server Response Format:**

```json
{
  "response": "Success or error message or query results"
}
```

## Special Features

1. Batch Processing:
   -Execute multiple SQL commands from script files
   -Process large operations efficiently
2. Aggregation Functions:
   -Implement AVG, MIN, MAX, SUM, COUNT operations
   -Support for TOP N queries
   -DISTINCT operation support
3. Set Operations:
   -UNION, INTERSECT, EXCEPT operations between query results
   -Support for logical operations (AND, OR, NOT)
4. Advanced Query Support:
   -Subquery processing
   -Join operations
   -Filter conditions
