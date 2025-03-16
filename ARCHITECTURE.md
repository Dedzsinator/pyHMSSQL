# pyHMSSQL Architecture

## System Overview

pyHMSSQL is a database management system built with a client-server architecture. The system consists of several components that work together to provide database functionality.

![Architecture Diagram](https://via.placeholder.com/800x400?text=pyHMSSQL+Architecture)

## Core Components

### Client Side

1. **CLI Client** (`client/cli_client.py`)
   - Provides a command-line interface for interacting with the database
   - Parses user commands and sends them to the server
   - Displays results from the server
   - Handles connection management
   - Supports batch execution from files
   - Implements session-based authentication

2. **GUI Client** (`client/gui_client.py` & `client/gui/main_window.py`)
   - Provides a graphical user interface for database operations
   - Offers form-based input for common database operations
   - Displays results and notifications to the user
   - Uses Tkinter for the UI components

3. **Shared Utilities** (`shared/` directory)
   - Contains common code used by both client and server
   - Implements network communication protocol (JSON over sockets)
   - Defines constants and utility functions
   - Handles JSON serialization/deserialization
   - Provides custom JSON encoder for MongoDB ObjectId

### Server Side

1. **Server** (`server/server.py`)
   - Listens for client connections on a predefined port
   - Routes requests to appropriate components
   - Returns results to clients
   - Handles multiple concurrent client connections
   - Provides error handling and logging
   - Manages user authentication and sessions
   - Implements role-based access control

2. **Catalog Manager** (`server/catalog_manager.py`)
   - Manages database metadata (schemas, tables, columns)
   - Stores information about indexes
   - Provides CRUD operations for database objects
   - Uses MongoDB for persistent storage of metadata
   - Handles user authentication and registration
   - Manages user preferences
   - Manages views, stored procedures, functions, and triggers
   - Handles temporary tables for sessions
   - Provides management functions for all database objects

3. **Index Manager** (`server/index_manager.py`)
   - Creates and maintains indexes for fast data retrieval
   - Implements B+ Tree data structure for efficient lookups
   - Provides methods for updating and querying indexes
   - Handles serialization and deserialization of index structures
   - Manages index files on disk

4. **B+ Tree Implementation** (`server/bptree.py`)
   - Custom implementation of the B+ Tree data structure
   - Provides efficient key-value lookups
   - Supports range queries
   - Implements node splitting and balancing
   - Handles serialization for persistence

5. **Planner** (`server/planner.py`)
   - Parses SQL queries (SELECT, INSERT, UPDATE, DELETE)
   - Generates execution plans for queries
   - Handles complex query structures (joins, subqueries)
   - Transforms SQL statements into executable operations
   - Uses Haskell-based SQL parser (`SQLParser.hs`)

6. **Optimizer** (`server/optimizer.py`)
   - Analyzes execution plans to improve performance
   - Uses indexes for efficient data access
   - Implements join optimizations (hash joins, index joins, sort-merge joins)
   - Applies techniques like filter pushdown, expression rewriting
   - Handles join reordering and index selection
   - Optimizes sort operations and limit clauses

7. **Execution Engine** (`server/execution_engine.py`)
   - Executes query plans
   - Interacts with storage engine (MongoDB)
   - Returns results to the server
   - Performs CRUD operations on actual data
   - Implements join algorithms and aggregation functions
   - Handles set operations and logical operations
   - Supports transactions (begin, commit, rollback)
   - Respects user preferences

8. **Procedure Manager**
   - Executes stored procedures
   - Manages procedure context and variables
   - Handles control flow (IF, WHILE, etc.)
   - Supports transaction management within procedures

9. **Function Manager**
   - Executes user-defined functions
   - Handles return values
   - Supports function calls within SQL queries
   - Manages function context and variables

10. **Trigger Manager**
    - Monitors database events (INSERT, UPDATE, DELETE)
    - Executes associated triggers when events occur
    - Provides access to OLD and NEW row values
    - Handles trigger chaining and recursion prevention

## Data Flow

1. **Authentication Flow**:
   - Client sends login credentials
   - Server authenticates against stored user records
   - Server generates and returns a session ID
   - Client includes session ID in subsequent requests
   - Server validates the session ID before processing requests

2. **Query Execution Pipeline**:
   - Client submits a command or query
   - Server validates user permissions for the operation
   - For schema operations (CREATE/DROP), the Catalog Manager handles the request
   - For data operations (SELECT/INSERT/DELETE/UPDATE):
     - SQL is parsed using the Haskell parser
     - The Planner creates an execution plan
     - The Optimizer improves the plan for efficiency
     - The Execution Engine executes the optimized plan
     - Results are returned to the client

3. **Index Usage**:
   - When a query involves a field with an index, the index is used for lookups
   - B+ Tree indexes provide efficient key-based and range-based access
   - When data is modified, indexes are updated accordingly
   - The optimizer selects appropriate indexes for query conditions

4. **Advanced Query Processing**:
   - For views, the view definition is retrieved and executed
   - For procedure calls, the procedure body is executed step by step
   - For function calls, the function is executed and its result integrated into the query
   - For triggers, associated triggers are executed when table events occur

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

## Join Algorithms

The system implements multiple join algorithms:

1. **Hash Join**:
   - Builds a hash table on the smaller relation
   - Probes the hash table with the larger relation
   - Efficient for equality joins

2. **Sort-Merge Join**:
   - Sorts both relations on the join key
   - Merges the sorted relations
   - Efficient for sorted data

3. **Index Join**:
   - Uses an index on one relation
   - Looks up matching records for each tuple in the other relation
   - Efficient when an index exists on the join column

4. **Nested Loop Join**:
   - Fallback algorithm when others aren't applicable
   - Iterates through both relations

## Advanced Features

### Views Management

The system supports database views:

- **Creation and Storage**: Views are stored in the catalog
- **Query Resolution**: When a view is referenced, its query is executed
- **Metadata Management**: View definitions are accessible through the catalog
- **Security**: Access to views follows the same permission model as tables

### Stored Procedures and Functions

The system supports stored procedures and functions:

- **Procedure Execution**:
  - Procedures are parsed and stored in the catalog
  - When called, procedures are executed in a controlled environment
  - Procedures can contain multiple SQL statements
  - Parameters are supported for flexible execution

- **Function Execution**:
  - Functions are compiled and stored in the catalog
  - Functions can be called from SQL statements
  - Return values are integrated into the calling query
  - Functions support parameters and local variables

### Triggers

The system implements database triggers:

- **Event Monitoring**:
  - INSERT, UPDATE, and DELETE events are monitored
  - When an event occurs on a table with triggers, they are executed

- **Execution Context**:
  - Triggers have access to OLD and NEW row values
  - Triggers execute in the context of the transaction
  - Multiple triggers on the same event are executed in order

- **Management**:
  - Triggers can be created, dropped, and enabled/disabled
  - Metadata about triggers is stored in the catalog

### Temporary Tables

The system supports temporary tables:

- **Session Isolation**:
  - Temporary tables are only visible to the creating session
  - Tables are automatically dropped when the session ends

- **Use Cases**:
  - Complex query intermediate results
  - Multi-step data processing
  - Transaction-specific data storage

## Query Optimization Techniques

The Optimizer implements several strategies:

1. **Index-Based Access**:
   - Uses indexes for efficient data retrieval
   - Avoids full table scans when possible

2. **Join Optimization**:
   - Selects the most efficient join algorithm based on available indexes
   - Reorders joins to minimize intermediate results
   - Pushes filters down to reduce early result sizes

3. **Predicate Pushdown**:
   - Pushes filter conditions closer to data sources
   - Reduces intermediate result sizes

4. **Join Reordering**:
   - Reorders join operations to minimize intermediate results
   - Prioritizes joins with available indexes

5. **Expression Rewriting**:
   - Simplifies and normalizes expressions
   - Eliminates redundant conditions
   - Merges multiple filters

6. **Sort-Limit Optimization**:
   - Converts sort + limit to top-N operation
   - More efficient for retrieving top results

## Security Model

The system implements a role-based security model:

1. **User Authentication**:
   - Password-based authentication
   - Secure password hashing (SHA-256)
   - Session-based authorization

2. **Role-Based Access Control**:
   - Different user roles (admin, user)
   - Permission checking for operations
   - Query filtering based on user permissions

3. **Session Management**:
   - UUID-based session identifiers
   - Session tracking on the server
   - Session termination on logout

## Storage Layer

pyHMSSQL uses MongoDB as its storage engine with these additions:

- **Metadata Collections**:
  - `views` collection for view definitions
  - `procedures` collection for stored procedure definitions
  - `functions` collection for function definitions
  - `triggers` collection for trigger definitions

- **In-Memory Storage**:
  - Temporary tables stored in memory, linked to session ID
  - Not persisted between server restarts

## Communication Protocol

The client and server communicate using a simple JSON-based protocol:

**Client Request Format:**

```json
{
  "action": "login|register|logout|create_database|drop_database|create_table|drop_table|create_index|query",
  "username": "username",
  "password": "password",
  "role": "admin|user",
  "session_id": "uuid",
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
  "response": "Success or error message or query results",
  "session_id": "uuid for login responses",
  "role": "user role for login responses"
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
5. Transaction Support:
   -Begin, commit, and rollback operations
   -Transaction state tracking
6. User Preferences:
   -Configurable result limits
   -Pretty printing options
   -Per-user preference storage
