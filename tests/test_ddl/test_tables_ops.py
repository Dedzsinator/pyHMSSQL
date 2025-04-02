"""
Tests for DDL table operations (CREATE TABLE, DROP TABLE, etc.).
"""
import pytest

class TestCreateTable:
    """Test CREATE TABLE operation."""
    
    def test_create_simple_table(self, schema_manager, catalog_manager):
        """Test creating a simple table."""
        plan = {
            "type": "CREATE_TABLE",
            "table": "test_table",
            "columns": [
                "id INT NOT NULL PRIMARY KEY",
                "name TEXT NOT NULL",
                "description TEXT"
            ]
        }
        
        result = schema_manager.execute_create_table(plan)
        
        assert result["status"] == "success"
        tables = catalog_manager.list_tables("test_db")
        assert "test_table" in tables
        
        # Verify schema
        schema = catalog_manager.get_table_schema("test_table")
        columns = [col["name"] for col in schema]
        assert "id" in columns
        assert "name" in columns
        assert "description" in columns
    
    def test_create_duplicate_table(self, schema_manager, catalog_manager, test_table):
        """Test attempting to create a duplicate table."""
        plan = {
            "type": "CREATE_TABLE",
            "table": test_table,
            "columns": ["id INT PRIMARY KEY", "name TEXT"]
        }
        
        result = schema_manager.execute_create_table(plan)
        
        assert result["status"] == "error"
        assert "already exists" in result["error"]
    
    def test_create_table_with_constraints(self, schema_manager, catalog_manager):
        """Test creating a table with various constraints."""
        plan = {
            "type": "CREATE_TABLE",
            "table": "products",
            "columns": [
                "id INT NOT NULL PRIMARY KEY",
                "name TEXT NOT NULL",
                "price DECIMAL NOT NULL",
                "category_id INT",
                "FOREIGN KEY (category_id) REFERENCES categories(id)"
            ]
        }
        
        result = schema_manager.execute_create_table(plan)
        
        assert result["status"] == "success"
        tables = catalog_manager.list_tables("test_db")
        assert "products" in tables
        
        # Verify constraints in schema
        schema = catalog_manager.get_table_schema("products")
        assert any("FOREIGN KEY" in str(schema).upper())

class TestDropTable:
    """Test DROP TABLE operation."""
    
    def test_drop_existing_table(self, schema_manager, catalog_manager, test_table):
        """Test dropping an existing table."""
        plan = {
            "type": "DROP_TABLE",
            "table": test_table
        }
        
        result = schema_manager.execute_drop_table(plan)
        
        assert result["status"] == "success"
        tables = catalog_manager.list_tables("test_db")
        assert test_table not in tables
    
    def test_drop_nonexistent_table(self, schema_manager):
        """Test attempting to drop a non-existent table."""
        plan = {
            "type": "DROP_TABLE",
            "table": "nonexistent_table"
        }
        
        result = schema_manager.execute_drop_table(plan)
        
        assert result["status"] == "error"
        assert "does not exist" in result["error"]