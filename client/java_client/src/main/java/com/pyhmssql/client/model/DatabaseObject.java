package com.pyhmssql.client.model;

/**
 * Interface for all database objects
 */
public interface DatabaseObject {
    /**
     * The type of database object (table, column, etc.)
     */
    public enum DatabaseObjectType {
        DATABASE,
        TABLE,
        VIEW,
        COLUMN,
        INDEX,
        PROCEDURE,
        FUNCTION,
        TRIGGER,
        CONSTRAINT,
        UNKNOWN
    }
    
    /**
     * Gets the type of this database object
     * @return The database object type
     */
    DatabaseObjectType getType();
    
    /**
     * Gets the display name of this object
     * @return The name to display in UI
     */
    String getDisplayName();
}