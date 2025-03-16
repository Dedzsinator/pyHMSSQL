package model;

import java.util.Objects;

/**
 * Model class representing a database object (database, table, column, etc.)
 */
public class DatabaseObject {
    public enum ObjectType {
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
    
    private final String name;
    private final ObjectType type;
    private final String database;
    private final String parent;  // parent object name (e.g., table name for a column)
    
    public DatabaseObject(String name, ObjectType type) {
        this(name, type, null, null);
    }
    
    public DatabaseObject(String name, ObjectType type, String database) {
        this(name, type, database, null);
    }
    
    public DatabaseObject(String name, ObjectType type, String database, String parent) {
        this.name = name;
        this.type = type;
        this.database = database;
        this.parent = parent;
    }
    
    public String getName() {
        return name;
    }
    
    public ObjectType getType() {
        return type;
    }
    
    public String getDatabase() {
        return database;
    }
    
    public String getParent() {
        return parent;
    }
    
    /**
     * Gets the full name of this object including database and parent if applicable
     * @return The full name
     */
    public String getFullName() {
        StringBuilder fullName = new StringBuilder();
        
        if (database != null && !database.isEmpty()) {
            fullName.append(database).append(".");
            
            if (type == ObjectType.COLUMN && parent != null && !parent.isEmpty()) {
                fullName.append(parent).append(".");
            }
        } else if (type == ObjectType.COLUMN && parent != null && !parent.isEmpty()) {
            fullName.append(parent).append(".");
        }
        
        fullName.append(name);
        return fullName.toString();
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatabaseObject that = (DatabaseObject) o;
        return Objects.equals(name, that.name) && 
               type == that.type && 
               Objects.equals(database, that.database) && 
               Objects.equals(parent, that.parent);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(name, type, database, parent);
    }
    
    @Override
    public String toString() {
        return getFullName();
    }
}