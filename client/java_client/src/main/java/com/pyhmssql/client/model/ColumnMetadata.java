package model;

import java.util.Objects;

/**
 * Model class representing a database column's metadata
 */
public class ColumnMetadata {
    private final String name;
    private final String type;
    private final boolean primaryKey;
    private final boolean nullable;
    
    public ColumnMetadata(String name, String type, boolean primaryKey, boolean nullable) {
        this.name = name;
        this.type = type;
        this.primaryKey = primaryKey;
        this.nullable = nullable;
    }
    
    public String getName() {
        return name;
    }
    
    public String getType() {
        return type;
    }
    
    public boolean isPrimaryKey() {
        return primaryKey;
    }
    
    public boolean isNullable() {
        return nullable;
    }
    
    /**
     * Determines if this column can be used in a join condition.
     * Typically primary keys and foreign keys are used in joins.
     * 
     * @return true if the column is suitable for joins
     */
    public boolean isJoinable() {
        return primaryKey || name.toLowerCase().endsWith("_id") || name.toLowerCase().equals("id");
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnMetadata that = (ColumnMetadata) o;
        return Objects.equals(name, that.name);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
    
    @Override
    public String toString() {
        return name + " (" + type + ")";
    }
}