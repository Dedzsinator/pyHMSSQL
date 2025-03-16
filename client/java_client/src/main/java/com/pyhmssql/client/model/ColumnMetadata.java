package com.pyhmssql.client.model;

import java.util.Objects;

/**
 * Model class for column metadata
 */
public class ColumnMetadata implements DatabaseObject {
    private String name;
    private String type;
    private boolean primaryKey;
    private boolean nullable;
    
    public ColumnMetadata(String name, String type, boolean primaryKey, boolean nullable) {
        this.name = name;
        this.type = type;
        this.primaryKey = primaryKey;
        this.nullable = nullable;
    }
    
    public String getName() {
        return name;
    }
    
    public boolean isPrimaryKey() {
        return primaryKey;
    }
    
    public boolean isNullable() {
        return nullable;
    }
    
    @Override
    public DatabaseObjectType getType() {
        return DatabaseObjectType.COLUMN;
    }
    
    @Override
    public String getDisplayName() {
        return name;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnMetadata that = (ColumnMetadata) o;
        return primaryKey == that.primaryKey && 
               nullable == that.nullable && 
               Objects.equals(name, that.name) && 
               Objects.equals(type, that.type);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(name, type, primaryKey, nullable);
    }
    
    @Override
    public String toString() {
        return name + " (" + type + ")" + (primaryKey ? " PK" : "");
    }
}