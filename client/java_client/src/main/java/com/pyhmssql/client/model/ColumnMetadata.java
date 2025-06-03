package com.pyhmssql.client.model;

/**
 * Represents metadata for a database column
 */
public class ColumnMetadata {
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

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }
}