package com.pyhmssql.client.model;

import java.util.List;
import java.util.ArrayList;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.Map;

/**
 * Model class representing a database table's metadata
 */
public class TableMetadata {
    private final String name;
    private final List<ColumnMetadata> columns;
    
    public TableMetadata(String name, List<ColumnMetadata> columns) {
        this.name = name;
        this.columns = columns != null ? columns : new ArrayList<>();
    }
    
    public String getName() {
        return name;
    }
    
    public List<ColumnMetadata> getColumns() {
        return columns;
    }
    
    /**
     * Get a column by name
     * @param columnName The name of the column to retrieve
     * @return The column metadata, or null if not found
     */
    public ColumnMetadata getColumn(String columnName) {
        if (columnName == null) return null;
        
        return columns.stream()
            .filter(col -> columnName.equals(col.getName()))
            .findFirst()
            .orElse(null);
    }

    public static TableMetadata fromColumnsData(String tableName, List<Map<String, Object>> columnsData) {
        List<ColumnMetadata> columns = columnsData.stream()
            .map(data -> new ColumnMetadata(
                (String) data.get("name"),
                (String) data.get("type"),
                (Boolean) data.getOrDefault("primary_key", false),
                (Boolean) data.getOrDefault("nullable", true)
            ))
            .collect(Collectors.toList());
        
        return new TableMetadata(tableName, columns);
    }
    
    /**
     * Checks if the table has a column with the specified name
     * @param columnName The column name to check
     * @return true if the column exists, false otherwise
     */
    public boolean hasColumn(String columnName) {
        return getColumn(columnName) != null;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableMetadata that = (TableMetadata) o;
        return Objects.equals(name, that.name);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
    
    @Override
    public String toString() {
        return name;
    }
}