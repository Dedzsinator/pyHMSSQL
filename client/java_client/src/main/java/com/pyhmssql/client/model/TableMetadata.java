package com.pyhmssql.client.model;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

/**
 * Represents metadata for a database table
 */
public class TableMetadata {
    private String name;
    private List<ColumnMetadata> columns;

    public TableMetadata(String name, List<ColumnMetadata> columns) {
        this.name = name;
        this.columns = columns != null ? columns : new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<ColumnMetadata> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnMetadata> columns) {
        this.columns = columns;
    }

    /**
     * Creates TableMetadata from columns data received from server
     * 
     * @param tableName   Name of the table
     * @param columnsData List of column data maps
     * @return TableMetadata instance
     */
    public static TableMetadata fromColumnsData(String tableName, List<Map<String, Object>> columnsData) {
        List<ColumnMetadata> columns = new ArrayList<>();

        if (columnsData != null) {
            for (Map<String, Object> columnData : columnsData) {
                String name = (String) columnData.get("name");
                String type = (String) columnData.get("type");
                boolean primaryKey = (Boolean) columnData.getOrDefault("primary_key", false);
                boolean nullable = (Boolean) columnData.getOrDefault("nullable", true);

                columns.add(new ColumnMetadata(name, type, primaryKey, nullable));
            }
        }

        return new TableMetadata(tableName, columns);
    }
}