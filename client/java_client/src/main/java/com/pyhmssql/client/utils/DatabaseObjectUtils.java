package com.pyhmssql.client.utils;

import com.pyhmssql.client.model.DatabaseObject;
import com.pyhmssql.client.model.TableMetadata;
import com.pyhmssql.client.model.ColumnMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utilities for working with database objects
 */
public class DatabaseObjectUtils {
    
    /**
     * Create table metadata from server response
     * @param tableName Table name
     * @param columnsData List of column data maps
     * @return TableMetadata object
     */
    public static TableMetadata createTableMetadata(String tableName, List<Map<String, Object>> columnsData) {
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
     * Get the appropriate icon name for a database object
     * @param object DatabaseObject
     * @return Icon name for the object type
     */
    // Fix the incompatible type error with a proper type check:
    public static String getIconForDatabaseObject(DatabaseObject object) {
        switch (object.getType()) {
            case DATABASE:
                return "database-icon";
            case TABLE:
                return "table-icon";
            case VIEW:
                return "view-icon";
            case COLUMN:
                if (object instanceof ColumnMetadata) {
                    ColumnMetadata colMetadata = (ColumnMetadata) object;
                    if (colMetadata.isPrimaryKey()) {
                        return "primary-key-icon";
                    }
                }
                return "column-icon";
            case INDEX:
                return "index-icon";
            case PROCEDURE:
                return "procedure-icon";
            case FUNCTION:
                return "function-icon";
            case TRIGGER:
                return "trigger-icon";
            default:
                return "default-icon";
        }
    }
    
    /**
     * Check if a column type is numeric
     * @param columnType SQL column type
     * @return True if numeric
     */
    public static boolean isNumericType(String columnType) {
        return columnType != null && (
            columnType.equalsIgnoreCase("INT") ||
            columnType.equalsIgnoreCase("INTEGER") ||
            columnType.equalsIgnoreCase("SMALLINT") ||
            columnType.equalsIgnoreCase("TINYINT") ||
            columnType.equalsIgnoreCase("MEDIUMINT") ||
            columnType.equalsIgnoreCase("BIGINT") ||
            columnType.equalsIgnoreCase("DECIMAL") ||
            columnType.equalsIgnoreCase("NUMERIC") ||
            columnType.equalsIgnoreCase("FLOAT") ||
            columnType.equalsIgnoreCase("DOUBLE") ||
            columnType.equalsIgnoreCase("REAL")
        );
    }
    
    /**
     * Check if a column type is a string type
     * @param columnType SQL column type
     * @return True if string type
     */
    public static boolean isStringType(String columnType) {
        return columnType != null && (
            columnType.equalsIgnoreCase("CHAR") ||
            columnType.equalsIgnoreCase("VARCHAR") ||
            columnType.equalsIgnoreCase("TEXT") ||
            columnType.equalsIgnoreCase("TINYTEXT") ||
            columnType.equalsIgnoreCase("MEDIUMTEXT") ||
            columnType.equalsIgnoreCase("LONGTEXT") ||
            columnType.equalsIgnoreCase("NCHAR") ||
            columnType.equalsIgnoreCase("NVARCHAR") ||
            columnType.equalsIgnoreCase("NTEXT")
        );
    }
    
    /**
     * Check if a column type is a date/time type
     * @param columnType SQL column type
     * @return True if date/time type
     */
    public static boolean isDateTimeType(String columnType) {
        return columnType != null && (
            columnType.equalsIgnoreCase("DATE") ||
            columnType.equalsIgnoreCase("TIME") ||
            columnType.equalsIgnoreCase("DATETIME") ||
            columnType.equalsIgnoreCase("TIMESTAMP") ||
            columnType.equalsIgnoreCase("YEAR")
        );
    }
    
    /**
     * Get a list of valid operators for a column type
     * @param columnType SQL column type
     * @return List of valid operators
     */
    public static List<String> getValidOperatorsForType(String columnType) {
        List<String> operators = new ArrayList<>();
        
        // Common operators for all types
        operators.add("=");
        operators.add("<>");
        operators.add("IS NULL");
        operators.add("IS NOT NULL");
        
        if (isNumericType(columnType)) {
            operators.add(">");
            operators.add("<");
            operators.add(">=");
            operators.add("<=");
            operators.add("BETWEEN");
        } else if (isStringType(columnType)) {
            operators.add("LIKE");
            operators.add("NOT LIKE");
            operators.add("IN");
            operators.add("NOT IN");
            operators.add("STARTS WITH");
            operators.add("ENDS WITH");
            operators.add("CONTAINS");
        } else if (isDateTimeType(columnType)) {
            operators.add(">");
            operators.add("<");
            operators.add(">=");
            operators.add("<=");
            operators.add("BETWEEN");
        }
        
        return operators;
    }
}