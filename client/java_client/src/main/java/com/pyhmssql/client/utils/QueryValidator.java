package com.pyhmssql.client.utils;

import com.pyhmssql.client.model.ColumnSelectionModel;
import com.pyhmssql.client.model.JoinModel;
import com.pyhmssql.client.model.QueryModel;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for validating query models
 */
public class QueryValidator {
    
    /**
     * Validate a query model
     * @param queryModel The model to validate
     * @return List of validation error messages, empty if valid
     */
    public static List<String> validate(QueryModel queryModel) {
        List<String> errors = new ArrayList<>();
        
        // Check database
        if (queryModel.getDatabase() == null || queryModel.getDatabase().isEmpty()) {
            errors.add("No database selected");
        }
        
        // Check tables
        if (queryModel.getTables().isEmpty()) {
            errors.add("No tables selected");
        }
        
        // For SELECT queries, validate projections
        if (queryModel.getType() == QueryModel.QueryType.SELECT) {
            validateSelectQuery(queryModel, errors);
        }
        
        // For UPDATE queries
        else if (queryModel.getType() == QueryModel.QueryType.UPDATE) {
            validateUpdateQuery(queryModel, errors);
        }
        
        // For DELETE queries
        else if (queryModel.getType() == QueryModel.QueryType.DELETE) {
            validateDeleteQuery(queryModel, errors);
        }
        
        // For INSERT queries
        else if (queryModel.getType() == QueryModel.QueryType.INSERT) {
            validateInsertQuery(queryModel, errors);
        }
        
        return errors;
    }
    
    /**
     * Validate a SELECT query
     */
    private static void validateSelectQuery(QueryModel queryModel, List<String> errors) {
        // Validate columns
        List<ColumnSelectionModel> selectedColumns = queryModel.getColumns().stream()
            .filter(ColumnSelectionModel::isSelected)
            .toList();
            
        if (selectedColumns.isEmpty()) {
            errors.add("No columns selected for projection");
        }
        
        // Validate joins if multiple tables
        if (queryModel.getTables().size() > 1) {
            validateJoins(queryModel, errors);
        }
    }
    
    /**
     * Validate an UPDATE query
     */
    private static void validateUpdateQuery(QueryModel queryModel, List<String> errors) {
        // Check for multiple tables
        if (queryModel.getTables().size() > 1) {
            errors.add("UPDATE queries should have only one table");
        }
        
        // Check for at least one column to update
        if (queryModel.getColumns().isEmpty()) {
            errors.add("No columns selected for update");
        }
    }
    
    /**
     * Validate a DELETE query
     */
    private static void validateDeleteQuery(QueryModel queryModel, List<String> errors) {
        // Check for multiple tables
        if (queryModel.getTables().size() > 1) {
            errors.add("DELETE queries should have only one table");
        }
        
        // Check for WHERE clause (safety check)
        if (queryModel.getWhereConditions().isEmpty()) {
            errors.add("WARNING: DELETE without WHERE will affect all rows");
        }
    }
    
    /**
     * Validate an INSERT query
     */
    private static void validateInsertQuery(QueryModel queryModel, List<String> errors) {
        // Check for multiple tables
        if (queryModel.getTables().size() > 1) {
            errors.add("INSERT queries should have only one table");
        }
        
        // Check for at least one column
        if (queryModel.getColumns().isEmpty()) {
            errors.add("No columns selected for insert");
        }
    }
    
    /**
     * Validate joins between tables
     */
    private static void validateJoins(QueryModel queryModel, List<String> errors) {
        List<String> tables = queryModel.getTables();
        List<JoinModel> joins = queryModel.getJoins();
        
        // Check if all tables are joined
        if (tables.size() > 1 && joins.isEmpty()) {
            errors.add("Multiple tables selected but no joins defined");
            return;
        }
        
        // Create a list of joined tables
        List<String> joinedTables = new ArrayList<>();
        
        // Add the first table to start with
        if (!tables.isEmpty()) {
            joinedTables.add(tables.get(0));
        }
        
        // Check each join to see if it connects to a table we've already joined
        boolean progress;
        do {
            progress = false;
            
            for (JoinModel join : joins) {
                String leftTable = join.getLeftTable();
                String rightTable = join.getRightTable();
                
                if (joinedTables.contains(leftTable) && !joinedTables.contains(rightTable)) {
                    joinedTables.add(rightTable);
                    progress = true;
                } else if (joinedTables.contains(rightTable) && !joinedTables.contains(leftTable)) {
                    joinedTables.add(leftTable);
                    progress = true;
                }
            }
        } while (progress);
        
        // Check if all tables are now joined
        for (String table : tables) {
            if (!joinedTables.contains(table)) {
                errors.add("Table '" + table + "' is not joined with other tables");
            }
        }
    }
}