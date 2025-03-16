package controllers;

import model.ColumnSelectionModel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Controller for managing column projections (SELECT clause)
 */
public class ProjectionController {
    private final QueryBuilderController queryBuilderController;
    private final Map<String, Object> projectionUIComponents;
    private Consumer<String> onColumnRemoved;
    
    public ProjectionController(QueryBuilderController queryBuilderController) {
        this.queryBuilderController = queryBuilderController;
        this.projectionUIComponents = new HashMap<>();
    }
    
    /**
     * Add a column to the projection list
     * @param table Table name
     * @param column Column name
     * @param alias Optional alias
     * @param aggregate Optional aggregate function
     * @return True if added successfully
     */
    public boolean addColumn(String table, String column, String alias, 
                          ColumnSelectionModel.AggregateFunction aggregate) {
        if (hasColumn(table, column)) {
            return false; // Already exists
        }
        
        ColumnSelectionModel columnModel = new ColumnSelectionModel(table, column, alias, aggregate, true);
        queryBuilderController.getQueryModel().addColumn(columnModel);
        return true;
    }
    
    /**
     * Update a column's properties
     * @param table Table name
     * @param column Column name
     * @param alias New alias
     * @param aggregate New aggregate function
     * @param selected Whether the column is selected
     */
    public void updateColumn(String table, String column, String alias, 
                           ColumnSelectionModel.AggregateFunction aggregate, boolean selected) {
        List<ColumnSelectionModel> columns = queryBuilderController.getQueryModel().getColumns();
        
        for (ColumnSelectionModel col : columns) {
            if (col.getTable().equals(table) && col.getColumn().equals(column)) {
                col.setAlias(alias);
                col.setAggregate(aggregate);
                col.setSelected(selected);
                return;
            }
        }
    }
    
    /**
     * Remove a column from the projection list
     * @param table Table name
     * @param column Column name
     */
    public void removeColumn(String table, String column) {
        String columnKey = getColumnKey(table, column);
        
        // Find and remove the model
        List<ColumnSelectionModel> columns = queryBuilderController.getQueryModel().getColumns();
        columns.removeIf(col -> col.getTable().equals(table) && col.getColumn().equals(column));
        
        // Remove UI component reference
        projectionUIComponents.remove(columnKey);
        
        // Notify listeners
        if (onColumnRemoved != null) {
            onColumnRemoved.accept(columnKey);
        }
    }
    
    /**
     * Register a UI component for a column
     * @param table Table name
     * @param column Column name
     * @param component UI component representing the column
     */
    public void registerUIComponent(String table, String column, Object component) {
        String columnKey = getColumnKey(table, column);
        projectionUIComponents.put(columnKey, component);
    }
    
    /**
     * Get the UI component for a column
     * @param table Table name
     * @param column Column name
     * @return UI component or null if not found
     */
    public Object getUIComponent(String table, String column) {
        String columnKey = getColumnKey(table, column);
        return projectionUIComponents.get(columnKey);
    }
    
    /**
     * Set the handler for column removal events
     * @param handler Consumer that handles column keys
     */
    public void setOnColumnRemoved(Consumer<String> handler) {
        this.onColumnRemoved = handler;
    }
    
    /**
     * Get all columns in the projection
     * @return List of ColumnSelectionModel
     */
    public List<ColumnSelectionModel> getColumns() {
        return queryBuilderController.getQueryModel().getColumns();
    }
    
    /**
     * Get columns for a specific table
     * @param tableName Table name
     * @return List of ColumnSelectionModel for the table
     */
    public List<ColumnSelectionModel> getColumnsForTable(String tableName) {
        return queryBuilderController.getQueryModel().getColumns().stream()
            .filter(col -> col.getTable().equals(tableName))
            .collect(Collectors.toList());
    }
    
    /**
     * Check if a column is already in the projection
     * @param table Table name
     * @param column Column name
     * @return True if column is in projection
     */
    public boolean hasColumn(String table, String column) {
        String columnKey = getColumnKey(table, column);
        return projectionUIComponents.containsKey(columnKey);
    }
    
    /**
     * Clear all columns from the projection
     */
    public void clearAllColumns() {
        queryBuilderController.getQueryModel().getColumns().clear();
        projectionUIComponents.clear();
    }
    
    /**
     * Generate a unique key for a column
     * @param table Table name
     * @param column Column name
     * @return Unique key string
     */
    private String getColumnKey(String table, String column) {
        return table + "." + column;
    }
}