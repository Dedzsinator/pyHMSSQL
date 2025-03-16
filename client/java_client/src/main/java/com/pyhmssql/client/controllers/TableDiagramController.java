package com.pyhmssql.client.controllers;

import com.pyhmssql.client.model.TableMetadata;
import javafx.geometry.Point2D;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Controller for managing tables in the diagram area
 */
public class TableDiagramController {
    private final QueryBuilderController queryBuilderController;
    private final Map<String, Object> tableUIComponents;
    private final Map<String, Point2D> tablePositions;
    private Consumer<String> onTableRemoved;
    
    public TableDiagramController(QueryBuilderController queryBuilderController) {
        this.queryBuilderController = queryBuilderController;
        this.tableUIComponents = new HashMap<>();
        this.tablePositions = new HashMap<>();
    }
    
    /**
     * Add a table to the diagram
     * @param tableName Table name
     * @param x X position
     * @param y Y position
     * @return CompletableFuture with TableMetadata
     */
    public CompletableFuture<TableMetadata> addTable(String tableName, double x, double y) {
        // Check if already exists
        if (tableUIComponents.containsKey(tableName)) {
            return CompletableFuture.completedFuture(null);
        }
        
        // Store position
        tablePositions.put(tableName, new Point2D(x, y));
        
        // Add to query model and get metadata
        return queryBuilderController.addTable(tableName);
    }
    
    /**
     * Remove a table from the diagram
     * @param tableName Table name
     */
    public void removeTable(String tableName) {
        queryBuilderController.removeTable(tableName);
        
        // Remove UI component reference
        tableUIComponents.remove(tableName);
        
        // Remove position
        tablePositions.remove(tableName);
        
        // Notify listeners
        if (onTableRemoved != null) {
            onTableRemoved.accept(tableName);
        }
    }
    
    /**
     * Update a table's position
     * @param tableName Table name
     * @param x New X position
     * @param y New Y position
     */
    public void updateTablePosition(String tableName, double x, double y) {
        tablePositions.put(tableName, new Point2D(x, y));
    }
    
    /**
     * Get a table's position
     * @param tableName Table name
     * @return Point2D with position or null if not found
     */
    public Point2D getTablePosition(String tableName) {
        return tablePositions.get(tableName);
    }
    
    /**
     * Register a UI component for a table
     * @param tableName Table name
     * @param component UI component representing the table
     */
    public void registerUIComponent(String tableName, Object component) {
        tableUIComponents.put(tableName, component);
    }
    
    /**
     * Get the UI component for a table
     * @param tableName Table name
     * @return UI component or null if not found
     */
    public Object getUIComponent(String tableName) {
        return tableUIComponents.get(tableName);
    }
    
    /**
     * Set the handler for table removal events
     * @param handler Consumer that handles table names
     */
    public void setOnTableRemoved(Consumer<String> handler) {
        this.onTableRemoved = handler;
    }
    
    /**
     * Get all tables in the diagram
     * @return List of table names
     */
    public List<String> getTables() {
        return queryBuilderController.getQueryModel().getTables();
    }
    
    /**
     * Check if a table is in the diagram
     * @param tableName Table name
     * @return True if table exists
     */
    public boolean hasTable(String tableName) {
        return tableUIComponents.containsKey(tableName);
    }
    
    /**
     * Clear all tables from the diagram
     */
    public void clearAllTables() {
        List<String> tables = getTables();
        
        // Copy the list to avoid concurrent modification
        List<String> tablesCopy = tables.stream().collect(Collectors.toList());
        
        // Remove each table
        for (String tableName : tablesCopy) {
            removeTable(tableName);
        }
    }
    
    /**
     * Get metadata for a table
     * @param tableName Table name
     * @return CompletableFuture with TableMetadata
     */
    public CompletableFuture<TableMetadata> getTableMetadata(String tableName) {
        return queryBuilderController.getTableMetadata(tableName);
    }
}