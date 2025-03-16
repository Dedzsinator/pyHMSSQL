package com.pyhmssql.client.controllers;

import com.pyhmssql.client.model.JoinModel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Controller for managing table joins
 */
public class JoinController {
    private final QueryBuilderController queryBuilderController;
    private final Map<String, Object> joinUIComponents;
    private Consumer<String> onJoinRemoved;
    
    public JoinController(QueryBuilderController queryBuilderController) {
        this.queryBuilderController = queryBuilderController;
        this.joinUIComponents = new HashMap<>();
    }
    
    /**
     * Add a join between tables
     * @param joinType Type of join (INNER, LEFT, RIGHT, FULL)
     * @param leftTable Left table name
     * @param leftColumn Left column name
     * @param rightTable Right table name
     * @param rightColumn Right column name
     * @return True if added successfully
     */
    public boolean addJoin(String joinType, String leftTable, String leftColumn,
                         String rightTable, String rightColumn) {
        // Check if tables exist
        List<String> tables = queryBuilderController.getQueryModel().getTables();
        if (!tables.contains(leftTable) || !tables.contains(rightTable)) {
            return false;
        }
        
        // Check for duplicate joins
        String joinKey = getJoinKey(leftTable, leftColumn, rightTable, rightColumn);
        if (joinUIComponents.containsKey(joinKey)) {
            return false;
        }
        
        // Create and add the join
        JoinModel join = new JoinModel(
            JoinModel.JoinType.fromString(joinType),
            leftTable, leftColumn,
            rightTable, rightColumn
        );
        
        queryBuilderController.getQueryModel().addJoin(join);
        return true;
    }
    
    /**
     * Update a join's properties
     * @param joinKey The join key to update
     * @param joinType New join type
     */
    public void updateJoin(String joinKey, String joinType) {
        // Parse the join key
        String[] parts = joinKey.split("\\|");
        if (parts.length != 4) return;
        
        String leftTable = parts[0];
        String leftColumn = parts[1];
        String rightTable = parts[2];
        String rightColumn = parts[3];
        
        // Find and update the join
        List<JoinModel> joins = queryBuilderController.getQueryModel().getJoins();
        for (JoinModel join : joins) {
            if (join.getLeftTable().equals(leftTable) && 
                join.getLeftColumn().equals(leftColumn) &&
                join.getRightTable().equals(rightTable) && 
                join.getRightColumn().equals(rightColumn)) {
                
                join.setType(JoinModel.JoinType.fromString(joinType));
                return;
            }
        }
    }
    
    /**
     * Remove a join
     * @param leftTable Left table name
     * @param leftColumn Left column name
     * @param rightTable Right table name
     * @param rightColumn Right column name
     */
    public void removeJoin(String leftTable, String leftColumn,
                          String rightTable, String rightColumn) {
        String joinKey = getJoinKey(leftTable, leftColumn, rightTable, rightColumn);
        
        // Find and remove the join
        List<JoinModel> joins = queryBuilderController.getQueryModel().getJoins();
        joins.removeIf(join -> 
            join.getLeftTable().equals(leftTable) && 
            join.getLeftColumn().equals(leftColumn) &&
            join.getRightTable().equals(rightTable) && 
            join.getRightColumn().equals(rightColumn)
        );
        
        // Remove UI component reference
        joinUIComponents.remove(joinKey);
        
        // Notify listeners
        if (onJoinRemoved != null) {
            onJoinRemoved.accept(joinKey);
        }
    }
    
    /**
     * Register a UI component for a join
     * @param leftTable Left table name
     * @param leftColumn Left column name
     * @param rightTable Right table name
     * @param rightColumn Right column name 
     * @param component UI component representing the join
     */
    public void registerUIComponent(String leftTable, String leftColumn,
                                  String rightTable, String rightColumn, Object component) {
        String joinKey = getJoinKey(leftTable, leftColumn, rightTable, rightColumn);
        joinUIComponents.put(joinKey, component);
    }
    
    /**
     * Get the UI component for a join
     * @param leftTable Left table name
     * @param leftColumn Left column name
     * @param rightTable Right table name
     * @param rightColumn Right column name
     * @return UI component or null if not found
     */
    public Object getUIComponent(String leftTable, String leftColumn,
                               String rightTable, String rightColumn) {
        String joinKey = getJoinKey(leftTable, leftColumn, rightTable, rightColumn);
        return joinUIComponents.get(joinKey);
    }
    
    /**
     * Set the handler for join removal events
     * @param handler Consumer that handles join keys
     */
    public void setOnJoinRemoved(Consumer<String> handler) {
        this.onJoinRemoved = handler;
    }
    
    /**
     * Get all joins
     * @return List of JoinModel
     */
    public List<JoinModel> getJoins() {
        return queryBuilderController.getQueryModel().getJoins();
    }
    
    /**
     * Get joins for a specific table
     * @param tableName Table name
     * @return List of JoinModel involving the table
     */
    public List<JoinModel> getJoinsForTable(String tableName) {
        return queryBuilderController.getQueryModel().getJoins().stream()
            .filter(join -> join.getLeftTable().equals(tableName) || join.getRightTable().equals(tableName))
            .collect(Collectors.toList());
    }
    
    /**
     * Check if a join already exists
     * @param leftTable Left table name
     * @param leftColumn Left column name
     * @param rightTable Right table name
     * @param rightColumn Right column name
     * @return True if join exists
     */
    public boolean hasJoin(String leftTable, String leftColumn,
                         String rightTable, String rightColumn) {
        String joinKey = getJoinKey(leftTable, leftColumn, rightTable, rightColumn);
        return joinUIComponents.containsKey(joinKey);
    }
    
    /**
     * Clear all joins
     */
    public void clearAllJoins() {
        queryBuilderController.getQueryModel().getJoins().clear();
        joinUIComponents.clear();
    }
    
    /**
     * Generate a unique key for a join
     * @param leftTable Left table name
     * @param leftColumn Left column name
     * @param rightTable Right table name
     * @param rightColumn Right column name
     * @return Unique key string
     */
    private String getJoinKey(String leftTable, String leftColumn,
                            String rightTable, String rightColumn) {
        return leftTable + "|" + leftColumn + "|" + rightTable + "|" + rightColumn;
    }
}