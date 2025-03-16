package controllers;

import main.ConnectionManager;
import model.ColumnSelectionModel;
import model.ConditionModel;
import model.JoinModel;
import model.QueryModel;
import model.TableMetadata;
import utils.QueryValidator;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Main controller for query building functionality
 */
public class QueryBuilderController {
    private final ConnectionManager connectionManager;
    private final QueryModel queryModel;
    private final ProjectionController projectionController;
    private final JoinController joinController;
    private final TableDiagramController tableDiagramController;
    private final Map<String, TableMetadata> tableMetadataCache;
    private Consumer<String> onQueryBuilt;
    
    public QueryBuilderController(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        this.queryModel = new QueryModel();
        this.tableMetadataCache = new HashMap<>();
        
        // Initialize sub-controllers
        this.projectionController = new ProjectionController(this);
        this.joinController = new JoinController(this);
        this.tableDiagramController = new TableDiagramController(this);
    }
    
    /**
     * Get the query model
     * @return QueryModel instance
     */
    public QueryModel getQueryModel() {
        return queryModel;
    }
    
    /**
     * Get the projection controller
     * @return ProjectionController instance
     */
    public ProjectionController getProjectionController() {
        return projectionController;
    }
    
    /**
     * Get the join controller
     * @return JoinController instance
     */
    public JoinController getJoinController() {
        return joinController;
    }
    
    /**
     * Get the table diagram controller
     * @return TableDiagramController instance
     */
    public TableDiagramController getTableDiagramController() {
        return tableDiagramController;
    }
    
    /**
     * Set the database context for the query
     * @param database Database name
     */
    public void setDatabase(String database) {
        queryModel.setDatabase(database);
        // Clear caches when database changes
        tableMetadataCache.clear();
    }
    
    /**
     * Add a table to the query
     * @param tableName Table name
     * @return CompletableFuture with TableMetadata
     */
    public CompletableFuture<TableMetadata> addTable(String tableName) {
        // First check cache
        if (tableMetadataCache.containsKey(tableName)) {
            TableMetadata metadata = tableMetadataCache.get(tableName);
            queryModel.addTable(tableName);
            return CompletableFuture.completedFuture(metadata);
        }
        
        // Load from server
        return connectionManager.getColumns(queryModel.getDatabase(), tableName)
            .thenApply(result -> {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> columnsData = (List<Map<String, Object>>) result.get("columns");
                
                // Create table metadata
                TableMetadata metadata = TableMetadata.fromColumnsData(tableName, columnsData);
                
                // Cache it
                tableMetadataCache.put(tableName, metadata);
                
                // Add to model
                queryModel.addTable(tableName);
                
                return metadata;
            });
    }
    
    /**
     * Remove a table from the query
     * @param tableName Table name
     */
    public void removeTable(String tableName) {
        queryModel.removeTable(tableName);
        
        // Remove all columns from this table
        List<ColumnSelectionModel> columnsToRemove = projectionController.getColumnsForTable(tableName);
        for (ColumnSelectionModel col : columnsToRemove) {
            projectionController.removeColumn(col.getTable(), col.getColumn());
        }
        
        // Remove all conditions referencing this table
        queryModel.getWhereConditions().removeIf(condition -> 
            condition.getTable().equals(tableName));
        
        // Remove all joins involving this table
        queryModel.getJoins().removeIf(join -> 
            join.getLeftTable().equals(tableName) || join.getRightTable().equals(tableName));
    }
    
    /**
     * Get table metadata
     * @param tableName Table name
     * @return CompletableFuture with TableMetadata
     */
    public CompletableFuture<TableMetadata> getTableMetadata(String tableName) {
        // First check cache
        if (tableMetadataCache.containsKey(tableName)) {
            return CompletableFuture.completedFuture(tableMetadataCache.get(tableName));
        }
        
        // Load from server
        return connectionManager.getColumns(queryModel.getDatabase(), tableName)
            .thenApply(result -> {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> columnsData = (List<Map<String, Object>>) result.get("columns");
                
                // Create table metadata
                TableMetadata metadata = TableMetadata.fromColumnsData(tableName, columnsData);
                
                // Cache it
                tableMetadataCache.put(tableName, metadata);
                
                return metadata;
            });
    }
    
    /**
     * Add a WHERE condition
     * @param condition ConditionModel to add
     */
    public void addWhereCondition(ConditionModel condition) {
        queryModel.addWhereCondition(condition);
    }
    
    /**
     * Remove a WHERE condition
     * @param condition ConditionModel to remove
     */
    public void removeWhereCondition(ConditionModel condition) {
        queryModel.removeWhereCondition(condition);
    }
    
    /**
     * Build the SQL query
     * @return SQL string
     */
    public String buildQuery() {
        String sql = queryModel.toSql();
        
        // Validate query
        List<String> validationErrors = QueryValidator.validate(queryModel);
        if (!validationErrors.isEmpty()) {
            throw new IllegalStateException("Invalid query: " + String.join(", ", validationErrors));
        }
        
        // Notify listeners
        if (onQueryBuilt != null) {
            onQueryBuilt.accept(sql);
        }
        
        return sql;
    }
    
    /**
     * Execute the current query
     * @return CompletableFuture with query results
     */
    public CompletableFuture<Map<String, Object>> executeQuery() {
        String sql = buildQuery();
        return connectionManager.executeQuery(sql);
    }
    
    /**
     * Set handler for when a query is built
     * @param handler Consumer that receives the SQL string
     */
    public void setOnQueryBuilt(Consumer<String> handler) {
        this.onQueryBuilt = handler;
    }
    
    /**
     * Reset the query builder to initial state
     */
    public void reset() {
        queryModel.clear();
    }
}