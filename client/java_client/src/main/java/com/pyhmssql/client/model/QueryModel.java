package model;

import java.util.*;

/**
 * Model class representing a SQL query with all its components
 */
public class QueryModel {
    public enum QueryType {
        SELECT, INSERT, UPDATE, DELETE
    }
    
    private String name;
    private QueryType type;
    private boolean distinct;
    private String database; // Database context for this query
    private List<String> tables;
    private List<JoinModel> joins;
    private List<ColumnSelectionModel> columns;
    private List<ConditionModel> whereConditions;
    private String orderByColumn;
    private boolean orderAscending;
    private int limit;
    
    public QueryModel() {
        // Default values
        this.name = "New Query";
        this.type = QueryType.SELECT;
        this.distinct = false;
        this.database = null;
        this.tables = new ArrayList<>();
        this.joins = new ArrayList<>();
        this.columns = new ArrayList<>();
        this.whereConditions = new ArrayList<>();
        this.orderByColumn = null;
        this.orderAscending = true;
        this.limit = 0;
    }

    
    // Getters and setters
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public QueryType getType() {
        return type;
    }
    
    public void setType(QueryType type) {
        this.type = type;
    }
    
    public boolean isDistinct() {
        return distinct;
    }
    
    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }
    
    /**
     * Gets the current database context for this query
     * @return The database name or null if not set
     */
    public String getDatabase() {
        return database;
    }
    
    /**
     * Sets the database context for this query
     * @param database The database name
     */
    public void setDatabase(String database) {
        this.database = database;
    }
    
    public List<String> getTables() {
        return tables;
    }
    
    public void addTable(String table) {
        if (!tables.contains(table)) {
            tables.add(table);
        }
    }
    
    public void removeTable(String table) {
        tables.remove(table);
        
        // Also remove related elements
        joins.removeIf(join -> join.getLeftTable().equals(table) || join.getRightTable().equals(table));
        columns.removeIf(col -> col.getTable().equals(table));
        whereConditions.removeIf(cond -> cond.getTable().equals(table));
        
        // Reset order by if it was on this table
        if (orderByColumn != null && orderByColumn.startsWith(table + ".")) {
            orderByColumn = null;
        }
    }
    
    public List<JoinModel> getJoins() {
        return joins;
    }
    
    public void addJoin(JoinModel join) {
        joins.add(join);
    }
    
    public void removeJoin(JoinModel join) {
        joins.remove(join);
    }
    
    public List<ColumnSelectionModel> getColumns() {
        return columns;
    }
    
    public void addColumn(ColumnSelectionModel column) {
        // Check if column already exists
        for (ColumnSelectionModel col : columns) {
            if (col.getTable().equals(column.getTable()) && 
                col.getColumn().equals(column.getColumn())) {
                return; // Already exists
            }
        }
        columns.add(column);
    }
    
    public void removeColumn(ColumnSelectionModel column) {
        columns.remove(column);
        
        // Reset order by if it was this column
        if (orderByColumn != null && 
            orderByColumn.equals(column.getTable() + "." + column.getColumn())) {
            orderByColumn = null;
        }
    }
    
    public List<ConditionModel> getWhereConditions() {
        return whereConditions;
    }
    
    public void addWhereCondition(ConditionModel condition) {
        whereConditions.add(condition);
    }
    
    public void removeWhereCondition(ConditionModel condition) {
        whereConditions.remove(condition);
    }
    
    public String getOrderByColumn() {
        return orderByColumn;
    }
    
    public void setOrderByColumn(String orderByColumn) {
        this.orderByColumn = orderByColumn;
    }
    
    public boolean isOrderAscending() {
        return orderAscending;
    }
    
    public void setOrderAscending(boolean orderAscending) {
        this.orderAscending = orderAscending;
    }
    
    public int getLimit() {
        return limit;
    }
    
    public void setLimit(int limit) {
        this.limit = limit;
    }
    
    /**
     * Clear all query components
     */
    public void clear() {
        tables.clear();
        joins.clear();
        columns.clear();
        whereConditions.clear();
        orderByColumn = null;
        orderAscending = true;
        limit = 0;
    }
}