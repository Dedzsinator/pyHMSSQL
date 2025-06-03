package com.pyhmssql.client.main;

import com.pyhmssql.client.model.QueryModel;
import com.pyhmssql.client.model.QueryModel.QueryType;

/**
 * Utility class for building SQL queries programmatically
 */
public class QueryBuilder {
    private final QueryModel model;

    public QueryBuilder() {
        this.model = new QueryModel();
    }

    public QueryBuilder(QueryModel model) {
        this.model = model;
    }

    /**
     * Set the query type
     */
    public QueryBuilder queryType(QueryType type) {
        model.setType(type);
        return this;
    }

    /**
     * Set the query name
     */
    public QueryBuilder name(String name) {
        model.setName(name);
        return this;
    }

    /**
     * Set DISTINCT flag
     */
    public QueryBuilder distinct(boolean distinct) {
        model.setDistinct(distinct);
        return this;
    }

    /**
     * Set the database context
     */
    public QueryBuilder database(String database) {
        model.setDatabase(database);
        return this;
    }

    /**
     * Add a table to the query
     */
    public QueryBuilder addTable(String table) {
        model.addTable(table);
        return this;
    }

    /**
     * Set ORDER BY clause
     */
    public QueryBuilder orderBy(String column, boolean ascending) {
        model.setOrderByColumn(column);
        model.setOrderAscending(ascending);
        return this;
    }

    /**
     * Set LIMIT clause
     */
    public QueryBuilder limit(int limit) {
        model.setLimit(limit);
        return this;
    }

    /**
     * Build the SQL query
     */
    public String build() {
        return model.toSql();
    }

    /**
     * Get the query model
     */
    public QueryModel getModel() {
        return model;
    }

    /**
     * Clear the query
     */
    public QueryBuilder clear() {
        model.clear();
        return this;
    }

    /**
     * Create a copy of this builder
     */
    public QueryBuilder copy() {
        return new QueryBuilder(model.copy());
    }
}