package com.pyhmssql.client.model;

/**
 * Model representing a column selection in a query
 */
public class ColumnSelectionModel {
    public enum AggregateFunction {
        NONE(""),
        COUNT("COUNT"),
        SUM("SUM"),
        AVG("AVG"),
        MIN("MIN"),
        MAX("MAX");

        private final String sql;

        AggregateFunction(String sql) {
            this.sql = sql;
        }

        public String getSql() {
            return sql;
        }
    }

    private String table;
    private String column;
    private String alias;
    private boolean selected;
    private String insertValue;
    private String updateValue;
    private AggregateFunction aggregateFunction;

    public ColumnSelectionModel(String table, String column) {
        this.table = table;
        this.column = column;
        this.selected = true;
        this.alias = "";
        this.insertValue = "";
        this.updateValue = "";
        this.aggregateFunction = AggregateFunction.NONE;
    }

    // Additional constructor to match usage in controllers
    public ColumnSelectionModel(String table, String column, String alias, AggregateFunction aggregateFunction,
            boolean selected) {
        this.table = table;
        this.column = column;
        this.alias = alias != null ? alias : "";
        this.selected = selected;
        this.insertValue = "";
        this.updateValue = "";
        this.aggregateFunction = aggregateFunction != null ? aggregateFunction : AggregateFunction.NONE;
    }

    // Getters and setters
    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public boolean isSelected() {
        return selected;
    }

    public void setSelected(boolean selected) {
        this.selected = selected;
    }

    public String getInsertValue() {
        return insertValue;
    }

    public void setInsertValue(String insertValue) {
        this.insertValue = insertValue;
    }

    public String getUpdateValue() {
        return updateValue;
    }

    public void setUpdateValue(String updateValue) {
        this.updateValue = updateValue;
    }

    public AggregateFunction getAggregateFunction() {
        return aggregateFunction;
    }

    public void setAggregateFunction(AggregateFunction aggregateFunction) {
        this.aggregateFunction = aggregateFunction;
    }

    // Add setAggregate method for backward compatibility
    public void setAggregate(AggregateFunction aggregateFunction) {
        this.aggregateFunction = aggregateFunction;
    }

    // String version for convenience
    public void setAggregateFunction(String aggregateFunction) {
        if (aggregateFunction == null || aggregateFunction.isEmpty()) {
            this.aggregateFunction = AggregateFunction.NONE;
        } else {
            try {
                this.aggregateFunction = AggregateFunction.valueOf(aggregateFunction.toUpperCase());
            } catch (IllegalArgumentException e) {
                this.aggregateFunction = AggregateFunction.NONE;
            }
        }
    }

    // Legacy getValue method for backward compatibility
    public String getValue() {
        return insertValue;
    }

    public void setValue(String value) {
        this.insertValue = value;
    }

    /**
     * Generate SQL representation of this column selection
     * 
     * @return SQL string
     */
    public String toSql() {
        StringBuilder sql = new StringBuilder();

        if (aggregateFunction != AggregateFunction.NONE) {
            sql.append(aggregateFunction.getSql()).append("(");
        }

        sql.append(table).append(".").append(column);

        if (aggregateFunction != AggregateFunction.NONE) {
            sql.append(")");
        }

        if (alias != null && !alias.isEmpty()) {
            sql.append(" AS ").append(alias);
        }

        return sql.toString();
    }

    @Override
    public String toString() {
        return table + "." + column + (alias.isEmpty() ? "" : " AS " + alias);
    }
}