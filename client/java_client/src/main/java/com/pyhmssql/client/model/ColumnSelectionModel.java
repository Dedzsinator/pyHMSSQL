package model;

import java.util.Objects;

/**
 * Model class representing a column selection in a query
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
        
        public static AggregateFunction fromString(String value) {
            if (value == null || value.isEmpty()) {
                return NONE;
            }
            
            for (AggregateFunction func : values()) {
                if (func.getSql().equalsIgnoreCase(value)) {
                    return func;
                }
            }
            return NONE;
        }
        
        @Override
        public String toString() {
            return sql;
        }
    }
    
    private final String table;
    private final String column;
    private String alias;
    private AggregateFunction aggregate;
    private boolean selected;
    
    public ColumnSelectionModel(String table, String column) {
        this(table, column, "", AggregateFunction.NONE, true);
    }
    
    public ColumnSelectionModel(String table, String column, String alias, 
                                AggregateFunction aggregate, boolean selected) {
        this.table = table;
        this.column = column;
        this.alias = alias;
        this.aggregate = aggregate;
        this.selected = selected;
    }
    
    public String getTable() {
        return table;
    }
    
    public String getColumn() {
        return column;
    }
    
    public String getAlias() {
        return alias;
    }
    
    public void setAlias(String alias) {
        this.alias = alias != null ? alias : "";
    }
    
    public AggregateFunction getAggregate() {
        return aggregate;
    }
    
    public void setAggregate(AggregateFunction aggregate) {
        this.aggregate = aggregate;
    }
    
    public void setAggregate(String aggregateStr) {
        this.aggregate = AggregateFunction.fromString(aggregateStr);
    }
    
    public boolean isSelected() {
        return selected;
    }
    
    public void setSelected(boolean selected) {
        this.selected = selected;
    }
    
    /**
     * Generates the SQL column expression
     * @return SQL representation of this column selection
     */
    public String toSql() {
        StringBuilder sql = new StringBuilder();
        
        // Add aggregate function if present
        if (aggregate != AggregateFunction.NONE) {
            sql.append(aggregate.getSql()).append("(");
        }
        
        // Add table and column
        sql.append(table).append(".").append(column);
        
        // Close aggregate function if present
        if (aggregate != AggregateFunction.NONE) {
            sql.append(")");
        }
        
        // Add alias if present
        if (alias != null && !alias.isEmpty()) {
            sql.append(" AS ").append(alias);
        }
        
        return sql.toString();
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnSelectionModel that = (ColumnSelectionModel) o;
        return Objects.equals(table, that.table) && 
               Objects.equals(column, that.column);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(table, column);
    }
    
    @Override
    public String toString() {
        return (aggregate != AggregateFunction.NONE ? aggregate + "(" : "") +
               table + "." + column +
               (aggregate != AggregateFunction.NONE ? ")" : "") +
               (alias != null && !alias.isEmpty() ? " AS " + alias : "");
    }
}