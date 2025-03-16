package model;

import java.util.Objects;

/**
 * Model class representing a condition in a WHERE clause
 */
public class ConditionModel {
    public enum Operator {
        EQUALS("="),
        NOT_EQUALS("<>"),
        GREATER_THAN(">"),
        LESS_THAN("<"),
        GREATER_THAN_EQUALS(">="),
        LESS_THAN_EQUALS("<="),
        LIKE("LIKE"),
        IN("IN"),
        NOT_IN("NOT IN"),
        IS_NULL("IS NULL"),
        IS_NOT_NULL("IS NOT NULL");
        
        private final String sql;
        
        Operator(String sql) {
            this.sql = sql;
        }
        
        public String getSql() {
            return sql;
        }
        
        public static Operator fromString(String value) {
            if (value == null || value.isEmpty()) {
                return EQUALS;
            }
            
            for (Operator op : values()) {
                if (op.getSql().equalsIgnoreCase(value)) {
                    return op;
                }
            }
            return EQUALS;
        }
        
        /**
         * Checks if this operator requires a value
         */
        public boolean requiresValue() {
            return this != IS_NULL && this != IS_NOT_NULL;
        }
        
        @Override
        public String toString() {
            return sql;
        }
    }
    
    private String table;
    private String column;
    private Operator operator;
    private String value;
    
    public ConditionModel() {
        this("", "", Operator.EQUALS, "");
    }
    
    public ConditionModel(String table, String column, Operator operator, String value) {
        this.table = table;
        this.column = column;
        this.operator = operator;
        this.value = value;
    }
    
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
    
    public Operator getOperator() {
        return operator;
    }
    
    public void setOperator(Operator operator) {
        this.operator = operator;
    }
    
    public void setOperator(String operatorStr) {
        this.operator = Operator.fromString(operatorStr);
    }
    
    public String getValue() {
        return value;
    }
    
    public void setValue(String value) {
        this.value = value;
    }
    
    /**
     * Determines if a string value should be quoted in SQL
     */
    private boolean shouldQuoteValue(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        
        // Don't quote if it's a subquery or parameter
        if (value.startsWith("(") || value.startsWith("@")) {
            return false;
        }
        
        // Don't quote numbers
        try {
            Double.parseDouble(value);
            return false;
        } catch (NumberFormatException e) {
            return true;
        }
    }
    
    /**
     * Generates the SQL condition
     * @return SQL representation of this condition
     */
    public String toSql() {
        StringBuilder sql = new StringBuilder();
        
        sql.append(table).append(".").append(column)
           .append(" ").append(operator.getSql());
        
        if (operator.requiresValue()) {
            sql.append(" ");
            
            if (shouldQuoteValue(value)) {
                sql.append("'").append(value).append("'");
            } else {
                sql.append(value);
            }
        }
        
        return sql.toString();
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConditionModel that = (ConditionModel) o;
        return Objects.equals(table, that.table) && 
               Objects.equals(column, that.column) && 
               operator == that.operator && 
               Objects.equals(value, that.value);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(table, column, operator, value);
    }
    
    @Override
    public String toString() {
        return table + "." + column + " " + operator + 
               (operator.requiresValue() ? " " + value : "");
    }
}