package com.pyhmssql.client.model;

/**
 * Model representing a WHERE condition in a query
 */
public class ConditionModel {
    private String table;
    private String column;
    private String operator;
    private String value;
    private String logicalOperator; // AND, OR

    public ConditionModel() {
        this.operator = "=";
        this.logicalOperator = "AND";
    }

    public ConditionModel(String table, String column, String operator, String value) {
        this.table = table;
        this.column = column;
        this.operator = operator;
        this.value = value;
        this.logicalOperator = "AND";
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

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getLogicalOperator() {
        return logicalOperator;
    }

    public void setLogicalOperator(String logicalOperator) {
        this.logicalOperator = logicalOperator;
    }

    /**
     * Generate SQL representation of this condition
     * 
     * @return SQL string
     */
    public String toSql() {
        StringBuilder sql = new StringBuilder();

        sql.append(table).append(".").append(column).append(" ").append(operator);

        if (!operator.equals("IS NULL") && !operator.equals("IS NOT NULL")) {
            sql.append(" ");

            // Quote string values if they don't start with a special character
            if (value != null && !value.isEmpty() &&
                    !value.startsWith("(") && !value.startsWith("@") &&
                    !isNumeric(value)) {
                sql.append("'").append(value).append("'");
            } else {
                sql.append(value);
            }
        }

        return sql.toString();
    }

    private boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    @Override
    public String toString() {
        return toSql();
    }
}