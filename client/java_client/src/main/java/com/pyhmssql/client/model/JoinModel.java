package com.pyhmssql.client.model;

/**
 * Model representing a JOIN operation in a query
 */
public class JoinModel {
    public enum JoinType {
        INNER("INNER JOIN"),
        LEFT("LEFT JOIN"),
        RIGHT("RIGHT JOIN"),
        FULL("FULL JOIN"),
        CROSS("CROSS JOIN");

        private final String sql;

        JoinType(String sql) {
            this.sql = sql;
        }

        public String getSql() {
            return sql;
        }

        public static JoinType fromString(String joinTypeStr) {
            if (joinTypeStr == null || joinTypeStr.isEmpty()) {
                return INNER;
            }

            String normalized = joinTypeStr.trim().toUpperCase().replace(" ", "_");

            try {
                return JoinType.valueOf(normalized);
            } catch (IllegalArgumentException e) {
                // If exact match fails, try partial matches
                if (normalized.contains("LEFT"))
                    return LEFT;
                if (normalized.contains("RIGHT"))
                    return RIGHT;
                if (normalized.contains("FULL"))
                    return FULL;
                if (normalized.contains("CROSS"))
                    return CROSS;
                if (normalized.contains("INNER"))
                    return INNER;

                // Default to INNER JOIN
                return INNER;
            }
        }
    }

    private JoinType joinType;
    private String leftTable;
    private String leftColumn;
    private String rightTable;
    private String rightColumn;

    public JoinModel(JoinType joinType, String leftTable, String leftColumn,
            String rightTable, String rightColumn) {
        this.joinType = joinType;
        this.leftTable = leftTable;
        this.leftColumn = leftColumn;
        this.rightTable = rightTable;
        this.rightColumn = rightColumn;
    }

    // String constructor for backward compatibility
    public JoinModel(String joinType, String leftTable, String leftColumn,
            String rightTable, String rightColumn) {
        this.joinType = JoinType.valueOf(joinType.replace(" ", "_").toUpperCase());
        this.leftTable = leftTable;
        this.leftColumn = leftColumn;
        this.rightTable = rightTable;
        this.rightColumn = rightColumn;
    }

    // Getters and setters
    public JoinType getJoinType() {
        return joinType;
    }

    public void setJoinType(JoinType joinType) {
        this.joinType = joinType;
    }

    public String getLeftTable() {
        return leftTable;
    }

    public void setLeftTable(String leftTable) {
        this.leftTable = leftTable;
    }

    public String getLeftColumn() {
        return leftColumn;
    }

    public void setLeftColumn(String leftColumn) {
        this.leftColumn = leftColumn;
    }

    public String getRightTable() {
        return rightTable;
    }

    public void setRightTable(String rightTable) {
        this.rightTable = rightTable;
    }

    public String getRightColumn() {
        return rightColumn;
    }

    public void setRightColumn(String rightColumn) {
        this.rightColumn = rightColumn;
    }

    /**
     * Generate SQL representation of this join
     * 
     * @return SQL string
     */
    public String toSql() {
        return joinType.getSql() + " " + rightTable + " ON " +
                leftTable + "." + leftColumn + " = " +
                rightTable + "." + rightColumn;
    }

    @Override
    public String toString() {
        return toSql();
    }
}