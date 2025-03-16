package model;

import java.util.Objects;

/**
 * Model class representing a join between two tables
 */
public class JoinModel {
    public enum JoinType {
        INNER("INNER JOIN"),
        LEFT("LEFT JOIN"),
        RIGHT("RIGHT JOIN"),
        FULL("FULL JOIN");
        
        private final String sql;
        
        JoinType(String sql) {
            this.sql = sql;
        }
        
        public String getSql() {
            return sql;
        }
        
        @Override
        public String toString() {
            return sql;
        }
    }
    
    private JoinType type;
    private String leftTable;
    private String leftColumn;
    private String rightTable;
    private String rightColumn;
    
    public JoinModel(JoinType type, String leftTable, String leftColumn, 
                     String rightTable, String rightColumn) {
        this.type = type;
        this.leftTable = leftTable;
        this.leftColumn = leftColumn;
        this.rightTable = rightTable;
        this.rightColumn = rightColumn;
    }
    
    public JoinType getType() {
        return type;
    }
    
    public void setType(JoinType type) {
        this.type = type;
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
     * Generates the SQL JOIN clause
     * @return SQL representation of this JOIN
     */
    public String toSql() {
        return type.getSql() + " " + rightTable + " ON " +
               leftTable + "." + leftColumn + " = " +
               rightTable + "." + rightColumn;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JoinModel joinModel = (JoinModel) o;
        return Objects.equals(leftTable, joinModel.leftTable) &&
               Objects.equals(leftColumn, joinModel.leftColumn) &&
               Objects.equals(rightTable, joinModel.rightTable) &&
               Objects.equals(rightColumn, joinModel.rightColumn);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(leftTable, leftColumn, rightTable, rightColumn);
    }
    
    @Override
    public String toString() {
        return leftTable + "." + leftColumn + " " + 
               type.toString() + " " + 
               rightTable + "." + rightColumn;
    }

    public static JoinType fromString(String type) {
        switch (type.toUpperCase()) {
            case "INNER JOIN":
                return JoinType.INNER;
            case "LEFT JOIN":
                return JoinType.LEFT;
            case "RIGHT JOIN":
                return JoinType.RIGHT;
            case "FULL JOIN":
                return JoinType.FULL;
            default:
                throw new IllegalArgumentException("Invalid join type: " + type);
        }
    }
}