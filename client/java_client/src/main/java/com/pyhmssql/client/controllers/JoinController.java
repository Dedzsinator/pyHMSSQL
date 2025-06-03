package com.pyhmssql.client.controllers;

import com.pyhmssql.client.model.JoinModel;
import com.pyhmssql.client.model.JoinModel.JoinType;
import java.util.List;
import java.util.ArrayList;

/**
 * Controller for managing JOIN operations in visual query builder
 */
public class JoinController {
    private List<JoinModel> joins;

    public JoinController() {
        this.joins = new ArrayList<>();
    }

    public void addJoin(String joinTypeStr, String leftTable, String leftColumn,
            String rightTable, String rightColumn) {
        JoinType joinType = JoinType.fromString(joinTypeStr);
        JoinModel join = new JoinModel(joinType, leftTable, leftColumn, rightTable, rightColumn);
        joins.add(join);
    }

    public void removeJoin(JoinModel join) {
        joins.remove(join);
    }

    public List<JoinModel> getJoins() {
        return new ArrayList<>(joins);
    }

    public void clearJoins() {
        joins.clear();
    }

    public String generateJoinSQL() {
        StringBuilder sql = new StringBuilder();
        for (JoinModel join : joins) {
            if (sql.length() > 0) {
                sql.append(" ");
            }
            sql.append(join.toSql());
        }
        return sql.toString();
    }

    public JoinModel createJoin(String joinTypeStr, String leftTable, String leftColumn,
            String rightTable, String rightColumn) {
        JoinType joinType = JoinType.fromString(joinTypeStr);
        return new JoinModel(joinType, leftTable, leftColumn, rightTable, rightColumn);
    }

    public boolean hasJoins() {
        return !joins.isEmpty();
    }

    public int getJoinCount() {
        return joins.size();
    }
}