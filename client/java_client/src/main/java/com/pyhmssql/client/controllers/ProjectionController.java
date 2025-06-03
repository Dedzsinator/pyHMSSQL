package com.pyhmssql.client.controllers;

import com.pyhmssql.client.model.ColumnSelectionModel;
import com.pyhmssql.client.model.ColumnSelectionModel.AggregateFunction;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * Controller for managing column projections (SELECT clause) in visual query
 * builder
 */
public class ProjectionController {
    private List<ColumnSelectionModel> selectedColumns;
    private boolean selectAll;

    public ProjectionController() {
        this.selectedColumns = new ArrayList<>();
        this.selectAll = false;
    }

    public void addColumn(String table, String column) {
        addColumn(table, column, "", AggregateFunction.NONE, true);
    }

    public void addColumn(String table, String column, String alias) {
        addColumn(table, column, alias, AggregateFunction.NONE, true);
    }

    public void addColumn(String table, String column, String alias,
            AggregateFunction aggregateFunction, boolean selected) {
        // Check if column already exists
        for (ColumnSelectionModel col : selectedColumns) {
            if (col.getTable().equals(table) && col.getColumn().equals(column)) {
                return; // Already exists
            }
        }

        ColumnSelectionModel columnSelection = new ColumnSelectionModel(
                table, column, alias, aggregateFunction, selected);
        selectedColumns.add(columnSelection);
    }

    public void removeColumn(ColumnSelectionModel column) {
        selectedColumns.remove(column);
    }

    public void removeColumn(String table, String column) {
        selectedColumns.removeIf(col -> col.getTable().equals(table) && col.getColumn().equals(column));
    }

    public void updateColumnAggregate(String table, String column, AggregateFunction aggregate) {
        for (ColumnSelectionModel col : selectedColumns) {
            if (col.getTable().equals(table) && col.getColumn().equals(column)) {
                col.setAggregate(aggregate);
                break;
            }
        }
    }

    public void updateColumnAlias(String table, String column, String alias) {
        for (ColumnSelectionModel col : selectedColumns) {
            if (col.getTable().equals(table) && col.getColumn().equals(column)) {
                col.setAlias(alias);
                break;
            }
        }
    }

    public List<ColumnSelectionModel> getSelectedColumns() {
        return new ArrayList<>(selectedColumns);
    }

    public List<ColumnSelectionModel> getActiveColumns() {
        return selectedColumns.stream()
                .filter(ColumnSelectionModel::isSelected)
                .collect(Collectors.toList());
    }

    public void clearColumns() {
        selectedColumns.clear();
    }

    public boolean isSelectAll() {
        return selectAll;
    }

    public void setSelectAll(boolean selectAll) {
        this.selectAll = selectAll;
    }

    public String generateProjectionSQL() {
        if (selectAll || selectedColumns.isEmpty()) {
            return "*";
        }

        List<String> columnSqls = selectedColumns.stream()
                .filter(ColumnSelectionModel::isSelected)
                .map(ColumnSelectionModel::toSql)
                .collect(Collectors.toList());

        if (columnSqls.isEmpty()) {
            return "*";
        }

        return String.join(", ", columnSqls);
    }

    public boolean hasColumns() {
        return !selectedColumns.isEmpty();
    }

    public int getColumnCount() {
        return selectedColumns.size();
    }

    public int getActiveColumnCount() {
        return (int) selectedColumns.stream()
                .filter(ColumnSelectionModel::isSelected)
                .count();
    }

    public void toggleColumnSelection(String table, String column) {
        for (ColumnSelectionModel col : selectedColumns) {
            if (col.getTable().equals(table) && col.getColumn().equals(column)) {
                col.setSelected(!col.isSelected());
                break;
            }
        }
    }

    public void selectAllColumns(boolean selected) {
        for (ColumnSelectionModel col : selectedColumns) {
            col.setSelected(selected);
        }
    }

    public void removeColumnsForTable(String tableName) {
        selectedColumns.removeIf(col -> col.getTable().equals(tableName));
    }

    public List<ColumnSelectionModel> getColumnsForTable(String tableName) {
        return selectedColumns.stream()
                .filter(col -> col.getTable().equals(tableName))
                .collect(Collectors.toList());
    }
}