package com.pyhmssql.client.views;

import com.pyhmssql.client.main.ConnectionManager;
import com.pyhmssql.client.model.TableMetadata;
import com.pyhmssql.client.model.ColumnMetadata;

import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.input.TransferMode;
import javafx.scene.input.Dragboard;
import javafx.scene.shape.Line;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.application.Platform;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Visual Query Builder component that allows users to build SQL queries
 * graphically
 */
public class VisualQueryBuilder extends BorderPane {
    private final ConnectionManager connectionManager;
    private final Consumer<String> onQueryBuilt;
    private final Map<String, TableView<Map<String, String>>> tableNodes = new HashMap<>();
    private final List<JoinLine> joinLines = new ArrayList<>();
    private final List<JoinCondition> joinConditions = new ArrayList<>();
    private final List<WhereCondition> whereConditions = new ArrayList<>();
    private final List<ColumnSelection> selectedColumns = new ArrayList<>();

    private Pane diagramPane;
    private String currentDatabase;
    private VBox tablesPanel;
    private HBox conditionsPanel;
    private VBox projectionsPanel;
    private TextField queryNameField;
    private ComboBox<String> queryTypeComboBox;
    private ComboBox<String> orderByComboBox;
    private ComboBox<String> sortOrderComboBox;
    private Spinner<Integer> limitSpinner;
    private CheckBox distinctCheckBox;
    private ComboBox<String> databaseComboBox;

    public VisualQueryBuilder(ConnectionManager connectionManager, Consumer<String> onQueryBuilt) {
        this.connectionManager = connectionManager;
        this.onQueryBuilt = onQueryBuilt;
        setupUI();
    }

    private void setupUI() {
        setPadding(new Insets(10));
        setupQuerySettingsPanel();
        setupTablesPanel();
        setupDiagramArea();
        setupQueryComponentsPanel();
        setupActionPanel();
    }

    private void setupQuerySettingsPanel() {
        HBox settingsPanel = new HBox(10);
        settingsPanel.setPadding(new Insets(5));
        settingsPanel.setAlignment(Pos.CENTER_LEFT);

        queryNameField = new TextField();
        queryNameField.setPromptText("MyQuery");
        queryNameField.setPrefWidth(150);

        queryTypeComboBox = new ComboBox<>(FXCollections.observableArrayList(
                "SELECT", "INSERT", "UPDATE", "DELETE"));
        queryTypeComboBox.setValue("SELECT");
        queryTypeComboBox.setOnAction(e -> updateUIForQueryType());

        distinctCheckBox = new CheckBox("DISTINCT");

        orderByComboBox = new ComboBox<>();
        orderByComboBox.setPromptText("Select column");

        sortOrderComboBox = new ComboBox<>(FXCollections.observableArrayList("ASC", "DESC"));
        sortOrderComboBox.setValue("ASC");

        limitSpinner = new Spinner<>(0, 10000, 100, 10);
        limitSpinner.setEditable(true);
        limitSpinner.setPrefWidth(100);

        databaseComboBox = new ComboBox<>();
        databaseComboBox.setPromptText("Select Database");
        databaseComboBox.setPrefWidth(150);

        loadDatabases();

        databaseComboBox.setOnAction(e -> {
            currentDatabase = databaseComboBox.getValue();
            loadTablesForDatabase();
            clearDiagramArea();
        });

        settingsPanel.getChildren().addAll(
                new Label("Query Name:"), queryNameField,
                new Label("Type:"), queryTypeComboBox,
                distinctCheckBox,
                new Label("ORDER BY:"), orderByComboBox, sortOrderComboBox,
                new Label("LIMIT:"), limitSpinner,
                new Label("Database:"), databaseComboBox);

        setTop(settingsPanel);
    }

    private void loadDatabases() {
        connectionManager.getDatabases().thenAccept(result -> {
            try {
                if (result.containsKey("error")) {
                    System.err.println("Error loading databases: " + result.get("error"));
                    return;
                }

                List<String> databases = new ArrayList<>();

                if (result.containsKey("databases")) {
                    Object dbObj = result.get("databases");
                    if (dbObj instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<Object> dbList = (List<Object>) dbObj;
                        for (Object db : dbList) {
                            databases.add(db.toString());
                        }
                    }
                } else if (result.containsKey("rows")) {
                    Object rowsObj = result.get("rows");
                    if (rowsObj instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<Object> rows = (List<Object>) rowsObj;
                        for (Object rowObj : rows) {
                            if (rowObj instanceof List) {
                                @SuppressWarnings("unchecked")
                                List<Object> row = (List<Object>) rowObj;
                                if (!row.isEmpty()) {
                                    databases.add(row.get(0).toString());
                                }
                            } else {
                                databases.add(rowObj.toString());
                            }
                        }
                    }
                }

                Platform.runLater(() -> {
                    databaseComboBox.getItems().clear();
                    databaseComboBox.getItems().addAll(databases);
                });
            } catch (Exception e) {
                System.err.println("Error processing database list: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }

    private void setupTablesPanel() {
        VBox leftPanel = new VBox(10);
        leftPanel.setPadding(new Insets(5));
        leftPanel.setMinWidth(200);

        Label tablesLabel = new Label("Available Tables");
        tablesLabel.setFont(Font.font("System", FontWeight.BOLD, 14));

        tablesPanel = new VBox(5);
        tablesPanel.setPadding(new Insets(5));

        ScrollPane scrollPane = new ScrollPane(tablesPanel);
        scrollPane.setFitToWidth(true);
        scrollPane.setPrefHeight(600);

        leftPanel.getChildren().addAll(tablesLabel, scrollPane);
        setLeft(leftPanel);
    }

    private void setupDiagramArea() {
        diagramPane = new Pane();
        diagramPane.setStyle("-fx-background-color: #f5f5f5; -fx-border-color: #cccccc;");

        diagramPane.setOnDragOver(event -> {
            if (event.getGestureSource() != diagramPane && event.getDragboard().hasString()) {
                event.acceptTransferModes(TransferMode.COPY);
            }
            event.consume();
        });

        diagramPane.setOnDragDropped(event -> {
            Dragboard db = event.getDragboard();
            boolean success = false;
            if (db.hasString()) {
                String tableName = db.getString();
                addTableToDiagram(tableName, event.getX(), event.getY());
                success = true;
            }
            event.setDropCompleted(success);
            event.consume();
        });

        ScrollPane scrollPane = new ScrollPane(diagramPane);
        scrollPane.setPrefSize(800, 600);
        scrollPane.setFitToWidth(true);
        scrollPane.setFitToHeight(true);

        setCenter(scrollPane);
    }

    private void setupQueryComponentsPanel() {
        VBox rightPanel = new VBox(15);
        rightPanel.setPadding(new Insets(10));
        rightPanel.setPrefWidth(300);

        Label projectionsLabel = new Label("SELECT Columns");
        projectionsLabel.setFont(Font.font("System", FontWeight.BOLD, 14));

        projectionsPanel = new VBox(5);
        ScrollPane projectionsScrollPane = new ScrollPane(projectionsPanel);
        projectionsScrollPane.setFitToWidth(true);
        projectionsScrollPane.setPrefHeight(200);

        Label conditionsLabel = new Label("WHERE Conditions");
        conditionsLabel.setFont(Font.font("System", FontWeight.BOLD, 14));

        conditionsPanel = new HBox(5);
        conditionsPanel.setPadding(new Insets(5));

        Button addConditionButton = new Button("Add Condition");
        addConditionButton.setOnAction(e -> addWhereCondition());

        ScrollPane conditionsScrollPane = new ScrollPane(conditionsPanel);
        conditionsScrollPane.setFitToWidth(true);
        conditionsScrollPane.setPrefHeight(150);

        rightPanel.getChildren().addAll(
                projectionsLabel, projectionsScrollPane,
                conditionsLabel, addConditionButton, conditionsScrollPane);

        setRight(rightPanel);
    }

    private void setupActionPanel() {
        VBox bottomPanel = new VBox(10);
        bottomPanel.setPadding(new Insets(10));

        Label sqlLabel = new Label("Generated SQL");
        TextArea sqlTextArea = new TextArea();
        sqlTextArea.setEditable(false);
        sqlTextArea.setPrefHeight(100);

        HBox buttonsPanel = new HBox(10);
        buttonsPanel.setAlignment(Pos.CENTER_RIGHT);

        Button buildButton = new Button("Build Query");
        buildButton.setOnAction(e -> {
            String sql = buildSQLQuery();
            sqlTextArea.setText(sql);
        });

        Button applyButton = new Button("Apply to Editor");
        applyButton.setOnAction(e -> {
            String sql = sqlTextArea.getText();
            if (sql != null && !sql.isEmpty() && onQueryBuilt != null) {
                onQueryBuilt.accept(sql);
            }
        });

        Button clearButton = new Button("Clear All");
        clearButton.setOnAction(e -> {
            clearDiagramArea();
            sqlTextArea.clear();
        });

        buttonsPanel.getChildren().addAll(clearButton, buildButton, applyButton);
        bottomPanel.getChildren().addAll(sqlLabel, sqlTextArea, buttonsPanel);
        setBottom(bottomPanel);
    }

    private void loadTablesForDatabase() {
        if (currentDatabase == null || currentDatabase.isEmpty()) {
            System.err.println("Cannot load tables: No database selected");
            return;
        }

        connectionManager.getTables(currentDatabase).thenAccept(result -> {
            try {
                if (result.containsKey("error")) {
                    String errorMsg = result.get("error").toString();
                    System.err.println("Error loading tables: " + errorMsg);
                    Platform.runLater(() -> {
                        tablesPanel.getChildren().clear();
                        tablesPanel.getChildren().add(new Label("Error: " + errorMsg));
                    });
                    return;
                }

                List<String> tables = new ArrayList<>();

                if (result.containsKey("tables")) {
                    Object tablesObj = result.get("tables");
                    if (tablesObj instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<Object> tablesList = (List<Object>) tablesObj;
                        for (Object table : tablesList) {
                            tables.add(table.toString());
                        }
                    }
                } else if (result.containsKey("rows")) {
                    Object rowsObj = result.get("rows");
                    if (rowsObj instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<Object> rows = (List<Object>) rowsObj;
                        for (Object rowObj : rows) {
                            if (rowObj instanceof List) {
                                @SuppressWarnings("unchecked")
                                List<Object> row = (List<Object>) rowObj;
                                if (!row.isEmpty()) {
                                    tables.add(row.get(0).toString());
                                }
                            } else {
                                tables.add(rowObj.toString());
                            }
                        }
                    }
                }

                Platform.runLater(() -> {
                    try {
                        tablesPanel.getChildren().clear();

                        if (tables.isEmpty()) {
                            tablesPanel.getChildren().add(
                                    new Label("No tables found in database " + currentDatabase));
                            return;
                        }

                        for (String tableName : tables) {
                            Label tableLabel = new Label(tableName);
                            tableLabel.setStyle(
                                    "-fx-background-color: #e0e0e0; -fx-padding: 5px; -fx-border-color: #999999;");
                            tableLabel.setPrefWidth(180);

                            tableLabel.setOnDragDetected(event -> {
                                Dragboard db = tableLabel.startDragAndDrop(TransferMode.COPY);
                                javafx.scene.input.ClipboardContent content = new javafx.scene.input.ClipboardContent();
                                content.putString(tableName);
                                db.setContent(content);
                                event.consume();
                            });

                            tablesPanel.getChildren().add(tableLabel);
                        }
                    } catch (Exception e) {
                        System.err.println("UI error when displaying tables: " + e.getMessage());
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                System.err.println("Error processing tables response: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }

    public void addTable(String tableName, double x, double y) {
        addTableToDiagram(tableName, x, y);
    }

    private void addTableToDiagram(String tableName, double x, double y) {
        if (tableNodes.containsKey(tableName)) {
            return;
        }

        getTableMetadata(tableName).thenAccept(metadata -> {
            Platform.runLater(() -> {
                TableView<Map<String, String>> tableView = new TableView<>();
                tableView.setPrefSize(200, 200);

                TableColumn<Map<String, String>, String> nameColumn = new TableColumn<>("Column");
                nameColumn.setCellValueFactory(data -> new javafx.beans.property.SimpleStringProperty(
                        data.getValue().get("name")));

                TableColumn<Map<String, String>, String> typeColumn = new TableColumn<>("Type");
                typeColumn.setCellValueFactory(data -> new javafx.beans.property.SimpleStringProperty(
                        data.getValue().get("type")));

                tableView.getColumns().addAll(nameColumn, typeColumn);

                ObservableList<Map<String, String>> data = FXCollections.observableArrayList();
                for (ColumnMetadata column : metadata.getColumns()) {
                    Map<String, String> row = new HashMap<>();
                    row.put("name", column.getName());
                    row.put("type", column.getType().toString());
                    data.add(row);

                    orderByComboBox.getItems().add(tableName + "." + column.getName());
                }

                tableView.setItems(data);

                Label titleLabel = new Label(tableName);
                titleLabel.setFont(Font.font("System", FontWeight.BOLD, 12));
                titleLabel.setPadding(new Insets(5));
                titleLabel.setStyle("-fx-background-color: #4a6da7; -fx-text-fill: white;");
                titleLabel.setPrefWidth(200);

                VBox tableContainer = new VBox();
                tableContainer.getChildren().addAll(titleLabel, tableView);
                tableContainer.setLayoutX(x);
                tableContainer.setLayoutY(y);
                tableContainer.setStyle("-fx-border-color: #666666; -fx-background-color: white;");

                final Delta dragDelta = new Delta();

                titleLabel.setOnMousePressed(event -> {
                    dragDelta.x = tableContainer.getLayoutX() - event.getSceneX();
                    dragDelta.y = tableContainer.getLayoutY() - event.getSceneY();
                });

                titleLabel.setOnMouseDragged(event -> {
                    tableContainer.setLayoutX(event.getSceneX() + dragDelta.x);
                    tableContainer.setLayoutY(event.getSceneY() + dragDelta.y);
                    updateJoinLines();
                });

                tableView.setRowFactory(tv -> {
                    TableRow<Map<String, String>> row = new TableRow<>();
                    row.setOnMouseClicked(event -> {
                        if (!row.isEmpty()) {
                            String columnName = row.getItem().get("name");
                            addColumnToProjection(tableName, columnName);
                        }
                    });
                    return row;
                });

                tableNodes.put(tableName, tableView);
                diagramPane.getChildren().add(tableContainer);
            });
        });
    }

    private CompletableFuture<TableMetadata> getTableMetadata(String tableName) {
        CompletableFuture<TableMetadata> future = new CompletableFuture<>();

        connectionManager.getColumns(currentDatabase, tableName).thenAccept(result -> {
            List<ColumnMetadata> columns = new ArrayList<>();

            if (result.containsKey("columns")) {
                Object columnsObj = result.get("columns");
                if (columnsObj instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<Object> columnsData = (List<Object>) columnsObj;

                    for (Object columnObj : columnsData) {
                        if (columnObj instanceof Map) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> data = (Map<String, Object>) columnObj;
                            String name = data.get("name") != null ? data.get("name").toString() : "unknown";
                            String type = data.get("type") != null ? data.get("type").toString() : "VARCHAR";
                            boolean primaryKey = Boolean
                                    .parseBoolean(data.getOrDefault("primary_key", false).toString());
                            boolean nullable = Boolean.parseBoolean(data.getOrDefault("nullable", true).toString());

                            columns.add(new ColumnMetadata(name, type, primaryKey, nullable));
                        }
                    }
                }
            }

            TableMetadata metadata = new TableMetadata(tableName, columns);
            future.complete(metadata);
        }).exceptionally(ex -> {
            System.err.println("Exception getting table metadata: " + ex.getMessage());
            future.complete(new TableMetadata(tableName, new ArrayList<>()));
            return null;
        });

        return future;
    }

    private void addColumnToProjection(String tableName, String columnName) {
        for (ColumnSelection col : selectedColumns) {
            if (col.getTable().equals(tableName) && col.getColumn().equals(columnName)) {
                return;
            }
        }

        ColumnSelection columnSelection = new ColumnSelection(tableName, columnName);
        selectedColumns.add(columnSelection);

        Platform.runLater(() -> {
            HBox columnRow = new HBox(5);
            columnRow.setAlignment(Pos.CENTER_LEFT);

            CheckBox selectBox = new CheckBox();
            selectBox.setSelected(true);
            selectBox.setOnAction(e -> columnSelection.setSelected(selectBox.isSelected()));

            Label columnLabel = new Label(tableName + "." + columnName);

            TextField aliasField = new TextField();
            aliasField.setPromptText("Alias");
            aliasField.textProperty().addListener((obs, oldVal, newVal) -> columnSelection.setAlias(newVal));

            Button removeButton = new Button("×");
            removeButton.setOnAction(e -> {
                selectedColumns.remove(columnSelection);
                projectionsPanel.getChildren().remove(columnRow);
            });

            columnRow.getChildren().addAll(selectBox, columnLabel, aliasField, removeButton);
            projectionsPanel.getChildren().add(columnRow);
        });
    }

    private void addWhereCondition() {
        if (tableNodes.isEmpty()) {
            showAlert("No tables added to the query");
            return;
        }

        WhereCondition condition = new WhereCondition();
        whereConditions.add(condition);

        HBox conditionRow = new HBox(5);
        conditionRow.setAlignment(Pos.CENTER_LEFT);

        ComboBox<String> tableCombo = new ComboBox<>();
        tableCombo.getItems().addAll(tableNodes.keySet());
        if (!tableCombo.getItems().isEmpty()) {
            tableCombo.setValue(tableCombo.getItems().get(0));
            condition.setTable(tableCombo.getValue());
        }

        ComboBox<String> columnCombo = new ComboBox<>();
        ComboBox<String> operatorCombo = new ComboBox<>();
        operatorCombo.getItems().addAll("=", "<>", ">", "<", ">=", "<=", "LIKE", "IN", "IS NULL", "IS NOT NULL");
        operatorCombo.setValue("=");

        TextField valueField = new TextField();
        valueField.setPromptText("Value");

        Button removeButton = new Button("×");
        removeButton.setOnAction(e -> {
            whereConditions.remove(condition);
            conditionsPanel.getChildren().remove(conditionRow);
        });

        conditionRow.getChildren().addAll(tableCombo, columnCombo, operatorCombo, valueField, removeButton);
        conditionsPanel.getChildren().add(conditionRow);
    }

    private void updateJoinLines() {
        // Implementation for updating join lines when tables are moved
    }

    private void clearDiagramArea() {
        diagramPane.getChildren().clear();
        tableNodes.clear();
        joinLines.clear();
        joinConditions.clear();
        selectedColumns.clear();
        whereConditions.clear();
        projectionsPanel.getChildren().clear();
        conditionsPanel.getChildren().clear();
        orderByComboBox.getItems().clear();
    }

    private String buildSQLQuery() {
        StringBuilder sql = new StringBuilder();
        String queryType = queryTypeComboBox.getValue();

        switch (queryType) {
            case "SELECT":
                buildSelectQuery(sql);
                break;
            default:
                sql.append("-- Query type not implemented");
        }

        return sql.toString();
    }

    private void buildSelectQuery(StringBuilder sql) {
        sql.append("SELECT ");

        if (distinctCheckBox.isSelected()) {
            sql.append("DISTINCT ");
        }

        if (selectedColumns.isEmpty()) {
            sql.append("*");
        } else {
            boolean first = true;
            for (ColumnSelection col : selectedColumns) {
                if (!col.isSelected())
                    continue;

                if (!first) {
                    sql.append(", ");
                }

                sql.append(col.getTable()).append(".").append(col.getColumn());

                if (!col.getAlias().isEmpty()) {
                    sql.append(" AS ").append(col.getAlias());
                }

                first = false;
            }
        }

        if (!tableNodes.isEmpty()) {
            String baseTable = tableNodes.keySet().iterator().next();
            sql.append("\nFROM ").append(baseTable);
        }

        if (!whereConditions.isEmpty()) {
            sql.append("\nWHERE ");
            boolean first = true;
            for (WhereCondition cond : whereConditions) {
                if (!first) {
                    sql.append(" AND ");
                }

                sql.append(cond.getTable()).append(".").append(cond.getColumn())
                        .append(" ").append(cond.getOperator());

                if (!cond.getOperator().equals("IS NULL") && !cond.getOperator().equals("IS NOT NULL")) {
                    sql.append(" '").append(cond.getValue()).append("'");
                }

                first = false;
            }
        }

        String orderByColumn = orderByComboBox.getValue();
        if (orderByColumn != null && !orderByColumn.isEmpty()) {
            sql.append("\nORDER BY ").append(orderByColumn).append(" ").append(sortOrderComboBox.getValue());
        }

        int limit = limitSpinner.getValue();
        if (limit > 0) {
            sql.append("\nLIMIT ").append(limit);
        }
    }

    private void updateUIForQueryType() {
        String queryType = queryTypeComboBox.getValue();
        switch (queryType) {
            case "SELECT":
                distinctCheckBox.setDisable(false);
                orderByComboBox.setDisable(false);
                sortOrderComboBox.setDisable(false);
                limitSpinner.setDisable(false);
                break;
            default:
                distinctCheckBox.setDisable(true);
                orderByComboBox.setDisable(true);
                sortOrderComboBox.setDisable(true);
                limitSpinner.setDisable(true);
                break;
        }
    }

    private void showAlert(String message) {
        Alert alert = new Alert(Alert.AlertType.WARNING);
        alert.setTitle("Warning");
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.showAndWait();
    }

    public void setCurrentDatabase(String dbName) {
        if (dbName == null || dbName.isEmpty()) {
            System.err.println("Warning: Empty database name provided");
            return;
        }

        this.currentDatabase = dbName;
        connectionManager.setCurrentDatabase(dbName);

        Platform.runLater(() -> {
            try {
                if (!databaseComboBox.getItems().contains(dbName)) {
                    connectionManager.getDatabases().thenAccept(result -> {
                        List<String> databases = new ArrayList<>();

                        if (result.containsKey("databases")) {
                            Object dbObj = result.get("databases");
                            if (dbObj instanceof List) {
                                @SuppressWarnings("unchecked")
                                List<Object> dbList = (List<Object>) dbObj;
                                for (Object db : dbList) {
                                    databases.add(db.toString());
                                }
                            }
                        }

                        Platform.runLater(() -> {
                            databaseComboBox.getItems().clear();
                            databaseComboBox.getItems().addAll(databases);
                            databaseComboBox.setValue(dbName);
                            loadTablesForDatabase();
                        });
                    });
                } else {
                    databaseComboBox.setValue(dbName);
                    loadTablesForDatabase();
                }
            } catch (Exception e) {
                System.err.println("Error setting current database: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }

    // Inner classes
    private static class Delta {
        double x, y;
    }

    private static class JoinLine {
        private final Line line;
        private final Label joinLabel;
        private final Label conditionLabel;
        private final JoinCondition join;

        public JoinLine(Line line, Label joinLabel, Label conditionLabel, JoinCondition join) {
            this.line = line;
            this.joinLabel = joinLabel;
            this.conditionLabel = conditionLabel;
            this.join = join;
        }

        public Line getLine() {
            return line;
        }

        public Label getJoinLabel() {
            return joinLabel;
        }

        public Label getConditionLabel() {
            return conditionLabel;
        }

        public JoinCondition getJoin() {
            return join;
        }
    }

    private static class JoinCondition {
        private final String joinType;
        private final String leftTable;
        private final String leftColumn;
        private final String rightTable;
        private final String rightColumn;

        public JoinCondition(String joinType, String leftTable, String leftColumn,
                String rightTable, String rightColumn) {
            this.joinType = joinType;
            this.leftTable = leftTable;
            this.leftColumn = leftColumn;
            this.rightTable = rightTable;
            this.rightColumn = rightColumn;
        }

        public String getJoinType() {
            return joinType;
        }

        public String getLeftTable() {
            return leftTable;
        }

        public String getLeftColumn() {
            return leftColumn;
        }

        public String getRightTable() {
            return rightTable;
        }

        public String getRightColumn() {
            return rightColumn;
        }
    }

    private static class WhereCondition {
        private String table;
        private String column;
        private String operator = "=";
        private String value = "";

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
    }

    private static class ColumnSelection {
        private final String table;
        private final String column;
        private String alias = "";
        private String aggregate = "";
        private boolean selected = true;

        public ColumnSelection(String table, String column) {
            this.table = table;
            this.column = column;
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
            this.alias = alias;
        }

        public String getAggregate() {
            return aggregate;
        }

        public void setAggregate(String aggregate) {
            this.aggregate = aggregate;
        }

        public boolean isSelected() {
            return selected;
        }

        public void setSelected(boolean selected) {
            this.selected = selected;
        }
    }
}