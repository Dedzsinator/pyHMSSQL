package com.pyhmssql.client.views;

import com.pyhmssql.client.main.ConnectionManager;
import com.pyhmssql.client.model.TableMetadata;
import com.pyhmssql.client.model.ColumnMetadata;
import com.pyhmssql.client.utils.SQLFormatter;

import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.input.MouseEvent;
import javafx.scene.input.TransferMode;
import javafx.scene.input.ClipboardContent;
import javafx.scene.input.Dragboard;
import javafx.scene.paint.Color;
import javafx.scene.shape.Line;
import javafx.scene.shape.Circle;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.scene.Node;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Visual Query Builder component that allows users to build SQL queries graphically
 */
public class VisualQueryBuilder extends BorderPane {
    private final ConnectionManager connectionManager;
    private final Consumer<String> onQueryBuilt;
    private final Map<String, TableView> tableNodes = new HashMap<>();
    private final List<JoinLine> joinLines = new ArrayList<>();
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
    
    // Store the join conditions
    private final List<JoinCondition> joinConditions = new ArrayList<>();
    
    // Store the WHERE conditions
    private final List<WhereCondition> whereConditions = new ArrayList<>();
    
    // Store the selected columns for projection
    private final List<ColumnSelection> selectedColumns = new ArrayList<>();
    
    /**
     * Create a new visual query builder
     * 
     * @param connectionManager The connection manager to use
     * @param onQueryBuilt Callback for when a query is built
     */
    public VisualQueryBuilder(ConnectionManager connectionManager, Consumer<String> onQueryBuilt) {
        this.connectionManager = connectionManager;
        this.onQueryBuilt = onQueryBuilt;
        
        setupUI();
    }
    
    /**
     * Sets up the main UI components
     */
    private void setupUI() {
        setPadding(new Insets(10));
        
        // Top section - Query type and settings
        setupQuerySettingsPanel();
        
        // Left panel - Available tables
        setupTablesPanel();
        
        // Center section - Visual diagram area
        setupDiagramArea();
        
        // Right panel - Query components (projections, conditions, etc.)
        setupQueryComponentsPanel();
        
        // Bottom panel - Generated SQL and actions
        setupActionPanel();
    }
    
    /**
     * Sets up the top panel with query settings
     */
    private void setupQuerySettingsPanel() {
        HBox settingsPanel = new HBox(10);
        settingsPanel.setPadding(new Insets(5));
        settingsPanel.setAlignment(Pos.CENTER_LEFT);
        
        // Query name
        Label nameLabel = new Label("Query Name:");
        queryNameField = new TextField();
        queryNameField.setPromptText("MyQuery");
        queryNameField.setPrefWidth(150);
        
        // Query type
        Label typeLabel = new Label("Query Type:");
        queryTypeComboBox = new ComboBox<>(FXCollections.observableArrayList(
            "SELECT", "INSERT", "UPDATE", "DELETE"
        ));
        queryTypeComboBox.setValue("SELECT");
        queryTypeComboBox.setOnAction(e -> updateUIForQueryType());
        
        // DISTINCT option
        distinctCheckBox = new CheckBox("DISTINCT");
        
        // ORDER BY
        Label orderByLabel = new Label("ORDER BY:");
        orderByComboBox = new ComboBox<>();
        orderByComboBox.setPromptText("Select column");
        
        // Sort order
        sortOrderComboBox = new ComboBox<>(FXCollections.observableArrayList(
            "ASC", "DESC"
        ));
        sortOrderComboBox.setValue("ASC");
        
        // LIMIT
        Label limitLabel = new Label("LIMIT:");
        limitSpinner = new Spinner<>(0, 10000, 100, 10);
        limitSpinner.setEditable(true);
        limitSpinner.setPrefWidth(100);
        
        // Database selector
        Label dbLabel = new Label("Database:");
        ComboBox<String> databaseComboBox = new ComboBox<>();
        databaseComboBox.setPromptText("Select Database");
        databaseComboBox.setPrefWidth(150);
        
        // Load databases
        connectionManager.getDatabases().thenAccept(result -> {
            if (result.containsKey("databases")) {
                @SuppressWarnings("unchecked")
                List<String> databases = (List<String>) result.get("databases");
                javafx.application.Platform.runLater(() -> {
                    databaseComboBox.getItems().clear();
                    databaseComboBox.getItems().addAll(databases);
                });
            }
        });
        
        // Handle database selection
        databaseComboBox.setOnAction(e -> {
            currentDatabase = databaseComboBox.getValue();
            connectionManager.setCurrentDatabase(currentDatabase);
            loadTablesForDatabase();
            clearDiagramArea();
        });
        
        settingsPanel.getChildren().addAll(
            nameLabel, queryNameField, 
            typeLabel, queryTypeComboBox,
            distinctCheckBox,
            orderByLabel, orderByComboBox, sortOrderComboBox,
            limitLabel, limitSpinner,
            dbLabel, databaseComboBox
        );
        
        setTop(settingsPanel);
    }
    
    /**
     * Sets up the left panel with available tables
     */
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
    
    /**
     * Sets up the center diagram area
     */
    private void setupDiagramArea() {
        diagramPane = new Pane();
        diagramPane.setStyle("-fx-background-color: #f5f5f5; -fx-border-color: #cccccc;");
        
        // Enable drag-and-drop
        diagramPane.setOnDragOver(event -> {
            if (event.getGestureSource() != diagramPane && 
                event.getDragboard().hasString()) {
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
    
    /**
     * Sets up the right panel with query components
     */
    private void setupQueryComponentsPanel() {
        VBox rightPanel = new VBox(15);
        rightPanel.setPadding(new Insets(10));
        rightPanel.setPrefWidth(300);
        
        // Projections section (SELECT columns)
        Label projectionsLabel = new Label("SELECT Columns");
        projectionsLabel.setFont(Font.font("System", FontWeight.BOLD, 14));
        
        projectionsPanel = new VBox(5);
        ScrollPane projectionsScrollPane = new ScrollPane(projectionsPanel);
        projectionsScrollPane.setFitToWidth(true);
        projectionsScrollPane.setPrefHeight(200);
        
        // WHERE conditions section
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
            conditionsLabel, addConditionButton, conditionsScrollPane
        );
        
        setRight(rightPanel);
    }
    
    /**
     * Sets up the bottom panel with action buttons
     */
    private void setupActionPanel() {
        VBox bottomPanel = new VBox(10);
        bottomPanel.setPadding(new Insets(10));
        
        // Generated SQL area
        Label sqlLabel = new Label("Generated SQL");
        TextArea sqlTextArea = new TextArea();
        sqlTextArea.setEditable(false);
        sqlTextArea.setPrefHeight(100);
        
        // Action buttons
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
    
    /**
     * Loads tables for the selected database
     */
    private void loadTablesForDatabase() {
        if (currentDatabase == null || currentDatabase.isEmpty()) {
            return;
        }
        
        connectionManager.getTables(currentDatabase)
            .thenAccept(result -> {
                if (result.containsKey("tables")) {
                    @SuppressWarnings("unchecked")
                    List<String> tables = (List<String>) result.get("tables");
                    
                    javafx.application.Platform.runLater(() -> {
                        tablesPanel.getChildren().clear();
                        
                        for (String table : tables) {
                            Label tableLabel = new Label(table);
                            tableLabel.setPadding(new Insets(5));
                            tableLabel.setPrefWidth(180);
                            tableLabel.setStyle("-fx-border-color: #cccccc; -fx-background-color: #eeeeee;");
                            
                            // Make table draggable
                            tableLabel.setOnDragDetected(event -> {
                                Dragboard db = tableLabel.startDragAndDrop(TransferMode.COPY);
                                ClipboardContent content = new ClipboardContent();
                                content.putString(table);
                                db.setContent(content);
                                event.consume();
                            });
                            
                            // Double-click to add to diagram
                            tableLabel.setOnMouseClicked(event -> {
                                if (event.getClickCount() == 2) {
                                    addTableToDiagram(table, 50, 50);
                                }
                            });
                            
                            tablesPanel.getChildren().add(tableLabel);
                        }
                    });
                }
            });
    }
    
    /**
     * Adds a table to the diagram area
     */
    private void addTableToDiagram(String tableName, double x, double y) {
        if (tableNodes.containsKey(tableName)) {
            // Table is already in the diagram
            return;
        }
        
        // Get table metadata to show columns
        getTableMetadata(tableName).thenAccept(metadata -> {
            javafx.application.Platform.runLater(() -> {
                // Create table view
                TableView<Map<String, String>> tableView = new TableView<>();
                tableView.setPrefSize(200, 200);
                
                // Set up columns
                TableColumn<Map<String, String>, String> nameColumn = new TableColumn<>("Column");
                nameColumn.setCellValueFactory(data -> new javafx.beans.property.SimpleStringProperty(
                    data.getValue().get("name")
                ));
                
                TableColumn<Map<String, String>, String> typeColumn = new TableColumn<>("Type");
                typeColumn.setCellValueFactory(data -> new javafx.beans.property.SimpleStringProperty(
                    data.getValue().get("type")
                ));
                
                tableView.getColumns().addAll(nameColumn, typeColumn);
                
                // Add data rows
                ObservableList<Map<String, String>> data = FXCollections.observableArrayList();
                for (ColumnMetadata column : metadata.getColumns()) {
                    Map<String, String> row = new HashMap<>();
                    row.put("name", column.getName());
                    row.put("type", column.getType().toString());
                    data.add(row);
                    
                    // Also update the ORDER BY combobox
                    orderByComboBox.getItems().add(tableName + "." + column.getName());
                }
                
                tableView.setItems(data);
                
                // Add title
                Label titleLabel = new Label(tableName);
                titleLabel.setFont(Font.font("System", FontWeight.BOLD, 12));
                titleLabel.setPadding(new Insets(5));
                titleLabel.setStyle("-fx-background-color: #4a6da7; -fx-text-fill: white;");
                titleLabel.setPrefWidth(200);
                
                // Create container for the table
                VBox tableContainer = new VBox();
                tableContainer.getChildren().addAll(titleLabel, tableView);
                tableContainer.setLayoutX(x);
                tableContainer.setLayoutY(y);
                tableContainer.setStyle("-fx-border-color: #666666; -fx-background-color: white;");
                
                // Make the table draggable
                final Delta dragDelta = new Delta();
                
                titleLabel.setOnMousePressed(event -> {
                    dragDelta.x = tableContainer.getLayoutX() - event.getSceneX();
                    dragDelta.y = tableContainer.getLayoutY() - event.getSceneY();
                });
                
                titleLabel.setOnMouseDragged(event -> {
                    tableContainer.setLayoutX(event.getSceneX() + dragDelta.x);
                    tableContainer.setLayoutY(event.getSceneY() + dragDelta.y);
                    
                    // Redraw join lines
                    updateJoinLines();
                });
                
                // Allow selecting columns for query
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
                
                // Close button
                Button closeButton = new Button("×");
                closeButton.setStyle("-fx-background-color: transparent; -fx-text-fill: white;");
                closeButton.setOnAction(e -> removeTableFromDiagram(tableName));
                
                // Add close button to title bar
                HBox titleBox = new HBox();
                titleBox.setAlignment(Pos.CENTER_LEFT);
                Region spacer = new Region();
                HBox.setHgrow(spacer, Priority.ALWAYS);
                titleLabel.setMaxWidth(Double.MAX_VALUE);
                titleBox.getChildren().addAll(titleLabel, spacer, closeButton);
                
                tableContainer.getChildren().set(0, titleBox);
                
                // Store in map
                tableNodes.put(tableName, tableView);
                
                // Add to diagram
                diagramPane.getChildren().add(tableContainer);
                
                // Add join points for joining tables
                if (diagramPane.getChildren().size() > 1) {
                    setupJoinCapability(tableName, tableView, tableContainer);
                }
            });
        });
    }
    
    /**
     * Gets table metadata from the server
     */
    private CompletableFuture<TableMetadata> getTableMetadata(String tableName) {
        CompletableFuture<TableMetadata> future = new CompletableFuture<>();
        
        connectionManager.getColumns(currentDatabase, tableName)
            .thenAccept(result -> {
                if (result.containsKey("columns")) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> columnsData = (List<Map<String, Object>>) result.get("columns");
                    
                    List<ColumnMetadata> columns = columnsData.stream()
                        .map(data -> new ColumnMetadata(
                            (String) data.get("name"),
                            (String) data.get("type"),
                            (Boolean) data.getOrDefault("primary_key", false),
                            (Boolean) data.getOrDefault("nullable", true)
                        ))
                        .collect(Collectors.toList());
                    
                    TableMetadata metadata = new TableMetadata(tableName, columns);
                    future.complete(metadata);
                } else {
                    // Create empty metadata if columns not found
                    future.complete(new TableMetadata(tableName, new ArrayList<>()));
                }
            })
            .exceptionally(ex -> {
                future.completeExceptionally(ex);
                return null;
            });
            
        return future;
    }
    
    /**
     * Sets up join capability between tables
     */
    private void setupJoinCapability(String tableName, TableView tableView, VBox tableContainer) {
        // Create a join point
        Circle joinPoint = new Circle(5, Color.BLUE);
        joinPoint.setStroke(Color.BLACK);
        joinPoint.setLayoutX(tableContainer.getLayoutX() + tableContainer.getWidth() / 2);
        joinPoint.setLayoutY(tableContainer.getLayoutY() + 10);
        
        // Add join capability
        joinPoint.setOnMousePressed(event -> {
            if (event.isControlDown()) {
                startJoin(tableName, tableContainer);
            }
        });
        
        tableContainer.setOnMouseClicked(event -> {
            if (event.isControlDown() && diagramPane.getChildren().size() > 1) {
                startJoin(tableName, tableContainer);
            }
        });
    }
    
    /**
     * Starts a join operation between tables
     */
    private void startJoin(String tableName, VBox tableContainer) {
        // Show join dialog
        Dialog<JoinCondition> dialog = new Dialog<>();
        dialog.setTitle("Create Join");
        dialog.setHeaderText("Join " + tableName + " with another table");
        
        // Set up the dialog
        ButtonType joinButtonType = new ButtonType("Join", ButtonBar.ButtonData.OK_DONE);
        dialog.getDialogPane().getButtonTypes().addAll(joinButtonType, ButtonType.CANCEL);
        
        // Create the join form
        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(20, 150, 10, 10));
        
        // Join type combo box
        ComboBox<String> joinTypeCombo = new ComboBox<>();
        joinTypeCombo.getItems().addAll("INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN");
        joinTypeCombo.setValue("INNER JOIN");
        
        // Table combo box - all tables except current one
        ComboBox<String> tableCombo = new ComboBox<>();
        tableCombo.getItems().addAll(
            tableNodes.keySet().stream()
                .filter(name -> !name.equals(tableName))
                .collect(Collectors.toList())
        );
        
        if (!tableCombo.getItems().isEmpty()) {
            tableCombo.setValue(tableCombo.getItems().get(0));
        }
        
        // Column combo boxes
        ComboBox<String> leftColumnCombo = new ComboBox<>();
        ComboBox<String> rightColumnCombo = new ComboBox<>();
        
        // Load columns for the left table (current table)
        getTableMetadata(tableName).thenAccept(metadata -> {
            javafx.application.Platform.runLater(() -> {
                leftColumnCombo.getItems().addAll(
                    metadata.getColumns().stream()
                        .map(ColumnMetadata::getName)
                        .collect(Collectors.toList())
                );
                
                if (!leftColumnCombo.getItems().isEmpty()) {
                    leftColumnCombo.setValue(leftColumnCombo.getItems().get(0));
                }
            });
        });
        
        // Update right columns when right table changes
        tableCombo.setOnAction(e -> {
            String rightTable = tableCombo.getValue();
            if (rightTable != null) {
                rightColumnCombo.getItems().clear();
                
                getTableMetadata(rightTable).thenAccept(metadata -> {
                    javafx.application.Platform.runLater(() -> {
                        rightColumnCombo.getItems().addAll(
                            metadata.getColumns().stream()
                                .map(ColumnMetadata::getName)
                                .collect(Collectors.toList())
                        );
                        
                        if (!rightColumnCombo.getItems().isEmpty()) {
                            rightColumnCombo.setValue(rightColumnCombo.getItems().get(0));
                        }
                    });
                });
            }
        });
        
        // Trigger the right table selection
        tableCombo.fireEvent(new javafx.event.ActionEvent());
        
        grid.add(new Label("Join Type:"), 0, 0);
        grid.add(joinTypeCombo, 1, 0);
        grid.add(new Label("Left Table:"), 0, 1);
        grid.add(new Label(tableName), 1, 1);
        grid.add(new Label("Left Column:"), 0, 2);
        grid.add(leftColumnCombo, 1, 2);
        grid.add(new Label("Right Table:"), 0, 3);
        grid.add(tableCombo, 1, 3);
        grid.add(new Label("Right Column:"), 0, 4);
        grid.add(rightColumnCombo, 1, 4);
        
        dialog.getDialogPane().setContent(grid);
        
        // Convert the result
        dialog.setResultConverter(dialogButton -> {
            if (dialogButton == joinButtonType) {
                return new JoinCondition(
                    joinTypeCombo.getValue(),
                    tableName,
                    leftColumnCombo.getValue(),
                    tableCombo.getValue(),
                    rightColumnCombo.getValue()
                );
            }
            return null;
        });
        
        Optional<JoinCondition> result = dialog.showAndWait();
        result.ifPresent(joinCondition -> {
            // Add the join
            joinConditions.add(joinCondition);
            
            // Draw the join line
            drawJoinLine(joinCondition);
        });
    }
    
    /**
     * Draws a join line between tables
     */
    private void drawJoinLine(JoinCondition join) {
        VBox leftTable = findTableNode(join.getLeftTable());
        VBox rightTable = findTableNode(join.getRightTable());
        
        if (leftTable == null || rightTable == null) {
            return;
        }
        
        // Create the line
        Line line = new Line();
        line.setStartX(leftTable.getLayoutX() + leftTable.getWidth());
        line.setStartY(leftTable.getLayoutY() + 30);
        line.setEndX(rightTable.getLayoutX());
        line.setEndY(rightTable.getLayoutY() + 30);
        line.setStrokeWidth(2);
        
        // Style based on join type
        switch (join.getJoinType()) {
            case "INNER JOIN":
                line.setStroke(Color.BLACK);
                break;
            case "LEFT JOIN":
                line.setStroke(Color.BLUE);
                break;
            case "RIGHT JOIN":
                line.setStroke(Color.GREEN);
                break;
            case "FULL JOIN":
                line.setStroke(Color.PURPLE);
                break;
            default:
                line.setStroke(Color.GRAY);
        }
        
        // Add join label
        Label joinLabel = new Label(join.getJoinType());
        joinLabel.setLayoutX((line.getStartX() + line.getEndX()) / 2 - 40);
        joinLabel.setLayoutY((line.getStartY() + line.getEndY()) / 2 - 15);
        joinLabel.setStyle("-fx-background-color: white; -fx-border-color: black; -fx-padding: 2;");
        
        // Add condition label
        Label conditionLabel = new Label(
            join.getLeftTable() + "." + join.getLeftColumn() + 
            " = " + 
            join.getRightTable() + "." + join.getRightColumn()
        );
        conditionLabel.setLayoutX((line.getStartX() + line.getEndX()) / 2 - 70);
        conditionLabel.setLayoutY((line.getStartY() + line.getEndY()) / 2 + 5);
        conditionLabel.setStyle("-fx-background-color: #f8f8f8; -fx-border-color: #cccccc; -fx-padding: 2;");
        
        // Add to diagram
        diagramPane.getChildren().addAll(line, joinLabel, conditionLabel);
        
        // Store for updating
        joinLines.add(new JoinLine(line, joinLabel, conditionLabel, join));
    }
    
    /**
     * Updates join lines when tables are moved
     */
    private void updateJoinLines() {
        for (JoinLine joinLine : joinLines) {
            VBox leftTable = findTableNode(joinLine.getJoin().getLeftTable());
            VBox rightTable = findTableNode(joinLine.getJoin().getRightTable());
            
            if (leftTable != null && rightTable != null) {
                // Update line coordinates
                joinLine.getLine().setStartX(leftTable.getLayoutX() + leftTable.getWidth());
                joinLine.getLine().setStartY(leftTable.getLayoutY() + 30);
                joinLine.getLine().setEndX(rightTable.getLayoutX());
                joinLine.getLine().setEndY(rightTable.getLayoutY() + 30);
                
                // Update labels
                joinLine.getJoinLabel().setLayoutX((joinLine.getLine().getStartX() + joinLine.getLine().getEndX()) / 2 - 40);
                joinLine.getJoinLabel().setLayoutY((joinLine.getLine().getStartY() + joinLine.getLine().getEndY()) / 2 - 15);
                
                joinLine.getConditionLabel().setLayoutX((joinLine.getLine().getStartX() + joinLine.getLine().getEndX()) / 2 - 70);
                joinLine.getConditionLabel().setLayoutY((joinLine.getLine().getStartY() + joinLine.getLine().getEndY()) / 2 + 5);
            }
        }
    }
    
    /**
     * Adds a column to the projection list (SELECT)
     */
    private void addColumnToProjection(String tableName, String columnName) {
        // Check if already added
        for (ColumnSelection col : selectedColumns) {
            if (col.getTable().equals(tableName) && col.getColumn().equals(columnName)) {
                return; // Already added
            }
        }
        
        ColumnSelection columnSelection = new ColumnSelection(tableName, columnName);
        selectedColumns.add(columnSelection);
        
        // Update UI
        javafx.application.Platform.runLater(() -> {
            HBox columnRow = new HBox(5);
            columnRow.setAlignment(Pos.CENTER_LEFT);
            
            CheckBox selectBox = new CheckBox();
            selectBox.setSelected(true);
            selectBox.setOnAction(e -> columnSelection.setSelected(selectBox.isSelected()));
            
            Label columnLabel = new Label(tableName + "." + columnName);
            
            TextField aliasField = new TextField();
            aliasField.setPromptText("Alias");
            aliasField.textProperty().addListener((obs, oldVal, newVal) -> 
                columnSelection.setAlias(newVal)
            );
            
            ComboBox<String> aggregateCombo = new ComboBox<>();
            aggregateCombo.getItems().addAll("", "COUNT", "SUM", "AVG", "MIN", "MAX");
            aggregateCombo.setValue("");
            aggregateCombo.setOnAction(e -> 
                columnSelection.setAggregate(aggregateCombo.getValue())
            );
            
            Button removeButton = new Button("×");
            removeButton.setOnAction(e -> {
                selectedColumns.remove(columnSelection);
                projectionsPanel.getChildren().remove(columnRow);
            });
            
            columnRow.getChildren().addAll(
                selectBox, columnLabel, new Label("as"), aliasField, 
                new Label("agg"), aggregateCombo, removeButton
            );
            
            projectionsPanel.getChildren().add(columnRow);
        });
    }
    
        /**
     * Adds a WHERE condition to the query
     */
    private void addWhereCondition() {
        if (tableNodes.isEmpty()) {
            showAlert("No tables added to the query");
            return;
        }
        
        WhereCondition condition = new WhereCondition();
        whereConditions.add(condition);
        
        // Create UI components
        HBox conditionRow = new HBox(5);
        conditionRow.setAlignment(Pos.CENTER_LEFT);
        
        // Table and column selectors
        ComboBox<String> tableCombo = new ComboBox<>();
        tableCombo.getItems().addAll(tableNodes.keySet());
        if (!tableCombo.getItems().isEmpty()) {
            tableCombo.setValue(tableCombo.getItems().get(0));
            condition.setTable(tableCombo.getValue());
        }
        
        ComboBox<String> columnCombo = new ComboBox<>();
        updateColumnComboForTable(tableCombo.getValue(), columnCombo);
        columnCombo.setOnAction(e -> condition.setColumn(columnCombo.getValue()));
        
        // Update columns when table changes
        tableCombo.setOnAction(e -> {
            condition.setTable(tableCombo.getValue());
            updateColumnComboForTable(tableCombo.getValue(), columnCombo);
        });
        
        // Value field - moved up before it's referenced
        TextField valueField = new TextField();
        valueField.setPromptText("Value");
        
        // Operator selection
        ComboBox<String> operatorCombo = new ComboBox<>();
        operatorCombo.getItems().addAll("=", "<>", ">", "<", ">=", "<=", "LIKE", "IN", "NOT IN", "IS NULL", "IS NOT NULL");
        operatorCombo.setValue("=");
        operatorCombo.setOnAction(e -> {
            condition.setOperator(operatorCombo.getValue());
            updateValueFieldVisibility(operatorCombo.getValue(), valueField, conditionRow);
        });
        
        valueField.textProperty().addListener((obs, oldVal, newVal) -> 
            condition.setValue(newVal)
        );
        
        // Remove button
        Button removeButton = new Button("×");
        removeButton.setOnAction(e -> {
            whereConditions.remove(condition);
            conditionsPanel.getChildren().remove(conditionRow);
        });
        
        // Add components to row
        conditionRow.getChildren().addAll(
            tableCombo, new Label("."), columnCombo, 
            operatorCombo, valueField, removeButton
        );
        
        // Add to panel
        conditionsPanel.getChildren().add(conditionRow);
    }
    
    /**
     * Updates the column combo box based on the selected table
     */
    private void updateColumnComboForTable(String tableName, ComboBox<String> columnCombo) {
        if (tableName == null) return;
        
        columnCombo.getItems().clear();
        
        getTableMetadata(tableName).thenAccept(metadata -> {
            javafx.application.Platform.runLater(() -> {
                columnCombo.getItems().addAll(
                    metadata.getColumns().stream()
                        .map(ColumnMetadata::getName)
                        .collect(Collectors.toList())
                );
                
                if (!columnCombo.getItems().isEmpty()) {
                    columnCombo.setValue(columnCombo.getItems().get(0));
                }
            });
        });
    }
    
    /**
     * Updates value field visibility based on the operator
     */
    private void updateValueFieldVisibility(String operator, TextField valueField, HBox row) {
        if (operator.equals("IS NULL") || operator.equals("IS NOT NULL")) {
            valueField.setVisible(false);
            valueField.setManaged(false);
        } else {
            valueField.setVisible(true);
            valueField.setManaged(true);
        }
    }
    
    /**
     * Removes a table from the diagram
     */
    private void removeTableFromDiagram(String tableName) {
        // Find the table container
        VBox tableContainer = findTableNode(tableName);
        if (tableContainer != null) {
            // Remove from diagram
            diagramPane.getChildren().remove(tableContainer);
            
            // Remove from map
            tableNodes.remove(tableName);
            
            // Remove associated join lines
            List<JoinLine> linesToRemove = joinLines.stream()
                .filter(line -> line.getJoin().getLeftTable().equals(tableName) || 
                               line.getJoin().getRightTable().equals(tableName))
                .collect(Collectors.toList());
            
            for (JoinLine line : linesToRemove) {
                diagramPane.getChildren().remove(line.getLine());
                diagramPane.getChildren().remove(line.getJoinLabel());
                diagramPane.getChildren().remove(line.getConditionLabel());
                joinLines.remove(line);
                joinConditions.remove(line.getJoin());
            }
            
            // Remove columns from projections
            List<ColumnSelection> columnsToRemove = selectedColumns.stream()
                .filter(col -> col.getTable().equals(tableName))
                .collect(Collectors.toList());
            
            for (ColumnSelection col : columnsToRemove) {
                selectedColumns.remove(col);
                
                // Find and remove UI component
                for (int i = 0; i < projectionsPanel.getChildren().size(); i++) {
                    Node node = projectionsPanel.getChildren().get(i);
                    if (node instanceof HBox) {
                        HBox row = (HBox) node;
                        for (Node child : row.getChildren()) {
                            if (child instanceof Label) {
                                Label label = (Label) child;
                                if (label.getText().startsWith(tableName + ".")) {
                                    projectionsPanel.getChildren().remove(row);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            
            // Update ORDER BY combo
            orderByComboBox.getItems().removeIf(item -> item.startsWith(tableName + "."));
        }
    }
    
    /**
     * Finds a table container by name
     */
    private VBox findTableNode(String tableName) {
        for (Node node : diagramPane.getChildren()) {
            if (node instanceof VBox) {
                VBox container = (VBox) node;
                if (container.getChildren().size() > 0) {
                    Node firstChild = container.getChildren().get(0);
                    if (firstChild instanceof HBox) {
                        HBox titleBox = (HBox) firstChild;
                        for (Node titleChild : titleBox.getChildren()) {
                            if (titleChild instanceof Label) {
                                Label titleLabel = (Label) titleChild;
                                if (titleLabel.getText().equals(tableName)) {
                                    return container;
                                }
                            }
                        }
                    }
                }
            }
        }
        return null;
    }
    
    /**
     * Clears the diagram area
     */
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
    
    /**
     * Builds the SQL query based on the visual components
     */
    private String buildSQLQuery() {
        StringBuilder sql = new StringBuilder();
        
        String queryType = queryTypeComboBox.getValue();
        
        switch (queryType) {
            case "SELECT":
                buildSelectQuery(sql);
                break;
            case "INSERT":
                buildInsertQuery(sql);
                break;
            case "UPDATE":
                buildUpdateQuery(sql);
                break;
            case "DELETE":
                buildDeleteQuery(sql);
                break;
            default:
                sql.append("-- Query type not implemented");
        }
        
        return sql.toString();
    }
    
    /**
     * Builds a SELECT query
     */
    private void buildSelectQuery(StringBuilder sql) {
        sql.append("SELECT ");
        
        if (distinctCheckBox.isSelected()) {
            sql.append("DISTINCT ");
        }
        
        // Add selected columns
        if (selectedColumns.isEmpty()) {
            sql.append("*");
        } else {
            boolean first = true;
            for (ColumnSelection col : selectedColumns) {
                if (!col.isSelected()) continue;
                
                if (!first) {
                    sql.append(", ");
                }
                
                if (!col.getAggregate().isEmpty()) {
                    sql.append(col.getAggregate())
                       .append("(")
                       .append(col.getTable())
                       .append(".")
                       .append(col.getColumn())
                       .append(")");
                } else {
                    sql.append(col.getTable())
                       .append(".")
                       .append(col.getColumn());
                }
                
                if (!col.getAlias().isEmpty()) {
                    sql.append(" AS ")
                       .append(col.getAlias());
                }
                
                first = false;
            }
        }
        
        // Add tables and joins
        if (!tableNodes.isEmpty()) {
            String baseTable = tableNodes.keySet().iterator().next();
            sql.append("\nFROM ")
               .append(baseTable);
            
            // Add joins
            for (JoinCondition join : joinConditions) {
                sql.append("\n")
                   .append(join.getJoinType())
                   .append(" ")
                   .append(join.getRightTable())
                   .append(" ON ")
                   .append(join.getLeftTable())
                   .append(".")
                   .append(join.getLeftColumn())
                   .append(" = ")
                   .append(join.getRightTable())
                   .append(".")
                   .append(join.getRightColumn());
            }
        }
        
        // Add WHERE conditions
        if (!whereConditions.isEmpty()) {
            sql.append("\nWHERE ");
            
            boolean first = true;
            for (WhereCondition cond : whereConditions) {
                if (!first) {
                    sql.append(" AND ");
                }
                
                sql.append(cond.getTable())
                   .append(".")
                   .append(cond.getColumn())
                   .append(" ")
                   .append(cond.getOperator());
                
                if (!cond.getOperator().equals("IS NULL") && 
                    !cond.getOperator().equals("IS NOT NULL")) {
                    sql.append(" ");
                    
                    // Quote string values if they don't start with a special character
                    String value = cond.getValue();
                    if (value != null && !value.isEmpty() && 
                        !value.startsWith("(") && !value.startsWith("@") && 
                        !isNumeric(value)) {
                        sql.append("'")
                           .append(value)
                           .append("'");
                    } else {
                        sql.append(value);
                    }
                }
                
                first = false;
            }
        }
        
        // Add ORDER BY
        String orderByColumn = orderByComboBox.getValue();
        if (orderByColumn != null && !orderByColumn.isEmpty()) {
            sql.append("\nORDER BY ")
               .append(orderByColumn)
               .append(" ")
               .append(sortOrderComboBox.getValue());
        }
        
        // Add LIMIT
        int limit = limitSpinner.getValue();
        if (limit > 0) {
            sql.append("\nLIMIT ")
               .append(limit);
        }
    }
    
    /**
     * Builds an INSERT query
     */
    private void buildInsertQuery(StringBuilder sql) {
        if (tableNodes.isEmpty()) {
            sql.append("-- No tables selected for INSERT");
            return;
        }
        
        String tableName = tableNodes.keySet().iterator().next();
        sql.append("INSERT INTO ")
           .append(tableName)
           .append(" (");
        
        // Add columns
        boolean first = true;
        for (ColumnSelection col : selectedColumns) {
            if (!col.isSelected()) continue;
            
            if (!first) {
                sql.append(", ");
            }
            
            sql.append(col.getColumn());
            first = false;
        }
        
        sql.append(")\nVALUES (");
        
        // Add placeholders for values
        first = true;
        for (ColumnSelection col : selectedColumns) {
            if (!col.isSelected()) continue;
            
            if (!first) {
                sql.append(", ");
            }
            
            sql.append("?");
            first = false;
        }
        
        sql.append(")");
    }
    
    /**
     * Builds an UPDATE query
     */
    private void buildUpdateQuery(StringBuilder sql) {
        if (tableNodes.isEmpty()) {
            sql.append("-- No tables selected for UPDATE");
            return;
        }
        
        String tableName = tableNodes.keySet().iterator().next();
        sql.append("UPDATE ")
           .append(tableName)
           .append("\nSET ");
        
        // Add columns to update
        boolean first = true;
        for (ColumnSelection col : selectedColumns) {
            if (!col.isSelected()) continue;
            
            if (!first) {
                sql.append(", ");
            }
            
            sql.append(col.getColumn())
               .append(" = ?");
            
            first = false;
        }
        
        // Add WHERE conditions
        if (!whereConditions.isEmpty()) {
            sql.append("\nWHERE ");
            
            first = true;
            for (WhereCondition cond : whereConditions) {
                if (!first) {
                    sql.append(" AND ");
                }
                
                sql.append(cond.getTable())
                   .append(".")
                   .append(cond.getColumn())
                   .append(" ")
                   .append(cond.getOperator());
                
                if (!cond.getOperator().equals("IS NULL") && 
                    !cond.getOperator().equals("IS NOT NULL")) {
                    sql.append(" ");
                    
                    // Quote string values if needed
                    String value = cond.getValue();
                    if (value != null && !value.isEmpty() && 
                        !value.startsWith("(") && !value.startsWith("@") && 
                        !isNumeric(value)) {
                        sql.append("'")
                           .append(value)
                           .append("'");
                    } else {
                        sql.append(value);
                    }
                }
                
                first = false;
            }
        }
    }
    
    /**
     * Builds a DELETE query
     */
    private void buildDeleteQuery(StringBuilder sql) {
        if (tableNodes.isEmpty()) {
            sql.append("-- No tables selected for DELETE");
            return;
        }
        
        String tableName = tableNodes.keySet().iterator().next();
        sql.append("DELETE FROM ")
           .append(tableName);
        
        // Add WHERE conditions
        if (!whereConditions.isEmpty()) {
            sql.append("\nWHERE ");
            
            boolean first = true;
            for (WhereCondition cond : whereConditions) {
                if (!first) {
                    sql.append(" AND ");
                }
                
                sql.append(cond.getTable())
                   .append(".")
                   .append(cond.getColumn())
                   .append(" ")
                   .append(cond.getOperator());
                
                if (!cond.getOperator().equals("IS NULL") && 
                    !cond.getOperator().equals("IS NOT NULL")) {
                    sql.append(" ");
                    
                    // Quote string values if needed
                    String value = cond.getValue();
                    if (value != null && !value.isEmpty() && 
                        !value.startsWith("(") && !value.startsWith("@") && 
                        !isNumeric(value)) {
                        sql.append("'")
                           .append(value)
                           .append("'");
                    } else {
                        sql.append(value);
                    }
                }
                
                first = false;
            }
        }
    }
    
    /**
     * Updates the UI based on the selected query type
     */
    private void updateUIForQueryType() {
        String queryType = queryTypeComboBox.getValue();
        
        // Show/hide components based on query type
        switch (queryType) {
            case "SELECT":
                distinctCheckBox.setDisable(false);
                orderByComboBox.setDisable(false);
                sortOrderComboBox.setDisable(false);
                limitSpinner.setDisable(false);
                break;
            case "INSERT":
            case "UPDATE":
            case "DELETE":
                distinctCheckBox.setDisable(true);
                orderByComboBox.setDisable(true);
                sortOrderComboBox.setDisable(true);
                limitSpinner.setDisable(true);
                break;
        }
    }
    
    /**
     * Shows an alert message
     */
    private void showAlert(String message) {
        Alert alert = new Alert(Alert.AlertType.WARNING);
        alert.setTitle("Warning");
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.showAndWait();
    }
    
    /**
     * Checks if a string is numeric
     */
    private boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    
    /**
     * Helper class for table dragging
     */
    private class Delta {
        double x, y;
    }
    
    /**
     * Class to store join line graphics
     */
    private class JoinLine {
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
    
    /**
     * Class to store join condition data
     */
    private class JoinCondition {
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
    
    /**
     * Class to store WHERE condition data
     */
    private class WhereCondition {
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
    
    /**
     * Class to store column selection data for projections
     */
    private class ColumnSelection {
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