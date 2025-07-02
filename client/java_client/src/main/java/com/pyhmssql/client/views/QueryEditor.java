package com.pyhmssql.client.views;

import com.pyhmssql.client.main.ConnectionManager;
import com.pyhmssql.client.utils.SQLSyntaxHighlighter;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import org.fxmisc.richtext.CodeArea;

import java.util.Map;

/**
 * SQL Query Editor with syntax highlighting and execution capabilities
 */
public class QueryEditor extends BorderPane {
    private final ConnectionManager connectionManager;
    private CodeArea codeArea;
    private TableView<Map<String, Object>> resultTable;
    private TextArea outputArea;
    private boolean hasUnsavedChanges = false;

    public QueryEditor(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        initializeComponents();
        setupLayout();
    }

    private void initializeComponents() {
        // SQL Editor
        codeArea = new CodeArea();
        codeArea.setStyle("-fx-font-family: 'Courier New', monospace; -fx-font-size: 12px;");
        codeArea.replaceText("-- Enter your SQL query here\nSELECT * FROM ");

        // Apply syntax highlighting
        SQLSyntaxHighlighter.applySyntaxHighlighting(codeArea);

        // Track changes
        codeArea.textProperty().addListener((obs, oldText, newText) -> {
            hasUnsavedChanges = true;
        });

        // Results table
        resultTable = new TableView<>();
        resultTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);

        // Output area for messages
        outputArea = new TextArea();
        outputArea.setEditable(false);
        outputArea.setPrefRowCount(3);
        outputArea.setStyle("-fx-font-family: monospace;");
    }

    private void setupLayout() {
        // Toolbar
        ToolBar toolbar = new ToolBar();
        Button executeBtn = new Button("Execute (F5)");
        Button clearBtn = new Button("Clear");
        Button formatBtn = new Button("Format");

        executeBtn.setOnAction(e -> executeQuery());
        clearBtn.setOnAction(e -> clearEditor());
        formatBtn.setOnAction(e -> formatQuery());

        toolbar.getItems().addAll(executeBtn, new Separator(), clearBtn, formatBtn);

        // Split pane for editor and results
        SplitPane splitPane = new SplitPane();
        splitPane.setOrientation(javafx.geometry.Orientation.VERTICAL);

        // Editor section
        VBox editorSection = new VBox();
        editorSection.getChildren().addAll(
                new Label("SQL Query:"),
                codeArea);
        VBox.setVgrow(codeArea, Priority.ALWAYS);

        // Results section
        TabPane resultsTabPane = new TabPane();

        Tab resultsTab = new Tab("Results");
        resultsTab.setContent(resultTable);
        resultsTab.setClosable(false);

        Tab outputTab = new Tab("Output");
        outputTab.setContent(outputArea);
        outputTab.setClosable(false);

        resultsTabPane.getTabs().addAll(resultsTab, outputTab);

        splitPane.getItems().addAll(editorSection, resultsTabPane);
        splitPane.setDividerPositions(0.6);

        setTop(toolbar);
        setCenter(splitPane);
    }

    public void executeQuery() {
        String query = codeArea.getText().trim();
        if (query.isEmpty()) {
            outputArea.setText("No query to execute.");
            return;
        }

        // Clear previous results
        resultTable.getColumns().clear();
        resultTable.getItems().clear();
        outputArea.setText("Executing query...\n");

        connectionManager.executeQuery(query).thenAccept(result -> {
            Platform.runLater(() -> {
                if (result.containsKey("error")) {
                    outputArea.setText("Error: " + result.get("error"));
                } else {
                    handleQueryResult(result);
                    hasUnsavedChanges = false;
                }
            });
        }).exceptionally(throwable -> {
            Platform.runLater(() -> {
                outputArea.setText("Error: " + throwable.getMessage());
            });
            return null;
        });
    }

    private void handleQueryResult(Map<String, Object> result) {
        StringBuilder output = new StringBuilder();
        output.append("Query executed successfully.\n");

        if (result.containsKey("rows")) {
            Object rowsObj = result.get("rows");
            if (rowsObj instanceof java.util.List) {
                @SuppressWarnings("unchecked")
                java.util.List<Object> rows = (java.util.List<Object>) rowsObj;
                output.append("Rows returned: ").append(rows.size()).append("\n");

                // Create table columns if we have column info
                if (result.containsKey("columns") && result.get("columns") instanceof java.util.List) {
                    @SuppressWarnings("unchecked")
                    java.util.List<String> columns = (java.util.List<String>) result.get("columns");

                    for (String columnName : columns) {
                        TableColumn<Map<String, Object>, String> column = new TableColumn<>(columnName);
                        column.setCellValueFactory(data -> {
                            Object value = data.getValue().get(columnName);
                            return new javafx.beans.property.SimpleStringProperty(
                                    value != null ? value.toString() : "NULL");
                        });
                        resultTable.getColumns().add(column);
                    }

                    // Add data to table
                    for (Object rowObj : rows) {
                        if (rowObj instanceof java.util.List) {
                            @SuppressWarnings("unchecked")
                            java.util.List<Object> row = (java.util.List<Object>) rowObj;
                            java.util.Map<String, Object> rowMap = new java.util.HashMap<>();
                            for (int i = 0; i < columns.size() && i < row.size(); i++) {
                                rowMap.put(columns.get(i), row.get(i));
                            }
                            resultTable.getItems().add(rowMap);
                        }
                    }
                }
            }
        } else {
            output.append("Query completed.\n");
        }

        outputArea.setText(output.toString());
    }

    private void clearEditor() {
        codeArea.clear();
        resultTable.getColumns().clear();
        resultTable.getItems().clear();
        outputArea.clear();
        hasUnsavedChanges = false;
    }

    private void formatQuery() {
        try {
            String formatted = codeArea.getText(); // Basic implementation
            codeArea.replaceText(formatted);
        } catch (Exception e) {
            outputArea.setText("Error formatting query: " + e.getMessage());
        }
    }

    public String getQueryText() {
        return codeArea.getText();
    }

    public void setQueryText(String text) {
        codeArea.replaceText(text);
        hasUnsavedChanges = false;
    }

    public String getSelectedText() {
        return codeArea.getSelectedText();
    }

    public boolean hasUnsavedChanges() {
        return hasUnsavedChanges;
    }

    public void refreshDatabases() {
        // Implementation for refreshing database list
    }
}