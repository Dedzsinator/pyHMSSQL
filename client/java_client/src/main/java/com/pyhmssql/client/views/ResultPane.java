package com.pyhmssql.client.views;

import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.beans.property.SimpleStringProperty;
import java.util.*;

public class ResultPane extends BorderPane {
    private TableView<ObservableList<String>> tableView;
    private TextArea messageArea;
    private TabPane tabPane;
    private Label statusLabel;

    public ResultPane() {
        // Set up the UI
        tabPane = new TabPane();

        // Results tab
        tableView = new TableView<>();
        tableView.setPlaceholder(new Label("No data to display"));

        // Messages tab
        messageArea = new TextArea();
        messageArea.setEditable(false);

        // Status label
        statusLabel = new Label("Ready");

        // Create tabs
        Tab resultsTab = new Tab("Results", new BorderPane(tableView));
        resultsTab.setClosable(false);

        Tab messagesTab = new Tab("Messages", new BorderPane(messageArea));
        messagesTab.setClosable(false);

        tabPane.getTabs().addAll(resultsTab, messagesTab);

        // Create main layout
        VBox mainContent = new VBox();
        mainContent.getChildren().addAll(tabPane, statusLabel);
        VBox.setVgrow(tabPane, Priority.ALWAYS);

        setCenter(mainContent);
    }

    public void displayResults(Map<String, Object> result) {
        if (result == null) {
            showMessage("No result returned from server");
            return;
        }

        if (result.containsKey("error")) {
            showError((String) result.get("error"));
            return;
        }

        // Check if this is a result set or a message
        if (result.containsKey("rows")) {
            displayResultSet(result);
        } else if (result.containsKey("response")) {
            showMessage((String) result.get("response"));
        } else {
            showMessage("Unknown response format: " + result);
        }
    }

    private void displayResultSet(Map<String, Object> result) {
        try {
            Object rowsObj = result.get("rows");
            Object columnsObj = result.get("columns");

            List<Map<String, Object>> rows = null;
            List<String> columns = null;

            // Handle different types of rows data
            if (rowsObj instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> rawRows = (List<Object>) rowsObj;
                rows = new ArrayList<>();

                for (Object rowObj : rawRows) {
                    if (rowObj instanceof Map) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> rowMap = (Map<String, Object>) rowObj;
                        rows.add(rowMap);
                    } else if (rowObj instanceof List) {
                        // Convert list to map using column names
                        @SuppressWarnings("unchecked")
                        List<Object> rowList = (List<Object>) rowObj;
                        Map<String, Object> rowMap = new HashMap<>();

                        if (columnsObj instanceof List) {
                            @SuppressWarnings("unchecked")
                            List<String> colNames = (List<String>) columnsObj;
                            for (int i = 0; i < Math.min(rowList.size(), colNames.size()); i++) {
                                rowMap.put(colNames.get(i), rowList.get(i));
                            }
                        } else {
                            // Generate column names if not provided
                            for (int i = 0; i < rowList.size(); i++) {
                                rowMap.put("col" + i, rowList.get(i));
                            }
                        }
                        rows.add(rowMap);
                    }
                }
            }

            // Handle different types of columns data
            if (columnsObj instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> rawColumns = (List<Object>) columnsObj;
                columns = new ArrayList<>();

                for (Object colObj : rawColumns) {
                    if (colObj instanceof String) {
                        columns.add((String) colObj);
                    } else {
                        columns.add(colObj.toString());
                    }
                }
            }

            // If we couldn't parse the data properly, show error
            if (rows == null || columns == null) {
                showMessage("Error: Unable to parse query results. Raw data: " + result);
                return;
            }

            if (rows.isEmpty()) {
                showMessage("Query executed successfully. No results to display.");
                return;
            }

            // Clear existing data
            tableView.getColumns().clear();
            tableView.getItems().clear();

            // Add columns
            for (String column : columns) {
                TableColumn<ObservableList<String>, String> tableColumn = new TableColumn<>(column);
                final int columnIndex = tableView.getColumns().size();

                tableColumn.setCellValueFactory(param -> {
                    ObservableList<String> row = param.getValue();
                    if (row == null || columnIndex >= row.size()) {
                        return new SimpleStringProperty("");
                    }
                    return new SimpleStringProperty(row.get(columnIndex));
                });

                tableView.getColumns().add(tableColumn);
            }

            // Add rows
            for (Map<String, Object> row : rows) {
                ObservableList<String> observableRow = FXCollections.observableArrayList();

                for (String column : columns) {
                    Object value = row.get(column);
                    observableRow.add(value != null ? value.toString() : "NULL");
                }

                tableView.getItems().add(observableRow);
            }

            // Show the results tab
            tabPane.getSelectionModel().select(0);

            // Show summary message
            showMessage("Query executed successfully. " + rows.size() + " rows returned.");

        } catch (Exception e) {
            System.err.println("Error displaying result set: " + e.getMessage());
            e.printStackTrace();
            showError("Error displaying results: " + e.getMessage() + ". Raw data: " + result);
        }
    }

    private void showMessage(String message) {
        messageArea.appendText(message + "\n");
        tabPane.getSelectionModel().select(1);
    }

    public void clear() {
        tableView.getColumns().clear();
        tableView.getItems().clear();
        messageArea.clear();
    }

    public void showLoading() {
        ProgressIndicator progressIndicator = new ProgressIndicator();
        tableView.setPlaceholder(progressIndicator);
        if (statusLabel != null) {
            statusLabel.setText("Executing query...");
        }
    }

    public void showError(String message) {
        tableView.setPlaceholder(new Label("Error: " + message));
        if (statusLabel != null) {
            statusLabel.setText("Error: " + message);
        }
    }
}