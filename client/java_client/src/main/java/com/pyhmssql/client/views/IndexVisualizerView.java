package com.pyhmssql.client.views;

import com.pyhmssql.client.main.ConnectionManager;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.scene.web.WebView;
import javafx.scene.web.WebEngine;
import javafx.geometry.Insets;
import java.util.Map;

/**
 * Component for visualizing B+ tree indexes
 */
public class IndexVisualizerView extends BorderPane {
    private ConnectionManager connectionManager;
    private ComboBox<String> indexComboBox;
    private ComboBox<String> tableComboBox;
    private WebView webView;
    private Button visualizeButton;

    public IndexVisualizerView(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        setupUI();
    }

    private void setupUI() {
        setPadding(new Insets(10));

        // Top control panel
        GridPane controlPanel = new GridPane();
        controlPanel.setHgap(10);
        controlPanel.setVgap(10);
        controlPanel.setPadding(new Insets(5));

        // Table selector
        Label tableLabel = new Label("Table:");
        tableComboBox = new ComboBox<>();
        tableComboBox.setPromptText("Select Table");
        tableComboBox.setOnAction(e -> loadIndexesForTable());

        // Index selector
        Label indexLabel = new Label("Index:");
        indexComboBox = new ComboBox<>();
        indexComboBox.setPromptText("Select Index");

        // Visualize button
        visualizeButton = new Button("Visualize");
        visualizeButton.setOnAction(e -> visualizeIndex());

        // Add to control panel
        controlPanel.add(tableLabel, 0, 0);
        controlPanel.add(tableComboBox, 1, 0);
        controlPanel.add(indexLabel, 0, 1);
        controlPanel.add(indexComboBox, 1, 1);
        controlPanel.add(visualizeButton, 2, 1);

        // Web view for visualization
        webView = new WebView();
        webView.setPrefSize(800, 600);
        WebEngine webEngine = webView.getEngine();

        // Layout
        setTop(controlPanel);
        setCenter(webView);

        // Load tables when database changes
        loadTables();
    }

    private void loadTables() {
        String currentDatabase = connectionManager.getCurrentDatabase();
        if (currentDatabase == null || currentDatabase.isEmpty()) {
            return;
        }

        connectionManager.getTables(currentDatabase)
                .thenAccept(result -> {
                    if (result.containsKey("tables")) {
                        @SuppressWarnings("unchecked")
                        java.util.List<String> tables = (java.util.List<String>) result.get("tables");

                        javafx.application.Platform.runLater(() -> {
                            tableComboBox.getItems().clear();
                            tableComboBox.getItems().addAll(tables);
                        });
                    }
                });
    }

    private void loadIndexesForTable() {
        String currentDatabase = connectionManager.getCurrentDatabase();
        String selectedTable = tableComboBox.getValue();

        if (currentDatabase == null || currentDatabase.isEmpty() ||
                selectedTable == null || selectedTable.isEmpty()) {
            return;
        }

        connectionManager.getIndexes(currentDatabase, selectedTable)
                .thenAccept(result -> {
                    if (result.containsKey("indexes")) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> indexes = (Map<String, Object>) result.get("indexes");

                        javafx.application.Platform.runLater(() -> {
                            indexComboBox.getItems().clear();
                            indexComboBox.getItems().addAll(indexes.keySet());

                            if (!indexComboBox.getItems().isEmpty()) {
                                indexComboBox.setValue(indexComboBox.getItems().get(0));
                            }
                        });
                    }
                });
    }

    private void visualizeIndex() {
        String currentDatabase = connectionManager.getCurrentDatabase();
        String selectedTable = tableComboBox.getValue();
        String selectedIndex = indexComboBox.getValue();

        if (currentDatabase == null || currentDatabase.isEmpty() ||
                selectedTable == null || selectedTable.isEmpty() ||
                selectedIndex == null || selectedIndex.isEmpty()) {
            showAlert("Please select a database, table, and index to visualize.");
            return;
        }

        connectionManager.visualizeBPTree(currentDatabase, selectedIndex, selectedTable)
                .thenAccept(result -> {
                    if (result.containsKey("visualization")) {
                        String htmlContent = (String) result.get("visualization");

                        javafx.application.Platform.runLater(() -> {
                            webView.getEngine().loadContent(htmlContent);
                        });
                    } else if (result.containsKey("error")) {
                        showAlert("Visualization error: " + result.get("error"));
                    }
                });
    }

    private void showAlert(String message) {
        javafx.application.Platform.runLater(() -> {
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setTitle("Warning");
            alert.setHeaderText(null);
            alert.setContentText(message);
            alert.showAndWait();
        });
    }

    public void refresh() {
        loadTables();
    }
}