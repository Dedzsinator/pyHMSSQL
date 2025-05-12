package com.pyhmssql.client.views;

import com.pyhmssql.client.main.ConnectionManager;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.scene.web.WebView;
import javafx.scene.web.WebEngine;
import javafx.geometry.Insets;
import java.util.Map;
import javafx.application.Platform;

/**
 * Component for visualizing B+ tree indexes
 */
public class IndexVisualizerView extends BorderPane {
    private ConnectionManager connectionManager;
    private ComboBox<String> indexComboBox;
    private ComboBox<String> tableComboBox;
    private ComboBox<String> databaseComboBox; // Added database ComboBox
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

        // Database selector
        Label dbLabel = new Label("Database:");
        databaseComboBox = new ComboBox<>();
        databaseComboBox.setPromptText("Select Database");
        databaseComboBox.setPrefWidth(150);
        databaseComboBox.setOnAction(e -> {
            String selectedDatabase = databaseComboBox.getValue();
            if (selectedDatabase != null && !selectedDatabase.isEmpty()) {
                connectionManager.setCurrentDatabase(selectedDatabase);
                loadTables();
                // Clear other selections
                tableComboBox.getItems().clear();
                tableComboBox.setPromptText("Select Table");
                indexComboBox.getItems().clear();
                indexComboBox.setPromptText("Select Index");
            }
        });

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

        // Add to control panel - adding database selector first
        controlPanel.add(dbLabel, 0, 0);
        controlPanel.add(databaseComboBox, 1, 0);
        controlPanel.add(tableLabel, 0, 1);
        controlPanel.add(tableComboBox, 1, 1);
        controlPanel.add(indexLabel, 0, 2);
        controlPanel.add(indexComboBox, 1, 2);
        controlPanel.add(visualizeButton, 2, 2);

        // Web view for visualization
        webView = new WebView();
        webView.setPrefSize(800, 600);
        WebEngine webEngine = webView.getEngine();

        // Layout
        setTop(controlPanel);
        setCenter(webView);

        // Load databases immediately
        loadDatabases();
    }

    // New method to load available databases
    private void loadDatabases() {
        // Show a loading message
        Platform.runLater(() -> {
            databaseComboBox.setPromptText("Loading databases...");
        });

        connectionManager.getDatabases()
                .thenAccept(result -> {
                    System.out.println("Database response: " + result); // Debug output

                    // Check for standard "databases" key
                    if (result.containsKey("databases")) {
                        @SuppressWarnings("unchecked")
                        java.util.List<String> databases = (java.util.List<String>) result.get("databases");
                        updateDatabaseDropdown(databases);
                    }
                    // Check for results in "rows" format (common alternate format)
                    else if (result.containsKey("rows")) {
                        @SuppressWarnings("unchecked")
                        java.util.List<java.util.List<?>> rows = (java.util.List<java.util.List<?>>) result.get("rows");

                        java.util.List<String> databases = new java.util.ArrayList<>();
                        for (java.util.List<?> row : rows) {
                            if (!row.isEmpty()) {
                                databases.add(row.get(0).toString());
                            }
                        }
                        updateDatabaseDropdown(databases);
                    }
                    // If no recognized format, try direct execution of SHOW DATABASES
                    else {
                        showAlert("Could not retrieve databases. Please try again.");
                        Platform.runLater(() -> {
                            databaseComboBox.setPromptText("Select Database");
                        });

                        // Try fallback direct query
                        connectionManager.executeQuery("SHOW DATABASES")
                                .thenAccept(fallbackResult -> {
                                    System.out.println("Fallback database response: " + fallbackResult); // Debug
                                    if (fallbackResult.containsKey("rows")) {
                                        @SuppressWarnings("unchecked")
                                        java.util.List<java.util.List<?>> rows = (java.util.List<java.util.List<?>>) fallbackResult
                                                .get("rows");

                                        java.util.List<String> databases = new java.util.ArrayList<>();
                                        for (java.util.List<?> row : rows) {
                                            if (!row.isEmpty()) {
                                                databases.add(row.get(0).toString());
                                            }
                                        }
                                        updateDatabaseDropdown(databases);
                                    }
                                });
                    }
                })
                .exceptionally(ex -> {
                    System.err.println("Error loading databases: " + ex.getMessage());
                    ex.printStackTrace();
                    Platform.runLater(() -> {
                        showAlert("Error loading databases: " + ex.getMessage());
                        databaseComboBox.setPromptText("Error loading databases");
                    });
                    return null;
                });
    }

    // Helper method to update the database dropdown UI
    private void updateDatabaseDropdown(java.util.List<String> databases) {
        Platform.runLater(() -> {
            databaseComboBox.getItems().clear();

            if (databases != null && !databases.isEmpty()) {
                databaseComboBox.getItems().addAll(databases);

                // If there's a current database in the connection manager, select it
                String currentDb = connectionManager.getCurrentDatabase();
                if (currentDb != null && !currentDb.isEmpty() &&
                        databaseComboBox.getItems().contains(currentDb)) {
                    databaseComboBox.setValue(currentDb);
                    loadTables(); // Load tables for this database
                } else if (!databases.isEmpty()) {
                    // If no current database, select the first one
                    databaseComboBox.setValue(databases.get(0));
                    connectionManager.setCurrentDatabase(databases.get(0));
                    loadTables();
                }

                databaseComboBox.setPromptText("Select Database");
            } else {
                databaseComboBox.setPromptText("No databases available");
            }
        });
    }

    private void loadTables() {
        String currentDatabase = connectionManager.getCurrentDatabase();
        if (currentDatabase == null || currentDatabase.isEmpty()) {
            // Get the selected database from the ComboBox
            currentDatabase = databaseComboBox.getValue();
            if (currentDatabase == null || currentDatabase.isEmpty()) {
                showAlert("Please select a database first.");
                return;
            }
            // Set the current database in the connection manager
            connectionManager.setCurrentDatabase(currentDatabase);
        }

        connectionManager.getTables(currentDatabase)
                .thenAccept(result -> {
                    if (result.containsKey("tables")) {
                        @SuppressWarnings("unchecked")
                        java.util.List<String> tables = (java.util.List<String>) result.get("tables");

                        Platform.runLater(() -> {
                            tableComboBox.getItems().clear();
                            tableComboBox.getItems().addAll(tables);
                        });
                    }
                });
    }

    private void loadIndexesForTable() {
        String currentDatabase = connectionManager.getCurrentDatabase();
        String selectedTable = tableComboBox.getValue();

        if (currentDatabase == null || currentDatabase.isEmpty()) {
            currentDatabase = databaseComboBox.getValue();
            if (currentDatabase == null || currentDatabase.isEmpty()) {
                showAlert("Please select a database first.");
                return;
            }
            connectionManager.setCurrentDatabase(currentDatabase);
        }

        if (selectedTable == null || selectedTable.isEmpty()) {
            return;
        }

        connectionManager.getIndexes(currentDatabase, selectedTable)
                .thenAccept(result -> {
                    if (result.containsKey("indexes")) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> indexes = (Map<String, Object>) result.get("indexes");

                        Platform.runLater(() -> {
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

        if (currentDatabase == null || currentDatabase.isEmpty()) {
            currentDatabase = databaseComboBox.getValue();
            if (currentDatabase == null || currentDatabase.isEmpty()) {
                showAlert("Please select a database.");
                return;
            }
            connectionManager.setCurrentDatabase(currentDatabase);
        }

        if (selectedTable == null || selectedTable.isEmpty() ||
                selectedIndex == null || selectedIndex.isEmpty()) {
            showAlert("Please select a table and index to visualize.");
            return;
        }

        connectionManager.visualizeBPTree(currentDatabase, selectedIndex, selectedTable)
                .thenAccept(result -> {
                    if (result.containsKey("visualization")) {
                        String htmlContent = (String) result.get("visualization");

                        Platform.runLater(() -> {
                            webView.getEngine().loadContent(htmlContent);
                        });
                    } else if (result.containsKey("error")) {
                        showAlert("Visualization error: " + result.get("error"));
                    }
                });
    }

    private void showAlert(String message) {
        Platform.runLater(() -> {
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setTitle("Warning");
            alert.setHeaderText(null);
            alert.setContentText(message);
            alert.showAndWait();
        });
    }

    public void refresh() {
        loadDatabases();
    }
}