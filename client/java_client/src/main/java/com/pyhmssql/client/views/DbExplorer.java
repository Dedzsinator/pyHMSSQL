package com.pyhmssql.client.views;

import com.pyhmssql.client.main.ConnectionManager;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import org.kordamp.ikonli.javafx.FontIcon;
import org.kordamp.ikonli.material2.Material2AL;
import org.kordamp.ikonli.material2.Material2MZ;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Database explorer tree view showing databases, tables, and columns
 */
public class DbExplorer extends VBox {
    private final ConnectionManager connectionManager;
    private TreeView<String> treeView;
    private ProgressIndicator loadingIndicator;
    private Label statusLabel;

    public DbExplorer(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        initializeUI();
        setupEventHandlers();
    }

    private void initializeUI() {
        setPadding(new Insets(10));
        setSpacing(5);

        // Header
        HBox header = new HBox(10);
        header.setAlignment(javafx.geometry.Pos.CENTER_LEFT);

        Label titleLabel = new Label("Database Explorer");
        titleLabel.setStyle("-fx-font-weight: bold; -fx-font-size: 14px;");

        Button refreshBtn = new Button("", new FontIcon(Material2AL.AUTORENEW));
        refreshBtn.setTooltip(new Tooltip("Refresh databases"));
        refreshBtn.setOnAction(e -> refreshDatabases());

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        header.getChildren().addAll(titleLabel, spacer, refreshBtn);

        // Tree view
        treeView = new TreeView<>();
        treeView.setShowRoot(false);
        treeView.setCellFactory(tv -> new DatabaseTreeCell());

        // Loading indicator
        loadingIndicator = new ProgressIndicator();
        loadingIndicator.setMaxSize(30, 30);
        loadingIndicator.setVisible(false);

        // Status label
        statusLabel = new Label("Not connected");
        statusLabel.setStyle("-fx-text-fill: #666666; -fx-font-size: 11px;");

        VBox.setVgrow(treeView, Priority.ALWAYS);
        getChildren().addAll(header, treeView, loadingIndicator, statusLabel);
    }

    private void setupEventHandlers() {
        // Listen for connection changes
        connectionManager.addConnectionListener(connected -> {
            Platform.runLater(() -> {
                if (connected) {
                    statusLabel.setText("Connected");
                    refreshDatabases();
                } else {
                    statusLabel.setText("Not connected");
                    treeView.setRoot(null);
                }
            });
        });

        // Handle tree item selection
        treeView.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            if (newVal != null) {
                handleTreeSelection(newVal);
            }
        });
    }

    public void refreshDatabases() {
        if (!connectionManager.isConnected()) {
            statusLabel.setText("Not connected");
            return;
        }

        loadingIndicator.setVisible(true);
        statusLabel.setText("Loading databases...");

        connectionManager.getDatabases().thenAccept(response -> {
            Platform.runLater(() -> {
                loadingIndicator.setVisible(false);

                if (response.containsKey("error")) {
                    statusLabel.setText("Error: " + response.get("error"));
                    return;
                }

                // Handle different response formats from the server
                List<String> databases = new ArrayList<>();

                if (response.containsKey("databases")) {
                    @SuppressWarnings("unchecked")
                    List<String> dbList = (List<String>) response.get("databases");
                    databases.addAll(dbList);
                } else if (response.containsKey("rows")) {
                    @SuppressWarnings("unchecked")
                    List<Object> rows = (List<Object>) response.get("rows");
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

                if (!databases.isEmpty()) {
                    updateDatabaseTree(databases);
                    statusLabel.setText(databases.size() + " databases found");
                } else {
                    statusLabel.setText("No databases found");
                }
            });
        }).exceptionally(throwable -> {
            Platform.runLater(() -> {
                loadingIndicator.setVisible(false);
                statusLabel.setText("Failed to load databases: " + throwable.getMessage());
            });
            return null;
        });
    }

    private void updateDatabaseTree(List<String> databases) {
        TreeItem<String> root = new TreeItem<>("Databases");

        for (String database : databases) {
            TreeItem<String> dbItem = new TreeItem<>(database);
            dbItem.setGraphic(new FontIcon(Material2MZ.STORAGE));

            // Add placeholder for tables
            TreeItem<String> loadingItem = new TreeItem<>("Loading...");
            dbItem.getChildren().add(loadingItem);

            // Load tables when database is expanded
            dbItem.expandedProperty().addListener((obs, wasExpanded, isExpanded) -> {
                if (isExpanded && dbItem.getChildren().size() == 1 &&
                        "Loading...".equals(dbItem.getChildren().get(0).getValue())) {
                    loadTables(dbItem, database);
                }
            });

            root.getChildren().add(dbItem);
        }

        treeView.setRoot(root);
    }

    private void loadTables(TreeItem<String> dbItem, String database) {
        connectionManager.getTables(database).thenAccept(response -> {
            Platform.runLater(() -> {
                dbItem.getChildren().clear();

                if (response.containsKey("error")) {
                    TreeItem<String> errorItem = new TreeItem<>("Error: " + response.get("error"));
                    dbItem.getChildren().add(errorItem);
                    return;
                }

                if (response.containsKey("tables")) {
                    @SuppressWarnings("unchecked")
                    List<String> tables = (List<String>) response.get("tables");

                    for (String table : tables) {
                        TreeItem<String> tableItem = new TreeItem<>(table);
                        tableItem.setGraphic(new FontIcon(Material2AL.APPS));

                        // Add placeholder for columns
                        TreeItem<String> loadingItem = new TreeItem<>("Loading...");
                        tableItem.getChildren().add(loadingItem);

                        // Load columns when table is expanded
                        tableItem.expandedProperty().addListener((obs, wasExpanded, isExpanded) -> {
                            if (isExpanded && tableItem.getChildren().size() == 1 &&
                                    "Loading...".equals(tableItem.getChildren().get(0).getValue())) {
                                loadColumns(tableItem, database, table);
                            }
                        });

                        dbItem.getChildren().add(tableItem);
                    }
                } else {
                    TreeItem<String> noTablesItem = new TreeItem<>("No tables");
                    dbItem.getChildren().add(noTablesItem);
                }
            });
        });
    }

    private void loadColumns(TreeItem<String> tableItem, String database, String table) {
        connectionManager.getColumns(database, table).thenAccept(response -> {
            Platform.runLater(() -> {
                tableItem.getChildren().clear();

                if (response.containsKey("error")) {
                    TreeItem<String> errorItem = new TreeItem<>("Error: " + response.get("error"));
                    tableItem.getChildren().add(errorItem);
                    return;
                }

                if (response.containsKey("columns")) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> columns = (List<Map<String, Object>>) response.get("columns");

                    for (Map<String, Object> column : columns) {
                        String columnName = (String) column.get("name");
                        String columnType = (String) column.get("type");
                        String displayText = columnName + " (" + columnType + ")";

                        TreeItem<String> columnItem = new TreeItem<>(displayText);
                        columnItem.setGraphic(new FontIcon(Material2AL.ACCOUNT_BOX));

                        tableItem.getChildren().add(columnItem);
                    }
                } else {
                    TreeItem<String> noColumnsItem = new TreeItem<>("No columns");
                    tableItem.getChildren().add(noColumnsItem);
                }
            });
        });
    }

    private void handleTreeSelection(TreeItem<String> item) {
        if (item == null)
            return;

        String value = item.getValue();
        TreeItem<String> parent = item.getParent();

        // Determine what was selected and show context menu or perform action
        if (parent != null && parent.getParent() != null) {
            // This is likely a column
            TreeItem<String> tableItem = parent;
            TreeItem<String> dbItem = parent.getParent();

            if (dbItem.getParent() != null) { // Ensure we have the right hierarchy
                String database = dbItem.getValue();
                String table = tableItem.getValue();
                // Could show column details or generate SELECT query
            }
        } else if (parent != null && parent.getValue().equals("Databases")) {
            // This is a database
            connectionManager.setCurrentDatabase(value);
        }
    }

    /**
     * Custom tree cell for database objects
     */
    private static class DatabaseTreeCell extends TreeCell<String> {
        @Override
        protected void updateItem(String item, boolean empty) {
            super.updateItem(item, empty);

            if (empty || item == null) {
                setText(null);
                setGraphic(null);
            } else {
                setText(item);

                // Set icon based on tree level
                TreeItem<String> treeItem = getTreeItem();
                if (treeItem != null) {
                    if (treeItem.getParent() == null) {
                        // Root level
                        setGraphic(new FontIcon(Material2AL.FOLDER));
                    } else if (treeItem.getParent().getValue().equals("Databases")) {
                        // Database level
                        setGraphic(new FontIcon(Material2MZ.STORAGE));
                    } else if (treeItem.getParent().getParent() != null &&
                            treeItem.getParent().getParent().getValue().equals("Databases")) {
                        // Table level
                        setGraphic(new FontIcon(Material2AL.APPS));
                    } else {
                        // Column level
                        setGraphic(new FontIcon(Material2AL.ACCOUNT_BOX));
                    }
                }
            }
        }
    }
}