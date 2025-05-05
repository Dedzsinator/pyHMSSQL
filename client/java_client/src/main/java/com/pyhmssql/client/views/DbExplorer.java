package com.pyhmssql.client.views;

import com.pyhmssql.client.main.ConnectionManager;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.scene.input.MouseEvent;
import javafx.scene.input.MouseButton;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

public class DbExplorer extends VBox {
    private TreeView<String> treeView;
    private TreeItem<String> rootItem;
    private TreeItem<String> tableNode = null;
    private ConnectionManager connectionManager;
    private String selectedDatabase;

    // Store tree items for easy reference
    private Map<String, TreeItem<String>> databaseNodes = new HashMap<>();

    public DbExplorer(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;

        // Set up the UI
        setPrefWidth(250);
        setMinWidth(200);

        // Create tree view
        rootItem = new TreeItem<>("Databases");
        rootItem.setExpanded(true);

        treeView = new TreeView<>(rootItem);
        treeView.setShowRoot(true);

        // Add context menu for tree items
        treeView.setOnMouseClicked(this::handleMouseClick);

        getChildren().add(treeView);

        // Add toolbar with refresh button
        Button refreshButton = new Button("Refresh");
        refreshButton.setOnAction(e -> refreshDatabases());

        ToolBar toolBar = new ToolBar(refreshButton);
        getChildren().add(0, toolBar);
    }

    public void refreshDatabases() {
        // Clear existing items
        rootItem.getChildren().clear();
        databaseNodes.clear();

        // Request databases from the server
        connectionManager.showAllTables()
                .thenAccept(result -> {
                    if (result.containsKey("error")) {
                        showError((String) result.get("error"));
                        return;
                    }

                    // Check for the new response format with direct columns and rows fields
                    if (result.containsKey("columns") && result.containsKey("rows")) {
                        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
                        if (rows != null) {
                            // Group tables by database
                            Map<String, List<String>> databaseTableMap = new HashMap<>();

                            for (List<Object> row : rows) {
                                if (row.size() >= 2) {
                                    String dbName = row.get(0).toString();
                                    String tableName = row.get(1).toString();

                                    // Add table to database map
                                    if (!databaseTableMap.containsKey(dbName)) {
                                        databaseTableMap.put(dbName, new ArrayList<>());
                                    }
                                    databaseTableMap.get(dbName).add(tableName);
                                }
                            }

                            // Create tree nodes for each database and its tables
                            for (Map.Entry<String, List<String>> entry : databaseTableMap.entrySet()) {
                                String dbName = entry.getKey();
                                List<String> tables = entry.getValue();

                                // Create database node
                                TreeItem<String> dbNode = new TreeItem<>(dbName);
                                rootItem.getChildren().add(dbNode);
                                databaseNodes.put(dbName, dbNode);

                                // Create table nodes directly
                                for (String tableName : tables) {
                                    TreeItem<String> tableNode = new TreeItem<>(tableName);
                                    dbNode.getChildren().add(tableNode);
                                    // Add placeholder for columns
                                    tableNode.getChildren().add(new TreeItem<>("Loading..."));
                                }
                            }
                        }
                    } else {
                        // Process the rows returned from "SHOW DATABASES" command (old format)
                        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
                        if (rows != null) {
                            for (List<Object> row : rows) {
                                if (!row.isEmpty()) {
                                    String db = row.get(0).toString();
                                    TreeItem<String> dbNode = new TreeItem<>(db);
                                    rootItem.getChildren().add(dbNode);
                                    databaseNodes.put(db, dbNode);

                                    // Automatically query tables for each database
                                    loadTables(db);
                                }
                            }
                        } else {
                            // Fall back to old format if rows not found
                            List<String> databases = (List<String>) result.get("databases");
                            if (databases != null) {
                                for (String db : databases) {
                                    TreeItem<String> dbNode = new TreeItem<>(db);
                                    rootItem.getChildren().add(dbNode);
                                    databaseNodes.put(db, dbNode);

                                    // Automatically query tables for each database
                                    loadTables(db);
                                }
                            }
                        }
                    }
                });
    }

    private void loadTables(String database) {
        TreeItem<String> dbNode = databaseNodes.get(database);
        if (dbNode == null)
            return;

        // Clear existing items and add a loading indicator
        dbNode.getChildren().clear();
        dbNode.getChildren().add(new TreeItem<>("Loading..."));

        // Request tables from the server
        connectionManager.getTables(database)
                .thenAccept(result -> {
                    // Clear loading indicator
                    dbNode.getChildren().clear();

                    if (result.containsKey("error")) {
                        showError((String) result.get("error"));
                        return;
                    }

                    // Add tables to tree
                    List<String> tables = (List<String>) result.get("tables");
                    if (tables != null) {
                        for (String table : tables) {
                            TreeItem<String> tableNode = new TreeItem<>(table);
                            dbNode.getChildren().add(tableNode);

                            // Add placeholder for columns
                            tableNode.getChildren().add(new TreeItem<>("Loading..."));
                        }
                    }
                });
    }

    private void loadColumns(String database, String table) {
        TreeItem<String> dbNode = databaseNodes.get(database);
        if (dbNode == null)
            return;

        // Find the table node
        for (TreeItem<String> node : dbNode.getChildren()) {
            if (node.getValue().equals(table)) {
                tableNode = node;
                break;
            }
        }

        if (tableNode == null)
            return;

        // Clear existing items and add a loading indicator
        tableNode.getChildren().clear();
        tableNode.getChildren().add(new TreeItem<>("Loading..."));

        // Request columns from the server
        connectionManager.getColumns(database, table)
                .thenAccept(result -> {
                    // Clear loading indicator
                    tableNode.getChildren().clear();

                    if (result.containsKey("error")) {
                        showError((String) result.get("error"));
                        return;
                    }

                    // Add columns to tree
                    Map<String, Object> columns = (Map<String, Object>) result.get("columns");
                    if (columns != null) {
                        for (Map.Entry<String, Object> entry : columns.entrySet()) {
                            String columnName = entry.getKey();
                            Map<String, String> columnInfo = (Map<String, String>) entry.getValue();
                            String columnType = columnInfo.get("type");

                            TreeItem<String> columnNode = new TreeItem<>(
                                    columnName + " (" + columnType + ")");
                            tableNode.getChildren().add(columnNode);
                        }
                    }
                });
    }

    private void handleMouseClick(MouseEvent event) {
        if (event.getButton() == MouseButton.PRIMARY && event.getClickCount() == 2) {
            TreeItem<String> selectedItem = treeView.getSelectionModel().getSelectedItem();
            if (selectedItem == null)
                return;

            // Handle double-click based on the type of node
            if (selectedItem.getParent() == rootItem) {
                // Database node
                String dbName = selectedItem.getValue();
                selectedDatabase = dbName;
                loadTables(dbName);
                selectedItem.setExpanded(true);
            } else if (selectedItem.getParent() != null && selectedItem.getParent().getParent() == rootItem) {
                // Table node
                String tableName = selectedItem.getValue();
                String dbName = selectedItem.getParent().getValue();
                loadColumns(dbName, tableName);
                selectedItem.setExpanded(true);
            }
        } else if (event.getButton() == MouseButton.SECONDARY) {
            // Show context menu
            TreeItem<String> selectedItem = treeView.getSelectionModel().getSelectedItem();
            if (selectedItem == null)
                return;

            // Create context menu based on the type of node
            ContextMenu contextMenu = new ContextMenu();

            if (selectedItem == rootItem) {
                // Root node (Databases)
                MenuItem refreshItem = new MenuItem("Refresh");
                refreshItem.setOnAction(e -> refreshDatabases());
                MenuItem newDbItem = new MenuItem("New Database...");
                newDbItem.setOnAction(e -> showNewDatabaseDialog());
                contextMenu.getItems().addAll(refreshItem, newDbItem);
            } else if (selectedItem.getParent() == rootItem) {
                // Database node
                String dbName = selectedItem.getValue();
                selectedDatabase = dbName;

                MenuItem refreshItem = new MenuItem("Refresh");
                refreshItem.setOnAction(e -> loadTables(dbName));
                MenuItem newTableItem = new MenuItem("New Table...");
                newTableItem.setOnAction(e -> showNewTableDialog(dbName));
                MenuItem dropDbItem = new MenuItem("Drop Database");
                dropDbItem.setOnAction(e -> dropDatabase(dbName));
                contextMenu.getItems().addAll(refreshItem, newTableItem, new SeparatorMenuItem(), dropDbItem);
            } else if (selectedItem.getParent() != null && selectedItem.getParent().getParent() == rootItem) {
                // Table node
                String tableName = selectedItem.getValue();
                String dbName = selectedItem.getParent().getValue();

                MenuItem refreshItem = new MenuItem("Refresh");
                refreshItem.setOnAction(e -> loadColumns(dbName, tableName));
                MenuItem selectDataItem = new MenuItem("Select Top 1000 Rows");
                selectDataItem.setOnAction(e -> executeSelectQuery(dbName, tableName));
                MenuItem newQueryItem = new MenuItem("New Query");
                newQueryItem.setOnAction(e -> openNewQueryTab(dbName, tableName));
                MenuItem dropTableItem = new MenuItem("Drop Table");
                dropTableItem.setOnAction(e -> dropTable(dbName, tableName));
                contextMenu.getItems().addAll(refreshItem, selectDataItem, newQueryItem, new SeparatorMenuItem(),
                        dropTableItem);
            }

            // Show the context menu
            contextMenu.show(treeView, event.getScreenX(), event.getScreenY());
        }
    }

    private void showNewDatabaseDialog() {
        TextInputDialog dialog = new TextInputDialog();
        dialog.setTitle("Create Database");
        dialog.setHeaderText("Create a new database");
        dialog.setContentText("Database name:");

        dialog.showAndWait().ifPresent(inputName -> {
            // Create a final copy of the variable that can be used in the lambda
            if (!inputName.isEmpty()) {
                executeQuery("CREATE DATABASE " + inputName);
            }
        });
    }

    private void showNewTableDialog(String dbName) {
        TextInputDialog dialog = new TextInputDialog();
        dialog.setTitle("Create Table");
        dialog.setHeaderText("Create a new table in database " + dbName);
        dialog.setContentText("SQL Statement:");

        dialog.showAndWait().ifPresent(inputSql -> {
            // Create a final copy of the variable that can be used in the lambda
            if (!inputSql.isEmpty()) {
                executeQuery(inputSql);
            }
        });
    }

    private void dropDatabase(String dbName) {
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Drop Database");
        alert.setHeaderText("Drop database " + dbName + "?");
        alert.setContentText("This action cannot be undone!");

        alert.showAndWait().ifPresent(result -> {
            if (result == ButtonType.OK) {
                executeQuery("DROP DATABASE " + dbName);
            }
        });
    }

    private void dropTable(String dbName, String tableName) {
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle("Drop Table");
        alert.setHeaderText("Drop table " + tableName + " from database " + dbName + "?");
        alert.setContentText("This action cannot be undone!");

        alert.showAndWait().ifPresent(result -> {
            if (result == ButtonType.OK) {
                executeQuery("DROP TABLE " + dbName + "." + tableName);
            }
        });
    }

    private void executeSelectQuery(String dbName, String tableName) {
        // This would typically open a new query tab with the SELECT statement
        String query = "SELECT TOP 1000 * FROM " + dbName + "." + tableName;
        openNewQueryTab(dbName, tableName, query);
    }

    private void openNewQueryTab(String dbName, String tableName) {
        openNewQueryTab(dbName, tableName, null);
    }

    private void openNewQueryTab(String dbName, String tableName, String query) {
        // This would be implemented in the main application to open a new query tab
        // For now, we'll just execute the query if provided
        if (query != null) {
            executeQuery(query);
        }
    }

    private CompletableFuture<Map<String, Object>> executeQuery(String query) {
        return connectionManager.executeQuery(query)
                .thenApply(result -> {
                    if (result.containsKey("error")) {
                        showError((String) result.get("error"));
                    } else {
                        refreshDatabases(); // Refresh the tree after modifications
                    }
                    return result;
                });
    }

    private void showError(String error) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("Error");
        alert.setHeaderText(null);
        alert.setContentText(error);
        alert.showAndWait();
    }

    public String getSelectedDatabase() {
        return selectedDatabase;
    }
}