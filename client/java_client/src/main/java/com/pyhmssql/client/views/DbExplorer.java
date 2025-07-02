package com.pyhmssql.client.views;

import com.pyhmssql.client.main.ConnectionManager;
import javafx.application.Platform;
import javafx.scene.control.*;
import javafx.scene.layout.VBox;
import javafx.geometry.Insets;

import java.util.List;
import java.util.Map;

/**
 * Database explorer tree view component
 */
public class DbExplorer extends VBox {
    private final ConnectionManager connectionManager;
    private TreeView<String> treeView;
    private TreeItem<String> rootItem;

    public DbExplorer(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        setupUI();
        loadDatabases();
    }

    private void setupUI() {
        setPadding(new Insets(10));
        setSpacing(5);

        // Create tree view
        rootItem = new TreeItem<>("Databases");
        rootItem.setExpanded(true);
        treeView = new TreeView<>(rootItem);
        treeView.setShowRoot(true);
        treeView.setPrefHeight(400);

        // Add refresh button
        Button refreshButton = new Button("Refresh");
        refreshButton.setOnAction(e -> refreshDatabases());

        getChildren().addAll(refreshButton, treeView);
    }

    private void loadDatabases() {
        if (connectionManager == null)
            return;

        connectionManager.getDatabases().thenAccept(result -> {
            Platform.runLater(() -> {
                rootItem.getChildren().clear();

                if (result.containsKey("databases")) {
                    @SuppressWarnings("unchecked")
                    List<String> databases = (List<String>) result.get("databases");

                    for (String dbName : databases) {
                        TreeItem<String> dbItem = new TreeItem<>(dbName);
                        rootItem.getChildren().add(dbItem);

                        // Load tables for this database
                        loadTables(dbItem, dbName);
                    }
                }
            });
        }).exceptionally(ex -> {
            Platform.runLater(() -> {
                TreeItem<String> errorItem = new TreeItem<>("Error loading databases");
                rootItem.getChildren().add(errorItem);
            });
            return null;
        });
    }

    private void loadTables(TreeItem<String> dbItem, String database) {
        connectionManager.getTables(database).thenAccept(result -> {
            Platform.runLater(() -> {
                if (result.containsKey("tables")) {
                    @SuppressWarnings("unchecked")
                    List<String> tables = (List<String>) result.get("tables");

                    for (String tableName : tables) {
                        TreeItem<String> tableItem = new TreeItem<>(tableName);
                        dbItem.getChildren().add(tableItem);
                    }
                }
            });
        });
    }

    public void refreshDatabases() {
        loadDatabases();
    }
}