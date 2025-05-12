package com.pyhmssql.client.main;

import com.pyhmssql.client.views.*;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.application.Platform;
import javafx.scene.control.Alert.AlertType;
import java.util.List;

public class MainWindow extends BorderPane {

    private MenuBar menuBar;
    private TabPane tabPane;
    private DbExplorer dbExplorer;
    private ConnectionManager connectionManager;

    public MainWindow() {
        connectionManager = new ConnectionManager();

        // Initialize UI components
        initMenuBar();
        initTabPane();
        initDbExplorer();

        // Start with login panel
        showLoginPanel();
    }

    private void initMenuBar() {
        menuBar = new MenuBar();

        // File menu
        Menu fileMenu = new Menu("File");
        MenuItem connectItem = new MenuItem("Connect to Server");
        connectItem.setOnAction(e -> showLoginPanel());
        MenuItem exitItem = new MenuItem("Exit");
        exitItem.setOnAction(e -> System.exit(0));
        fileMenu.getItems().addAll(connectItem, new SeparatorMenuItem(), exitItem);

        // Edit menu
        Menu editMenu = new Menu("Edit");
        MenuItem copyItem = new MenuItem("Copy");
        copyItem.setOnAction(e -> {
            // Get the currently focused component
            if (getScene() != null && getScene().getFocusOwner() instanceof TextInputControl) {
                ((TextInputControl) getScene().getFocusOwner()).copy();
            }
        });
        MenuItem pasteItem = new MenuItem("Paste");
        pasteItem.setOnAction(e -> {
            // Get the currently focused component
            if (getScene() != null && getScene().getFocusOwner() instanceof TextInputControl) {
                ((TextInputControl) getScene().getFocusOwner()).paste();
            }
        });
        editMenu.getItems().addAll(copyItem, pasteItem);

        // Query menu
        Menu queryMenu = new Menu("Query");
        MenuItem executeItem = new MenuItem("Execute");
        executeItem.setOnAction(e -> {
            // Find active query editor and execute its query
            QueryEditor activeEditor = getCurrentQueryEditor();
            if (activeEditor != null) {
                activeEditor.executeQuery();
            }
        });
        MenuItem newQueryItem = new MenuItem("New Query");
        newQueryItem.setOnAction(e -> openNewQueryTab());
        queryMenu.getItems().addAll(executeItem, newQueryItem);

        // Tools menu
        Menu toolsMenu = new Menu("Tools");
        MenuItem queryBuilderItem = new MenuItem("Query Builder");
        queryBuilderItem.setOnAction(e -> openQueryBuilder());
        MenuItem indexViewerItem = new MenuItem("Index Viewer");
        indexViewerItem.setOnAction(e -> openIndexViewer());
        MenuItem transactionItem = new MenuItem("Transaction Manager");
        transactionItem.setOnAction(e -> openTransactionManager());
        MenuItem preferencesItem = new MenuItem("User Preferences");
        preferencesItem.setOnAction(e -> openPreferencesDialog());
        toolsMenu.getItems().addAll(queryBuilderItem, indexViewerItem, transactionItem, preferencesItem);

        // Database menu
        Menu databaseMenu = new Menu("Database");
        MenuItem refreshItem = new MenuItem("Refresh Databases");
        refreshItem.setOnAction(e -> {
            if (dbExplorer != null) {
                dbExplorer.refreshDatabases();
            }

            // Also refresh any open query editors
            for (Tab tab : tabPane.getTabs()) {
                if (tab.getContent() instanceof SplitPane) {
                    SplitPane splitPane = (SplitPane) tab.getContent();
                    for (javafx.scene.Node node : splitPane.getItems()) {
                        if (node instanceof QueryEditor) {
                            ((QueryEditor) node).refreshDatabases();
                        }
                    }
                }
            }
        });
        databaseMenu.getItems().add(refreshItem);

        menuBar.getMenus().addAll(fileMenu, editMenu, queryMenu, toolsMenu, databaseMenu);
        setTop(menuBar);
    }

    // Helper to get the current active query editor
    private QueryEditor getCurrentQueryEditor() {
        Tab selectedTab = tabPane.getSelectionModel().getSelectedItem();
        if (selectedTab != null && selectedTab.getContent() instanceof SplitPane) {
            SplitPane splitPane = (SplitPane) selectedTab.getContent();
            for (javafx.scene.Node node : splitPane.getItems()) {
                if (node instanceof QueryEditor) {
                    return (QueryEditor) node;
                }
            }
        }
        return null;
    }

    private void initTabPane() {
        tabPane = new TabPane();
        // Add tab pane styles to ensure it's clearly defined on the right side
        tabPane.setStyle("-fx-border-color: #cccccc; -fx-border-width: 0 0 0 1;");
        // Make sure tab pane expands to fill available space
        VBox.setVgrow(tabPane, Priority.ALWAYS);
        HBox.setHgrow(tabPane, Priority.ALWAYS);
    }

    private void initDbExplorer() {
        dbExplorer = new DbExplorer(connectionManager);

        // Add handler for Query Builder table events
        dbExplorer.addEventHandler(DbExplorer.QueryBuilderTableEvent.getEventTypeQB(), event -> {
            openTableInQueryBuilder(event.getDbName(), event.getTableName());
        });

        dbExplorer.addEventHandler(DbExplorer.NewQueryEvent.getEventTypeNQ(), event -> {
            String title = event.getTableName() != null
                    ? "Query - " + event.getTableName()
                    : "New Query";

            String query = event.getQuery() != null
                    ? event.getQuery()
                    : "";

            openNewQueryTab(title, query);
        });

        // Create split pane with correct divider position
        SplitPane splitPane = new SplitPane();
        splitPane.getItems().add(dbExplorer);
        splitPane.getItems().add(tabPane);

        // Set a more appropriate divider position to give more space to the tabPane
        splitPane.setDividerPositions(0.2);

        // Ensure the split pane fills the available space
        VBox.setVgrow(splitPane, Priority.ALWAYS);
        HBox.setHgrow(splitPane, Priority.ALWAYS);

        setCenter(splitPane);
    }

    private void showLoginPanel() {
        LoginPanel loginPanel = new LoginPanel(connectionManager);
        loginPanel.setOnLoginSuccess(() -> {
            Platform.runLater(() -> {
                if (dbExplorer != null) {
                    dbExplorer.refreshDatabases();
                }
                openNewQueryTab();
            });
        });

        Dialog<ButtonType> dialog = new Dialog<>();
        dialog.setTitle("Connect to Server");
        dialog.getDialogPane().setContent(loginPanel);
        dialog.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);

        // Add a "Discover" button to find servers on the network
        ButtonType discoverButtonType = new ButtonType("Discover Servers", ButtonBar.ButtonData.OTHER);
        dialog.getDialogPane().getButtonTypes().add(discoverButtonType);

        // Get the OK button and make it default
        Button okButton = (Button) dialog.getDialogPane().lookupButton(ButtonType.OK);
        okButton.setText("Login");
        okButton.setDefaultButton(true);

        // Always make sure the dialog is initialized before handling button actions
        Platform.runLater(() -> {
            dialog.setResultConverter(buttonType -> {
                if (buttonType == ButtonType.OK) {
                    // Instead of just closing, trigger the login process
                    loginPanel.login();
                    return null;
                } else if (buttonType == discoverButtonType) {
                    discoverServers();
                    return null;
                }
                return buttonType;
            });
        });

        dialog.showAndWait();
    }

    private void discoverServers() {
        // Show a waiting dialog
        Alert waitAlert = new Alert(AlertType.INFORMATION);
        waitAlert.setTitle("Server Discovery");
        waitAlert.setHeaderText("Discovering HMSSQL Servers...");
        waitAlert.setContentText("Please wait while scanning the network...");

        // We don't want to block with showAndWait, but we want to show it
        Platform.runLater(() -> waitAlert.show());

        // Start server discovery
        connectionManager.discoverServers().thenAccept(servers -> {
            // Close the waiting dialog
            Platform.runLater(() -> waitAlert.close());

            if (servers.isEmpty()) {
                Platform.runLater(() -> {
                    Alert alert = new Alert(AlertType.INFORMATION);
                    alert.setTitle("Server Discovery");
                    alert.setHeaderText("No Servers Found");
                    alert.setContentText("No HMSSQL servers were found on the network.");
                    alert.showAndWait();
                });
            } else {
                Platform.runLater(() -> showServerSelectionDialog(servers));
            }
        }).exceptionally(ex -> {
            // Close the waiting dialog and show error
            Platform.runLater(() -> {
                waitAlert.close();

                Alert alert = new Alert(AlertType.ERROR);
                alert.setTitle("Server Discovery");
                alert.setHeaderText("Discovery Error");
                alert.setContentText("Error discovering servers: " + ex.getMessage());
                alert.showAndWait();
            });

            return null;
        });
    }

    private void showServerSelectionDialog(List<ConnectionManager.ServerInfo> servers) {
        // Create a dialog for server selection
        Dialog<ConnectionManager.ServerInfo> dialog = new Dialog<>();
        dialog.setTitle("Select Server");
        dialog.setHeaderText("Select an HMSSQL Server to connect to:");

        // Set the button types
        ButtonType selectButtonType = new ButtonType("Select", ButtonBar.ButtonData.OK_DONE);
        dialog.getDialogPane().getButtonTypes().addAll(selectButtonType, ButtonType.CANCEL);

        // Create a list view for server selection
        ListView<ConnectionManager.ServerInfo> serverListView = new ListView<>();
        servers.forEach(server -> serverListView.getItems().add(server));
        serverListView.getSelectionModel().selectFirst();

        dialog.getDialogPane().setContent(serverListView);

        // Convert the result
        dialog.setResultConverter(dialogButton -> {
            if (dialogButton == selectButtonType) {
                return serverListView.getSelectionModel().getSelectedItem();
            }
            return null;
        });

        // Show the dialog and handle selection
        dialog.showAndWait().ifPresent(server -> {
            connectionManager.setServerDetails(server.getHost(), server.getPort());

            // Update the login panel with the selected server
            Dialog<ButtonType> loginDialog = new Dialog<>();
            loginDialog.setTitle("Login to " + server.getName());

            LoginPanel loginPanel = new LoginPanel(connectionManager);
            loginPanel.setOnLoginSuccess(() -> {
                if (dbExplorer != null) {
                    dbExplorer.refreshDatabases();
                }
                openNewQueryTab();
            });
            loginPanel.setServerInfo(server.getHost(), server.getPort());

            loginDialog.getDialogPane().setContent(loginPanel);
            loginDialog.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);

            // Get the OK button and make it default
            Button okButton = (Button) loginDialog.getDialogPane().lookupButton(ButtonType.OK);
            okButton.setText("Login");
            okButton.setDefaultButton(true);

            // Set up the result converter
            loginDialog.setResultConverter(buttonType -> {
                if (buttonType == ButtonType.OK) {
                    loginPanel.login();
                }
                return buttonType;
            });

            loginDialog.showAndWait();
        });
    }

    private void openNewQueryTab(String title, String sql) {
        // Ensure we're on the JavaFX thread
        if (!Platform.isFxApplicationThread()) {
            Platform.runLater(() -> openNewQueryTab(title, sql));
            return;
        }

        Tab queryTab = new Tab(title);
        queryTab.setClosable(true);

        // Create query editor with results pane
        SplitPane splitPane = new SplitPane();
        splitPane.setOrientation(javafx.geometry.Orientation.VERTICAL);

        QueryEditor queryEditor = new QueryEditor(connectionManager);
        ResultPane resultPane = new ResultPane();

        // Connect editor to results pane
        queryEditor.setOnExecuteQuery(resultPane::displayResults);

        // Add components to the SplitPane
        splitPane.getItems().add(queryEditor);
        splitPane.getItems().add(resultPane);
        splitPane.setDividerPositions(0.6);

        queryTab.setContent(splitPane);

        // Now that everything is initialized, set the query text
        if (sql != null && !sql.isEmpty()) {
            // Small delay to ensure the CodeArea is fully initialized
            Platform.runLater(() -> queryEditor.setQuery(sql));
        }

        // Set the database if available
        if (dbExplorer != null) {
            String selectedDb = dbExplorer.getSelectedDatabase();
            if (selectedDb != null && !selectedDb.isEmpty()) {
                queryEditor.setDatabase(selectedDb);
            }
        }

        tabPane.getTabs().add(queryTab);
        tabPane.getSelectionModel().select(queryTab);
    }

    private void openNewQueryTab() {
        openNewQueryTab("New Query", "");
    }

    private void openQueryBuilder() {
        // Create a new tab for the query builder
        Tab builderTab = new Tab("Query Builder");
        builderTab.setClosable(true);

        // Create the query builder component
        VisualQueryBuilder queryBuilder = new VisualQueryBuilder(connectionManager, sql -> {
            // This consumer gets called when the user clicks "Apply to Editor"
            // Create a new tab with the generated SQL
            openNewQueryTab("Generated Query", sql);
        });

        // Set the visual query builder as the tab content
        builderTab.setContent(queryBuilder);

        // Add the tab to the tab pane and select it
        tabPane.getTabs().add(builderTab);
        tabPane.getSelectionModel().select(builderTab);

        // Set database if one is selected
        if (dbExplorer != null) {
            String selectedDb = dbExplorer.getSelectedDatabase();
            if (selectedDb != null && !selectedDb.isEmpty()) {
                queryBuilder.setCurrentDatabase(selectedDb);
            }
        }
    }

    private void openIndexViewer() {
        Tab viewerTab = new Tab("Index Viewer");
        viewerTab.setClosable(true);

        IndexVisualizerView visualizer = new IndexVisualizerView(connectionManager);

        viewerTab.setContent(visualizer);
        tabPane.getTabs().add(viewerTab);
        tabPane.getSelectionModel().select(viewerTab);
    }

    private void openTransactionManager() {
        Tab transactionTab = new Tab("Transactions");
        transactionTab.setClosable(true);

        TransactionPanel transactionPanel = new TransactionPanel(connectionManager);

        transactionTab.setContent(transactionPanel);
        tabPane.getTabs().add(transactionTab);
        tabPane.getSelectionModel().select(transactionTab);
    }

    private void openPreferencesDialog() {
        UserPreferencesDialog dialog = new UserPreferencesDialog(connectionManager);
        dialog.initOwner(getScene().getWindow());
        dialog.showAndWait();
    }

    /**
     * Opens a table in the visual query builder
     * 
     * @param dbName    Database name
     * @param tableName Table name
     */
    private void openTableInQueryBuilder(String dbName, String tableName) {
        // Create a new tab for the query builder
        Tab builderTab = new Tab("Query Builder - " + tableName);
        builderTab.setClosable(true);

        // Create the query builder component
        VisualQueryBuilder queryBuilder = new VisualQueryBuilder(connectionManager, sql -> {
            // Create a new tab with the generated SQL when Apply is clicked
            openNewQueryTab("Generated Query", sql);
        });

        // Set the query builder as the tab content
        builderTab.setContent(queryBuilder);

        // Add the tab and select it
        tabPane.getTabs().add(builderTab);
        tabPane.getSelectionModel().select(builderTab);

        // Set database and add table using the public methods
        Platform.runLater(() -> {
            try {
                // Set the database first
                queryBuilder.setCurrentDatabase(dbName);

                // Wait a bit for the database to load, then add the table
                new java.util.Timer().schedule(
                        new java.util.TimerTask() {
                            @Override
                            public void run() {
                                Platform.runLater(() -> {
                                    try {
                                        // Use the public method to add table
                                        queryBuilder.addTable(tableName, 50, 50);
                                    } catch (Exception ex) {
                                        System.err.println("Error adding table to diagram: " + ex.getMessage());
                                    }
                                });
                            }
                        },
                        500 // Short delay to ensure database is fully loaded
                );
            } catch (Exception ex) {
                System.err.println("Error in query builder: " + ex.getMessage());
            }
        });
    }
}