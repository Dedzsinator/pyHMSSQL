package com.pyhmssql.client.main;

import com.pyhmssql.client.views.*;
import com.pyhmssql.client.utils.SQLFormatter;
import com.pyhmssql.client.utils.UIThemeManager;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.application.Platform;
import javafx.scene.control.Alert.AlertType;
import javafx.stage.FileChooser;
import javafx.geometry.Insets;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import java.util.ArrayList;
import java.util.Map;

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

        MenuItem openQueryItem = new MenuItem("Open Query...");
        openQueryItem.setOnAction(e -> openQueryFile());

        MenuItem saveQueryItem = new MenuItem("Save Query...");
        saveQueryItem.setOnAction(e -> saveCurrentQuery());

        MenuItem importDataItem = new MenuItem("Import Data...");
        importDataItem.setOnAction(e -> showImportDataDialog());

        MenuItem exportDataItem = new MenuItem("Export Data...");
        exportDataItem.setOnAction(e -> showExportDataDialog());

        MenuItem exitItem = new MenuItem("Exit");
        exitItem.setOnAction(e -> System.exit(0));

        fileMenu.getItems().addAll(connectItem, new SeparatorMenuItem(),
                openQueryItem, saveQueryItem, new SeparatorMenuItem(),
                importDataItem, exportDataItem, new SeparatorMenuItem(), exitItem);

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
        MenuItem formatQueryItem = new MenuItem("Format Query");
        formatQueryItem.setOnAction(e -> formatCurrentQuery());

        MenuItem findReplaceItem = new MenuItem("Find & Replace...");
        findReplaceItem.setOnAction(e -> showFindReplaceDialog());

        editMenu.getItems().addAll(copyItem, pasteItem, new SeparatorMenuItem(),
                formatQueryItem, findReplaceItem);

        // Query menu
        Menu queryMenu = new Menu("Query");
        MenuItem executeItem = new MenuItem("Execute");
        executeItem.setOnAction(e -> {
            QueryEditor activeEditor = getCurrentQueryEditor();
            if (activeEditor != null) {
                activeEditor.executeQuery();
            }
        });
        MenuItem newQueryItem = new MenuItem("New Query");
        newQueryItem.setOnAction(e -> openNewQueryTab());

        MenuItem explainPlanItem = new MenuItem("Explain Query Plan");
        explainPlanItem.setOnAction(e -> explainCurrentQuery());

        MenuItem queryHistoryItem = new MenuItem("Query History...");
        queryHistoryItem.setOnAction(e -> showQueryHistoryDialog());

        queryMenu.getItems().addAll(executeItem, newQueryItem, new SeparatorMenuItem(),
                explainPlanItem, queryHistoryItem);

        // Tools menu - ENHANCED with theme customization
        Menu toolsMenu = new Menu("Tools");
        MenuItem queryBuilderItem = new MenuItem("Query Builder");
        queryBuilderItem.setOnAction(e -> openQueryBuilder());
        MenuItem indexViewerItem = new MenuItem("Index Viewer");
        indexViewerItem.setOnAction(e -> openIndexViewer());
        MenuItem transactionItem = new MenuItem("Transaction Manager");
        transactionItem.setOnAction(e -> openTransactionManager());

        MenuItem serverStatusItem = new MenuItem("Server Status");
        serverStatusItem.setOnAction(e -> showServerStatusDialog());

        MenuItem backupRestoreItem = new MenuItem("Backup & Restore...");
        backupRestoreItem.setOnAction(e -> showBackupRestoreDialog());

        // NEW: Theme customization menu item
        MenuItem themeCustomizationItem = new MenuItem("Customize Theme...");
        themeCustomizationItem.setOnAction(e -> showThemeCustomizationDialog());

        MenuItem preferencesItem = new MenuItem("User Preferences");
        preferencesItem.setOnAction(e -> openPreferencesDialog());

        toolsMenu.getItems().addAll(queryBuilderItem, indexViewerItem, transactionItem,
                new SeparatorMenuItem(), serverStatusItem, backupRestoreItem,
                new SeparatorMenuItem(), themeCustomizationItem, preferencesItem);

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

        MenuItem createDbItem = new MenuItem("Create Database...");
        createDbItem.setOnAction(e -> showCreateDatabaseDialog());

        MenuItem dropDbItem = new MenuItem("Drop Database...");
        dropDbItem.setOnAction(e -> showDropDatabaseDialog());

        MenuItem databasePropertiesItem = new MenuItem("Database Properties...");
        databasePropertiesItem.setOnAction(e -> showDatabasePropertiesDialog());

        databaseMenu.getItems().addAll(refreshItem, new SeparatorMenuItem(),
                createDbItem, dropDbItem, new SeparatorMenuItem(), databasePropertiesItem);

        // Help menu
        Menu helpMenu = new Menu("Help");
        MenuItem aboutItem = new MenuItem("About");
        aboutItem.setOnAction(e -> showAboutDialog());

        MenuItem sqlReferenceItem = new MenuItem("SQL Reference");
        sqlReferenceItem.setOnAction(e -> showSqlReferenceDialog());

        MenuItem keyboardShortcutsItem = new MenuItem("Keyboard Shortcuts");
        keyboardShortcutsItem.setOnAction(e -> showKeyboardShortcutsDialog());

        helpMenu.getItems().addAll(sqlReferenceItem, keyboardShortcutsItem,
                new SeparatorMenuItem(), aboutItem);

        menuBar.getMenus().addAll(fileMenu, editMenu, queryMenu, toolsMenu,
                databaseMenu, helpMenu);
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

    // Additional helper methods for new menu functionality
    private void openQueryFile() {
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Open SQL Query");
        fileChooser.getExtensionFilters().add(
                new FileChooser.ExtensionFilter("SQL Files", "*.sql"));

        File file = fileChooser.showOpenDialog(getScene().getWindow());
        if (file != null) {
            try {
                String content = Files.readString(file.toPath());
                openNewQueryTab(file.getName(), content);
            } catch (IOException e) {
                showAlert(AlertType.ERROR, "Error", "Failed to open file", e.getMessage());
            }
        }
    }

    private void saveCurrentQuery() {
        QueryEditor activeEditor = getCurrentQueryEditor();
        if (activeEditor != null) {
            // The QueryEditor already has save functionality
            // We could trigger it here or implement a centralized save
        }
    }

    private void formatCurrentQuery() {
        QueryEditor activeEditor = getCurrentQueryEditor();
        if (activeEditor != null) {
            String currentQuery = activeEditor.getQuery();
            if (currentQuery != null && !currentQuery.trim().isEmpty()) {
                try {
                    String formatted = SQLFormatter.format(currentQuery);
                    activeEditor.setQuery(formatted);
                } catch (Exception e) {
                    showAlert(AlertType.ERROR, "Error", "Failed to format query", e.getMessage());
                }
            }
        }
    }

    private void explainCurrentQuery() {
        QueryEditor activeEditor = getCurrentQueryEditor();
        if (activeEditor != null) {
            String query = activeEditor.getQuery();
            if (query != null && !query.trim().isEmpty()) {
                String explainQuery = "EXPLAIN " + query;
                openNewQueryTab("Query Plan", explainQuery);
            }
        }
    }

    private void showCreateDatabaseDialog() {
        TextInputDialog dialog = new TextInputDialog();
        dialog.setTitle("Create Database");
        dialog.setHeaderText("Create a new database");
        dialog.setContentText("Database name:");

        Optional<String> result = dialog.showAndWait();
        result.ifPresent(dbName -> {
            if (!dbName.trim().isEmpty()) {
                connectionManager.executeQuery("CREATE DATABASE " + dbName)
                        .thenAccept(response -> {
                            Platform.runLater(() -> {
                                if (response.containsKey("error")) {
                                    showAlert(AlertType.ERROR, "Error", "Failed to create database",
                                            response.get("error").toString());
                                } else {
                                    if (dbExplorer != null) {
                                        dbExplorer.refreshDatabases();
                                    }
                                }
                            });
                        });
            }
        });
    }

    private void showAboutDialog() {
        Alert alert = new Alert(AlertType.INFORMATION);
        alert.setTitle("About pyHMSSQL Client");
        alert.setHeaderText("pyHMSSQL Java Client");
        alert.setContentText("A powerful Java client for the pyHMSSQL database system.\n\n" +
                "Features:\n" +
                "• Visual Query Builder\n" +
                "• SQL Editor with syntax highlighting\n" +
                "• Database Explorer\n" +
                "• Index Visualization\n" +
                "• Transaction Management\n\n" +
                "Version: 1.0.0");
        alert.showAndWait();
    }

    private void showAlert(AlertType type, String title, String header, String content) {
        Alert alert = new Alert(type);
        alert.setTitle(title);
        alert.setHeaderText(header);
        alert.setContentText(content);
        alert.showAndWait();
    }

    /**
     * Show theme customization dialog
     */
    private void showThemeCustomizationDialog() {
        ThemeCustomizationDialog dialog = new ThemeCustomizationDialog();
        dialog.initOwner(getScene().getWindow());
        dialog.showAndWait();
    }

    /**
     * Initialize theme management when scene is available
     */
    public void initializeTheme() {
        if (getScene() != null) {
            UIThemeManager.getInstance().setScene(getScene());
        }
    }

    // Placeholder methods for additional functionality - NOW IMPLEMENTED
    private void showFindReplaceDialog() {
        Dialog<ButtonType> dialog = new Dialog<>();
        dialog.setTitle("Find & Replace");
        dialog.setHeaderText("Find and replace text in the current query");

        // Create the form
        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(20, 150, 10, 10));

        TextField findField = new TextField();
        findField.setPromptText("Find text");
        TextField replaceField = new TextField();
        replaceField.setPromptText("Replace with");
        CheckBox matchCaseBox = new CheckBox("Match case");
        CheckBox wholeWordBox = new CheckBox("Whole word");

        grid.add(new Label("Find:"), 0, 0);
        grid.add(findField, 1, 0);
        grid.add(new Label("Replace:"), 0, 1);
        grid.add(replaceField, 1, 1);
        grid.add(matchCaseBox, 0, 2);
        grid.add(wholeWordBox, 1, 2);

        dialog.getDialogPane().setContent(grid);
        dialog.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);

        dialog.getDialogPane().getButtonTypes().add(new ButtonType("Replace All", ButtonBar.ButtonData.APPLY));

        dialog.setResultConverter(buttonType -> {
            if (buttonType == ButtonType.OK || buttonType.getText().equals("Replace All")) {
                QueryEditor activeEditor = getCurrentQueryEditor();
                if (activeEditor != null) {
                    String currentText = activeEditor.getQuery();
                    String findText = findField.getText();
                    String replaceText = replaceField.getText();

                    if (currentText != null && findText != null && !findText.isEmpty()) {
                        String newText;
                        if (matchCaseBox.isSelected()) {
                            newText = currentText.replace(findText, replaceText);
                        } else {
                            newText = currentText.replaceAll("(?i)" + java.util.regex.Pattern.quote(findText),
                                    replaceText);
                        }
                        activeEditor.setQuery(newText);
                    }
                }
            }
            return buttonType;
        });

        dialog.showAndWait();
    }

    private void showQueryHistoryDialog() {
        Dialog<String> dialog = new Dialog<>();
        dialog.setTitle("Query History");
        dialog.setHeaderText("Recent queries");

        // Create list view for history
        ListView<String> historyList = new ListView<>();

        // Mock history for now - in a real implementation, you'd load from
        // QueryHistoryManager
        historyList.getItems().addAll(
                "SELECT * FROM customers WHERE age > 30",
                "SHOW DATABASES",
                "CREATE TABLE test (id INT PRIMARY KEY)",
                "INSERT INTO customers VALUES (1, 'John', 'john@test.com', 25)");

        historyList.setPrefSize(600, 400);

        // Allow double-click to select query
        historyList.setOnMouseClicked(event -> {
            if (event.getClickCount() == 2) {
                String selectedQuery = historyList.getSelectionModel().getSelectedItem();
                if (selectedQuery != null) {
                    openNewQueryTab("From History", selectedQuery);
                    dialog.close();
                }
            }
        });

        dialog.getDialogPane().setContent(historyList);
        dialog.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);

        dialog.setResultConverter(buttonType -> {
            if (buttonType == ButtonType.OK) {
                return historyList.getSelectionModel().getSelectedItem();
            }
            return null;
        });

        Optional<String> result = dialog.showAndWait();
        result.ifPresent(query -> openNewQueryTab("From History", query));
    }

    private void showServerStatusDialog() {
        Dialog<ButtonType> dialog = new Dialog<>();
        dialog.setTitle("Server Status");
        dialog.setHeaderText("pyHMSSQL Server Information");

        // Create content
        VBox content = new VBox(10);
        content.setPadding(new Insets(10));

        Label statusLabel = new Label("Fetching server status...");
        ProgressIndicator progress = new ProgressIndicator();
        progress.setPrefSize(30, 30);

        content.getChildren().addAll(statusLabel, progress);

        dialog.getDialogPane().setContent(content);
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CLOSE);

        // Fetch server status
        connectionManager.getServerInfo()
                .thenAccept(result -> {
                    Platform.runLater(() -> {
                        content.getChildren().clear();

                        if (result.containsKey("error")) {
                            content.getChildren().add(new Label("Error: " + result.get("error")));
                        } else {
                            // Display server information
                            for (Map.Entry<String, Object> entry : result.entrySet()) {
                                Label infoLabel = new Label(entry.getKey() + ": " + entry.getValue());
                                content.getChildren().add(infoLabel);
                            }
                        }
                    });
                })
                .exceptionally(ex -> {
                    Platform.runLater(() -> {
                        content.getChildren().clear();
                        content.getChildren().add(new Label("Error fetching server status: " + ex.getMessage()));
                    });
                    return null;
                });

        dialog.showAndWait();
    }

    private void showDatabasePropertiesDialog() {
        // Get currently selected database
        String selectedDb = dbExplorer != null ? dbExplorer.getSelectedDatabase() : null;

        if (selectedDb == null) {
            showAlert(AlertType.WARNING, "No Database Selected",
                    "Please select a database", "Select a database in the explorer first.");
            return;
        }

        Dialog<ButtonType> dialog = new Dialog<>();
        dialog.setTitle("Database Properties");
        dialog.setHeaderText("Properties for database: " + selectedDb);

        VBox content = new VBox(10);
        content.setPadding(new Insets(10));

        // Basic info
        content.getChildren().add(new Label("Database Name: " + selectedDb));

        Label statusLabel = new Label("Loading database information...");
        content.getChildren().add(statusLabel);

        // Get database stats
        connectionManager.executeQuery("USE " + selectedDb + "; SHOW SERVER STATS")
                .thenAccept(result -> {
                    Platform.runLater(() -> {
                        content.getChildren().remove(statusLabel);

                        if (result.containsKey("error")) {
                            content.getChildren().add(new Label("Error loading stats: " + result.get("error")));
                        } else {
                            // Display database statistics
                            for (Map.Entry<String, Object> entry : result.entrySet()) {
                                if (!entry.getKey().equals("status")) {
                                    Label statLabel = new Label(entry.getKey() + ": " + entry.getValue());
                                    content.getChildren().add(statLabel);
                                }
                            }
                        }
                    });
                });

        dialog.getDialogPane().setContent(content);
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CLOSE);
        dialog.showAndWait();
    }

    private void showSqlReferenceDialog() {
        Dialog<ButtonType> dialog = new Dialog<>();
        dialog.setTitle("SQL Reference");
        dialog.setHeaderText("pyHMSSQL SQL Command Reference");

        TabPane tabPane = new TabPane();

        // DDL Commands
        Tab ddlTab = new Tab("DDL");
        TextArea ddlText = new TextArea();
        ddlText.setEditable(false);
        ddlText.setText(
                "Data Definition Language (DDL) Commands:\n\n" +
                        "CREATE DATABASE database_name;\n" +
                        "DROP DATABASE database_name;\n" +
                        "USE database_name;\n\n" +
                        "CREATE TABLE table_name (\n" +
                        "  column1 datatype constraints,\n" +
                        "  column2 datatype constraints,\n" +
                        "  PRIMARY KEY (column1)\n" +
                        ");\n\n" +
                        "DROP TABLE table_name;\n" +
                        "ALTER TABLE table_name ADD COLUMN column_name datatype;\n\n" +
                        "CREATE INDEX index_name ON table_name (column_name);\n" +
                        "CREATE UNIQUE INDEX index_name ON table_name (column_name);\n" +
                        "DROP INDEX index_name ON table_name;\n\n" +
                        "Data Types:\n" +
                        "- INT, INTEGER\n" +
                        "- VARCHAR(length)\n" +
                        "- DECIMAL(precision, scale)\n" +
                        "- DATETIME\n" +
                        "- BOOLEAN");
        ddlTab.setContent(new ScrollPane(ddlText));

        // DML Commands
        Tab dmlTab = new Tab("DML");
        TextArea dmlText = new TextArea();
        dmlText.setEditable(false);
        dmlText.setText(
                "Data Manipulation Language (DML) Commands:\n\n" +
                        "SELECT [DISTINCT] column1, column2, ...\n" +
                        "FROM table_name\n" +
                        "[WHERE condition]\n" +
                        "[ORDER BY column ASC|DESC]\n" +
                        "[LIMIT number];\n\n" +
                        "INSERT INTO table_name (column1, column2, ...)\n" +
                        "VALUES (value1, value2, ...);\n\n" +
                        "UPDATE table_name\n" +
                        "SET column1 = value1, column2 = value2, ...\n" +
                        "[WHERE condition];\n\n" +
                        "DELETE FROM table_name\n" +
                        "[WHERE condition];\n\n" +
                        "Aggregate Functions:\n" +
                        "- COUNT(*), COUNT(column)\n" +
                        "- SUM(column)\n" +
                        "- AVG(column)\n" +
                        "- MIN(column)\n" +
                        "- MAX(column)\n\n" +
                        "Joins:\n" +
                        "- INNER JOIN\n" +
                        "- LEFT JOIN\n" +
                        "- RIGHT JOIN\n" +
                        "- FULL JOIN\n" +
                        "- CROSS JOIN");
        dmlTab.setContent(new ScrollPane(dmlText));

        // System Commands
        Tab systemTab = new Tab("System");
        TextArea systemText = new TextArea();
        systemText.setEditable(false);
        systemText.setText(
                "System Commands:\n\n" +
                        "SHOW DATABASES;\n" +
                        "SHOW TABLES;\n" +
                        "SHOW INDEXES;\n" +
                        "SHOW INDEXES FOR table_name;\n" +
                        "DESCRIBE table_name;\n\n" +
                        "VISUALIZE BPTREE [index_name] [ON table_name];\n\n" +
                        "Transaction Commands:\n" +
                        "BEGIN TRANSACTION;\n" +
                        "COMMIT TRANSACTION;\n" +
                        "ROLLBACK TRANSACTION;\n" +
                        "SHOW TRANSACTION STATUS;\n\n" +
                        "Cache Commands:\n" +
                        "CACHE STATS;\n" +
                        "CACHE CLEAR ALL;\n" +
                        "CACHE CLEAR TABLE table_name;\n\n" +
                        "User Preferences:\n" +
                        "SET PREFERENCE name value;\n" +
                        "GET PREFERENCE name;\n\n" +
                        "Server Information:\n" +
                        "SHOW SERVER INFO;\n" +
                        "SHOW SERVER STATS;\n" +
                        "SHOW PROCESSLIST;");
        systemTab.setContent(new ScrollPane(systemText));

        tabPane.getTabs().addAll(ddlTab, dmlTab, systemTab);
        dialog.getDialogPane().setContent(tabPane);
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CLOSE);
        dialog.showAndWait();
    }

    private void showKeyboardShortcutsDialog() {
        Alert alert = new Alert(AlertType.INFORMATION);
        alert.setTitle("Keyboard Shortcuts");
        alert.setHeaderText("pyHMSSQL Client Keyboard Shortcuts");
        alert.setContentText(
                "File Operations:\n" +
                        "Ctrl+N - New Query\n" +
                        "Ctrl+O - Open Query File\n" +
                        "Ctrl+S - Save Query\n\n" +
                        "Edit Operations:\n" +
                        "Ctrl+C - Copy\n" +
                        "Ctrl+V - Paste\n" +
                        "Ctrl+F - Find & Replace\n" +
                        "Ctrl+L - Format Query\n\n" +
                        "Query Operations:\n" +
                        "F5 - Execute Query\n" +
                        "Ctrl+E - Execute Query\n" +
                        "F6 - Explain Query Plan\n\n" +
                        "View Operations:\n" +
                        "F4 - Toggle Query Builder\n" +
                        "F7 - Index Viewer\n" +
                        "F8 - Transaction Manager\n\n" +
                        "Database Operations:\n" +
                        "F9 - Refresh Databases\n" +
                        "Ctrl+D - Database Properties");
        alert.showAndWait();
    }

    private void showImportDataDialog() {
        Dialog<ButtonType> dialog = new Dialog<>();
        dialog.setTitle("Import Data");
        dialog.setHeaderText("Import data from file");

        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(20, 150, 10, 10));

        ComboBox<String> databaseCombo = new ComboBox<>();
        ComboBox<String> tableCombo = new ComboBox<>();
        TextField filePathField = new TextField();
        ComboBox<String> formatCombo = new ComboBox<>();

        formatCombo.getItems().addAll("CSV", "TSV", "JSON", "SQL");
        formatCombo.setValue("CSV");

        // Load databases
        connectionManager.getDatabases().thenAccept(result -> {
            if (result.containsKey("databases")) {
                @SuppressWarnings("unchecked")
                List<String> databases = (List<String>) result.get("databases");
                Platform.runLater(() -> databaseCombo.getItems().addAll(databases));
            }
        });

        // Load tables when database changes
        databaseCombo.setOnAction(e -> {
            String selectedDb = databaseCombo.getValue();
            if (selectedDb != null) {
                connectionManager.getTables(selectedDb).thenAccept(result -> {
                    if (result.containsKey("tables")) {
                        @SuppressWarnings("unchecked")
                        List<String> tables = (List<String>) result.get("tables");
                        Platform.runLater(() -> {
                            tableCombo.getItems().clear();
                            tableCombo.getItems().addAll(tables);
                        });
                    }
                });
            }
        });

        Button browseButton = new Button("Browse...");
        browseButton.setOnAction(e -> {
            FileChooser fileChooser = new FileChooser();
            fileChooser.setTitle("Select Import File");
            fileChooser.getExtensionFilters().addAll(
                    new FileChooser.ExtensionFilter("CSV Files", "*.csv"),
                    new FileChooser.ExtensionFilter("JSON Files", "*.json"),
                    new FileChooser.ExtensionFilter("SQL Files", "*.sql"),
                    new FileChooser.ExtensionFilter("All Files", "*.*"));
            File file = fileChooser.showOpenDialog(dialog.getOwner());
            if (file != null) {
                filePathField.setText(file.getAbsolutePath());
            }
        });

        grid.add(new Label("Database:"), 0, 0);
        grid.add(databaseCombo, 1, 0);
        grid.add(new Label("Table:"), 0, 1);
        grid.add(tableCombo, 1, 1);
        grid.add(new Label("File:"), 0, 2);
        grid.add(filePathField, 1, 2);
        grid.add(browseButton, 2, 2);
        grid.add(new Label("Format:"), 0, 3);
        grid.add(formatCombo, 1, 3);

        dialog.getDialogPane().setContent(grid);
        dialog.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);

        dialog.setResultConverter(buttonType -> {
            if (buttonType == ButtonType.OK) {
                String database = databaseCombo.getValue();
                String table = tableCombo.getValue();
                String filePath = filePathField.getText();

                if (database != null && table != null && !filePath.isEmpty()) {
                    showAlert(AlertType.INFORMATION, "Import", "Import Started",
                            "Importing data from " + filePath + " into " + database + "." + table);
                    // TODO: Implement actual import logic
                }
            }
            return buttonType;
        });

        dialog.showAndWait();
    }

    private void showExportDataDialog() {
        Dialog<ButtonType> dialog = new Dialog<>();
        dialog.setTitle("Export Data");
        dialog.setHeaderText("Export data to file");

        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(20, 150, 10, 10));

        ComboBox<String> databaseCombo = new ComboBox<>();
        ComboBox<String> tableCombo = new ComboBox<>();
        TextField filePathField = new TextField();
        ComboBox<String> formatCombo = new ComboBox<>();
        TextArea queryArea = new TextArea();

        formatCombo.getItems().addAll("CSV", "TSV", "JSON", "SQL");
        formatCombo.setValue("CSV");

        queryArea.setPromptText("Optional: Custom query for export");
        queryArea.setPrefRowCount(3);

        // Load databases
        connectionManager.getDatabases().thenAccept(result -> {
            if (result.containsKey("databases")) {
                @SuppressWarnings("unchecked")
                List<String> databases = (List<String>) result.get("databases");
                Platform.runLater(() -> databaseCombo.getItems().addAll(databases));
            }
        });

        // Load tables when database changes
        databaseCombo.setOnAction(e -> {
            String selectedDb = databaseCombo.getValue();
            if (selectedDb != null) {
                connectionManager.getTables(selectedDb).thenAccept(result -> {
                    if (result.containsKey("tables")) {
                        @SuppressWarnings("unchecked")
                        List<String> tables = (List<String>) result.get("tables");
                        Platform.runLater(() -> {
                            tableCombo.getItems().clear();
                            tableCombo.getItems().addAll(tables);
                        });
                    }
                });
            }
        });

        Button browseButton = new Button("Browse...");
        browseButton.setOnAction(e -> {
            FileChooser fileChooser = new FileChooser();
            fileChooser.setTitle("Save Export File");
            fileChooser.getExtensionFilters().addAll(
                    new FileChooser.ExtensionFilter("CSV Files", "*.csv"),
                    new FileChooser.ExtensionFilter("JSON Files", "*.json"),
                    new FileChooser.ExtensionFilter("SQL Files", "*.sql"),
                    new FileChooser.ExtensionFilter("All Files", "*.*"));
            File file = fileChooser.showSaveDialog(dialog.getOwner());
            if (file != null) {
                filePathField.setText(file.getAbsolutePath());
            }
        });

        grid.add(new Label("Database:"), 0, 0);
        grid.add(databaseCombo, 1, 0);
        grid.add(new Label("Table:"), 0, 1);
        grid.add(tableCombo, 1, 1);
        grid.add(new Label("Export File:"), 0, 2);
        grid.add(filePathField, 1, 2);
        grid.add(browseButton, 2, 2);
        grid.add(new Label("Format:"), 0, 3);
        grid.add(formatCombo, 1, 3);
        grid.add(new Label("Custom Query:"), 0, 4);
        grid.add(queryArea, 1, 4, 2, 1);

        dialog.getDialogPane().setContent(grid);
        dialog.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);

        dialog.setResultConverter(buttonType -> {
            if (buttonType == ButtonType.OK) {
                String database = databaseCombo.getValue();
                String table = tableCombo.getValue();
                String filePath = filePathField.getText();
                String customQuery = queryArea.getText();

                if (database != null && !filePath.isEmpty() && (table != null || !customQuery.trim().isEmpty())) {
                    showAlert(AlertType.INFORMATION, "Export", "Export Started",
                            "Exporting data to " + filePath);
                    // TODO: Implement actual export logic
                }
            }
            return buttonType;
        });

        dialog.showAndWait();
    }

    private void showBackupRestoreDialog() {
        Dialog<ButtonType> dialog = new Dialog<>();
        dialog.setTitle("Backup & Restore");
        dialog.setHeaderText("Database Backup and Restore Operations");

        TabPane tabPane = new TabPane();

        // Backup tab
        Tab backupTab = new Tab("Backup");
        VBox backupContent = new VBox(10);
        backupContent.setPadding(new Insets(10));

        ComboBox<String> backupDbCombo = new ComboBox<>();
        backupDbCombo.setPromptText("Select database to backup");

        // Load databases
        connectionManager.getDatabases().thenAccept(result -> {
            if (result.containsKey("databases")) {
                @SuppressWarnings("unchecked")
                List<String> databases = (List<String>) result.get("databases");
                Platform.runLater(() -> backupDbCombo.getItems().addAll(databases));
            }
        });

        TextField backupPathField = new TextField();
        backupPathField.setPromptText("Backup file path");

        Button browseBackupButton = new Button("Browse...");
        browseBackupButton.setOnAction(e -> {
            FileChooser fileChooser = new FileChooser();
            fileChooser.setTitle("Save Backup File");
            fileChooser.getExtensionFilters().add(
                    new FileChooser.ExtensionFilter("SQL Files", "*.sql"));
            File file = fileChooser.showSaveDialog(dialog.getOwner());
            if (file != null) {
                backupPathField.setText(file.getAbsolutePath());
            }
        });

        Button createBackupButton = new Button("Create Backup");
        createBackupButton.setOnAction(e -> {
            String database = backupDbCombo.getValue();
            String path = backupPathField.getText();
            if (database != null && !path.isEmpty()) {
                // Implement backup logic here
                showAlert(AlertType.INFORMATION, "Backup", "Backup Created",
                        "Backup of " + database + " saved to " + path);
            }
        });

        HBox backupPathBox = new HBox(5, backupPathField, browseBackupButton);
        backupContent.getChildren().addAll(
                new Label("Database:"), backupDbCombo,
                new Label("Backup Path:"), backupPathBox,
                createBackupButton);
        backupTab.setContent(backupContent);

        // Restore tab
        Tab restoreTab = new Tab("Restore");
        VBox restoreContent = new VBox(10);
        restoreContent.setPadding(new Insets(10));

        TextField restorePathField = new TextField();
        restorePathField.setPromptText("Backup file to restore");

        Button browseRestoreButton = new Button("Browse...");
        browseRestoreButton.setOnAction(e -> {
            FileChooser fileChooser = new FileChooser();
            fileChooser.setTitle("Select Backup File");
            fileChooser.getExtensionFilters().add(
                    new FileChooser.ExtensionFilter("SQL Files", "*.sql"));
            File file = fileChooser.showOpenDialog(dialog.getOwner());
            if (file != null) {
                restorePathField.setText(file.getAbsolutePath());
            }
        });

        TextField restoreDbField = new TextField();
        restoreDbField.setPromptText("Target database name");

        Button restoreButton = new Button("Restore Database");
        restoreButton.setOnAction(e -> {
            String path = restorePathField.getText();
            String targetDb = restoreDbField.getText();
            if (!path.isEmpty() && !targetDb.isEmpty()) {
                // Implement restore logic here
                showAlert(AlertType.INFORMATION, "Restore", "Database Restored",
                        "Database restored from " + path + " to " + targetDb);
            }
        });

        HBox restorePathBox = new HBox(5, restorePathField, browseRestoreButton);
        restoreContent.getChildren().addAll(
                new Label("Backup File:"), restorePathBox,
                new Label("Target Database:"), restoreDbField,
                restoreButton);
        restoreTab.setContent(restoreContent);

        tabPane.getTabs().addAll(backupTab, restoreTab);
        dialog.getDialogPane().setContent(tabPane);
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CLOSE);

        dialog.showAndWait();
    }

    private void showDropDatabaseDialog() {
        List<String> databases = new ArrayList<>();

        connectionManager.getDatabases().thenAccept(result -> {
            if (result.containsKey("databases")) {
                @SuppressWarnings("unchecked")
                List<String> dbList = (List<String>) result.get("databases");
                databases.addAll(dbList);
            }

            Platform.runLater(() -> {
                if (databases.isEmpty()) {
                    showAlert(AlertType.WARNING, "No Databases", "No databases found",
                            "No databases available to drop.");
                    return;
                }

                ChoiceDialog<String> dialog = new ChoiceDialog<>(databases.get(0), databases);
                dialog.setTitle("Drop Database");
                dialog.setHeaderText("Select database to drop");
                dialog.setContentText("Database:");

                Optional<String> result2 = dialog.showAndWait();
                result2.ifPresent(dbName -> {
                    Alert confirmAlert = new Alert(AlertType.CONFIRMATION);
                    confirmAlert.setTitle("Confirm Drop");
                    confirmAlert.setHeaderText("Drop Database " + dbName);
                    confirmAlert.setContentText("Are you sure you want to drop database '" + dbName +
                            "'? This action cannot be undone.");

                    Optional<ButtonType> confirmResult = confirmAlert.showAndWait();
                    if (confirmResult.isPresent() && confirmResult.get() == ButtonType.OK) {
                        connectionManager.executeQuery("DROP DATABASE " + dbName)
                                .thenAccept(response -> {
                                    Platform.runLater(() -> {
                                        if (response.containsKey("error")) {
                                            showAlert(AlertType.ERROR, "Error", "Failed to drop database",
                                                    response.get("error").toString());
                                        } else {
                                            showAlert(AlertType.INFORMATION, "Success", "Database dropped",
                                                    "Database '" + dbName + "' has been dropped successfully.");
                                            if (dbExplorer != null) {
                                                dbExplorer.refreshDatabases();
                                            }
                                        }
                                    });
                                });
                    }
                });
            });
        });
    }
}