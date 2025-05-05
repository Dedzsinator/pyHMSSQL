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
        MenuItem pasteItem = new MenuItem("Paste");
        editMenu.getItems().addAll(copyItem, pasteItem);

        // Query menu
        Menu queryMenu = new Menu("Query");
        MenuItem executeItem = new MenuItem("Execute");
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

        menuBar.getMenus().addAll(fileMenu, editMenu, queryMenu, toolsMenu);
        setTop(menuBar);
    }

    private void initTabPane() {
        tabPane = new TabPane();
        setCenter(tabPane);
    }

    private void initDbExplorer() {
        dbExplorer = new DbExplorer(connectionManager);

        // Add to left side with splitter
        SplitPane splitPane = new SplitPane();
        splitPane.getItems().add(dbExplorer);
        splitPane.getItems().add(tabPane);
        splitPane.setDividerPositions(0.25);

        setCenter(splitPane);
    }

    private void showLoginPanel() {
        LoginPanel loginPanel = new LoginPanel(connectionManager);
        loginPanel.setOnLoginSuccess(() -> {
            dbExplorer.refreshDatabases();
            openNewQueryTab();
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
                dbExplorer.refreshDatabases();
                openNewQueryTab();
            });
            loginPanel.setServerInfo(server.getHost(), server.getPort());

            loginDialog.getDialogPane().setContent(loginPanel);
            loginDialog.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);
            loginDialog.showAndWait();
        });
    }

    private void openNewQueryTab(String title, String sql) {
        Tab queryTab = new Tab(title);
        queryTab.setClosable(true);

        // Create query editor with results pane
        SplitPane splitPane = new SplitPane();
        splitPane.setOrientation(javafx.geometry.Orientation.VERTICAL);

        QueryEditor queryEditor = new QueryEditor(connectionManager);
        ResultPane resultPane = new ResultPane();

        // Set the query text
        queryEditor.setQuery(sql);

        // Set the database if available
        String selectedDb = dbExplorer.getSelectedDatabase();
        if (selectedDb != null && !selectedDb.isEmpty()) {
            queryEditor.setDatabase(selectedDb);
        }

        // Connect editor to results pane
        queryEditor.setOnExecuteQuery(resultPane::displayResults);

        splitPane.getItems().addAll(queryEditor, resultPane);
        splitPane.setDividerPositions(0.5);

        queryTab.setContent(splitPane);
        tabPane.getTabs().add(queryTab);
        tabPane.getSelectionModel().select(queryTab);
    }

    private void openNewQueryTab() {
        openNewQueryTab("New Query", "");
    }

    private void openQueryBuilder() {
        Tab builderTab = new Tab("Query Builder");
        builderTab.setClosable(true);

        VisualQueryBuilder visualBuilder = new VisualQueryBuilder(connectionManager,
                sql -> {
                    // Open the generated SQL in a new query tab
                    openNewQueryTab("Query", sql);
                });

        builderTab.setContent(visualBuilder);
        tabPane.getTabs().add(builderTab);
        tabPane.getSelectionModel().select(builderTab);
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
        dialog.showAndWait();
    }
}