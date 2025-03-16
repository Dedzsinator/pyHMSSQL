package com.pyhmssql.client.main;

import com.pyhmssql.client.views.DbExplorer;
import com.pyhmssql.client.views.LoginPanel;
import com.pyhmssql.client.views.VisualQueryBuilder;
import com.pyhmssql.client.views.QueryEditor;
import com.pyhmssql.client.views.ResultPane;

import javafx.scene.control.*;
import javafx.scene.layout.*;

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
        queryMenu.getItems().addAll(executeItem, newQueryItem);
        
        // Tools menu
        Menu toolsMenu = new Menu("Tools");
        MenuItem queryBuilderItem = new MenuItem("Query Builder");
        queryBuilderItem.setOnAction(e -> openQueryBuilder());
        toolsMenu.getItems().add(queryBuilderItem);
        
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
        dialog.showAndWait();
    }
    
    /**
     * Opens a new query tab with the given SQL
     * @param title Tab title
     * @param sql SQL query to display
     */
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
        Tab queryTab = new Tab("New Query");
        queryTab.setClosable(true);
        
        // Create query editor with results pane
        SplitPane splitPane = new SplitPane();
        splitPane.setOrientation(javafx.geometry.Orientation.VERTICAL);
        
        QueryEditor queryEditor = new QueryEditor(connectionManager);
        ResultPane resultPane = new ResultPane();
        
        // Connect editor to results pane
        queryEditor.setOnExecuteQuery(resultPane::displayResults);
        
        splitPane.getItems().addAll(queryEditor, resultPane);
        splitPane.setDividerPositions(0.5);
        
        queryTab.setContent(splitPane);
        tabPane.getTabs().add(queryTab);
        tabPane.getSelectionModel().select(queryTab);
    }
    
    private void openQueryBuilder() {
        Tab builderTab = new Tab("Query Builder");
        builderTab.setClosable(true);
        
        // Create a VisualQueryBuilder directly instead of QueryBuilder
        VisualQueryBuilder visualBuilder = new VisualQueryBuilder(connectionManager, 
            sql -> {
                // Open the generated SQL in a new query tab
                openNewQueryTab("Query", sql);
            });
        
        builderTab.setContent(visualBuilder);
        tabPane.getTabs().add(builderTab);
        tabPane.getSelectionModel().select(builderTab);
    }
}