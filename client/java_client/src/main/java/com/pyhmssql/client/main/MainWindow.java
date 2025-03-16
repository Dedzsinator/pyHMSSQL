package main;

import views.DbExplorer;
import views.LoginPanel;
import views.VisualQueryBuilder;
import views.QueryEditor;
import views.ResultPane;

import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.geometry.Insets;

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
        
        QueryBuilder queryBuilder = new QueryBuilder(connectionManager, dbExplorer.getSelectedDatabase());
        builderTab.setContent(queryBuilder);
        
        tabPane.getTabs().add(builderTab);
        tabPane.getSelectionModel().select(builderTab);
    }
}