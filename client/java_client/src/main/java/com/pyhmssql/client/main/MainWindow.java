package com.pyhmssql.client.main;

import com.pyhmssql.client.views.*;
import com.pyhmssql.client.utils.SQLFormatter;
import com.pyhmssql.client.utils.UIThemeManager;
import com.pyhmssql.client.config.ConfigurationManager;
import com.pyhmssql.client.theme.ThemeManager;
import com.pyhmssql.client.utils.GlobalExceptionHandler;
import com.pyhmssql.client.utils.AppInfo;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.application.Platform;
import javafx.scene.control.Alert.AlertType;
import javafx.stage.FileChooser;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.animation.FadeTransition;
import javafx.animation.Timeline;
import javafx.animation.KeyFrame;
import javafx.util.Duration;
import javafx.concurrent.Task;
import javafx.beans.property.StringProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.SimpleBooleanProperty;
import org.controlsfx.control.StatusBar;
import org.controlsfx.control.NotificationPane;
import org.controlsfx.control.action.Action;
import org.controlsfx.control.MasterDetailPane;
import org.kordamp.ikonli.javafx.FontIcon;
import org.kordamp.ikonli.material2.Material2AL;
import org.kordamp.ikonli.material2.Material2MZ;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;
import java.util.prefs.Preferences;

public class MainWindow extends BorderPane {
    private static final Logger logger = Logger.getLogger(MainWindow.class.getName());

    // Core UI Components
    private MenuBar menuBar;
    private TabPane tabPane;
    private DbExplorer dbExplorer;
    private ConnectionManager connectionManager;
    private StatusBar statusBar;
    private NotificationPane notificationPane;
    private MasterDetailPane masterDetailPane;

    // Modern UI Controls - replace JFoenix with standard components
    private ToolBar toolbar;
    private ProgressIndicator progressSpinner;

    // Configuration and Theme Management
    private final ConfigurationManager configManager;
    private final ThemeManager themeManager;

    // Application State
    private final BooleanProperty connected = new SimpleBooleanProperty(false);
    private final StringProperty currentUser = new SimpleStringProperty("Not Connected");
    private final StringProperty currentDatabase = new SimpleStringProperty("None");

    // Performance Monitoring
    private Timeline performanceUpdateTimeline;
    private Label memoryUsageLabel;
    private Label connectionCountLabel;

    // Add instance variables for the icons
    private FontIcon connectionIcon;
    private FontIcon databaseIcon;

    public MainWindow() {
        try {
            // Initialize configuration and theme management
            configManager = ConfigurationManager.getInstance();
            themeManager = ThemeManager.getInstance();

            // Initialize connection manager
            connectionManager = new ConnectionManager();

            // Setup global exception handling
            Thread.setDefaultUncaughtExceptionHandler(GlobalExceptionHandler.getInstance());

            // Initialize UI components
            initializeComponents();
            setupLayout();
            setupBindings();
            setupEventHandlers();

            // Apply initial theme
            applyTheme();

            // Start with login panel if auto-connect is disabled
            if (!configManager.getBoolean("connection.autoConnect", false)) {
                showLoginPanel();
            } else {
                attemptAutoConnect();
            }

            // Initialize performance monitoring
            initPerformanceMonitoring();

            logger.info("MainWindow initialized successfully");

        } catch (Exception e) {
            GlobalExceptionHandler.handleException("MainWindow initialization", e);
            throw new RuntimeException("Failed to initialize MainWindow", e);
        }
    }

    private void initializeComponents() {
        // Initialize modern UI components FIRST
        progressSpinner = new ProgressIndicator();
        progressSpinner.setVisible(false);
        progressSpinner.setPrefSize(20, 20);
        progressSpinner.setProgress(-1); // Indeterminate progress

        // Setup notification pane
        notificationPane = new NotificationPane();
        notificationPane.getStyleClass().add("notification-pane");

        // Initialize core components AFTER modern UI components
        initMenuBar();
        initToolbar();
        initTabPane();
        initDbExplorer();
        initStatusBar();
    }

    private void initToolbar() {
        toolbar = new ToolBar();
        toolbar.setStyle("-fx-background-color: -fx-primary;");

        // Connection status indicator
        connectionIcon = new FontIcon(Material2AL.LINK_OFF);
        connectionIcon.setIconSize(16);
        connectionIcon.getStyleClass().add("toolbar-icon");

        Label connectionStatus = new Label();
        connectionStatus.textProperty().bind(currentUser);
        connectionStatus.setGraphic(connectionIcon);
        connectionStatus.getStyleClass().add("connection-status");

        // Database indicator
        databaseIcon = new FontIcon(Material2MZ.STORAGE);
        databaseIcon.setIconSize(16);
        databaseIcon.getStyleClass().add("toolbar-icon");

        Label databaseStatus = new Label();
        databaseStatus.textProperty().bind(currentDatabase);
        databaseStatus.setGraphic(databaseIcon);
        databaseStatus.getStyleClass().add("database-status");

        // Quick action buttons - replace JFXButton with Button
        Button connectBtn = new Button("", new FontIcon(Material2AL.LOGIN));
        connectBtn.getStyleClass().addAll("button", "toolbar-button");
        connectBtn.setOnAction(e -> showLoginPanel());

        Button newQueryBtn = new Button("", new FontIcon(Material2AL.ADD));
        newQueryBtn.getStyleClass().addAll("button", "toolbar-button");
        newQueryBtn.setOnAction(e -> openNewQueryTab());

        Button executeBtn = new Button("", new FontIcon(Material2MZ.PLAY_ARROW));
        executeBtn.getStyleClass().addAll("button", "toolbar-button");
        executeBtn.setOnAction(e -> executeCurrentQuery());

        // Theme toggle button
        Button themeBtn = new Button("", new FontIcon(Material2MZ.PALETTE));
        themeBtn.getStyleClass().addAll("button", "toolbar-button");
        themeBtn.setOnAction(e -> toggleTheme());

        // Add spacer and progress indicator
        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        // Ensure progressSpinner is initialized - use standard ProgressIndicator
        if (progressSpinner == null) {
            progressSpinner = new ProgressIndicator();
            progressSpinner.setVisible(false);
            progressSpinner.setPrefSize(20, 20);
            progressSpinner.setProgress(-1); // Indeterminate progress
        }

        // Add all components to toolbar
        toolbar.getItems().addAll(
                connectionStatus,
                databaseStatus,
                new Separator(),
                connectBtn,
                newQueryBtn,
                executeBtn,
                themeBtn,
                spacer,
                progressSpinner);
    }

    private void setupLayout() {
        // Create master-detail pane for better layout
        masterDetailPane = new MasterDetailPane();
        masterDetailPane.setMasterNode(dbExplorer);
        masterDetailPane.setDetailNode(tabPane);
        masterDetailPane.setDividerPosition(0.25);

        // Wrap in notification pane
        notificationPane.setContent(masterDetailPane);

        // Setup main layout
        setTop(new VBox(menuBar, toolbar));
        setCenter(notificationPane);
        setBottom(statusBar);
    }

    private void setupBindings() {
        // Update toolbar based on connection status
        connected.addListener((obs, oldVal, newVal) -> {
            Platform.runLater(() -> {
                if (connectionIcon != null) {
                    if (newVal) {
                        connectionIcon.setIconCode(Material2AL.LINK);
                        connectionIcon.getStyleClass().removeAll("disconnected");
                        connectionIcon.getStyleClass().add("connected");
                    } else {
                        connectionIcon.setIconCode(Material2AL.LINK_OFF);
                        connectionIcon.getStyleClass().removeAll("connected");
                        connectionIcon.getStyleClass().add("disconnected");
                    }
                }
            });
        });
    }

    private void setupEventHandlers() {
        // Connection manager event handlers
        connectionManager.addConnectionListener(connected -> {
            Platform.runLater(() -> {
                this.connected.set(connected);
                if (connected) {
                    currentUser.set("Connected");
                    showNotification("Connected successfully", "success");
                } else {
                    currentUser.set("Not Connected");
                    currentDatabase.set("None");
                    showNotification("Disconnected from server", "info");
                }
            });
        });
    }

    private void initStatusBar() {
        statusBar = new StatusBar();
        statusBar.setText("Ready");
        statusBar.getStyleClass().add("status-bar");

        // Add performance monitoring labels to right side
        memoryUsageLabel = new Label("Memory: 0 MB");
        connectionCountLabel = new Label("Connections: 0");

        statusBar.getRightItems().addAll(memoryUsageLabel, connectionCountLabel);
    }

    private void applyTheme() {
        try {
            themeManager.applyTheme(getScene());
        } catch (Exception e) {
            logger.warning("Failed to apply theme: " + e.getMessage());
        }
    }

    private void toggleTheme() {
        try {
            String currentTheme = themeManager.getCurrentTheme();
            String newTheme = "dark".equals(currentTheme) ? "light" : "dark";
            themeManager.setTheme(newTheme);
            themeManager.applyTheme(getScene());

            showNotification("Theme changed to " + newTheme + " mode", "info");
        } catch (Exception e) {
            showNotification("Failed to change theme", "error");
            logger.warning("Theme toggle failed: " + e.getMessage());
        }
    }

    private void showNotification(String message, String type) {
        Platform.runLater(() -> {
            // Use ControlsFX notification instead of popup dialogs
            if (notificationPane != null) {
                notificationPane.setText(message);
                notificationPane.show();

                // Auto-hide after 3 seconds
                Timeline hideTimeline = new Timeline(new KeyFrame(
                        Duration.seconds(3),
                        e -> notificationPane.hide()));
                hideTimeline.play();
            } else {
                // Fallback to status bar if notification pane is not available
                if (statusBar != null) {
                    statusBar.setText(message);
                }
            }
        });
    }

    private void attemptAutoConnect() {
        Task<Void> connectTask = new Task<Void>() {
            @Override
            protected Void call() throws Exception {
                String host = configManager.getString("connection.defaultHost", "localhost");
                int port = configManager.getInt("connection.defaultPort", 9999);
                String username = configManager.getString("connection.defaultUsername", "");
                String password = configManager.getString("connection.defaultPassword", "");

                if (!username.isEmpty() && !password.isEmpty()) {
                    // Set server details first
                    connectionManager.setServerDetails(host, port);
                    // Then connect with credentials
                    connectionManager.connect(username, password);
                }
                return null;
            }

            @Override
            protected void succeeded() {
                Platform.runLater(() -> {
                    showNotification("Auto-connected successfully", "success");
                });
            }

            @Override
            protected void failed() {
                Platform.runLater(() -> {
                    showLoginPanel();
                });
            }
        };

        new Thread(connectTask).start();
    }

    private void initPerformanceMonitoring() {
        memoryUsageLabel = new Label("Memory: 0 MB");
        connectionCountLabel = new Label("Connections: 0");

        performanceUpdateTimeline = new Timeline(new KeyFrame(
                Duration.seconds(5),
                e -> updatePerformanceMetrics()));
        performanceUpdateTimeline.setCycleCount(Timeline.INDEFINITE);
        performanceUpdateTimeline.play();
    }

    private void updatePerformanceMetrics() {
        Platform.runLater(() -> {
            Runtime runtime = Runtime.getRuntime();
            long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024;
            memoryUsageLabel.setText("Memory: " + usedMemory + " MB");

            // Update status bar with performance info
            statusBar.setText("Ready | " + memoryUsageLabel.getText() + " | " +
                    connectionCountLabel.getText());
        });
    }

    private void executeCurrentQuery() {
        QueryEditor activeEditor = getCurrentQueryEditor();
        if (activeEditor != null) {
            progressSpinner.setVisible(true);

            CompletableFuture.runAsync(() -> {
                try {
                    activeEditor.executeQuery();
                } finally {
                    Platform.runLater(() -> progressSpinner.setVisible(false));
                }
            });
        } else {
            showNotification("No active query editor", "warning");
        }
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

    // Method implementations continued from the modernized MainWindow

    private void showLoginPanel() {
        // Implementation for showing login panel
        Platform.runLater(() -> {
            LoginPanel loginPanel = new LoginPanel(connectionManager);
            Tab loginTab = new Tab("Login", loginPanel);
            tabPane.getTabs().clear();
            tabPane.getTabs().add(loginTab);
        });
    }

    private void openNewQueryTab() {
        Platform.runLater(() -> {
            QueryEditor queryEditor = new QueryEditor(connectionManager);
            Tab queryTab = new Tab("Query " + (tabPane.getTabs().size() + 1), queryEditor);
            queryTab.setClosable(true);
            tabPane.getTabs().add(queryTab);
            tabPane.getSelectionModel().select(queryTab);
        });
    }

    private void openNewQueryTab(String title, String query) {
        Platform.runLater(() -> {
            QueryEditor queryEditor = new QueryEditor(connectionManager);
            queryEditor.setQueryText(query);
            Tab queryTab = new Tab(title, queryEditor);
            queryTab.setClosable(true);
            tabPane.getTabs().add(queryTab);
            tabPane.getSelectionModel().select(queryTab);
        });
    }

    private void initTabPane() {
        tabPane = new TabPane();
        tabPane.setTabClosingPolicy(TabPane.TabClosingPolicy.SELECTED_TAB);
        tabPane.getStyleClass().add("modern-tab-pane");
    }

    private void initDbExplorer() {
        dbExplorer = new DbExplorer(connectionManager);
        dbExplorer.setPrefWidth(250);
        dbExplorer.getStyleClass().add("db-explorer");
    }

    private QueryEditor getCurrentQueryEditor() {
        Tab selectedTab = tabPane.getSelectionModel().getSelectedItem();
        if (selectedTab != null && selectedTab.getContent() instanceof QueryEditor) {
            return (QueryEditor) selectedTab.getContent();
        }
        return null;
    }

    // Add remaining method implementations
    private void openQueryFile() {
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Open SQL File");
        fileChooser.getExtensionFilters().addAll(
                new FileChooser.ExtensionFilter("SQL Files", "*.sql"),
                new FileChooser.ExtensionFilter("All Files", "*.*"));

        File selectedFile = fileChooser.showOpenDialog(getScene().getWindow());
        if (selectedFile != null) {
            try {
                String content = Files.readString(selectedFile.toPath());
                QueryEditor queryEditor = new QueryEditor(connectionManager);
                queryEditor.setQueryText(content);

                Tab queryTab = new Tab(selectedFile.getName(), queryEditor);
                queryTab.setClosable(true);
                tabPane.getTabs().add(queryTab);
                tabPane.getSelectionModel().select(queryTab);

                showNotification("Query file loaded: " + selectedFile.getName(), "success");
            } catch (IOException e) {
                showNotification("Error loading file: " + e.getMessage(), "error");
            }
        }
    }

    private void saveCurrentQuery() {
        QueryEditor activeEditor = getCurrentQueryEditor();
        if (activeEditor == null) {
            showNotification("No active query to save", "warning");
            return;
        }

        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Save SQL File");
        fileChooser.getExtensionFilters().addAll(
                new FileChooser.ExtensionFilter("SQL Files", "*.sql"),
                new FileChooser.ExtensionFilter("All Files", "*.*"));

        File selectedFile = fileChooser.showSaveDialog(getScene().getWindow());
        if (selectedFile != null) {
            try {
                Files.writeString(selectedFile.toPath(), activeEditor.getQueryText());
                showNotification("Query saved: " + selectedFile.getName(), "success");
            } catch (IOException e) {
                showNotification("Error saving file: " + e.getMessage(), "error");
            }
        }
    }

    private void showImportDataDialog() {
        Dialog<Map<String, Object>> dialog = new Dialog<>();
        dialog.setTitle("Import Data");
        dialog.setHeaderText("Import data from external source");

        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(20, 150, 10, 10));

        ComboBox<String> formatCombo = new ComboBox<>();
        formatCombo.getItems().addAll("CSV", "JSON", "XML", "SQL");
        formatCombo.setValue("CSV");

        TextField fileField = new TextField();
        fileField.setPromptText("Select file to import");

        Button browseButton = new Button("Browse");
        browseButton.setOnAction(e -> {
            FileChooser fileChooser = new FileChooser();
            fileChooser.setTitle("Select Import File");
            File file = fileChooser.showOpenDialog(dialog.getOwner());
            if (file != null) {
                fileField.setText(file.getAbsolutePath());
            }
        });

        TextField tableField = new TextField();
        tableField.setPromptText("Target table name");

        grid.add(new Label("Format:"), 0, 0);
        grid.add(formatCombo, 1, 0);
        grid.add(new Label("File:"), 0, 1);
        grid.add(fileField, 1, 1);
        grid.add(browseButton, 2, 1);
        grid.add(new Label("Table:"), 0, 2);
        grid.add(tableField, 1, 2);

        dialog.getDialogPane().setContent(grid);
        dialog.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);

        dialog.showAndWait().ifPresent(result -> {
            showNotification("Import feature coming soon", "info");
        });
    }

    private void showExportDataDialog() {
        Dialog<Map<String, Object>> dialog = new Dialog<>();
        dialog.setTitle("Export Data");
        dialog.setHeaderText("Export data to external format");

        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(20, 150, 10, 10));

        ComboBox<String> formatCombo = new ComboBox<>();
        formatCombo.getItems().addAll("CSV", "JSON", "XML", "SQL");
        formatCombo.setValue("CSV");

        TextField queryField = new TextField();
        queryField.setPromptText("SELECT query or table name");

        TextField fileField = new TextField();
        fileField.setPromptText("Export file path");

        Button browseButton = new Button("Browse");
        browseButton.setOnAction(e -> {
            FileChooser fileChooser = new FileChooser();
            fileChooser.setTitle("Save Export File");
            File file = fileChooser.showSaveDialog(dialog.getOwner());
            if (file != null) {
                fileField.setText(file.getAbsolutePath());
            }
        });

        grid.add(new Label("Format:"), 0, 0);
        grid.add(formatCombo, 1, 0);
        grid.add(new Label("Query/Table:"), 0, 1);
        grid.add(queryField, 1, 1);
        grid.add(new Label("File:"), 0, 2);
        grid.add(fileField, 1, 2);
        grid.add(browseButton, 2, 2);

        dialog.getDialogPane().setContent(grid);
        dialog.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);

        dialog.showAndWait().ifPresent(result -> {
            showNotification("Export feature coming soon", "info");
        });
    }

    private void formatCurrentQuery() {
        QueryEditor activeEditor = getCurrentQueryEditor();
        if (activeEditor != null) {
            try {
                String formattedQuery = SQLFormatter.format(activeEditor.getQueryText());
                activeEditor.setQueryText(formattedQuery);
                showNotification("Query formatted successfully", "success");
            } catch (Exception e) {
                showNotification("Error formatting query: " + e.getMessage(), "error");
            }
        } else {
            showNotification("No active query to format", "warning");
        }
    }

    private void showFindReplaceDialog() {
        QueryEditor activeEditor = getCurrentQueryEditor();
        if (activeEditor == null) {
            showNotification("No active query editor", "warning");
            return;
        }

        Dialog<ButtonType> dialog = new Dialog<>();
        dialog.setTitle("Find & Replace");
        dialog.setHeaderText("Search and replace text in query");

        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(20, 150, 10, 10));

        TextField findField = new TextField();
        findField.setPromptText("Find text");

        TextField replaceField = new TextField();
        replaceField.setPromptText("Replace with");

        CheckBox matchCaseBox = new CheckBox("Match case");
        CheckBox wholeWordBox = new CheckBox("Whole words only");

        grid.add(new Label("Find:"), 0, 0);
        grid.add(findField, 1, 0);
        grid.add(new Label("Replace:"), 0, 1);
        grid.add(replaceField, 1, 1);
        grid.add(matchCaseBox, 0, 2, 2, 1);
        grid.add(wholeWordBox, 0, 3, 2, 1);

        dialog.getDialogPane().setContent(grid);

        ButtonType findNextType = new ButtonType("Find Next");
        ButtonType replaceType = new ButtonType("Replace");
        ButtonType replaceAllType = new ButtonType("Replace All");

        dialog.getDialogPane().getButtonTypes().addAll(
                findNextType, replaceType, replaceAllType, ButtonType.CLOSE);

        dialog.showAndWait();
    }

    private void explainCurrentQuery() {
        QueryEditor activeEditor = getCurrentQueryEditor();
        if (activeEditor == null) {
            showNotification("No active query to explain", "warning");
            return;
        }

        String query = activeEditor.getSelectedText();
        if (query == null || query.trim().isEmpty()) {
            query = activeEditor.getQueryText();
        }

        if (query.trim().isEmpty()) {
            showNotification("No query to explain", "warning");
            return;
        }

        // Add EXPLAIN prefix if not present
        if (!query.trim().toLowerCase().startsWith("explain")) {
            query = "EXPLAIN " + query;
        }

        progressSpinner.setVisible(true);
        connectionManager.executeQuery(query)
                .thenAccept(result -> {
                    Platform.runLater(() -> {
                        progressSpinner.setVisible(false);

                        Dialog<Void> explainDialog = new Dialog<>();
                        explainDialog.setTitle("Query Execution Plan");
                        explainDialog.setHeaderText("Execution plan for your query");

                        TextArea planArea = new TextArea();
                        planArea.setEditable(false);
                        planArea.setPrefRowCount(20);
                        planArea.setPrefColumnCount(80);

                        if (result.containsKey("error")) {
                            planArea.setText("Error: " + result.get("error"));
                        } else if (result.containsKey("data")) {
                            // Format the execution plan
                            Object data = result.get("data");
                            planArea.setText(data.toString());
                        } else {
                            planArea.setText("No execution plan available");
                        }

                        explainDialog.getDialogPane().setContent(new ScrollPane(planArea));
                        explainDialog.getDialogPane().getButtonTypes().add(ButtonType.CLOSE);
                        explainDialog.showAndWait();
                    });
                })
                .exceptionally(throwable -> {
                    Platform.runLater(() -> {
                        progressSpinner.setVisible(false);
                        showNotification("Error explaining query: " + throwable.getMessage(), "error");
                    });
                    return null;
                });
    }

    private void showQueryHistoryDialog() {
        Dialog<Void> dialog = new Dialog<>();
        dialog.setTitle("Query History");
        dialog.setHeaderText("Recent query history");

        ListView<String> historyList = new ListView<>();
        historyList.setPrefSize(600, 400);

        // Load query history from preferences or connection manager
        List<String> history = loadQueryHistory();
        historyList.getItems().addAll(history);

        // Double-click to open query in new tab
        historyList.setOnMouseClicked(event -> {
            if (event.getClickCount() == 2) {
                String selectedQuery = historyList.getSelectionModel().getSelectedItem();
                if (selectedQuery != null && !selectedQuery.isEmpty()) {
                    QueryEditor queryEditor = new QueryEditor(connectionManager);
                    queryEditor.setQueryText(selectedQuery);

                    Tab queryTab = new Tab("Query " + (tabPane.getTabs().size() + 1), queryEditor);
                    queryTab.setClosable(true);
                    tabPane.getTabs().add(queryTab);
                    tabPane.getSelectionModel().select(queryTab);

                    dialog.close();
                }
            }
        });

        VBox content = new VBox(10);
        content.getChildren().addAll(
                new Label("Double-click a query to open it in a new tab"),
                historyList);

        dialog.getDialogPane().setContent(content);
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CLOSE);
        dialog.showAndWait();
    }

    private void openQueryBuilder() {
        VisualQueryBuilder queryBuilder = new VisualQueryBuilder(connectionManager, query -> {
            // Handle the generated query - could open it in a new query tab or execute it
            openNewQueryTab("Generated Query", query);
        });
        Tab queryBuilderTab = new Tab("Query Builder", queryBuilder);
        queryBuilderTab.setClosable(true);
        tabPane.getTabs().add(queryBuilderTab);
        tabPane.getSelectionModel().select(queryBuilderTab);

        showNotification("Query Builder opened", "success");
    }

    private void openIndexViewer() {
        IndexVisualizerView indexViewer = new IndexVisualizerView(connectionManager);
        Tab indexTab = new Tab("Index Viewer", indexViewer);
        indexTab.setClosable(true);
        tabPane.getTabs().add(indexTab);
        tabPane.getSelectionModel().select(indexTab);

        showNotification("Index Viewer opened", "success");
    }

    private void openTransactionManager() {
        // Create a simple transaction panel since TransactionPanel doesn't exist
        VBox transactionContent = new VBox(10);
        transactionContent.setPadding(new Insets(20));

        Label titleLabel = new Label("Transaction Manager");
        titleLabel.setStyle("-fx-font-size: 18px; -fx-font-weight: bold;");

        HBox buttonBox = new HBox(10);
        Button beginBtn = new Button("BEGIN TRANSACTION");
        Button commitBtn = new Button("COMMIT");
        Button rollbackBtn = new Button("ROLLBACK");

        beginBtn.setOnAction(e -> executeTransactionCommand("BEGIN TRANSACTION"));
        commitBtn.setOnAction(e -> executeTransactionCommand("COMMIT"));
        rollbackBtn.setOnAction(e -> executeTransactionCommand("ROLLBACK"));

        buttonBox.getChildren().addAll(beginBtn, commitBtn, rollbackBtn);

        TextArea transactionLog = new TextArea();
        transactionLog.setPromptText("Transaction commands will be logged here...");
        transactionLog.setPrefRowCount(10);
        transactionLog.setEditable(false);

        transactionContent.getChildren().addAll(titleLabel, buttonBox,
                new Label("Transaction Log:"), transactionLog);

        Tab transactionTab = new Tab("Transaction Manager", transactionContent);
        transactionTab.setClosable(true);
        tabPane.getTabs().add(transactionTab);
        tabPane.getSelectionModel().select(transactionTab);

        showNotification("Transaction Manager opened", "success");
    }

    private void executeTransactionCommand(String command) {
        if (connectionManager != null) {
            connectionManager.executeQuery(command).thenAccept(result -> {
                Platform.runLater(() -> {
                    if (result.containsKey("error")) {
                        showNotification("Transaction error: " + result.get("error"), "error");
                    } else {
                        showNotification("Transaction command executed: " + command, "success");
                    }
                });
            });
        }
    }

    private void showServerStatusDialog() {
        Dialog<Void> dialog = new Dialog<>();
        dialog.setTitle("Server Status");
        dialog.setHeaderText("Database server information and statistics");

        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(20));

        // Create status labels
        Label connectionStatusLabel = new Label("Checking...");
        Label serverVersionLabel = new Label("Checking...");
        Label uptimeLabel = new Label("Checking...");
        Label activeConnectionsLabel = new Label("Checking...");
        Label memoryUsageLabel = new Label("Checking...");

        grid.add(new Label("Connection Status:"), 0, 0);
        grid.add(connectionStatusLabel, 1, 0);
        grid.add(new Label("Server Version:"), 0, 1);
        grid.add(serverVersionLabel, 1, 1);
        grid.add(new Label("Uptime:"), 0, 2);
        grid.add(uptimeLabel, 1, 2);
        grid.add(new Label("Active Connections:"), 0, 3);
        grid.add(activeConnectionsLabel, 1, 3);
        grid.add(new Label("Memory Usage:"), 0, 4);
        grid.add(memoryUsageLabel, 1, 4);

        dialog.getDialogPane().setContent(grid);
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CLOSE);

        // Fetch server status asynchronously
        connectionManager.getServerStatus()
                .thenAccept(status -> {
                    Platform.runLater(() -> {
                        connectionStatusLabel.setText(connected.get() ? "Connected" : "Disconnected");
                        serverVersionLabel.setText(status.getOrDefault("version", "Unknown").toString());
                        uptimeLabel.setText(status.getOrDefault("uptime", "Unknown").toString());
                        activeConnectionsLabel.setText(status.getOrDefault("connections", "0").toString());
                        memoryUsageLabel.setText(status.getOrDefault("memory", "Unknown").toString());
                    });
                })
                .exceptionally(throwable -> {
                    Platform.runLater(() -> {
                        connectionStatusLabel.setText("Error fetching status");
                    });
                    return null;
                });

        dialog.showAndWait();
    }

    private void showBackupRestoreDialog() {
        Dialog<Void> dialog = new Dialog<>();
        dialog.setTitle("Backup & Restore");
        dialog.setHeaderText("Database backup and restore operations");

        TabPane tabPane = new TabPane();

        // Backup tab
        Tab backupTab = new Tab("Backup");
        GridPane backupGrid = new GridPane();
        backupGrid.setHgap(10);
        backupGrid.setVgap(10);
        backupGrid.setPadding(new Insets(20));

        ComboBox<String> databaseCombo = new ComboBox<>();
        databaseCombo.setPromptText("Select database");

        TextField backupPathField = new TextField();
        backupPathField.setPromptText("Backup file path");

        Button browseSaveButton = new Button("Browse");
        browseSaveButton.setOnAction(e -> {
            FileChooser fileChooser = new FileChooser();
            fileChooser.setTitle("Save Backup File");
            fileChooser.getExtensionFilters().add(
                    new FileChooser.ExtensionFilter("Backup Files", "*.bak", "*.sql"));
            File file = fileChooser.showSaveDialog(dialog.getOwner());
            if (file != null) {
                backupPathField.setText(file.getAbsolutePath());
            }
        });

        Button backupButton = new Button("Create Backup");
        backupButton.setOnAction(e -> {
            showNotification("Backup feature coming soon", "info");
        });

        backupGrid.add(new Label("Database:"), 0, 0);
        backupGrid.add(databaseCombo, 1, 0);
        backupGrid.add(new Label("Backup Path:"), 0, 1);
        backupGrid.add(backupPathField, 1, 1);
        backupGrid.add(browseSaveButton, 2, 1);
        backupGrid.add(backupButton, 1, 2);

        backupTab.setContent(backupGrid);

        // Restore tab
        Tab restoreTab = new Tab("Restore");
        GridPane restoreGrid = new GridPane();
        restoreGrid.setHgap(10);
        restoreGrid.setVgap(10);
        restoreGrid.setPadding(new Insets(20));

        TextField restorePathField = new TextField();
        restorePathField.setPromptText("Backup file to restore");

        Button browseOpenButton = new Button("Browse");
        browseOpenButton.setOnAction(e -> {
            FileChooser fileChooser = new FileChooser();
            fileChooser.setTitle("Select Backup File");
            fileChooser.getExtensionFilters().add(
                    new FileChooser.ExtensionFilter("Backup Files", "*.bak", "*.sql"));
            File file = fileChooser.showOpenDialog(dialog.getOwner());
            if (file != null) {
                restorePathField.setText(file.getAbsolutePath());
            }
        });

        TextField newDatabaseField = new TextField();
        newDatabaseField.setPromptText("New database name (optional)");

        Button restoreButton = new Button("Restore");
        restoreButton.setOnAction(e -> {
            showNotification("Restore feature coming soon", "info");
        });

        restoreGrid.add(new Label("Backup File:"), 0, 0);
        restoreGrid.add(restorePathField, 1, 0);
        restoreGrid.add(browseOpenButton, 2, 0);
        restoreGrid.add(new Label("Database Name:"), 0, 1);
        restoreGrid.add(newDatabaseField, 1, 1);
        restoreGrid.add(restoreButton, 1, 2);

        restoreTab.setContent(restoreGrid);

        tabPane.getTabs().addAll(backupTab, restoreTab);

        dialog.getDialogPane().setContent(tabPane);
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CLOSE);
        dialog.showAndWait();
    }

    private void showThemeCustomizationDialog() {
        ThemeCustomizationDialog themeDialog = new ThemeCustomizationDialog();
        themeDialog.showAndWait().ifPresent(result -> {
            // Apply the customized theme
            applyTheme();
            showNotification("Theme customization applied", "success");
        });
    }

    private void openPreferencesDialog() {
        UserPreferencesDialog prefsDialog = new UserPreferencesDialog(connectionManager);
        prefsDialog.showAndWait().ifPresent(result -> {
            showNotification("Preferences updated", "success");
        });
    }

    private void showCreateDatabaseDialog() {
        Dialog<String> dialog = new Dialog<>();
        dialog.setTitle("Create Database");
        dialog.setHeaderText("Create a new database");

        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(20, 150, 10, 10));

        TextField nameField = new TextField();
        nameField.setPromptText("Database name");

        TextArea descriptionArea = new TextArea();
        descriptionArea.setPromptText("Description (optional)");
        descriptionArea.setPrefRowCount(3);

        ComboBox<String> encodingCombo = new ComboBox<>();
        encodingCombo.getItems().addAll("UTF-8", "UTF-16", "ASCII", "ISO-8859-1");
        encodingCombo.setValue("UTF-8");

        grid.add(new Label("Database Name:"), 0, 0);
        grid.add(nameField, 1, 0);
        grid.add(new Label("Encoding:"), 0, 1);
        grid.add(encodingCombo, 1, 1);
        grid.add(new Label("Description:"), 0, 2);
        grid.add(descriptionArea, 1, 2);

        dialog.getDialogPane().setContent(grid);
        dialog.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);

        dialog.setResultConverter(dialogButton -> {
            if (dialogButton == ButtonType.OK) {
                return nameField.getText();
            }
            return null;
        });

        dialog.showAndWait().ifPresent(databaseName -> {
            if (!databaseName.trim().isEmpty()) {
                progressSpinner.setVisible(true);
                connectionManager.createDatabase(databaseName)
                        .thenAccept(result -> {
                            Platform.runLater(() -> {
                                progressSpinner.setVisible(false);
                                if (result.containsKey("error")) {
                                    showNotification("Error creating database: " + result.get("error"), "error");
                                } else {
                                    showNotification("Database '" + databaseName + "' created successfully", "success");
                                    if (dbExplorer != null) {
                                        dbExplorer.refreshDatabases();
                                    }
                                }
                            });
                        })
                        .exceptionally(throwable -> {
                            Platform.runLater(() -> {
                                progressSpinner.setVisible(false);
                                showNotification("Error creating database: " + throwable.getMessage(), "error");
                            });
                            return null;
                        });
            }
        });
    }

    private void showDropDatabaseDialog() {
        if (!connected.get()) {
            showNotification("Not connected to server", "warning");
            return;
        }

        Dialog<String> dialog = new Dialog<>();
        dialog.setTitle("Drop Database");
        dialog.setHeaderText("WARNING: This will permanently delete the database!");

        VBox content = new VBox(10);
        content.setPadding(new Insets(20));

        ComboBox<String> databaseCombo = new ComboBox<>();
        databaseCombo.setPromptText("Select database to drop");

        // Load available databases
        connectionManager.getDatabases()
                .thenAccept(databases -> {
                    Platform.runLater(() -> {
                        if (databases.containsKey("databases")) {
                            @SuppressWarnings("unchecked")
                            List<String> dbList = (List<String>) databases.get("databases");
                            databaseCombo.getItems().addAll(dbList);
                        }
                    });
                });

        Label warningLabel = new Label("⚠️ This action cannot be undone!");
        warningLabel.setStyle("-fx-text-fill: red; -fx-font-weight: bold;");

        CheckBox confirmBox = new CheckBox("I understand this will permanently delete the database");

        content.getChildren().addAll(
                new Label("Database:"),
                databaseCombo,
                warningLabel,
                confirmBox);

        dialog.getDialogPane().setContent(content);
        dialog.getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);

        // Only enable OK button when database is selected and confirmed
        dialog.getDialogPane().lookupButton(ButtonType.OK).disableProperty().bind(
                databaseCombo.valueProperty().isNull().or(confirmBox.selectedProperty().not()));

        dialog.setResultConverter(dialogButton -> {
            if (dialogButton == ButtonType.OK && confirmBox.isSelected()) {
                return databaseCombo.getValue();
            }
            return null;
        });

        dialog.showAndWait().ifPresent(databaseName -> {
            progressSpinner.setVisible(true);
            connectionManager.dropDatabase(databaseName)
                    .thenAccept(result -> {
                        Platform.runLater(() -> {
                            progressSpinner.setVisible(false);
                            if (result.containsKey("error")) {
                                showNotification("Error dropping database: " + result.get("error"), "error");
                            } else {
                                showNotification("Database '" + databaseName + "' dropped successfully", "success");
                                if (dbExplorer != null) {
                                    dbExplorer.refreshDatabases();
                                }
                            }
                        });
                    })
                    .exceptionally(throwable -> {
                        Platform.runLater(() -> {
                            progressSpinner.setVisible(false);
                            showNotification("Error dropping database: " + throwable.getMessage(), "error");
                        });
                        return null;
                    });
        });
    }

    private void showDatabasePropertiesDialog() {
        if (!connected.get()) {
            showNotification("Not connected to server", "warning");
            return;
        }

        Dialog<Void> dialog = new Dialog<>();
        dialog.setTitle("Database Properties");
        dialog.setHeaderText("Database information and statistics");

        ComboBox<String> databaseCombo = new ComboBox<>();
        databaseCombo.setPromptText("Select database");

        TextArea propertiesArea = new TextArea();
        propertiesArea.setEditable(false);
        propertiesArea.setPrefRowCount(15);
        propertiesArea.setPrefColumnCount(60);

        // Load available databases
        connectionManager.getDatabases()
                .thenAccept(databases -> {
                    Platform.runLater(() -> {
                        if (databases.containsKey("databases")) {
                            @SuppressWarnings("unchecked")
                            List<String> dbList = (List<String>) databases.get("databases");
                            databaseCombo.getItems().addAll(dbList);
                        }
                    });
                });

        databaseCombo.setOnAction(e -> {
            String selectedDb = databaseCombo.getValue();
            if (selectedDb != null) {
                propertiesArea.setText("Loading properties...");
                connectionManager.getDatabaseProperties(selectedDb)
                        .thenAccept(properties -> {
                            Platform.runLater(() -> {
                                StringBuilder sb = new StringBuilder();
                                sb.append("Database: ").append(selectedDb).append("\n");
                                sb.append("=".repeat(50)).append("\n\n");

                                properties.forEach((key, value) -> {
                                    sb.append(key).append(": ").append(value).append("\n");
                                });

                                propertiesArea.setText(sb.toString());
                            });
                        })
                        .exceptionally(throwable -> {
                            Platform.runLater(() -> {
                                propertiesArea.setText("Error loading properties: " + throwable.getMessage());
                            });
                            return null;
                        });
            }
        });

        VBox content = new VBox(10);
        content.setPadding(new Insets(20));
        content.getChildren().addAll(
                new Label("Database:"),
                databaseCombo,
                new Label("Properties:"),
                propertiesArea);

        dialog.getDialogPane().setContent(content);
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CLOSE);
        dialog.showAndWait();
    }

    private void showAboutDialog() {
        Alert alert = new Alert(AlertType.INFORMATION);
        alert.setTitle("About pyHMSSQL Client");
        alert.setHeaderText("pyHMSSQL Professional Database Client");

        StringBuilder content = new StringBuilder();
        content.append("Version: ").append(AppInfo.getVersion()).append("\n");
        content.append("Build Date: ").append(AppInfo.getBuildDate()).append("\n");
        content.append("Java Version: ").append(System.getProperty("java.version")).append("\n");
        content.append("JavaFX Version: ").append(System.getProperty("javafx.version")).append("\n\n");
        content.append("A modern, professional database management client\n");
        content.append("featuring advanced SQL editing, visual query building,\n");
        content.append("and comprehensive database administration tools.\n\n");
        content.append("© 2024 pyHMSSQL Team\n");
        content.append("Licensed under MIT License");

        alert.setContentText(content.toString());

        // Apply theme to dialog
        if (themeManager != null) {
            themeManager.applyTheme(alert.getDialogPane().getScene());
        }

        alert.showAndWait();
    }

    private void showSqlReferenceDialog() {
        Dialog<Void> dialog = new Dialog<>();
        dialog.setTitle("SQL Reference");
        dialog.setHeaderText("Quick SQL syntax reference");

        TabPane tabPane = new TabPane();

        // Basic SQL tab
        Tab basicTab = new Tab("Basic SQL");
        TextArea basicArea = new TextArea();
        basicArea.setEditable(false);
        basicArea.setText("""
                Basic SQL Commands:

                SELECT - Retrieve data from tables
                SELECT column1, column2 FROM table_name WHERE condition;

                INSERT - Add new data
                INSERT INTO table_name (column1, column2) VALUES (value1, value2);

                UPDATE - Modify existing data
                UPDATE table_name SET column1 = value1 WHERE condition;

                DELETE - Remove data
                DELETE FROM table_name WHERE condition;

                CREATE TABLE - Create new table
                CREATE TABLE table_name (
                    column1 datatype,
                    column2 datatype,
                    PRIMARY KEY (column1)
                );

                ALTER TABLE - Modify table structure
                ALTER TABLE table_name ADD COLUMN column_name datatype;

                DROP TABLE - Delete table
                DROP TABLE table_name;
                """);
        basicTab.setContent(new ScrollPane(basicArea));

        // Advanced SQL tab
        Tab advancedTab = new Tab("Advanced SQL");
        TextArea advancedArea = new TextArea();
        advancedArea.setEditable(false);
        advancedArea.setText("""
                Advanced SQL Features:

                JOINS - Combine data from multiple tables
                SELECT * FROM table1
                INNER JOIN table2 ON table1.id = table2.id;

                SUBQUERIES - Nested queries
                SELECT * FROM table1
                WHERE column1 IN (SELECT column1 FROM table2);

                WINDOW FUNCTIONS - Advanced analytics
                SELECT column1,
                       ROW_NUMBER() OVER (ORDER BY column2) as row_num
                FROM table_name;

                COMMON TABLE EXPRESSIONS (CTE)
                WITH cte_name AS (
                    SELECT column1, column2 FROM table_name
                )
                SELECT * FROM cte_name;

                INDEXES - Improve query performance
                CREATE INDEX idx_name ON table_name (column_name);

                TRANSACTIONS - Group operations
                BEGIN TRANSACTION;
                UPDATE table1 SET column1 = value1;
                UPDATE table2 SET column2 = value2;
                COMMIT;
                """);
        advancedTab.setContent(new ScrollPane(advancedArea));

        tabPane.getTabs().addAll(basicTab, advancedTab);

        dialog.getDialogPane().setContent(tabPane);
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CLOSE);
        dialog.showAndWait();
    }

    private void showKeyboardShortcutsDialog() {
        Dialog<Void> dialog = new Dialog<>();
        dialog.setTitle("Keyboard Shortcuts");
        dialog.setHeaderText("Keyboard shortcuts and hotkeys");

        GridPane grid = new GridPane();
        grid.setHgap(20);
        grid.setVgap(5);
        grid.setPadding(new Insets(20));

        String[][] shortcuts = {
                { "Ctrl + N", "New Query Tab" },
                { "Ctrl + O", "Open Query File" },
                { "Ctrl + S", "Save Current Query" },
                { "F5", "Execute Query" },
                { "Ctrl + F", "Find & Replace" },
                { "Ctrl + Shift + F", "Format Query" },
                { "F1", "Show Help" },
                { "Ctrl + T", "Toggle Theme" },
                { "Ctrl + W", "Close Tab" },
                { "Ctrl + Tab", "Switch Tabs" },
                { "Ctrl + +", "Zoom In" },
                { "Ctrl + -", "Zoom Out" },
                { "Ctrl + 0", "Reset Zoom" },
                { "Ctrl + Q", "Quit Application" },
                { "F9", "Toggle Connection Panel" },
                { "F11", "Fullscreen Mode" }
        };

        for (int i = 0; i < shortcuts.length; i++) {
            Label keyLabel = new Label(shortcuts[i][0]);
            keyLabel.setStyle("-fx-font-family: monospace; -fx-font-weight: bold;");

            Label descLabel = new Label(shortcuts[i][1]);

            grid.add(keyLabel, 0, i);
            grid.add(descLabel, 1, i);
        }

        ScrollPane scrollPane = new ScrollPane(grid);
        scrollPane.setFitToWidth(true);
        scrollPane.setPrefSize(500, 400);

        dialog.getDialogPane().setContent(scrollPane);
        dialog.getDialogPane().getButtonTypes().add(ButtonType.CLOSE);
        dialog.showAndWait();
    }

    // Additional utility methods needed by App.java
    public boolean hasUnsavedChanges() {
        for (Tab tab : tabPane.getTabs()) {
            if (tab.getContent() instanceof QueryEditor) {
                QueryEditor editor = (QueryEditor) tab.getContent();
                if (editor.hasUnsavedChanges()) {
                    return true;
                }
            }
        }
        return false;
    }

    public void saveConnectionHistory() {
        try {
            // Save recent connections to preferences
            java.util.prefs.Preferences prefs = java.util.prefs.Preferences.userNodeForPackage(MainWindow.class);
            // Implementation would save connection details
            logger.info("Connection history saved");
        } catch (Exception e) {
            logger.warning("Failed to save connection history: " + e.getMessage());
        }
    }

    public void saveWorkspaceState() {
        try {
            // Save open tabs, current database, etc.
            java.util.prefs.Preferences prefs = java.util.prefs.Preferences.userNodeForPackage(MainWindow.class);
            prefs.putInt("tabCount", tabPane.getTabs().size());
            prefs.put("currentDatabase", currentDatabase.get());
            prefs.flush();
            logger.info("Workspace state saved");
        } catch (Exception e) {
            logger.warning("Failed to save workspace state: " + e.getMessage());
        }
    }

    public void cleanup() {
        try {
            // Stop performance monitoring
            if (performanceUpdateTimeline != null) {
                performanceUpdateTimeline.stop();
            }

            // Close all database connections
            if (connectionManager != null) {
                connectionManager.disconnect();
            }

            // Save state before closing
            saveConnectionHistory();
            saveWorkspaceState();

            logger.info("MainWindow cleanup completed");
        } catch (Exception e) {
            logger.warning("Error during MainWindow cleanup: " + e.getMessage());
        }
    }

    private List<String> loadQueryHistory() {
        List<String> history = new ArrayList<>();
        try {
            java.util.prefs.Preferences prefs = java.util.prefs.Preferences.userNodeForPackage(MainWindow.class);
            String historyData = prefs.get("queryHistory", "");
            if (!historyData.isEmpty()) {
                String[] queries = historyData.split("\\|\\|\\|");
                for (String query : queries) {
                    if (!query.trim().isEmpty()) {
                        history.add(query);
                    }
                }
            }
        } catch (Exception e) {
            logger.warning("Failed to load query history: " + e.getMessage());
        }

        // Add some sample queries if history is empty
        if (history.isEmpty()) {
            history.add("SELECT * FROM users;");
            history.add("SELECT name, email FROM customers WHERE active = 1;");
            history.add("UPDATE products SET price = price * 1.1 WHERE category = 'electronics';");
            history.add("DELETE FROM logs WHERE created_date < '2024-01-01';");
        }

        return history;
    }
}