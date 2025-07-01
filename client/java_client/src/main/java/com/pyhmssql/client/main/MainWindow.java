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
import com.jfoenix.controls.*;
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

    // Modern UI Controls
    private JFXToolbar toolbar;
    private JFXSnackbar snackbar;
    private JFXSpinner progressSpinner;

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
        // Initialize core components
        initMenuBar();
        initToolbar();
        initTabPane();
        initDbExplorer();
        initStatusBar();

        // Initialize modern UI components
        progressSpinner = new JFXSpinner();
        progressSpinner.setVisible(false);
        progressSpinner.setPrefSize(20, 20);

        snackbar = new JFXSnackbar();
        snackbar.setPrefWidth(400);

        // Setup notification pane
        notificationPane = new NotificationPane();
        notificationPane.getStyleClass().add("notification-pane");
    }

    private void initToolbar() {
        toolbar = new JFXToolbar();
        toolbar.setStyle("-fx-background-color: -fx-primary;");

        // Connection status indicator
        FontIcon connectionIcon = new FontIcon(Material2AL.LINK_OFF);
        connectionIcon.setIconSize(16);
        connectionIcon.getStyleClass().add("toolbar-icon");

        Label connectionStatus = new Label();
        connectionStatus.textProperty().bind(currentUser);
        connectionStatus.setGraphic(connectionIcon);
        connectionStatus.getStyleClass().add("connection-status");

        // Database indicator
        FontIcon databaseIcon = new FontIcon(Material2MZ.STORAGE);
        databaseIcon.setIconSize(16);
        databaseIcon.getStyleClass().add("toolbar-icon");

        Label databaseStatus = new Label();
        databaseStatus.textProperty().bind(currentDatabase);
        databaseStatus.setGraphic(databaseIcon);
        databaseStatus.getStyleClass().add("database-status");

        // Quick action buttons
        JFXButton connectBtn = new JFXButton("", new FontIcon(Material2AL.LOGIN));
        connectBtn.getStyleClass().addAll("jfx-button", "toolbar-button");
        connectBtn.setOnAction(e -> showLoginPanel());

        JFXButton newQueryBtn = new JFXButton("", new FontIcon(Material2AL.ADD));
        newQueryBtn.getStyleClass().addAll("jfx-button", "toolbar-button");
        newQueryBtn.setOnAction(e -> openNewQueryTab());

        JFXButton executeBtn = new JFXButton("", new FontIcon(Material2MZ.PLAY_ARROW));
        executeBtn.getStyleClass().addAll("jfx-button", "toolbar-button");
        executeBtn.setOnAction(e -> executeCurrentQuery());

        // Theme toggle button
        JFXButton themeBtn = new JFXButton("", new FontIcon(Material2MZ.PALETTE));
        themeBtn.getStyleClass().addAll("jfx-button", "toolbar-button");
        themeBtn.setOnAction(e -> toggleTheme());

        // Add spacer and progress indicator
        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);

        HBox leftBox = new HBox(10, connectionStatus, databaseStatus);
        leftBox.setAlignment(Pos.CENTER_LEFT);

        HBox rightBox = new HBox(5, connectBtn, newQueryBtn, executeBtn, themeBtn, progressSpinner);
        rightBox.setAlignment(Pos.CENTER_RIGHT);

        toolbar.setLeft(leftBox);
        toolbar.setRight(rightBox);
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

        // Add snackbar
        StackPane root = new StackPane(this);
        snackbar.registerSnackbarContainer(root);
    }

    private void setupBindings() {
        // Update toolbar based on connection status
        connected.addListener((obs, oldVal, newVal) -> {
            Platform.runLater(() -> {
                FontIcon icon = (FontIcon) ((Label) toolbar.getLeft()).getGraphic();
                if (newVal) {
                    icon.setIconCode(Material2AL.LINK);
                    icon.getStyleClass().removeAll("disconnected");
                    icon.getStyleClass().add("connected");
                } else {
                    icon.setIconCode(Material2AL.LINK_OFF);
                    icon.getStyleClass().removeAll("connected");
                    icon.getStyleClass().add("disconnected");
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
            snackbar.fireEvent(new JFXSnackbar.SnackbarEvent(
                    new JFXSnackbarLayout(message, "DISMISS", e -> snackbar.close()),
                    Duration.millis(3000),
                    null));
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

    // Add remaining method stubs for compilation
    private void openQueryFile() {
        /* Implementation */ }

    private void saveCurrentQuery() {
        /* Implementation */ }

    private void showImportDataDialog() {
        /* Implementation */ }

    private void showExportDataDialog() {
        /* Implementation */ }

    private void formatCurrentQuery() {
        /* Implementation */ }

    private void showFindReplaceDialog() {
        /* Implementation */ }

    private void explainCurrentQuery() {
        /* Implementation */ }

    private void showQueryHistoryDialog() {
        /* Implementation */ }

    private void openQueryBuilder() {
        /* Implementation */ }

    private void openIndexViewer() {
        /* Implementation */ }

    private void openTransactionManager() {
        /* Implementation */ }

    private void showServerStatusDialog() {
        /* Implementation */ }

    private void showBackupRestoreDialog() {
        /* Implementation */ }

    private void showThemeCustomizationDialog() {
        /* Implementation */ }

    private void openPreferencesDialog() {
        /* Implementation */ }

    private void showCreateDatabaseDialog() {
        /* Implementation */ }

    private void showDropDatabaseDialog() {
        /* Implementation */ }

    private void showDatabasePropertiesDialog() {
        /* Implementation */ }

    private void showAboutDialog() {
        /* Implementation */ }

    private void showSqlReferenceDialog() {
        /* Implementation */ }

    private void showKeyboardShortcutsDialog() {
        /* Implementation */ }
}