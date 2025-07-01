package com.pyhmssql.client.main;

import com.pyhmssql.client.config.ConfigurationManager;
import com.pyhmssql.client.theme.ThemeManager;
import com.pyhmssql.client.utils.GlobalExceptionHandler;
import com.pyhmssql.client.utils.AppInfo;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.image.Image;
import javafx.stage.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * pyHMSSQL Professional Client - Main Application Entry Point
 * 
 * A world-class JavaFX database management client featuring:
 * - Modern Material Design 3 UI
 * - Advanced SQL editing with syntax highlighting
 * - Visual query builder
 * - Real-time performance monitoring
 * - Multi-database connection management
 * - Professional theming system
 * - Comprehensive configuration management
 */
public class App extends Application {
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    
    private ConfigurationManager config;
    private ThemeManager themeManager;
    private Stage primaryStage;
    
    // Application lifecycle
    
    @Override
    public void init() throws Exception {
        super.init();
        
        // Initialize core services
        initializeConfiguration();
        initializeThemeManager();
        initializeExceptionHandling();
        
        logger.info("=== pyHMSSQL Professional Client Starting ===");
        logger.info("Version: {}", AppInfo.getVersion());
        logger.info("Build Date: {}", AppInfo.getBuildDate());
        logger.info("Java Version: {}", System.getProperty("java.version"));
        logger.info("JavaFX Version: {}", System.getProperty("javafx.version"));
        
        // Validate configuration
        if (!config.validateConfiguration()) {
            logger.error("Configuration validation failed - some features may not work correctly");
        }
        
        config.logConfigurationInfo();
        
        // Platform-specific initialization
        Platform.setImplicitExit(true);
        
        // Configure JVM shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::onApplicationExit));
    }

    @Override
    public void start(Stage stage) {
        this.primaryStage = stage;
        
        try {
            // Configure the primary stage
            configurePrimaryStage(stage);
            
            // Create and configure the main window
            MainWindow mainWindow = new MainWindow();
            Scene scene = createMainScene(mainWindow);
            
            // Apply theme and show the window
            themeManager.applyTheme(scene);
            stage.setScene(scene);
            
            // Show the application
            showApplication(stage);
            
            logger.info("Application started successfully");
            
        } catch (Exception e) {
            logger.error("Failed to start application", e);
            showErrorDialog("Startup Error", 
                          "Failed to start pyHMSSQL Client: " + e.getMessage());
            Platform.exit();
        }
    }
    
    @Override
    public void stop() throws Exception {
        logger.info("Application shutting down...");
        
        try {
            // Save application state
            saveApplicationState();
            
            // Clean up resources
            cleanup();
            
        } catch (Exception e) {
            logger.error("Error during application shutdown", e);
        } finally {
            super.stop();
        }
    }
    
    // Initialization methods
    
    private void initializeConfiguration() {
        try {
            config = ConfigurationManager.getInstance();
            logger.info("Configuration initialized for environment: {}", 
                      config.getEnvironment());
        } catch (Exception e) {
            logger.error("Failed to initialize configuration", e);
            throw new RuntimeException("Configuration initialization failed", e);
        }
    }
    
    private void initializeThemeManager() {
        try {
            themeManager = ThemeManager.getInstance();
            logger.info("Theme manager initialized with theme: {}", 
                      themeManager.getCurrentTheme());
        } catch (Exception e) {
            logger.error("Failed to initialize theme manager", e);
            throw new RuntimeException("Theme manager initialization failed", e);
        }
    }
    
    private void initializeExceptionHandling() {
        // Set up global exception handling
        Thread.setDefaultUncaughtExceptionHandler(new GlobalExceptionHandler());
        
        // JavaFX platform exception handling
        Platform.runLater(() -> {
            Thread.currentThread().setUncaughtExceptionHandler(
                new GlobalExceptionHandler());
        });
    }
    
    // Stage configuration
    
    private void configurePrimaryStage(Stage stage) {
        // Set application info
        stage.setTitle(String.format("%s v%s", 
                      config.getString("name", "pyHMSSQL Professional Client"),
                      AppInfo.getVersion()));
        
        // Set application icon
        try {
            Image icon = new Image(Objects.requireNonNull(
                getClass().getResourceAsStream("/icons/app-icon.png")));
            stage.getIcons().add(icon);
        } catch (Exception e) {
            logger.warn("Could not load application icon", e);
        }
        
        // Configure window properties
        stage.setMinWidth(ConfigurationManager.UI.Window.getMinWidth());
        stage.setMinHeight(ConfigurationManager.UI.Window.getMinHeight());
        
        if (ConfigurationManager.UI.Window.rememberSize()) {
            stage.setWidth(ConfigurationManager.UI.Window.getWidth());
            stage.setHeight(ConfigurationManager.UI.Window.getHeight());
        }
        
        stage.setMaximized(ConfigurationManager.UI.Window.isMaximized());
        
        // Handle window close request
        stage.setOnCloseRequest(event -> {
            if (!handleCloseRequest()) {
                event.consume();
            }
        });
    }
    
    private Scene createMainScene(MainWindow mainWindow) {
        Scene scene = new Scene(mainWindow, 
                               ConfigurationManager.UI.Window.getWidth(),
                               ConfigurationManager.UI.Window.getHeight());
        
        // Set scene properties
        scene.getStylesheets().add(
            Objects.requireNonNull(getClass().getResource("/styles/base.css"))
                   .toExternalForm());
        
        return scene;
    }
    
    private void showApplication(Stage stage) {
        // Show the stage
        stage.show();
        
        // Request focus
        stage.requestFocus();
        
        // Center on screen if not remembering position
        if (!ConfigurationManager.UI.Window.rememberPosition()) {
            stage.centerOnScreen();
        }
        
        // Log window information
        logger.debug("Main window displayed - Size: {}x{}, Position: ({}, {})",
                    stage.getWidth(), stage.getHeight(),
                    stage.getX(), stage.getY());
    }
    
    // Application lifecycle management
    
    private boolean handleCloseRequest() {
        try {
            // Check if there are unsaved changes
            if (hasUnsavedChanges()) {
                // Show confirmation dialog
                Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
                alert.setTitle("Confirm Exit");
                alert.setHeaderText("Unsaved Changes");
                alert.setContentText("You have unsaved changes. Do you want to exit without saving?");
                
                return alert.showAndWait()
                           .filter(response -> response == Alert.ButtonType.OK)
                           .isPresent();
            }
            
            return true;
            
        } catch (Exception e) {
            logger.error("Error handling close request", e);
            return true; // Allow exit on error
        }
    }
    
    private boolean hasUnsavedChanges() {
        // TODO: Implement unsaved changes detection
        return false;
    }
    
    private void saveApplicationState() {
        try {
            // Save window state
            if (primaryStage != null && ConfigurationManager.UI.Window.rememberSize()) {
                // TODO: Save window dimensions and position to user preferences
            }
            
            // Save recent connections
            // TODO: Implement connection state persistence
            
            // Save workspace state
            // TODO: Implement workspace state persistence
            
            logger.info("Application state saved");
            
        } catch (Exception e) {
            logger.error("Failed to save application state", e);
        }
    }
    
    private void cleanup() {
        try {
            // Close database connections
            // TODO: Implement connection cleanup
            
            // Clear caches
            if (config != null) {
                config.clearCache();
            }
            
            // Untrack scenes from theme manager
            if (themeManager != null && primaryStage != null && primaryStage.getScene() != null) {
                themeManager.untrackScene(primaryStage.getScene());
            }
            
            logger.info("Cleanup completed");
            
        } catch (Exception e) {
            logger.error("Error during cleanup", e);
        }
    }
    
    private void onApplicationExit() {
        logger.info("JVM shutdown hook executed");
    }
    
    // Error handling
    
    private void showErrorDialog(String title, String message) {
        Platform.runLater(() -> {
            Alert alert = new Alert(Alert.AlertType.ERROR);
            alert.setTitle(title);
            alert.setHeaderText("Application Error");
            alert.setContentText(message);
            
            // Apply theme to dialog
            if (themeManager != null) {
                themeManager.applyTheme(alert.getDialogPane().getScene());
            }
            
            alert.showAndWait();
        });
    }
    
    // Application entry point
    
    public static void main(String[] args) {
        try {
            // Set system properties for better rendering
            System.setProperty("prism.lcdtext", "false");
            System.setProperty("prism.text", "t2k");
            
            // Enable hardware acceleration if available
            System.setProperty("prism.forceGPU", "true");
            
            // Improve font rendering
            System.setProperty("awt.useSystemAAFontSettings", "lcd");
            System.setProperty("swing.aatext", "true");
            
            // Launch the JavaFX application
            launch(args);
            
        } catch (Exception e) {
            Logger logger = LoggerFactory.getLogger(App.class);
            logger.error("Fatal error during application startup", e);
            System.exit(1);
        }
    }
}
        Platform.runLater(() -> {
            Alert alert = new Alert(Alert.AlertType.ERROR);
            alert.setTitle(title);
            alert.setHeaderText(null);
            alert.setContentText(message);
            alert.showAndWait();
        });
    }

    public static void main(String[] args) {
        launch(args);
    }
}