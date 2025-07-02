package com.pyhmssql.client.utils;

import javafx.scene.control.Alert;
import javafx.scene.control.ButtonType;
import javafx.scene.control.TextArea;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.Priority;
import javafx.application.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

/**
 * Enhanced Global Exception Handler with detailed error reporting and
 * user-friendly dialogs
 */
public class EnhancedGlobalExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(EnhancedGlobalExceptionHandler.class);
    private static EnhancedGlobalExceptionHandler instance;
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private EnhancedGlobalExceptionHandler() {
    }

    public static EnhancedGlobalExceptionHandler getInstance() {
        if (instance == null) {
            instance = new EnhancedGlobalExceptionHandler();
        }
        return instance;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        logger.error("Uncaught exception in thread {}: {}", t.getName(), e.getMessage(), e);

        if (Platform.isFxApplicationThread()) {
            showErrorDialog(e, "Uncaught Exception in " + t.getName());
        } else {
            Platform.runLater(() -> showErrorDialog(e, "Uncaught Exception in " + t.getName()));
        }
    }

    /**
     * Handle exceptions with context information
     */
    public static void handleException(String context, Throwable throwable) {
        logger.error("Exception in {}: {}", context, throwable.getMessage(), throwable);

        Platform.runLater(() -> {
            showErrorDialog(throwable, "Error in " + context);
        });
    }

    /**
     * Handle exceptions with custom user message
     */
    public static void handleException(String context, Throwable throwable, String userMessage) {
        logger.error("Exception in {}: {}", context, throwable.getMessage(), throwable);

        Platform.runLater(() -> {
            showCustomErrorDialog(throwable, "Error in " + context, userMessage);
        });
    }

    /**
     * Show detailed error dialog with expandable stack trace
     */
    private static void showErrorDialog(Throwable throwable, String title) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("Application Error");
        alert.setHeaderText(title);
        alert.setContentText(createUserFriendlyMessage(throwable));

        // Create expandable Exception Details
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);
        String exceptionText = sw.toString();

        TextArea textArea = new TextArea(exceptionText);
        textArea.setEditable(false);
        textArea.setWrapText(true);
        textArea.setMaxWidth(Double.MAX_VALUE);
        textArea.setMaxHeight(Double.MAX_VALUE);

        GridPane.setVgrow(textArea, Priority.ALWAYS);
        GridPane.setHgrow(textArea, Priority.ALWAYS);

        GridPane expContent = new GridPane();
        expContent.setMaxWidth(Double.MAX_VALUE);
        expContent.add(textArea, 0, 0);

        alert.getDialogPane().setExpandableContent(expContent);
        alert.getDialogPane().setPrefSize(600, 400);

        // Add timestamp to the dialog
        alert.setContentText(alert.getContentText() + "\n\nTime: " +
                LocalDateTime.now().format(TIMESTAMP_FORMATTER));

        alert.showAndWait();
    }

    /**
     * Show custom error dialog with user-friendly message
     */
    private static void showCustomErrorDialog(Throwable throwable, String title, String userMessage) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("Application Error");
        alert.setHeaderText(title);
        alert.setContentText(userMessage);

        // Add technical details as expandable content
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);
        String exceptionText = "Technical Details:\n\n" + sw.toString();

        TextArea textArea = new TextArea(exceptionText);
        textArea.setEditable(false);
        textArea.setWrapText(true);
        textArea.setMaxWidth(Double.MAX_VALUE);
        textArea.setMaxHeight(Double.MAX_VALUE);

        GridPane.setVgrow(textArea, Priority.ALWAYS);
        GridPane.setHgrow(textArea, Priority.ALWAYS);

        GridPane expContent = new GridPane();
        expContent.setMaxWidth(Double.MAX_VALUE);
        expContent.add(textArea, 0, 0);

        alert.getDialogPane().setExpandableContent(expContent);
        alert.getDialogPane().setPrefSize(600, 400);

        alert.showAndWait();
    }

    /**
     * Create user-friendly error messages based on exception type
     */
    private static String createUserFriendlyMessage(Throwable throwable) {
        String className = throwable.getClass().getSimpleName();
        String message = throwable.getMessage();

        return switch (className) {
            case "IllegalAccessError" -> {
                if (message != null && message.contains("com.jfoenix")) {
                    yield "UI component compatibility issue detected. The application will use standard components instead.";
                }
                yield "An access permission error occurred: " + (message != null ? message : "Unknown access error");
            }
            case "ConnectionException", "ConnectException", "SocketException" ->
                "Unable to connect to the database server. Please check your connection settings and ensure the server is running.";
            case "SQLException" ->
                "A database error occurred: " + (message != null ? message : "Unknown database error");
            case "FileNotFoundException" ->
                "A required file could not be found: " + (message != null ? message : "Unknown file");
            case "IOException" ->
                "An input/output error occurred. Please check file permissions and disk space.";
            case "OutOfMemoryError" ->
                "The application has run out of memory. Please restart the application and consider increasing memory allocation.";
            case "SecurityException" ->
                "A security error occurred. Please check your permissions and security settings.";
            case "IllegalArgumentException" ->
                "Invalid input provided: " + (message != null ? message : "Please check your input values");
            case "RuntimeException" ->
                "An unexpected error occurred: " + (message != null ? message : "Please try again or contact support");
            default ->
                "An unexpected error occurred: " + (message != null ? message : className);
        };
    }

    /**
     * Show confirmation dialog for potentially dangerous operations
     */
    public static boolean showConfirmationDialog(String title, String message, String details) {
        Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
        alert.setTitle(title);
        alert.setHeaderText("Please confirm this action");
        alert.setContentText(message);

        if (details != null && !details.isEmpty()) {
            TextArea textArea = new TextArea(details);
            textArea.setEditable(false);
            textArea.setWrapText(true);
            textArea.setMaxWidth(Double.MAX_VALUE);
            textArea.setMaxHeight(Double.MAX_VALUE);

            GridPane.setVgrow(textArea, Priority.ALWAYS);
            GridPane.setHgrow(textArea, Priority.ALWAYS);

            GridPane expContent = new GridPane();
            expContent.setMaxWidth(Double.MAX_VALUE);
            expContent.add(textArea, 0, 0);

            alert.getDialogPane().setExpandableContent(expContent);
        }

        Optional<ButtonType> result = alert.showAndWait();
        return result.isPresent() && result.get() == ButtonType.OK;
    }

    /**
     * Show information dialog for system notifications
     */
    public static void showInfoDialog(String title, String message) {
        Platform.runLater(() -> {
            Alert alert = new Alert(Alert.AlertType.INFORMATION);
            alert.setTitle(title);
            alert.setHeaderText(null);
            alert.setContentText(message);
            alert.showAndWait();
        });
    }

    /**
     * Show warning dialog for important notifications
     */
    public static void showWarningDialog(String title, String message) {
        Platform.runLater(() -> {
            Alert alert = new Alert(Alert.AlertType.WARNING);
            alert.setTitle(title);
            alert.setHeaderText("Warning");
            alert.setContentText(message);
            alert.showAndWait();
        });
    }

    /**
     * Log exception without showing dialog (for background operations)
     */
    public static void logException(String context, Throwable throwable) {
        logger.error("Exception in {}: {}", context, throwable.getMessage(), throwable);
    }

    /**
     * Handle network-related exceptions specifically
     */
    public static void handleNetworkException(Throwable throwable) {
        String userMessage = "Network connectivity issue detected. Please check your internet connection and server availability.";
        handleException("Network Operation", throwable, userMessage);
    }

    /**
     * Handle database-related exceptions specifically
     */
    public static void handleDatabaseException(Throwable throwable) {
        String userMessage = "Database operation failed. Please check your connection and try again.";
        handleException("Database Operation", throwable, userMessage);
    }

    /**
     * Handle file operation exceptions specifically
     */
    public static void handleFileException(Throwable throwable, String fileName) {
        String userMessage = "File operation failed for: " + fileName
                + ". Please check file permissions and availability.";
        handleException("File Operation", throwable, userMessage);
    }
}
