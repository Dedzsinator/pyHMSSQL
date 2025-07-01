package com.pyhmssql.client.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javafx.application.Platform;
import javafx.scene.control.Alert;

/**
 * Global exception handler for the pyHMSSQL client application.
 * Provides centralized error handling and logging.
 */
public class GlobalExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);
    private static GlobalExceptionHandler instance;

    private GlobalExceptionHandler() {
    }

    public static synchronized GlobalExceptionHandler getInstance() {
        if (instance == null) {
            instance = new GlobalExceptionHandler();
        }
        return instance;
    }

    @Override
    public void uncaughtException(Thread thread, Throwable throwable) {
        logger.error("Uncaught exception in thread: {}", thread.getName(), throwable);

        // Handle JavaFX Application Thread exceptions specially
        if (Platform.isFxApplicationThread()) {
            handleFxException(throwable);
        } else {
            handleBackgroundException(thread, throwable);
        }
    }

    private void handleFxException(Throwable throwable) {
        try {
            // Show error dialog on JavaFX thread
            Platform.runLater(() -> {
                try {
                    Alert alert = new Alert(Alert.AlertType.ERROR);
                    alert.setTitle("Application Error");
                    alert.setHeaderText("An unexpected error occurred");
                    alert.setContentText(getErrorMessage(throwable));
                    alert.showAndWait();
                } catch (Exception e) {
                    logger.error("Failed to show error dialog", e);
                }
            });
        } catch (Exception e) {
            logger.error("Failed to handle JavaFX exception", e);
        }
    }

    private void handleBackgroundException(Thread thread, Throwable throwable) {
        // Log background thread exceptions
        logger.error("Background thread {} encountered an error", thread.getName(), throwable);

        // Optionally notify the user depending on the error severity
        if (isCriticalError(throwable)) {
            Platform.runLater(() -> {
                Alert alert = new Alert(Alert.AlertType.WARNING);
                alert.setTitle("Background Process Error");
                alert.setHeaderText("A background process encountered an error");
                alert.setContentText("Please check the logs for details.");
                alert.show();
            });
        }
    }

    private String getErrorMessage(Throwable throwable) {
        String message = throwable.getMessage();
        if (message == null || message.trim().isEmpty()) {
            message = throwable.getClass().getSimpleName();
        }

        // Truncate very long messages
        if (message.length() > 200) {
            message = message.substring(0, 197) + "...";
        }

        return message;
    }

    private boolean isCriticalError(Throwable throwable) {
        // Define what constitutes a critical error that should notify the user
        return throwable instanceof OutOfMemoryError ||
                throwable instanceof StackOverflowError ||
                throwable instanceof InternalError ||
                (throwable instanceof RuntimeException &&
                        throwable.getMessage() != null &&
                        throwable.getMessage().toLowerCase().contains("critical"));
    }

    /**
     * Static method to handle exceptions programmatically
     */
    public static void handleException(String context, Throwable throwable) {
        logger.error("Exception in context: {}", context, throwable);

        if (Platform.isFxApplicationThread()) {
            Alert alert = new Alert(Alert.AlertType.ERROR);
            alert.setTitle("Error");
            alert.setHeaderText(context);
            alert.setContentText(throwable.getMessage());
            alert.showAndWait();
        }
    }

    /**
     * Static method to handle exceptions with user-friendly messages
     */
    public static void handleException(String context, Throwable throwable, String userMessage) {
        logger.error("Exception in context: {}", context, throwable);

        if (Platform.isFxApplicationThread()) {
            Alert alert = new Alert(Alert.AlertType.ERROR);
            alert.setTitle("Error");
            alert.setHeaderText(context);
            alert.setContentText(userMessage);
            alert.showAndWait();
        }
    }
}
