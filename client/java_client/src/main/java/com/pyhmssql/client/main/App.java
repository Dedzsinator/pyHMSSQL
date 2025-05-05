package com.pyhmssql.client.main;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.stage.Stage;

public class App extends Application {

    @Override
    public void start(Stage stage) {
        try {
            // Set exception handler for JavaFX thread
            Thread.currentThread().setUncaughtExceptionHandler((thread, throwable) -> {
                System.err.println("Uncaught exception in JavaFX thread:");
                throwable.printStackTrace();
                showErrorDialog("Application Error", throwable.getMessage());
            });

            MainWindow mainWindow = new MainWindow();
            Scene scene = new Scene(mainWindow, 1200, 800);
            stage.setScene(scene);
            stage.setTitle("pyHMSSQL Client");
            stage.show();
        } catch (Exception e) {
            System.err.println("Error starting application:");
            e.printStackTrace();
            showErrorDialog("Startup Error", "Failed to start application: " + e.getMessage());
        }
    }

    private void showErrorDialog(String title, String message) {
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