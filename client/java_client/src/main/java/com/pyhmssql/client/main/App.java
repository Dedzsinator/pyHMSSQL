package com.pyhmssql.client.main;

import com.pyhmssql.client.utils.UIThemeManager;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.stage.Stage;

public class App extends Application {

    @Override
    public void start(Stage stage) {
        try {
            MainWindow mainWindow = new MainWindow();
            Scene scene = new Scene(mainWindow, 1400, 900);

            // Initialize theme management
            UIThemeManager.getInstance().setScene(scene);
            mainWindow.initializeTheme();

            stage.setTitle("pyHMSSQL Client");
            stage.setScene(scene);
            stage.setMaximized(true);
            stage.show();
        } catch (Exception e) {
            e.printStackTrace();
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