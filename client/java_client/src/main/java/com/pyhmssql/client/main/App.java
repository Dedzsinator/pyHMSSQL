package main;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.stage.Stage;

public class App extends Application {
    
    @Override
    public void start(Stage primaryStage) {
        MainWindow mainWindow = new MainWindow();
        Scene scene = new Scene(mainWindow, 1200, 800);
        
        // Add CSS styling
        scene.getStylesheets().add(getClass().getResource("/css/style.css").toExternalForm());
        
        primaryStage.setTitle("pyHMSSQL Management Studio");
        primaryStage.setScene(scene);
        primaryStage.show();
    }
    
    public static void main(String[] args) {
        launch(args);
    }
}