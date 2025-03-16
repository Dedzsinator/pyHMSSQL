package com.pyhmssql.client.views;

import com.pyhmssql.client.main.ConnectionManager;
import javafx.geometry.Insets;
import javafx.scene.control.*;
import javafx.scene.layout.*;

public class LoginPanel extends GridPane {
    private TextField usernameField;
    private PasswordField passwordField;
    private Button loginButton;
    private ConnectionManager connectionManager;
    private Runnable onLoginSuccess;
    
    public LoginPanel(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        
        // Set up the UI
        setPadding(new Insets(20));
        setHgap(10);
        setVgap(10);
        
        // Username field
        Label usernameLabel = new Label("Username:");
        usernameField = new TextField();
        usernameField.setPromptText("Enter username");
        
        // Password field
        Label passwordLabel = new Label("Password:");
        passwordField = new PasswordField();
        passwordField.setPromptText("Enter password");
        
        // Login button
        loginButton = new Button("Login");
        loginButton.setDefaultButton(true);
        loginButton.setOnAction(e -> login());
        
        // Add components to the grid
        add(usernameLabel, 0, 0);
        add(usernameField, 1, 0);
        add(passwordLabel, 0, 1);
        add(passwordField, 1, 1);
        
        // Add a status label
        Label statusLabel = new Label();
        statusLabel.setVisible(false);
        add(statusLabel, 0, 3, 2, 1);
        
        // Server settings section
        TitledPane serverSettingsPane = createServerSettingsPane();
        add(serverSettingsPane, 0, 4, 2, 1);
    }
    
    private TitledPane createServerSettingsPane() {
        GridPane grid = new GridPane();
        grid.setPadding(new Insets(10));
        grid.setHgap(10);
        grid.setVgap(5);
        
        // Server address
        Label serverLabel = new Label("Server:");
        TextField serverField = new TextField("localhost");
        
        // Port
        Label portLabel = new Label("Port:");
        TextField portField = new TextField("9999");
        
        grid.add(serverLabel, 0, 0);
        grid.add(serverField, 1, 0);
        grid.add(portLabel, 0, 1);
        grid.add(portField, 1, 1);
        
        TitledPane pane = new TitledPane("Server Settings", grid);
        pane.setExpanded(false);
        return pane;
    }
    
    private void login() {
        String username = usernameField.getText();
        String password = passwordField.getText();
        
        if (username.isEmpty() || password.isEmpty()) {
            showError("Username and password cannot be empty");
            return;
        }
        
        loginButton.setDisable(true);
        loginButton.setText("Connecting...");
        
        connectionManager.connect(username, password)
            .thenAccept(result -> {
                if (result.containsKey("error")) {
                    showError((String) result.get("error"));
                    loginButton.setDisable(false);
                    loginButton.setText("Login");
                } else if (result.containsKey("session_id")) {
                    // Login successful
                    if (onLoginSuccess != null) {
                        onLoginSuccess.run();
                    }
                    // Close the dialog
                    getScene().getWindow().hide();
                } else {
                    showError("Unknown error occurred");
                    loginButton.setDisable(false);
                    loginButton.setText("Login");
                }
            });
    }
    
    private void showError(String error) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("Login Error");
        alert.setHeaderText(null);
        alert.setContentText(error);
        alert.showAndWait();
    }
    
    public void setOnLoginSuccess(Runnable onLoginSuccess) {
        this.onLoginSuccess = onLoginSuccess;
    }
}