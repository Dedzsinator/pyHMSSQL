package com.pyhmssql.client.views;

import com.pyhmssql.client.main.ConnectionManager;
import javafx.geometry.Insets;
import javafx.scene.control.*;
import javafx.scene.layout.*;

public class LoginPanel extends GridPane {
    private TextField usernameField;
    private PasswordField passwordField;
    private ConnectionManager connectionManager;
    private Runnable onLoginSuccess;
    private TextField hostField;
    private TextField portField;
    private Label statusLabel;

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

        // Add components to the grid
        add(usernameLabel, 0, 0);
        add(usernameField, 1, 0);
        add(passwordLabel, 0, 1);
        add(passwordField, 1, 1);

        // Add a status label
        statusLabel = new Label();
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
        hostField = new TextField("localhost");

        // Port
        Label portLabel = new Label("Port:");
        portField = new TextField("9999");

        grid.add(serverLabel, 0, 0);
        grid.add(hostField, 1, 0);
        grid.add(portLabel, 0, 1);
        grid.add(portField, 1, 1);

        TitledPane pane = new TitledPane("Server Settings", grid);
        pane.setExpanded(false);
        return pane;
    }

    // Changed from private to public so it can be called from MainWindow
    public void login() {
        String username = usernameField.getText();
        String password = passwordField.getText();

        if (username.isEmpty() || password.isEmpty()) {
            showError("Username and password cannot be empty");
            return;
        }

        // Get server details from form fields
        try {
            String host = hostField.getText();
            int port = Integer.parseInt(portField.getText());

            // Update connection manager with the server details from form
            connectionManager.setServerDetails(host, port);
        } catch (NumberFormatException e) {
            showError("Invalid port number");
            return;
        }

        // Update status visual feedback
        statusLabel.setText("Connecting...");
        statusLabel.setVisible(true);

        connectionManager.connect(username, password)
                .thenAccept(result -> {
                    if (result.containsKey("error")) {
                        showError((String) result.get("error"));
                        statusLabel.setVisible(false);
                    } else if (result.containsKey("session_id")) {
                        // Login successful
                        if (onLoginSuccess != null) {
                            onLoginSuccess.run();
                        }
                        // Close the dialog
                        getScene().getWindow().hide();
                    } else {
                        showError("Unknown error occurred");
                        statusLabel.setVisible(false);
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

    public void setServerInfo(String host, int port) {
        if (hostField != null) {
            hostField.setText(host);
        }
        if (portField != null) {
            portField.setText(String.valueOf(port));
        }
    }
}