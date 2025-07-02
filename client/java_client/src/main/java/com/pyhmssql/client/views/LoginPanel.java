package com.pyhmssql.client.views;

import com.pyhmssql.client.main.ConnectionManager;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.*;
import javafx.scene.layout.*;

/**
 * Login panel for database connection
 */
public class LoginPanel extends VBox {
    private final ConnectionManager connectionManager;
    private TextField serverField;
    private TextField portField;
    private TextField usernameField;
    private PasswordField passwordField;
    private Button connectButton;
    private Label statusLabel;

    public LoginPanel(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        setupUI();
    }

    private void setupUI() {
        setAlignment(Pos.CENTER);
        setPadding(new Insets(50));
        setSpacing(20);

        // Title
        Label titleLabel = new Label("Connect to Database");
        titleLabel.setStyle("-fx-font-size: 24px; -fx-font-weight: bold;");

        // Form
        GridPane form = new GridPane();
        form.setHgap(10);
        form.setVgap(15);
        form.setAlignment(Pos.CENTER);

        // Server
        form.add(new Label("Server:"), 0, 0);
        serverField = new TextField("localhost");
        serverField.setPrefWidth(200);
        form.add(serverField, 1, 0);

        // Port
        form.add(new Label("Port:"), 0, 1);
        portField = new TextField("9999");
        portField.setPrefWidth(200);
        form.add(portField, 1, 1);

        // Username
        form.add(new Label("Username:"), 0, 2);
        usernameField = new TextField();
        usernameField.setPrefWidth(200);
        form.add(usernameField, 1, 2);

        // Password
        form.add(new Label("Password:"), 0, 3);
        passwordField = new PasswordField();
        passwordField.setPrefWidth(200);
        form.add(passwordField, 1, 3);

        // Connect button
        connectButton = new Button("Connect");
        connectButton.setPrefWidth(100);
        connectButton.setDefaultButton(true);
        connectButton.setOnAction(e -> connect());

        // Status label
        statusLabel = new Label("");
        statusLabel.setStyle("-fx-text-fill: red;");

        getChildren().addAll(titleLabel, form, connectButton, statusLabel);
    }

    private void connect() {
        String server = serverField.getText().trim();
        String portStr = portField.getText().trim();
        String username = usernameField.getText().trim();
        String password = passwordField.getText();

        if (server.isEmpty() || username.isEmpty()) {
            statusLabel.setText("Please fill in required fields");
            return;
        }

        try {
            int port = Integer.parseInt(portStr);
            connectButton.setDisable(true);
            statusLabel.setText("Connecting...");

            connectionManager.setServerDetails(server, port);
            connectionManager.connect(username, password).thenAccept(result -> {
                Platform.runLater(() -> {
                    connectButton.setDisable(false);
                    if (result.containsKey("error")) {
                        statusLabel.setText("Error: " + result.get("error"));
                    } else {
                        statusLabel.setText("Connected successfully!");
                        statusLabel.setStyle("-fx-text-fill: green;");
                    }
                });
            });
        } catch (NumberFormatException e) {
            statusLabel.setText("Invalid port number");
        }
    }
}