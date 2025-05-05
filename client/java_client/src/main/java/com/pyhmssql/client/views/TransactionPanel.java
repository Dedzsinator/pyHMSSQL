package com.pyhmssql.client.views;

import com.pyhmssql.client.main.ConnectionManager;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.geometry.Insets;

public class TransactionPanel extends BorderPane {
    private final ConnectionManager connectionManager;
    private Button beginButton;
    private Button commitButton;
    private Button rollbackButton;
    private Label statusLabel;
    private boolean transactionActive = false;

    public TransactionPanel(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        setupUI();
    }

    private void setupUI() {
        setPadding(new Insets(10));

        // Create buttons
        beginButton = new Button("Begin Transaction");
        commitButton = new Button("Commit");
        rollbackButton = new Button("Rollback");
        statusLabel = new Label("No active transaction");

        // Disable commit and rollback initially
        commitButton.setDisable(true);
        rollbackButton.setDisable(true);

        // Add event handlers
        beginButton.setOnAction(e -> beginTransaction());
        commitButton.setOnAction(e -> commitTransaction());
        rollbackButton.setOnAction(e -> rollbackTransaction());

        // Create layout
        HBox buttonBox = new HBox(10);
        buttonBox.getChildren().addAll(beginButton, commitButton, rollbackButton);

        VBox mainLayout = new VBox(10);
        mainLayout.getChildren().addAll(buttonBox, statusLabel);

        setCenter(mainLayout);
    }

    private void beginTransaction() {
        connectionManager.startTransaction()
                .thenAccept(result -> {
                    javafx.application.Platform.runLater(() -> {
                        if (result.containsKey("response")) {
                            transactionActive = true;
                            updateUI();
                            statusLabel.setText("Transaction started");
                        } else if (result.containsKey("error")) {
                            statusLabel.setText("Error: " + result.get("error"));
                        }
                    });
                });
    }

    private void commitTransaction() {
        connectionManager.commitTransaction()
                .thenAccept(result -> {
                    javafx.application.Platform.runLater(() -> {
                        if (result.containsKey("response")) {
                            transactionActive = false;
                            updateUI();
                            statusLabel.setText("Transaction committed");
                        } else if (result.containsKey("error")) {
                            statusLabel.setText("Error: " + result.get("error"));
                        }
                    });
                });
    }

    private void rollbackTransaction() {
        connectionManager.rollbackTransaction()
                .thenAccept(result -> {
                    javafx.application.Platform.runLater(() -> {
                        if (result.containsKey("response")) {
                            transactionActive = false;
                            updateUI();
                            statusLabel.setText("Transaction rolled back");
                        } else if (result.containsKey("error")) {
                            statusLabel.setText("Error: " + result.get("error"));
                        }
                    });
                });
    }

    private void updateUI() {
        beginButton.setDisable(transactionActive);
        commitButton.setDisable(!transactionActive);
        rollbackButton.setDisable(!transactionActive);
    }
}