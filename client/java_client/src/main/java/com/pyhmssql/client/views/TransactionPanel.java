package com.pyhmssql.client.views;

import com.pyhmssql.client.main.ConnectionManager;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.geometry.Insets;
import javafx.application.Platform;
import java.util.Map;

public class TransactionPanel extends BorderPane {

    private ConnectionManager connectionManager;
    private boolean transactionActive = false;
    private Button beginButton;
    private Button commitButton;
    private Button rollbackButton;
    private Label statusLabel;

    public TransactionPanel(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        setupUI();
    }

    private void setupUI() {
        setPadding(new Insets(10));

        // Create the UI components
        VBox content = new VBox(10);

        statusLabel = new Label("No active transaction");

        beginButton = new Button("Begin Transaction");
        beginButton.setOnAction(e -> beginTransaction());

        commitButton = new Button("Commit Transaction");
        commitButton.setOnAction(e -> commitTransaction());
        commitButton.setDisable(true);

        rollbackButton = new Button("Rollback Transaction");
        rollbackButton.setOnAction(e -> rollbackTransaction());
        rollbackButton.setDisable(true);

        content.getChildren().addAll(statusLabel, beginButton, commitButton, rollbackButton);
        setCenter(content);
    }

    private void beginTransaction() {
        connectionManager.executeQuery("BEGIN TRANSACTION")
                .thenAccept(result -> {
                    Platform.runLater(() -> {
                        if (result.containsKey("response") || result.containsKey("status")) {
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
        connectionManager.executeQuery("COMMIT TRANSACTION")
                .thenAccept(result -> {
                    Platform.runLater(() -> {
                        if (result.containsKey("response") || result.containsKey("status")) {
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
                    Platform.runLater(() -> {
                        if (result.containsKey("response") || result.containsKey("status")) {
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