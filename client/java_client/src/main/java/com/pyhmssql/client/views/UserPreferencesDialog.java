package com.pyhmssql.client.views;

import com.pyhmssql.client.main.ConnectionManager;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.geometry.Insets;
import javafx.application.Platform;

import java.util.HashMap;
import java.util.Map;

/**
 * Dialog for editing user preferences
 */
public class UserPreferencesDialog extends Dialog<ButtonType> {
    private final ConnectionManager connectionManager;
    private final Map<String, Object> updatedPrefs = new HashMap<>();

    // UI Components
    private TextField resultLimitField;
    private CheckBox autoCommitCheckBox;
    private CheckBox debugModeCheckBox;
    private ComboBox<String> defaultDatabaseCombo;
    private Spinner<Integer> timeoutSpinner;
    private CheckBox autoCompleteCheckBox;

    public UserPreferencesDialog(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        initDialog();
        createContent();
        setupButtons();
        loadCurrentPreferences();
    }

    private void initDialog() {
        setTitle("User Preferences");
        setHeaderText("Configure your application preferences");
        setResizable(true);
        getDialogPane().setPrefSize(500, 400);
    }

    private void createContent() {
        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(20));

        int row = 0;

        // Result Limit
        grid.add(new Label("Query Result Limit:"), 0, row);
        resultLimitField = new TextField("1000");
        resultLimitField.setPromptText("Maximum rows to display");
        grid.add(resultLimitField, 1, row++);

        // Auto Commit
        autoCommitCheckBox = new CheckBox("Auto Commit Transactions");
        autoCommitCheckBox.setSelected(true);
        grid.add(autoCommitCheckBox, 0, row++, 2, 1);

        // Debug Mode
        debugModeCheckBox = new CheckBox("Enable Debug Mode");
        debugModeCheckBox.setSelected(false);
        grid.add(debugModeCheckBox, 0, row++, 2, 1);

        // Default Database
        grid.add(new Label("Default Database:"), 0, row);
        defaultDatabaseCombo = new ComboBox<>();
        defaultDatabaseCombo.setPromptText("Select default database");
        defaultDatabaseCombo.setPrefWidth(200);
        grid.add(defaultDatabaseCombo, 1, row++);

        // Query Timeout
        grid.add(new Label("Query Timeout (seconds):"), 0, row);
        timeoutSpinner = new Spinner<>(5, 300, 30, 5);
        timeoutSpinner.setEditable(true);
        timeoutSpinner.setPrefWidth(100);
        grid.add(timeoutSpinner, 1, row++);

        // Auto Complete
        autoCompleteCheckBox = new CheckBox("Enable SQL Auto-completion");
        autoCompleteCheckBox.setSelected(true);
        grid.add(autoCompleteCheckBox, 0, row++, 2, 1);

        // Load available databases for the combo box
        loadDatabases();

        getDialogPane().setContent(grid);
    }

    private void loadDatabases() {
        connectionManager.getDatabases().thenAccept(result -> {
            if (result.containsKey("databases")) {
                @SuppressWarnings("unchecked")
                java.util.List<String> databases = (java.util.List<String>) result.get("databases");
                Platform.runLater(() -> {
                    defaultDatabaseCombo.getItems().clear();
                    defaultDatabaseCombo.getItems().addAll(databases);
                });
            }
        }).exceptionally(ex -> {
            System.err.println("Error loading databases for preferences: " + ex.getMessage());
            return null;
        });
    }

    private void loadCurrentPreferences() {
        connectionManager.getPreferences().thenAccept(result -> {
            Platform.runLater(() -> {
                if (result.containsKey("preferences")) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> prefs = (Map<String, Object>) result.get("preferences");

                    // Load values from preferences
                    if (prefs.containsKey("result_limit")) {
                        resultLimitField.setText(prefs.get("result_limit").toString());
                    }
                    if (prefs.containsKey("auto_commit")) {
                        autoCommitCheckBox.setSelected(Boolean.parseBoolean(prefs.get("auto_commit").toString()));
                    }
                    if (prefs.containsKey("debug_mode")) {
                        debugModeCheckBox.setSelected(Boolean.parseBoolean(prefs.get("debug_mode").toString()));
                    }
                    if (prefs.containsKey("default_database")) {
                        String defaultDb = prefs.get("default_database").toString();
                        if (defaultDatabaseCombo.getItems().contains(defaultDb)) {
                            defaultDatabaseCombo.setValue(defaultDb);
                        }
                    }
                    if (prefs.containsKey("query_timeout")) {
                        try {
                            int timeout = Integer.parseInt(prefs.get("query_timeout").toString());
                            timeoutSpinner.getValueFactory().setValue(timeout);
                        } catch (NumberFormatException e) {
                            // Use default value
                        }
                    }
                    if (prefs.containsKey("auto_complete")) {
                        autoCompleteCheckBox.setSelected(Boolean.parseBoolean(prefs.get("auto_complete").toString()));
                    }
                }
            });
        }).exceptionally(ex -> {
            System.err.println("Error loading preferences: " + ex.getMessage());
            return null;
        });
    }

    private void setupButtons() {
        ButtonType saveButtonType = new ButtonType("Save", ButtonBar.ButtonData.OK_DONE);
        ButtonType cancelButtonType = new ButtonType("Cancel", ButtonBar.ButtonData.CANCEL_CLOSE);

        getDialogPane().getButtonTypes().addAll(saveButtonType, cancelButtonType);

        // Get the save button and set up the action
        Button saveButton = (Button) getDialogPane().lookupButton(saveButtonType);
        saveButton.setOnAction(e -> {
            collectPreferences();
            savePreferences();
        });

        // Set up result converter
        setResultConverter(buttonType -> {
            if (buttonType == saveButtonType) {
                collectPreferences();
                savePreferences();
            }
            return buttonType;
        });
    }

    private void collectPreferences() {
        updatedPrefs.clear();

        try {
            updatedPrefs.put("result_limit", Integer.parseInt(resultLimitField.getText()));
        } catch (NumberFormatException e) {
            updatedPrefs.put("result_limit", 1000);
        }

        updatedPrefs.put("auto_commit", autoCommitCheckBox.isSelected());
        updatedPrefs.put("debug_mode", debugModeCheckBox.isSelected());

        if (defaultDatabaseCombo.getValue() != null) {
            updatedPrefs.put("default_database", defaultDatabaseCombo.getValue());
        }

        updatedPrefs.put("query_timeout", timeoutSpinner.getValue());
        updatedPrefs.put("auto_complete", autoCompleteCheckBox.isSelected());
    }

    private void savePreferences() {
        connectionManager.updatePreferences(updatedPrefs)
                .thenAccept(result -> {
                    if (result.containsKey("error")) {
                        Platform.runLater(() -> showError("Failed to save preferences: " + result.get("error")));
                    } else {
                        // Update successful
                        Platform.runLater(() -> showInfo("Preferences saved successfully!"));
                    }
                })
                .exceptionally(ex -> {
                    Platform.runLater(() -> showError("Failed to save preferences: " + ex.getMessage()));
                    return null;
                });
    }

    private void showError(String message) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("Error");
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.initOwner(getOwner());
        alert.showAndWait();
    }

    private void showInfo(String message) {
        Alert alert = new Alert(Alert.AlertType.INFORMATION);
        alert.setTitle("Success");
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.initOwner(getOwner());
        alert.showAndWait();
    }
}