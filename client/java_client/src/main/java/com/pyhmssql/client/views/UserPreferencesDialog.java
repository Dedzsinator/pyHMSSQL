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
        setTitle("User Preferences");
        setHeaderText("Configure application settings");

        TabPane tabPane = new TabPane();

        // General tab
        Tab generalTab = new Tab("General");
        GridPane generalGrid = new GridPane();
        generalGrid.setHgap(10);
        generalGrid.setVgap(10);
        generalGrid.setPadding(new Insets(20));

        CheckBox autoConnectBox = new CheckBox("Auto-connect on startup");
        CheckBox rememberWindowBox = new CheckBox("Remember window size and position");
        CheckBox showLineNumbersBox = new CheckBox("Show line numbers in editor");

        generalGrid.add(autoConnectBox, 0, 0, 2, 1);
        generalGrid.add(rememberWindowBox, 0, 1, 2, 1);
        generalGrid.add(showLineNumbersBox, 0, 2, 2, 1);

        generalTab.setContent(generalGrid);

        // Editor tab
        Tab editorTab = new Tab("Editor");
        GridPane editorGrid = new GridPane();
        editorGrid.setHgap(10);
        editorGrid.setVgap(10);
        editorGrid.setPadding(new Insets(20));

        ComboBox<String> fontFamilyCombo = new ComboBox<>();
        fontFamilyCombo.getItems().addAll("Courier New", "Monaco", "Consolas", "DejaVu Sans Mono");
        fontFamilyCombo.setValue("Courier New");

        Spinner<Integer> fontSizeSpinner = new Spinner<>(8, 24, 12);
        Spinner<Integer> tabSizeSpinner = new Spinner<>(2, 8, 4);

        CheckBox wordWrapBox = new CheckBox("Enable word wrap");
        CheckBox syntaxHighlightBox = new CheckBox("Enable syntax highlighting");

        editorGrid.add(new Label("Font Family:"), 0, 0);
        editorGrid.add(fontFamilyCombo, 1, 0);
        editorGrid.add(new Label("Font Size:"), 0, 1);
        editorGrid.add(fontSizeSpinner, 1, 1);
        editorGrid.add(new Label("Tab Size:"), 0, 2);
        editorGrid.add(tabSizeSpinner, 1, 2);
        editorGrid.add(wordWrapBox, 0, 3, 2, 1);
        editorGrid.add(syntaxHighlightBox, 0, 4, 2, 1);

        editorTab.setContent(editorGrid);

        tabPane.getTabs().addAll(generalTab, editorTab);

        getDialogPane().setContent(tabPane);
        getDialogPane().getButtonTypes().addAll(ButtonType.OK, ButtonType.CANCEL);

        // Load current preferences
        loadCurrentPreferences();
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
}