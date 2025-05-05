package com.pyhmssql.client.views;

import com.pyhmssql.client.main.ConnectionManager;
import javafx.geometry.Insets;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.util.StringConverter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Dialog for editing user preferences
 */
public class UserPreferencesDialog extends Dialog<ButtonType> {
    private final ConnectionManager connectionManager;
    private Map<String, Control> controlMap = new HashMap<>();
    private Map<String, Object> preferences = new HashMap<>();

    // Define preference keys
    private static final String PREF_MAX_ROWS = "max_rows_display";
    private static final String PREF_QUERY_TIMEOUT = "query_timeout";
    private static final String PREF_EDITOR_FONT_SIZE = "editor_font_size";
    private static final String PREF_SYNTAX_HIGHLIGHTING = "syntax_highlighting";
    private static final String PREF_AUTO_COMPLETE = "auto_complete";
    private static final String PREF_RESULT_FORMAT = "result_format";

    public UserPreferencesDialog(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;

        setTitle("User Preferences");
        setHeaderText("Configure your preferences");

        initControls();
        loadPreferences();

        getDialogPane().getButtonTypes().addAll(ButtonType.APPLY, ButtonType.CANCEL);

        setResultConverter(buttonType -> {
            if (buttonType == ButtonType.APPLY) {
                savePreferences();
            }
            return buttonType;
        });
    }

    private void initControls() {
        // Create tabs for different preference categories
        TabPane tabPane = new TabPane();

        // General preferences tab
        Tab generalTab = new Tab("General", createGeneralPreferencesPane());
        generalTab.setClosable(false);

        // Editor preferences tab
        Tab editorTab = new Tab("SQL Editor", createEditorPreferencesPane());
        editorTab.setClosable(false);

        // Results preferences tab
        Tab resultsTab = new Tab("Results", createResultsPreferencesPane());
        resultsTab.setClosable(false);

        tabPane.getTabs().addAll(generalTab, editorTab, resultsTab);

        getDialogPane().setContent(tabPane);
        getDialogPane().setPrefSize(450, 350);
    }

    private GridPane createGeneralPreferencesPane() {
        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(20));

        // Query Timeout
        Label timeoutLabel = new Label("Query Timeout (seconds):");
        Spinner<Integer> timeoutSpinner = new Spinner<>(5, 300, 60, 5);
        timeoutSpinner.setEditable(true);
        timeoutSpinner.setPrefWidth(100);
        controlMap.put(PREF_QUERY_TIMEOUT, timeoutSpinner);

        // Default file location
        Label formatLabel = new Label("Default Result Format:");
        ComboBox<String> formatCombo = new ComboBox<>();
        formatCombo.getItems().addAll("Table", "JSON", "CSV");
        formatCombo.setValue("Table");
        controlMap.put(PREF_RESULT_FORMAT, formatCombo);

        grid.add(timeoutLabel, 0, 0);
        grid.add(timeoutSpinner, 1, 0);
        grid.add(formatLabel, 0, 1);
        grid.add(formatCombo, 1, 1);

        return grid;
    }

    private GridPane createEditorPreferencesPane() {
        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(20));

        // Font Size
        Label fontSizeLabel = new Label("Editor Font Size:");
        Spinner<Integer> fontSizeSpinner = new Spinner<>(8, 24, 12, 1);
        fontSizeSpinner.setEditable(true);
        fontSizeSpinner.setPrefWidth(100);
        controlMap.put(PREF_EDITOR_FONT_SIZE, fontSizeSpinner);

        // Syntax highlighting
        CheckBox syntaxHighlightingCheck = new CheckBox("Enable Syntax Highlighting");
        syntaxHighlightingCheck.setSelected(true);
        controlMap.put(PREF_SYNTAX_HIGHLIGHTING, syntaxHighlightingCheck);

        // Auto-complete
        CheckBox autoCompleteCheck = new CheckBox("Enable Auto-Complete");
        autoCompleteCheck.setSelected(true);
        controlMap.put(PREF_AUTO_COMPLETE, autoCompleteCheck);

        // Layout
        grid.add(fontSizeLabel, 0, 0);
        grid.add(fontSizeSpinner, 1, 0);
        grid.add(syntaxHighlightingCheck, 0, 1, 2, 1);
        grid.add(autoCompleteCheck, 0, 2, 2, 1);

        return grid;
    }

    private GridPane createResultsPreferencesPane() {
        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(20));

        // Max rows to display
        Label maxRowsLabel = new Label("Maximum Rows to Display:");
        Spinner<Integer> maxRowsSpinner = new Spinner<>(100, 10000, 1000, 100);
        maxRowsSpinner.setEditable(true);
        maxRowsSpinner.setPrefWidth(100);
        controlMap.put(PREF_MAX_ROWS, maxRowsSpinner);

        grid.add(maxRowsLabel, 0, 0);
        grid.add(maxRowsSpinner, 1, 0);

        return grid;
    }

    private void loadPreferences() {
        // Show loading indicator
        ProgressIndicator progress = new ProgressIndicator();
        progress.setMaxSize(50, 50);
        VBox loading = new VBox(progress, new Label("Loading preferences..."));
        loading.setAlignment(javafx.geometry.Pos.CENTER);
        loading.setSpacing(10);
        getDialogPane().setContent(loading);

        // Set default values first - so we have something if the server call fails
        setDefaultPreferences();

        // Load preferences from server
        connectionManager.getPreferences()
                .thenAccept(result -> {
                    if (result.containsKey("preferences")) {
                        try {
                            // We got the preferences
                            @SuppressWarnings("unchecked")
                            Map<String, Object> prefs = (Map<String, Object>) result.get("preferences");
                            // Only update if we got valid preferences
                            if (prefs != null && !prefs.isEmpty()) {
                                preferences = prefs;
                            }
                        } catch (Exception e) {
                            // If any error occurs, we'll use the defaults
                            e.printStackTrace();
                        }
                    }

                    javafx.application.Platform.runLater(() -> {
                        updateControlsWithPreferences();

                        // Re-add the tabs
                        TabPane tabPane = new TabPane();
                        Tab generalTab = new Tab("General", createGeneralPreferencesPane());
                        generalTab.setClosable(false);
                        Tab editorTab = new Tab("SQL Editor", createEditorPreferencesPane());
                        editorTab.setClosable(false);
                        Tab resultsTab = new Tab("Results", createResultsPreferencesPane());
                        resultsTab.setClosable(false);
                        tabPane.getTabs().addAll(generalTab, editorTab, resultsTab);
                        getDialogPane().setContent(tabPane);
                    });
                })
                .exceptionally(ex -> {
                    // Just use the default preferences we set earlier
                    ex.printStackTrace();

                    javafx.application.Platform.runLater(() -> {
                        updateControlsWithPreferences();

                        TabPane tabPane = new TabPane();
                        Tab generalTab = new Tab("General", createGeneralPreferencesPane());
                        generalTab.setClosable(false);
                        Tab editorTab = new Tab("SQL Editor", createEditorPreferencesPane());
                        editorTab.setClosable(false);
                        Tab resultsTab = new Tab("Results", createResultsPreferencesPane());
                        resultsTab.setClosable(false);
                        tabPane.getTabs().addAll(generalTab, editorTab, resultsTab);
                        getDialogPane().setContent(tabPane);
                    });
                    return null;
                });
    }

    private void updateControlsWithPreferences() {
        // Update UI controls with preference values
        for (String key : controlMap.keySet()) {
            Control control = controlMap.get(key);
            Object value = preferences.get(key);

            if (value != null) {
                if (control instanceof Spinner) {
                    @SuppressWarnings("unchecked")
                    Spinner<Integer> spinner = (Spinner<Integer>) control;
                    if (value instanceof Number) {
                        spinner.getValueFactory().setValue(((Number) value).intValue());
                    } else if (value instanceof String) {
                        try {
                            spinner.getValueFactory().setValue(Integer.parseInt((String) value));
                        } catch (NumberFormatException e) {
                            // Ignore parsing error, keep default
                        }
                    }
                } else if (control instanceof CheckBox) {
                    CheckBox checkBox = (CheckBox) control;
                    if (value instanceof Boolean) {
                        checkBox.setSelected((Boolean) value);
                    } else if (value instanceof String) {
                        checkBox.setSelected(Boolean.parseBoolean((String) value));
                    }
                } else if (control instanceof ComboBox) {
                    @SuppressWarnings("unchecked")
                    ComboBox<String> comboBox = (ComboBox<String>) control;
                    comboBox.setValue(value.toString());
                }
            }
        }
    }

    private void setDefaultPreferences() {
        // Set default values when preferences don't exist
        preferences.put(PREF_MAX_ROWS, 1000);
        preferences.put(PREF_QUERY_TIMEOUT, 60);
        preferences.put(PREF_EDITOR_FONT_SIZE, 12);
        preferences.put(PREF_SYNTAX_HIGHLIGHTING, true);
        preferences.put(PREF_AUTO_COMPLETE, true);
        preferences.put(PREF_RESULT_FORMAT, "Table");
    }

    private void savePreferences() {
        // Get values from UI controls
        Map<String, Object> updatedPrefs = new HashMap<>();

        for (String key : controlMap.keySet()) {
            Control control = controlMap.get(key);

            if (control instanceof Spinner) {
                @SuppressWarnings("unchecked")
                Spinner<Integer> spinner = (Spinner<Integer>) control;
                updatedPrefs.put(key, spinner.getValue());
            } else if (control instanceof CheckBox) {
                CheckBox checkBox = (CheckBox) control;
                updatedPrefs.put(key, checkBox.isSelected());
            } else if (control instanceof ComboBox) {
                @SuppressWarnings("unchecked")
                ComboBox<String> comboBox = (ComboBox<String>) control;
                updatedPrefs.put(key, comboBox.getValue());
            }
        }

        // Save to server
        connectionManager.updatePreferences(updatedPrefs)
                .thenAccept(result -> {
                    if (result.containsKey("error")) {
                        javafx.application.Platform
                                .runLater(() -> showError("Failed to save preferences: " + result.get("error")));
                    } else {
                        // Update successful
                        javafx.application.Platform.runLater(() -> showInfo("Preferences saved successfully!"));
                    }
                })
                .exceptionally(ex -> {
                    javafx.application.Platform
                            .runLater(() -> showError("Failed to save preferences: " + ex.getMessage()));
                    return null;
                });
    }

    private void showError(String message) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("Error");
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.showAndWait();
    }

    private void showInfo(String message) {
        Alert alert = new Alert(Alert.AlertType.INFORMATION);
        alert.setTitle("Information");
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.showAndWait();
    }
}