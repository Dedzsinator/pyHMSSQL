package com.pyhmssql.client.views;

import com.pyhmssql.client.main.ConnectionManager;
import com.pyhmssql.client.utils.SQLSyntaxHighlighter;
import javafx.application.Platform;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.geometry.Insets;
import javafx.stage.FileChooser;
import org.fxmisc.richtext.CodeArea;
import org.fxmisc.richtext.LineNumberFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.function.Consumer;

/**
 * SQL Query Editor with syntax highlighting and execution capabilities
 */
public class QueryEditor extends BorderPane {
    private final ConnectionManager connectionManager;
    private CodeArea codeArea;
    private ComboBox<String> databaseComboBox;
    private Button executeButton;
    private Button saveButton;
    private Button loadButton;
    private Button formatButton;
    private Label statusLabel;
    private Consumer<Map<String, Object>> onExecuteQuery;
    private String currentDatabase;

    public QueryEditor(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        setupUI();
        loadDatabases();
    }

    private void setupUI() {
        // Top toolbar
        HBox toolbar = new HBox(10);
        toolbar.setPadding(new Insets(5));
        toolbar.setStyle("-fx-background-color: #f0f0f0; -fx-border-color: #cccccc; -fx-border-width: 0 0 1 0;");

        // Database selection
        Label dbLabel = new Label("Database:");
        databaseComboBox = new ComboBox<>();
        databaseComboBox.setPrefWidth(150);
        databaseComboBox.setOnAction(e -> {
            currentDatabase = databaseComboBox.getValue();
            if (currentDatabase != null) {
                connectionManager.setCurrentDatabase(currentDatabase);
            }
        });

        // Buttons
        executeButton = new Button("Execute (F5)");
        executeButton.setStyle("-fx-background-color: #4CAF50; -fx-text-fill: white;");
        executeButton.setOnAction(e -> executeQuery());

        saveButton = new Button("Save");
        saveButton.setOnAction(e -> saveQuery());

        loadButton = new Button("Load");
        loadButton.setOnAction(e -> loadQuery());

        formatButton = new Button("Format");
        formatButton.setOnAction(e -> formatQuery());

        Button clearButton = new Button("Clear");
        clearButton.setOnAction(e -> codeArea.clear());

        // Status label
        statusLabel = new Label("Ready");
        statusLabel.setStyle("-fx-text-fill: #666666;");

        // Add spacing between groups
        Region spacer1 = new Region();
        HBox.setHgrow(spacer1, Priority.ALWAYS);

        toolbar.getChildren().addAll(
                dbLabel, databaseComboBox,
                new Separator(),
                executeButton, saveButton, loadButton, formatButton, clearButton,
                spacer1, statusLabel);

        setTop(toolbar);

        // Code area with syntax highlighting
        codeArea = new CodeArea();
        codeArea.setParagraphGraphicFactory(LineNumberFactory.get(codeArea));
        codeArea.setStyle("-fx-font-family: 'Courier New', monospace; -fx-font-size: 12px;");

        // Apply SQL syntax highlighting
        SQLSyntaxHighlighter.applySyntaxHighlighting(codeArea);

        // Load CSS for syntax highlighting
        try {
            String css = getClass().getResource("/css/sql-highlighting.css").toExternalForm();
            codeArea.getStylesheets().add(css);
        } catch (Exception e) {
            System.err.println("Could not load SQL highlighting CSS: " + e.getMessage());
        }

        // Set placeholder text
        codeArea.replaceText(0, 0,
                "-- Enter your SQL query here\n-- Press F5 or click Execute to run\n\nSELECT * FROM ");

        // Add keyboard shortcuts
        codeArea.setOnKeyPressed(event -> {
            switch (event.getCode()) {
                case F5:
                    event.consume();
                    executeQuery();
                    break;
                case S:
                    if (event.isControlDown()) {
                        event.consume();
                        saveQuery();
                    }
                    break;
                case O:
                    if (event.isControlDown()) {
                        event.consume();
                        loadQuery();
                    }
                    break;
                case L:
                    if (event.isControlDown()) {
                        event.consume();
                        formatQuery();
                    }
                    break;
                default:
                    // Handle all other key codes - no action needed
                    break;
            }
        });

        // Wrap in scroll pane
        ScrollPane scrollPane = new ScrollPane(codeArea);
        scrollPane.setFitToWidth(true);
        scrollPane.setFitToHeight(true);

        setCenter(scrollPane);
    }

    private void loadDatabases() {
        connectionManager.getDatabases().thenAccept(result -> {
            Platform.runLater(() -> {
                try {
                    databaseComboBox.getItems().clear();

                    if (result.containsKey("error")) {
                        statusLabel.setText("Error loading databases: " + result.get("error"));
                        return;
                    }

                    if (result.containsKey("databases") && result.get("databases") instanceof java.util.List) {
                        @SuppressWarnings("unchecked")
                        java.util.List<String> databases = (java.util.List<String>) result.get("databases");
                        databaseComboBox.getItems().addAll(databases);

                        if (!databases.isEmpty()) {
                            databaseComboBox.setValue(databases.get(0));
                            currentDatabase = databases.get(0);
                            connectionManager.setCurrentDatabase(currentDatabase);
                        }
                    }
                } catch (Exception e) {
                    statusLabel.setText("Error processing databases: " + e.getMessage());
                }
            });
        }).exceptionally(ex -> {
            Platform.runLater(() -> statusLabel.setText("Failed to load databases: " + ex.getMessage()));
            return null;
        });
    }

    public void executeQuery() {
        String query = codeArea.getText().trim();
        if (query.isEmpty()) {
            statusLabel.setText("No query to execute");
            return;
        }

        executeButton.setDisable(true);
        statusLabel.setText("Executing query...");

        connectionManager.executeQuery(query).thenAccept(result -> {
            Platform.runLater(() -> {
                executeButton.setDisable(false);

                if (result.containsKey("error")) {
                    statusLabel.setText("Query failed: " + result.get("error"));
                } else {
                    statusLabel.setText("Query executed successfully");
                }

                if (onExecuteQuery != null) {
                    onExecuteQuery.accept(result);
                }
            });
        }).exceptionally(ex -> {
            Platform.runLater(() -> {
                executeButton.setDisable(false);
                statusLabel.setText("Query error: " + ex.getMessage());
            });
            return null;
        });
    }

    private void saveQuery() {
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Save SQL Query");
        fileChooser.getExtensionFilters().add(
                new FileChooser.ExtensionFilter("SQL Files", "*.sql"));

        File file = fileChooser.showSaveDialog(getScene().getWindow());
        if (file != null) {
            try {
                Files.writeString(file.toPath(), codeArea.getText());
                statusLabel.setText("Query saved to " + file.getName());
            } catch (IOException e) {
                statusLabel.setText("Failed to save: " + e.getMessage());
            }
        }
    }

    private void loadQuery() {
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Load SQL Query");
        fileChooser.getExtensionFilters().add(
                new FileChooser.ExtensionFilter("SQL Files", "*.sql"));

        File file = fileChooser.showOpenDialog(getScene().getWindow());
        if (file != null) {
            try {
                String content = Files.readString(file.toPath());
                codeArea.replaceText(content);
                statusLabel.setText("Query loaded from " + file.getName());
            } catch (IOException e) {
                statusLabel.setText("Failed to load: " + e.getMessage());
            }
        }
    }

    private void formatQuery() {
        String query = codeArea.getText();
        if (!query.trim().isEmpty()) {
            try {
                // Basic SQL formatting - can be enhanced with a proper SQL formatter
                String formatted = formatSQL(query);
                codeArea.replaceText(formatted);
                statusLabel.setText("Query formatted");
            } catch (Exception e) {
                statusLabel.setText("Format error: " + e.getMessage());
            }
        }
    }

    private String formatSQL(String sql) {
        // Basic SQL formatting
        return sql.replaceAll("(?i)\\bSELECT\\b", "SELECT")
                .replaceAll("(?i)\\bFROM\\b", "\nFROM")
                .replaceAll("(?i)\\bWHERE\\b", "\nWHERE")
                .replaceAll("(?i)\\bORDER BY\\b", "\nORDER BY")
                .replaceAll("(?i)\\bGROUP BY\\b", "\nGROUP BY")
                .replaceAll("(?i)\\bHAVING\\b", "\nHAVING")
                .replaceAll("(?i)\\bJOIN\\b", "\nJOIN")
                .replaceAll("(?i)\\bLEFT JOIN\\b", "\nLEFT JOIN")
                .replaceAll("(?i)\\bRIGHT JOIN\\b", "\nRIGHT JOIN")
                .replaceAll("(?i)\\bINNER JOIN\\b", "\nINNER JOIN")
                .replaceAll("(?i)\\bFULL JOIN\\b", "\nFULL JOIN")
                .replaceAll("(?i)\\bUNION\\b", "\nUNION")
                .replaceAll("(?i)\\bINSERT INTO\\b", "INSERT INTO")
                .replaceAll("(?i)\\bUPDATE\\b", "UPDATE")
                .replaceAll("(?i)\\bDELETE FROM\\b", "DELETE FROM")
                .replaceAll("(?i)\\bCREATE\\b", "CREATE")
                .replaceAll("(?i)\\bDROP\\b", "DROP")
                .replaceAll("(?i)\\bALTER\\b", "ALTER")
                .replaceAll("\\s+", " ")
                .trim();
    }

    // Getters and setters
    public String getQuery() {
        return codeArea.getText();
    }

    public void setQuery(String query) {
        if (query != null) {
            codeArea.replaceText(query);
        }
    }

    // Additional methods needed by MainWindow
    public String getQueryText() {
        return codeArea.getText();
    }

    public void setQueryText(String query) {
        if (query != null) {
            codeArea.replaceText(query);
        }
    }

    public String getSelectedText() {
        return codeArea.getSelectedText();
    }

    public boolean hasUnsavedChanges() {
        // For now, assume always unsaved if there's text
        // In a real implementation, you'd track the saved state
        return !codeArea.getText().trim().isEmpty();
    }

    public void setOnExecuteQuery(Consumer<Map<String, Object>> handler) {
        this.onExecuteQuery = handler;
    }

    public void setDatabase(String database) {
        this.currentDatabase = database;
        if (database != null && databaseComboBox.getItems().contains(database)) {
            databaseComboBox.setValue(database);
            connectionManager.setCurrentDatabase(database);
        }
    }

    public void refreshDatabases() {
        loadDatabases();
    }

    public String getCurrentDatabase() {
        return currentDatabase;
    }
}