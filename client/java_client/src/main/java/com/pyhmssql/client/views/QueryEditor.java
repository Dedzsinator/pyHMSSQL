package com.pyhmssql.client.views;

import com.pyhmssql.client.main.ConnectionManager;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import org.fxmisc.richtext.CodeArea;
import org.fxmisc.richtext.LineNumberFactory;
import org.fxmisc.richtext.model.StyleSpans;
import org.fxmisc.richtext.model.StyleSpansBuilder;
import org.fxmisc.flowless.VirtualizedScrollPane;
import javafx.scene.input.KeyCode;
import javafx.scene.input.KeyEvent;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Map;
import java.util.function.Consumer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.List;
import java.util.ArrayList;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import javafx.stage.FileChooser;
import javafx.application.Platform;

public class QueryEditor extends BorderPane {
    private CodeArea codeArea;
    private ConnectionManager connectionManager;
    private Consumer<Map<String, Object>> onExecuteQuery;
    private ResultPane resultPane;
    private ExecutorService executor;
    private ToolBar toolBar;
    private String currentDatabase;
    private Label statusLabel; // Added status label
    private ComboBox<String> databaseComboBox;
    private ComboBox<String> historyComboBox;
    private ArrayList<String> queryHistory = new ArrayList<>();

    // SQL keywords for syntax highlighting
    private static final String[] KEYWORDS = new String[] {
            "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "TABLE",
            "DATABASE", "INDEX", "VIEW", "AND", "OR", "NOT", "NULL", "IS", "IN", "LIKE",
            "GROUP", "BY", "HAVING", "ORDER", "ASC", "DESC", "JOIN", "INNER", "LEFT", "RIGHT",
            "OUTER", "ON", "AS", "COUNT", "AVG", "SUM", "MIN", "MAX", "DISTINCT", "UNION",
            "ALL", "CASE", "WHEN", "THEN", "ELSE", "END", "BEGIN", "COMMIT", "ROLLBACK",
            "PROCEDURE", "FUNCTION", "TRIGGER", "REFERENCES", "FOREIGN", "PRIMARY", "KEY",
            "CONSTRAINT", "CHECK", "DEFAULT", "AUTO_INCREMENT", "VALUES", "INTO", "SET",
            "TRANSACTION", "TRUNCATE", "ALTER", "ADD", "COLUMN", "MODIFY", "RENAME", "TO"
    };

    private static final String KEYWORD_PATTERN = "\\b(" + String.join("|", KEYWORDS) + ")\\b";
    private static final String STRING_PATTERN = "'[^']*'";
    private static final String COMMENT_PATTERN = "--[^\n]*";
    private static final String MULTILINE_COMMENT_PATTERN = "/\\*[^*]*\\*+(?:[^/*][^*]*\\*+)*/";
    private static final String NUMBER_PATTERN = "\\b\\d+(\\.\\d+)?([eE][+-]?\\d+)?\\b";

    private static final Pattern PATTERN = Pattern.compile(
            "(?<KEYWORD>" + KEYWORD_PATTERN + ")"
                    + "|(?<STRING>" + STRING_PATTERN + ")"
                    + "|(?<COMMENT>" + COMMENT_PATTERN + ")"
                    + "|(?<MLCOMMENT>" + MULTILINE_COMMENT_PATTERN + ")"
                    + "|(?<NUMBER>" + NUMBER_PATTERN + ")",
            Pattern.CASE_INSENSITIVE);

    public QueryEditor(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        this.executor = Executors.newSingleThreadExecutor();

        // Set up the UI
        setPadding(new Insets(0));

        // Create status label first (to avoid null pointer)
        statusLabel = new Label("Ready");
        statusLabel.setPadding(new Insets(5, 10, 5, 10));

        // Create code area with syntax highlighting
        setupCodeArea();

        // Create toolbar
        setupToolbar();

        // Create results pane
        resultPane = new ResultPane();

        // Add status label at the bottom
        HBox statusBar = new HBox();
        statusBar.getChildren().add(statusLabel);
        setBottom(statusBar);
    }

    private void setupCodeArea() {
        codeArea = new CodeArea();

        // Set initial content before setting up line numbers and other features
        codeArea.appendText("-- Write your SQL query here\n");

        // Now set up line numbers factory
        codeArea.setParagraphGraphicFactory(LineNumberFactory.get(codeArea));

        // Setup syntax highlighting with proper error handling
        codeArea.multiPlainChanges()
                .successionEnds(Duration.ofMillis(500))
                .retainLatestUntilLater(executor)
                .supplyTask(() -> {
                    return new javafx.concurrent.Task<StyleSpans<Collection<String>>>() {
                        @Override
                        protected StyleSpans<Collection<String>> call() throws Exception {
                            String text = codeArea.getText();
                            if (text == null || text.isEmpty()) {
                                // Return empty style spans for empty text
                                return new StyleSpansBuilder<Collection<String>>()
                                        .add(Collections.emptyList(), 0)
                                        .create();
                            }
                            return computeHighlighting(text);
                        }
                    };
                })
                .awaitLatest(codeArea.multiPlainChanges())
                .filterMap(t -> {
                    if (t.isSuccess()) {
                        return Optional.of(t.get());
                    } else {
                        t.getFailure().printStackTrace();
                        return Optional.empty();
                    }
                })
                .subscribe(highlighting -> {
                    if (highlighting != null && codeArea.getLength() > 0) {
                        try {
                            codeArea.setStyleSpans(0, highlighting);
                        } catch (IndexOutOfBoundsException e) {
                            // Ignore index out of bounds - this can happen during rapid edits
                            System.err.println("Ignored style spans error: " + e.getMessage());
                        }
                    }
                });

        // Add keyboard shortcuts
        codeArea.addEventHandler(KeyEvent.KEY_PRESSED, event -> {
            if (event.isControlDown() && event.getCode() == KeyCode.ENTER) {
                executeQuery();
                event.consume();
            } else if (event.isControlDown() && event.getCode() == KeyCode.F) {
                formatQuery();
                event.consume();
            } else if (event.isControlDown() && event.getCode() == KeyCode.S) {
                saveQuery();
                event.consume();
            }
        });

        // Add syntax highlighting styles
        try {
            java.net.URL cssResource = getClass().getResource("/css/sql-highlighting.css");
            if (cssResource != null) {
                codeArea.getStylesheets().add(cssResource.toExternalForm());
            } else {
                // Apply default styles directly if the CSS file is not found
                addDefaultStyles();
            }
        } catch (Exception e) {
            System.err.println("Could not load SQL highlighting CSS: " + e.getMessage());
            addDefaultStyles();
        }
    }

    /**
     * Adds default syntax highlighting styles when the CSS file isn't found
     */
    private void addDefaultStyles() {
        // Create an inline style to use as fallback
        String defaultCss = ".keyword { -fx-fill: blue; -fx-font-weight: bold; } " +
                ".string { -fx-fill: green; } " +
                ".comment { -fx-fill: gray; -fx-font-style: italic; } " +
                ".number { -fx-fill: #a04040; } ";

        codeArea.setStyle(defaultCss);
    }

    private void setupToolbar() {
        toolBar = new ToolBar();

        // Execute button
        Button executeButton = new Button("Execute");
        executeButton.setOnAction(e -> executeQuery());
        executeButton.setTooltip(new Tooltip("Execute the current query (Ctrl+Enter)"));

        // Format button
        Button formatButton = new Button("Format");
        formatButton.setOnAction(e -> formatQuery());
        formatButton.setTooltip(new Tooltip("Format the SQL query (Ctrl+F)"));

        // Save button
        Button saveButton = new Button("Save");
        saveButton.setOnAction(e -> saveQuery());
        saveButton.setTooltip(new Tooltip("Save the query to a file (Ctrl+S)"));

        // Database selector
        databaseComboBox = new ComboBox<>();
        databaseComboBox.setPromptText("Select Database");
        databaseComboBox.setPrefWidth(150);
        databaseComboBox.setOnAction(e -> {
            currentDatabase = databaseComboBox.getValue();
            if (currentDatabase != null && !currentDatabase.isEmpty()) {
                connectionManager.setCurrentDatabase(currentDatabase);
                statusLabel.setText("Database changed to: " + currentDatabase);
            }
        });

        // Load databases immediately
        loadDatabases();

        // History dropdown
        historyComboBox = new ComboBox<>();
        historyComboBox.setPromptText("Query History");
        historyComboBox.setPrefWidth(200);
        historyComboBox.setOnAction(e -> {
            String selectedQuery = historyComboBox.getValue();
            if (selectedQuery != null && !selectedQuery.isEmpty()) {
                codeArea.replaceText(selectedQuery);
            }
        });

        // New query button
        Button newQueryButton = new Button("New Query");
        newQueryButton.setOnAction(e -> codeArea.replaceText("-- Write your SQL query here\n"));

        // Add components to toolbar
        toolBar.getItems().addAll(
                executeButton,
                new Separator(),
                formatButton,
                saveButton,
                new Separator(),
                new Label("Database:"),
                databaseComboBox,
                new Separator(),
                historyComboBox,
                new Separator(),
                newQueryButton);

        // Layout components
        setTop(toolBar);

        // Use the standalone QueryEditor (don't add VirtualizedScrollPane in the
        // constructor)
        setCenter(new VirtualizedScrollPane<>(codeArea));
    }

    private void loadDatabases() {
        // Set status message
        statusLabel.setText("Loading databases...");

        connectionManager.getDatabases()
                .thenAccept(result -> {
                    if (result.containsKey("databases")) {
                        @SuppressWarnings("unchecked")
                        List<String> databases = (List<String>) result.get("databases");
                        javafx.application.Platform.runLater(() -> {
                            databaseComboBox.getItems().clear();
                            databaseComboBox.getItems().addAll(databases);

                            // Select current database if set
                            if (currentDatabase != null && databases.contains(currentDatabase)) {
                                databaseComboBox.setValue(currentDatabase);
                            }

                            statusLabel.setText("Databases loaded successfully");
                        });
                    } else {
                        Platform.runLater(() -> {
                            statusLabel.setText("Failed to load databases");
                        });
                    }
                })
                .exceptionally(ex -> {
                    Platform.runLater(() -> {
                        statusLabel.setText("Error: " + ex.getMessage());
                    });
                    ex.printStackTrace();
                    return null;
                });
    }

    private StyleSpans<Collection<String>> computeHighlighting(String text) {
        Matcher matcher = PATTERN.matcher(text.toUpperCase());
        int lastTokenEnd = 0;
        StyleSpansBuilder<Collection<String>> spansBuilder = new StyleSpansBuilder<>();

        while (matcher.find()) {
            String styleClass = matcher.group("KEYWORD") != null ? "keyword"
                    : matcher.group("STRING") != null ? "string"
                            : matcher.group("COMMENT") != null ? "comment"
                                    : matcher.group("MLCOMMENT") != null ? "comment"
                                            : matcher.group("NUMBER") != null ? "number" : null; // never happens

            assert styleClass != null;

            spansBuilder.add(Collections.emptyList(), matcher.start() - lastTokenEnd);
            spansBuilder.add(Collections.singleton(styleClass), matcher.end() - matcher.start());
            lastTokenEnd = matcher.end();
        }

        spansBuilder.add(Collections.emptyList(), text.length() - lastTokenEnd);
        return spansBuilder.create();
    }

    public void executeQuery() {
        String query = codeArea.getText();
        if (query.trim().isEmpty()) {
            showAlert(Alert.AlertType.WARNING, "Warning",
                    "Query cannot be empty", "Please enter a SQL query to execute.");
            return;
        }

        // Update status
        statusLabel.setText("Executing query...");

        // Check if current database is selected
        if (currentDatabase == null || currentDatabase.isEmpty()) {
            if (databaseComboBox.getItems().size() > 0) {
                currentDatabase = databaseComboBox.getItems().get(0);
                databaseComboBox.setValue(currentDatabase);
                connectionManager.setCurrentDatabase(currentDatabase);
                statusLabel.setText("Selected default database: " + currentDatabase);
            } else {
                showAlert(Alert.AlertType.WARNING, "Warning",
                        "No Database Selected", "Please select a database before executing queries.");
                statusLabel.setText("Error: No database selected");
                return;
            }
        }

        // Show a loading indicator
        if (resultPane != null) {
            resultPane.showLoading();
        } else {
            // Create the result pane if it doesn't exist
            resultPane = new ResultPane();
            resultPane.showLoading();
        }

        // Add query to history
        addToQueryHistory(query);

        connectionManager.executeQuery(query)
                .thenAccept(result -> {
                    javafx.application.Platform.runLater(() -> {
                        if (resultPane != null) {
                            resultPane.displayResults(result);
                        }

                        // Update status with result message
                        if (result.containsKey("error")) {
                            statusLabel.setText("Error: " + result.get("error"));
                        } else if (result.containsKey("message")) {
                            statusLabel.setText(result.get("message").toString());
                        } else {
                            statusLabel.setText("Query executed successfully");
                        }

                        // If we have a callback, invoke it
                        if (onExecuteQuery != null) {
                            onExecuteQuery.accept(result);
                        }
                    });
                })
                .exceptionally(ex -> {
                    javafx.application.Platform.runLater(() -> {
                        if (resultPane != null) {
                            resultPane.showError("Error executing query: " + ex.getMessage());
                        }
                        statusLabel.setText("Error: " + ex.getMessage());
                    });
                    ex.printStackTrace();
                    return null;
                });
    }

    private void addToQueryHistory(String query) {
        // Limit history size and avoid duplicates
        if (!queryHistory.contains(query)) {
            queryHistory.add(0, query); // Add to beginning

            // Limit history size to 20 entries
            if (queryHistory.size() > 20) {
                queryHistory.remove(queryHistory.size() - 1);
            }

            // Update history dropdown
            Platform.runLater(() -> {
                historyComboBox.getItems().clear();
                for (String historyItem : queryHistory) {
                    // Truncate long queries for display
                    String displayText = historyItem.length() > 50
                            ? historyItem.substring(0, 47) + "..."
                            : historyItem;
                    historyComboBox.getItems().add(displayText);
                }
            });
        }
    }

    private void formatQuery() {
        // Get the current query
        String query = codeArea.getText();
        if (query.trim().isEmpty()) {
            statusLabel.setText("No query to format");
            return;
        }

        statusLabel.setText("Formatting query...");

        // Simple SQL formatting logic
        try {
            StringBuilder formatted = new StringBuilder();
            String[] lines = query.split("\n");

            // Process each line
            for (String line : lines) {
                // Skip empty lines and comments
                if (line.trim().isEmpty() || line.trim().startsWith("--")) {
                    formatted.append(line).append("\n");
                    continue;
                }

                // Process keywords
                String formattedLine = line;
                for (String keyword : KEYWORDS) {
                    String pattern = "(?i)\\b" + keyword + "\\b";
                    formattedLine = formattedLine.replaceAll(pattern, keyword.toUpperCase());
                }

                formatted.append(formattedLine).append("\n");
            }

            // Update the code area with formatted SQL
            final String formattedQuery = formatted.toString();
            Platform.runLater(() -> {
                codeArea.replaceText(formattedQuery);
                statusLabel.setText("Query formatted successfully");
            });
        } catch (Exception e) {
            statusLabel.setText("Error formatting query: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void saveQuery() {
        // Open a file save dialog
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Save SQL Query");
        fileChooser.getExtensionFilters().add(
                new FileChooser.ExtensionFilter("SQL Files", "*.sql"));

        // Get the window to show the dialog
        if (getScene() == null) {
            statusLabel.setText("Error: Cannot save - window not available");
            return;
        }

        File file = fileChooser.showSaveDialog(getScene().getWindow());
        if (file != null) {
            try {
                Files.write(file.toPath(), codeArea.getText().getBytes());
                statusLabel.setText("Query saved to " + file.getPath());
            } catch (IOException e) {
                statusLabel.setText("Error saving file: " + e.getMessage());
                showAlert(Alert.AlertType.ERROR, "Error",
                        "Save failed", "Failed to save query: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private void showAlert(Alert.AlertType type, String title, String header, String content) {
        Alert alert = new Alert(type);
        alert.setTitle(title);
        alert.setHeaderText(header);
        alert.setContentText(content);
        alert.showAndWait();
    }

    public void setQuery(String query) {
        if (query == null) {
            return;
        }

        if (!Platform.isFxApplicationThread()) {
            Platform.runLater(() -> setQuery(query));
            return;
        }

        try {
            // Replace text safely within bounds
            codeArea.clear();
            codeArea.appendText(query);
        } catch (Exception e) {
            System.err.println("Error setting query text: " + e.getMessage());
            e.printStackTrace();

            // Fallback approach
            Platform.runLater(() -> {
                try {
                    codeArea.clear();
                    codeArea.appendText(query);
                } catch (Exception ex) {
                    System.err.println("Fallback for setting query text failed: " + ex.getMessage());
                }
            });
        }
    }

    public String getQuery() {
        return codeArea.getText();
    }

    public void setDatabase(String database) {
        this.currentDatabase = database;
        connectionManager.setCurrentDatabase(database);

        // Update the combo box
        if (databaseComboBox != null && database != null) {
            Platform.runLater(() -> {
                if (!databaseComboBox.getItems().contains(database)) {
                    databaseComboBox.getItems().add(database);
                }
                databaseComboBox.setValue(database);
                statusLabel.setText("Database set to: " + database);
            });
        }
    }

    public String getDatabase() {
        return currentDatabase;
    }

    public void setOnExecuteQuery(Consumer<Map<String, Object>> onExecuteQuery) {
        this.onExecuteQuery = onExecuteQuery;
    }

    /**
     * Returns the code area component
     * 
     * @return the CodeArea instance
     */
    public CodeArea getCodeArea() {
        return codeArea;
    }

    /**
     * Refresh the database list
     */
    public void refreshDatabases() {
        loadDatabases();
    }
}