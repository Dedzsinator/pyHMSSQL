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
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import javafx.stage.FileChooser;

public class QueryEditor extends BorderPane {
    private CodeArea codeArea;
    private ConnectionManager connectionManager;
    private Consumer<Map<String, Object>> onExecuteQuery;
    private ResultPane resultPane;
    private ExecutorService executor;
    private ToolBar toolBar;
    private String currentDatabase;
    
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
        Pattern.CASE_INSENSITIVE
    );
    
    public QueryEditor(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        this.executor = Executors.newSingleThreadExecutor();
        
        // Set up the UI
        setPadding(new Insets(0));
        
        // Create code area with syntax highlighting
        setupCodeArea();
        
        // Create toolbar
        setupToolbar();
        
        // Create results pane
        setupResultPane();
        
        // Layout components
        setTop(toolBar);
        
        SplitPane splitPane = new SplitPane();
        splitPane.setOrientation(Orientation.VERTICAL);
        splitPane.getItems().addAll(
            new VirtualizedScrollPane<>(codeArea),
            resultPane
        );
        splitPane.setDividerPositions(0.6);
        
        setCenter(splitPane);
    }
    
    private void setupCodeArea() {
        codeArea = new CodeArea();
        codeArea.setParagraphGraphicFactory(LineNumberFactory.get(codeArea));
        
        codeArea.multiPlainChanges()
            .successionEnds(Duration.ofMillis(500))
            .retainLatestUntilLater(executor)
            .supplyTask(() -> {
                return new javafx.concurrent.Task<StyleSpans<Collection<String>>>() {
                    @Override
                    protected StyleSpans<Collection<String>> call() throws Exception {
                        return computeHighlighting(codeArea.getText());
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
            .subscribe(highlighting -> codeArea.setStyleSpans(0, highlighting));
        
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
        
        // Initial text with a placeholder
        codeArea.replaceText(0, 0, "-- Write your SQL query here\n");
        
        // Add syntax highlighting styles
        codeArea.getStylesheets().add(getClass().getResource("/css/sql-highlighting.css").toExternalForm());
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
        ComboBox<String> databaseComboBox = new ComboBox<>();
        databaseComboBox.setPromptText("Select Database");
        databaseComboBox.setPrefWidth(150);
        databaseComboBox.setOnAction(e -> {
            currentDatabase = databaseComboBox.getValue();
            connectionManager.setCurrentDatabase(currentDatabase);
        });
        
        // History dropdown
        ComboBox<String> historyComboBox = new ComboBox<>();
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
            newQueryButton
        );
        
        // Load databases into combo box
        loadDatabases(databaseComboBox);
    }
    
    private void setupResultPane() {
        resultPane = new ResultPane();
    }
    
    private void loadDatabases(ComboBox<String> databaseComboBox) {
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
                    });
                }
            })
            .exceptionally(ex -> {
                ex.printStackTrace();
                return null;
            });
    }
    
    private StyleSpans<Collection<String>> computeHighlighting(String text) {
        Matcher matcher = PATTERN.matcher(text.toUpperCase());
        int lastTokenEnd = 0;
        StyleSpansBuilder<Collection<String>> spansBuilder = new StyleSpansBuilder<>();
        
        while (matcher.find()) {
            String styleClass = 
                matcher.group("KEYWORD") != null ? "keyword" :
                matcher.group("STRING") != null ? "string" :
                matcher.group("COMMENT") != null ? "comment" :
                matcher.group("MLCOMMENT") != null ? "comment" :
                matcher.group("NUMBER") != null ? "number" :
                null; // never happens
                
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
        
        // Show a loading indicator
        resultPane.showLoading();
        
        connectionManager.executeQuery(query)
            .thenAccept(result -> {
                javafx.application.Platform.runLater(() -> {
                    resultPane.displayResults(result);
                    
                    // If we have a callback, invoke it
                    if (onExecuteQuery != null) {
                        onExecuteQuery.accept(result);
                    }
                });
            })
            .exceptionally(ex -> {
                javafx.application.Platform.runLater(() -> {
                    resultPane.showError("Error executing query: " + ex.getMessage());
                });
                ex.printStackTrace();
                return null;
            });
    }
    
    private void formatQuery() {
        // In a real implementation, this would call a SQL formatter library
        // For now, we'll just show a placeholder message
        showAlert(Alert.AlertType.INFORMATION, "Format SQL", 
                  "SQL formatting", "SQL formatting will be implemented in a future version.");
    }
    
    private void saveQuery() {
        // Open a file save dialog
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Save SQL Query");
        fileChooser.getExtensionFilters().add(
            new FileChooser.ExtensionFilter("SQL Files", "*.sql")
        );
        
        File file = fileChooser.showSaveDialog(getScene().getWindow());
        if (file != null) {
            try {
                Files.write(file.toPath(), codeArea.getText().getBytes());
                showAlert(Alert.AlertType.INFORMATION, "Save SQL", 
                          "Query saved", "Query successfully saved to " + file.getPath());
            } catch (IOException e) {
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
        if (query != null) {
            codeArea.replaceText(query);
        }
    }
    
    public String getQuery() {
        return codeArea.getText();
    }
    
    public void setDatabase(String database) {
        this.currentDatabase = database;
        connectionManager.setCurrentDatabase(database);
    }
    
    public String getDatabase() {
        return currentDatabase;
    }
    
    public void setOnExecuteQuery(Consumer<Map<String, Object>> onExecuteQuery) {
        this.onExecuteQuery = onExecuteQuery;
    }
}