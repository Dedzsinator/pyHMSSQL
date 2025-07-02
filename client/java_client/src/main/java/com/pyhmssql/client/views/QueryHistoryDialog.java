package com.pyhmssql.client.views;

import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.*;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.Window;
import org.kordamp.ikonli.javafx.FontIcon;
import org.kordamp.ikonli.material2.Material2AL;
import org.kordamp.ikonli.material2.Material2MZ;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Professional Query History Dialog with search, filtering, and management
 * features
 */
public class QueryHistoryDialog extends Stage {
    private TableView<QueryHistoryEntry> historyTable;
    private ObservableList<QueryHistoryEntry> historyData;
    private TextField searchField;
    private ComboBox<String> statusFilter;
    private TextArea queryPreview;
    private Consumer<String> onQuerySelected;

    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static class QueryHistoryEntry {
        private final SimpleStringProperty timestamp;
        private final SimpleStringProperty query;
        private final SimpleStringProperty status;
        private final SimpleStringProperty duration;
        private final SimpleStringProperty database;

        public QueryHistoryEntry(String timestamp, String query, String status, String duration, String database) {
            this.timestamp = new SimpleStringProperty(timestamp);
            this.query = new SimpleStringProperty(query);
            this.status = new SimpleStringProperty(status);
            this.duration = new SimpleStringProperty(duration);
            this.database = new SimpleStringProperty(database);
        }

        // Getters
        public String getTimestamp() {
            return timestamp.get();
        }

        public String getQuery() {
            return query.get();
        }

        public String getStatus() {
            return status.get();
        }

        public String getDuration() {
            return duration.get();
        }

        public String getDatabase() {
            return database.get();
        }

        // Property getters for TableView
        public SimpleStringProperty timestampProperty() {
            return timestamp;
        }

        public SimpleStringProperty queryProperty() {
            return query;
        }

        public SimpleStringProperty statusProperty() {
            return status;
        }

        public SimpleStringProperty durationProperty() {
            return duration;
        }

        public SimpleStringProperty databaseProperty() {
            return database;
        }
    }

    public QueryHistoryDialog(Window owner) {
        initOwner(owner);
        initModality(Modality.APPLICATION_MODAL);
        setTitle("Query History");

        createUI();
        setupEventHandlers();
        loadSampleData(); // Replace with actual data loading

        setScene(new Scene(createContent(), 900, 600));
    }

    private void createUI() {
        // Initialize data
        historyData = FXCollections.observableArrayList();

        // Create table
        createTable();

        // Create search and filter controls
        createSearchControls();

        // Create query preview
        createQueryPreview();
    }

    private VBox createContent() {
        VBox root = new VBox(10);
        root.setPadding(new Insets(20));
        root.getStyleClass().add("dialog-content");

        // Header
        Label titleLabel = new Label("Query History");
        titleLabel.getStyleClass().addAll("dialog-title", "text-primary");

        // Search and filter bar
        HBox searchBar = createSearchBar();

        // Main content - split pane
        SplitPane splitPane = new SplitPane();
        splitPane.setOrientation(javafx.geometry.Orientation.VERTICAL);
        splitPane.setDividerPositions(0.6);

        // Table container
        VBox tableContainer = new VBox(5);
        Label tableLabel = new Label("Query History");
        tableLabel.getStyleClass().add("section-header");

        tableContainer.getChildren().addAll(tableLabel, historyTable);
        VBox.setVgrow(historyTable, Priority.ALWAYS);

        // Preview container
        VBox previewContainer = new VBox(5);
        Label previewLabel = new Label("Query Preview");
        previewLabel.getStyleClass().add("section-header");

        previewContainer.getChildren().addAll(previewLabel, queryPreview);
        VBox.setVgrow(queryPreview, Priority.ALWAYS);

        splitPane.getItems().addAll(tableContainer, previewContainer);

        // Buttons
        HBox buttonBar = createButtonBar();

        root.getChildren().addAll(titleLabel, searchBar, splitPane, buttonBar);
        VBox.setVgrow(splitPane, Priority.ALWAYS);

        return root;
    }

    private void createTable() {
        historyTable = new TableView<>();
        historyTable.getStyleClass().add("modern-table");

        // Timestamp column
        TableColumn<QueryHistoryEntry, String> timestampCol = new TableColumn<>("Timestamp");
        timestampCol.setCellValueFactory(new PropertyValueFactory<>("timestamp"));
        timestampCol.setPrefWidth(150);

        // Query column
        TableColumn<QueryHistoryEntry, String> queryCol = new TableColumn<>("Query");
        queryCol.setCellValueFactory(new PropertyValueFactory<>("query"));
        queryCol.setPrefWidth(300);
        queryCol.setCellFactory(column -> new TableCell<QueryHistoryEntry, String>() {
            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setText(null);
                } else {
                    // Truncate long queries for display
                    String displayText = item.length() > 50 ? item.substring(0, 50) + "..." : item;
                    setText(displayText);
                    setTooltip(new Tooltip(item));
                }
            }
        });

        // Status column
        TableColumn<QueryHistoryEntry, String> statusCol = new TableColumn<>("Status");
        statusCol.setCellValueFactory(new PropertyValueFactory<>("status"));
        statusCol.setPrefWidth(100);
        statusCol.setCellFactory(column -> new TableCell<QueryHistoryEntry, String>() {
            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setText(null);
                    getStyleClass().removeAll("status-success", "status-error", "status-warning");
                } else {
                    setText(item);
                    getStyleClass().removeAll("status-success", "status-error", "status-warning");
                    switch (item.toLowerCase()) {
                        case "success":
                            getStyleClass().add("status-success");
                            break;
                        case "error":
                            getStyleClass().add("status-error");
                            break;
                        case "warning":
                            getStyleClass().add("status-warning");
                            break;
                    }
                }
            }
        });

        // Duration column
        TableColumn<QueryHistoryEntry, String> durationCol = new TableColumn<>("Duration");
        durationCol.setCellValueFactory(new PropertyValueFactory<>("duration"));
        durationCol.setPrefWidth(100);

        // Database column
        TableColumn<QueryHistoryEntry, String> databaseCol = new TableColumn<>("Database");
        databaseCol.setCellValueFactory(new PropertyValueFactory<>("database"));
        databaseCol.setPrefWidth(150);

        historyTable.getColumns().addAll(timestampCol, queryCol, statusCol, durationCol, databaseCol);
        historyTable.setItems(historyData);

        // Enable multiple selection
        historyTable.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);
    }

    private void createSearchControls() {
        searchField = new TextField();
        searchField.setPromptText("Search queries...");
        searchField.getStyleClass().add("search-field");

        statusFilter = new ComboBox<>();
        statusFilter.getItems().addAll("All", "Success", "Error", "Warning");
        statusFilter.setValue("All");
        statusFilter.getStyleClass().add("filter-combo");
    }

    private void createQueryPreview() {
        queryPreview = new TextArea();
        queryPreview.setEditable(false);
        queryPreview.setPromptText("Select a query to preview...");
        queryPreview.getStyleClass().add("code-area");
        queryPreview.setPrefRowCount(8);
    }

    private HBox createSearchBar() {
        HBox searchBar = new HBox(10);
        searchBar.setAlignment(javafx.geometry.Pos.CENTER_LEFT);

        Label searchLabel = new Label("Search:");
        searchLabel.getStyleClass().add("form-label");

        Label filterLabel = new Label("Status:");
        filterLabel.getStyleClass().add("form-label");

        Button clearButton = new Button("Clear");
        clearButton.setGraphic(new FontIcon(Material2AL.CLEAR));
        clearButton.getStyleClass().addAll("secondary-button", "small-button");
        clearButton.setOnAction(e -> clearFilters());

        Button refreshButton = new Button("Refresh");
        refreshButton.setGraphic(new FontIcon(Material2MZ.REFRESH));
        refreshButton.getStyleClass().addAll("secondary-button", "small-button");
        refreshButton.setOnAction(e -> refreshHistory());

        HBox.setHgrow(searchField, Priority.ALWAYS);

        searchBar.getChildren().addAll(
                searchLabel, searchField,
                filterLabel, statusFilter,
                clearButton, refreshButton);

        return searchBar;
    }

    private HBox createButtonBar() {
        HBox buttonBar = new HBox(10);
        buttonBar.setAlignment(javafx.geometry.Pos.CENTER_RIGHT);

        Button useQueryButton = new Button("Use Query");
        useQueryButton.setGraphic(new FontIcon(Material2AL.ASSIGNMENT));
        useQueryButton.getStyleClass().add("primary-button");
        useQueryButton.setOnAction(e -> useSelectedQuery());

        Button deleteButton = new Button("Delete Selected");
        deleteButton.setGraphic(new FontIcon(Material2AL.DELETE));
        deleteButton.getStyleClass().add("danger-button");
        deleteButton.setOnAction(e -> deleteSelectedQueries());

        Button clearAllButton = new Button("Clear All");
        clearAllButton.setGraphic(new FontIcon(Material2AL.DELETE_SWEEP));
        clearAllButton.getStyleClass().add("danger-button");
        clearAllButton.setOnAction(e -> clearAllHistory());

        Button closeButton = new Button("Close");
        closeButton.setGraphic(new FontIcon(Material2AL.CLOSE));
        closeButton.getStyleClass().add("secondary-button");
        closeButton.setOnAction(e -> close());

        buttonBar.getChildren().addAll(useQueryButton, deleteButton, clearAllButton, closeButton);

        return buttonBar;
    }

    private void setupEventHandlers() {
        // Table selection
        historyTable.getSelectionModel().selectedItemProperty().addListener((obs, oldSelection, newSelection) -> {
            if (newSelection != null) {
                queryPreview.setText(newSelection.getQuery());
            } else {
                queryPreview.clear();
            }
        });

        // Search filtering
        searchField.textProperty().addListener((obs, oldText, newText) -> filterHistory());
        statusFilter.setOnAction(e -> filterHistory());

        // Double-click to use query
        historyTable.setRowFactory(tv -> {
            TableRow<QueryHistoryEntry> row = new TableRow<>();
            row.setOnMouseClicked(event -> {
                if (event.getClickCount() == 2 && !row.isEmpty()) {
                    useSelectedQuery();
                }
            });
            return row;
        });
    }

    private void filterHistory() {
        String searchText = searchField.getText().toLowerCase();
        String statusText = statusFilter.getValue();

        ObservableList<QueryHistoryEntry> filteredData = FXCollections.observableArrayList();

        for (QueryHistoryEntry entry : historyData) {
            boolean matchesSearch = searchText.isEmpty() ||
                    entry.getQuery().toLowerCase().contains(searchText) ||
                    entry.getDatabase().toLowerCase().contains(searchText);

            boolean matchesStatus = "All".equals(statusText) ||
                    entry.getStatus().equalsIgnoreCase(statusText);

            if (matchesSearch && matchesStatus) {
                filteredData.add(entry);
            }
        }

        historyTable.setItems(filteredData);
    }

    private void clearFilters() {
        searchField.clear();
        statusFilter.setValue("All");
        historyTable.setItems(historyData);
    }

    private void refreshHistory() {
        // TODO: Implement actual history loading from database/file
        loadSampleData();
    }

    private void useSelectedQuery() {
        QueryHistoryEntry selected = historyTable.getSelectionModel().getSelectedItem();
        if (selected != null && onQuerySelected != null) {
            onQuerySelected.accept(selected.getQuery());
            close();
        }
    }

    private void deleteSelectedQueries() {
        List<QueryHistoryEntry> selected = new ArrayList<>(historyTable.getSelectionModel().getSelectedItems());
        if (!selected.isEmpty()) {
            Alert confirmation = new Alert(Alert.AlertType.CONFIRMATION);
            confirmation.setTitle("Delete Queries");
            confirmation.setHeaderText("Delete selected queries?");
            confirmation.setContentText("This action cannot be undone.");

            if (confirmation.showAndWait().orElse(ButtonType.CANCEL) == ButtonType.OK) {
                historyData.removeAll(selected);
            }
        }
    }

    private void clearAllHistory() {
        Alert confirmation = new Alert(Alert.AlertType.CONFIRMATION);
        confirmation.setTitle("Clear All History");
        confirmation.setHeaderText("Clear all query history?");
        confirmation.setContentText("This action cannot be undone.");

        if (confirmation.showAndWait().orElse(ButtonType.CANCEL) == ButtonType.OK) {
            historyData.clear();
            queryPreview.clear();
        }
    }

    private void loadSampleData() {
        // Sample data - replace with actual history loading
        historyData.clear();
        historyData.addAll(
                new QueryHistoryEntry(
                        LocalDateTime.now().minusHours(1).format(DATETIME_FORMATTER),
                        "SELECT * FROM users WHERE age > 25",
                        "Success",
                        "0.125s",
                        "company_db"),
                new QueryHistoryEntry(
                        LocalDateTime.now().minusHours(2).format(DATETIME_FORMATTER),
                        "UPDATE products SET price = price * 1.1 WHERE category = 'electronics'",
                        "Success",
                        "0.856s",
                        "inventory_db"),
                new QueryHistoryEntry(
                        LocalDateTime.now().minusHours(3).format(DATETIME_FORMATTER),
                        "SELECT COUNT(*) FROM orders WHERE date >= '2024-01-01'",
                        "Success",
                        "0.045s",
                        "sales_db"),
                new QueryHistoryEntry(
                        LocalDateTime.now().minusHours(4).format(DATETIME_FORMATTER),
                        "CREATE INDEX idx_user_email ON users(email)",
                        "Error",
                        "0.012s",
                        "company_db"),
                new QueryHistoryEntry(
                        LocalDateTime.now().minusHours(5).format(DATETIME_FORMATTER),
                        "EXPLAIN ANALYZE SELECT * FROM large_table WHERE status = 'active'",
                        "Warning",
                        "12.345s",
                        "analytics_db"));
    }

    public void setOnQuerySelected(Consumer<String> callback) {
        this.onQuerySelected = callback;
    }

    public void addQuery(String query, String status, String duration, String database) {
        Platform.runLater(() -> {
            QueryHistoryEntry entry = new QueryHistoryEntry(
                    LocalDateTime.now().format(DATETIME_FORMATTER),
                    query,
                    status,
                    duration,
                    database);
            historyData.add(0, entry); // Add to beginning
        });
    }
}
