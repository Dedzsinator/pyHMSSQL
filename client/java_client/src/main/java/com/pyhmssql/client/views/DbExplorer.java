package com.pyhmssql.client.views;

import com.pyhmssql.client.main.ConnectionManager;
import javafx.scene.control.*;
import javafx.scene.layout.VBox;
import javafx.application.Platform;
import javafx.event.Event;
import javafx.event.EventType;
import javafx.geometry.Insets;
import org.kordamp.ikonli.javafx.FontIcon;
import org.kordamp.ikonli.material2.Material2AL;
import org.kordamp.ikonli.material2.Material2MZ;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.geometry.Pos;
import javafx.concurrent.Task;

import java.util.*;

/**
 * Enhanced Database explorer component with modern UI and advanced features
 */
public class DbExplorer extends VBox {
    private final ConnectionManager connectionManager;
    private final TreeView<String> treeView;
    private final TreeItem<String> rootItem;
    private String selectedDatabase;

    // Modern UI components
    private TextField searchField;
    private ComboBox<String> filterComboBox;
    private Button refreshButton;
    private ProgressIndicator loadingIndicator;
    private Label statusLabel;

    // Custom events for communication with main window
    public static class QueryBuilderTableEvent extends Event {
        public static final EventType<QueryBuilderTableEvent> QB_EVENT_TYPE = new EventType<>(Event.ANY,
                "QUERY_BUILDER_TABLE");

        private final String dbName;
        private final String tableName;

        public QueryBuilderTableEvent(String dbName, String tableName) {
            super(QB_EVENT_TYPE);
            this.dbName = dbName;
            this.tableName = tableName;
        }

        public String getDbName() {
            return dbName;
        }

        public String getTableName() {
            return tableName;
        }

        public static EventType<QueryBuilderTableEvent> getEventTypeQB() {
            return QB_EVENT_TYPE;
        }
    }

    public static class NewQueryEvent extends Event {
        public static final EventType<NewQueryEvent> NQ_EVENT_TYPE = new EventType<>(Event.ANY, "NEW_QUERY");

        private final String tableName;
        private final String query;

        public NewQueryEvent(String tableName, String query) {
            super(NQ_EVENT_TYPE);
            this.tableName = tableName;
            this.query = query;
        }

        public String getTableName() {
            return tableName;
        }

        public String getQuery() {
            return query;
        }

        public static EventType<NewQueryEvent> getEventTypeNQ() {
            return NQ_EVENT_TYPE;
        }
    }

    public DbExplorer(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;

        setPadding(new Insets(5));
        setSpacing(5);
        getStyleClass().add("db-explorer");

        // Create modern header
        createModernHeader();

        // Create tree view
        rootItem = new TreeItem<>("Databases");
        rootItem.setExpanded(true);
        treeView = new TreeView<>(rootItem);
        treeView.setShowRoot(true);
        treeView.getStyleClass().add("modern-tree-view");

        // Set up context menu
        setupContextMenu();

        // Handle selection
        treeView.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            if (newVal != null) {
                handleSelection(newVal);
            }
        });

        // Add tree view with progress overlay
        createTreeViewContainer();

        // Status bar
        statusLabel = new Label("Ready");
        statusLabel.getStyleClass().add("status-label");

        getChildren().addAll(statusLabel);
        VBox.setVgrow(treeView, Priority.ALWAYS);

        // Listen for connection status
        connectionManager.addConnectionListener(connected -> {
            if (connected) {
                Platform.runLater(this::refreshDatabases);
            } else {
                Platform.runLater(() -> {
                    rootItem.getChildren().clear();
                    statusLabel.setText("Disconnected");
                });
            }
        });

        // Initial load if already connected
        if (connectionManager.isConnected()) {
            refreshDatabases();
        }
    }

    private void createModernHeader() {
        Label headerLabel = new Label("Database Explorer");
        headerLabel.getStyleClass().addAll("header-label", "text-primary");

        // Search field with icon
        searchField = new TextField();
        searchField.setPromptText("Search databases, tables...");
        searchField.getStyleClass().add("search-field");

        FontIcon searchIcon = new FontIcon(Material2AL.SEARCH);
        searchIcon.getStyleClass().add("search-icon");
        searchField.setLeft(searchIcon);

        // Filter dropdown
        filterComboBox = new ComboBox<>();
        filterComboBox.getItems().addAll("All Items", "Databases Only", "Tables Only", "Columns Only");
        filterComboBox.setValue("All Items");
        filterComboBox.getStyleClass().add("filter-combo");

        // Refresh button with icon
        refreshButton = new Button();
        refreshButton.setGraphic(new FontIcon(Material2MZ.REFRESH));
        refreshButton.getStyleClass().addAll("icon-button", "refresh-button");
        refreshButton.setOnAction(e -> refreshDatabases());
        refreshButton.setTooltip(new Tooltip("Refresh database list"));

        // Loading indicator
        loadingIndicator = new ProgressIndicator();
        loadingIndicator.setPrefSize(20, 20);
        loadingIndicator.setVisible(false);

        // Search and filter handling
        searchField.textProperty()
                .addListener((obs, oldText, newText) -> filterTreeView(newText, filterComboBox.getValue()));

        filterComboBox.setOnAction(e -> filterTreeView(searchField.getText(), filterComboBox.getValue()));

        // Layout
        HBox searchBar = new HBox(8);
        searchBar.getChildren().addAll(searchField, filterComboBox, refreshButton, loadingIndicator);
        searchBar.setAlignment(Pos.CENTER_LEFT);
        HBox.setHgrow(searchField, Priority.ALWAYS);
        searchBar.getStyleClass().add("search-bar");

        getChildren().addAll(headerLabel, searchBar);
    }

    private void createTreeViewContainer() {
        // Create container for tree view with overlay support
        StackPane treeContainer = new StackPane();
        treeContainer.getChildren().add(treeView);
        treeContainer.getStyleClass().add("tree-container");

        getChildren().add(treeContainer);
        VBox.setVgrow(treeContainer, Priority.ALWAYS);
    }

    private void filterTreeView(String searchText, String filterType) {
        if (searchText == null)
            searchText = "";
        if (filterType == null)
            filterType = "All Items";

        final String search = searchText.toLowerCase().trim();
        final String filter = filterType;

        // Apply filtering logic
        Platform.runLater(() -> {
            filterTreeItem(rootItem, search, filter);
            if (!search.isEmpty() || !"All Items".equals(filter)) {
                expandFilteredItems(rootItem);
            }
        });
    }

    private boolean filterTreeItem(TreeItem<String> item, String searchText, String filterType) {
        if (item == null || item == rootItem)
            return true;

        String itemText = item.getValue().toLowerCase();
        boolean textMatch = searchText.isEmpty() || itemText.contains(searchText);
        boolean typeMatch = matchesFilter(item, filterType);

        boolean shouldShow = textMatch && typeMatch;

        // Check children
        boolean hasVisibleChildren = false;
        for (TreeItem<String> child : item.getChildren()) {
            if (filterTreeItem(child, searchText, filterType)) {
                hasVisibleChildren = true;
            }
        }

        shouldShow = shouldShow || hasVisibleChildren;

        // Update visibility (JavaFX doesn't have built-in filtering, so we simulate it)
        item.setExpanded(shouldShow && hasVisibleChildren);

        return shouldShow;
    }

    private boolean matchesFilter(TreeItem<String> item, String filterType) {
        if ("All Items".equals(filterType))
            return true;

        TreeItem<String> parent = item.getParent();
        if (parent == null)
            return true;

        return switch (filterType) {
            case "Databases Only" -> parent == rootItem;
            case "Tables Only" -> parent != rootItem && parent.getParent() == rootItem;
            case "Columns Only" -> parent != rootItem && parent.getParent() != rootItem &&
                    parent.getParent().getParent() == rootItem;
            default -> true;
        };
    }

    private void expandFilteredItems(TreeItem<String> item) {
        if (item != null && !item.getChildren().isEmpty()) {
            item.setExpanded(true);
            for (TreeItem<String> child : item.getChildren()) {
                expandFilteredItems(child);
            }
        }
    }

    /**
     * Refreshes the database list
     */
    public void refreshDatabases() {
        System.out.println("[DEBUG] DbExplorer: Refreshing databases...");

        loadingIndicator.setVisible(true);
        statusLabel.setText("Loading databases...");
        connectionManager.getDatabases()
                .thenAccept(result -> {
                    System.out.println("[DEBUG] DbExplorer: Received database result: " + result);

                    Platform.runLater(() -> {
                        try {
                            rootItem.getChildren().clear();

                            if (result.containsKey("error")) {
                                String error = result.get("error").toString();
                                System.err.println("Error loading databases: " + error);
                                TreeItem<String> errorItem = new TreeItem<>("Error: " + error);
                                rootItem.getChildren().add(errorItem);
                                return;
                            }

                            // Handle different response formats
                            List<String> databases = new ArrayList<>();

                            if (result.containsKey("databases")) {
                                @SuppressWarnings("unchecked")
                                List<String> dbList = (List<String>) result.get("databases");
                                databases.addAll(dbList);
                            } else if (result.containsKey("rows")) {
                                @SuppressWarnings("unchecked")
                                List<List<Object>> rows = (List<List<Object>>) result.get("rows");
                                for (List<Object> row : rows) {
                                    if (!row.isEmpty()) {
                                        databases.add(row.get(0).toString());
                                    }
                                }
                            } else {
                                System.err.println("Unexpected database response format: " + result.keySet());
                                TreeItem<String> errorItem = new TreeItem<>("Unexpected response format");
                                rootItem.getChildren().add(errorItem);
                                return;
                            }

                            if (databases.isEmpty()) {
                                TreeItem<String> noDbItem = new TreeItem<>("No databases found");
                                rootItem.getChildren().add(noDbItem);
                                return;
                            }

                            // Add database items
                            for (String dbName : databases) {
                                TreeItem<String> dbItem = new TreeItem<>(dbName);
                                TreeItem<String> loadingItem = new TreeItem<>("Loading tables...");
                                dbItem.getChildren().add(loadingItem);
                                rootItem.getChildren().add(dbItem);

                                // Load tables when database is expanded
                                dbItem.expandedProperty().addListener((obs, wasExpanded, isNowExpanded) -> {
                                    if (isNowExpanded && dbItem.getChildren().size() == 1 &&
                                            dbItem.getChildren().get(0).getValue().equals("Loading tables...")) {
                                        loadTablesForDatabase(dbItem, dbName);
                                    }
                                });
                            }

                            System.out.println(
                                    "[DEBUG] DbExplorer: Successfully loaded " + databases.size() + " databases");

                        } catch (Exception e) {
                            System.err.println("Error processing database list: " + e.getMessage());
                            e.printStackTrace();
                            TreeItem<String> errorItem = new TreeItem<>("Error: " + e.getMessage());
                            rootItem.getChildren().add(errorItem);
                        } finally {
                            loadingIndicator.setVisible(false);
                            statusLabel.setText("");
                        }
                    });
                })
                .exceptionally(ex -> {
                    System.err.println("Exception loading databases: " + ex.getMessage());
                    ex.printStackTrace();
                    Platform.runLater(() -> {
                        rootItem.getChildren().clear();
                        TreeItem<String> errorItem = new TreeItem<>("Error: " + ex.getMessage());
                        rootItem.getChildren().add(errorItem);
                    });
                    return null;
                });
    }

    /**
     * Loads tables for a specific database
     */
    private void loadTablesForDatabase(TreeItem<String> dbItem, String dbName) {
        System.out.println("[DEBUG] DbExplorer: Loading tables for database: " + dbName);

        connectionManager.getTables(dbName)
                .thenAccept(result -> {
                    System.out.println("[DEBUG] DbExplorer: Received tables result: " + result);

                    Platform.runLater(() -> {
                        try {
                            dbItem.getChildren().clear();

                            if (result.containsKey("error")) {
                                String error = result.get("error").toString();
                                System.err.println("Error loading tables for " + dbName + ": " + error);
                                TreeItem<String> errorItem = new TreeItem<>("Error: " + error);
                                dbItem.getChildren().add(errorItem);
                                return;
                            }

                            // Handle different response formats
                            List<String> tables = new ArrayList<>();

                            if (result.containsKey("tables")) {
                                @SuppressWarnings("unchecked")
                                List<String> tableList = (List<String>) result.get("tables");
                                tables.addAll(tableList);
                            } else if (result.containsKey("rows")) {
                                @SuppressWarnings("unchecked")
                                List<List<Object>> rows = (List<List<Object>>) result.get("rows");
                                for (List<Object> row : rows) {
                                    if (!row.isEmpty()) {
                                        tables.add(row.get(0).toString());
                                    }
                                }
                            } else {
                                System.err.println(
                                        "Unexpected tables response format for " + dbName + ": " + result.keySet());
                                TreeItem<String> errorItem = new TreeItem<>("Unexpected response format");
                                dbItem.getChildren().add(errorItem);
                                return;
                            }

                            if (tables.isEmpty()) {
                                TreeItem<String> noTablesItem = new TreeItem<>("No tables found");
                                dbItem.getChildren().add(noTablesItem);
                                return;
                            }

                            // Add table items
                            for (String tableName : tables) {
                                TreeItem<String> tableItem = new TreeItem<>(tableName);
                                TreeItem<String> loadingItem = new TreeItem<>("Loading columns...");
                                tableItem.getChildren().add(loadingItem);
                                dbItem.getChildren().add(tableItem);

                                // Load columns when table is expanded
                                tableItem.expandedProperty().addListener((obs, wasExpanded, isNowExpanded) -> {
                                    if (isNowExpanded && tableItem.getChildren().size() == 1 &&
                                            tableItem.getChildren().get(0).getValue().equals("Loading columns...")) {
                                        loadColumnsForTable(tableItem, dbName, tableName);
                                    }
                                });
                            }

                            System.out.println("[DEBUG] DbExplorer: Successfully loaded " + tables.size()
                                    + " tables for " + dbName);

                        } catch (Exception e) {
                            System.err.println("Error processing tables for " + dbName + ": " + e.getMessage());
                            e.printStackTrace();
                            TreeItem<String> errorItem = new TreeItem<>("Error: " + e.getMessage());
                            dbItem.getChildren().add(errorItem);
                        }
                    });
                })
                .exceptionally(ex -> {
                    System.err.println("Exception loading tables for " + dbName + ": " + ex.getMessage());
                    ex.printStackTrace();
                    Platform.runLater(() -> {
                        dbItem.getChildren().clear();
                        TreeItem<String> errorItem = new TreeItem<>("Error: " + ex.getMessage());
                        dbItem.getChildren().add(errorItem);
                    });
                    return null;
                });
    }

    /**
     * Loads columns for a specific table
     */
    private void loadColumnsForTable(TreeItem<String> tableItem, String dbName, String tableName) {
        connectionManager.getColumns(dbName, tableName)
                .thenAccept(result -> {
                    Platform.runLater(() -> {
                        tableItem.getChildren().clear();

                        if (result.containsKey("error")) {
                            TreeItem<String> errorItem = new TreeItem<>("Error: " + result.get("error"));
                            tableItem.getChildren().add(errorItem);
                            return;
                        }

                        if (result.containsKey("columns")) {
                            @SuppressWarnings("unchecked")
                            List<Map<String, Object>> columns = (List<Map<String, Object>>) result.get("columns");

                            if (columns.isEmpty()) {
                                TreeItem<String> noColumnsItem = new TreeItem<>("No columns found");
                                tableItem.getChildren().add(noColumnsItem);
                            } else {
                                for (Map<String, Object> column : columns) {
                                    String columnName = (String) column.get("name");
                                    String columnType = (String) column.get("type");
                                    boolean isPrimaryKey = (Boolean) column.getOrDefault("primary_key", false);

                                    String displayName = columnName + " (" + columnType + ")";
                                    if (isPrimaryKey) {
                                        displayName += " [PK]";
                                    }

                                    TreeItem<String> columnItem = new TreeItem<>(displayName);
                                    tableItem.getChildren().add(columnItem);
                                }
                            }
                        } else if (result.containsKey("rows")) {
                            @SuppressWarnings("unchecked")
                            List<List<Object>> rows = (List<List<Object>>) result.get("rows");

                            for (List<Object> row : rows) {
                                if (row.size() >= 2) {
                                    String columnName = row.get(0).toString();
                                    String columnType = row.get(1).toString();
                                    String displayName = columnName + " (" + columnType + ")";

                                    TreeItem<String> columnItem = new TreeItem<>(displayName);
                                    tableItem.getChildren().add(columnItem);
                                }
                            }
                        }
                    });
                })
                .exceptionally(ex -> {
                    Platform.runLater(() -> {
                        tableItem.getChildren().clear();
                        TreeItem<String> errorItem = new TreeItem<>("Error: " + ex.getMessage());
                        tableItem.getChildren().add(errorItem);
                    });
                    return null;
                });
    }

    /**
     * Sets up the context menu for tree items
     */
    private void setupContextMenu() {
        ContextMenu contextMenu = new ContextMenu();

        MenuItem selectFromMenuItem = new MenuItem("SELECT * FROM");
        selectFromMenuItem.setOnAction(e -> {
            TreeItem<String> selectedItem = treeView.getSelectionModel().getSelectedItem();
            if (selectedItem != null) {
                String[] path = getItemPath(selectedItem);
                if (path.length >= 2) { // Database and table
                    String query = "SELECT * FROM " + path[1] + " LIMIT 100;";
                    fireEvent(new NewQueryEvent(path[1], query));
                }
            }
        });

        MenuItem queryBuilderMenuItem = new MenuItem("Open in Query Builder");
        queryBuilderMenuItem.setOnAction(e -> {
            TreeItem<String> selectedItem = treeView.getSelectionModel().getSelectedItem();
            if (selectedItem != null) {
                String[] path = getItemPath(selectedItem);
                if (path.length >= 2) { // Database and table
                    fireEvent(new QueryBuilderTableEvent(path[0], path[1]));
                }
            }
        });

        MenuItem describeMenuItem = new MenuItem("DESCRIBE TABLE");
        describeMenuItem.setOnAction(e -> {
            TreeItem<String> selectedItem = treeView.getSelectionModel().getSelectedItem();
            if (selectedItem != null) {
                String[] path = getItemPath(selectedItem);
                if (path.length >= 2) { // Database and table
                    String query = "DESCRIBE " + path[1] + ";";
                    fireEvent(new NewQueryEvent(path[1] + " - Schema", query));
                }
            }
        });

        MenuItem showIndexesMenuItem = new MenuItem("SHOW INDEXES");
        showIndexesMenuItem.setOnAction(e -> {
            TreeItem<String> selectedItem = treeView.getSelectionModel().getSelectedItem();
            if (selectedItem != null) {
                String[] path = getItemPath(selectedItem);
                if (path.length >= 2) { // Database and table
                    String query = "SHOW INDEXES FOR " + path[1] + ";";
                    fireEvent(new NewQueryEvent(path[1] + " - Indexes", query));
                }
            }
        });

        MenuItem refreshMenuItem = new MenuItem("Refresh");
        refreshMenuItem.setOnAction(e -> refreshDatabases());

        contextMenu.getItems().addAll(
                selectFromMenuItem,
                queryBuilderMenuItem,
                new SeparatorMenuItem(),
                describeMenuItem,
                showIndexesMenuItem,
                new SeparatorMenuItem(),
                refreshMenuItem);

        treeView.setContextMenu(contextMenu);

        // Update menu items based on selection
        contextMenu.setOnShowing(e -> {
            TreeItem<String> selectedItem = treeView.getSelectionModel().getSelectedItem();
            boolean isTable = false;

            if (selectedItem != null) {
                String[] path = getItemPath(selectedItem);
                isTable = path.length >= 2 && !selectedItem.getValue().startsWith("Loading") &&
                        !selectedItem.getValue().startsWith("Error") && !selectedItem.getValue().startsWith("No ");
            }

            selectFromMenuItem.setDisable(!isTable);
            queryBuilderMenuItem.setDisable(!isTable);
            describeMenuItem.setDisable(!isTable);
            showIndexesMenuItem.setDisable(!isTable);
        });
    }

    /**
     * Handles tree item selection
     */
    private void handleSelection(TreeItem<String> item) {
        String[] path = getItemPath(item);

        if (path.length >= 1 && !path[0].equals("Databases")) {
            selectedDatabase = path[0];
        }

        // Set the current database in the connection manager
        if (selectedDatabase != null) {
            connectionManager.setCurrentDatabase(selectedDatabase);
        }
    }

    /**
     * Gets the path from root to the selected item
     */
    private String[] getItemPath(TreeItem<String> item) {
        List<String> path = new ArrayList<>();
        TreeItem<String> current = item;

        while (current != null && current != rootItem) {
            path.add(0, current.getValue());
            current = current.getParent();
        }

        return path.toArray(new String[0]);
    }

    /**
     * Gets the currently selected database
     */
    public String getSelectedDatabase() {
        return selectedDatabase;
    }

    /**
     * Filters tree items based on the search term
     */
    private void filterTreeItems(TreeItem<String> parentItem, String searchTerm) {
        for (TreeItem<String> item : parentItem.getChildren()) {
            boolean matches = item.getValue().toLowerCase().contains(searchTerm);
            item.setVisible(matches);

            // Recursively filter child items
            if (!matches) {
                filterTreeItems(item, searchTerm);
            }
        }
    }

    /**
     * Applies the selected filter to the tree items
     */
    private void applyFilterToTreeItems(TreeItem<String> parentItem, String filter) {
        for (TreeItem<String> item : parentItem.getChildren()) {
            boolean visible = true;

            if (filter.equals("Databases")) {
                visible = !item.getValue().startsWith("Loading") && !item.getValue().startsWith("Error") &&
                        !item.getValue().startsWith("No ") && !item.getValue().contains(".");
            } else if (filter.equals("Tables")) {
                visible = item.getValue().contains(".");
            } else if (filter.equals("Columns")) {
                visible = item.getValue().contains(" (") && item.getValue().endsWith(")");
            }

            item.setVisible(visible);

            // Recursively apply filter to child items
            if (visible) {
                applyFilterToTreeItems(item, filter);
            }
        }
    }
}