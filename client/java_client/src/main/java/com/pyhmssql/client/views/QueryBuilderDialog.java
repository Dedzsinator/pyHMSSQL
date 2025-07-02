package com.pyhmssql.client.views;

import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.stage.Modality;
import javafx.stage.Stage;
import javafx.stage.Window;
import org.kordamp.ikonli.javafx.FontIcon;
import org.kordamp.ikonli.material2.Material2AL;
import org.kordamp.ikonli.material2.Material2MZ;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Professional Visual Query Builder with drag-and-drop interface
 */
public class QueryBuilderDialog extends Stage {
    private VBox tablesPanel;
    private VBox fieldsPanel;
    private VBox conditionsPanel;
    private TextArea queryPreview;
    private ComboBox<String> queryTypeCombo;
    private Consumer<String> onQueryGenerated;

    // Query building components
    private ListView<String> selectedFieldsList;
    private ListView<String> conditionsList;
    private CheckBox distinctCheckBox;
    private TextField limitField;
    private ComboBox<String> orderByField;
    private ComboBox<String> orderDirectionCombo;

    private String currentDatabase;
    private String currentTable;

    public QueryBuilderDialog(Window owner) {
        initOwner(owner);
        initModality(Modality.APPLICATION_MODAL);
        setTitle("Visual Query Builder");

        createUI();
        setupEventHandlers();

        setScene(new Scene(createContent(), 1000, 700));
    }

    public QueryBuilderDialog(Window owner, String database, String table) {
        this(owner);
        this.currentDatabase = database;
        this.currentTable = table;
        initializeWithTable(database, table);
    }

    private void createUI() {
        selectedFieldsList = new ListView<>();
        conditionsList = new ListView<>();

        distinctCheckBox = new CheckBox("DISTINCT");
        limitField = new TextField();
        limitField.setPromptText("Limit (optional)");

        orderByField = new ComboBox<>();
        orderByField.setPromptText("Order by field");

        orderDirectionCombo = new ComboBox<>();
        orderDirectionCombo.getItems().addAll("ASC", "DESC");
        orderDirectionCombo.setValue("ASC");

        queryTypeCombo = new ComboBox<>();
        queryTypeCombo.getItems().addAll("SELECT", "INSERT", "UPDATE", "DELETE");
        queryTypeCombo.setValue("SELECT");

        createPanels();
        createQueryPreview();
    }

    private VBox createContent() {
        VBox root = new VBox(10);
        root.setPadding(new Insets(20));
        root.getStyleClass().add("dialog-content");

        // Header
        Label titleLabel = new Label("Visual Query Builder");
        titleLabel.getStyleClass().addAll("dialog-title", "text-primary");

        // Query type selector
        HBox headerBar = new HBox(10);
        headerBar.setAlignment(Pos.CENTER_LEFT);
        Label typeLabel = new Label("Query Type:");
        typeLabel.getStyleClass().add("form-label");
        headerBar.getChildren().addAll(typeLabel, queryTypeCombo);

        // Main content - split pane
        SplitPane mainSplitPane = new SplitPane();
        mainSplitPane.setOrientation(Orientation.HORIZONTAL);
        mainSplitPane.setDividerPositions(0.7);

        // Left side - query building panels
        SplitPane leftSplitPane = new SplitPane();
        leftSplitPane.setOrientation(Orientation.VERTICAL);
        leftSplitPane.setDividerPositions(0.4, 0.7);

        leftSplitPane.getItems().addAll(tablesPanel, fieldsPanel, conditionsPanel);

        // Right side - query preview and options
        VBox rightPanel = createRightPanel();

        mainSplitPane.getItems().addAll(leftSplitPane, rightPanel);

        // Buttons
        HBox buttonBar = createButtonBar();

        root.getChildren().addAll(titleLabel, headerBar, mainSplitPane, buttonBar);
        VBox.setVgrow(mainSplitPane, Priority.ALWAYS);

        return root;
    }

    private void createPanels() {
        // Tables panel
        tablesPanel = new VBox(5);
        tablesPanel.setPadding(new Insets(10));
        tablesPanel.getStyleClass().add("panel");

        Label tablesLabel = new Label("Tables");
        tablesLabel.getStyleClass().add("panel-header");

        ComboBox<String> databaseCombo = new ComboBox<>();
        databaseCombo.setPromptText("Select database...");
        databaseCombo.getStyleClass().add("full-width");

        ComboBox<String> tableCombo = new ComboBox<>();
        tableCombo.setPromptText("Select table...");
        tableCombo.getStyleClass().add("full-width");

        Button addTableButton = new Button("Add Table");
        addTableButton.setGraphic(new FontIcon(Material2AL.ADD));
        addTableButton.getStyleClass().addAll("primary-button", "small-button");

        ListView<String> selectedTablesList = new ListView<>();
        selectedTablesList.setPrefHeight(100);
        selectedTablesList.getStyleClass().add("modern-list");

        tablesPanel.getChildren().addAll(
                tablesLabel, databaseCombo, tableCombo, addTableButton, selectedTablesList);

        // Fields panel
        fieldsPanel = new VBox(5);
        fieldsPanel.setPadding(new Insets(10));
        fieldsPanel.getStyleClass().add("panel");

        Label fieldsLabel = new Label("Select Fields");
        fieldsLabel.getStyleClass().add("panel-header");

        ListView<String> availableFieldsList = new ListView<>();
        availableFieldsList.setPrefHeight(120);
        availableFieldsList.getStyleClass().add("modern-list");
        availableFieldsList.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);

        HBox fieldButtons = new HBox(5);
        Button addFieldButton = new Button("Add");
        addFieldButton.setGraphic(new FontIcon(Material2MZ.KEYBOARD_ARROW_RIGHT));
        addFieldButton.getStyleClass().addAll("secondary-button", "small-button");

        Button removeFieldButton = new Button("Remove");
        removeFieldButton.setGraphic(new FontIcon(Material2AL.KEYBOARD_ARROW_LEFT));
        removeFieldButton.getStyleClass().addAll("secondary-button", "small-button");

        Button addAllFieldsButton = new Button("Add All");
        addAllFieldsButton.setGraphic(new FontIcon(Material2MZ.KEYBOARD_DOUBLE_ARROW_RIGHT));
        addAllFieldsButton.getStyleClass().addAll("secondary-button", "small-button");

        fieldButtons.getChildren().addAll(addFieldButton, removeFieldButton, addAllFieldsButton);
        fieldButtons.setAlignment(Pos.CENTER);

        selectedFieldsList.setPrefHeight(120);
        selectedFieldsList.getStyleClass().add("modern-list");
        selectedFieldsList.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);

        fieldsPanel.getChildren().addAll(
                fieldsLabel,
                new Label("Available Fields:"),
                availableFieldsList,
                fieldButtons,
                new Label("Selected Fields:"),
                selectedFieldsList);

        // Conditions panel
        conditionsPanel = new VBox(5);
        conditionsPanel.setPadding(new Insets(10));
        conditionsPanel.getStyleClass().add("panel");

        Label conditionsLabel = new Label("WHERE Conditions");
        conditionsLabel.getStyleClass().add("panel-header");

        HBox conditionBuilder = new HBox(5);
        conditionBuilder.setAlignment(Pos.CENTER_LEFT);

        ComboBox<String> fieldCombo = new ComboBox<>();
        fieldCombo.setPromptText("Field");

        ComboBox<String> operatorCombo = new ComboBox<>();
        operatorCombo.getItems().addAll("=", "!=", "<", ">", "<=", ">=", "LIKE", "IN", "BETWEEN", "IS NULL",
                "IS NOT NULL");
        operatorCombo.setValue("=");

        TextField valueField = new TextField();
        valueField.setPromptText("Value");

        ComboBox<String> logicalCombo = new ComboBox<>();
        logicalCombo.getItems().addAll("AND", "OR");
        logicalCombo.setValue("AND");

        Button addConditionButton = new Button("Add");
        addConditionButton.setGraphic(new FontIcon(Material2AL.ADD));
        addConditionButton.getStyleClass().addAll("primary-button", "small-button");

        conditionBuilder.getChildren().addAll(
                fieldCombo, operatorCombo, valueField, logicalCombo, addConditionButton);

        conditionsList.setPrefHeight(100);
        conditionsList.getStyleClass().add("modern-list");

        Button removeConditionButton = new Button("Remove Selected");
        removeConditionButton.setGraphic(new FontIcon(Material2AL.DELETE));
        removeConditionButton.getStyleClass().addAll("danger-button", "small-button");

        conditionsPanel.getChildren().addAll(
                conditionsLabel, conditionBuilder, conditionsList, removeConditionButton);
    }

    private VBox createRightPanel() {
        VBox rightPanel = new VBox(10);
        rightPanel.setPadding(new Insets(10));
        rightPanel.getStyleClass().add("panel");

        // Query options
        Label optionsLabel = new Label("Query Options");
        optionsLabel.getStyleClass().add("panel-header");

        VBox optionsBox = new VBox(8);
        optionsBox.getChildren().addAll(
                distinctCheckBox,
                createLimitSection(),
                createOrderBySection(),
                createGroupBySection());

        rightPanel.getChildren().addAll(optionsLabel, optionsBox);

        return rightPanel;
    }

    private void createQueryPreview() {
        queryPreview = new TextArea();
        queryPreview.setEditable(false);
        queryPreview.setPromptText("Query will be generated here...");
        queryPreview.getStyleClass().add("code-area");
        queryPreview.setPrefRowCount(10);
    }

    private VBox createLimitSection() {
        VBox limitSection = new VBox(3);
        Label limitLabel = new Label("Limit Results:");
        limitLabel.getStyleClass().add("form-label");
        limitField.getStyleClass().add("form-field");
        limitSection.getChildren().addAll(limitLabel, limitField);
        return limitSection;
    }

    private VBox createOrderBySection() {
        VBox orderSection = new VBox(3);
        Label orderLabel = new Label("Order By:");
        orderLabel.getStyleClass().add("form-label");

        HBox orderBox = new HBox(5);
        orderByField.getStyleClass().add("form-field");
        orderDirectionCombo.getStyleClass().add("form-field");
        HBox.setHgrow(orderByField, Priority.ALWAYS);
        orderBox.getChildren().addAll(orderByField, orderDirectionCombo);

        orderSection.getChildren().addAll(orderLabel, orderBox);
        return orderSection;
    }

    private VBox createGroupBySection() {
        VBox groupSection = new VBox(3);
        Label groupLabel = new Label("Group By:");
        groupLabel.getStyleClass().add("form-label");

        ComboBox<String> groupByField = new ComboBox<>();
        groupByField.setPromptText("Select field to group by");
        groupByField.getStyleClass().add("form-field");

        groupSection.getChildren().addAll(groupLabel, groupByField);
        return groupSection;
    }

    private HBox createButtonBar() {
        HBox buttonBar = new HBox(10);
        buttonBar.setAlignment(Pos.CENTER_RIGHT);

        Button generateButton = new Button("Generate Query");
        generateButton.setGraphic(new FontIcon(Material2AL.BUILD));
        generateButton.getStyleClass().add("primary-button");
        generateButton.setOnAction(e -> generateQuery());

        Button executeButton = new Button("Execute Query");
        executeButton.setGraphic(new FontIcon(Material2MZ.PLAY_ARROW));
        executeButton.getStyleClass().add("success-button");
        executeButton.setOnAction(e -> executeQuery());

        Button resetButton = new Button("Reset");
        resetButton.setGraphic(new FontIcon(Material2MZ.REFRESH));
        resetButton.getStyleClass().add("secondary-button");
        resetButton.setOnAction(e -> resetBuilder());

        Button closeButton = new Button("Close");
        closeButton.setGraphic(new FontIcon(Material2AL.CLOSE));
        closeButton.getStyleClass().add("secondary-button");
        closeButton.setOnAction(e -> close());

        buttonBar.getChildren().addAll(generateButton, executeButton, resetButton, closeButton);

        return buttonBar;
    }

    private void setupEventHandlers() {
        // Query type change
        queryTypeCombo.setOnAction(e -> updateUIForQueryType());

        // Auto-generate query on changes
        selectedFieldsList.getItems().addListener((javafx.collections.ListChangeListener<String>) c -> generateQuery());
        conditionsList.getItems().addListener((javafx.collections.ListChangeListener<String>) c -> generateQuery());
        distinctCheckBox.setOnAction(e -> generateQuery());
        limitField.textProperty().addListener((obs, oldText, newText) -> generateQuery());
        orderByField.setOnAction(e -> generateQuery());
        orderDirectionCombo.setOnAction(e -> generateQuery());
    }

    private void updateUIForQueryType() {
        String queryType = queryTypeCombo.getValue();
        // Update UI based on query type
        boolean isSelect = "SELECT".equals(queryType);

        distinctCheckBox.setDisable(!isSelect);
        limitField.setDisable(!isSelect);
        orderByField.setDisable(!isSelect);
        orderDirectionCombo.setDisable(!isSelect);

        generateQuery();
    }

    private void generateQuery() {
        String queryType = queryTypeCombo.getValue();
        StringBuilder query = new StringBuilder();

        switch (queryType) {
            case "SELECT":
                generateSelectQuery(query);
                break;
            case "INSERT":
                generateInsertQuery(query);
                break;
            case "UPDATE":
                generateUpdateQuery(query);
                break;
            case "DELETE":
                generateDeleteQuery(query);
                break;
        }

        Platform.runLater(() -> queryPreview.setText(query.toString()));
    }

    private void generateSelectQuery(StringBuilder query) {
        query.append("SELECT ");

        if (distinctCheckBox.isSelected()) {
            query.append("DISTINCT ");
        }

        // Fields
        if (selectedFieldsList.getItems().isEmpty()) {
            query.append("*");
        } else {
            query.append(String.join(", ", selectedFieldsList.getItems()));
        }

        // FROM clause
        if (currentTable != null) {
            query.append("\nFROM ").append(currentTable);
        } else {
            query.append("\nFROM table_name");
        }

        // WHERE clause
        if (!conditionsList.getItems().isEmpty()) {
            query.append("\nWHERE ");
            query.append(String.join("\n  ", conditionsList.getItems()));
        }

        // ORDER BY clause
        if (orderByField.getValue() != null && !orderByField.getValue().trim().isEmpty()) {
            query.append("\nORDER BY ").append(orderByField.getValue());
            query.append(" ").append(orderDirectionCombo.getValue());
        }

        // LIMIT clause
        if (!limitField.getText().trim().isEmpty()) {
            try {
                Integer.parseInt(limitField.getText().trim());
                query.append("\nLIMIT ").append(limitField.getText().trim());
            } catch (NumberFormatException e) {
                // Invalid limit value, skip
            }
        }

        query.append(";");
    }

    private void generateInsertQuery(StringBuilder query) {
        query.append("INSERT INTO ");
        if (currentTable != null) {
            query.append(currentTable);
        } else {
            query.append("table_name");
        }

        if (!selectedFieldsList.getItems().isEmpty()) {
            query.append(" (");
            query.append(String.join(", ", selectedFieldsList.getItems()));
            query.append(")");
        }

        query.append("\nVALUES (");
        if (!selectedFieldsList.getItems().isEmpty()) {
            String[] placeholders = new String[selectedFieldsList.getItems().size()];
            Arrays.fill(placeholders, "?");
            query.append(String.join(", ", placeholders));
        } else {
            query.append("value1, value2, ...");
        }
        query.append(");");
    }

    private void generateUpdateQuery(StringBuilder query) {
        query.append("UPDATE ");
        if (currentTable != null) {
            query.append(currentTable);
        } else {
            query.append("table_name");
        }

        query.append("\nSET ");
        if (!selectedFieldsList.getItems().isEmpty()) {
            List<String> setClause = selectedFieldsList.getItems().stream()
                    .map(field -> field + " = ?")
                    .toList();
            query.append(String.join(", ", setClause));
        } else {
            query.append("field1 = value1, field2 = value2");
        }

        // WHERE clause
        if (!conditionsList.getItems().isEmpty()) {
            query.append("\nWHERE ");
            query.append(String.join("\n  ", conditionsList.getItems()));
        } else {
            query.append("\nWHERE condition");
        }

        query.append(";");
    }

    private void generateDeleteQuery(StringBuilder query) {
        query.append("DELETE FROM ");
        if (currentTable != null) {
            query.append(currentTable);
        } else {
            query.append("table_name");
        }

        // WHERE clause
        if (!conditionsList.getItems().isEmpty()) {
            query.append("\nWHERE ");
            query.append(String.join("\n  ", conditionsList.getItems()));
        } else {
            query.append("\nWHERE condition");
        }

        query.append(";");
    }

    private void executeQuery() {
        if (onQueryGenerated != null) {
            onQueryGenerated.accept(queryPreview.getText());
            close();
        }
    }

    private void resetBuilder() {
        selectedFieldsList.getItems().clear();
        conditionsList.getItems().clear();
        distinctCheckBox.setSelected(false);
        limitField.clear();
        orderByField.setValue(null);
        orderDirectionCombo.setValue("ASC");
        queryTypeCombo.setValue("SELECT");
        queryPreview.clear();
    }

    private void initializeWithTable(String database, String table) {
        this.currentDatabase = database;
        this.currentTable = table;

        // TODO: Load table fields and populate the available fields list
        // This would typically involve calling the connection manager to get column
        // information
        Platform.runLater(() -> {
            // Sample fields - replace with actual field loading
            orderByField.getItems().addAll("id", "name", "created_at", "updated_at");
            generateQuery();
        });
    }

    public void setOnQueryGenerated(Consumer<String> callback) {
        this.onQueryGenerated = callback;
    }
}
