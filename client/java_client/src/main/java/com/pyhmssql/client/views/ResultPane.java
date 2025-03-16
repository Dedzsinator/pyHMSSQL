package views;

import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

public class ResultPane extends BorderPane {
    private TableView<ObservableList<String>> tableView;
    private TextArea messageArea;
    private TabPane tabPane;
    
    public ResultPane() {
        // Set up the UI
        tabPane = new TabPane();
        
        // Results tab
        tableView = new TableView<>();
        tableView.setPlaceholder(new Label("No data to display"));
        
        // Messages tab
        messageArea = new TextArea();
        messageArea.setEditable(false);
        
        // Create tabs
        Tab resultsTab = new Tab("Results", new BorderPane(tableView));
        resultsTab.setClosable(false);
        
        Tab messagesTab = new Tab("Messages", new BorderPane(messageArea));
        messagesTab.setClosable(false);
        
        tabPane.getTabs().addAll(resultsTab, messagesTab);
        
        setCenter(tabPane);
    }
    
    public void displayResults(Map<String, Object> result) {
        if (result == null) {
            showMessage("No result returned from server");
            return;
        }
        
        if (result.containsKey("error")) {
            showError((String) result.get("error"));
            return;
        }
        
        // Check if this is a result set or a message
        if (result.containsKey("rows")) {
            displayResultSet(result);
        } else if (result.containsKey("response")) {
            showMessage((String) result.get("response"));
        } else {
            showMessage("Unknown response format: " + result);
        }
    }
    
    private void displayResultSet(Map<String, Object> result) {
        List<Map<String, Object>> rows = (List<Map<String, Object>>) result.get("rows");
        List<String> columns = (List<String>) result.get("columns");
        
        if (rows == null || columns == null || rows.isEmpty()) {
            showMessage("Query executed successfully. No results to display.");
            return;
        }
        
        // Clear existing data
        tableView.getColumns().clear();
        tableView.getItems().clear();
        
        // Add columns
        for (String column : columns) {
            TableColumn<ObservableList<String>, String> tableColumn = new TableColumn<>(column);
            final int columnIndex = tableView.getColumns().size();
            
            tableColumn.setCellValueFactory(param -> {
                ObservableList<String> row = param.getValue();
                if (row == null || columnIndex >= row.size()) {
                    return null;
                }
                return new SimpleStringProperty(row.get(columnIndex));
            });
            
            tableView.getColumns().add(tableColumn);
        }
        
        // Add rows
        for (Map<String, Object> row : rows) {
            ObservableList<String> observableRow = FXCollections.observableArrayList();
            
            for (String column : columns) {
                Object value = row.get(column);
                observableRow.add(value != null ? value.toString() : "NULL");
            }
            
            tableView.getItems().add(observableRow);
        }
        
        // Show the results tab
        tabPane.getSelectionModel().select(0);
        
        // Show summary message
        showMessage("Query executed successfully. " + rows.size() + " rows returned.");
    }
    
    private void showMessage(String message) {
        messageArea.appendText(message + "\n");
        tabPane.getSelectionModel().select(1);
    }
    
    private void showError(String error) {
        messageArea.appendText("ERROR: " + error + "\n");
        tabPane.getSelectionModel().select(1);
    }
    
    public void clear() {
        tableView.getColumns().clear();
        tableView.getItems().clear();
        messageArea.clear();
    }
}