package com.pyhmssql.client.main;

import com.pyhmssql.client.views.VisualQueryBuilder;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;

/**
 * Wrapper class for VisualQueryBuilder to manage query builder tabs
 */
public class QueryBuilder {
    private final TabPane tabPane;
    private final ConnectionManager connectionManager;
    
    public QueryBuilder(TabPane tabPane, ConnectionManager connectionManager) {
        this.tabPane = tabPane;
        this.connectionManager = connectionManager;
    }
    
    public void openQueryBuilder() {
        Tab builderTab = new Tab("Query Builder");
        builderTab.setClosable(true);
        
        VisualQueryBuilder visualBuilder = new VisualQueryBuilder(connectionManager, sql -> {
            // Open the generated SQL in a new query tab
            openSqlInNewTab(sql);
        });
        
        builderTab.setContent(visualBuilder);
        tabPane.getTabs().add(builderTab);
        tabPane.getSelectionModel().select(builderTab);
    }
    
    private void openSqlInNewTab(String sql) {
        // This method would be implemented to create a new query tab with the SQL
        // and is called when the query builder generates SQL that should be edited
    }
}