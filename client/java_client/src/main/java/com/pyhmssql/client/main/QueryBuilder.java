package com.pyhmssql.client.main;

import com.pyhmssql.client.views.QueryEditor;
import com.pyhmssql.client.views.ResultPane;
import com.pyhmssql.client.views.VisualQueryBuilder;
import javafx.scene.control.SplitPane;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.geometry.Orientation;

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
        // Create a new query tab with the generated SQL
        Tab queryTab = new Tab("Generated Query");
        queryTab.setClosable(true);

        // Create a split pane for query editor and results
        SplitPane splitPane = new SplitPane();
        splitPane.setOrientation(Orientation.VERTICAL);

        // Create the query editor with the generated SQL
        QueryEditor queryEditor = new QueryEditor(connectionManager);
        queryEditor.setQuery(sql);

        // Create a results pane
        ResultPane resultPane = new ResultPane();

        // Connect the query editor to the results pane
        queryEditor.setOnExecuteQuery(resultPane::displayResults);

        // Add components to the split pane
        splitPane.getItems().addAll(new org.fxmisc.flowless.VirtualizedScrollPane<>(queryEditor.getCodeArea()),
                resultPane);
        splitPane.setDividerPositions(0.6);

        // Set as tab content
        queryTab.setContent(splitPane);

        // Add and select the new tab
        tabPane.getTabs().add(queryTab);
        tabPane.getSelectionModel().select(queryTab);
    }
}