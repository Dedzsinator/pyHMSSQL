package com.pyhmssql.client.utils;

import com.pyhmssql.client.model.QueryHistoryItem;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.prefs.Preferences;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Manages query history for the application
 */
public class QueryHistoryManager {
    private static final int MAX_HISTORY_SIZE = 50;
    private static final String PREF_KEY = "query_history";
    
    private final List<QueryHistoryItem> queryHistory;
    private final Preferences prefs;
    private final ObjectMapper objectMapper;
    
    /**
     * Create a new query history manager
     */
    public QueryHistoryManager() {
        prefs = Preferences.userNodeForPackage(QueryHistoryManager.class);
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        queryHistory = loadHistory();
    }
    
    /**
     * Add a query to the history
     * @param sql SQL query text
     * @param database Database the query was executed on
     * @param successful Whether execution was successful
     * @param executionTime Execution time in milliseconds
     */
    public void addQuery(String sql, String database, boolean successful, long executionTime) {
        QueryHistoryItem item = new QueryHistoryItem(
            sql, 
            database,
            LocalDateTime.now(),
            successful,
            executionTime
        );
        
        // Remove duplicates of the same query
        queryHistory.removeIf(h -> h.getQuery().equals(sql));
        
        // Add to the start of the list
        queryHistory.add(0, item);
        
        // Trim to maximum size
        if (queryHistory.size() > MAX_HISTORY_SIZE) {
            queryHistory.subList(MAX_HISTORY_SIZE, queryHistory.size()).clear();
        }
        
        // Save to preferences
        saveHistory();
    }
    
    /**
     * Get the query history
     * @return List of QueryHistoryItem
     */
    public List<QueryHistoryItem> getHistory() {
        return new ArrayList<>(queryHistory);
    }
    
    /**
     * Clear the query history
     */
    public void clearHistory() {
        queryHistory.clear();
        saveHistory();
    }
    
    /**
     * Save history to preferences
     */
    private void saveHistory() {
        try {
            String json = objectMapper.writeValueAsString(queryHistory);
            prefs.put(PREF_KEY, json);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Load history from preferences
     * @return List of QueryHistoryItem
     */
    private List<QueryHistoryItem> loadHistory() {
        try {
            String json = prefs.get(PREF_KEY, "[]");
            CollectionType type = objectMapper.getTypeFactory()
                .constructCollectionType(List.class, QueryHistoryItem.class);
            return objectMapper.readValue(json, type);
        } catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }
}