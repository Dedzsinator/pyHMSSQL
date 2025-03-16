package model;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Model class representing a query history item
 */
public class QueryHistoryItem {
    private final String query;
    private final String database;
    private final LocalDateTime timestamp;
    private final boolean successful;
    private final long executionTimeMs;
    
    public QueryHistoryItem(String query, String database) {
        this(query, database, LocalDateTime.now(), true, 0);
    }
    
    public QueryHistoryItem(String query, String database, 
                            LocalDateTime timestamp, boolean successful, 
                            long executionTimeMs) {
        this.query = query;
        this.database = database;
        this.timestamp = timestamp;
        this.successful = successful;
        this.executionTimeMs = executionTimeMs;
    }
    
    public String getQuery() {
        return query;
    }
    
    public String getDatabase() {
        return database;
    }
    
    public LocalDateTime getTimestamp() {
        return timestamp;
    }
    
    public boolean isSuccessful() {
        return successful;
    }
    
    public long getExecutionTimeMs() {
        return executionTimeMs;
    }
    
    /**
     * Returns a shortened version of the query suitable for display in history lists
     */
    public String getShortQuery() {
        // Limit to 50 characters and trim whitespace
        String shortQuery = query.replaceAll("\\s+", " ").trim();
        return shortQuery.length() > 50 ? 
            shortQuery.substring(0, 47) + "..." : 
            shortQuery;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryHistoryItem that = (QueryHistoryItem) o;
        return Objects.equals(query, that.query) && 
               Objects.equals(timestamp, that.timestamp);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(query, timestamp);
    }
    
    @Override
    public String toString() {
        return getShortQuery() + " [" + database + "] - " + timestamp;
    }
}