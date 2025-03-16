package main;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

public class ConnectionManager {
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 9999;
    private static final int TIMEOUT_MS = 5000;
    
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private ObjectMapper mapper = new ObjectMapper();
    private String sessionId;
    private String role;
    private String currentDatabase;
    
    // Connection event listeners
    private List<ConnectionListener> listeners = new ArrayList<>();
    
    public CompletableFuture<Map<String, Object>> connect(String username, String password) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (socket == null || socket.isClosed()) {
                    socket = new Socket();
                    socket.connect(new InetSocketAddress(DEFAULT_HOST, DEFAULT_PORT), TIMEOUT_MS);
                    out = new PrintWriter(socket.getOutputStream(), true);
                    in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                }
                
                Map<String, String> request = Map.of(
                    "action", "login",
                    "username", username,
                    "password", password
                );
                
                String jsonRequest = mapper.writeValueAsString(request);
                out.println(jsonRequest);
                
                String response = in.readLine();
                Map<String, Object> result = mapper.readValue(response, Map.class);
                
                if (result.containsKey("session_id")) {
                    sessionId = (String) result.get("session_id");
                    role = (String) result.get("role");
                    notifyConnectionEstablished();
                }
                
                return result;
            } catch (IOException e) {
                e.printStackTrace();
                return Map.of("error", "Connection failed: " + e.getMessage());
            }
        });
    }
    
    public CompletableFuture<Map<String, Object>> executeQuery(String query) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (socket == null || socket.isClosed() || sessionId == null) {
                    return Map.of("error", "Not connected. Please login first.");
                }
                
                Map<String, Object> request = new HashMap<>();
                request.put("action", "query");
                request.put("session_id", sessionId);
                request.put("query", query);
                
                if (currentDatabase != null) {
                    request.put("database", currentDatabase);
                }
                
                String jsonRequest = mapper.writeValueAsString(request);
                out.println(jsonRequest);
                
                String response = in.readLine();
                return mapper.readValue(response, Map.class);
            } catch (IOException e) {
                e.printStackTrace();
                return Map.of("error", "Query execution failed: " + e.getMessage());
            }
        });
    }
    
    public CompletableFuture<Map<String, Object>> getDatabases() {
        return executeQuery("SHOW DATABASES");
    }
    
    public CompletableFuture<Map<String, Object>> getTables(String database) {
        return executeQuery("SHOW TABLES FROM " + database);
    }
    
    public CompletableFuture<Map<String, Object>> getColumns(String database, String table) {
        return executeQuery("SHOW COLUMNS FROM " + database + "." + table);
    }
    
    public CompletableFuture<Map<String, Object>> createDatabase(String dbName) {
        return executeQuery("CREATE DATABASE " + dbName);
    }
    
    public CompletableFuture<Map<String, Object>> dropDatabase(String dbName) {
        return executeQuery("DROP DATABASE " + dbName);
    }
    
    public CompletableFuture<Map<String, Object>> createTable(String dbName, String tableName, Map<String, String> columns) {
        StringBuilder query = new StringBuilder("CREATE TABLE ");
        query.append(dbName).append(".").append(tableName).append(" (");
        
        boolean first = true;
        for (Map.Entry<String, String> column : columns.entrySet()) {
            if (!first) {
                query.append(", ");
            }
            query.append(column.getKey()).append(" ").append(column.getValue());
            first = false;
        }
        query.append(")");
        
        return executeQuery(query.toString());
    }
    
    public void setCurrentDatabase(String database) {
        this.currentDatabase = database;
    }
    
    public String getCurrentDatabase() {
        return currentDatabase;
    }
    
    public String getRole() {
        return role;
    }
    
    public boolean isConnected() {
        return socket != null && !socket.isClosed() && sessionId != null;
    }
    
    public void addConnectionListener(ConnectionListener listener) {
        listeners.add(listener);
    }
    
    public void removeConnectionListener(ConnectionListener listener) {
        listeners.remove(listener);
    }
    
    private void notifyConnectionEstablished() {
        for (ConnectionListener listener : listeners) {
            listener.onConnectionEstablished();
        }
    }
    
    private void notifyConnectionClosed() {
        for (ConnectionListener listener : listeners) {
            listener.onConnectionClosed();
        }
    }
    
    public void disconnect() {
        try {
            if (socket != null && !socket.isClosed()) {
                if (sessionId != null) {
                    Map<String, String> request = Map.of(
                        "action", "logout",
                        "session_id", sessionId
                    );
                    
                    String jsonRequest = mapper.writeValueAsString(request);
                    out.println(jsonRequest);
                    sessionId = null;
                    role = null;
                }
                
                socket.close();
                notifyConnectionClosed();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    // Connection listener interface
    public interface ConnectionListener {
        void onConnectionEstablished();
        void onConnectionClosed();
    }
}