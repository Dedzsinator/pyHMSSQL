package com.pyhmssql.client.main;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Manages the connection to the pyHMSSQL server
 */
public class ConnectionManager {
    private Socket socket;
    private PrintWriter writer;
    private BufferedReader reader;
    private String sessionId;
    private String currentDatabase;
    private final ObjectMapper objectMapper;
    private final List<ConnectionListener> listeners = new ArrayList<>();
    private String serverHost = "localhost";
    private int serverPort = 9999;
    private boolean debugMode = true; // Enable debug output

    // Store credentials for reconnection
    private String storedUsername;
    private String storedPassword;
    private boolean autoReconnect = true;

    // Server discovery constants
    private static final int DISCOVERY_PORT = 9998;
    private static final int DISCOVERY_TIMEOUT = 3000; // 3 seconds

    private DatagramSocket discoverySocket;
    private boolean isDiscovering = false;
    private final Map<String, ServerInfo> availableServers = new HashMap<>();

    // Inner class to hold server information
    public static class ServerInfo {
        private String host;
        private int port;
        private String name;
        private long lastSeen;

        public ServerInfo(String host, int port, String name) {
            this.host = host;
            this.port = port;
            this.name = name;
            this.lastSeen = System.currentTimeMillis();
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public String getName() {
            return name;
        }

        public long getLastSeen() {
            return lastSeen;
        }

        public void setLastSeen(long time) {
            this.lastSeen = time;
        }

        @Override
        public String toString() {
            return name + " (" + host + ":" + port + ")";
        }
    }

    public interface ConnectionListener {
        void onConnectionStatusChanged(boolean connected);
    }

    public ConnectionManager() {
        this.objectMapper = new ObjectMapper();
    }

    public void addConnectionListener(ConnectionListener listener) {
        listeners.add(listener);
    }

    /**
     * Start discovering HMSSQL servers on the network
     * 
     * @return CompletableFuture that completes when discovery is finished
     */
    public CompletableFuture<List<ServerInfo>> discoverServers() {
        CompletableFuture<List<ServerInfo>> future = new CompletableFuture<>();

        // Clear existing servers
        availableServers.clear();

        // Create and start discovery thread
        Thread discoveryThread = new Thread(() -> {
            try {
                // Create UDP socket for discovery
                discoverySocket = new DatagramSocket(DISCOVERY_PORT);
                discoverySocket.setSoTimeout(500); // Short timeout for responsive stopping
                isDiscovering = true;

                byte[] buffer = new byte[4096];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                long startTime = System.currentTimeMillis();

                while (isDiscovering && System.currentTimeMillis() - startTime < DISCOVERY_TIMEOUT) {
                    try {
                        discoverySocket.receive(packet);
                        String message = new String(packet.getData(), 0, packet.getLength());

                        try {
                            // Parse server info from JSON
                            Map<String, Object> serverInfo = objectMapper.readValue(message,
                                    new TypeReference<Map<String, Object>>() {
                                    });

                            if ("HMSSQL".equals(serverInfo.get("service"))) {
                                String host = (String) serverInfo.get("host");
                                int port = (Integer) serverInfo.get("port");
                                String name = (String) serverInfo.get("name");

                                // Use host:port as key
                                String key = host + ":" + port;

                                synchronized (availableServers) {
                                    if (availableServers.containsKey(key)) {
                                        availableServers.get(key).setLastSeen(System.currentTimeMillis());
                                    } else {
                                        availableServers.put(key, new ServerInfo(host, port, name));
                                    }
                                }
                            }
                        } catch (IOException e) {
                            System.err.println("Error parsing server info: " + e.getMessage());
                        }
                    } catch (SocketTimeoutException e) {
                        // This is expected with the short timeout
                    }
                }

                // Complete the future with available servers
                synchronized (availableServers) {
                    future.complete(new ArrayList<>(availableServers.values()));
                }

            } catch (Exception e) {
                future.completeExceptionally(e);
            } finally {
                stopDiscovery();
            }
        });

        discoveryThread.setDaemon(true);
        discoveryThread.start();

        return future;
    }

    /**
     * Stop server discovery
     */
    public void stopDiscovery() {
        isDiscovering = false;
        if (discoverySocket != null && !discoverySocket.isClosed()) {
            discoverySocket.close();
            discoverySocket = null;
        }
    }

    public CompletableFuture<Map<String, Object>> showAllTables() {
        Map<String, Object> request = new HashMap<>();
        request.put("action", "query");
        request.put("type", "query"); // Explicitly set type field
        request.put("query", "SHOW ALL_TABLES");

        return sendRequest(request)
                .thenApply(response -> {
                    // If the response already has the columns/rows format, we're good
                    if (response.containsKey("columns") && response.containsKey("rows")) {
                        return response;
                    }

                    // If we got a tree structure, convert it to a flattened table format
                    if (response.containsKey("tree") && response.get("tree") instanceof List) {
                        List<Map<String, Object>> tree = (List<Map<String, Object>>) response.get("tree");
                        List<List<String>> rows = new ArrayList<>();

                        // Flatten the tree structure into rows
                        for (Map<String, Object> database : tree) {
                            String dbName = (String) database.get("name");
                            if (database.containsKey("children")) {
                                List<Map<String, Object>> tables = (List<Map<String, Object>>) database.get("children");
                                for (Map<String, Object> table : tables) {
                                    List<String> row = new ArrayList<>();
                                    row.add(dbName);
                                    row.add((String) table.get("name"));
                                    rows.add(row);
                                }
                            }
                        }

                        // Create a new response with table format that client can understand
                        Map<String, Object> tableResponse = new HashMap<>();
                        tableResponse.put("columns", Arrays.asList("DATABASE_NAME", "TABLE_NAME"));
                        tableResponse.put("rows", rows);
                        tableResponse.put("status", "success");
                        return tableResponse;
                    }
                    return response;
                });
    }

    /**
     * Get list of available servers from last discovery
     * 
     * @return List of server info objects
     */
    public List<ServerInfo> getAvailableServers() {
        synchronized (availableServers) {
            return new ArrayList<>(availableServers.values());
        }
    }

    /**
     * Try to connect to the first available server
     * 
     * @param username Username for login
     * @param password Password for login
     * @return CompletableFuture with connection result
     */
    public CompletableFuture<Map<String, Object>> connectToFirstAvailableServer(String username, String password) {
        CompletableFuture<Map<String, Object>> result = new CompletableFuture<>();

        discoverServers().thenAccept(servers -> {
            if (servers.isEmpty()) {
                Map<String, Object> error = new HashMap<>();
                error.put("error", "No servers found");
                result.complete(error);
                return;
            }

            // Use the first server
            ServerInfo server = servers.get(0);
            setServerDetails(server.getHost(), server.getPort());

            // Connect to the server
            connect(username, password).thenAccept(response -> {
                result.complete(response);
            });
        }).exceptionally(ex -> {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Server discovery failed: " + ex.getMessage());
            result.complete(error);
            return null;
        });

        return result;
    }

    /**
     * Get all user preferences
     * 
     * @return CompletableFuture containing the preferences
     */
    public CompletableFuture<Map<String, Object>> getPreferences() {
        CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();

        try {
            // First check if we have a valid connection
            if (socket == null || !socket.isConnected()) {
                Map<String, Object> defaultPrefs = new HashMap<>();
                defaultPrefs.put("preferences", new HashMap<>());
                future.complete(defaultPrefs);
                return future;
            }

            // Otherwise execute the query
            executeQuery("GET PREFERENCES")
                    .thenAccept(result -> future.complete(result))
                    .exceptionally(ex -> {
                        // If there's an error, return empty preferences
                        Map<String, Object> defaultPrefs = new HashMap<>();
                        defaultPrefs.put("preferences", new HashMap<>());
                        future.complete(defaultPrefs);
                        return null;
                    });
        } catch (Exception e) {
            // Handle any exceptions
            Map<String, Object> defaultPrefs = new HashMap<>();
            defaultPrefs.put("preferences", new HashMap<>());
            future.complete(defaultPrefs);
        }

        return future;
    }

    /**
     * Update multiple preferences at once
     * 
     * @param preferences Map of preference key-value pairs to update
     * @return CompletableFuture containing the result
     */
    public CompletableFuture<Map<String, Object>> updatePreferences(Map<String, Object> preferences) {
        CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();

        try {
            // Check if we have a valid connection
            if (socket == null || !socket.isConnected()) {
                Map<String, Object> error = new HashMap<>();
                error.put("error", "Not connected to server");
                future.complete(error);
                return future;
            }

            // Create a chain of preference update operations
            CompletableFuture<Map<String, Object>> chain = CompletableFuture.completedFuture(new HashMap<>());

            for (Map.Entry<String, Object> entry : preferences.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                // Build each link in the chain
                chain = chain.thenCompose(prev -> {
                    String valueStr = value.toString();
                    // If string value, add quotes
                    if (value instanceof String && !valueStr.startsWith("'") && !valueStr.endsWith("'")) {
                        valueStr = "'" + valueStr + "'";
                    }
                    return setPreference(key, valueStr);
                });
            }

            // After all preferences are set, return success
            chain.thenAccept(result -> {
                Map<String, Object> response = new HashMap<>();
                response.put("message", "Preferences updated successfully");
                future.complete(response);
            }).exceptionally(ex -> {
                Map<String, Object> errorResult = new HashMap<>();
                errorResult.put("error", "Error updating preferences: " + ex.getMessage());
                future.complete(errorResult);
                return null;
            });
        } catch (Exception e) {
            // Handle any exceptions
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Error updating preferences: " + e.getMessage());
            future.complete(error);
        }

        return future;
    }

    // Send data using the same protocol as Python's send_data
    private void sendDataToServer(Socket socket, Map<String, Object> data) throws IOException {
        // Add type field to match server expectations (same as action)
        if (data.containsKey("action") && !data.containsKey("type")) {
            data.put("type", data.get("action"));
        }

        // Convert data to JSON
        String jsonData = objectMapper.writeValueAsString(data);
        byte[] jsonBytes = jsonData.getBytes("UTF-8");

        if (debugMode) {
            System.out.println("[DEBUG] Sending data to server: " + jsonData);
        }

        // Use buffered streams for more reliable transmission
        BufferedOutputStream bos = new BufferedOutputStream(socket.getOutputStream());

        // Send the JSON data directly without length prefixing
        bos.write(jsonBytes);
        bos.flush();

        if (debugMode) {
            System.out.println("[DEBUG] Data sent successfully");
        }
    }

    // Receive data using the same protocol as Python's receive_data
    private Map<String, Object> receiveDataFromServer(Socket socket) throws IOException {
        try {
            // Use buffered stream for more reliable reception
            BufferedInputStream bis = new BufferedInputStream(socket.getInputStream());
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();

            // Read data in chunks until no more is available
            byte[] chunk = new byte[4096];
            int bytesRead;

            while ((bytesRead = bis.read(chunk)) != -1) {
                buffer.write(chunk, 0, bytesRead);
                if (bytesRead < chunk.length) {
                    break; // End of message when we get less than a full chunk
                }
            }

            byte[] responseData = buffer.toByteArray();

            if (responseData.length == 0) {
                throw new IOException("No data received from server");
            }

            // Convert JSON to Map
            String jsonData = new String(responseData, "UTF-8");

            if (debugMode) {
                System.out.println("[DEBUG] Received JSON from server: " + jsonData);
            }

            return objectMapper.readValue(jsonData, new TypeReference<Map<String, Object>>() {
            });
        } catch (IOException e) {
            System.err.println("Error receiving data: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Attempt to silently reconnect to the server using stored credentials
     * 
     * @return CompletableFuture with reconnection result
     */
    private synchronized CompletableFuture<Boolean> reconnect() {
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        if (storedUsername == null || storedPassword == null) {
            if (debugMode) {
                System.out.println("[DEBUG] No stored credentials for reconnection");
            }
            future.complete(false);
            return future;
        }

        if (debugMode) {
            System.out.println("[DEBUG] Attempting to reconnect to server");
        }

        // Create a new socket for reconnection
        try {
            // Close any existing socket
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }

            // Create login request
            Map<String, Object> request = new HashMap<>();
            request.put("action", "login");
            request.put("type", "login");
            request.put("username", storedUsername);
            request.put("password", storedPassword);

            // Create a new socket
            socket = new Socket();
            socket.setSoTimeout(15000);
            socket.connect(new InetSocketAddress(serverHost, serverPort), 5000);

            if (debugMode) {
                System.out.println("[DEBUG] Reconnection socket created successfully");
            }

            // Set up streams
            writer = new PrintWriter(socket.getOutputStream(), true);
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            // Send login request
            sendDataToServer(socket, request);

            // Receive response
            Map<String, Object> response = receiveDataFromServer(socket);

            if (response.containsKey("session_id")) {
                sessionId = (String) response.get("session_id");
                if (debugMode) {
                    System.out.println("[DEBUG] Reconnection successful, new session_id: " + sessionId);
                }
                notifyListeners(true);
                future.complete(true);
            } else {
                if (debugMode) {
                    System.out.println("[DEBUG] Reconnection failed: " + response.get("error"));
                }
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                    socket = null;
                }
                notifyListeners(false);
                future.complete(false);
            }
        } catch (Exception e) {
            if (debugMode) {
                System.out.println("[DEBUG] Reconnection error: " + e.getMessage());
            }
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                    socket = null;
                }
            } catch (IOException ioe) {
                System.err.println("Error closing socket during reconnection: " + ioe.getMessage());
            }
            notifyListeners(false);
            future.complete(false);
        }

        return future;
    }

    private CompletableFuture<Map<String, Object>> sendRequest(Map<String, Object> request) {
        CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();

        // Create a new thread to handle the request asynchronously
        new Thread(() -> {
            try {
                // Add session ID if available
                if (sessionId != null) {
                    request.put("session_id", sessionId);
                }

                // Add type field to match server expectations (same as action)
                if (request.containsKey("action") && !request.containsKey("type")) {
                    request.put("type", request.get("action"));
                }

                if (debugMode) {
                    System.out.println("[DEBUG] Preparing to send request: " + request);
                }

                // Check if we need to reconnect
                boolean needsConnection = (socket == null || !socket.isConnected() || socket.isClosed());

                if (needsConnection && autoReconnect && storedUsername != null) {
                    if (debugMode) {
                        System.out.println("[DEBUG] Connection lost, attempting reconnection");
                    }

                    // Try to reconnect
                    Boolean reconnected = reconnect().get(); // Wait for reconnection

                    if (!reconnected) {
                        throw new IOException("Not connected to server. Automatic reconnection failed.");
                    }
                } else if (needsConnection) {
                    throw new IOException("Not connected to server. Please log in first.");
                }

                // Create a new socket for each request (stateless protocol)
                Socket requestSocket = new Socket();
                requestSocket.setSoTimeout(15000);
                requestSocket.connect(new InetSocketAddress(serverHost, serverPort), 5000);

                if (debugMode) {
                    System.out.println("[DEBUG] Request socket connected successfully");
                }

                // Use the temporary connection for this request
                sendDataToServer(requestSocket, request);

                // Receive and process response
                Map<String, Object> response = receiveDataFromServer(requestSocket);

                // Close the socket after the request is complete
                requestSocket.close();

                future.complete(response);
            } catch (Exception e) {
                System.err.println("Request error:");
                e.printStackTrace();
                Map<String, Object> error = new HashMap<>();
                error.put("error", "Request error: " + e.getMessage());
                future.complete(error);

                // If connection issues occurred, notify listeners
                if (e instanceof SocketException || e instanceof IOException) {
                    notifyListeners(false);
                }
            }
        }).start();

        return future;
    }

    public CompletableFuture<Map<String, Object>> connect(String username, String password) {
        CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();

        // Store credentials for reconnection
        storedUsername = username;
        storedPassword = password;

        // Create a new thread to handle the connection asynchronously
        new Thread(() -> {
            try {
                System.out.println("[DEBUG] Attempting to connect to: " + serverHost + ":" + serverPort);

                // Create login request
                Map<String, Object> request = new HashMap<>();
                request.put("action", "login");
                request.put("type", "login");
                request.put("username", username);
                request.put("password", password);

                if (debugMode) {
                    System.out.println("[DEBUG] Login request payload: " + objectMapper.writeValueAsString(request));
                }

                // Create a new socket
                socket = new Socket();
                socket.setSoTimeout(15000);
                socket.connect(new InetSocketAddress(serverHost, serverPort), 5000);

                if (debugMode) {
                    System.out.println("[DEBUG] Socket connected successfully.");
                }

                // Set up streams
                writer = new PrintWriter(socket.getOutputStream(), true);
                reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                // Send login request
                sendDataToServer(socket, request);

                // Receive response
                Map<String, Object> response = receiveDataFromServer(socket);
                System.out.println("[DEBUG] Received login response: " + response);

                // Update session ID if present
                if (response.containsKey("session_id")) {
                    sessionId = (String) response.get("session_id");
                    notifyListeners(true);

                    // We need to close this socket because the server will have closed it
                    socket.close();

                    future.complete(response);
                } else {
                    // If login failed, clean stored credentials
                    storedUsername = null;
                    storedPassword = null;

                    // Close the socket
                    socket.close();
                    socket = null;

                    notifyListeners(false);
                    future.complete(response);
                }
            } catch (Exception e) {
                System.err.println("Connection error:");
                e.printStackTrace();

                // Clean stored credentials on error
                storedUsername = null;
                storedPassword = null;

                Map<String, Object> error = new HashMap<>();
                error.put("error", "Connection error: " + e.getMessage());
                future.complete(error);

                // Clean up on error
                if (socket != null && !socket.isClosed()) {
                    try {
                        socket.close();
                        socket = null;
                    } catch (IOException ioe) {
                        System.err.println("Error closing socket: " + ioe.getMessage());
                    }
                }
                notifyListeners(false);
            }
        }).start();

        return future;
    }

    public void disconnect() {
        try {
            if (socket != null && !socket.isClosed()) {
                // Send logout request if we have a valid session
                if (sessionId != null) {
                    Map<String, Object> request = new HashMap<>();
                    request.put("action", "logout");
                    request.put("session_id", sessionId);

                    try {
                        sendDataToServer(socket, request);
                        // We don't need to wait for a response
                    } catch (IOException e) {
                        System.err.println("Error sending logout request: " + e.getMessage());
                    }
                }

                socket.close();
                socket = null;
            }

            if (writer != null) {
                writer.close();
                writer = null;
            }

            if (reader != null) {
                reader.close();
                reader = null;
            }

            sessionId = null;
            notifyListeners(false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Helper method for executing queries - simplified
    public CompletableFuture<Map<String, Object>> executeQuery(String sql) {
        Map<String, Object> request = new HashMap<>();
        request.put("action", "query");
        request.put("type", "query"); // Explicitly set type field
        request.put("query", sql);

        if (currentDatabase != null) {
            request.put("database", currentDatabase);
        }

        return sendRequest(request);
    }

    public void setServerDetails(String host, int port) {
        this.serverHost = host;
        this.serverPort = port;
    }

    private void notifyListeners(boolean connected) {
        for (ConnectionListener listener : listeners) {
            listener.onConnectionStatusChanged(connected);
        }
    }

    public CompletableFuture<Map<String, Object>> getDatabases() {
        // Use a clean approach each time
        Map<String, Object> request = new HashMap<>();
        request.put("action", "query");
        request.put("type", "query"); // Explicitly set type field
        request.put("query", "SHOW DATABASES");

        return sendRequest(request);
    }

    // Method for getting table list
    public CompletableFuture<Map<String, Object>> getTables(String database) {
        return executeQuery("USE " + database + "; SHOW TABLES");
    }

    // Method for getting columns of a table
    public CompletableFuture<Map<String, Object>> getColumns(String database, String table) {
        return executeQuery("USE " + database + "; DESCRIBE " + table);
    }

    // Method for setting current database
    public void setCurrentDatabase(String database) {
        this.currentDatabase = database;
        // Asynchronously set the database on the server
        executeQuery("USE " + database);
    }

    // Method for getting current database
    public String getCurrentDatabase() {
        return currentDatabase;
    }

    // New methods to support visual query builder features
    public CompletableFuture<Map<String, Object>> getIndexes(String database, String table) {
        if (table != null && !table.isEmpty()) {
            return executeQuery("USE " + database + "; SHOW INDEXES FOR " + table);
        } else {
            return executeQuery("USE " + database + "; SHOW INDEXES");
        }
    }

    public CompletableFuture<Map<String, Object>> visualizeBPTree(String database, String indexName, String table) {
        String query = "USE " + database + "; VISUALIZE BPTREE";
        if (indexName != null && !indexName.isEmpty()) {
            query += " " + indexName;
            if (table != null && !table.isEmpty()) {
                query += " ON " + table;
            }
        } else if (table != null && !table.isEmpty()) {
            query += " ON " + table;
        }

        return executeQuery(query);
    }

    public CompletableFuture<Map<String, Object>> startTransaction() {
        return executeQuery("BEGIN TRANSACTION");
    }

    public CompletableFuture<Map<String, Object>> commitTransaction() {
        return executeQuery("COMMIT TRANSACTION");
    }

    public CompletableFuture<Map<String, Object>> rollbackTransaction() {
        return executeQuery("ROLLBACK TRANSACTION");
    }

    public CompletableFuture<Map<String, Object>> executeSetOperation(String type, String leftQuery,
            String rightQuery) {
        String setQuery = "(" + leftQuery + ") " + type + " (" + rightQuery + ")";
        return executeQuery(setQuery);
    }

    public CompletableFuture<Map<String, Object>> getPreference(String name) {
        return executeQuery("GET PREFERENCE " + name);
    }

    public CompletableFuture<Map<String, Object>> setPreference(String name, String value) {
        return executeQuery("SET PREFERENCE " + name + " " + value);
    }

    /**
     * Enable or disable debug mode
     * 
     * @param debug Whether to enable debug mode
     */
    public void setDebugMode(boolean debug) {
        this.debugMode = debug;
    }

    /**
     * Check if debug mode is enabled
     * 
     * @return Whether debug mode is enabled
     */
    public boolean isDebugMode() {
        return debugMode;
    }

    /**
     * Enable or disable automatic reconnection
     * 
     * @param autoReconnect Whether to automatically reconnect
     */
    public void setAutoReconnect(boolean autoReconnect) {
        this.autoReconnect = autoReconnect;
    }

    /**
     * Check if auto reconnect is enabled
     * 
     * @return Whether auto reconnect is enabled
     */
    public boolean isAutoReconnect() {
        return autoReconnect;
    }
}