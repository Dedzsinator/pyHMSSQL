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
    @SuppressWarnings("unused")
    private PrintWriter writer;
    @SuppressWarnings("unused")
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
                discoverySocket = new DatagramSocket();
                discoverySocket.setBroadcast(true);
                discoverySocket.setSoTimeout(500); // Short timeout for responsive stopping
                isDiscovering = true;

                // Send discovery request
                String discoveryMessage = "{\"action\":\"discover\",\"type\":\"HMSSQL_CLIENT\"}";
                byte[] sendData = discoveryMessage.getBytes();

                // Broadcast to common subnets
                String[] broadcastAddresses = {
                        "255.255.255.255",
                        "192.168.1.255",
                        "192.168.0.255",
                        "10.0.0.255"
                };

                for (String broadcastAddr : broadcastAddresses) {
                    try {
                        InetAddress broadcast = InetAddress.getByName(broadcastAddr);
                        DatagramPacket sendPacket = new DatagramPacket(
                                sendData, sendData.length, broadcast, DISCOVERY_PORT);
                        discoverySocket.send(sendPacket);
                    } catch (Exception e) {
                        // Ignore broadcast errors for specific subnets
                    }
                }

                // Listen for responses
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

                            if ("HMSSQL_SERVER".equals(serverInfo.get("type"))) {
                                String host = packet.getAddress().getHostAddress();
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

    // Send data using the exact same protocol as Python's send_data
    private void sendDataToServer(Socket socket, Map<String, Object> data) throws IOException {
        // Add session_id if available and not already present
        if (sessionId != null && !data.containsKey("session_id")) {
            data.put("session_id", sessionId);
        }

        // Convert data to JSON exactly like the Python CLI client expects
        String jsonData = objectMapper.writeValueAsString(data);
        byte[] jsonBytes = jsonData.getBytes("UTF-8");

        if (debugMode) {
            System.out.println("[DEBUG] Sending to server: " + jsonData);
            System.out.println("[DEBUG] Data size: " + jsonBytes.length + " bytes");
        }

        // Send exactly like Python's send_data: 4-byte big-endian length + data
        OutputStream os = socket.getOutputStream();

        // Pack length as big-endian 4-byte integer (matching Python's struct.pack('>I',
        // length))
        int length = jsonBytes.length;
        byte[] lengthBytes = new byte[4];
        lengthBytes[0] = (byte) ((length >>> 24) & 0xFF);
        lengthBytes[1] = (byte) ((length >>> 16) & 0xFF);
        lengthBytes[2] = (byte) ((length >>> 8) & 0xFF);
        lengthBytes[3] = (byte) (length & 0xFF);

        // Send length prefix then data
        os.write(lengthBytes);
        os.write(jsonBytes);
        os.flush();

        if (debugMode) {
            System.out.println("[DEBUG] Successfully sent " + (4 + jsonBytes.length) + " total bytes");
        }
    }

    // Receive data using the exact same protocol as Python's receive_data
    private Map<String, Object> receiveDataFromServer(Socket socket) throws IOException {
        InputStream is = socket.getInputStream();

        // Read exactly 4 bytes for length (matching Python's struct.unpack('>I',
        // length_data))
        byte[] lengthBytes = new byte[4];
        int totalRead = 0;
        while (totalRead < 4) {
            int bytesRead = is.read(lengthBytes, totalRead, 4 - totalRead);
            if (bytesRead == -1) {
                throw new IOException("Server closed connection while reading length");
            }
            totalRead += bytesRead;
        }

        // Unpack big-endian length
        int length = ((lengthBytes[0] & 0xFF) << 24) |
                ((lengthBytes[1] & 0xFF) << 16) |
                ((lengthBytes[2] & 0xFF) << 8) |
                (lengthBytes[3] & 0xFF);

        if (debugMode) {
            System.out.println("[DEBUG] Server response length: " + length + " bytes");
        }

        // Validate length to prevent memory issues
        if (length <= 0 || length > 50 * 1024 * 1024) { // Max 50MB
            throw new IOException("Invalid response length from server: " + length);
        }

        // Read exactly 'length' bytes of JSON data
        byte[] jsonBytes = new byte[length];
        totalRead = 0;
        while (totalRead < length) {
            int bytesRead = is.read(jsonBytes, totalRead, length - totalRead);
            if (bytesRead == -1) {
                throw new IOException("Server closed connection while reading data");
            }
            totalRead += bytesRead;
        }

        String jsonData = new String(jsonBytes, "UTF-8");

        if (debugMode) {
            System.out.println("[DEBUG] Received from server: " + jsonData);
        }

        try {
            Map<String, Object> response = objectMapper.readValue(jsonData, new TypeReference<Map<String, Object>>() {
            });

            // Handle server responses properly
            if (response.containsKey("error")) {
                System.err.println("[ERROR] Server error: " + response.get("error"));
            }

            return response;
        } catch (Exception e) {
            System.err.println("[ERROR] Failed to parse server response: " + e.getMessage());
            System.err.println("[ERROR] Raw response: " + jsonData);
            throw new IOException("Invalid JSON response from server", e);
        }
    }

    public CompletableFuture<Map<String, Object>> connect(String username, String password) {
        CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();

        // Store credentials for reconnection
        storedUsername = username;
        storedPassword = password;

        new Thread(() -> {
            try {
                if (debugMode) {
                    System.out.println("[DEBUG] Connecting to: " + serverHost + ":" + serverPort);
                }

                // Create socket with proper settings for the server protocol
                socket = new Socket();
                socket.setSoTimeout(30000); // 30 second timeout
                socket.setTcpNoDelay(true); // Disable Nagle's algorithm
                socket.setKeepAlive(true); // Enable keep-alive

                // Connect with timeout
                socket.connect(new InetSocketAddress(serverHost, serverPort), 10000);

                if (debugMode) {
                    System.out.println("[DEBUG] Socket connected successfully");
                }

                // Create login request matching server expectations
                Map<String, Object> loginRequest = new HashMap<>();
                loginRequest.put("action", "login");
                loginRequest.put("username", username);
                loginRequest.put("password", password);

                // Send login request using proper protocol
                sendDataToServer(socket, loginRequest);

                // Receive login response
                Map<String, Object> response = receiveDataFromServer(socket);

                if (debugMode) {
                    System.out.println("[DEBUG] Login response: " + response);
                }

                // Handle login response
                if (response.containsKey("session_id")) {
                    sessionId = (String) response.get("session_id");
                    notifyListeners(true);

                    if (debugMode) {
                        System.out.println("[DEBUG] Login successful, session_id: " + sessionId);
                    }

                    future.complete(response);
                } else {
                    // Login failed
                    storedUsername = null;
                    storedPassword = null;
                    notifyListeners(false);
                    future.complete(response);
                }

            } catch (Exception e) {
                System.err.println("[ERROR] Connection failed: " + e.getMessage());
                e.printStackTrace();

                storedUsername = null;
                storedPassword = null;

                Map<String, Object> error = new HashMap<>();
                error.put("error", "Connection failed: " + e.getMessage());
                future.complete(error);

                if (socket != null && !socket.isClosed()) {
                    try {
                        socket.close();
                        socket = null;
                    } catch (IOException ioe) {
                        System.err.println("[ERROR] Error closing socket: " + ioe.getMessage());
                    }
                }
                notifyListeners(false);
            }
        }).start();

        return future;
    }

    private CompletableFuture<Map<String, Object>> sendRequest(Map<String, Object> request) {
        CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();

        new Thread(() -> {
            Socket requestSocket = null;
            try {
                // Validate session
                if (sessionId == null) {
                    throw new IOException("Not connected - no session ID");
                }

                if (debugMode) {
                    System.out.println("[DEBUG] Sending request: " + request.get("action"));
                }

                // Create new connection for each request (matching server architecture)
                requestSocket = new Socket();
                requestSocket.setSoTimeout(30000);
                requestSocket.setTcpNoDelay(true);
                requestSocket.connect(new InetSocketAddress(serverHost, serverPort), 10000);

                // Send request using proper protocol
                sendDataToServer(requestSocket, request);

                // Receive response
                Map<String, Object> response = receiveDataFromServer(requestSocket);

                // Close connection immediately (server closes after each request)
                requestSocket.close();

                if (debugMode) {
                    System.out.println("[DEBUG] Request completed successfully");
                }

                future.complete(response);

            } catch (Exception e) {
                System.err.println("[ERROR] Request failed: " + e.getMessage());
                e.printStackTrace();

                Map<String, Object> error = new HashMap<>();
                error.put("error", "Request failed: " + e.getMessage());
                future.complete(error);

                if (requestSocket != null && !requestSocket.isClosed()) {
                    try {
                        requestSocket.close();
                    } catch (IOException ioe) {
                        System.err.println("[ERROR] Error closing request socket: " + ioe.getMessage());
                    }
                }

                // Check if we lost connection
                if (e instanceof SocketException || e instanceof IOException) {
                    notifyListeners(false);
                }
            }
        }).start();

        return future;
    }

    /**
     * Get list of available databases using SQL SHOW command like CLI client
     */
    public CompletableFuture<Map<String, Object>> getDatabases() {
        // Send exactly like CLI client: "SHOW DATABASES" as SQL query
        return executeQuery("SHOW DATABASES")
                .thenApply(response -> {
                    if (debugMode) {
                        System.out.println("[DEBUG] Raw database response: " + response);
                    }

                    // Check for errors first
                    if (response.containsKey("error")) {
                        return response;
                    }

                    // Check if it's already in the expected format
                    if (response.containsKey("databases")) {
                        return response;
                    }

                    // Check if it's in rows format and convert
                    if (response.containsKey("rows")) {
                        Object rowsObj = response.get("rows");
                        List<String> databases = new ArrayList<>();

                        if (rowsObj instanceof List) {
                            @SuppressWarnings("unchecked")
                            List<Object> rows = (List<Object>) rowsObj;

                            for (Object rowObj : rows) {
                                if (rowObj instanceof List) {
                                    @SuppressWarnings("unchecked")
                                    List<Object> row = (List<Object>) rowObj;
                                    if (!row.isEmpty()) {
                                        databases.add(row.get(0).toString());
                                    }
                                } else {
                                    databases.add(rowObj.toString());
                                }
                            }
                        }

                        Map<String, Object> convertedResponse = new HashMap<>();
                        convertedResponse.put("databases", databases);
                        convertedResponse.put("status", "success");
                        return convertedResponse;
                    }

                    return response;
                });
    }

    /**
     * Method for executing queries - match CLI client format exactly
     */
    public CompletableFuture<Map<String, Object>> executeQuery(String sql) {
        Map<String, Object> request = new HashMap<>();
        // Match CLI client exactly: {"action": "query", "session_id": "...", "query":
        // "SHOW DATABASES"}
        request.put("action", "query");
        request.put("query", sql);

        return sendRequest(request);
    }

    /**
     * Get list of tables using SQL SHOW command like CLI client
     */
    public CompletableFuture<Map<String, Object>> getTables(String database) {
        // First set database using USE command, then show tables
        return executeQuery("USE " + database).thenCompose(setResult -> {
            if (setResult.containsKey("error")) {
                return CompletableFuture.completedFuture(setResult);
            }

            // Now execute SHOW TABLES exactly like CLI client
            return executeQuery("SHOW TABLES")
                    .thenApply(response -> {
                        if (debugMode) {
                            System.out.println("[DEBUG] Raw tables response: " + response);
                        }

                        // Process response similar to getDatabases
                        if (response.containsKey("error")) {
                            return response;
                        }

                        if (response.containsKey("tables")) {
                            return response;
                        }

                        if (response.containsKey("rows")) {
                            Object rowsObj = response.get("rows");
                            List<String> tables = new ArrayList<>();

                            if (rowsObj instanceof List) {
                                @SuppressWarnings("unchecked")
                                List<Object> rows = (List<Object>) rowsObj;

                                for (Object rowObj : rows) {
                                    if (rowObj instanceof List) {
                                        @SuppressWarnings("unchecked")
                                        List<Object> row = (List<Object>) rowObj;
                                        if (!row.isEmpty()) {
                                            tables.add(row.get(0).toString());
                                        }
                                    } else {
                                        tables.add(rowObj.toString());
                                    }
                                }
                            }

                            Map<String, Object> convertedResponse = new HashMap<>();
                            convertedResponse.put("tables", tables);
                            convertedResponse.put("status", "success");
                            return convertedResponse;
                        }

                        return response;
                    });
        });
    }

    /**
     * Get columns using SQL SHOW command like CLI client
     */
    public CompletableFuture<Map<String, Object>> getColumns(String database, String table) {
        // First set database using USE command, then show columns
        return executeQuery("USE " + database).thenCompose(setResult -> {
            if (setResult.containsKey("error")) {
                return CompletableFuture.completedFuture(setResult);
            }

            // Now execute SHOW COLUMNS FROM table exactly like CLI client
            return executeQuery("SHOW COLUMNS FROM " + table)
                    .thenApply(response -> {
                        if (debugMode) {
                            System.out.println("[DEBUG] Raw columns response: " + response);
                        }

                        if (response.containsKey("error")) {
                            return response;
                        }

                        if (response.containsKey("columns")) {
                            return response;
                        }

                        if (response.containsKey("rows")) {
                            Object rowsObj = response.get("rows");
                            List<Map<String, Object>> columns = new ArrayList<>();

                            if (rowsObj instanceof List) {
                                @SuppressWarnings("unchecked")
                                List<Object> rows = (List<Object>) rowsObj;

                                for (Object rowObj : rows) {
                                    Map<String, Object> columnInfo = new HashMap<>();

                                    if (rowObj instanceof List) {
                                        @SuppressWarnings("unchecked")
                                        List<Object> row = (List<Object>) rowObj;
                                        if (row.size() >= 2) {
                                            columnInfo.put("name", row.get(0).toString());
                                            columnInfo.put("type", row.get(1).toString());
                                            if (row.size() >= 3) {
                                                columnInfo.put("nullable", "YES".equals(row.get(2).toString()));
                                            }
                                            if (row.size() >= 4) {
                                                columnInfo.put("primary_key", "PRI".equals(row.get(3).toString()));
                                            }
                                        }
                                    } else {
                                        // Single value, assume it's the column name
                                        columnInfo.put("name", rowObj.toString());
                                        columnInfo.put("type", "VARCHAR");
                                        columnInfo.put("nullable", true);
                                        columnInfo.put("primary_key", false);
                                    }

                                    if (!columnInfo.isEmpty()) {
                                        columns.add(columnInfo);
                                    }
                                }
                            }

                            Map<String, Object> convertedResponse = new HashMap<>();
                            convertedResponse.put("columns", columns);
                            convertedResponse.put("status", "success");
                            return convertedResponse;
                        }

                        return response;
                    });
        });
    }

    /**
     * Set the current database using SQL USE command like CLI client
     */
    public CompletableFuture<Map<String, Object>> setCurrentDatabase(String database) {
        return executeQuery("USE " + database).thenApply(response -> {
            if (!response.containsKey("error")) {
                this.currentDatabase = database;
                if (debugMode) {
                    System.out.println("[DEBUG] Current database set to: " + database);
                }
            }
            return response;
        });
    }

    // Other utility methods using SQL commands like CLI client
    public CompletableFuture<Map<String, Object>> createDatabase(String databaseName) {
        return executeQuery("CREATE DATABASE " + databaseName);
    }

    public CompletableFuture<Map<String, Object>> dropDatabase(String databaseName) {
        return executeQuery("DROP DATABASE " + databaseName);
    }

    public CompletableFuture<Map<String, Object>> getServerStatus() {
        return executeQuery("SHOW SERVER STATUS");
    }

    public CompletableFuture<Map<String, Object>> getDatabaseProperties(String databaseName) {
        return executeQuery("USE " + databaseName + "; SHOW DATABASE PROPERTIES");
    }

    public CompletableFuture<Map<String, Object>> setPreference(String key, String value) {
        return executeQuery("SET PREFERENCE " + key + " " + value);
    }

    public CompletableFuture<Map<String, Object>> getPreferences() {
        return executeQuery("GET PREFERENCES");
    }

    public String getCurrentDatabase() {
        return this.currentDatabase;
    }

    public boolean isConnected() {
        return socket != null && socket.isConnected() && !socket.isClosed() && sessionId != null;
    }

    public void setServerDetails(String host, int port) {
        this.serverHost = host;
        this.serverPort = port;
    }

    public void disconnect() {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
            sessionId = null;
            currentDatabase = null;
            notifyListeners(false);

            if (debugMode) {
                System.out.println("[DEBUG] Disconnected from server");
            }
        } catch (IOException e) {
            if (debugMode) {
                System.err.println("[DEBUG] Error during disconnect: " + e.getMessage());
            }
        }
    }

    private void notifyListeners(boolean connected) {
        for (ConnectionListener listener : listeners) {
            listener.onConnectionStatusChanged(connected);
        }
    }

    public CompletableFuture<Map<String, Object>> getIndexes(String database, String table) {
        return executeQuery("USE " + database).thenCompose(setResult -> {
            if (setResult.containsKey("error")) {
                return CompletableFuture.completedFuture(setResult);
            }

            String command = table != null && !table.isEmpty()
                    ? "SHOW INDEXES FROM " + table
                    : "SHOW INDEXES";

            return executeQuery(command);
        });
    }

    public CompletableFuture<Map<String, Object>> visualizeBPTree(String database, String indexName, String table) {
        return executeQuery("USE " + database).thenCompose(setResult -> {
            if (setResult.containsKey("error")) {
                return CompletableFuture.completedFuture(setResult);
            }

            String command = "VISUALIZE BPTREE";
            if (indexName != null && !indexName.isEmpty()) {
                command += " " + indexName;
                if (table != null && !table.isEmpty()) {
                    command += " ON " + table;
                }
            } else if (table != null && !table.isEmpty()) {
                command += " ON " + table;
            }

            return executeQuery(command).thenApply(response -> {
                // If the server doesn't support visualization, create a simple HTML response
                if (response.containsKey("error")) {
                    Map<String, Object> htmlResponse = new HashMap<>();
                    htmlResponse.put("visualization", createSimpleVisualizationHTML(indexName, table));
                    return htmlResponse;
                }
                return response;
            });
        });
    }

    private String createSimpleVisualizationHTML(String indexName, String table) {
        return String.format("""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>B+ Tree Visualization</title>
                    <style>
                        body { font-family: Arial, sans-serif; margin: 20px; }
                        .tree-node {
                            border: 1px solid #333;
                            padding: 10px;
                            margin: 5px;
                            background-color: #f0f0f0;
                            text-align: center;
                        }
                        .message {
                            color: #666;
                            font-style: italic;
                            text-align: center;
                            margin-top: 50px;
                        }
                    </style>
                </head>
                <body>
                    <h2>B+ Tree Visualization</h2>
                    <p><strong>Index:</strong> %s</p>
                    <p><strong>Table:</strong> %s</p>
                    <div class="message">
                        <p>B+ Tree visualization is not currently supported by the server.</p>
                        <p>This feature requires server-side visualization support.</p>
                    </div>
                </body>
                </html>
                """, indexName != null ? indexName : "Unknown", table != null ? table : "Unknown");
    }

    public CompletableFuture<Map<String, Object>> rollbackTransaction() {
        return executeQuery("ROLLBACK TRANSACTION");
    }

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
}