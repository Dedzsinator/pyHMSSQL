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
                        @SuppressWarnings("unchecked")
                        List<Map<String, Object>> tree = (List<Map<String, Object>>) response.get("tree");
                        List<List<String>> rows = new ArrayList<>();

                        // Flatten the tree structure into rows
                        for (Map<String, Object> database : tree) {
                            String dbName = (String) database.get("name");
                            if (database.containsKey("children")) {
                                @SuppressWarnings("unchecked")
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

    // Send data using the exact same protocol as Python's send_data
    private void sendDataToServer(Socket socket, Map<String, Object> data) throws IOException {
        // Add type field to match server expectations (same as action)
        if (data.containsKey("action") && !data.containsKey("type")) {
            data.put("type", data.get("action"));
        }

        // Convert data to JSON
        String jsonData = objectMapper.writeValueAsString(data);
        byte[] jsonBytes = jsonData.getBytes("UTF-8");

        if (debugMode) {
            System.out.println("[DEBUG] Sending JSON: " + jsonData);
            System.out.println("[DEBUG] JSON bytes length: " + jsonBytes.length);
        }

        // Send data with 4-byte length prefix (big-endian) like Python
        // struct.pack('>I', length)
        OutputStream os = socket.getOutputStream();

        // Send length as 4 bytes in big-endian format
        int length = jsonBytes.length;
        byte[] lengthBytes = new byte[4];
        lengthBytes[0] = (byte) ((length >>> 24) & 0xFF);
        lengthBytes[1] = (byte) ((length >>> 16) & 0xFF);
        lengthBytes[2] = (byte) ((length >>> 8) & 0xFF);
        lengthBytes[3] = (byte) (length & 0xFF);

        os.write(lengthBytes);
        os.write(jsonBytes);
        os.flush();

        if (debugMode) {
            System.out.println("[DEBUG] Data sent successfully with 4-byte length prefix: " + length);
        }
    }

    // Receive data using the exact same protocol as Python's receive_data
    private Map<String, Object> receiveDataFromServer(Socket socket) throws IOException {
        try {
            InputStream is = socket.getInputStream();

            // Read 4-byte length prefix (big-endian)
            byte[] lengthBytes = new byte[4];
            int totalRead = 0;
            while (totalRead < 4) {
                int bytesRead = is.read(lengthBytes, totalRead, 4 - totalRead);
                if (bytesRead == -1) {
                    throw new IOException("Connection closed while reading length prefix");
                }
                totalRead += bytesRead;
            }

            // Convert 4 bytes to int (big-endian)
            int length = ((lengthBytes[0] & 0xFF) << 24) |
                    ((lengthBytes[1] & 0xFF) << 16) |
                    ((lengthBytes[2] & 0xFF) << 8) |
                    (lengthBytes[3] & 0xFF);

            if (debugMode) {
                System.out.println("[DEBUG] Expected message length: " + length);
            }

            if (length <= 0 || length > 10 * 1024 * 1024) { // Max 10MB
                throw new IOException("Invalid message length: " + length);
            }

            // Read exactly 'length' bytes of JSON data
            byte[] jsonBytes = new byte[length];
            totalRead = 0;
            while (totalRead < length) {
                int bytesRead = is.read(jsonBytes, totalRead, length - totalRead);
                if (bytesRead == -1) {
                    throw new IOException("Connection closed while reading message data");
                }
                totalRead += bytesRead;
            }

            String jsonData = new String(jsonBytes, "UTF-8");

            if (debugMode) {
                System.out.println("[DEBUG] Received JSON: " + jsonData);
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
    @SuppressWarnings("unused")
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

                // Create a new socket with appropriate settings
                socket = new Socket();
                socket.setSoTimeout(30000); // 30 second read timeout
                socket.setTcpNoDelay(true); // Disable Nagle's algorithm for faster transmission
                socket.connect(new InetSocketAddress(serverHost, serverPort), 10000); // 10 second connect timeout

                if (debugMode) {
                    System.out.println("[DEBUG] Socket connected successfully.");
                }

                // Send login request using the binary protocol
                sendDataToServer(socket, request);

                // Receive response using the binary protocol
                Map<String, Object> response = receiveDataFromServer(socket);
                System.out.println("[DEBUG] Received login response: " + response);

                // Update session ID if present
                if (response.containsKey("session_id")) {
                    sessionId = (String) response.get("session_id");
                    notifyListeners(true);
                    future.complete(response);
                } else {
                    // If login failed, clean stored credentials
                    storedUsername = null;
                    storedPassword = null;
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

    private CompletableFuture<Map<String, Object>> sendRequest(Map<String, Object> request) {
        CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();

        // Create a new thread to handle the request asynchronously
        new Thread(() -> {
            Socket requestSocket = null;
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

                // Check if we have session credentials
                if (sessionId == null) {
                    throw new IOException("Not connected to server. Please log in first.");
                }

                // Create a NEW socket for each request (Python server closes after each
                // response)
                requestSocket = new Socket();
                requestSocket.setSoTimeout(30000); // 30 second read timeout
                requestSocket.setTcpNoDelay(true); // Disable Nagle's algorithm for faster transmission
                requestSocket.connect(new InetSocketAddress(serverHost, serverPort), 10000);

                if (debugMode) {
                    System.out.println("[DEBUG] New request socket connected successfully");
                }

                // Use the new connection for this request
                sendDataToServer(requestSocket, request);

                // Receive and process response
                Map<String, Object> response = receiveDataFromServer(requestSocket);

                // Close the socket immediately after receiving response
                requestSocket.close();

                if (debugMode) {
                    System.out.println("[DEBUG] Request completed successfully");
                }

                future.complete(response);
            } catch (Exception e) {
                System.err.println("Request error:");
                e.printStackTrace();
                Map<String, Object> error = new HashMap<>();
                error.put("error", "Request error: " + e.getMessage());
                future.complete(error);

                // Clean up socket on error
                if (requestSocket != null && !requestSocket.isClosed()) {
                    try {
                        requestSocket.close();
                    } catch (IOException ioe) {
                        System.err.println("Error closing request socket: " + ioe.getMessage());
                    }
                }

                // If connection issues occurred, notify listeners
                if (e instanceof SocketException || e instanceof IOException) {
                    notifyListeners(false);
                }
            }
        }).start();

        return future;
    }

    // Method for executing queries - simplified
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

    /**
     * Check if currently connected to the server
     * 
     * @return true if connected, false otherwise
     */
    public boolean isConnected() {
        return socket != null && socket.isConnected() && !socket.isClosed() && sessionId != null;
    }

    /**
     * Notify connection listeners about connection status change
     * 
     * @param connected true if connected, false if disconnected
     */
    private void notifyConnectionListeners(boolean connected) {
        notifyListeners(connected);
    }

    public CompletableFuture<Map<String, Object>> getDatabases() {
        Map<String, Object> request = new HashMap<>();
        request.put("action", "query");
        request.put("type", "query");
        request.put("query", "SHOW DATABASES");

        return sendRequest(request)
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
                                } else if (rowObj instanceof Map) {
                                    @SuppressWarnings("unchecked")
                                    Map<String, Object> rowMap = (Map<String, Object>) rowObj;
                                    // Try common database column names
                                    Object dbName = rowMap.get("Database") != null ? rowMap.get("Database")
                                            : rowMap.get("database") != null ? rowMap.get("database")
                                                    : rowMap.get("name") != null ? rowMap.get("name")
                                                            : rowMap.values().iterator().next();
                                    if (dbName != null) {
                                        databases.add(dbName.toString());
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

                    // If no recognized format, return as-is
                    return response;
                });
    }

    // Method for getting table list - improved error handling
    public CompletableFuture<Map<String, Object>> getTables(String database) {
        String query = database != null && !database.isEmpty() ? "USE " + database + "; SHOW TABLES" : "SHOW TABLES";

        return executeQuery(query)
                .thenApply(response -> {
                    if (debugMode) {
                        System.out.println("[DEBUG] Raw tables response: " + response);
                    }

                    // Check for errors first
                    if (response.containsKey("error")) {
                        return response;
                    }

                    // Check if it's already in the expected format
                    if (response.containsKey("tables")) {
                        return response;
                    }

                    // Check if it's in rows format and convert
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
                                } else if (rowObj instanceof Map) {
                                    @SuppressWarnings("unchecked")
                                    Map<String, Object> rowMap = (Map<String, Object>) rowObj;
                                    // Try common table column names
                                    Object tableName = rowMap.get("Table") != null ? rowMap.get("Table")
                                            : rowMap.get("table") != null ? rowMap.get("table")
                                                    : rowMap.get("name") != null ? rowMap.get("name")
                                                            : rowMap.values().iterator().next();
                                    if (tableName != null) {
                                        tables.add(tableName.toString());
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

                    // If no recognized format, return as-is
                    return response;
                });
    }

    // Method for getting columns of a table
    public CompletableFuture<Map<String, Object>> getColumns(String database, String table) {
        return executeQuery("USE " + database + "; DESCRIBE " + table)
                .thenApply(response -> {
                    if (debugMode) {
                        System.out.println("[DEBUG] Raw columns response: " + response);
                    }

                    // Check for errors first
                    if (response.containsKey("error")) {
                        return response;
                    }

                    // Check if it's already in the expected format
                    if (response.containsKey("columns")) {
                        return response;
                    }

                    // Check if it's in rows format and convert
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
                                            columnInfo.put("nullable", row.get(2));
                                        }
                                        if (row.size() >= 4) {
                                            columnInfo.put("primary_key", row.get(3));
                                        }
                                    }
                                } else if (rowObj instanceof Map) {
                                    @SuppressWarnings("unchecked")
                                    Map<String, Object> rowMap = (Map<String, Object>) rowObj;
                                    columnInfo.putAll(rowMap);
                                } else {
                                    // Single value, assume it's the column name
                                    columnInfo.put("name", rowObj.toString());
                                    columnInfo.put("type", "VARCHAR");
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

                    // If no recognized format, return as-is
                    return response;
                });
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

    /**
     * Set the current database context
     * 
     * @param database Database name
     */
    public void setCurrentDatabase(String database) {
        this.currentDatabase = database;
        if (debugMode) {
            System.out.println("[DEBUG] Current database set to: " + database);
        }
    }

    /**
     * Get the current database context
     * 
     * @return Current database name or null if not set
     */
    public String getCurrentDatabase() {
        return this.currentDatabase;
    }

    /**
     * Get server information
     * 
     * @return CompletableFuture containing server info
     */
    public CompletableFuture<Map<String, Object>> getServerInfo() {
        return executeQuery("SHOW SERVER INFO");
    }

    /**
     * Rollback current transaction
     * 
     * @return CompletableFuture containing the result
     */
    public CompletableFuture<Map<String, Object>> rollbackTransaction() {
        return executeQuery("ROLLBACK TRANSACTION");
    }

    /**
     * Set a single preference
     * 
     * @param key   Preference key
     * @param value Preference value (already formatted as needed)
     * @return CompletableFuture containing the result
     */
    public CompletableFuture<Map<String, Object>> setPreference(String key, String value) {
        String query = "SET PREFERENCE " + key + " " + value;
        return executeQuery(query);
    }

    /**
     * Get server status and statistics
     * 
     * @return CompletableFuture containing server status information
     */
    public CompletableFuture<Map<String, Object>> getServerStatus() {
        return executeQuery("SHOW SERVER STATUS");
    }

    /**
     * Create a new database
     * 
     * @param databaseName Name of the database to create
     * @return CompletableFuture containing the result
     */
    public CompletableFuture<Map<String, Object>> createDatabase(String databaseName) {
        return executeQuery("CREATE DATABASE " + databaseName);
    }

    /**
     * Drop a database
     * 
     * @param databaseName Name of the database to drop
     * @return CompletableFuture containing the result
     */
    public CompletableFuture<Map<String, Object>> dropDatabase(String databaseName) {
        return executeQuery("DROP DATABASE " + databaseName);
    }

    /**
     * Get database properties and statistics
     * 
     * @param databaseName Name of the database
     * @return CompletableFuture containing database properties
     */
    public CompletableFuture<Map<String, Object>> getDatabaseProperties(String databaseName) {
        return executeQuery("USE " + databaseName + "; SHOW DATABASE PROPERTIES");
    }

    /**
     * Disconnect from the server and cleanup resources
     */
    public void disconnect() {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
            sessionId = null;
            currentDatabase = null;
            notifyConnectionListeners(false);

            if (debugMode) {
                System.out.println("[DEBUG] Disconnected from server");
            }
        } catch (IOException e) {
            if (debugMode) {
                System.err.println("[DEBUG] Error during disconnect: " + e.getMessage());
            }
        }
    }
}