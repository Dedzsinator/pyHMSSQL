package com.pyhmssql.client.rest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import okhttp3.*;

/**
 * Client for interacting with the HMSSQL REST API
 */
public class RestApiClient {
    private final String baseUrl;
    private final OkHttpClient client;
    private final ObjectMapper objectMapper;
    private String sessionId;

    /**
     * Create a new REST client for HMSSQL
     * 
     * @param host Hostname or IP address
     * @param port Port number
     */
    public RestApiClient(String host, int port) {
        this.baseUrl = "http://" + host + ":" + port + "/api";
        this.client = new OkHttpClient();
        this.objectMapper = new ObjectMapper();
        this.sessionId = null;
    }

    /**
     * Login to the database
     * 
     * @param username Username
     * @param password Password
     * @return CompletableFuture with login result
     */
    public CompletableFuture<Map<String, Object>> login(String username, String password) {
        CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();

        try {
            Map<String, String> body = new HashMap<>();
            body.put("username", username);
            body.put("password", password);

            String jsonBody = objectMapper.writeValueAsString(body);

            RequestBody requestBody = RequestBody.create(
                    jsonBody,
                    MediaType.parse("application/json"));

            Request request = new Request.Builder()
                    .url(baseUrl + "/login")
                    .post(requestBody)
                    .build();

            client.newCall(request).enqueue(new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    Map<String, Object> error = new HashMap<>();
                    error.put("error", "Connection error: " + e.getMessage());
                    error.put("status", "error");
                    future.complete(error);
                }

                @Override
                public void onResponse(Call call, Response response) throws IOException {
                    String responseBody = response.body().string();
                    Map<String, Object> result = objectMapper.readValue(
                            responseBody,
                            new TypeReference<Map<String, Object>>() {
                            });

                    if (response.isSuccessful() && result.containsKey("session_id")) {
                        sessionId = (String) result.get("session_id");
                    }

                    future.complete(result);
                }
            });
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Request error: " + e.getMessage());
            error.put("status", "error");
            future.complete(error);
        }

        return future;
    }

    /**
     * Execute a SQL query
     * 
     * @param query SQL query string
     * @return CompletableFuture with query result
     */
    public CompletableFuture<Map<String, Object>> executeQuery(String query) {
        CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();

        if (sessionId == null) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Not logged in");
            error.put("status", "error");
            future.complete(error);
            return future;
        }

        try {
            Map<String, String> body = new HashMap<>();
            body.put("query", query);

            String jsonBody = objectMapper.writeValueAsString(body);

            RequestBody requestBody = RequestBody.create(
                    jsonBody,
                    MediaType.parse("application/json"));

            Request request = new Request.Builder()
                    .url(baseUrl + "/query")
                    .post(requestBody)
                    .addHeader("Authorization", "Bearer " + sessionId)
                    .build();

            client.newCall(request).enqueue(new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    Map<String, Object> error = new HashMap<>();
                    error.put("error", "Connection error: " + e.getMessage());
                    error.put("status", "error");
                    future.complete(error);
                }

                @Override
                public void onResponse(Call call, Response response) throws IOException {
                    String responseBody = response.body().string();
                    Map<String, Object> result = objectMapper.readValue(
                            responseBody,
                            new TypeReference<Map<String, Object>>() {
                            });

                    future.complete(result);
                }
            });
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Request error: " + e.getMessage());
            error.put("status", "error");
            future.complete(error);
        }

        return future;
    }

    /**
     * Logout from the database
     * 
     * @return CompletableFuture with logout result
     */
    public CompletableFuture<Map<String, Object>> logout() {
        CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();

        if (sessionId == null) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Not logged in");
            error.put("status", "error");
            future.complete(error);
            return future;
        }

        try {
            Request request = new Request.Builder()
                    .url(baseUrl + "/logout")
                    .post(RequestBody.create("", MediaType.parse("application/json")))
                    .addHeader("Authorization", "Bearer " + sessionId)
                    .build();

            client.newCall(request).enqueue(new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    Map<String, Object> error = new HashMap<>();
                    error.put("error", "Connection error: " + e.getMessage());
                    error.put("status", "error");
                    future.complete(error);
                }

                @Override
                public void onResponse(Call call, Response response) throws IOException {
                    String responseBody = response.body().string();
                    Map<String, Object> result = objectMapper.readValue(
                            responseBody,
                            new TypeReference<Map<String, Object>>() {
                            });

                    if (response.isSuccessful()) {
                        sessionId = null;
                    }

                    future.complete(result);
                }
            });
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Request error: " + e.getMessage());
            error.put("status", "error");
            future.complete(error);
        }

        return future;
    }

    /**
     * Get server status
     * 
     * @return CompletableFuture with status information
     */
    public CompletableFuture<Map<String, Object>> getStatus() {
        CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();

        if (sessionId == null) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Not logged in");
            error.put("status", "error");
            future.complete(error);
            return future;
        }

        try {
            Request request = new Request.Builder()
                    .url(baseUrl + "/status")
                    .get()
                    .addHeader("Authorization", "Bearer " + sessionId)
                    .build();

            client.newCall(request).enqueue(new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    Map<String, Object> error = new HashMap<>();
                    error.put("error", "Connection error: " + e.getMessage());
                    error.put("status", "error");
                    future.complete(error);
                }

                @Override
                public void onResponse(Call call, Response response) throws IOException {
                    String responseBody = response.body().string();
                    Map<String, Object> result = objectMapper.readValue(
                            responseBody,
                            new TypeReference<Map<String, Object>>() {
                            });

                    future.complete(result);
                }
            });
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Request error: " + e.getMessage());
            error.put("status", "error");
            future.complete(error);
        }

        return future;
    }

    /**
     * Get list of databases
     * 
     * @return CompletableFuture with databases list
     */
    public CompletableFuture<Map<String, Object>> getDatabases() {
        return makeGetRequest("/databases");
    }

    /**
     * Get list of tables for current database
     * 
     * @return CompletableFuture with tables list
     */
    public CompletableFuture<Map<String, Object>> getTables() {
        return makeGetRequest("/tables");
    }

    /**
     * Get table schema information
     * 
     * @param tableName Name of the table
     * @return CompletableFuture with table schema
     */
    public CompletableFuture<Map<String, Object>> getTableSchema(String tableName) {
        return makeGetRequest("/table/" + tableName);
    }

    /**
     * Get indexes information
     * 
     * @return CompletableFuture with indexes list
     */
    public CompletableFuture<Map<String, Object>> getIndexes() {
        return makeGetRequest("/indexes");
    }

    /**
     * Set current database
     * 
     * @param database Database name
     * @return CompletableFuture with result
     */
    public CompletableFuture<Map<String, Object>> useDatabase(String database) {
        return makePostRequest("/use_database", Map.of("database", database));
    }

    /**
     * Register a new user
     * 
     * @param username Username
     * @param password Password
     * @param role     User role (optional)
     * @return CompletableFuture with registration result
     */
    public CompletableFuture<Map<String, Object>> register(String username, String password, String role) {
        Map<String, Object> body = new HashMap<>();
        body.put("username", username);
        body.put("password", password);
        if (role != null) {
            body.put("role", role);
        }
        return makePostRequest("/register", body);
    }

    /**
     * Start a transaction
     * 
     * @return CompletableFuture with result
     */
    public CompletableFuture<Map<String, Object>> beginTransaction() {
        return executeQuery("BEGIN TRANSACTION");
    }

    /**
     * Commit current transaction
     * 
     * @return CompletableFuture with result
     */
    public CompletableFuture<Map<String, Object>> commitTransaction() {
        return executeQuery("COMMIT TRANSACTION");
    }

    /**
     * Rollback current transaction
     * 
     * @return CompletableFuture with result
     */
    public CompletableFuture<Map<String, Object>> rollbackTransaction() {
        return executeQuery("ROLLBACK TRANSACTION");
    }

    /**
     * Get user preferences
     * 
     * @return CompletableFuture with preferences
     */
    public CompletableFuture<Map<String, Object>> getPreferences() {
        return makeGetRequest("/preferences");
    }

    /**
     * Set a preference
     * 
     * @param key   Preference key
     * @param value Preference value
     * @return CompletableFuture with result
     */
    public CompletableFuture<Map<String, Object>> setPreference(String key, Object value) {
        Map<String, Object> body = new HashMap<>();
        body.put("key", key);
        body.put("value", value);
        return makePostRequest("/preferences", body);
    }

    // Helper methods for common request patterns
    private CompletableFuture<Map<String, Object>> makeGetRequest(String endpoint) {
        CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();

        if (sessionId == null) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Not logged in");
            error.put("status", "error");
            future.complete(error);
            return future;
        }

        try {
            Request request = new Request.Builder()
                    .url(baseUrl + endpoint)
                    .get()
                    .addHeader("Authorization", "Bearer " + sessionId)
                    .build();

            client.newCall(request).enqueue(new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    Map<String, Object> error = new HashMap<>();
                    error.put("error", "Connection error: " + e.getMessage());
                    error.put("status", "error");
                    future.complete(error);
                }

                @Override
                public void onResponse(Call call, Response response) throws IOException {
                    String responseBody = response.body().string();
                    Map<String, Object> result = objectMapper.readValue(
                            responseBody,
                            new TypeReference<Map<String, Object>>() {
                            });
                    future.complete(result);
                }
            });
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Request error: " + e.getMessage());
            error.put("status", "error");
            future.complete(error);
        }

        return future;
    }

    private CompletableFuture<Map<String, Object>> makePostRequest(String endpoint, Map<String, Object> body) {
        CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();

        if (sessionId == null && !endpoint.equals("/register")) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Not logged in");
            error.put("status", "error");
            future.complete(error);
            return future;
        }

        try {
            String jsonBody = objectMapper.writeValueAsString(body);
            RequestBody requestBody = RequestBody.create(
                    jsonBody,
                    MediaType.parse("application/json"));

            Request.Builder requestBuilder = new Request.Builder()
                    .url(baseUrl + endpoint)
                    .post(requestBody);

            if (sessionId != null) {
                requestBuilder.addHeader("Authorization", "Bearer " + sessionId);
            }

            Request request = requestBuilder.build();

            client.newCall(request).enqueue(new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    Map<String, Object> error = new HashMap<>();
                    error.put("error", "Connection error: " + e.getMessage());
                    error.put("status", "error");
                    future.complete(error);
                }

                @Override
                public void onResponse(Call call, Response response) throws IOException {
                    String responseBody = response.body().string();
                    Map<String, Object> result = objectMapper.readValue(
                            responseBody,
                            new TypeReference<Map<String, Object>>() {
                            });
                    future.complete(result);
                }
            });
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Request error: " + e.getMessage());
            error.put("status", "error");
            future.complete(error);
        }

        return future;
    }

    /**
     * Check if currently logged in
     * 
     * @return True if logged in, false otherwise
     */
    public boolean isLoggedIn() {
        return sessionId != null;
    }

    /**
     * Get the current session ID
     * 
     * @return Session ID or null if not logged in
     */
    public String getSessionId() {
        return sessionId;
    }
}