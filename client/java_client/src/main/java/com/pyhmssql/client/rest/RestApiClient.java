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