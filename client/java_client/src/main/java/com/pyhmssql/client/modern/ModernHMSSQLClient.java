package com.pyhmssql.client.modern;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.border.TitledBorder;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.JTableHeader;
import javax.swing.text.BadLocationException;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyledDocument;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.swing.SwingWorker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Modern HMSSQL Client - A professional, clean database management GUI
 * Features:
 * - Material Design inspired interface
 * - Proper script execution with multi-line support
 * - Syntax highlighting for SQL
 * - Connection management
 * - Result visualization
 * - Query history
 * - Professional styling
 */
public class ModernHMSSQLClient extends JFrame {

    // Core components
    private JTextPane queryEditor;
    private JTable resultTable;
    private JTextArea logArea;
    private JLabel statusLabel;
    private JButton connectButton;
    private JButton executeButton;
    private JButton executeScriptButton;
    private JTextField serverField;
    private JTextField portField;
    private JTextField usernameField;
    private JPasswordField passwordField;

    // Connection state
    private String serverHost = "localhost";
    private int serverPort = 8080;
    private String sessionId = null;
    private String currentUser = null;
    private boolean isConnected = false;

    // Utilities
    private ObjectMapper objectMapper = new ObjectMapper();
    private List<String> queryHistory = new ArrayList<>();
    private int historyIndex = -1;

    // Colors for modern design
    private static final Color PRIMARY_COLOR = new Color(33, 150, 243);
    private static final Color SECONDARY_COLOR = new Color(63, 81, 181);
    private static final Color SUCCESS_COLOR = new Color(76, 175, 80);
    private static final Color ERROR_COLOR = new Color(244, 67, 54);
    private static final Color BACKGROUND_COLOR = new Color(250, 250, 250);
    private static final Color CARD_COLOR = Color.WHITE;
    private static final Color TEXT_COLOR = new Color(33, 33, 33);
    private static final Color SECONDARY_TEXT_COLOR = new Color(117, 117, 117);

    public ModernHMSSQLClient() {
        initializeUI();
        setupKeyBindings();
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setTitle("HMSSQL Professional Client v2.0");
        setSize(1400, 900);
        setLocationRelativeTo(null);

        // Set modern look and feel
        try {
            // Use Nimbus if available, otherwise default
            for (UIManager.LookAndFeelInfo info : UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        applyModernStyling();
    }

    private void initializeUI() {
        setLayout(new BorderLayout());

        // Create main panels
        JPanel topPanel = createConnectionPanel();
        JPanel centerPanel = createCenterPanel();
        JPanel bottomPanel = createStatusPanel();

        add(topPanel, BorderLayout.NORTH);
        add(centerPanel, BorderLayout.CENTER);
        add(bottomPanel, BorderLayout.SOUTH);
    }

    private JPanel createConnectionPanel() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBackground(CARD_COLOR);
        panel.setBorder(new EmptyBorder(10, 10, 10, 10));

        // Connection form
        JPanel formPanel = new JPanel(new GridBagLayout());
        formPanel.setBackground(CARD_COLOR);
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);

        // Server field
        gbc.gridx = 0;
        gbc.gridy = 0;
        formPanel.add(new JLabel("Server:"), gbc);
        gbc.gridx = 1;
        serverField = new JTextField("localhost", 15);
        formPanel.add(serverField, gbc);

        // Port field
        gbc.gridx = 2;
        formPanel.add(new JLabel("Port:"), gbc);
        gbc.gridx = 3;
        portField = new JTextField("8080", 8);
        formPanel.add(portField, gbc);

        // Username field
        gbc.gridx = 4;
        formPanel.add(new JLabel("Username:"), gbc);
        gbc.gridx = 5;
        usernameField = new JTextField(15);
        formPanel.add(usernameField, gbc);

        // Password field
        gbc.gridx = 6;
        formPanel.add(new JLabel("Password:"), gbc);
        gbc.gridx = 7;
        passwordField = new JPasswordField(15);
        formPanel.add(passwordField, gbc);

        // Connect button
        gbc.gridx = 8;
        connectButton = createStyledButton("Connect", PRIMARY_COLOR);
        connectButton.addActionListener(e -> handleConnection());
        formPanel.add(connectButton, gbc);

        panel.add(formPanel, BorderLayout.CENTER);

        return panel;
    }

    private JPanel createCenterPanel() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBackground(BACKGROUND_COLOR);

        // Create splitter
        JSplitPane mainSplitter = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
        mainSplitter.setResizeWeight(0.6);
        mainSplitter.setBorder(null);

        // Top panel with query editor and buttons
        JPanel queryPanel = createQueryPanel();

        // Bottom panel with results and logs
        JSplitPane bottomSplitter = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
        bottomSplitter.setResizeWeight(0.7);
        bottomSplitter.setBorder(null);

        JPanel resultsPanel = createResultsPanel();
        JPanel logPanel = createLogPanel();

        bottomSplitter.setLeftComponent(resultsPanel);
        bottomSplitter.setRightComponent(logPanel);

        mainSplitter.setTopComponent(queryPanel);
        mainSplitter.setBottomComponent(bottomSplitter);

        panel.add(mainSplitter, BorderLayout.CENTER);

        return panel;
    }

    private JPanel createQueryPanel() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBackground(CARD_COLOR);
        panel.setBorder(new TitledBorder("SQL Query Editor"));

        // Toolbar
        JPanel toolbar = new JPanel(new FlowLayout(FlowLayout.LEFT));
        toolbar.setBackground(CARD_COLOR);

        executeButton = createStyledButton("Execute Query", SUCCESS_COLOR);
        executeButton.addActionListener(e -> executeQuery());
        executeButton.setEnabled(false);

        executeScriptButton = createStyledButton("Execute Script", SECONDARY_COLOR);
        executeScriptButton.addActionListener(e -> executeScript());
        executeScriptButton.setEnabled(false);

        JButton loadScriptButton = createStyledButton("Load Script", SECONDARY_TEXT_COLOR);
        loadScriptButton.addActionListener(e -> loadScript());

        JButton saveQueryButton = createStyledButton("Save Query", SECONDARY_TEXT_COLOR);
        saveQueryButton.addActionListener(e -> saveQuery());

        JButton clearButton = createStyledButton("Clear", ERROR_COLOR);
        clearButton.addActionListener(e -> queryEditor.setText(""));

        toolbar.add(executeButton);
        toolbar.add(executeScriptButton);
        toolbar.add(new JSeparator(SwingConstants.VERTICAL));
        toolbar.add(loadScriptButton);
        toolbar.add(saveQueryButton);
        toolbar.add(clearButton);

        // Query editor with syntax highlighting
        queryEditor = new JTextPane();
        queryEditor.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 14));
        queryEditor.setText("-- Enter your SQL query here\nSELECT * FROM users LIMIT 10;");

        JScrollPane editorScroll = new JScrollPane(queryEditor);
        editorScroll.setPreferredSize(new Dimension(800, 300));

        panel.add(toolbar, BorderLayout.NORTH);
        panel.add(editorScroll, BorderLayout.CENTER);

        return panel;
    }

    private JPanel createResultsPanel() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBackground(CARD_COLOR);
        panel.setBorder(new TitledBorder("Query Results"));

        // Results table
        resultTable = new JTable();
        resultTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        resultTable.setRowHeight(25);
        resultTable.setShowGrid(true);
        resultTable.setGridColor(new Color(224, 224, 224));

        JTableHeader header = resultTable.getTableHeader();
        header.setBackground(PRIMARY_COLOR);
        header.setForeground(Color.WHITE);
        header.setFont(header.getFont().deriveFont(Font.BOLD));

        JScrollPane tableScroll = new JScrollPane(resultTable);
        tableScroll.setPreferredSize(new Dimension(600, 200));

        panel.add(tableScroll, BorderLayout.CENTER);

        return panel;
    }

    private JPanel createLogPanel() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBackground(CARD_COLOR);
        panel.setBorder(new TitledBorder("Execution Log"));

        logArea = new JTextArea();
        logArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        logArea.setEditable(false);
        logArea.setBackground(new Color(248, 248, 248));

        JScrollPane logScroll = new JScrollPane(logArea);
        logScroll.setPreferredSize(new Dimension(300, 200));

        JButton clearLogButton = createStyledButton("Clear Log", ERROR_COLOR);
        clearLogButton.addActionListener(e -> logArea.setText(""));

        JPanel logToolbar = new JPanel(new FlowLayout(FlowLayout.RIGHT));
        logToolbar.setBackground(CARD_COLOR);
        logToolbar.add(clearLogButton);

        panel.add(logScroll, BorderLayout.CENTER);
        panel.add(logToolbar, BorderLayout.SOUTH);

        return panel;
    }

    private JPanel createStatusPanel() {
        JPanel panel = new JPanel(new BorderLayout());
        panel.setBackground(CARD_COLOR);
        panel.setBorder(new EmptyBorder(5, 10, 5, 10));

        statusLabel = new JLabel("Ready - Not Connected");
        statusLabel.setForeground(SECONDARY_TEXT_COLOR);

        JLabel timeLabel = new JLabel();
        Timer timer = new Timer(1000, e -> {
            timeLabel.setText(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        });
        timer.start();

        panel.add(statusLabel, BorderLayout.WEST);
        panel.add(timeLabel, BorderLayout.EAST);

        return panel;
    }

    private JButton createStyledButton(String text, Color color) {
        JButton button = new JButton(text);
        button.setBackground(color);
        button.setForeground(Color.WHITE);
        button.setFocusPainted(false);
        button.setBorderPainted(false);
        button.setFont(button.getFont().deriveFont(Font.BOLD));
        button.setCursor(new Cursor(Cursor.HAND_CURSOR));

        // Add hover effect
        button.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseEntered(java.awt.event.MouseEvent evt) {
                button.setBackground(color.darker());
            }

            public void mouseExited(java.awt.event.MouseEvent evt) {
                button.setBackground(color);
            }
        });

        return button;
    }

    private void applyModernStyling() {
        // Set modern font
        Font modernFont = new Font("Segoe UI", Font.PLAIN, 12);
        setUIFont(modernFont);

        // Set background color
        getContentPane().setBackground(BACKGROUND_COLOR);
    }

    private void setUIFont(Font font) {
        java.util.Enumeration<Object> keys = UIManager.getDefaults().keys();
        while (keys.hasMoreElements()) {
            Object key = keys.nextElement();
            Object value = UIManager.get(key);
            if (value instanceof Font) {
                UIManager.put(key, font);
            }
        }
    }

    private void setupKeyBindings() {
        // Ctrl+Enter to execute query
        queryEditor.getInputMap().put(KeyStroke.getKeyStroke(KeyEvent.VK_ENTER, KeyEvent.CTRL_DOWN_MASK), "execute");
        queryEditor.getActionMap().put("execute", new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                if (isConnected) {
                    executeQuery();
                }
            }
        });

        // F5 to execute
        queryEditor.getInputMap().put(KeyStroke.getKeyStroke(KeyEvent.VK_F5, 0), "execute");

        // Ctrl+L to clear
        queryEditor.getInputMap().put(KeyStroke.getKeyStroke(KeyEvent.VK_L, KeyEvent.CTRL_DOWN_MASK), "clear");
        queryEditor.getActionMap().put("clear", new AbstractAction() {
            @Override
            public void actionPerformed(ActionEvent e) {
                queryEditor.setText("");
            }
        });
    }

    private void handleConnection() {
        if (isConnected) {
            disconnect();
        } else {
            connect();
        }
    }

    private void connect() {
        String server = serverField.getText().trim();
        String port = portField.getText().trim();
        String username = usernameField.getText().trim();
        String password = new String(passwordField.getPassword());

        if (server.isEmpty() || port.isEmpty() || username.isEmpty()) {
            showError("Please fill in all connection fields");
            return;
        }

        try {
            serverHost = server;
            serverPort = Integer.parseInt(port);
        } catch (NumberFormatException e) {
            showError("Invalid port number");
            return;
        }

        // Perform connection in background
        SwingWorker<Boolean, Void> worker = new SwingWorker<Boolean, Void>() {
            @Override
            protected Boolean doInBackground() throws Exception {
                return performLogin(username, password);
            }

            @Override
            protected void done() {
                try {
                    boolean success = get();
                    if (success) {
                        isConnected = true;
                        currentUser = username;
                        connectButton.setText("Disconnect");
                        connectButton.setBackground(ERROR_COLOR);
                        executeButton.setEnabled(true);
                        executeScriptButton.setEnabled(true);
                        statusLabel.setText("Connected as " + username + " @ " + server + ":" + port);
                        statusLabel.setForeground(SUCCESS_COLOR);
                        log("Connected successfully as " + username);

                        // Disable connection form
                        serverField.setEnabled(false);
                        portField.setEnabled(false);
                        usernameField.setEnabled(false);
                        passwordField.setEnabled(false);

                    } else {
                        showError("Login failed. Please check your credentials.");
                    }
                } catch (Exception e) {
                    showError("Connection failed: " + e.getMessage());
                }
            }
        };

        worker.execute();
    }

    private void disconnect() {
        if (sessionId != null) {
            try {
                performLogout();
            } catch (Exception e) {
                log("Error during logout: " + e.getMessage());
            }
        }

        isConnected = false;
        sessionId = null;
        currentUser = null;
        connectButton.setText("Connect");
        connectButton.setBackground(PRIMARY_COLOR);
        executeButton.setEnabled(false);
        executeScriptButton.setEnabled(false);
        statusLabel.setText("Ready - Not Connected");
        statusLabel.setForeground(SECONDARY_TEXT_COLOR);

        // Re-enable connection form
        serverField.setEnabled(true);
        portField.setEnabled(true);
        usernameField.setEnabled(true);
        passwordField.setEnabled(true);

        log("Disconnected from server");
    }

    private boolean performLogin(String username, String password) {
        try {
            Map<String, Object> request = new HashMap<>();
            request.put("action", "login");
            request.put("username", username);
            request.put("password", password);

            JsonNode response = sendRequest(request);

            if (response.has("session_id")) {
                sessionId = response.get("session_id").asText();
                return true;
            }

            return false;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private void performLogout() throws Exception {
        if (sessionId == null)
            return;

        Map<String, Object> request = new HashMap<>();
        request.put("action", "logout");
        request.put("session_id", sessionId);

        sendRequest(request);
    }

    private void executeQuery() {
        if (!isConnected) {
            showError("Not connected to server");
            return;
        }

        String query = queryEditor.getText().trim();
        if (query.isEmpty()) {
            showError("Please enter a query");
            return;
        }

        // Add to history
        queryHistory.add(query);
        historyIndex = queryHistory.size() - 1;

        // Execute in background
        SwingWorker<JsonNode, Void> worker = new SwingWorker<JsonNode, Void>() {
            @Override
            protected JsonNode doInBackground() throws Exception {
                log("Executing query...");

                Map<String, Object> request = new HashMap<>();
                request.put("action", "query");
                request.put("session_id", sessionId);
                request.put("query", query);

                return sendRequest(request);
            }

            @Override
            protected void done() {
                try {
                    JsonNode response = get();
                    displayQueryResult(response);
                } catch (Exception e) {
                    showError("Query execution failed: " + e.getMessage());
                    log("ERROR: " + e.getMessage());
                }
            }
        };

        worker.execute();
    }

    private void executeScript() {
        if (!isConnected) {
            showError("Not connected to server");
            return;
        }

        String script = queryEditor.getText().trim();
        if (script.isEmpty()) {
            showError("Please enter or load a script");
            return;
        }

        // Parse script into individual statements
        List<String> statements = parseScript(script);

        if (statements.isEmpty()) {
            showError("No valid SQL statements found in script");
            return;
        }

        log("Executing script with " + statements.size() + " statements...");

        // Execute statements sequentially
        SwingWorker<Void, String> worker = new SwingWorker<Void, String>() {
            @Override
            protected Void doInBackground() throws Exception {
                int successCount = 0;
                int errorCount = 0;

                for (int i = 0; i < statements.size(); i++) {
                    String statement = statements.get(i).trim();
                    if (statement.isEmpty() || statement.startsWith("--")) {
                        continue;
                    }

                    publish("Executing statement " + (i + 1) + "/" + statements.size() + ": " +
                            (statement.length() > 50 ? statement.substring(0, 50) + "..." : statement));

                    try {
                        Map<String, Object> request = new HashMap<>();
                        request.put("action", "query");
                        request.put("session_id", sessionId);
                        request.put("query", statement);

                        JsonNode response = sendRequest(request);

                        if (response.has("error")) {
                            publish("ERROR in statement " + (i + 1) + ": " + response.get("error").asText());
                            errorCount++;
                        } else {
                            publish("SUCCESS: Statement " + (i + 1) + " executed");
                            successCount++;
                        }

                        // Small delay to prevent overwhelming the server
                        Thread.sleep(100);

                    } catch (Exception e) {
                        publish("ERROR in statement " + (i + 1) + ": " + e.getMessage());
                        errorCount++;
                    }
                }

                publish("Script execution completed. Success: " + successCount + ", Errors: " + errorCount);
                return null;
            }

            @Override
            protected void process(List<String> chunks) {
                for (String message : chunks) {
                    log(message);
                }
            }
        };

        worker.execute();
    }

    private List<String> parseScript(String script) {
        List<String> statements = new ArrayList<>();

        // Split by semicolon, but handle BATCH INSERT specially
        String[] lines = script.split("\n");
        StringBuilder currentStatement = new StringBuilder();
        boolean inBatchInsert = false;
        int openParens = 0;
        int closeParens = 0;

        for (String line : lines) {
            line = line.trim();

            // Skip empty lines and comments
            if (line.isEmpty() || line.startsWith("--") || line.startsWith("#")) {
                continue;
            }

            // Check if starting a BATCH INSERT
            if (!inBatchInsert && line.toUpperCase().startsWith("BATCH INSERT")) {
                currentStatement = new StringBuilder(line);
                inBatchInsert = true;
                openParens = line.length() - line.replace("(", "").length();
                closeParens = line.length() - line.replace(")", "").length();
                continue;
            }

            // If in BATCH INSERT, collect lines until complete
            if (inBatchInsert) {
                currentStatement.append(" ").append(line);
                openParens += line.length() - line.replace("(", "").length();
                closeParens += line.length() - line.replace(")", "").length();

                // Check if BATCH INSERT is complete
                if (openParens <= closeParens) {
                    statements.add(currentStatement.toString());
                    currentStatement = new StringBuilder();
                    inBatchInsert = false;
                    openParens = 0;
                    closeParens = 0;
                }
                continue;
            }

            // For regular statements, split by semicolon
            if (line.endsWith(";")) {
                currentStatement.append(" ").append(line.substring(0, line.length() - 1));
                statements.add(currentStatement.toString().trim());
                currentStatement = new StringBuilder();
            } else {
                currentStatement.append(" ").append(line);
            }
        }

        // Add any remaining statement
        if (currentStatement.length() > 0) {
            statements.add(currentStatement.toString().trim());
        }

        return statements;
    }

    private void loadScript() {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setFileFilter(new FileNameExtensionFilter("SQL Files (*.sql)", "sql"));

        int result = fileChooser.showOpenDialog(this);
        if (result == JFileChooser.APPROVE_OPTION) {
            File file = fileChooser.getSelectedFile();
            try {
                String content = new String(Files.readAllBytes(file.toPath()));
                queryEditor.setText(content);
                log("Loaded script: " + file.getName());
            } catch (IOException e) {
                showError("Failed to load script: " + e.getMessage());
            }
        }
    }

    private void saveQuery() {
        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setFileFilter(new FileNameExtensionFilter("SQL Files (*.sql)", "sql"));

        int result = fileChooser.showSaveDialog(this);
        if (result == JFileChooser.APPROVE_OPTION) {
            File file = fileChooser.getSelectedFile();
            if (!file.getName().endsWith(".sql")) {
                file = new File(file.getAbsolutePath() + ".sql");
            }

            try {
                Files.write(file.toPath(), queryEditor.getText().getBytes());
                log("Saved query to: " + file.getName());
            } catch (IOException e) {
                showError("Failed to save query: " + e.getMessage());
            }
        }
    }

    private void displayQueryResult(JsonNode response) {
        if (response.has("error")) {
            showError("Query Error: " + response.get("error").asText());
            log("ERROR: " + response.get("error").asText());
            return;
        }

        // Clear previous results
        resultTable.setModel(new DefaultTableModel());

        if (response.has("columns") && response.has("rows")) {
            // Tabular data
            JsonNode columnsNode = response.get("columns");
            JsonNode rowsNode = response.get("rows");

            String[] columns = new String[columnsNode.size()];
            for (int i = 0; i < columnsNode.size(); i++) {
                columns[i] = columnsNode.get(i).asText();
            }

            Object[][] data = new Object[rowsNode.size()][columns.length];
            for (int i = 0; i < rowsNode.size(); i++) {
                JsonNode row = rowsNode.get(i);
                for (int j = 0; j < row.size(); j++) {
                    JsonNode cell = row.get(j);
                    data[i][j] = cell.isNull() ? "NULL" : cell.asText();
                }
            }

            DefaultTableModel model = new DefaultTableModel(data, columns) {
                @Override
                public boolean isCellEditable(int row, int column) {
                    return false;
                }
            };

            resultTable.setModel(model);

            // Auto-resize columns
            for (int i = 0; i < resultTable.getColumnCount(); i++) {
                resultTable.getColumnModel().getColumn(i).setPreferredWidth(120);
            }

            log("Query completed successfully. " + data.length + " rows returned.");

        } else if (response.has("message")) {
            log("SUCCESS: " + response.get("message").asText());

            if (response.has("rows_affected")) {
                log("Rows affected: " + response.get("rows_affected").asInt());
            }
        } else {
            log("Query executed successfully");
        }
    }

    private JsonNode sendRequest(Map<String, Object> request) throws Exception {
        try (Socket socket = new Socket(serverHost, serverPort)) {
            // Send request
            String jsonRequest = objectMapper.writeValueAsString(request);
            byte[] data = jsonRequest.getBytes("UTF-8");

            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeInt(data.length);
            out.write(data);
            out.flush();

            // Receive response
            DataInputStream in = new DataInputStream(socket.getInputStream());
            int responseLength = in.readInt();
            byte[] responseData = new byte[responseLength];
            in.readFully(responseData);

            String jsonResponse = new String(responseData, "UTF-8");
            return objectMapper.readTree(jsonResponse);
        }
    }

    private void log(String message) {
        SwingUtilities.invokeLater(() -> {
            String timestamp = new SimpleDateFormat("HH:mm:ss").format(new Date());
            logArea.append("[" + timestamp + "] " + message + "\n");
            logArea.setCaretPosition(logArea.getDocument().getLength());
        });
    }

    private void showError(String message) {
        JOptionPane.showMessageDialog(this, message, "Error", JOptionPane.ERROR_MESSAGE);
        log("ERROR: " + message);
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            new ModernHMSSQLClient().setVisible(true);
        });
    }
}
