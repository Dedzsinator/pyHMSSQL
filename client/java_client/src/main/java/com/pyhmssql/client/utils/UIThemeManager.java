package com.pyhmssql.client.utils;

import javafx.scene.paint.Color;
import javafx.scene.Scene;
import javafx.scene.Node;
import javafx.scene.Parent;
import java.util.HashMap;
import java.util.Map;
import java.util.prefs.Preferences;

/**
 * Manages UI themes and color customization
 */
public class UIThemeManager {
    private static UIThemeManager instance;
    private final Preferences prefs;
    private final Map<String, Color> currentTheme;
    private Scene currentScene;

    // Default theme colors
    public static final Map<String, Color> DEFAULT_THEME = new HashMap<>();
    static {
        DEFAULT_THEME.put("background", Color.WHITE);
        DEFAULT_THEME.put("foreground", Color.BLACK);
        DEFAULT_THEME.put("accent", Color.web("#0078d4"));
        DEFAULT_THEME.put("secondary", Color.web("#f0f0f0"));
        DEFAULT_THEME.put("border", Color.web("#cccccc"));
        DEFAULT_THEME.put("menubar", Color.web("#f8f8f8"));
        DEFAULT_THEME.put("toolbar", Color.web("#f0f0f0"));
        DEFAULT_THEME.put("button", Color.web("#e1e1e1"));
        DEFAULT_THEME.put("button_hover", Color.web("#d0d0d0"));
        DEFAULT_THEME.put("text_background", Color.WHITE);
        DEFAULT_THEME.put("text_foreground", Color.BLACK);
        DEFAULT_THEME.put("selection", Color.web("#0078d4"));
        DEFAULT_THEME.put("tab_background", Color.web("#f8f8f8"));
        DEFAULT_THEME.put("tab_active", Color.WHITE);
        DEFAULT_THEME.put("panel_background", Color.web("#f5f5f5"));
        DEFAULT_THEME.put("dialog_background", Color.WHITE);
        DEFAULT_THEME.put("error", Color.web("#d13438"));
        DEFAULT_THEME.put("warning", Color.web("#ff8c00"));
        DEFAULT_THEME.put("success", Color.web("#107c10"));
        DEFAULT_THEME.put("info", Color.web("#0078d4"));
    }

    // Dark theme colors
    public static final Map<String, Color> DARK_THEME = new HashMap<>();
    static {
        DARK_THEME.put("background", Color.web("#2d2d30"));
        DARK_THEME.put("foreground", Color.web("#cccccc"));
        DARK_THEME.put("accent", Color.web("#0e639c"));
        DARK_THEME.put("secondary", Color.web("#3e3e42"));
        DARK_THEME.put("border", Color.web("#3f3f46"));
        DARK_THEME.put("menubar", Color.web("#2d2d30"));
        DARK_THEME.put("toolbar", Color.web("#2d2d30"));
        DARK_THEME.put("button", Color.web("#3e3e42"));
        DARK_THEME.put("button_hover", Color.web("#4e4e52"));
        DARK_THEME.put("text_background", Color.web("#1e1e1e"));
        DARK_THEME.put("text_foreground", Color.web("#cccccc"));
        DARK_THEME.put("selection", Color.web("#264f78"));
        DARK_THEME.put("tab_background", Color.web("#2d2d30"));
        DARK_THEME.put("tab_active", Color.web("#3e3e42"));
        DARK_THEME.put("panel_background", Color.web("#252526"));
        DARK_THEME.put("dialog_background", Color.web("#2d2d30"));
        DARK_THEME.put("error", Color.web("#f14c4c"));
        DARK_THEME.put("warning", Color.web("#ffcc02"));
        DARK_THEME.put("success", Color.web("#89d185"));
        DARK_THEME.put("info", Color.web("#3794ff"));
    }

    private UIThemeManager() {
        prefs = Preferences.userNodeForPackage(UIThemeManager.class);
        currentTheme = new HashMap<>();
        loadTheme();
    }

    public static UIThemeManager getInstance() {
        if (instance == null) {
            instance = new UIThemeManager();
        }
        return instance;
    }

    public void setScene(Scene scene) {
        this.currentScene = scene;
        applyTheme();
    }

    public Color getColor(String key) {
        return currentTheme.getOrDefault(key, DEFAULT_THEME.get(key));
    }

    public void setColor(String key, Color color) {
        currentTheme.put(key, color);
        saveColor(key, color);
        if (currentScene != null) {
            applyTheme();
        }
    }

    public void applyPresetTheme(Map<String, Color> theme) {
        currentTheme.clear();
        currentTheme.putAll(theme);
        saveTheme();
        if (currentScene != null) {
            applyTheme();
        }
    }

    private void loadTheme() {
        currentTheme.clear();
        for (String key : DEFAULT_THEME.keySet()) {
            String colorStr = prefs.get("color_" + key, null);
            if (colorStr != null) {
                try {
                    currentTheme.put(key, Color.web(colorStr));
                } catch (Exception e) {
                    currentTheme.put(key, DEFAULT_THEME.get(key));
                }
            } else {
                currentTheme.put(key, DEFAULT_THEME.get(key));
            }
        }
    }

    private void saveTheme() {
        for (Map.Entry<String, Color> entry : currentTheme.entrySet()) {
            saveColor(entry.getKey(), entry.getValue());
        }
    }

    private void saveColor(String key, Color color) {
        prefs.put("color_" + key, toHexString(color));
    }

    private void applyTheme() {
        if (currentScene == null)
            return;

        // Generate CSS from current theme
        String css = generateCSS();

        // Clear existing stylesheets and add new one
        currentScene.getStylesheets().clear();

        // Create a data URL with the CSS
        String dataUrl = "data:text/css;base64," +
                java.util.Base64.getEncoder().encodeToString(css.getBytes());
        currentScene.getStylesheets().add(dataUrl);
    }

    private String generateCSS() {
        StringBuilder css = new StringBuilder();

        // Root styling
        css.append(".root {\n");
        css.append("  -fx-base: ").append(toHexString(getColor("background"))).append(";\n");
        css.append("  -fx-background: ").append(toHexString(getColor("background"))).append(";\n");
        css.append("  -fx-control-inner-background: ").append(toHexString(getColor("text_background"))).append(";\n");
        css.append("}\n\n");

        // MenuBar
        css.append(".menu-bar {\n");
        css.append("  -fx-background-color: ").append(toHexString(getColor("menubar"))).append(";\n");
        css.append("  -fx-border-color: ").append(toHexString(getColor("border"))).append(";\n");
        css.append("  -fx-border-width: 0 0 1 0;\n");
        css.append("}\n\n");

        css.append(".menu-bar .label {\n");
        css.append("  -fx-text-fill: ").append(toHexString(getColor("foreground"))).append(";\n");
        css.append("}\n\n");

        // ToolBar
        css.append(".tool-bar {\n");
        css.append("  -fx-background-color: ").append(toHexString(getColor("toolbar"))).append(";\n");
        css.append("  -fx-border-color: ").append(toHexString(getColor("border"))).append(";\n");
        css.append("}\n\n");

        // Buttons
        css.append(".button {\n");
        css.append("  -fx-background-color: ").append(toHexString(getColor("button"))).append(";\n");
        css.append("  -fx-text-fill: ").append(toHexString(getColor("foreground"))).append(";\n");
        css.append("  -fx-border-color: ").append(toHexString(getColor("border"))).append(";\n");
        css.append("}\n\n");

        css.append(".button:hover {\n");
        css.append("  -fx-background-color: ").append(toHexString(getColor("button_hover"))).append(";\n");
        css.append("}\n\n");

        css.append(".button:pressed {\n");
        css.append("  -fx-background-color: ").append(toHexString(getColor("accent"))).append(";\n");
        css.append("}\n\n");

        // Text controls
        css.append(".text-field, .text-area, .combo-box {\n");
        css.append("  -fx-background-color: ").append(toHexString(getColor("text_background"))).append(";\n");
        css.append("  -fx-text-fill: ").append(toHexString(getColor("text_foreground"))).append(";\n");
        css.append("  -fx-border-color: ").append(toHexString(getColor("border"))).append(";\n");
        css.append("}\n\n");

        // TabPane
        css.append(".tab-pane {\n");
        css.append("  -fx-background-color: ").append(toHexString(getColor("tab_background"))).append(";\n");
        css.append("}\n\n");

        css.append(".tab {\n");
        css.append("  -fx-background-color: ").append(toHexString(getColor("tab_background"))).append(";\n");
        css.append("}\n\n");

        css.append(".tab:selected {\n");
        css.append("  -fx-background-color: ").append(toHexString(getColor("tab_active"))).append(";\n");
        css.append("}\n\n");

        css.append(".tab .tab-label {\n");
        css.append("  -fx-text-fill: ").append(toHexString(getColor("foreground"))).append(";\n");
        css.append("}\n\n");

        // Panels and Panes
        css.append(".split-pane, .border-pane, .v-box, .h-box {\n");
        css.append("  -fx-background-color: ").append(toHexString(getColor("panel_background"))).append(";\n");
        css.append("}\n\n");

        // Tables
        css.append(".table-view {\n");
        css.append("  -fx-background-color: ").append(toHexString(getColor("text_background"))).append(";\n");
        css.append("  -fx-control-inner-background: ").append(toHexString(getColor("text_background"))).append(";\n");
        css.append("  -fx-selection-bar: ").append(toHexString(getColor("selection"))).append(";\n");
        css.append("}\n\n");

        css.append(".table-view .label {\n");
        css.append("  -fx-text-fill: ").append(toHexString(getColor("text_foreground"))).append(";\n");
        css.append("}\n\n");

        // TreeView (for DbExplorer)
        css.append(".tree-view {\n");
        css.append("  -fx-background-color: ").append(toHexString(getColor("text_background"))).append(";\n");
        css.append("}\n\n");

        css.append(".tree-cell {\n");
        css.append("  -fx-text-fill: ").append(toHexString(getColor("text_foreground"))).append(";\n");
        css.append("}\n\n");

        css.append(".tree-cell:selected {\n");
        css.append("  -fx-background-color: ").append(toHexString(getColor("selection"))).append(";\n");
        css.append("}\n\n");

        // Labels
        css.append(".label {\n");
        css.append("  -fx-text-fill: ").append(toHexString(getColor("foreground"))).append(";\n");
        css.append("}\n\n");

        // CodeArea for SQL highlighting
        css.append(".code-area {\n");
        css.append("  -fx-background-color: ").append(toHexString(getColor("text_background"))).append(";\n");
        css.append("}\n\n");

        css.append(".code-area .text {\n");
        css.append("  -fx-fill: ").append(toHexString(getColor("text_foreground"))).append(";\n");
        css.append("}\n\n");

        return css.toString();
    }

    public static String toHexString(Color color) {
        return String.format("#%02X%02X%02X",
                (int) (color.getRed() * 255),
                (int) (color.getGreen() * 255),
                (int) (color.getBlue() * 255));
    }

    public Map<String, Color> getCurrentTheme() {
        return new HashMap<>(currentTheme);
    }

    public void resetToDefault() {
        applyPresetTheme(DEFAULT_THEME);
    }

    public void applyDarkTheme() {
        applyPresetTheme(DARK_THEME);
    }
}
