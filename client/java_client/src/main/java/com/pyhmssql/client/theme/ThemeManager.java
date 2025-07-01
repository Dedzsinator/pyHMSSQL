package com.pyhmssql.client.theme;

import com.pyhmssql.client.config.ConfigurationManager;
import javafx.scene.Scene;
import javafx.scene.paint.Color;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Modern theme manager for pyHMSSQL client.
 * Supports multiple themes with dark/light mode switching.
 */
public class ThemeManager {
    private static final Logger logger = LoggerFactory.getLogger(ThemeManager.class);
    private static ThemeManager instance;
    
    private final Map<String, Theme> themes = new ConcurrentHashMap<>();
    private final Map<Scene, String> sceneThemes = new ConcurrentHashMap<>();
    private String currentTheme;
    
    private ThemeManager() {
        initializeBuiltinThemes();
        this.currentTheme = ConfigurationManager.UI.getTheme();
    }
    
    public static ThemeManager getInstance() {
        if (instance == null) {
            synchronized (ThemeManager.class) {
                if (instance == null) {
                    instance = new ThemeManager();
                }
            }
        }
        return instance;
    }
    
    private void initializeBuiltinThemes() {
        // Dark theme
        themes.put("dark", new Theme("dark", "Dark Theme", 
                "/styles/dark-theme.css",
                Color.rgb(43, 43, 43),      // background
                Color.rgb(60, 60, 60),      // surface
                Color.rgb(30, 144, 255),    // primary
                Color.rgb(255, 255, 255),   // on-background
                Color.rgb(255, 69, 0),      // error
                Color.rgb(0, 255, 127)      // success
        ));
        
        // Light theme
        themes.put("light", new Theme("light", "Light Theme",
                "/styles/light-theme.css",
                Color.rgb(248, 249, 250),   // background
                Color.rgb(255, 255, 255),   // surface
                Color.rgb(0, 123, 255),     // primary
                Color.rgb(33, 37, 41),      // on-background
                Color.rgb(220, 53, 69),     // error
                Color.rgb(40, 167, 69)      // success
        ));
        
        // Modern dark theme
        themes.put("modern-dark", new Theme("modern-dark", "Modern Dark",
                "/styles/modern-dark-theme.css",
                Color.rgb(18, 18, 18),      // background
                Color.rgb(32, 32, 32),      // surface
                Color.rgb(101, 85, 255),    // primary
                Color.rgb(255, 255, 255),   // on-background
                Color.rgb(255, 82, 82),     // error
                Color.rgb(105, 240, 174)    // success
        ));
        
        // High contrast theme
        themes.put("high-contrast", new Theme("high-contrast", "High Contrast",
                "/styles/high-contrast-theme.css",
                Color.rgb(0, 0, 0),         // background
                Color.rgb(16, 16, 16),      // surface
                Color.rgb(255, 255, 0),     // primary
                Color.rgb(255, 255, 255),   // on-background
                Color.rgb(255, 0, 0),       // error
                Color.rgb(0, 255, 0)        // success
        ));
        
        logger.info("Initialized {} built-in themes", themes.size());
    }
    
    public void applyTheme(Scene scene) {
        applyTheme(scene, currentTheme);
    }
    
    public void applyTheme(Scene scene, String themeName) {
        if (scene == null) {
            logger.warn("Cannot apply theme to null scene");
            return;
        }
        
        Theme theme = themes.get(themeName);
        if (theme == null) {
            logger.warn("Theme not found: {}, using default", themeName);
            theme = themes.get("dark");
        }
        
        try {
            // Clear existing stylesheets
            scene.getStylesheets().clear();
            
            // Add base stylesheets
            scene.getStylesheets().add(getClass().getResource("/styles/base.css").toExternalForm());
            
            // Add theme-specific stylesheet
            URL themeUrl = getClass().getResource(theme.getStylesheetPath());
            if (themeUrl != null) {
                scene.getStylesheets().add(themeUrl.toExternalForm());
            } else {
                logger.warn("Theme stylesheet not found: {}", theme.getStylesheetPath());
            }
            
            // Track scene theme
            sceneThemes.put(scene, themeName);
            
            logger.debug("Applied theme '{}' to scene", themeName);
            
        } catch (Exception e) {
            logger.error("Failed to apply theme: {}", themeName, e);
        }
    }
    
    public void setCurrentTheme(String themeName) {
        if (themes.containsKey(themeName)) {
            this.currentTheme = themeName;
            
            // Apply to all tracked scenes
            sceneThemes.keySet().forEach(scene -> applyTheme(scene, themeName));
            
            logger.info("Changed current theme to: {}", themeName);
        } else {
            logger.warn("Theme not found: {}", themeName);
        }
    }
    
    /**
     * Sets the theme (alias for setCurrentTheme)
     */
    public void setTheme(String themeName) {
        setCurrentTheme(themeName);
    }
    
    public String getCurrentTheme() {
        return currentTheme;
    }
    
    public Theme getTheme(String name) {
        return themes.get(name);
    }
    
    public Theme getCurrentThemeObject() {
        return themes.get(currentTheme);
    }
    
    public Map<String, String> getAvailableThemes() {
        Map<String, String> result = new HashMap<>();
        themes.forEach((key, theme) -> result.put(key, theme.getDisplayName()));
        return result;
    }
    
    public void registerTheme(Theme theme) {
        themes.put(theme.getName(), theme);
        logger.info("Registered custom theme: {}", theme.getName());
    }
    
    public void unregisterTheme(String themeName) {
        if (themes.remove(themeName) != null) {
            logger.info("Unregistered theme: {}", themeName);
        }
    }
    
    public boolean isDarkTheme() {
        return isDarkTheme(currentTheme);
    }
    
    public boolean isDarkTheme(String themeName) {
        Theme theme = themes.get(themeName);
        return theme != null && theme.isDark();
    }
    
    public void toggleTheme() {
        String newTheme = isDarkTheme() ? "light" : "dark";
        setCurrentTheme(newTheme);
    }
    
    public void untrackScene(Scene scene) {
        sceneThemes.remove(scene);
    }
    
    // CSS utilities for dynamic styling
    public String getCSSColor(String colorName) {
        Theme theme = getCurrentThemeObject();
        if (theme == null) return "#000000";
        
        switch (colorName.toLowerCase()) {
            case "background":
                return toHexString(theme.getBackground());
            case "surface":
                return toHexString(theme.getSurface());
            case "primary":
                return toHexString(theme.getPrimary());
            case "on-background":
                return toHexString(theme.getOnBackground());
            case "error":
                return toHexString(theme.getError());
            case "success":
                return toHexString(theme.getSuccess());
            default:
                return "#000000";
        }
    }
    
    private String toHexString(Color color) {
        return String.format("#%02X%02X%02X",
                (int) (color.getRed() * 255),
                (int) (color.getGreen() * 255),
                (int) (color.getBlue() * 255));
    }
    
    // Theme statistics
    public void logThemeStatistics() {
        if (logger.isDebugEnabled()) {
            logger.debug("=== Theme Statistics ===");
            logger.debug("Current theme: {}", currentTheme);
            logger.debug("Available themes: {}", themes.keySet());
            logger.debug("Tracked scenes: {}", sceneThemes.size());
            logger.debug("Is dark theme: {}", isDarkTheme());
            logger.debug("========================");
        }
    }
    
    /**
     * Theme data class
     */
    public static class Theme {
        private final String name;
        private final String displayName;
        private final String stylesheetPath;
        private final Color background;
        private final Color surface;
        private final Color primary;
        private final Color onBackground;
        private final Color error;
        private final Color success;
        
        public Theme(String name, String displayName, String stylesheetPath,
                    Color background, Color surface, Color primary,
                    Color onBackground, Color error, Color success) {
            this.name = name;
            this.displayName = displayName;
            this.stylesheetPath = stylesheetPath;
            this.background = background;
            this.surface = surface;
            this.primary = primary;
            this.onBackground = onBackground;
            this.error = error;
            this.success = success;
        }
        
        public String getName() { return name; }
        public String getDisplayName() { return displayName; }
        public String getStylesheetPath() { return stylesheetPath; }
        public Color getBackground() { return background; }
        public Color getSurface() { return surface; }
        public Color getPrimary() { return primary; }
        public Color getOnBackground() { return onBackground; }
        public Color getError() { return error; }
        public Color getSuccess() { return success; }
        
        public boolean isDark() {
            // Consider a theme dark if the background is darker than middle gray
            double brightness = background.getBrightness();
            return brightness < 0.5;
        }
        
        @Override
        public String toString() {
            return displayName;
        }
    }
}
