package com.pyhmssql.client.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import javafx.scene.paint.Color;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Professional configuration manager for pyHMSSQL client.
 * Supports HOCON format with environment-specific overrides.
 */
public class ConfigurationManager {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationManager.class);
    private static ConfigurationManager instance;

    private final Config config;
    private final Map<String, Object> cache = new ConcurrentHashMap<>();
    private final String environment;

    private ConfigurationManager() {
        this.environment = detectEnvironment();
        this.config = loadConfiguration();
        logger.info("Configuration loaded for environment: {}", environment);
    }

    public static ConfigurationManager getInstance() {
        if (instance == null) {
            synchronized (ConfigurationManager.class) {
                if (instance == null) {
                    instance = new ConfigurationManager();
                }
            }
        }
        return instance;
    }

    private String detectEnvironment() {
        String env = System.getProperty("app.environment");
        if (env == null) {
            env = System.getenv("APP_ENVIRONMENT");
        }
        if (env == null) {
            boolean devMode = Boolean.parseBoolean(System.getProperty("com.pyhmssql.client.dev", "false"));
            env = devMode ? "dev" : "prod";
        }
        return env.toLowerCase();
    }

    private Config loadConfiguration() {
        try {
            Config baseConfig = ConfigFactory.load("application.conf");

            // Load environment-specific overrides
            Config envConfig = ConfigFactory.empty();
            if (baseConfig.hasPath(environment)) {
                envConfig = baseConfig.getConfig(environment);
            }

            // Load user-specific overrides
            File userConfigFile = new File(System.getProperty("user.home"),
                    ".pyhmssql/client.conf");
            Config userConfig = ConfigFactory.empty();
            if (userConfigFile.exists()) {
                userConfig = ConfigFactory.parseFile(userConfigFile);
                logger.info("Loaded user configuration from: {}", userConfigFile.getAbsolutePath());
            }

            // Resolve configuration with precedence: user > environment > base
            return userConfig
                    .withFallback(envConfig)
                    .withFallback(baseConfig.getConfig("app"))
                    .resolve();

        } catch (Exception e) {
            logger.error("Failed to load configuration, using defaults", e);
            return ConfigFactory.empty();
        }
    }

    // Configuration getters with caching

    public String getString(String path) {
        return getCached(path, () -> config.hasPath(path) ? config.getString(path) : null);
    }

    public String getString(String path, String defaultValue) {
        String value = getString(path);
        return value != null ? value : defaultValue;
    }

    public int getInt(String path) {
        return getCached(path, () -> config.hasPath(path) ? config.getInt(path) : 0);
    }

    public int getInt(String path, int defaultValue) {
        return config.hasPath(path) ? config.getInt(path) : defaultValue;
    }

    public double getDouble(String path) {
        return getCached(path, () -> config.hasPath(path) ? config.getDouble(path) : 0.0);
    }

    public double getDouble(String path, double defaultValue) {
        return config.hasPath(path) ? config.getDouble(path) : defaultValue;
    }

    public boolean getBoolean(String path) {
        return getCached(path, () -> config.hasPath(path) ? config.getBoolean(path) : false);
    }

    public boolean getBoolean(String path, boolean defaultValue) {
        return config.hasPath(path) ? config.getBoolean(path) : defaultValue;
    }

    public Duration getDuration(String path) {
        return getCached(path, () -> config.hasPath(path) ? config.getDuration(path) : Duration.ZERO);
    }

    public Duration getDuration(String path, Duration defaultValue) {
        return config.hasPath(path) ? config.getDuration(path) : defaultValue;
    }

    @SuppressWarnings("unchecked")
    public List<String> getStringList(String path) {
        return getCached(path, () -> config.hasPath(path) ? config.getStringList(path) : List.of());
    }

    public Optional<Config> getConfig(String path) {
        try {
            return config.hasPath(path) ? Optional.of(config.getConfig(path)) : Optional.empty();
        } catch (Exception e) {
            logger.warn("Failed to get config for path: {}", path, e);
            return Optional.empty();
        }
    }

    // UI-specific configuration methods

    public static class UI {
        public static String getTheme() {
            return getInstance().getString("ui.theme", "dark");
        }

        public static String getLanguage() {
            return getInstance().getString("ui.language", "en");
        }

        public static double getScaleFactor() {
            return getInstance().getDouble("ui.scale-factor", 1.0);
        }

        public static class Window {
            public static int getWidth() {
                return getInstance().getInt("ui.window.width", 1400);
            }

            public static int getHeight() {
                return getInstance().getInt("ui.window.height", 900);
            }

            public static int getMinWidth() {
                return getInstance().getInt("ui.window.min-width", 1000);
            }

            public static int getMinHeight() {
                return getInstance().getInt("ui.window.min-height", 600);
            }

            public static boolean isMaximized() {
                return getInstance().getBoolean("ui.window.maximized", false);
            }

            public static boolean rememberPosition() {
                return getInstance().getBoolean("ui.window.remember-position", true);
            }

            public static boolean rememberSize() {
                return getInstance().getBoolean("ui.window.remember-size", true);
            }
        }

        public static class Editor {
            public static String getFontFamily() {
                return getInstance().getString("ui.editor.font-family", "JetBrains Mono");
            }

            public static int getFontSize() {
                return getInstance().getInt("ui.editor.font-size", 14);
            }

            public static int getTabSize() {
                return getInstance().getInt("ui.editor.tab-size", 4);
            }

            public static boolean isAutoCompleteEnabled() {
                return getInstance().getBoolean("ui.editor.auto-complete", true);
            }

            public static boolean isSyntaxHighlightingEnabled() {
                return getInstance().getBoolean("ui.editor.syntax-highlighting", true);
            }

            public static boolean areLineNumbersEnabled() {
                return getInstance().getBoolean("ui.editor.line-numbers", true);
            }

            public static boolean isWordWrapEnabled() {
                return getInstance().getBoolean("ui.editor.word-wrap", false);
            }

            public static boolean isBracketMatchingEnabled() {
                return getInstance().getBoolean("ui.editor.bracket-matching", true);
            }
        }
    }

    // Database configuration
    public static class Database {
        public static Duration getDefaultTimeout() {
            return getInstance().getDuration("database.default-timeout", Duration.ofSeconds(30));
        }

        public static int getMaxConnections() {
            return getInstance().getInt("database.max-connections", 10);
        }

        public static int getConnectionPoolSize() {
            return getInstance().getInt("database.connection-pool-size", 5);
        }

        public static int getRetryAttempts() {
            return getInstance().getInt("database.retry-attempts", 3);
        }

        public static Duration getRetryDelay() {
            return getInstance().getDuration("database.retry-delay", Duration.ofSeconds(1));
        }

        public static class Query {
            public static int getMaxResultRows() {
                return getInstance().getInt("database.query.max-result-rows", 10000);
            }

            public static Duration getTimeout() {
                return getInstance().getDuration("database.query.timeout", Duration.ofMinutes(1));
            }

            public static boolean isAutoCommit() {
                return getInstance().getBoolean("database.query.auto-commit", true);
            }

            public static String getTransactionIsolation() {
                return getInstance().getString("database.query.transaction-isolation", "READ_COMMITTED");
            }
        }
    }

    // Performance configuration
    public static class Performance {
        public static int getUIThreadPoolSize() {
            return getInstance().getInt("performance.ui-thread-pool-size", 4);
        }

        public static int getBackgroundThreadPoolSize() {
            return getInstance().getInt("performance.background-thread-pool-size", 8);
        }

        public static class Cache {
            public static boolean isEnabled() {
                return getInstance().getBoolean("performance.cache.enabled", true);
            }

            public static int getMaxSize() {
                return getInstance().getInt("performance.cache.max-size", 1000);
            }

            public static Duration getTTL() {
                return getInstance().getDuration("performance.cache.ttl", Duration.ofMinutes(5));
            }
        }
    }

    // Security configuration
    public static class Security {
        public static boolean isSSLVerificationEnabled() {
            return getInstance().getBoolean("security.ssl-verification", true);
        }

        public static boolean shouldStorePasswords() {
            return getInstance().getBoolean("security.store-passwords", false);
        }

        public static Duration getSessionTimeout() {
            return getInstance().getDuration("security.session-timeout", Duration.ofHours(1));
        }

        public static boolean isAutoLogoutEnabled() {
            return getInstance().getBoolean("security.auto-logout", true);
        }
    }

    // Keyboard shortcuts
    public static class Shortcuts {
        public static String getExecuteQuery() {
            return getInstance().getString("shortcuts.execute-query", "Ctrl+Enter");
        }

        public static String getNewQuery() {
            return getInstance().getString("shortcuts.new-query", "Ctrl+N");
        }

        public static String getSaveQuery() {
            return getInstance().getString("shortcuts.save-query", "Ctrl+S");
        }

        public static String getOpenQuery() {
            return getInstance().getString("shortcuts.open-query", "Ctrl+O");
        }

        public static String getFormatSQL() {
            return getInstance().getString("shortcuts.format-sql", "Ctrl+Shift+F");
        }

        public static String getToggleComment() {
            return getInstance().getString("shortcuts.toggle-comment", "Ctrl+/");
        }

        public static String getFind() {
            return getInstance().getString("shortcuts.find", "Ctrl+F");
        }

        public static String getReplace() {
            return getInstance().getString("shortcuts.replace", "Ctrl+H");
        }

        public static String getAutoComplete() {
            return getInstance().getString("shortcuts.auto-complete", "Ctrl+Space");
        }
    }

    // Development configuration
    public static class Development {
        public static boolean isDebugMode() {
            return getInstance().getBoolean("development.debug-mode", false);
        }

        public static boolean isMockDataEnabled() {
            return getInstance().getBoolean("development.mock-data", false);
        }

        public static boolean isHotReloadEnabled() {
            return getInstance().getBoolean("development.hot-reload", false);
        }

        public static boolean areDevToolsEnabled() {
            return getInstance().getBoolean("development.dev-tools", false);
        }

        public static boolean isProfilingEnabled() {
            return getInstance().getBoolean("development.profiling", false);
        }
    }

    // Utility methods

    public String getEnvironment() {
        return environment;
    }

    public void clearCache() {
        cache.clear();
        logger.info("Configuration cache cleared");
    }

    public void reloadConfiguration() {
        clearCache();
        // Note: In a real implementation, you might want to reload the config
        logger.info("Configuration cache cleared (reload would require restart)");
    }

    @SuppressWarnings("unchecked")
    private <T> T getCached(String path, CacheLoader<T> loader) {
        return (T) cache.computeIfAbsent(path, k -> {
            try {
                return loader.load();
            } catch (Exception e) {
                logger.warn("Failed to load configuration for path: {}", path, e);
                return null;
            }
        });
    }

    @FunctionalInterface
    private interface CacheLoader<T> {
        T load() throws Exception;
    }

    // Configuration validation
    public boolean validateConfiguration() {
        try {
            // Validate critical configuration paths
            String[] requiredPaths = {
                    "name", "version", "ui.theme", "database.default-timeout"
            };

            for (String path : requiredPaths) {
                if (!config.hasPath(path)) {
                    logger.error("Required configuration path missing: {}", path);
                    return false;
                }
            }

            // Validate value ranges
            if (getInt("ui.window.width") < 800) {
                logger.warn("Window width is too small, using minimum");
            }

            if (getInt("database.max-connections") <= 0) {
                logger.error("Database max connections must be positive");
                return false;
            }

            logger.info("Configuration validation passed");
            return true;

        } catch (Exception e) {
            logger.error("Configuration validation failed", e);
            return false;
        }
    }

    // Debug information
    public void logConfigurationInfo() {
        if (logger.isDebugEnabled()) {
            logger.debug("=== Configuration Information ===");
            logger.debug("Environment: {}", environment);
            logger.debug("App Name: {}", getString("name"));
            logger.debug("App Version: {}", getString("version"));
            logger.debug("Theme: {}", UI.getTheme());
            logger.debug("Debug Mode: {}", Development.isDebugMode());
            logger.debug("Cache Size: {}", cache.size());
            logger.debug("================================");
        }
    }
}
