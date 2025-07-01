package com.pyhmssql.client.utils;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * Application information utility class.
 * Provides version, build date, and other application metadata.
 */
public class AppInfo {
    private static final String VERSION;
    private static final String BUILD_DATE;
    private static final String BUILD_TIME;
    private static final String GIT_COMMIT;
    private static final String APPLICATION_NAME;
    
    static {
        Properties props = new Properties();
        try (InputStream is = AppInfo.class.getResourceAsStream("/app.properties")) {
            if (is != null) {
                props.load(is);
            }
        } catch (IOException e) {
            // Ignore - will use defaults
        }
        
        VERSION = props.getProperty("app.version", "2.0.0");
        BUILD_DATE = props.getProperty("app.build.date", getCurrentDate());
        BUILD_TIME = props.getProperty("app.build.time", getCurrentTime());
        GIT_COMMIT = props.getProperty("app.git.commit", "unknown");
        APPLICATION_NAME = props.getProperty("app.name", "pyHMSSQL Professional Client");
    }
    
    public static String getVersion() {
        return VERSION;
    }
    
    public static String getBuildDate() {
        return BUILD_DATE;
    }
    
    public static String getBuildTime() {
        return BUILD_TIME;
    }
    
    public static String getGitCommit() {
        return GIT_COMMIT;
    }
    
    public static String getApplicationName() {
        return APPLICATION_NAME;
    }
    
    public static String getFullVersion() {
        return String.format("%s v%s (Build: %s %s)", 
                           APPLICATION_NAME, VERSION, BUILD_DATE, BUILD_TIME);
    }
    
    public static String getBuildInfo() {
        return String.format("Version: %s\nBuild Date: %s %s\nGit Commit: %s", 
                           VERSION, BUILD_DATE, BUILD_TIME, GIT_COMMIT);
    }
    
    private static String getCurrentDate() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }
    
    private static String getCurrentTime() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
    }
    
    // System information
    public static String getJavaVersion() {
        return System.getProperty("java.version");
    }
    
    public static String getJavaVendor() {
        return System.getProperty("java.vendor");
    }
    
    public static String getJavaFxVersion() {
        return System.getProperty("javafx.version", "unknown");
    }
    
    public static String getOperatingSystem() {
        return String.format("%s %s (%s)", 
                           System.getProperty("os.name"),
                           System.getProperty("os.version"),
                           System.getProperty("os.arch"));
    }
    
    public static String getSystemInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("Application: ").append(getFullVersion()).append("\n");
        sb.append("Java Version: ").append(getJavaVersion()).append("\n");
        sb.append("Java Vendor: ").append(getJavaVendor()).append("\n");
        sb.append("JavaFX Version: ").append(getJavaFxVersion()).append("\n");
        sb.append("Operating System: ").append(getOperatingSystem()).append("\n");
        sb.append("Available Processors: ").append(Runtime.getRuntime().availableProcessors()).append("\n");
        sb.append("Max Memory: ").append(Runtime.getRuntime().maxMemory() / (1024 * 1024)).append(" MB\n");
        sb.append("User Home: ").append(System.getProperty("user.home")).append("\n");
        sb.append("User Directory: ").append(System.getProperty("user.dir")).append("\n");
        return sb.toString();
    }
}
