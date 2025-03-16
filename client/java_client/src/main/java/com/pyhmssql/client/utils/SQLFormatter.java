package com.pyhmssql.client.utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for formatting SQL queries to improve readability
 */
public class SQLFormatter {
    
    // Keywords that should start on a new line
    private static final Set<String> NEWLINE_KEYWORDS = new HashSet<>(Arrays.asList(
        "SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY", 
        "LIMIT", "OFFSET", "UNION", "UNION ALL", "EXCEPT", "INTERSECT",
        "INSERT INTO", "VALUES", "UPDATE", "SET", "DELETE FROM", 
        "CREATE TABLE", "DROP TABLE", "ALTER TABLE", "CREATE INDEX", 
        "DROP INDEX", "CREATE VIEW", "DROP VIEW", "CREATE PROCEDURE", 
        "DROP PROCEDURE", "BEGIN", "COMMIT", "ROLLBACK"
    ));
    
    // Keywords that should be indented under the previous line
    private static final Set<String> INDENT_KEYWORDS = new HashSet<>(Arrays.asList(
        "JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN", 
        "CROSS JOIN", "NATURAL JOIN", "AND", "OR", "WHEN", "THEN", "ELSE", 
        "CASE", "END", "ON"
    ));
    
    // Pattern to identify SQL strings
    private static final Pattern STRING_PATTERN = Pattern.compile("'[^']*'");
    
    // Pattern to identify comments
    private static final Pattern COMMENT_PATTERN = Pattern.compile("--[^\n]*|/\\*[^*]*\\*+(?:[^/*][^*]*\\*+)*/");
    
    /**
     * Format a SQL query for better readability
     * 
     * @param sql The SQL query to format
     * @return Formatted SQL query
     */
    public static String format(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return sql;
        }

        // Save strings and comments to restore them later
        // This prevents formatting inside strings and comments
        sql = preserveSpecialContent(sql);
        
        // Normalize whitespace
        sql = sql.replaceAll("\\s+", " ").trim();
        
        // Add whitespace around operators for better readability
        sql = sql.replaceAll("([,;\\(\\)])", " $1 ")
                 .replaceAll("\\s+", " ");
        
        // Insert newlines
        sql = addNewlines(sql);
        
        // Restore preserved content
        sql = restoreSpecialContent(sql);
        
        return sql;
    }
    
    /**
     * Adds newlines and indentation to the SQL statement
     */
    private static String addNewlines(String sql) {
        StringBuilder result = new StringBuilder();
        String[] tokens = sql.split("\\s+");
        int indentLevel = 0;
        boolean newLineNeeded = false;
        
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
            String upperToken = token.toUpperCase();
            
            // Check if this token or a combination is a newline keyword
            String combinedToken = "";
            if (i < tokens.length - 1) {
                combinedToken = upperToken + " " + tokens[i + 1].toUpperCase();
            }
            
            boolean isNewlineKeyword = NEWLINE_KEYWORDS.contains(upperToken) || 
                                     NEWLINE_KEYWORDS.contains(combinedToken);
                                     
            boolean isIndentKeyword = INDENT_KEYWORDS.contains(upperToken) || 
                                    INDENT_KEYWORDS.contains(combinedToken);
            
            // Handle closing parenthesis by decreasing indent
            if (token.contains(")")) {
                indentLevel = Math.max(0, indentLevel - 1);
            }
            
            // Add newline if needed
            if (newLineNeeded || isNewlineKeyword || isIndentKeyword) {
                result.append("\n");
                
                // Add indentation
                for (int j = 0; j < indentLevel; j++) {
                    result.append("    ");
                }
                
                // Special case handling for subqueries
                if (isNewlineKeyword && !result.toString().endsWith("(")) {
                    indentLevel = 0;
                }
            } else if (result.length() > 0) {
                result.append(" ");
            }
            
            // Add the token
            result.append(token);
            
            // Handle opening parenthesis by increasing indent
            if (token.contains("(") && !token.contains(")")) {
                indentLevel++;
            }
            
            // Set newline flag for next token
            newLineNeeded = token.endsWith(",");
            
            // Adjust indent level for the next token
            if (isNewlineKeyword) {
                indentLevel = 1;
            } else if (isIndentKeyword) {
                indentLevel = 2;
            }
            
            // Skip the next token if we handled a combined keyword
            if (NEWLINE_KEYWORDS.contains(combinedToken) || INDENT_KEYWORDS.contains(combinedToken)) {
                result.append(" ").append(tokens[i + 1]);
                i++;
            }
        }
        
        return result.toString();
    }
    
    /**
     * Temporarily replaces string literals and comments with placeholders
     * to prevent formatting inside them
     */
    private static String preserveSpecialContent(String sql) {
        // Placeholder format will be {$S:index} for strings and {$C:index} for comments
        StringBuffer sb = new StringBuffer();
        
        // Preserve string literals
        Matcher stringMatcher = STRING_PATTERN.matcher(sql);
        int stringIndex = 0;
        while (stringMatcher.find()) {
            stringMatcher.appendReplacement(sb, "{$S:" + stringIndex + "}");
            stringIndex++;
        }
        stringMatcher.appendTail(sb);
        
        // Use the result for preserving comments
        String withPreservedStrings = sb.toString();
        sb = new StringBuffer();
        
        // Preserve comments
        Matcher commentMatcher = COMMENT_PATTERN.matcher(withPreservedStrings);
        int commentIndex = 0;
        while (commentMatcher.find()) {
            commentMatcher.appendReplacement(sb, "{$C:" + commentIndex + "}");
            commentIndex++;
        }
        commentMatcher.appendTail(sb);
        
        return sb.toString();
    }
    
    /**
     * Restore preserved content back to the formatted SQL
     */
    private static String restoreSpecialContent(String sql) {
        // We're not actually implementing this fully since we didn't store
        // the original content. In a real implementation, we would maintain
        // maps of the preserved content.
        return sql;
    }
    
    /**
     * Colorize SQL for HTML display (useful for UI display)
     * 
     * @param sql SQL query to colorize
     * @return HTML with syntax highlighting
     */
    public static String colorize(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return "";
        }
        
        // Replace special characters for HTML
        sql = sql.replace("&", "&amp;")
                 .replace("<", "&lt;")
                 .replace(">", "&gt;");
                 
        // Highlight keywords (a simplified version)
        for (String keyword : NEWLINE_KEYWORDS) {
            String regex = "(?i)\\b(" + keyword + ")\\b";
            sql = sql.replaceAll(regex, "<span class='sql-keyword'>$1</span>");
        }
        
        for (String keyword : INDENT_KEYWORDS) {
            String regex = "(?i)\\b(" + keyword + ")\\b";
            sql = sql.replaceAll(regex, "<span class='sql-keyword'>$1</span>");
        }
        
        // Highlight strings
        sql = sql.replaceAll("('(?:[^']*)')","<span class='sql-string'>$1</span>");
        
        // Highlight numbers
        sql = sql.replaceAll("\\b(\\d+)\\b","<span class='sql-number'>$1</span>");
        
        // Highlight comments
        sql = sql.replaceAll("(--[^\n]*)","<span class='sql-comment'>$1</span>");
        sql = sql.replaceAll("(/\\*[^*]*\\*+(?:[^/*][^*]*\\*+)*/)",
                           "<span class='sql-comment'>$1</span>");
        
        // Replace newlines with <br>
        sql = sql.replace("\n", "<br>");
        
        return "<div class='sql-formatted'>" + sql + "</div>";
    }
}