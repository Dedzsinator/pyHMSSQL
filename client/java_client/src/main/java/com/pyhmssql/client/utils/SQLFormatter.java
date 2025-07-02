package com.pyhmssql.client.utils;

/**
 * SQL code formatter utility
 */
public class SQLFormatter {

    public static String format(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return sql;
        }

        // Basic SQL formatting
        String formatted = sql
                .replaceAll("(?i)\\bSELECT\\b", "\nSELECT")
                .replaceAll("(?i)\\bFROM\\b", "\nFROM")
                .replaceAll("(?i)\\bWHERE\\b", "\nWHERE")
                .replaceAll("(?i)\\bORDER BY\\b", "\nORDER BY")
                .replaceAll("(?i)\\bGROUP BY\\b", "\nGROUP BY")
                .replaceAll("(?i)\\bHAVING\\b", "\nHAVING")
                .replaceAll("(?i)\\bJOIN\\b", "\n  JOIN")
                .replaceAll("(?i)\\bINNER JOIN\\b", "\n  INNER JOIN")
                .replaceAll("(?i)\\bLEFT JOIN\\b", "\n  LEFT JOIN")
                .replaceAll("(?i)\\bRIGHT JOIN\\b", "\n  RIGHT JOIN")
                .replaceAll("(?i)\\bFULL JOIN\\b", "\n  FULL JOIN")
                .replaceAll("(?i)\\bON\\b", "\n    ON")
                .replaceAll("(?i)\\bAND\\b", "\n  AND")
                .replaceAll("(?i)\\bOR\\b", "\n  OR")
                .replaceAll(",", ",\n  ")
                .replaceAll("\\s+", " ")
                .replaceAll("\\n\\s*\\n", "\n")
                .trim();

        return formatted;
    }
}