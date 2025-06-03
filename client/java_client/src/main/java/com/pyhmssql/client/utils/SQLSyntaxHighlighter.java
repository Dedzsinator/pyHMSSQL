package com.pyhmssql.client.utils;

import org.fxmisc.richtext.CodeArea;
import org.fxmisc.richtext.model.StyleSpans;
import org.fxmisc.richtext.model.StyleSpansBuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL Syntax Highlighter for CodeArea components
 */
public class SQLSyntaxHighlighter {

    // SQL Keywords
    private static final String[] KEYWORDS = new String[] {
            "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
            "TABLE", "INDEX", "DATABASE", "SCHEMA", "VIEW", "TRIGGER", "PROCEDURE", "FUNCTION",
            "INTO", "VALUES", "SET", "AND", "OR", "NOT", "NULL", "IS", "IN", "LIKE", "BETWEEN",
            "ORDER", "BY", "GROUP", "HAVING", "LIMIT", "OFFSET", "JOIN", "INNER", "LEFT", "RIGHT",
            "FULL", "OUTER", "CROSS", "ON", "UNION", "INTERSECT", "EXCEPT", "AS", "DISTINCT",
            "COUNT", "SUM", "AVG", "MIN", "MAX", "CASE", "WHEN", "THEN", "ELSE", "END",
            "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE", "CHECK", "DEFAULT", "AUTO_INCREMENT",
            "BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION", "SAVEPOINT", "GRANT", "REVOKE", "DENY",
            "USER", "ROLE", "PERMISSION", "IF", "EXISTS", "CASCADE", "RESTRICT", "NO", "ACTION",
            "SHOW", "DESCRIBE", "EXPLAIN", "USE", "BACKUP", "RESTORE", "DUMP", "LOAD", "CALL",
            "EXEC", "EXECUTE", "RETURN", "DECLARE", "CURSOR", "FETCH", "CLOSE", "OPEN",
            "WITH", "RECURSIVE", "CTE", "OVER", "PARTITION", "ROW_NUMBER", "RANK", "DENSE_RANK",
            "FIRST_VALUE", "LAST_VALUE", "LAG", "LEAD", "NTILE", "PERCENT_RANK", "CUME_DIST"
    };

    // Data Types
    private static final String[] TYPES = new String[] {
            "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "DECIMAL", "NUMERIC", "FLOAT", "REAL", "DOUBLE",
            "VARCHAR", "CHAR", "TEXT", "NVARCHAR", "NCHAR", "NTEXT", "CLOB", "BLOB",
            "DATE", "TIME", "DATETIME", "TIMESTAMP", "YEAR", "INTERVAL",
            "BOOLEAN", "BOOL", "BIT", "BINARY", "VARBINARY", "IMAGE",
            "JSON", "XML", "UUID", "GUID", "MONEY", "SMALLMONEY"
    };

    // Operators
    private static final String[] OPERATORS = new String[] {
            "=", "<>", "!=", "<", ">", "<=", ">=", "+", "-", "*", "/", "%", "||", "&&"
    };

    private static final String KEYWORD_PATTERN = "\\b(" + String.join("|", KEYWORDS) + ")\\b";
    private static final String TYPE_PATTERN = "\\b(" + String.join("|", TYPES) + ")\\b";
    private static final String OPERATOR_PATTERN = "("
            + String.join("|", OPERATORS).replace("+", "\\+").replace("*", "\\*").replace("(", "\\(")
                    .replace(")", "\\)").replace("|", "\\|").replace("[", "\\[").replace("]", "\\]")
            + ")";
    private static final String STRING_PATTERN = "'([^'\\\\]|\\\\.)*'";
    private static final String NUMBER_PATTERN = "\\b\\d+(\\.\\d+)?\\b";
    private static final String COMMENT_PATTERN = "--[^\r\n]*" + "|" + "/\\*(.|\\R)*?\\*/";
    private static final String IDENTIFIER_PATTERN = "\\b[a-zA-Z_][a-zA-Z0-9_]*\\b";

    private static final Pattern PATTERN = Pattern.compile(
            "(?<KEYWORD>" + KEYWORD_PATTERN + ")" +
                    "|(?<TYPE>" + TYPE_PATTERN + ")" +
                    "|(?<OPERATOR>" + OPERATOR_PATTERN + ")" +
                    "|(?<STRING>" + STRING_PATTERN + ")" +
                    "|(?<NUMBER>" + NUMBER_PATTERN + ")" +
                    "|(?<COMMENT>" + COMMENT_PATTERN + ")" +
                    "|(?<IDENTIFIER>" + IDENTIFIER_PATTERN + ")",
            Pattern.CASE_INSENSITIVE);

    /**
     * Apply syntax highlighting to a CodeArea
     * 
     * @param codeArea The CodeArea to highlight
     */
    public static void applySyntaxHighlighting(CodeArea codeArea) {
        codeArea.richChanges()
                .filter(ch -> !ch.getInserted().equals(ch.getRemoved()))
                .subscribe(change -> {
                    codeArea.setStyleSpans(0, computeHighlighting(codeArea.getText()));
                });
    }

    /**
     * Compute syntax highlighting for SQL text
     * 
     * @param text The SQL text to highlight
     * @return StyleSpans for the text
     */
    public static StyleSpans<Collection<String>> computeHighlighting(String text) {
        Matcher matcher = PATTERN.matcher(text);
        int lastKwEnd = 0;
        StyleSpansBuilder<Collection<String>> spansBuilder = new StyleSpansBuilder<>();

        while (matcher.find()) {
            String styleClass = matcher.group("KEYWORD") != null ? "sql-keyword"
                    : matcher.group("TYPE") != null ? "sql-type"
                            : matcher.group("OPERATOR") != null ? "sql-operator"
                                    : matcher.group("STRING") != null ? "sql-string"
                                            : matcher.group("NUMBER") != null ? "sql-number"
                                                    : matcher.group("COMMENT") != null ? "sql-comment"
                                                            : matcher.group("IDENTIFIER") != null ? "sql-identifier"
                                                                    : null;

            spansBuilder.add(Collections.emptyList(), matcher.start() - lastKwEnd);
            if (styleClass != null) {
                spansBuilder.add(Collections.singleton(styleClass), matcher.end() - matcher.start());
            } else {
                spansBuilder.add(Collections.emptyList(), matcher.end() - matcher.start());
            }
            lastKwEnd = matcher.end();
        }
        spansBuilder.add(Collections.emptyList(), text.length() - lastKwEnd);
        return spansBuilder.create();
    }

    /**
     * Get the CSS content for SQL syntax highlighting
     * 
     * @return CSS string
     */
    public static String getSQLHighlightingCSS() {
        return ".sql-keyword {\n" +
                "    -fx-fill: #0000FF;\n" +
                "    -fx-font-weight: bold;\n" +
                "}\n" +
                "\n" +
                ".sql-type {\n" +
                "    -fx-fill: #008080;\n" +
                "    -fx-font-weight: bold;\n" +
                "}\n" +
                "\n" +
                ".sql-operator {\n" +
                "    -fx-fill: #FF6600;\n" +
                "    -fx-font-weight: bold;\n" +
                "}\n" +
                "\n" +
                ".sql-string {\n" +
                "    -fx-fill: #008000;\n" +
                "}\n" +
                "\n" +
                ".sql-number {\n" +
                "    -fx-fill: #FF0000;\n" +
                "}\n" +
                "\n" +
                ".sql-comment {\n" +
                "    -fx-fill: #808080;\n" +
                "    -fx-font-style: italic;\n" +
                "}\n" +
                "\n" +
                ".sql-identifier {\n" +
                "    -fx-fill: #000000;\n" +
                "}\n" +
                "\n" +
                ".code-area {\n" +
                "    -fx-background-color: #FFFFFF;\n" +
                "    -fx-font-family: 'Courier New', monospace;\n" +
                "    -fx-font-size: 12px;\n" +
                "}\n" +
                "\n" +
                ".code-area .text {\n" +
                "    -fx-fill: #000000;\n" +
                "}\n" +
                "\n" +
                ".code-area .line-number {\n" +
                "    -fx-background-color: #F0F0F0;\n" +
                "    -fx-text-fill: #808080;\n" +
                "    -fx-border-color: #CCCCCC;\n" +
                "    -fx-border-width: 0 1px 0 0;\n" +
                "    -fx-padding: 0 5px;\n" +
                "    -fx-font-family: 'Courier New', monospace;\n" +
                "    -fx-font-size: 10px;\n" +
                "}\n" +
                "\n" +
                ".code-area .selection {\n" +
                "    -fx-fill: #3399FF;\n" +
                "}";
    }
}
