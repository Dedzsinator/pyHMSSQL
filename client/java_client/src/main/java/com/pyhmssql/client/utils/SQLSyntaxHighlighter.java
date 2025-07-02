package com.pyhmssql.client.utils;

import org.fxmisc.richtext.CodeArea;
import org.fxmisc.richtext.model.StyleSpans;
import org.fxmisc.richtext.model.StyleSpansBuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL syntax highlighter for CodeArea
 */
public class SQLSyntaxHighlighter {

        // SQL Keywords
        private static final String[] KEYWORDS = new String[] {
                        "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
                        "TABLE", "INDEX", "VIEW", "DATABASE", "SCHEMA", "PRIMARY", "KEY", "FOREIGN", "REFERENCES",
                        "NULL", "NOT", "DEFAULT", "AUTO_INCREMENT", "UNIQUE", "CHECK", "CONSTRAINT",
                        "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "JOIN", "ON", "AS", "DISTINCT",
                        "GROUP", "BY", "HAVING", "ORDER", "ASC", "DESC", "LIMIT", "OFFSET",
                        "UNION", "INTERSECT", "EXCEPT", "EXISTS", "IN", "BETWEEN", "LIKE", "IS",
                        "AND", "OR", "CASE", "WHEN", "THEN", "ELSE", "END", "IF", "WHILE", "FOR",
                        "BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION", "START", "SAVEPOINT",
                        "GRANT", "REVOKE", "DENY", "USER", "ROLE", "PRIVILEGES",
                        "COUNT", "SUM", "AVG", "MIN", "MAX", "SUBSTRING", "UPPER", "LOWER", "TRIM",
                        "CAST", "CONVERT", "COALESCE", "ISNULL", "NULLIF"
        };

        // Data Types
        private static final String[] TYPES = new String[] {
                        "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "DECIMAL", "NUMERIC", "FLOAT", "REAL",
                        "DOUBLE",
                        "VARCHAR", "CHAR", "TEXT", "NVARCHAR", "NCHAR", "NTEXT", "BINARY", "VARBINARY", "IMAGE",
                        "DATE", "TIME", "DATETIME", "TIMESTAMP", "YEAR", "BOOLEAN", "BOOL", "BIT",
                        "BLOB", "CLOB", "JSON", "XML", "UUID", "SERIAL", "AUTO_INCREMENT"
        };

        private static final String KEYWORD_PATTERN = "\\b(" + String.join("|", KEYWORDS) + ")\\b";
        private static final String TYPE_PATTERN = "\\b(" + String.join("|", TYPES) + ")\\b";
        private static final String STRING_PATTERN = "'([^'\\\\]|\\\\.)*'";
        private static final String NUMBER_PATTERN = "\\b\\d+(\\.\\d+)?\\b";
        private static final String COMMENT_PATTERN = "--[^\r\n]*" + "|" + "/\\*(.|\\R)*?\\*/";
        private static final String OPERATOR_PATTERN = "[+\\-*/=<>!]+|\\b(AND|OR|NOT|IN|LIKE|BETWEEN|IS|EXISTS)\\b";

        private static final Pattern PATTERN = Pattern.compile(
                        "(?<KEYWORD>" + KEYWORD_PATTERN + ")" +
                                        "|(?<TYPE>" + TYPE_PATTERN + ")" +
                                        "|(?<STRING>" + STRING_PATTERN + ")" +
                                        "|(?<NUMBER>" + NUMBER_PATTERN + ")" +
                                        "|(?<COMMENT>" + COMMENT_PATTERN + ")" +
                                        "|(?<OPERATOR>" + OPERATOR_PATTERN + ")",
                        Pattern.CASE_INSENSITIVE);

        public static void applySyntaxHighlighting(CodeArea codeArea) {
                codeArea.richChanges()
                                .filter(ch -> !ch.getInserted().equals(ch.getRemoved()))
                                .subscribe(change -> {
                                        codeArea.setStyleSpans(0, computeHighlighting(codeArea.getText()));
                                });
        }

        private static StyleSpans<Collection<String>> computeHighlighting(String text) {
                Matcher matcher = PATTERN.matcher(text);
                int lastKwEnd = 0;
                StyleSpansBuilder<Collection<String>> spansBuilder = new StyleSpansBuilder<>();

                while (matcher.find()) {
                        String styleClass = null;
                        if (matcher.group("KEYWORD") != null) {
                                styleClass = "sql-keyword";
                        } else if (matcher.group("TYPE") != null) {
                                styleClass = "sql-type";
                        } else if (matcher.group("STRING") != null) {
                                styleClass = "sql-string";
                        } else if (matcher.group("NUMBER") != null) {
                                styleClass = "sql-number";
                        } else if (matcher.group("COMMENT") != null) {
                                styleClass = "sql-comment";
                        } else if (matcher.group("OPERATOR") != null) {
                                styleClass = "sql-operator";
                        }

                        spansBuilder.add(Collections.emptyList(), matcher.start() - lastKwEnd);
                        spansBuilder.add(Collections.singleton(styleClass), matcher.end() - matcher.start());
                        lastKwEnd = matcher.end();
                }
                spansBuilder.add(Collections.emptyList(), text.length() - lastKwEnd);
                return spansBuilder.create();
        }
}
