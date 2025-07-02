package com.pyhmssql.client.modern;

/**
 * Simple launcher for the Modern HMSSQL Client
 */
public class Launcher {
    public static void main(String[] args) {
        // Set system properties for better UI
        System.setProperty("awt.useSystemAAFontSettings", "on");
        System.setProperty("swing.aatext", "true");
        System.setProperty("swing.plaf.metal.controlFont", "Dialog-12");
        System.setProperty("swing.plaf.metal.userFont", "Dialog-12");

        // Launch the modern client
        ModernHMSSQLClient.main(args);
    }
}
