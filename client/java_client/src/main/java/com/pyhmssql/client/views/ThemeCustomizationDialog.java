package com.pyhmssql.client.views;

import com.pyhmssql.client.utils.UIThemeManager;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.application.Platform;

import java.util.HashMap;
import java.util.Map;

/**
 * Dialog for customizing UI theme colors
 */
public class ThemeCustomizationDialog extends Dialog<ButtonType> {
    private final UIThemeManager themeManager;
    private final Map<String, ColorPicker> colorPickers;
    private final Map<String, Color> originalColors;

    public ThemeCustomizationDialog() {
        this.themeManager = UIThemeManager.getInstance();
        this.colorPickers = new HashMap<>();
        this.originalColors = new HashMap<>(themeManager.getCurrentTheme());

        initDialog();
        createContent();
        setupButtons();
    }

    private void initDialog() {
        setTitle("Theme Customization");
        setHeaderText("Customize the application theme colors");
        setResizable(true);

        // Set minimum size
        getDialogPane().setPrefSize(600, 700);
    }

    private void createContent() {
        VBox mainContent = new VBox(15);
        mainContent.setPadding(new Insets(20));

        // Preset themes section
        mainContent.getChildren().add(createPresetSection());

        // Custom colors section
        mainContent.getChildren().add(createCustomColorsSection());

        // Preview section
        mainContent.getChildren().add(createPreviewSection());

        ScrollPane scrollPane = new ScrollPane(mainContent);
        scrollPane.setFitToWidth(true);
        scrollPane.setPrefSize(580, 650);

        getDialogPane().setContent(scrollPane);
    }

    private VBox createPresetSection() {
        VBox presetSection = new VBox(10);

        Label presetLabel = new Label("Preset Themes");
        presetLabel.setStyle("-fx-font-weight: bold; -fx-font-size: 14px;");

        HBox presetButtons = new HBox(10);
        presetButtons.setAlignment(Pos.CENTER_LEFT);

        Button lightThemeBtn = new Button("Light Theme");
        lightThemeBtn.setOnAction(e -> {
            applyPresetTheme(UIThemeManager.DEFAULT_THEME);
        });

        Button darkThemeBtn = new Button("Dark Theme");
        darkThemeBtn.setOnAction(e -> {
            applyPresetTheme(UIThemeManager.DARK_THEME);
        });

        Button resetBtn = new Button("Reset to Default");
        resetBtn.setOnAction(e -> {
            applyPresetTheme(UIThemeManager.DEFAULT_THEME);
        });

        presetButtons.getChildren().addAll(lightThemeBtn, darkThemeBtn, resetBtn);
        presetSection.getChildren().addAll(presetLabel, presetButtons);

        return presetSection;
    }

    private VBox createCustomColorsSection() {
        VBox customSection = new VBox(10);

        Label customLabel = new Label("Custom Colors");
        customLabel.setStyle("-fx-font-weight: bold; -fx-font-size: 14px;");

        GridPane colorGrid = new GridPane();
        colorGrid.setHgap(15);
        colorGrid.setVgap(10);
        colorGrid.setPadding(new Insets(10));

        String[] colorCategories = {
                "Background Colors", "Text Colors", "Interface Elements", "Status Colors"
        };

        String[][] colorKeys = {
                { "background", "panel_background", "text_background", "dialog_background" },
                { "foreground", "text_foreground" },
                { "accent", "secondary", "border", "menubar", "toolbar", "button", "button_hover",
                        "selection", "tab_background", "tab_active" },
                { "error", "warning", "success", "info" }
        };

        String[][] colorLabels = {
                { "Main Background", "Panel Background", "Text Area Background", "Dialog Background" },
                { "Main Text", "Text Area Text" },
                { "Accent Color", "Secondary Color", "Border Color", "Menu Bar", "Toolbar",
                        "Button", "Button Hover", "Selection", "Tab Background", "Active Tab" },
                { "Error", "Warning", "Success", "Info" }
        };

        int row = 0;

        for (int category = 0; category < colorCategories.length; category++) {
            // Category header
            Label categoryLabel = new Label(colorCategories[category]);
            categoryLabel.setStyle("-fx-font-weight: bold; -fx-text-fill: #666666;");
            colorGrid.add(categoryLabel, 0, row, 2, 1);
            row++;

            // Color pickers for this category
            for (int i = 0; i < colorKeys[category].length; i++) {
                String key = colorKeys[category][i];
                String label = colorLabels[category][i];

                Label colorLabel = new Label(label + ":");
                ColorPicker colorPicker = new ColorPicker(themeManager.getColor(key));

                // Live preview on color change
                colorPicker.setOnAction(e -> {
                    themeManager.setColor(key, colorPicker.getValue());
                });

                colorPickers.put(key, colorPicker);

                colorGrid.add(colorLabel, 0, row);
                colorGrid.add(colorPicker, 1, row);
                row++;
            }

            // Add spacing between categories
            if (category < colorCategories.length - 1) {
                colorGrid.add(new Label(""), 0, row);
                row++;
            }
        }

        customSection.getChildren().addAll(customLabel, colorGrid);
        return customSection;
    }

    private VBox createPreviewSection() {
        VBox previewSection = new VBox(10);

        Label previewLabel = new Label("Preview");
        previewLabel.setStyle("-fx-font-weight: bold; -fx-font-size: 14px;");

        // Create a mini preview of UI elements
        VBox previewBox = new VBox(10);
        previewBox.setPadding(new Insets(15));
        previewBox.setStyle("-fx-border-color: #cccccc; -fx-border-width: 1;");

        // Sample menu bar
        MenuBar sampleMenuBar = new MenuBar();
        Menu sampleMenu = new Menu("File");
        sampleMenu.getItems().addAll(new MenuItem("New"), new MenuItem("Open"), new MenuItem("Save"));
        sampleMenuBar.getMenus().add(sampleMenu);

        // Sample buttons
        HBox buttonBox = new HBox(10);
        buttonBox.getChildren().addAll(
                new Button("Primary Button"),
                new Button("Secondary Button"),
                new CheckBox("Checkbox Option"));

        // Sample text field
        TextField sampleTextField = new TextField("Sample text input");

        // Sample table
        TableView<String> sampleTable = new TableView<>();
        TableColumn<String, String> col1 = new TableColumn<>("Column 1");
        TableColumn<String, String> col2 = new TableColumn<>("Column 2");
        sampleTable.getColumns().addAll(col1, col2);
        sampleTable.setPrefHeight(100);

        previewBox.getChildren().addAll(
                new Label("Sample UI Elements:"),
                sampleMenuBar,
                buttonBox,
                sampleTextField,
                sampleTable);

        previewSection.getChildren().addAll(previewLabel, previewBox);
        return previewSection;
    }

    private void applyPresetTheme(Map<String, Color> theme) {
        // Update color pickers
        for (Map.Entry<String, Color> entry : theme.entrySet()) {
            ColorPicker picker = colorPickers.get(entry.getKey());
            if (picker != null) {
                picker.setValue(entry.getValue());
            }
        }

        // Apply theme
        themeManager.applyPresetTheme(theme);
    }

    private void setupButtons() {
        ButtonType applyButtonType = new ButtonType("Apply", ButtonBar.ButtonData.APPLY);
        ButtonType cancelButtonType = new ButtonType("Cancel", ButtonBar.ButtonData.CANCEL_CLOSE);
        ButtonType okButtonType = new ButtonType("OK", ButtonBar.ButtonData.OK_DONE);

        getDialogPane().getButtonTypes().addAll(applyButtonType, cancelButtonType, okButtonType);

        // Handle button actions
        Button applyButton = (Button) getDialogPane().lookupButton(applyButtonType);
        Button cancelButton = (Button) getDialogPane().lookupButton(cancelButtonType);
        Button okButton = (Button) getDialogPane().lookupButton(okButtonType);

        applyButton.setOnAction(e -> {
            // Colors are already applied in real-time, so this just confirms
            Platform.runLater(() -> {
                Alert info = new Alert(Alert.AlertType.INFORMATION);
                info.setTitle("Theme Applied");
                info.setHeaderText(null);
                info.setContentText("Theme changes have been applied and saved.");
                info.initOwner(getOwner());
                info.showAndWait();
            });
        });

        cancelButton.setOnAction(e -> {
            // Restore original colors
            themeManager.applyPresetTheme(originalColors);
            close();
        });

        okButton.setOnAction(e -> {
            // Colors are already applied, just close
            close();
        });

        // Prevent dialog from closing when clicking outside
        setResultConverter(buttonType -> {
            if (buttonType == cancelButtonType) {
                // Restore original colors before closing
                themeManager.applyPresetTheme(originalColors);
            }
            return buttonType;
        });
    }
}
