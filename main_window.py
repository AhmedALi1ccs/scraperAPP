import sys
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QPushButton, QLabel, QFileDialog, QSpinBox, 
                             QLineEdit, QScrollArea, QGridLayout, QMessageBox, QHBoxLayout)
from PySide6.QtCore import Qt
import pandas as pd
from utils import clean_nan_values, clean_number_to_text, create_zip_file
from processor import process_files
from google_drive import GoogleDriveManager
from datetime import datetime
import os

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Log and List File Processor")
        self.setMinimumSize(1000, 700)
        
        # Initialize state
        self.list_file = None
        self.list_file_name = None
        self.log_files = []
        self.log_filenames = []
        self.conditions = []
        
        # Initialize Google Drive
        self.drive_manager = GoogleDriveManager()
        
        # Create central widget and layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)
        
        # Create scroll area
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        layout.addWidget(scroll)
        
        # Create content widget for scroll area
        content_widget = QWidget()
        self.content_layout = QVBoxLayout(content_widget)
        scroll.setWidget(content_widget)
        
        # Setup UI components
        self.setup_file_upload_section()
        self.setup_conditions_section()
        self.setup_process_section()
        
        # Apply styling
        self.apply_windows_styling()
    
    def apply_windows_styling(self):
        self.setStyleSheet("""
            QPushButton {
                background-color: #0078D7;
                color: white;
                border: none;
                padding: 8px;
                min-height: 30px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #1884DA;
            }
            QPushButton:pressed {
                background-color: #006CC1;
            }
            QLabel {
                font-size: 11pt;
            }
            QLineEdit, QSpinBox {
                padding: 6px;
                border: 1px solid #CCCCCC;
                border-radius: 4px;
                min-height: 25px;
            }
            QSpinBox::up-button, QSpinBox::down-button {
                width: 20px;
            }
        """)

    def setup_file_upload_section(self):
        upload_group = QWidget()
        upload_layout = QVBoxLayout(upload_group)
        
        # Section title
        title = QLabel("File Upload")
        title.setStyleSheet("font-size: 14pt; font-weight: bold; color: #333333;")
        upload_layout.addWidget(title)
        
        # List file upload
        list_upload_btn = QPushButton("Upload List File (CSV)")
        list_upload_btn.clicked.connect(self.upload_list_file)
        upload_layout.addWidget(list_upload_btn)
        
        self.list_file_label = QLabel("No list file uploaded")
        self.list_file_label.setStyleSheet("color: #666666;")
        upload_layout.addWidget(self.list_file_label)
        
        # Log files upload
        log_upload_btn = QPushButton("Upload Log Files (CSV)")
        log_upload_btn.clicked.connect(self.upload_log_files)
        upload_layout.addWidget(log_upload_btn)
        
        # Container for log files list with scroll area
        log_scroll = QScrollArea()
        log_scroll.setWidgetResizable(True)
        log_scroll.setMaximumHeight(200)
        
        self.log_files_container = QWidget()
        self.log_files_layout = QVBoxLayout(self.log_files_container)
        self.log_files_layout.addStretch()
        
        log_scroll.setWidget(self.log_files_container)
        upload_layout.addWidget(log_scroll)
        
        self.content_layout.addWidget(upload_group)

    def upload_list_file(self):
        file_name, _ = QFileDialog.getOpenFileName(
            self, 
            "Upload List File",
            "",
            "CSV Files (*.csv)"
        )
        if file_name:
            try:
                # Read CSV with optimized settings
                self.list_file = pd.read_csv(
                    file_name,
                    low_memory=False,
                    encoding='utf-8',
                    on_bad_lines='skip'
                )
                self.list_file_name = os.path.splitext(os.path.basename(file_name))[0]
                self.list_file_label.setText(f"List file uploaded: {self.list_file_name}")
                self.list_file_label.setStyleSheet("color: #28a745;")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error uploading list file: {str(e)}")
                self.list_file_label.setText("No list file uploaded")
                self.list_file_label.setStyleSheet("color: #dc3545;")
    def _create_log_file_widget(self, file_name: str, index: int) -> QWidget:
        """Create a widget for displaying a log file with remove button."""
        log_widget = QWidget()
        log_layout = QHBoxLayout(log_widget)
        log_layout.setContentsMargins(0, 0, 0, 0)
        
        # Add file name label
        file_label = QLabel(file_name)
        file_label.setStyleSheet("color: #28a745;")  # Success color
        log_layout.addWidget(file_label)
        
        # Add remove button
        remove_btn = QPushButton("Remove")
        remove_btn.setFixedWidth(80)
        remove_btn.setStyleSheet("""
            QPushButton {
                background-color: #dc3545;
                color: white;
                border: none;
                padding: 4px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #c82333;
            }
        """)
        
        # Handle file removal
        remove_btn.clicked.connect(lambda: self._remove_log_file(index, log_widget))
        log_layout.addWidget(remove_btn)
        
        return log_widget

    def _remove_log_file(self, index: int, widget: QWidget):
        """Remove a log file at the specified index."""
        if 0 <= index < len(self.log_files):
            self.log_files.pop(index)
            self.log_filenames.pop(index)
            widget.deleteLater()
            self._update_log_file_indices()

    def _update_log_file_indices(self):
        """Update the indices of all log file widgets."""
        for i in range(self.log_files_layout.count() - 1):
            widget = self.log_files_layout.itemAt(i).widget()
            if widget:
                button = widget.layout().itemAt(1).widget()
                if isinstance(button, QPushButton):
                    button.clicked.disconnect()
                    button.clicked.connect(lambda checked, idx=i: self._remove_log_file(idx, widget))

    def upload_log_files(self):
        file_names, _ = QFileDialog.getOpenFileNames(
            self,
            "Upload Log Files",
            "",
            "CSV Files (*.csv)"
        )
        
        if file_names:
            for file_path in file_names:
                try:
                    # Read CSV with optimized settings
                    df = pd.read_csv(
                        file_path,
                        low_memory=False,
                        encoding='utf-8',
                        on_bad_lines='skip'
                    )
                    
                    file_name = os.path.basename(file_path)
                    self.log_files.append(df)
                    self.log_filenames.append(file_name)
                    
                    # Create and add the log file widget
                    log_widget = self._create_log_file_widget(
                        file_name, 
                        len(self.log_files) - 1
                    )
                    self.log_files_layout.insertWidget(
                        self.log_files_layout.count() - 1, 
                        log_widget
                    )
                    
                except Exception as e:
                    QMessageBox.critical(
                        self, 
                        "Error", 
                        f"Error uploading {os.path.basename(file_path)}: {str(e)}"
                    )

    def setup_conditions_section(self):
        conditions_group = QWidget()
        conditions_layout = QVBoxLayout(conditions_group)
        
        # Section title
        title = QLabel("Conditions")
        title.setStyleSheet("font-size: 14pt; font-weight: bold; color: #333333;")
        conditions_layout.addWidget(title)
        
        # Input section
        input_widget = QWidget()
        input_layout = QGridLayout(input_widget)
        
        self.condition_type_input = QLineEdit()
        self.condition_type_input.setPlaceholderText("Enter condition type (e.g., voicemail, call)")
        
        self.threshold_input = QSpinBox()
        self.threshold_input.setMinimum(1)
        self.threshold_input.setMaximum(9999)
        
        add_condition_btn = QPushButton("Add Condition")
        add_condition_btn.clicked.connect(self.add_condition)
        
        input_layout.addWidget(QLabel("Condition Type:"), 0, 0)
        input_layout.addWidget(self.condition_type_input, 0, 1)
        input_layout.addWidget(QLabel("Threshold:"), 1, 0)
        input_layout.addWidget(self.threshold_input, 1, 1)
        input_layout.addWidget(add_condition_btn, 2, 0, 1, 2)
        
        conditions_layout.addWidget(input_widget)
        
        # Conditions list with scroll area
        conditions_scroll = QScrollArea()
        conditions_scroll.setWidgetResizable(True)
        conditions_scroll.setMaximumHeight(200)
        
        self.conditions_container = QWidget()
        self.conditions_layout = QVBoxLayout(self.conditions_container)
        self.conditions_layout.addStretch()
        
        conditions_scroll.setWidget(self.conditions_container)
        conditions_layout.addWidget(conditions_scroll)
        
        self.content_layout.addWidget(conditions_group)

    def add_condition(self):
        condition_type = self.condition_type_input.text().strip().capitalize()
        threshold = self.threshold_input.value()
        
        if not condition_type:
            QMessageBox.warning(self, "Warning", "Please enter a condition type.")
            return
        
        # Check for duplicate condition type
        if any(cond['type'] == condition_type for cond in self.conditions):
            QMessageBox.warning(self, "Warning", "This condition type already exists.")
            return
        
        condition = {
            "type": condition_type,
            "threshold": threshold
        }
        self.conditions.append(condition)
        
        # Create condition widget
        condition_widget = self._create_condition_widget(condition)
        self.conditions_layout.insertWidget(self.conditions_layout.count() - 1, condition_widget)
        
        # Clear inputs
        self.condition_type_input.clear()
        self.threshold_input.setValue(1)

    def _create_condition_widget(self, condition: dict) -> QWidget:
        """Create a widget for displaying a condition with remove button."""
        condition_widget = QWidget()
        condition_layout = QHBoxLayout(condition_widget)
        condition_layout.setContentsMargins(0, 0, 0, 0)
        
        condition_text = QLabel(f"• {condition['type']}: min count {condition['threshold']}")
        condition_text.setStyleSheet("color: #28a745;")
        condition_layout.addWidget(condition_text)
        
        remove_btn = QPushButton("Remove")
        remove_btn.setFixedWidth(80)
        remove_btn.setStyleSheet("""
            QPushButton {
                background-color: #dc3545;
                color: white;
                border: none;
                padding: 4px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #c82333;
            }
        """)
        
        remove_btn.clicked.connect(lambda: self._remove_condition(condition, condition_widget))
        condition_layout.addWidget(remove_btn)
        
        return condition_widget

    def _remove_condition(self, condition: dict, widget: QWidget):
        """Remove a condition and its widget."""
        self.conditions.remove(condition)
        widget.deleteLater()
    def setup_process_section(self):
        process_group = QWidget()
        process_layout = QVBoxLayout(process_group)
        
        title = QLabel("Process Files")
        title.setStyleSheet("font-size: 14pt; font-weight: bold; color: #333333;")
        process_layout.addWidget(title)
        
        process_btn = QPushButton("Process Files")
        process_btn.clicked.connect(self.process_files)
        process_layout.addWidget(process_btn)
        
        self.status_label = QLabel("")
        self.status_label.setStyleSheet("color: #666666;")
        process_layout.addWidget(self.status_label)
        
        self.content_layout.addWidget(process_group)

    def process_files(self):
        if not all([self.list_file is not None, self.log_files, self.conditions]):
            QMessageBox.warning(
                self,
                "Warning",
                "Please upload all required files and add at least one condition."
            )
            return
        
        try:
            # Update status
            self.status_label.setText("Processing files... Please wait.")
            self.status_label.setStyleSheet("color: #007bff;")
            QApplication.processEvents()
            
            current_date = datetime.now().strftime("%Y%m%d")
            
            # Process files
            updated_list_df, updated_log_dfs, removed_log_records = process_files(
                self.log_files, self.list_file, self.conditions, self.log_filenames
            )
            
            # Save removed records
            removed_save_path = self._save_removed_records(
                updated_list_df, removed_log_records, current_date
            )
            
            # Save scrubbed files
            scrubbed_save_path = self._save_scrubbed_files(
                updated_log_dfs, current_date
            )
            
            # Upload to Google Drive
            try:
                self.status_label.setText("Uploading to Google Drive...")
                QApplication.processEvents()
                self.upload_to_drive(
                    updated_list_df, 
                    updated_log_dfs, 
                    removed_log_records, 
                    current_date
                )
            except Exception as e:
                QMessageBox.warning(
                    self, 
                    "Warning", 
                    f"Files processed successfully but failed to upload to Google Drive: {str(e)}"
                )
            
            # Show success message
            self.status_label.setText("Files processed and saved successfully!")
            self.status_label.setStyleSheet("color: #28a745;")
            QMessageBox.information(
                self, 
                "Success",
                "Files processed and saved successfully!\n\n"
                "• Removed records: " + 
                (os.path.basename(removed_save_path) if removed_save_path else "Not saved") +
                "\n• Scrubbed files: " + 
                (os.path.basename(scrubbed_save_path) if scrubbed_save_path else "Not saved")
            )
            
        except Exception as e:
            self.status_label.setText(f"Error: {str(e)}")
            self.status_label.setStyleSheet("color: #dc3545;")
            QMessageBox.critical(self, "Error", f"Error processing files: {str(e)}")

    def _save_removed_records(self, updated_list_df, removed_log_records, current_date):
        """Save removed records to a ZIP file."""
        removed_dfs = {
            f'Updated_List_File_{current_date}': updated_list_df
        }
        
        for i, log_file_name in enumerate(self.log_filenames):
            if not removed_log_records[i].empty:
                removed_dfs[f'Removed_Records_{os.path.splitext(log_file_name)[0]}_{current_date}'] = removed_log_records[i]
        
        if removed_dfs:
            save_path, _ = QFileDialog.getSaveFileName(
                self,
                "Save Removed Records",
                f"removed_records_{current_date}.zip",
                "ZIP Files (*.zip)"
            )
            
            if save_path:
                with open(save_path, 'wb') as f:
                    f.write(create_zip_file(removed_dfs))
                return save_path
        
        return None

    def _save_scrubbed_files(self, updated_log_dfs, current_date):
        """Save scrubbed files to a ZIP file."""
        scrubbed_dfs = {}
        
        for i, log_file_name in enumerate(self.log_filenames):
            scrubbed_dfs[f'Scrubbed_{os.path.splitext(log_file_name)[0]}_{current_date}'] = updated_log_dfs[i]
        
        if scrubbed_dfs:
            save_path, _ = QFileDialog.getSaveFileName(
                self,
                "Save Scrubbed Files",
                f"scrubbed_files_{current_date}.zip",
                "ZIP Files (*.zip)"
            )
            
            if save_path:
                with open(save_path, 'wb') as f:
                    f.write(create_zip_file(scrubbed_dfs))
                return save_path
        
        return None

    def upload_to_drive(self, updated_list_df, updated_log_dfs, removed_log_records, current_date):
        """Upload files to Google Drive."""
        # Upload updated list file
        self.drive_manager.upload_dataframe(
            updated_list_df,
            f"Updated_{self.list_file_name}_{current_date}.csv",
            self.drive_manager.REMOVED_FOLDER_ID
        )
        
        # Upload log files and removed records
        for i, log_file_name in enumerate(self.log_filenames):
            base_name = os.path.splitext(log_file_name)[0]
            
            # Upload scrubbed log file
            self.drive_manager.upload_dataframe(
                updated_log_dfs[i],
                f"Scrubbed_{base_name}_{current_date}.csv",
                self.drive_manager.SCRUBBED_FOLDER_ID
            )
            
            # Upload removed records if they exist
            if not removed_log_records[i].empty:
                self.drive_manager.upload_dataframe(
                    removed_log_records[i],
                    f"Removed_Records_{base_name}_{current_date}.csv",
                    self.drive_manager.REMOVED_FOLDER_ID
                )


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    
    window = MainWindow()
    window.show()
    
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
