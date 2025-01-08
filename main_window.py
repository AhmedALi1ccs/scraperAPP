import sys
from PySide6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QPushButton, QLabel, QFileDialog, QSpinBox, 
                             QLineEdit, QScrollArea, QGridLayout, QMessageBox)
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
        self.setMinimumSize(1000, 700)  # Larger default size for Windows
        
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
        
        # Setup UI components with Windows-specific styling
        self.setup_file_upload_section()
        self.setup_conditions_section()
        self.setup_process_section()
        
        # Apply Windows-specific styling
        self.apply_windows_styling()
        
    def apply_windows_styling(self):
        # Set style sheet for Windows appearance
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
      
      # Container for log files list
      self.log_files_container = QWidget()
      self.log_files_layout = QVBoxLayout(self.log_files_container)
      upload_layout.addWidget(self.log_files_container)
      
      self.content_layout.addWidget(upload_group)
          
    def setup_conditions_section(self):
        conditions_group = QWidget()
        conditions_layout = QVBoxLayout(conditions_group)
        
        # Section title
        title = QLabel("Conditions")
        title.setStyleSheet("font-size: 14pt; font-weight: bold; color: #333333;")
        conditions_layout.addWidget(title)
        
        # Condition input section
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
        
        # Container for conditions list
        self.conditions_container = QWidget()
        self.conditions_layout = QVBoxLayout(self.conditions_container)
        conditions_layout.addWidget(self.conditions_container)
        
        self.content_layout.addWidget(conditions_group)
    def setup_process_section(self):
        process_group = QWidget()
        process_layout = QVBoxLayout(process_group)
        
        # Section title
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
        
    def upload_list_file(self):
        file_name, _ = QFileDialog.getOpenFileName(
        self, 
        "Upload List File",
        "",
        "CSV Files (*.csv)"
    )
        if file_name:
            try:
                self.list_file = pd.read_csv(file_name)
                self.list_file_name = os.path.splitext(os.path.basename(file_name))[0]
                self.list_file_label.setText(f"List file uploaded: {self.list_file_name}")
                self.list_file_label.setStyleSheet("color: #28a745;")  # Success color
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error uploading list file: {str(e)}")
                self.list_file_label.setStyleSheet("color: #dc3545;")  # Error color

    def upload_log_files(self):
       file_names, _ = QFileDialog.getOpenFileNames(
        self,
        "Upload Log Files",
        "",
        "CSV Files (*.csv)"
    )
      if file_names:
          try:
              for file_path in file_names:
                  df = pd.read_csv(file_path)
                  file_name = os.path.basename(file_path)
                  self.log_files.append(df)
                  self.log_filenames.append(file_name)
                  
                  # Create widget for this log file
                  log_widget = QWidget()
                  log_layout = QHBoxLayout(log_widget)
                  
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
                  
                  # Create closure to handle removal of this specific log file
                  def remove_log_file(idx=len(self.log_files)-1):
                      self.log_files.pop(idx)
                      self.log_filenames.pop(idx)
                      log_widget.deleteLater()
                      # Update indexes for remaining remove buttons
                      for i in range(idx, self.log_files_layout.count()):
                          widget = self.log_files_layout.itemAt(i).widget()
                          if widget:
                              remove_button = widget.layout().itemAt(1).widget()
                              remove_button.clicked.disconnect()
                              remove_button.clicked.connect(lambda x, j=i: remove_log_file(j))
                  
                  remove_btn.clicked.connect(lambda: remove_log_file())
                  log_layout.addWidget(remove_btn)
                  
                  # Add to log files list
                  self.log_files_layout.addWidget(log_widget)
              
          except Exception as e:
              QMessageBox.critical(self, "Error", f"Error uploading log files: {str(e)}")

    
    def add_condition(self):
        condition_type = self.condition_type_input.text().capitalize()
      threshold = self.threshold_input.value()
      
      if condition_type:
          condition = {
              "type": condition_type,
              "threshold": threshold
          }
          self.conditions.append(condition)
          
          # Create condition display widget
          condition_widget = QWidget()
          condition_layout = QHBoxLayout(condition_widget)
          
          # Add condition text
          condition_text = QLabel(f"• {condition['type']}: min count {condition['threshold']}")
          condition_text.setStyleSheet("color: #28a745;")
          condition_layout.addWidget(condition_text)
          
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
          
          # Create closure to handle removal of this specific condition
          def remove_this_condition():
              self.conditions.remove(condition)
              condition_widget.deleteLater()
              if not self.conditions:
                  self.conditions_container.setVisible(False)
          
          remove_btn.clicked.connect(remove_this_condition)
          condition_layout.addWidget(remove_btn)
          
          # Add to conditions list
          self.conditions_layout.addWidget(condition_widget)
          self.conditions_container.setVisible(True)
          
          # Clear inputs
          self.condition_type_input.clear()
          self.threshold_input.setValue(1)
          
          # Show success message
          QMessageBox.information(self, "Success", "Condition added successfully!")
    def update_conditions_label(self):
        if self.conditions:
            conditions_text = "Current Conditions:\n"
            for cond in self.conditions:
                conditions_text += f"• {cond['type']}: min count {cond['threshold']}\n"
            self.conditions_label.setStyleSheet("color: #28a745;")  # Success color
        else:
            conditions_text = "Current Conditions: None"
            self.conditions_label.setStyleSheet("color: #666666;")  # Normal color
        self.conditions_label.setText(conditions_text)
    
    def process_files(self):
        if not all([self.list_file is not None, self.log_files, self.conditions]):
            QMessageBox.warning(
                self,
                "Warning",
                "Please upload all required files and add at least one condition."
            )
            return
            
        try:
            # Show processing message
            self.status_label.setText("Processing files... Please wait.")
            self.status_label.setStyleSheet("color: #007bff;")  # Processing color
            QApplication.processEvents()  # Update UI
            
            current_date = datetime.now().strftime("%Y%m%d")
            
            # Process files
            updated_list_df, updated_log_dfs, removed_log_records = process_files(
                self.log_files, self.list_file, self.conditions, self.log_filenames
            )
            
            # Upload to Google Drive
            self.status_label.setText("Uploading to Google Drive...")
            QApplication.processEvents()
            self.upload_to_drive(updated_list_df, updated_log_dfs, removed_log_records, current_date)
            
            # Create local zip file
            self.status_label.setText("Creating ZIP file...")
            QApplication.processEvents()
            
            dfs_to_zip = {
                f'Updated_List_File_{current_date}': updated_list_df
            }
            
            for i, log_file_name in enumerate(self.log_filenames):
                log_base_name = log_file_name.replace('.csv', '')
                dfs_to_zip[f'Scrubbed_{log_base_name}_{current_date}'] = updated_log_dfs[i]
                if not removed_log_records[i].empty:
                    dfs_to_zip[f'Removed_Records_{log_base_name}_{current_date}'] = removed_log_records[i]
            
            zip_content = create_zip_file(dfs_to_zip)
            
            # Save zip file
            save_path, _ = QFileDialog.getSaveFileName(
                self,
                "Save Processed Files",
                f"all_processed_files_{current_date}.zip",
                "ZIP Files (*.zip)"
            )
            
            if save_path:
                with open(save_path, 'wb') as f:
                    f.write(zip_content)
                self.status_label.setText("Files processed and saved successfully!")
                self.status_label.setStyleSheet("color: #28a745;")  # Success color
                QMessageBox.information(self, "Success", "Files processed and saved successfully!")
            
        except Exception as e:
            self.status_label.setText(f"Error: {str(e)}")
            self.status_label.setStyleSheet("color: #dc3545;")  # Error color
            QMessageBox.critical(self, "Error", f"Error processing files: {str(e)}")
    
    def upload_to_drive(self, updated_list_df, updated_log_dfs, removed_log_records, current_date):
        try:
            # Upload updated list file
            self.drive_manager.upload_dataframe(
                updated_list_df,
                f"Updated_{self.list_file_name}_{current_date}.csv",
                self.drive_manager.REMOVED_FOLDER_ID
            )
            
            # Upload log files and removed records
            for i, log_file_name in enumerate(self.log_filenames):
                log_base_name = log_file_name.replace('.csv', '')
                
                # Upload scrubbed log file
                self.drive_manager.upload_dataframe(
                    updated_log_dfs[i],
                    f"Scrubbed_{log_base_name}_{current_date}.csv",
                    self.drive_manager.SCRUBBED_FOLDER_ID
                )
                
                # Upload removed records if they exist
                if not removed_log_records[i].empty:
                    self.drive_manager.upload_dataframe(
                        removed_log_records[i],
                        f"Removed_Records_{log_base_name}_{current_date}.csv",
                        self.drive_manager.REMOVED_FOLDER_ID
                    )
            
        except Exception as e:
            raise Exception(f"Error uploading to Google Drive: {str(e)}")

def main():
    app = QApplication(sys.argv)
    
    # Set Windows style
    app.setStyle("Fusion")
    
    window = MainWindow()
    window.show()
    sys.exit(app.exec())

if __name__ == '__main__':
    main()
