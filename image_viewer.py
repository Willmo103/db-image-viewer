"""
DB Image Viewer
Author: Will Morris <willmorris103@gmail.com>
License: MIT License

A simple utility application to connect to a SQLite or PostgreSQL database,
run a custom query, and display images from the results in a slideshow format.
This script is self-contained and uses PyQt6 for the graphical user interface.
"""

import sys
import sqlite3
import base64
import psycopg

from PyQt6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QLabel,
    QLineEdit,
    QTextEdit,
    QComboBox,
    QFormLayout,
    QMessageBox,
    QGroupBox,
)
from PyQt6.QtGui import QPixmap, QImage
from PyQt6.QtCore import Qt, QSize


class ImageViewer(QMainWindow):
    """
    Main application window for the Database Image Viewer.

    This class sets up the user interface, handles database connections,
    executes queries, and displays the results.
    """

    def __init__(self):
        """Initializes the main window and sets up the UI."""
        super().__init__()
        self.setWindowTitle("Database Image Viewer")
        self.setGeometry(100, 100, 900, 700)

        # --- Member Variables ---
        self.db_connection = None
        self.db_cursor = None
        self.results = []
        self.column_names = []
        self.current_index = -1
        self.db_group = None

        # --- UI Setup ---
        self.setup_ui()

    def setup_ui(self):
        """
        Constructs the user interface widgets and layouts.
        An "Idiot's Guide" hint: This function builds everything you see in the window.
        """
        # --- Main Layout ---
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)

        # --- Left Panel (Controls) ---
        controls_layout = QVBoxLayout()
        controls_layout.setSpacing(15)

        # DB Connection Group
        db_group = QGroupBox("1. Database Connection")
        db_layout = QFormLayout()
        self.db_type_combo = QComboBox()
        self.db_type_combo.addItems(["SQLite", "PostgreSQL"])
        self.db_type_combo.currentIndexChanged.connect(
            self.update_connection_fields
        )

        # Connection Fields
        self.sqlite_path_input = QLineEdit()
        self.pg_host_input = QLineEdit("localhost")
        self.pg_port_input = QLineEdit("5432")
        self.pg_dbname_input = QLineEdit("mydatabase")
        self.pg_user_input = QLineEdit("postgres")
        self.pg_pass_input = QLineEdit()
        self.pg_pass_input.setEchoMode(QLineEdit.EchoMode.Password)

        db_layout.addRow("Database Type:", self.db_type_combo)
        self.sqlite_row = db_layout.addRow(
            "DB File Path:", self.sqlite_path_input
        )
        self.pg_host_row = db_layout.addRow("Host:", self.pg_host_input)
        self.pg_port_row = db_layout.addRow("Port:", self.pg_port_input)
        self.pg_dbname_row = db_layout.addRow("DB Name:", self.pg_dbname_input)
        self.pg_user_row = db_layout.addRow("User:", self.pg_user_input)
        self.pg_pass_row = db_layout.addRow("Password:", self.pg_pass_input)
        db_group.setLayout(db_layout)
        self.db_group = db_group

        # Query Group
        query_group = QGroupBox("2. Query")
        query_layout = QVBoxLayout()
        query_layout.setSpacing(5)
        query_layout.addWidget(QLabel("SQL Query:"))
        self.query_input = QTextEdit(
            "SELECT id, name, image_data FROM images;"
        )
        self.query_input.setFixedHeight(100)
        query_layout.addWidget(self.query_input)

        image_col_layout = QHBoxLayout()
        image_col_layout.addWidget(QLabel("Image Column Name:"))
        self.image_column_input = QLineEdit("image_data")
        image_col_layout.addWidget(self.image_column_input)
        query_layout.addLayout(image_col_layout)
        query_group.setLayout(query_layout)

        # Run Button
        self.run_button = QPushButton("Connect & Run Query")
        self.run_button.setStyleSheet(
            "background-color: #4CAF50; color: white; padding: 10px;"
        )
        self.run_button.clicked.connect(self.run_query)

        controls_layout.addWidget(db_group)
        controls_layout.addWidget(query_group)
        controls_layout.addWidget(self.run_button)
        controls_layout.addStretch()  # Pushes everything up

        # --- Right Panel (Viewer) ---
        viewer_layout = QVBoxLayout()

        # Status/Counter Label
        self.status_label = QLabel("No query run yet.")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        # Image Display
        self.image_label = QLabel("Image will be displayed here")
        self.image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.image_label.setMinimumSize(QSize(400, 400))
        self.image_label.setStyleSheet(
            "border: 1px solid #ccc; background-color: #f0f0f0;"
        )

        # Other Data Display
        self.other_data_label = QLabel(
            "Other data from the row will appear here."
        )
        self.other_data_label.setAlignment(
            Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft
        )
        self.other_data_label.setWordWrap(True)

        # Navigation Buttons
        nav_layout = QHBoxLayout()
        self.prev_button = QPushButton("<< Previous")
        self.next_button = QPushButton("Next >>")
        self.prev_button.clicked.connect(self.prev_image)
        self.next_button.clicked.connect(self.next_image)
        nav_layout.addWidget(self.prev_button)
        nav_layout.addWidget(self.next_button)

        viewer_layout.addWidget(self.status_label)
        viewer_layout.addWidget(
            self.image_label, 1
        )  # Give image label more stretch
        viewer_layout.addWidget(QLabel("Row Data:"))
        viewer_layout.addWidget(self.other_data_label, 0)
        viewer_layout.addLayout(nav_layout)

        # Add panels to main layout
        main_layout.addLayout(controls_layout, 1)
        main_layout.addLayout(viewer_layout, 2)

        # Initial state
        self.update_connection_fields()
        self.update_nav_buttons()

    def update_connection_fields(self):
        """
        Shows or hides connection fields based on the selected database type.
        Hint: This makes the UI cleaner by only showing relevant options.
        """
        is_sqlite = self.db_type_combo.currentText() == "SQLite"

        # Get the form layout from the group box to find the labels
        form_layout = self.db_group.layout()

        # Handle SQLite field
        sqlite_label = form_layout.labelForField(self.sqlite_path_input)
        if sqlite_label:
            sqlite_label.setVisible(is_sqlite)
        self.sqlite_path_input.setVisible(is_sqlite)

        # Handle PostgreSQL fields
        pg_widgets = [
            self.pg_host_input,
            self.pg_port_input,
            self.pg_dbname_input,
            self.pg_user_input,
            self.pg_pass_input,
        ]
        for widget in pg_widgets:
            label = form_layout.labelForField(widget)
            if label:
                label.setVisible(not is_sqlite)
            widget.setVisible(not is_sqlite)

    def run_query(self):
        """
        Handles the 'Run Query' button click. Connects to the database,
        executes the query, fetches results, and displays the first record.
        """
        # --- Close existing connection if any ---
        if self.db_connection:
            self.db_connection.close()
            self.db_connection = None
            self.db_cursor = None

        # --- Get inputs ---
        db_type = self.db_type_combo.currentText()
        query_text = self.query_input.toPlainText()
        self.image_column_name = self.image_column_input.text().strip()

        if not self.image_column_name:
            QMessageBox.warning(
                self, "Input Error", "Please specify the image column name."
            )
            return

        # --- Connect to database ---
        try:
            if db_type == "SQLite":
                db_path = self.sqlite_path_input.text()
                if not db_path:
                    raise ValueError("SQLite database path cannot be empty.")
                self.db_connection = sqlite3.connect(db_path)
            else:  # PostgreSQL
                conninfo = (
                    f"host={self.pg_host_input.text()} "
                    f"port={self.pg_port_input.text()} "
                    f"dbname={self.pg_dbname_input.text()} "
                    f"user={self.pg_user_input.text()} "
                    f"password={self.pg_pass_input.text()}"
                )
                self.db_connection = psycopg.connect(conninfo)

            self.db_cursor = self.db_connection.cursor()

        except Exception as e:
            QMessageBox.critical(
                self,
                "Connection Error",
                f"Could not connect to database:\n{e}",
            )
            self.db_connection = None
            return

        # --- Execute query and fetch results ---
        try:
            self.db_cursor.execute(query_text)
            self.results = self.db_cursor.fetchall()
            self.column_names = [
                desc[0] for desc in self.db_cursor.description
            ]

            if self.image_column_name not in self.column_names:
                raise ValueError(
                    f"Image column '{self.image_column_name}' not found in query results."
                )

            if not self.results:
                self.status_label.setText(
                    "Query executed, but returned no results."
                )
                self.image_label.clear()
                self.other_data_label.clear()
                self.current_index = -1
            else:
                self.current_index = 0
                self.display_current_record()

        except Exception as e:
            QMessageBox.critical(
                self,
                "Query Error",
                f"An error occurred while executing the query:\n{e}",
            )
            self.results = []
            self.current_index = -1

        self.update_nav_buttons()

    def display_current_record(self):
        """
        Displays the image and data for the current record index.
        Hint: This is where the magic happens. It takes the raw data from the
        database and turns it into a picture and text on your screen.
        """
        if not (0 <= self.current_index < len(self.results)):
            return

        record = self.results[self.current_index]
        # Create a dictionary of column_name: value for easy lookup
        record_dict = dict(zip(self.column_names, record))

        # --- Process and display the image ---
        image_data = record_dict.get(self.image_column_name)
        self.image_label.clear()  # Clear previous image

        if image_data:
            try:
                image_bytes = None
                if isinstance(image_data, str):
                    # Handle base64 encoded strings
                    image_bytes = base64.b64decode(image_data)
                elif isinstance(image_data, (bytes, bytearray)):
                    # Handle raw binary data (BLOB/bytea)
                    image_bytes = bytes(image_data)

                if image_bytes:
                    q_image = QImage.fromData(image_bytes)
                    if q_image.isNull():
                        raise ValueError(
                            "Data could not be parsed as an image."
                        )
                    pixmap = QPixmap.fromImage(q_image)
                    # Scale pixmap to fit the label while keeping aspect ratio
                    self.image_label.setPixmap(
                        pixmap.scaled(
                            self.image_label.size(),
                            Qt.AspectRatioMode.KeepAspectRatio,
                            Qt.TransformationMode.SmoothTransformation,
                        )
                    )
                else:
                    self.image_label.setText("Unsupported image data type.")
            except Exception as e:
                self.image_label.setText(f"Error loading image:\n{e}")
        else:
            self.image_label.setText("No image in this record.")

        # --- Display other data ---
        other_data_str = ""
        for col, val in record_dict.items():
            if col != self.image_column_name:
                display_val = str(val)
                # Truncate very long values for display
                if len(display_val) > 150:
                    display_val = display_val[:150] + "..."
                other_data_str += f"<b>{col}:</b> {display_val}<br>"
        self.other_data_label.setText(other_data_str)

        # --- Update status ---
        self.status_label.setText(
            f"Displaying record {self.current_index + 1} of {len(self.results)}"
        )

    def next_image(self):
        """Moves to the next record in the results."""
        if self.current_index < len(self.results) - 1:
            self.current_index += 1
            self.display_current_record()
            self.update_nav_buttons()

    def prev_image(self):
        """Moves to the previous record in the results."""
        if self.current_index > 0:
            self.current_index -= 1
            self.display_current_record()
            self.update_nav_buttons()

    def update_nav_buttons(self):
        """Enables or disables navigation buttons based on the current index."""
        if not self.results:
            self.prev_button.setEnabled(False)
            self.next_button.setEnabled(False)
        else:
            self.prev_button.setEnabled(self.current_index > 0)
            self.next_button.setEnabled(
                self.current_index < len(self.results) - 1
            )

    def closeEvent(self, event):
        """Ensures the database connection is closed when the app exits."""
        if self.db_connection:
            self.db_connection.close()
            print("Database connection closed.")
        super().closeEvent(event)

    def resizeEvent(self, event):
        """
        Re-scales the image when the window is resized to keep it looking good.
        """
        if self.current_index != -1:
            self.display_current_record()
        super().resizeEvent(event)


if __name__ == "__main__":
    # --- Application Entry Point ---
    app = QApplication(sys.argv)
    viewer = ImageViewer()
    viewer.show()
    sys.exit(app.exec())
