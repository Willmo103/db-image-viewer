"""
Enhanced DB Image Viewer
Author: Will Morris <willmorris103@gmail.com>
License: MIT License

A comprehensive utility application to connect to a SQLite or PostgreSQL
database, run custom queries, display images in slideshow or grid format,
with caching, saved queries, and download functionality.
"""

import sys
import sqlite3
import base64
import psycopg2 as psycopg
import json
from pathlib import Path
import hashlib
from typing import Dict, List, Optional

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
    QProgressBar,
    QStatusBar,
    QScrollArea,
    QGridLayout,
    QFrame,
    QCheckBox,
    QSpinBox,
    QFileDialog,
    QListWidget,
    QListWidgetItem,
    QSplitter,
    QTabWidget,
    QTextBrowser,
)
from PyQt6.QtGui import QPixmap, QImage, QAction
from PyQt6.QtCore import Qt, QSize, QThread, pyqtSignal, QTimer
from dotenv import load_dotenv

load_dotenv()


class CacheManager:
    """Manages caching of query results and images to disk"""

    def __init__(self, cache_dir: str = "db_viewer_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.cache_index_file = self.cache_dir / "cache_index.json"
        self.load_cache_index()

    def load_cache_index(self):
        """Load the cache index from disk"""
        if self.cache_index_file.exists():
            try:
                with open(self.cache_index_file, "r") as f:
                    self.cache_index = json.load(f)
            except Exception as e:  # noqa: F841
                self.cache_index = {}
        else:
            self.cache_index = {}

    def save_cache_index(self):
        """Save the cache index to disk"""
        with open(self.cache_index_file, "w") as f:
            json.dump(self.cache_index, f, indent=2)

    def get_cache_key(self, connection_info: dict, query: str) -> str:
        """Generate a unique cache key for a query"""
        cache_data = f"{connection_info}{query}"
        return hashlib.md5(cache_data.encode()).hexdigest()

    def cache_results(self, cache_key: str, results: List, column_names: List):
        """Cache query results to disk"""
        cache_file = self.cache_dir / f"{cache_key}.json"
        cache_data = {
            "results": results,
            "column_names": column_names,
            "timestamp": QTimer().singleShot(
                0, lambda: None
            ),  # Current time placeholder
        }

        with open(cache_file, "w") as f:
            json.dump(cache_data, f, default=str, indent=2)

        self.cache_index[cache_key] = {
            "file": str(cache_file),
            "size": len(results),
        }
        self.save_cache_index()

    def get_cached_results(self, cache_key: str) -> Optional[Dict]:
        """Retrieve cached results if they exist"""
        if cache_key in self.cache_index:
            cache_file = Path(self.cache_index[cache_key]["file"])
            if cache_file.exists():
                try:
                    with open(cache_file, "r") as f:
                        return json.load(f)
                except Exception as e:  # noqa: F841
                    pass
        return None

    def clear_cache(self):
        """Clear all cached files"""
        for cache_key in list(self.cache_index.keys()):
            cache_file = Path(self.cache_index[cache_key]["file"])
            if cache_file.exists():
                cache_file.unlink()
        self.cache_index = {}
        self.save_cache_index()

    def get_cache_size(self) -> int:
        """Get total number of cached entries"""
        return sum(info["size"] for info in self.cache_index.values())


class ConfigManager:
    """Manages saving and loading of application configuration"""

    def __init__(self, config_file: str = "db_viewer_config.json"):
        self.config_file = Path(config_file)
        self.config = self.load_config()

    def load_config(self) -> dict:
        """Load configuration from file"""
        if self.config_file.exists():
            try:
                with open(self.config_file, "r") as f:
                    return json.load(f)
            except Exception as e:  # noqa: F841
                pass
        return {
            "connections": [],
            "queries": [],
            "last_connection": {},
            "last_query": "",
            "last_image_column": "image_data",
            "window_geometry": None,
            "grid_columns": 3,
        }

    def save_config(self):
        """Save configuration to file"""
        with open(self.config_file, "w") as f:
            json.dump(self.config, f, indent=2)

    def add_connection(self, connection_info: dict):
        """Add a connection to saved connections"""
        # Remove duplicates
        self.config["connections"] = [
            conn
            for conn in self.config["connections"]
            if conn.get("name") != connection_info.get("name")
        ]
        self.config["connections"].append(connection_info)
        self.save_config()

    def add_query(self, query: str, name: str = None):
        """Add a query to saved queries"""
        if not name:
            name = query[:50] + "..." if len(query) > 50 else query

        query_info = {"name": name, "query": query}

        # Remove duplicates
        self.config["queries"] = [
            q for q in self.config["queries"] if q.get("query") != query
        ]
        self.config["queries"].append(query_info)
        self.save_config()


class QueryWorker(QThread):
    """Worker thread for database queries to prevent UI blocking"""

    finished = pyqtSignal(list, list, str)  # results, column_names, error
    progress = pyqtSignal(str)

    def __init__(self, connection, query):
        super().__init__()
        self.connection = connection
        self.query = query

    def run(self):
        try:
            self.progress.emit("Executing query...")
            cursor = self.connection.cursor()
            cursor.execute(self.query)

            self.progress.emit("Fetching results...")
            results = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]

            self.progress.emit("Query completed successfully")
            self.finished.emit(results, column_names, "")
        except Exception as e:
            self.finished.emit([], [], str(e))


class ClickableImageLabel(QLabel):
    """Custom QLabel that emits signals when clicked"""

    clicked = pyqtSignal()

    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self.clicked.emit()
        super().mousePressEvent(event)


class ImageViewer(QMainWindow):
    """Enhanced main application window"""

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Enhanced Database Image Viewer")

        # Initialize managers
        self.config_manager = ConfigManager()
        self.cache_manager = CacheManager()

        # Member variables
        self.db_connection = None
        self.results = []
        self.column_names = []
        self.current_index = -1
        self.image_column_name = ""
        self.grid_items = []

        self.setup_ui()
        self.load_saved_settings()

        # Status bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.status_bar.addPermanentWidget(self.progress_bar)

    def setup_ui(self):
        """Setup the user interface"""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        # Create menu bar
        self.create_menu_bar()

        # Main splitter
        main_splitter = QSplitter(Qt.Orientation.Horizontal)

        # Left panel - controls
        left_panel = self.create_left_panel()
        main_splitter.addWidget(left_panel)

        # Right panel - viewer
        right_panel = self.create_right_panel()
        main_splitter.addWidget(right_panel)

        main_splitter.setSizes([400, 800])

        main_layout = QHBoxLayout(central_widget)
        main_layout.addWidget(main_splitter)

    def create_menu_bar(self):
        """Create application menu bar"""
        menubar = self.menuBar()

        # File menu
        file_menu = menubar.addMenu("File")

        export_action = QAction("Export Current Image", self)
        export_action.triggered.connect(self.export_current_image)
        file_menu.addAction(export_action)

        file_menu.addSeparator()

        exit_action = QAction("Exit", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

        # Cache menu
        cache_menu = menubar.addMenu("Cache")

        clear_cache_action = QAction("Clear Cache", self)
        clear_cache_action.triggered.connect(self.clear_cache)
        cache_menu.addAction(clear_cache_action)

        cache_info_action = QAction("Cache Info", self)
        cache_info_action.triggered.connect(self.show_cache_info)
        cache_menu.addAction(cache_info_action)

    def create_left_panel(self):
        """Create the left control panel"""
        panel = QWidget()
        layout = QVBoxLayout(panel)

        # Database connection section
        db_group = self.create_db_connection_group()
        layout.addWidget(db_group)

        # Saved connections
        saved_conn_group = self.create_saved_connections_group()
        layout.addWidget(saved_conn_group)

        # Query section
        query_group = self.create_query_group()
        layout.addWidget(query_group)

        # Saved queries
        saved_query_group = self.create_saved_queries_group()
        layout.addWidget(saved_query_group)

        # Options
        options_group = self.create_options_group()
        layout.addWidget(options_group)

        # Run button
        self.run_button = QPushButton("Connect & Run Query")
        self.run_button.setStyleSheet(
            "QPushButton { background-color: #4CAF50; color: white; padding: 10px; font-weight: bold; }"  # noqa
            "QPushButton:hover { background-color: #45a049; }"
        )
        self.run_button.clicked.connect(self.run_query)
        layout.addWidget(self.run_button)

        layout.addStretch()
        return panel

    def create_db_connection_group(self):
        """Create database connection group"""
        group = QGroupBox("Database Connection")
        layout = QFormLayout()

        self.db_type_combo = QComboBox()
        self.db_type_combo.addItems(["SQLite", "PostgreSQL"])
        self.db_type_combo.currentIndexChanged.connect(
            self.update_connection_fields
        )

        # Connection fields
        self.sqlite_path_input = QLineEdit()
        self.sqlite_browse_btn = QPushButton("Browse")
        self.sqlite_browse_btn.clicked.connect(self.browse_sqlite_file)

        sqlite_layout = QHBoxLayout()
        sqlite_layout.addWidget(self.sqlite_path_input)
        sqlite_layout.addWidget(self.sqlite_browse_btn)

        self.pg_host_input = QLineEdit("localhost")
        self.pg_port_input = QLineEdit("5432")
        self.pg_dbname_input = QLineEdit("mydatabase")
        self.pg_user_input = QLineEdit("postgres")
        self.pg_pass_input = QLineEdit()
        self.pg_pass_input.setEchoMode(QLineEdit.EchoMode.Password)

        # Connection name for saving
        self.conn_name_input = QLineEdit()

        layout.addRow("Database Type:", self.db_type_combo)
        layout.addRow("Connection Name:", self.conn_name_input)
        self.sqlite_row = layout.addRow("DB File Path:", sqlite_layout)
        self.pg_host_row = layout.addRow("Host:", self.pg_host_input)
        self.pg_port_row = layout.addRow("Port:", self.pg_port_input)
        self.pg_dbname_row = layout.addRow("DB Name:", self.pg_dbname_input)
        self.pg_user_row = layout.addRow("User:", self.pg_user_input)
        self.pg_pass_row = layout.addRow("Password:", self.pg_pass_input)

        # Save connection button
        save_conn_btn = QPushButton("Save Connection")
        save_conn_btn.clicked.connect(self.save_current_connection)
        layout.addRow(save_conn_btn)

        group.setLayout(layout)
        self.db_group = group
        return group

    def create_saved_connections_group(self):
        """Create saved connections group"""
        group = QGroupBox("Saved Connections")
        layout = QVBoxLayout()

        self.saved_connections_list = QListWidget()
        self.saved_connections_list.itemDoubleClicked.connect(
            self.load_saved_connection
        )
        layout.addWidget(self.saved_connections_list)

        btn_layout = QHBoxLayout()
        load_conn_btn = QPushButton("Load")
        load_conn_btn.clicked.connect(self.load_selected_connection)
        delete_conn_btn = QPushButton("Delete")
        delete_conn_btn.clicked.connect(self.delete_selected_connection)

        btn_layout.addWidget(load_conn_btn)
        btn_layout.addWidget(delete_conn_btn)
        layout.addLayout(btn_layout)

        group.setLayout(layout)
        return group

    def create_query_group(self):
        """Create query input group"""
        group = QGroupBox("Query")
        layout = QVBoxLayout()

        layout.addWidget(QLabel("SQL Query:"))
        self.query_input = QTextEdit(
            "SELECT id, name, image_data FROM images;"
        )
        self.query_input.setFixedHeight(100)
        layout.addWidget(self.query_input)

        # Query name and image column
        form_layout = QFormLayout()
        self.query_name_input = QLineEdit()
        self.image_column_input = QLineEdit("image_data")

        form_layout.addRow("Query Name:", self.query_name_input)
        form_layout.addRow("Image Column:", self.image_column_input)
        layout.addLayout(form_layout)

        # Save query button
        save_query_btn = QPushButton("Save Query")
        save_query_btn.clicked.connect(self.save_current_query)
        layout.addWidget(save_query_btn)

        group.setLayout(layout)
        return group

    def create_saved_queries_group(self):
        """Create saved queries group"""
        group = QGroupBox("Saved Queries")
        layout = QVBoxLayout()

        self.saved_queries_list = QListWidget()
        self.saved_queries_list.itemDoubleClicked.connect(
            self.load_saved_query
        )
        layout.addWidget(self.saved_queries_list)

        btn_layout = QHBoxLayout()
        load_query_btn = QPushButton("Load")
        load_query_btn.clicked.connect(self.load_selected_query)
        delete_query_btn = QPushButton("Delete")
        delete_query_btn.clicked.connect(self.delete_selected_query)

        btn_layout.addWidget(load_query_btn)
        btn_layout.addWidget(delete_query_btn)
        layout.addLayout(btn_layout)

        group.setLayout(layout)
        return group

    def create_options_group(self):
        """Create options group"""
        group = QGroupBox("Options")
        layout = QFormLayout()

        self.use_cache_checkbox = QCheckBox()
        self.use_cache_checkbox.setChecked(True)

        self.grid_columns_spinbox = QSpinBox()
        self.grid_columns_spinbox.setMinimum(1)
        self.grid_columns_spinbox.setMaximum(10)
        self.grid_columns_spinbox.setValue(3)

        layout.addRow("Use Cache:", self.use_cache_checkbox)
        layout.addRow("Grid Columns:", self.grid_columns_spinbox)

        group.setLayout(layout)
        return group

    def create_right_panel(self):
        """Create the right viewer panel"""
        panel = QWidget()
        layout = QVBoxLayout(panel)

        # View mode tabs
        self.view_tabs = QTabWidget()

        # Single view tab
        single_view = self.create_single_view()
        self.view_tabs.addTab(single_view, "Single View")

        # Grid view tab
        grid_view = self.create_grid_view()
        self.view_tabs.addTab(grid_view, "Grid View")

        # Info tab
        info_view = self.create_info_view()
        self.view_tabs.addTab(info_view, "Info")

        layout.addWidget(self.view_tabs)
        return panel

    def create_single_view(self):
        """Create single image view"""
        widget = QWidget()
        layout = QVBoxLayout(widget)

        # Status label
        self.status_label = QLabel("No query run yet.")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.status_label)

        # Image display
        self.image_label = ClickableImageLabel("Image will be displayed here")
        self.image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.image_label.setMinimumSize(QSize(400, 400))
        self.image_label.setStyleSheet(
            "border: 1px solid #ccc; background-color: #f9f9f9;"
        )
        self.image_label.clicked.connect(self.download_current_image)
        layout.addWidget(self.image_label, 1)

        # Other data display
        layout.addWidget(QLabel("Row Data:"))
        self.other_data_label = QLabel(
            "Other data from the row will appear here."
        )
        self.other_data_label.setAlignment(
            Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft
        )
        self.other_data_label.setWordWrap(True)
        self.other_data_label.setStyleSheet(
            "background-color: #f0f0f0; padding: 10px;"
        )
        layout.addWidget(self.other_data_label)

        # Navigation
        nav_layout = QHBoxLayout()
        self.prev_button = QPushButton("<< Previous")
        self.next_button = QPushButton("Next >>")
        self.download_button = QPushButton("Download Image")

        self.prev_button.clicked.connect(self.prev_image)
        self.next_button.clicked.connect(self.next_image)
        self.download_button.clicked.connect(self.download_current_image)

        nav_layout.addWidget(self.prev_button)
        nav_layout.addWidget(self.download_button)
        nav_layout.addWidget(self.next_button)
        layout.addLayout(nav_layout)

        return widget

    def create_grid_view(self):
        """Create grid image view"""
        widget = QWidget()
        layout = QVBoxLayout(widget)

        # Grid controls
        grid_controls = QHBoxLayout()
        grid_controls.addWidget(QLabel("Items per page:"))

        self.items_per_page = QSpinBox()
        self.items_per_page.setMinimum(9)
        self.items_per_page.setMaximum(100)
        self.items_per_page.setValue(21)
        grid_controls.addWidget(self.items_per_page)

        grid_controls.addStretch()

        self.page_label = QLabel("Page 1 of 1")
        grid_controls.addWidget(self.page_label)

        layout.addLayout(grid_controls)

        # Grid scroll area
        self.grid_scroll = QScrollArea()
        self.grid_widget = QWidget()
        self.grid_layout = QGridLayout(self.grid_widget)
        self.grid_scroll.setWidget(self.grid_widget)
        self.grid_scroll.setWidgetResizable(True)
        layout.addWidget(self.grid_scroll, 1)

        # Grid navigation
        grid_nav = QHBoxLayout()
        self.prev_page_btn = QPushButton("<< Previous Page")
        self.next_page_btn = QPushButton("Next Page >>")

        self.prev_page_btn.clicked.connect(self.prev_page)
        self.next_page_btn.clicked.connect(self.next_page)

        grid_nav.addWidget(self.prev_page_btn)
        grid_nav.addStretch()
        grid_nav.addWidget(self.next_page_btn)
        layout.addLayout(grid_nav)

        self.current_page = 0
        return widget

    def create_info_view(self):
        """Create info view for feedback and logs"""
        widget = QWidget()
        layout = QVBoxLayout(widget)

        self.info_browser = QTextBrowser()
        self.info_browser.setPlainText(
            "Application started. Ready for queries."
        )
        layout.addWidget(self.info_browser)

        return widget

    def browse_sqlite_file(self):
        """Browse for SQLite file"""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select SQLite Database",
            "",
            "SQLite Files (*.db *.sqlite *.sqlite3);;All Files (*)",
        )
        if file_path:
            self.sqlite_path_input.setText(file_path)

    def update_connection_fields(self):
        """Update connection fields based on database type"""
        is_sqlite = self.db_type_combo.currentText() == "SQLite"

        # Get form layout
        form_layout = self.db_group.layout()

        # Handle SQLite fields
        sqlite_label = form_layout.labelForField(
            self.sqlite_path_input.parent()
        )
        if sqlite_label:
            sqlite_label.setVisible(is_sqlite)
        self.sqlite_path_input.parent().setVisible(is_sqlite)

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

    def save_current_connection(self):
        """Save current connection settings"""
        conn_name = self.conn_name_input.text().strip()
        if not conn_name:
            QMessageBox.warning(
                self, "Error", "Please enter a connection name."
            )
            return

        conn_info = {
            "name": conn_name,
            "type": self.db_type_combo.currentText(),
        }

        if conn_info["type"] == "SQLite":
            conn_info["path"] = self.sqlite_path_input.text()
        else:
            conn_info.update(
                {
                    "host": self.pg_host_input.text(),
                    "port": self.pg_port_input.text(),
                    "dbname": self.pg_dbname_input.text(),
                    "user": self.pg_user_input.text(),
                    "password": self.pg_pass_input.text(),
                }
            )

        self.config_manager.add_connection(conn_info)
        self.refresh_saved_connections()
        self.add_info_message(f"Connection '{conn_name}' saved successfully.")

    def save_current_query(self):
        """Save current query"""
        query = self.query_input.toPlainText().strip()
        if not query:
            QMessageBox.warning(self, "Error", "Please enter a query.")
            return

        query_name = self.query_name_input.text().strip()
        self.config_manager.add_query(query, query_name)
        self.refresh_saved_queries()
        self.add_info_message("Query saved successfully.")

    def refresh_saved_connections(self):
        """Refresh the saved connections list"""
        self.saved_connections_list.clear()
        for conn in self.config_manager.config["connections"]:
            item = QListWidgetItem(f"{conn['name']} ({conn['type']})")
            item.setData(Qt.ItemDataRole.UserRole, conn)
            self.saved_connections_list.addItem(item)

    def refresh_saved_queries(self):
        """Refresh the saved queries list"""
        self.saved_queries_list.clear()
        for query in self.config_manager.config["queries"]:
            item = QListWidgetItem(query["name"])
            item.setData(Qt.ItemDataRole.UserRole, query)
            self.saved_queries_list.addItem(item)

    def load_selected_connection(self):
        """Load the selected connection"""
        current_item = self.saved_connections_list.currentItem()
        if current_item:
            self.load_saved_connection(current_item)

    def load_saved_connection(self, item):
        """Load a saved connection"""
        conn_info = item.data(Qt.ItemDataRole.UserRole)

        self.db_type_combo.setCurrentText(conn_info["type"])
        self.conn_name_input.setText(conn_info["name"])

        if conn_info["type"] == "SQLite":
            self.sqlite_path_input.setText(conn_info.get("path", ""))
        else:
            self.pg_host_input.setText(conn_info.get("host", "localhost"))
            self.pg_port_input.setText(conn_info.get("port", "5432"))
            self.pg_dbname_input.setText(conn_info.get("dbname", ""))
            self.pg_user_input.setText(conn_info.get("user", ""))
            self.pg_pass_input.setText(conn_info.get("password", ""))

        self.update_connection_fields()
        self.add_info_message(f"Loaded connection: {conn_info['name']}")

    def delete_selected_connection(self):
        """Delete the selected connection"""
        current_item = self.saved_connections_list.currentItem()
        if not current_item:
            return

        conn_info = current_item.data(Qt.ItemDataRole.UserRole)
        reply = QMessageBox.question(
            self,
            "Confirm Delete",
            f"Are you sure you want to delete connection '{conn_info['name']}'?",  # noqa: E501
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )

        if reply == QMessageBox.StandardButton.Yes:
            self.config_manager.config["connections"] = [
                c
                for c in self.config_manager.config["connections"]
                if c["name"] != conn_info["name"]
            ]
            self.config_manager.save_config()
            self.refresh_saved_connections()
            self.add_info_message(f"Deleted connection: {conn_info['name']}")

    def load_selected_query(self):
        """Load the selected query"""
        current_item = self.saved_queries_list.currentItem()
        if current_item:
            self.load_saved_query(current_item)

    def load_saved_query(self, item):
        """Load a saved query"""
        query_info = item.data(Qt.ItemDataRole.UserRole)
        self.query_input.setPlainText(query_info["query"])
        self.query_name_input.setText(query_info["name"])
        self.add_info_message(f"Loaded query: {query_info['name']}")

    def delete_selected_query(self):
        """Delete the selected query"""
        current_item = self.saved_queries_list.currentItem()
        if not current_item:
            return

        query_info = current_item.data(Qt.ItemDataRole.UserRole)
        reply = QMessageBox.question(
            self,
            "Confirm Delete",
            f"Are you sure you want to delete query '{query_info['name']}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )

        if reply == QMessageBox.StandardButton.Yes:
            self.config_manager.config["queries"] = [
                q
                for q in self.config_manager.config["queries"]
                if q["query"] != query_info["query"]
            ]
            self.config_manager.save_config()
            self.refresh_saved_queries()
            self.add_info_message(f"Deleted query: {query_info['name']}")

    def run_query(self):
        """Run the database query"""
        # Close existing connection
        if self.db_connection:
            self.db_connection.close()
            self.db_connection = None

        # Get inputs
        db_type = self.db_type_combo.currentText()
        query_text = self.query_input.toPlainText()
        self.image_column_name = self.image_column_input.text().strip()

        if not self.image_column_name:
            QMessageBox.warning(
                self, "Input Error", "Please specify the image column name."
            )
            return

        # Check cache first
        connection_info = self.get_connection_info()
        cache_key = self.cache_manager.get_cache_key(
            connection_info, query_text
        )

        if self.use_cache_checkbox.isChecked():
            cached_results = self.cache_manager.get_cached_results(cache_key)
            if cached_results:
                self.results = cached_results["results"]
                self.column_names = cached_results["column_names"]
                self.add_info_message("Loaded results from cache")
                self.process_results()
                return

        # Connect to database
        try:
            self.add_info_message("Connecting to database...")
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

            self.add_info_message("Connected successfully. Executing query...")

        except Exception as e:
            QMessageBox.critical(
                self,
                "Connection Error",
                f"Could not connect to database:\n{e}",
            )
            self.add_info_message(f"Connection failed: {e}")
            return

        # Start query worker thread
        self.run_button.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)  # Indeterminate progress

        self.query_worker = QueryWorker(self.db_connection, query_text)
        self.query_worker.finished.connect(self.on_query_finished)
        self.query_worker.progress.connect(self.add_info_message)
        self.query_worker.start()

    def get_connection_info(self) -> dict:
        """Get current connection info for caching"""
        if self.db_type_combo.currentText() == "SQLite":
            return {"type": "sqlite", "path": self.sqlite_path_input.text()}
        else:
            return {
                "type": "postgresql",
                "host": self.pg_host_input.text(),
                "port": self.pg_port_input.text(),
                "dbname": self.pg_dbname_input.text(),
                "user": self.pg_user_input.text(),
            }

    def on_query_finished(self, results: list, column_names: list, error: str):
        """Handle query completion"""
        self.run_button.setEnabled(True)
        self.progress_bar.setVisible(False)

        if error:
            QMessageBox.critical(
                self, "Query Error", f"An error occurred:\n{error}"
            )
            self.add_info_message(f"Query failed: {error}")
            return

        self.results = results
        self.column_names = column_names

        # Cache results if enabled
        if self.use_cache_checkbox.isChecked():
            connection_info = self.get_connection_info()
            query_text = self.query_input.toPlainText()
            cache_key = self.cache_manager.get_cache_key(
                connection_info, query_text
            )
            self.cache_manager.cache_results(cache_key, results, column_names)
            self.add_info_message("Results cached successfully")

        self.process_results()

    def process_results(self):
        """Process and display query results"""
        if self.image_column_name not in self.column_names:
            QMessageBox.warning(
                self,
                "Error",
                f"Image column '{self.image_column_name}' not found in query results.",  # noqa: E501
            )
            return

        if not self.results:
            self.status_label.setText(
                "Query executed, but returned no results."
            )
            self.image_label.clear()
            self.other_data_label.clear()
            self.current_index = -1
            self.add_info_message("No results found")
        else:
            self.current_index = 0
            self.display_current_record()
            self.update_grid_view()
            self.add_info_message(
                f"Query successful: {len(self.results)} records found"
            )

        self.update_nav_buttons()

    def display_current_record(self):
        """Display the current record in single view"""
        if not (0 <= self.current_index < len(self.results)):
            return

        record = self.results[self.current_index]
        record_dict = dict(zip(self.column_names, record))

        # Process and display image
        image_data = record_dict.get(self.image_column_name)
        self.image_label.clear()

        if image_data:
            try:
                image_bytes = self.process_image_data(image_data)
                if image_bytes:
                    q_image = QImage.fromData(image_bytes)
                    if q_image.isNull():
                        raise ValueError(
                            "Data could not be parsed as an image."
                        )
                    pixmap = QPixmap.fromImage(q_image)
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
                self.add_info_message(f"Image load error: {e}")
        else:
            self.image_label.setText("No image in this record.")

        # Display other data
        other_data_str = ""
        for col, val in record_dict.items():
            if col != self.image_column_name:
                display_val = str(val)
                if len(display_val) > 150:
                    display_val = display_val[:150] + "..."
                other_data_str += f"<b>{col}:</b> {display_val}<br>"
        self.other_data_label.setText(other_data_str)

        # Update status
        self.status_label.setText(
            f"Record {self.current_index + 1} of {len(self.results)}"
        )

    def process_image_data(self, image_data):
        """Process image data from database"""
        if isinstance(image_data, str):
            return base64.b64decode(image_data)
        elif isinstance(image_data, (bytes, bytearray)):
            return bytes(image_data)
        return None

    def update_grid_view(self):
        """Update the grid view with current results"""
        # Clear existing grid
        for item in self.grid_items:
            item.setParent(None)
        self.grid_items = []

        if not self.results:
            return

        items_per_page = self.items_per_page.value()
        columns = self.grid_columns_spinbox.value()

        start_idx = self.current_page * items_per_page
        end_idx = min(start_idx + items_per_page, len(self.results))

        row, col = 0, 0

        for idx in range(start_idx, end_idx):
            record = self.results[idx]
            record_dict = dict(zip(self.column_names, record))

            # Create grid item
            item_widget = QFrame()
            item_widget.setFrameStyle(QFrame.Shape.Box)
            item_widget.setFixedSize(200, 250)
            item_layout = QVBoxLayout(item_widget)

            # Image
            img_label = ClickableImageLabel()
            img_label.setFixedSize(180, 180)
            img_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            img_label.setStyleSheet(
                "border: 1px solid #ccc; background-color: #f9f9f9;"
            )

            image_data = record_dict.get(self.image_column_name)
            if image_data:
                try:
                    image_bytes = self.process_image_data(image_data)
                    if image_bytes:
                        q_image = QImage.fromData(image_bytes)
                        if not q_image.isNull():
                            pixmap = QPixmap.fromImage(q_image)
                            img_label.setPixmap(
                                pixmap.scaled(
                                    img_label.size(),
                                    Qt.AspectRatioMode.KeepAspectRatio,
                                    Qt.TransformationMode.SmoothTransformation,
                                )
                            )
                        else:
                            img_label.setText("Invalid\nImage")
                    else:
                        img_label.setText("Unsupported\nFormat")
                except Exception as e:  # noqa: F841
                    img_label.setText("Error\nLoading")
            else:
                img_label.setText("No Image")

            # Connect click to download
            img_label.clicked.connect(
                lambda idx=idx: self.download_image_by_index(idx)
            )

            # Label with primary info
            info_text = f"Record {idx + 1}"
            # Try to find a name or title column
            for col in ["name", "title", "filename", "id"]:
                if col in record_dict:
                    val = str(record_dict[col])
                    if len(val) > 20:
                        val = val[:20] + "..."
                    info_text = val
                    break

            info_label = QLabel(info_text)
            info_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            info_label.setWordWrap(True)

            item_layout.addWidget(img_label)
            item_layout.addWidget(info_label)

            self.grid_layout.addWidget(item_widget, row, col)
            self.grid_items.append(item_widget)

            col += 1
            if col >= columns:
                col = 0
                row += 1

        # Update page info
        total_pages = (len(self.results) - 1) // items_per_page + 1
        self.page_label.setText(
            f"Page {self.current_page + 1} of {total_pages}"
        )

        # Update page navigation buttons
        self.prev_page_btn.setEnabled(self.current_page > 0)
        self.next_page_btn.setEnabled(self.current_page < total_pages - 1)

    def prev_page(self):
        """Go to previous page in grid view"""
        if self.current_page > 0:
            self.current_page -= 1
            self.update_grid_view()

    def next_page(self):
        """Go to next page in grid view"""
        items_per_page = self.items_per_page.value()
        total_pages = (len(self.results) - 1) // items_per_page + 1
        if self.current_page < total_pages - 1:
            self.current_page += 1
            self.update_grid_view()

    def next_image(self):
        """Move to next record in single view"""
        if self.current_index < len(self.results) - 1:
            self.current_index += 1
            self.display_current_record()
            self.update_nav_buttons()

    def prev_image(self):
        """Move to previous record in single view"""
        if self.current_index > 0:
            self.current_index -= 1
            self.display_current_record()
            self.update_nav_buttons()

    def update_nav_buttons(self):
        """Update navigation button states"""
        if not self.results:
            self.prev_button.setEnabled(False)
            self.next_button.setEnabled(False)
            self.download_button.setEnabled(False)
        else:
            self.prev_button.setEnabled(self.current_index > 0)
            self.next_button.setEnabled(
                self.current_index < len(self.results) - 1
            )
            self.download_button.setEnabled(True)

    def download_current_image(self):
        """Download the currently displayed image"""
        if 0 <= self.current_index < len(self.results):
            self.download_image_by_index(self.current_index)

    def download_image_by_index(self, index: int):
        """Download image by record index"""
        if not (0 <= index < len(self.results)):
            return

        record = self.results[index]
        record_dict = dict(zip(self.column_names, record))
        image_data = record_dict.get(self.image_column_name)

        if not image_data:
            QMessageBox.information(
                self, "No Image", "This record contains no image data."
            )
            return

        try:
            image_bytes = self.process_image_data(image_data)
            if not image_bytes:
                QMessageBox.warning(
                    self, "Error", "Unable to process image data."
                )
                return

            # Determine file extension
            q_image = QImage.fromData(image_bytes)
            if q_image.isNull():
                QMessageBox.warning(self, "Error", "Invalid image data.")
                return

            # Get filename suggestion
            suggested_name = f"image_{index + 1}"
            for col in ["name", "filename", "title"]:
                if col in record_dict:
                    suggested_name = str(record_dict[col]).replace(" ", "_")
                    break

            # Show save dialog
            file_path, _ = QFileDialog.getSaveFileName(
                self,
                "Save Image",
                f"{suggested_name}.png",
                "PNG Files (*.png);;JPEG Files (*.jpg);;All Files (*)",
            )

            if file_path:
                if q_image.save(file_path):
                    self.add_info_message(f"Image saved: {file_path}")
                    QMessageBox.information(
                        self, "Success", f"Image saved to:\n{file_path}"
                    )
                else:
                    self.add_info_message(f"Failed to save image: {file_path}")
                    QMessageBox.warning(self, "Error", "Failed to save image.")

        except Exception as e:
            self.add_info_message(f"Download error: {e}")
            QMessageBox.critical(
                self, "Error", f"An error occurred while downloading:\n{e}"
            )

    def export_current_image(self):
        """Export current image (menu action)"""
        self.download_current_image()

    def clear_cache(self):
        """Clear all cached data"""
        reply = QMessageBox.question(
            self,
            "Confirm Clear Cache",
            "Are you sure you want to clear all cached data?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )

        if reply == QMessageBox.StandardButton.Yes:
            self.cache_manager.clear_cache()
            self.add_info_message("Cache cleared successfully")
            QMessageBox.information(
                self, "Cache Cleared", "All cached data has been cleared."
            )

    def show_cache_info(self):
        """Show cache information"""
        cache_size = self.cache_manager.get_cache_size()
        cache_entries = len(self.cache_manager.cache_index)

        QMessageBox.information(
            self,
            "Cache Information",
            f"Cache entries: {cache_entries}\n"
            f"Total cached records: {cache_size}\n"
            f"Cache directory: {self.cache_manager.cache_dir}",
        )

    def add_info_message(self, message: str):
        """Add a message to the info view"""
        from datetime import datetime

        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] {message}"

        current_text = self.info_browser.toPlainText()
        new_text = (
            current_text + "\n" + formatted_message
            if current_text
            else formatted_message
        )
        self.info_browser.setPlainText(new_text)

        # Auto-scroll to bottom
        cursor = self.info_browser.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        self.info_browser.setTextCursor(cursor)

        # Update status bar
        self.status_bar.showMessage(message, 5000)

    def load_saved_settings(self):
        """Load saved settings on startup"""
        config = self.config_manager.config

        # Restore window geometry
        if config.get("window_geometry"):
            try:
                self.restoreGeometry(bytes.fromhex(config["window_geometry"]))
            except Exception as e:  # noqa: F841
                pass

        # Load last connection
        if config.get("last_connection"):
            conn = config["last_connection"]
            if conn.get("type") == "SQLite":
                self.db_type_combo.setCurrentText("SQLite")
                self.sqlite_path_input.setText(conn.get("path", ""))
            else:
                self.db_type_combo.setCurrentText("PostgreSQL")
                self.pg_host_input.setText(conn.get("host", "localhost"))
                self.pg_port_input.setText(conn.get("port", "5432"))
                self.pg_dbname_input.setText(conn.get("dbname", ""))
                self.pg_user_input.setText(conn.get("user", ""))

        # Load last query
        if config.get("last_query"):
            self.query_input.setPlainText(config["last_query"])

        # Load last image column
        if config.get("last_image_column"):
            self.image_column_input.setText(config["last_image_column"])

        # Load grid columns
        if config.get("grid_columns"):
            self.grid_columns_spinbox.setValue(config["grid_columns"])

        # Refresh saved items
        self.refresh_saved_connections()
        self.refresh_saved_queries()

        self.update_connection_fields()

    def save_current_settings(self):
        """Save current settings"""
        config = self.config_manager.config

        # Save window geometry
        config["window_geometry"] = self.saveGeometry().toHex().data().decode()

        # Save last connection
        config["last_connection"] = self.get_connection_info()

        # Save last query and image column
        config["last_query"] = self.query_input.toPlainText()
        config["last_image_column"] = self.image_column_input.text()
        config["grid_columns"] = self.grid_columns_spinbox.value()

        self.config_manager.save_config()

    def closeEvent(self, event):
        """Handle application close"""
        self.save_current_settings()

        if self.db_connection:
            self.db_connection.close()
            self.add_info_message("Database connection closed")

        self.add_info_message("Application closing")
        super().closeEvent(event)

    def resizeEvent(self, event):
        """Handle window resize"""
        if hasattr(self, "current_index") and self.current_index != -1:
            self.display_current_record()
        super().resizeEvent(event)


def main():
    """Main application entry point"""
    app = QApplication(sys.argv)
    app.setApplicationName("Enhanced Database Image Viewer")
    app.setApplicationVersion("2.0")

    viewer = ImageViewer()
    viewer.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
