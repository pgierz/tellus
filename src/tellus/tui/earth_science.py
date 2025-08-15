"""Earth science-specific widgets and features for the Tellus TUI."""

from typing import Dict, List, Optional, Any, Tuple
import re
from datetime import datetime
from pathlib import Path

from textual.widget import Widget
from textual.containers import Container, Horizontal, Vertical
from textual.widgets import (
    Static, DataTable, Tree, Select, Checkbox, 
    ProgressBar, Input, Label, Button
)
from textual.reactive import reactive
from textual import on

from .widgets import FileTree


class NetCDFPreview(Widget):
    """Widget for previewing NetCDF file metadata and structure."""
    
    def __init__(self, **kwargs):
        """Initialize the NetCDF preview widget."""
        super().__init__(**kwargs)
        self.current_file = ""
        self.metadata = {}

    def compose(self):
        """Create the NetCDF preview layout."""
        with Container():
            yield Static("NetCDF File Structure", classes="section-header")
            
            with Horizontal():
                # Dimensions
                with Vertical(classes="netcdf-section"):
                    yield Static("Dimensions", classes="subsection-header")
                    yield DataTable(id="dimensions-table")
                
                # Variables
                with Vertical(classes="netcdf-section"):
                    yield Static("Variables", classes="subsection-header")
                    yield DataTable(id="variables-table")
            
            # Global attributes
            with Container():
                yield Static("Global Attributes", classes="subsection-header")
                yield DataTable(id="attributes-table")
            
            # Data preview
            with Container():
                yield Static("Data Preview", classes="subsection-header")
                yield Static("Select a variable to preview data", id="data-preview")

    def on_mount(self) -> None:
        """Initialize the NetCDF preview when mounted."""
        # Set up tables
        dims_table = self.query_one("#dimensions-table")
        dims_table.add_columns("Name", "Size", "Unlimited")
        
        vars_table = self.query_one("#variables-table")
        vars_table.add_columns("Name", "Type", "Dimensions", "Shape")
        
        attrs_table = self.query_one("#attributes-table")
        attrs_table.add_columns("Attribute", "Value")

    def update_file(self, file_path: str, metadata: Dict[str, Any]) -> None:
        """Update the preview with new file metadata."""
        self.current_file = file_path
        self.metadata = metadata
        
        # Update dimensions table
        dims_table = self.query_one("#dimensions-table")
        dims_table.clear()
        
        for dim_name, dim_info in metadata.get('dimensions', {}).items():
            dims_table.add_row(
                dim_name,
                str(dim_info.get('size', 'Unknown')),
                "Yes" if dim_info.get('unlimited', False) else "No"
            )
        
        # Update variables table
        vars_table = self.query_one("#variables-table")
        vars_table.clear()
        
        for var_name, var_info in metadata.get('variables', {}).items():
            dims_str = ', '.join(var_info.get('dimensions', []))
            shape_str = str(var_info.get('shape', []))
            
            vars_table.add_row(
                var_name,
                var_info.get('dtype', 'Unknown'),
                dims_str,
                shape_str
            )
        
        # Update attributes table
        attrs_table = self.query_one("#attributes-table")
        attrs_table.clear()
        
        for attr_name, attr_value in metadata.get('global_attributes', {}).items():
            # Truncate long values
            value_str = str(attr_value)
            if len(value_str) > 100:
                value_str = value_str[:97] + "..."
            
            attrs_table.add_row(attr_name, value_str)

    @on(DataTable.RowSelected, "#variables-table")
    def on_variable_selected(self, event: DataTable.RowSelected) -> None:
        """Handle variable selection for data preview."""
        if event.row_key:
            var_name = str(event.row_key.value)
            self.show_variable_preview(var_name)

    def show_variable_preview(self, variable_name: str) -> None:
        """Show preview of variable data."""
        preview_widget = self.query_one("#data-preview")
        
        var_info = self.metadata.get('variables', {}).get(variable_name, {})
        if not var_info:
            preview_widget.update("Variable not found")
            return
        
        # Show variable metadata
        preview_text = f"""Variable: {variable_name}
Type: {var_info.get('dtype', 'Unknown')}
Dimensions: {', '.join(var_info.get('dimensions', []))}
Shape: {var_info.get('shape', [])}

Attributes:"""
        
        for attr_name, attr_value in var_info.get('attributes', {}).items():
            preview_text += f"\n  {attr_name}: {attr_value}"
        
        # Add sample data if available
        if 'sample_data' in var_info:
            preview_text += f"\n\nSample Data:\n{var_info['sample_data']}"
        
        preview_widget.update(preview_text)


class ModelOutputClassifier(Widget):
    """Widget for classifying and organizing model output files."""
    
    def __init__(self, **kwargs):
        """Initialize the model output classifier."""
        super().__init__(**kwargs)
        self.file_classifications = {}

    def compose(self):
        """Create the classifier layout."""
        with Container():
            yield Static("Model Output Classification", classes="section-header")
            
            with Horizontal():
                # Classification rules
                with Vertical(classes="sidebar"):
                    yield Static("Classification Rules", classes="subsection-header")
                    
                    yield Label("Output Frequency:")
                    yield Select([
                        ("Daily", "daily"),
                        ("Monthly", "monthly"),
                        ("Yearly", "yearly"),
                        ("Instantaneous", "instant"),
                        ("Unknown", "unknown"),
                    ], id="frequency-filter")
                    
                    yield Label("Variable Type:")
                    yield Select([
                        ("Atmospheric", "atm"),
                        ("Oceanic", "ocean"),
                        ("Land", "land"),
                        ("Ice", "ice"),
                        ("Biogeochemical", "bgc"),
                        ("Mixed", "mixed"),
                    ], id="realm-filter")
                    
                    yield Label("File Purpose:")
                    yield Select([
                        ("Restart Files", "restart"),
                        ("History Files", "history"),
                        ("Diagnostic Files", "diagnostic"),
                        ("Forcing Files", "forcing"),
                        ("Auxiliary Files", "aux"),
                    ], id="purpose-filter")
                
                # File classification results
                with Vertical(classes="main-content"):
                    yield Static("Classified Files", classes="subsection-header")
                    yield DataTable(id="classified-files")
            
            # Actions
            with Horizontal(classes="action-buttons"):
                yield Button("Auto-Classify", id="auto-classify", variant="primary")
                yield Button("Apply Rules", id="apply-rules")
                yield Button("Export Classification", id="export-classification")

    def on_mount(self) -> None:
        """Initialize the classifier when mounted."""
        table = self.query_one("#classified-files")
        table.add_columns("File", "Frequency", "Realm", "Purpose", "Confidence")

    def classify_files(self, files: List[Dict[str, Any]]) -> None:
        """Classify a list of files based on Earth science patterns."""
        for file_info in files:
            classification = self._classify_single_file(file_info)
            self.file_classifications[file_info['path']] = classification
        
        self.update_classification_table()

    def _classify_single_file(self, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Classify a single file based on its properties."""
        filename = Path(file_info['path']).name
        
        # Initialize classification
        classification = {
            'frequency': 'unknown',
            'realm': 'unknown',
            'purpose': 'unknown',
            'confidence': 0.0
        }
        
        # Frequency classification based on filename patterns
        frequency_patterns = {
            'daily': [r'daily', r'_day_', r'_d\d{8}', r'_\d{8}_'],
            'monthly': [r'monthly', r'_mon_', r'_m\d{6}', r'_\d{6}_'],
            'yearly': [r'yearly', r'annual', r'_y\d{4}', r'_\d{4}_'],
            'instant': [r'instant', r'_inst_', r'snapshot'],
        }
        
        for freq, patterns in frequency_patterns.items():
            for pattern in patterns:
                if re.search(pattern, filename, re.IGNORECASE):
                    classification['frequency'] = freq
                    classification['confidence'] += 0.3
                    break
        
        # Realm classification
        realm_patterns = {
            'atm': [r'atm', r'atmosphere', r'_ta_', r'_ua_', r'_va_', r'_ps_'],
            'ocean': [r'ocean', r'_so_', r'_thetao_', r'_uo_', r'_vo_'],
            'land': [r'land', r'_mrso_', r'_lai_', r'_frac_'],
            'ice': [r'ice', r'_sic_', r'_sit_', r'seaice'],
            'bgc': [r'bgc', r'carbon', r'_co2_', r'_o2_', r'_no3_'],
        }
        
        for realm, patterns in realm_patterns.items():
            for pattern in patterns:
                if re.search(pattern, filename, re.IGNORECASE):
                    classification['realm'] = realm
                    classification['confidence'] += 0.3
                    break
        
        # Purpose classification
        purpose_patterns = {
            'restart': [r'restart', r'_rst_', r'_r\d{8}'],
            'history': [r'hist', r'_h\d_', r'history'],
            'diagnostic': [r'diag', r'diagnostic', r'_diag_'],
            'forcing': [r'forc', r'forcing', r'_bc_'],
            'aux': [r'aux', r'auxiliary', r'_grid_', r'_area_'],
        }
        
        for purpose, patterns in purpose_patterns.items():
            for pattern in patterns:
                if re.search(pattern, filename, re.IGNORECASE):
                    classification['purpose'] = purpose
                    classification['confidence'] += 0.2
                    break
        
        # Additional confidence from file extension
        if filename.endswith('.nc'):
            classification['confidence'] += 0.2
        
        return classification

    def update_classification_table(self) -> None:
        """Update the classification results table."""
        table = self.query_one("#classified-files")
        table.clear()
        
        for file_path, classification in self.file_classifications.items():
            confidence_str = f"{classification['confidence']:.1f}"
            
            table.add_row(
                Path(file_path).name,
                classification['frequency'].title(),
                classification['realm'].title(),
                classification['purpose'].title(),
                confidence_str
            )

    @on(Button.Pressed, "#auto-classify")
    def on_auto_classify(self) -> None:
        """Handle auto-classify button press."""
        # This would trigger automatic classification of all files
        pass

    @on(Button.Pressed, "#apply-rules")
    def on_apply_rules(self) -> None:
        """Handle apply rules button press."""
        # This would apply current filter settings to re-classify files
        pass

    @on(Button.Pressed, "#export-classification")
    def on_export_classification(self) -> None:
        """Handle export classification button press."""
        # This would export the classification results
        pass


class EarthSystemArchiveBrowser(Widget):
    """Specialized archive browser for Earth System Model data."""
    
    def __init__(self, **kwargs):
        """Initialize the Earth system archive browser."""
        super().__init__(**kwargs)
        self.current_archive = ""
        self.file_metadata = {}

    def compose(self):
        """Create the Earth system browser layout."""
        with Container():
            yield Static("Earth System Model Archive Browser", classes="section-header")
            
            with Horizontal():
                # File tree with Earth science organization
                with Vertical(classes="sidebar"):
                    yield Static("Archive Structure", classes="subsection-header")
                    yield FileTree(id="earth-file-tree")
                    
                    # Quick filters for Earth science data
                    yield Static("Quick Filters", classes="subsection-header")
                    yield Checkbox("Atmospheric Data", id="filter-atm")
                    yield Checkbox("Ocean Data", id="filter-ocean")
                    yield Checkbox("Land Data", id="filter-land")
                    yield Checkbox("Ice Data", id="filter-ice")
                    yield Checkbox("Restart Files", id="filter-restart")
                    yield Checkbox("NetCDF Files Only", id="filter-netcdf")
                
                # File preview with Earth science metadata
                with Vertical(classes="main-content"):
                    yield Static("File Details", classes="subsection-header")
                    yield NetCDFPreview(id="netcdf-preview")
            
            # Earth science-specific actions
            with Horizontal(classes="action-buttons"):
                yield Button("Extract by Realm", id="extract-realm", variant="primary")
                yield Button("Extract by Frequency", id="extract-frequency")
                yield Button("Create Subset", id="create-subset")
                yield Button("Verify Standards", id="verify-standards")

    def load_archive(self, archive_id: str, files: List[Dict[str, Any]]) -> None:
        """Load an archive with Earth science organization."""
        self.current_archive = archive_id
        
        # Organize files by Earth science categories
        organized_files = self._organize_earth_science_files(files)
        
        # Update file tree
        file_tree = self.query_one("#earth-file-tree")
        file_tree.update_files(organized_files)

    def _organize_earth_science_files(self, files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Organize files according to Earth science conventions."""
        organized = []
        
        # Group files by realm and frequency
        realm_structure = {
            'atmosphere': [],
            'ocean': [],
            'land': [],
            'ice': [],
            'other': []
        }
        
        for file_info in files:
            # Classify file
            classifier = ModelOutputClassifier()
            classification = classifier._classify_single_file(file_info)
            
            # Add classification to file info
            file_info['classification'] = classification
            
            # Determine realm category
            realm = classification.get('realm', 'unknown')
            if realm in realm_structure:
                realm_structure[realm].append(file_info)
            else:
                realm_structure['other'].append(file_info)
        
        # Create organized structure
        for realm, realm_files in realm_structure.items():
            if realm_files:
                # Group by frequency within realm
                freq_groups = {}
                for file_info in realm_files:
                    freq = file_info['classification'].get('frequency', 'unknown')
                    if freq not in freq_groups:
                        freq_groups[freq] = []
                    freq_groups[freq].append(file_info)
                
                # Add to organized list with structure
                for freq, freq_files in freq_groups.items():
                    for file_info in freq_files:
                        # Create virtual path for organization
                        original_path = file_info['path']
                        organized_path = f"{realm}/{freq}/{Path(original_path).name}"
                        
                        organized_file = file_info.copy()
                        organized_file['path'] = organized_path
                        organized_file['original_path'] = original_path
                        organized.append(organized_file)
        
        return organized

    @on(Checkbox.Changed)
    def on_filter_changed(self, event: Checkbox.Changed) -> None:
        """Handle filter checkbox changes."""
        # Apply filters to the file tree
        self._apply_filters()

    def _apply_filters(self) -> None:
        """Apply current filters to the file tree."""
        # Get current filter states
        filters = {
            'atm': self.query_one("#filter-atm").value,
            'ocean': self.query_one("#filter-ocean").value,
            'land': self.query_one("#filter-land").value,
            'ice': self.query_one("#filter-ice").value,
            'restart': self.query_one("#filter-restart").value,
            'netcdf': self.query_one("#filter-netcdf").value,
        }
        
        # This would filter the file tree based on the selected filters
        # Implementation would update the file tree display

    @on(Button.Pressed, "#extract-realm")
    def on_extract_by_realm(self) -> None:
        """Handle extract by realm button press."""
        # This would show a dialog to extract files by Earth science realm
        pass

    @on(Button.Pressed, "#extract-frequency")
    def on_extract_by_frequency(self) -> None:
        """Handle extract by frequency button press."""
        # This would show a dialog to extract files by temporal frequency
        pass

    @on(Button.Pressed, "#create-subset")
    def on_create_subset(self) -> None:
        """Handle create subset button press."""
        # This would create a spatial/temporal subset of the data
        pass

    @on(Button.Pressed, "#verify-standards")
    def on_verify_standards(self) -> None:
        """Handle verify standards button press."""
        # This would check files against Earth science standards (CF, CMIP, etc.)
        pass


class DatasetSummary(Widget):
    """Widget for displaying Earth science dataset summaries."""
    
    def __init__(self, **kwargs):
        """Initialize the dataset summary widget."""
        super().__init__(**kwargs)

    def compose(self):
        """Create the dataset summary layout."""
        with Container():
            yield Static("Dataset Summary", classes="section-header")
            
            with Horizontal():
                # Temporal coverage
                with Vertical(classes="summary-section"):
                    yield Static("Temporal Coverage", classes="subsection-header")
                    yield Static("", id="temporal-info")
                
                # Spatial coverage
                with Vertical(classes="summary-section"):
                    yield Static("Spatial Coverage", classes="subsection-header")
                    yield Static("", id="spatial-info")
            
            with Horizontal():
                # Variable summary
                with Vertical(classes="summary-section"):
                    yield Static("Variables", classes="subsection-header")
                    yield DataTable(id="variables-summary")
                
                # File statistics
                with Vertical(classes="summary-section"):
                    yield Static("File Statistics", classes="subsection-header")
                    yield Static("", id="file-stats")

    def on_mount(self) -> None:
        """Initialize the summary when mounted."""
        vars_table = self.query_one("#variables-summary")
        vars_table.add_columns("Variable", "Standard Name", "Units", "Files")

    def update_summary(self, archive_metadata: Dict[str, Any]) -> None:
        """Update the summary with archive metadata."""
        # Update temporal coverage
        temporal_info = self.query_one("#temporal-info")
        temporal_data = archive_metadata.get('temporal_coverage', {})
        
        start_date = temporal_data.get('start_date', 'Unknown')
        end_date = temporal_data.get('end_date', 'Unknown')
        frequency = temporal_data.get('frequency', 'Unknown')
        
        temporal_info.update(f"""Start Date: {start_date}
End Date: {end_date}
Frequency: {frequency}
Duration: {temporal_data.get('duration', 'Unknown')}""")
        
        # Update spatial coverage
        spatial_info = self.query_one("#spatial-info")
        spatial_data = archive_metadata.get('spatial_coverage', {})
        
        spatial_info.update(f"""Grid Type: {spatial_data.get('grid_type', 'Unknown')}
Resolution: {spatial_data.get('resolution', 'Unknown')}
Lat Range: {spatial_data.get('lat_range', 'Unknown')}
Lon Range: {spatial_data.get('lon_range', 'Unknown')}""")
        
        # Update variables table
        vars_table = self.query_one("#variables-summary")
        vars_table.clear()
        
        for var_info in archive_metadata.get('variables', []):
            vars_table.add_row(
                var_info.get('name', ''),
                var_info.get('standard_name', ''),
                var_info.get('units', ''),
                str(var_info.get('file_count', 0))
            )
        
        # Update file statistics
        file_stats = self.query_one("#file-stats")
        stats = archive_metadata.get('file_statistics', {})
        
        file_stats.update(f"""Total Files: {stats.get('total_files', 0)}
Total Size: {stats.get('total_size', 'Unknown')}
NetCDF Files: {stats.get('netcdf_files', 0)}
Average File Size: {stats.get('avg_file_size', 'Unknown')}
Compression Ratio: {stats.get('compression_ratio', 'Unknown')}""")