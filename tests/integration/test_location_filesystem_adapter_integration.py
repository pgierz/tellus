"""
Integration tests for Location with FSSpecAdapter and PathSandboxedFileSystem.

This test suite verifies that the FSSpecAdapter works correctly with the
PathSandboxedFileSystem fix, ensuring proper integration between the new
infrastructure adapters and the security fixes.
"""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tellus.domain.entities.location import LocationEntity, LocationKind
from tellus.infrastructure.adapters.fsspec_adapter import (FSSpecAdapter,
                                                           ProgressTracker)
from tellus.location import Location as LegacyLocation
from tellus.location import LocationKind as LegacyLocationKind
from tellus.location.sandboxed_filesystem import (PathSandboxedFileSystem,
                                                  PathValidationError)


class TestFSSpecAdapterIntegration:
    """Test FSSpecAdapter with PathSandboxedFileSystem integration."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_root = Path(tempfile.mkdtemp())
        self.source_dir = self.temp_root / "source_data"
        self.dest_dir = self.temp_root / "dest_data"
        self.outside_dir = self.temp_root / "outside_data"
        
        for d in [self.source_dir, self.dest_dir, self.outside_dir]:
            d.mkdir(parents=True)
        
        # Create test files
        self.test_files = {
            "small.txt": "small file content",
            "large.txt": "large file content " * 1000,
            "data.nc": "netcdf-like data content",
            "subdir/nested.txt": "nested file content"
        }
        
        for rel_path, content in self.test_files.items():
            file_path = self.source_dir / rel_path
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(content)
        
        # Create file outside sandbox (should be inaccessible)
        (self.outside_dir / "outside.txt").write_text("outside content")

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def test_fsspec_adapter_with_local_filesystem(self):
        """Test FSSpecAdapter with local filesystem and path sandboxing."""
        # Create LocationEntity for adapter
        entity = LocationEntity(
            name="local_adapter_test",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.source_dir),
                "storage_options": {"auto_mkdir": True}
            }
        )
        
        adapter = FSSpecAdapter(entity)
        
        # Test connection
        connection_result = adapter.test_connection()
        assert connection_result["success"] is True
        assert connection_result["protocol"] == "file"
        
        # Test file operations
        assert adapter.exists("small.txt")
        assert adapter.isfile("small.txt")
        assert not adapter.isdir("small.txt")
        
        assert adapter.exists("subdir")
        assert adapter.isdir("subdir")
        assert adapter.exists("subdir/nested.txt")
        
        # Test file info
        info = adapter.info("small.txt")
        assert "size" in info
        
        size = adapter.size("small.txt")
        assert size == len("small file content")

    def test_fsspec_adapter_file_download(self):
        """Test FSSpecAdapter file download with sandboxing."""
        entity = LocationEntity(
            name="download_test",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.source_dir)
            }
        )
        
        adapter = FSSpecAdapter(entity)
        
        # Test single file download
        local_path = self.dest_dir / "downloaded_small.txt"
        result = adapter.get_file("small.txt", str(local_path))
        
        assert result == str(local_path)
        assert local_path.exists()
        assert local_path.read_text() == "small file content"
        
        # Test nested file download
        nested_local = self.dest_dir / "downloaded_nested.txt"
        result = adapter.get_file("subdir/nested.txt", str(nested_local))
        
        assert nested_local.exists()
        assert nested_local.read_text() == "nested file content"
        
        # Test overwrite protection
        with pytest.raises(FileExistsError):
            adapter.get_file("small.txt", str(local_path), overwrite=False)
        
        # Test overwrite allowed
        result = adapter.get_file("large.txt", str(local_path), overwrite=True)
        assert local_path.read_text() == "large file content " * 1000

    def test_fsspec_adapter_with_progress_tracking(self):
        """Test FSSpecAdapter with progress tracking."""
        entity = LocationEntity(
            name="progress_test",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.source_dir)
            }
        )
        
        adapter = FSSpecAdapter(entity)
        
        # Create progress tracker
        tracker = ProgressTracker("test_download")
        
        # Download with progress tracking
        local_path = self.dest_dir / "progress_test.txt"
        result = adapter.get_file("large.txt", str(local_path), progress_tracker=tracker)
        
        assert result == str(local_path)
        assert local_path.exists()
        
        # Verify progress was tracked
        progress_info = tracker.get_progress_info()
        assert progress_info["operation"] == "test_download"
        assert progress_info["files_completed"] == 1
        assert progress_info["bytes_transferred"] > 0

    def test_fsspec_adapter_find_files(self):
        """Test FSSpecAdapter file finding with sandboxing."""
        entity = LocationEntity(
            name="find_test",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.source_dir)
            }
        )
        
        adapter = FSSpecAdapter(entity)
        
        # Test non-recursive find
        txt_files = list(adapter.find_files("*.txt"))
        assert len(txt_files) >= 2  # small.txt, large.txt
        
        file_names = [Path(f[0]).name for f in txt_files]
        assert "small.txt" in file_names
        assert "large.txt" in file_names
        
        # Test recursive find
        all_txt_files = list(adapter.find_files("*.txt", recursive=True))
        assert len(all_txt_files) >= 3  # includes subdir/nested.txt
        
        all_paths = [f[0] for f in all_txt_files]
        assert any("nested.txt" in path for path in all_paths)
        
        # Test pattern matching
        nc_files = list(adapter.find_files("*.nc"))
        assert len(nc_files) == 1
        assert Path(nc_files[0][0]).name == "data.nc"

    def test_fsspec_adapter_bulk_download(self):
        """Test FSSpecAdapter bulk file download."""
        entity = LocationEntity(
            name="bulk_test",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file", 
                "path": str(self.source_dir)
            }
        )
        
        adapter = FSSpecAdapter(entity)
        
        # Test bulk download of txt files
        downloaded = adapter.get_files("*.txt", str(self.dest_dir))
        
        assert len(downloaded) >= 2
        
        # Verify files were downloaded correctly
        for download_path in downloaded:
            download_file = Path(download_path)
            assert download_file.exists()
            
            # Verify content matches original
            if download_file.name == "small.txt":
                assert download_file.read_text() == "small file content"
            elif download_file.name == "large.txt":
                assert download_file.read_text() == "large file content " * 1000

    def test_fsspec_adapter_sandboxing_enforcement(self):
        """Test that FSSpecAdapter enforces sandboxing correctly."""
        entity = LocationEntity(
            name="sandbox_test",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.source_dir)
            }
        )
        
        adapter = FSSpecAdapter(entity)
        
        # Test that files outside sandbox are not accessible
        outside_path = str(self.outside_dir / "outside.txt")
        relative_outside = f"../../{self.outside_dir.name}/outside.txt"
        
        # These should not find files outside the sandbox
        assert not adapter.exists(relative_outside)
        
        # Test find_files doesn't escape sandbox
        all_files = list(adapter.find_files("*", recursive=True))
        outside_files = [f for f in all_files if "outside" in f[0]]
        assert len(outside_files) == 0

    @patch('fsspec.filesystem')
    def test_fsspec_adapter_with_remote_protocols(self, mock_fsspec):
        """Test FSSpecAdapter with remote protocols and sandboxing."""
        mock_fs = MagicMock()
        
        # Configure mock filesystem
        mock_fs.ls.return_value = ["file1.txt", "file2.txt"]
        mock_fs.size.return_value = 1024
        mock_fs.info.return_value = {"size": 1024, "type": "file"}
        mock_fs.exists.return_value = True
        mock_fs.isfile.return_value = True
        
        mock_fsspec.return_value = mock_fs
        
        # Create entity for remote protocol
        entity = LocationEntity(
            name="remote_test",
            kinds=[LocationKind.COMPUTE, LocationKind.FILESERVER],
            config={
                "protocol": "sftp",
                "path": "/remote/data/path",
                "storage_options": {
                    "host": "remote.example.com",
                    "username": "testuser",
                    "port": 22
                }
            }
        )
        
        adapter = FSSpecAdapter(entity)
        
        # Verify fsspec called with correct parameters
        expected_options = {
            "host": "remote.example.com",
            "username": "testuser",
            "port": 22
        }
        mock_fsspec.assert_called_with("sftp", **expected_options)
        
        # Test that adapter operations work through the mock
        assert adapter.exists("file1.txt")
        assert adapter.size("file1.txt") == 1024
        
        # Test connection test
        connection_result = adapter.test_connection()
        assert connection_result["protocol"] == "sftp"
        # Connection should succeed with mock
        assert connection_result["success"] is True


class TestLegacyLocationWithFSSpecPatterns:
    """Test legacy Location system working with FSSpec-like patterns."""
    
    def setup_method(self):
        """Set up test environment."""
        LegacyLocation._locations = {}
        self.temp_root = Path(tempfile.mkdtemp())
        self.locations_file = self.temp_root / "locations.json"
        LegacyLocation._locations_file = self.locations_file
        
        self.data_dir = self.temp_root / "legacy_data"
        self.data_dir.mkdir()
        
        # Create test files
        (self.data_dir / "model_output.nc").write_text("netcdf data")
        (self.data_dir / "log.txt").write_text("log content")
        subdir = self.data_dir / "analysis"
        subdir.mkdir()
        (subdir / "results.csv").write_text("csv,data,here")

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def test_legacy_location_fsspec_compatibility(self):
        """Test that legacy Location works with FSSpec-like operations."""
        location = LegacyLocation(
            name="legacy_fsspec_test",
            kinds=[LegacyLocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.data_dir),
                "fsspec_compatible": True
            }
        )
        
        fs = location.fs
        assert isinstance(fs, PathSandboxedFileSystem)
        
        # Test FSSpec-like operations
        assert fs.exists("model_output.nc")
        assert fs.isfile("model_output.nc")
        assert fs.isdir("analysis")
        
        # Test glob operations
        nc_files = fs.glob("*.nc")
        assert len(nc_files) == 1
        assert "model_output.nc" in nc_files[0]
        
        # Test walk operations  
        all_files = []
        for root, dirs, files in fs.walk(""):
            for file in files:
                all_files.append(f"{root}/{file}" if root else file)
        
        expected_files = ["model_output.nc", "log.txt", "analysis/results.csv"]
        for expected in expected_files:
            assert any(expected in found for found in all_files)

    def test_legacy_location_get_method_sandboxing(self):
        """Test that legacy Location.get method maintains sandboxing."""
        location = LegacyLocation(
            name="legacy_get_test",
            kinds=[LegacyLocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.data_dir)
            }
        )
        
        # Test normal get operation
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir) / "downloaded.nc"
            
            result = location.get("model_output.nc", str(local_path), show_progress=False)
            
            assert result == str(local_path)
            assert local_path.exists()
            assert local_path.read_text() == "netcdf data"
            
            # Test get with nested file
            nested_local = Path(temp_dir) / "nested_results.csv"
            result = location.get("analysis/results.csv", str(nested_local), show_progress=False)
            
            assert nested_local.exists()
            assert nested_local.read_text() == "csv,data,here"

    def test_legacy_location_mget_with_sandboxing(self):
        """Test that legacy Location.mget method maintains sandboxing."""
        location = LegacyLocation(
            name="legacy_mget_test",
            kinds=[LegacyLocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.data_dir)
            }
        )
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Test multi-get with pattern
            downloaded = location.mget(
                "*.txt", 
                temp_dir, 
                show_progress=False
            )
            
            assert len(downloaded) == 1
            downloaded_file = Path(downloaded[0])
            assert downloaded_file.name == "log.txt"
            assert downloaded_file.read_text() == "log content"
            
            # Test recursive mget
            downloaded_recursive = location.mget(
                "*.csv",
                temp_dir,
                recursive=True,
                show_progress=False
            )
            
            assert len(downloaded_recursive) == 1
            csv_file = Path(downloaded_recursive[0])
            assert csv_file.name == "results.csv"
            assert csv_file.read_text() == "csv,data,here"

    def test_legacy_location_find_files_sandboxing(self):
        """Test that legacy Location.find_files maintains sandboxing."""
        location = LegacyLocation(
            name="legacy_find_test",
            kinds=[LegacyLocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.data_dir)
            }
        )
        
        # Test non-recursive find
        txt_files = list(location.find_files("*.txt"))
        assert len(txt_files) == 1
        assert "log.txt" in txt_files[0][0]
        
        # Test recursive find
        all_files = list(location.find_files("*", recursive=True))
        assert len(all_files) == 3  # model_output.nc, log.txt, analysis/results.csv
        
        file_paths = [f[0] for f in all_files]
        assert any("model_output.nc" in path for path in file_paths)
        assert any("log.txt" in path for path in file_paths)
        assert any("results.csv" in path for path in file_paths)
        
        # Test find with base_path
        analysis_files = list(location.find_files("*.csv", base_path="analysis"))
        assert len(analysis_files) == 1
        assert "results.csv" in analysis_files[0][0]


class TestIntegratedWorkflowScenarios:
    """Test integrated workflows combining multiple components."""
    
    def setup_method(self):
        """Set up comprehensive test environment."""
        # Clear legacy locations
        LegacyLocation._locations = {}
        
        self.temp_root = Path(tempfile.mkdtemp())
        self.locations_file = self.temp_root / "locations.json"
        LegacyLocation._locations_file = self.locations_file
        
        # Create realistic Earth science data structure
        self.data_root = self.temp_root / "earth_science_data"
        self.model_dirs = {
            "CESM2": self.data_root / "CESM2" / "historical" / "r1i1p1f1",
            "GFDL": self.data_root / "GFDL" / "ssp585" / "r2i1p1f1",
            "UKESM": self.data_root / "UKESM" / "piControl" / "r1i1p1f2"
        }
        
        # Create directory structure and files
        for model, model_dir in self.model_dirs.items():
            model_dir.mkdir(parents=True)
            
            # Create typical climate model output files
            (model_dir / "atmos_daily.nc").write_text(f"{model} atmospheric daily data")
            (model_dir / "ocean_monthly.nc").write_text(f"{model} ocean monthly data")
            (model_dir / "land_yearly.nc").write_text(f"{model} land yearly data")
            (model_dir / "metadata.json").write_text(f'{{"model": "{model}", "status": "complete"}}')

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def test_multi_location_earth_science_workflow(self):
        """Test a realistic Earth science workflow with multiple locations."""
        # Create locations for different data types
        locations = {
            "model_output": LegacyLocation(
                name="model_output",
                kinds=[LegacyLocationKind.DISK, LegacyLocationKind.COMPUTE],
                config={
                    "protocol": "file",
                    "path": str(self.data_root),
                    "description": "Climate model output data"
                }
            ),
            "analysis_workspace": LegacyLocation(
                name="analysis_workspace", 
                kinds=[LegacyLocationKind.DISK],
                config={
                    "protocol": "file",
                    "path": str(self.temp_root / "analysis"),
                    "description": "Data analysis workspace"
                }
            )
        }
        
        # Create analysis workspace
        analysis_dir = Path(locations["analysis_workspace"].config["path"])
        analysis_dir.mkdir()
        
        # Step 1: Find all atmospheric data across models
        model_output_fs = locations["model_output"].fs
        
        atmos_files = model_output_fs.glob("*/*/*/atmos_daily.nc")
        assert len(atmos_files) == 3  # One per model
        
        # Step 2: Copy atmospheric data to analysis workspace
        analysis_fs = locations["analysis_workspace"].fs
        
        for i, atmos_file in enumerate(atmos_files):
            # Extract model name from path
            model_name = Path(atmos_file).parts[-4]  # Extract model from path
            
            # Read data from model location
            content = model_output_fs.read_text(atmos_file)
            
            # Write to analysis location with model-specific naming
            analysis_filename = f"atmos_{model_name.lower()}.nc"
            analysis_fs.write_text(analysis_filename, content)
        
        # Step 3: Verify analysis workspace has all files
        analysis_files = analysis_fs.glob("atmos_*.nc")
        assert len(analysis_files) == 3
        
        expected_models = ["cesm2", "gfdl", "ukesm"]
        for model in expected_models:
            expected_file = f"atmos_{model}.nc"
            assert any(expected_file in f for f in analysis_files)
        
        # Step 4: Verify sandboxing prevents cross-contamination
        # Model output location shouldn't see analysis files
        model_analysis_files = model_output_fs.glob("atmos_cesm2.nc")  # Analysis file name
        assert len(model_analysis_files) == 0
        
        # Analysis location shouldn't see original model structure
        assert not analysis_fs.exists("CESM2/historical/r1i1p1f1/atmos_daily.nc")

    def test_workflow_with_fsspec_adapter_and_legacy_location(self):
        """Test workflow combining FSSpecAdapter and legacy Location."""
        # Create legacy location
        legacy_location = LegacyLocation(
            name="legacy_workflow",
            kinds=[LegacyLocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.data_root)
            }
        )
        
        # Create modern LocationEntity for adapter
        modern_entity = LocationEntity(
            name="modern_workflow",
            kinds=[LocationKind.DISK],
            config={
                "protocol": "file",
                "path": str(self.data_root)
            }
        )
        
        adapter = FSSpecAdapter(modern_entity)
        
        # Both should access the same files
        legacy_fs = legacy_location.fs
        
        # Test that both interfaces see the same files
        cesm_file = "CESM2/historical/r1i1p1f1/atmos_daily.nc"
        
        assert legacy_fs.exists(cesm_file)
        assert adapter.exists(cesm_file)
        
        # Test content consistency
        legacy_content = legacy_fs.read_text(cesm_file)
        
        # For adapter, we need to get file info
        adapter_info = adapter.info(cesm_file)
        assert "size" in adapter_info
        
        # Both should find the same files
        legacy_nc_files = legacy_fs.glob("*/*/*/*_daily.nc")
        adapter_nc_files = list(adapter.find_files("*_daily.nc", recursive=True))
        
        assert len(legacy_nc_files) == len(adapter_nc_files) == 3
        
        # Test download through adapter
        with tempfile.TemporaryDirectory() as temp_dir:
            download_path = Path(temp_dir) / "downloaded_atmos.nc"
            result = adapter.get_file(cesm_file, str(download_path))
            
            assert Path(result).exists()
            assert Path(result).read_text() == legacy_content

    def test_persistence_across_workflow_steps(self):
        """Test that Location persistence works correctly across workflow steps."""
        # Step 1: Create initial locations
        step1_locations = [
            ("input_data", str(self.data_root)),
            ("temp_processing", str(self.temp_root / "temp")),
            ("final_output", str(self.temp_root / "output"))
        ]
        
        for name, path in step1_locations:
            Path(path).mkdir(parents=True, exist_ok=True)
            LegacyLocation(
                name=name,
                kinds=[LegacyLocationKind.DISK],
                config={
                    "protocol": "file",
                    "path": path,
                    "workflow_step": 1
                }
            )
        
        # Verify persistence
        assert self.locations_file.exists()
        
        # Step 2: Simulate process restart - clear memory and reload
        LegacyLocation._locations = {}
        LegacyLocation.load_locations()
        
        # Verify all locations loaded
        for name, _ in step1_locations:
            location = LegacyLocation.get_location(name)
            assert location is not None
            assert location.config["workflow_step"] == 1
        
        # Step 3: Perform processing using reloaded locations
        input_loc = LegacyLocation.get_location("input_data")
        temp_loc = LegacyLocation.get_location("temp_processing")
        output_loc = LegacyLocation.get_location("final_output")
        
        input_fs = input_loc.fs
        temp_fs = temp_loc.fs
        output_fs = output_loc.fs
        
        # Process CESM2 data through the workflow
        cesm_atmos = "CESM2/historical/r1i1p1f1/atmos_daily.nc"
        cesm_content = input_fs.read_text(cesm_atmos)
        
        # Stage 1: Copy to temp for processing
        temp_fs.write_text("cesm_atmos_temp.nc", cesm_content + " [processed]")
        
        # Stage 2: Generate final output
        processed_content = temp_fs.read_text("cesm_atmos_temp.nc")
        output_fs.write_text("cesm_atmos_final.nc", processed_content + " [finalized]")
        
        # Verify workflow completed correctly
        final_content = output_fs.read_text("cesm_atmos_final.nc")
        expected = "CESM2 atmospheric daily data [processed] [finalized]"
        assert final_content == expected
        
        # Verify each location only contains its own files
        assert input_fs.exists(cesm_atmos)
        assert not input_fs.exists("cesm_atmos_temp.nc")
        assert not input_fs.exists("cesm_atmos_final.nc")
        
        assert temp_fs.exists("cesm_atmos_temp.nc")
        assert not temp_fs.exists(cesm_atmos)
        assert not temp_fs.exists("cesm_atmos_final.nc")
        
        assert output_fs.exists("cesm_atmos_final.nc")
        assert not output_fs.exists(cesm_atmos)
        assert not output_fs.exists("cesm_atmos_temp.nc")