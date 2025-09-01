"""
Tests for ScoutFS filesystem implementation.

Tests the infrastructure adapter for ScoutFS with tape staging support,
including API integration, file staging, progress tracking, and error handling.
"""

import datetime
import time
from typing import Any, Dict
from unittest.mock import MagicMock, Mock, call, patch

import pytest
import requests
from fsspec.callbacks import Callback
from rich.text import Text

from tellus.infrastructure.adapters.scoutfs_filesystem import ScoutFSFileSystem


@pytest.fixture
def mock_requests():
    """Mock requests module for API calls."""
    with patch('tellus.infrastructure.adapters.scoutfs_filesystem.requests') as mock:
        yield mock


@pytest.fixture  
def mock_logger():
    """Mock logger for testing log messages."""
    with patch('tellus.infrastructure.adapters.scoutfs_filesystem.logger') as mock:
        yield mock


@pytest.fixture
def scoutfs_config():
    """Sample ScoutFS configuration."""
    return {
        "api_url": "https://test-hsm.example.com:8080/v1",
        "token": "test_token_123"
    }


@pytest.fixture
def sample_filesystem_response():
    """Sample response from ScoutFS /filesystems endpoint."""
    return {
        "fsids": [
            {
                "fsid": "12345",
                "mount": "/test/mount/path",
                "name": "test_filesystem"
            },
            {
                "fsid": "67890", 
                "mount": "/another/mount",
                "name": "another_filesystem"
            }
        ]
    }


@pytest.fixture
def sample_file_info():
    """Sample file info response from ScoutFS API."""
    return {
        "path": "/test/mount/path/file.nc",
        "size": 1048576,
        "onlineblocks": "100",
        "offlineblocks": "0",
        "type": "file"
    }


@pytest.fixture
def sample_staging_response():
    """Sample response from ScoutFS staging request."""
    return {
        "status": "success",
        "message": "Staging request submitted",
        "request_id": "stage_123"
    }


class TestScoutFSFileSystemInitialization:
    """Test ScoutFS filesystem initialization."""
    
    def test_init_with_minimal_config(self):
        """Test initialization with minimal configuration."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__') as mock_super_init:
            mock_super_init.return_value = None
            
            fs = ScoutFSFileSystem("test-host")
        
        assert fs._scoutfs_config == {}
        mock_super_init.assert_called_once_with("test-host")
    
    def test_init_with_scoutfs_config(self, scoutfs_config):
        """Test initialization with ScoutFS configuration."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__') as mock_super_init:
            mock_super_init.return_value = None
            
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        assert fs._scoutfs_config == scoutfs_config
        mock_super_init.assert_called_once_with("test-host")
    
    def test_init_with_ssh_kwargs(self):
        """Test initialization passes SSH kwargs to parent."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__') as mock_super_init:
            mock_super_init.return_value = None
            
            fs = ScoutFSFileSystem(
                "test-host",
                username="testuser",
                port=22,
                key_filename="/path/to/key"
            )
        
        mock_super_init.assert_called_once_with(
            "test-host",
            username="testuser", 
            port=22,
            key_filename="/path/to/key"
        )
    
    def test_protocol_attribute(self):
        """Test protocol attribute contains expected values."""
        assert ScoutFSFileSystem.protocol == ("scoutfs", "sftp", "ssh")


class TestScoutFSAPIIntegration:
    """Test ScoutFS API integration methods."""
    
    def test_scoutfs_api_url_default(self):
        """Test default API URL."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host")
        
        assert fs._scoutfs_api_url == "https://hsm.dmawi.de:8080/v1"
    
    def test_scoutfs_api_url_custom(self, scoutfs_config):
        """Test custom API URL from config."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        assert fs._scoutfs_api_url == "https://test-hsm.example.com:8080/v1"
    
    def test_scoutfs_generate_token_success(self, mock_requests):
        """Test successful token generation."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host")
        
        # Mock successful login response
        mock_response = Mock()
        mock_response.json.return_value = {"response": "new_token_456"}
        mock_response.raise_for_status.return_value = None
        mock_requests.post.return_value = mock_response
        
        token = fs._scoutfs_generate_token()
        
        assert token == "new_token_456"
        mock_requests.post.assert_called_once_with(
            "https://hsm.dmawi.de:8080/v1/security/login",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json"
            },
            json={
                "acct": "filestat",
                "pass": "filestat"
            },
            verify=False
        )
    
    def test_scoutfs_generate_token_failure(self, mock_requests):
        """Test token generation failure."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host")
        
        # Mock failed login response
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("Login failed")
        mock_requests.post.return_value = mock_response
        
        with pytest.raises(requests.HTTPError):
            fs._scoutfs_generate_token()
    
    def test_scoutfs_token_property_uses_existing(self, scoutfs_config):
        """Test token property uses existing token from config."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        token = fs._scoutfs_token
        
        assert token == "test_token_123"
    
    def test_scoutfs_token_property_generates_new(self):
        """Test token property generates new token when not in config."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host")
        
        with patch.object(fs, '_scoutfs_generate_token') as mock_generate:
            mock_generate.return_value = "generated_token_789"
            
            token = fs._scoutfs_token
        
        assert token == "generated_token_789"
        assert fs._scoutfs_config["token"] == "generated_token_789"
    
    def test_scoutfs_get_filesystems_success(self, mock_requests, scoutfs_config, sample_filesystem_response):
        """Test successful filesystem information retrieval."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        # Mock successful response
        mock_response = Mock()
        mock_response.json.return_value = sample_filesystem_response
        mock_response.raise_for_status.return_value = None
        mock_requests.get.return_value = mock_response
        
        result = fs._scoutfs_get_filesystems()
        
        assert result == sample_filesystem_response
        mock_requests.get.assert_called_once_with(
            "https://test-hsm.example.com:8080/v1/filesystems",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json", 
                "Authorization": "Bearer test_token_123"
            },
            verify=False
        )
    
    def test_get_fsid_for_path_success(self, scoutfs_config, sample_filesystem_response):
        """Test successful filesystem ID retrieval for path."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        with patch.object(fs, '_scoutfs_get_filesystems') as mock_get_fs:
            mock_get_fs.return_value = sample_filesystem_response
            
            fsid = fs._get_fsid_for_path("/test/mount/path/file.nc")
        
        assert fsid == "12345"
    
    def test_get_fsid_for_path_no_match(self, scoutfs_config, sample_filesystem_response):
        """Test filesystem ID retrieval when no path matches."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        with patch.object(fs, '_scoutfs_get_filesystems') as mock_get_fs:
            mock_get_fs.return_value = sample_filesystem_response
            
            with pytest.raises(AssertionError) as exc_info:
                fs._get_fsid_for_path("/nonexistent/path/file.nc")
        
        assert "Expected exactly one matching filesystem" in str(exc_info.value)
    
    def test_get_fsid_for_path_multiple_matches(self, scoutfs_config):
        """Test filesystem ID retrieval when multiple paths match."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        # Create response with overlapping mounts
        overlapping_response = {
            "fsids": [
                {"fsid": "123", "mount": "/test", "name": "test1"},
                {"fsid": "456", "mount": "/test/mount", "name": "test2"}
            ]
        }
        
        with patch.object(fs, '_scoutfs_get_filesystems') as mock_get_fs:
            mock_get_fs.return_value = overlapping_response
            
            # Should match the most specific mount
            fsid = fs._get_fsid_for_path("/test/mount/file.nc")
            
            assert fsid == "456"
    
    def test_scoutfs_file_success(self, mock_requests, scoutfs_config, sample_file_info):
        """Test successful file information retrieval."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        with patch.object(fs, '_get_fsid_for_path') as mock_get_fsid:
            mock_get_fsid.return_value = "12345"
            
            mock_response = Mock()
            mock_response.json.return_value = sample_file_info
            mock_response.raise_for_status.return_value = None
            mock_requests.get.return_value = mock_response
            
            result = fs._scoutfs_file("/test/mount/path/file.nc")
        
        assert result == sample_file_info
        mock_requests.get.assert_called_once_with(
            "https://test-hsm.example.com:8080/v1/file?fsid=12345&path=/test/mount/path/file.nc",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": "Bearer test_token_123"
            },
            verify=False
        )
    
    def test_scoutfs_request_success(self, mock_requests, scoutfs_config, sample_staging_response):
        """Test successful ScoutFS API request."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        with patch.object(fs, '_get_fsid_for_path') as mock_get_fsid:
            mock_get_fsid.return_value = "12345"
            
            mock_response = Mock()
            mock_response.json.return_value = sample_staging_response
            mock_response.raise_for_status.return_value = None
            mock_requests.post.return_value = mock_response
            
            result = fs._scoutfs_request("stage", "/test/mount/path/file.nc")
        
        assert result == sample_staging_response
        mock_requests.post.assert_called_once_with(
            "https://test-hsm.example.com:8080/v1/request/stage?fsid=12345&path=/test/mount/path/file.nc",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": "Bearer test_token_123"
            },
            json={"path": "/test/mount/path/file.nc"},
            verify=False
        )
    
    def test_scoutfs_queues_success(self, mock_requests, scoutfs_config):
        """Test successful queue information retrieval."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        queue_info = {"queues": [{"name": "staging", "size": 10}]}
        mock_response = Mock()
        mock_response.json.return_value = queue_info
        mock_response.raise_for_status.return_value = None
        mock_requests.get.return_value = mock_response
        
        result = fs._scoutfs_queues()
        
        assert result == queue_info
        mock_requests.get.assert_called_once_with(
            "https://test-hsm.example.com:8080/v1/queues",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": "Bearer test_token_123"
            },
            verify=False
        )


class TestScoutFSFileOperations:
    """Test ScoutFS file operation methods."""
    
    def test_queues_property(self, scoutfs_config):
        """Test queues property delegates to _scoutfs_queues."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        with patch.object(fs, '_scoutfs_queues') as mock_queues:
            mock_queues.return_value = {"test": "data"}
            
            result = fs.queues
            
            assert result == {"test": "data"}
            mock_queues.assert_called_once()
    
    def test_stage_method(self, scoutfs_config, sample_staging_response):
        """Test stage method delegates to _scoutfs_request."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        with patch.object(fs, '_scoutfs_request') as mock_request:
            mock_request.return_value = sample_staging_response
            
            result = fs.stage("/test/mount/path/file.nc")
            
            assert result == sample_staging_response
            mock_request.assert_called_once_with("stage", "/test/mount/path/file.nc")
    
    def test_info_with_scoutfs_success(self, scoutfs_config, sample_file_info):
        """Test info method with successful ScoutFS integration."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        base_info = {"name": "file.nc", "size": 1048576, "type": "file"}
        
        with patch('fsspec.implementations.sftp.SFTPFileSystem.info') as mock_super_info:
            mock_super_info.return_value = base_info
            
            with patch.object(fs, '_scoutfs_file') as mock_scoutfs_file:
                mock_scoutfs_file.return_value = sample_file_info
                
                result = fs.info("/test/mount/path/file.nc")
        
        expected = base_info.copy()
        expected["scoutfs_info"] = {
            "/file": sample_file_info,
            "/batchfile": None
        }
        
        assert result == expected
    
    def test_info_with_scoutfs_error(self, mock_logger, scoutfs_config):
        """Test info method with ScoutFS error handling."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        base_info = {"name": "file.nc", "size": 1048576, "type": "file"}
        
        with patch('fsspec.implementations.sftp.SFTPFileSystem.info') as mock_super_info:
            mock_super_info.return_value = base_info
            
            with patch.object(fs, '_scoutfs_file') as mock_scoutfs_file:
                mock_scoutfs_file.side_effect = Exception("API error")
                
                result = fs.info("/test/mount/path/file.nc")
        
        expected = base_info.copy()
        expected["scoutfs_info"] = {
            "/file": {"error": "API error"},
            "/batchfile": None
        }
        
        assert result == expected
        mock_logger.warning.assert_called_once()
    
    def test_is_online_file_online(self, scoutfs_config):
        """Test is_online method for online file."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        info_response = {
            "scoutfs_info": {
                "/file": {
                    "onlineblocks": "100",
                    "offlineblocks": "0"
                }
            }
        }
        
        with patch.object(fs, 'info') as mock_info:
            mock_info.return_value = info_response
            
            result = fs.is_online("/test/file.nc")
        
        assert result is True
    
    def test_is_online_file_offline(self, scoutfs_config):
        """Test is_online method for offline file."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        info_response = {
            "scoutfs_info": {
                "/file": {
                    "onlineblocks": "0",
                    "offlineblocks": "100"
                }
            }
        }
        
        with patch.object(fs, 'info') as mock_info:
            mock_info.return_value = info_response
            
            result = fs.is_online("/test/file.nc")
        
        assert result is False
    
    def test_is_online_partial_file(self, scoutfs_config):
        """Test is_online method for partially online file."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        info_response = {
            "scoutfs_info": {
                "/file": {
                    "onlineblocks": "50",
                    "offlineblocks": "50"
                }
            }
        }
        
        with patch.object(fs, 'info') as mock_info:
            mock_info.return_value = info_response
            
            result = fs.is_online("/test/file.nc")
        
        # Should return False for partially online files
        assert result is False
    
    def test_is_online_empty_blocks(self, scoutfs_config):
        """Test is_online method with empty block values."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        info_response = {
            "scoutfs_info": {
                "/file": {
                    "onlineblocks": "",
                    "offlineblocks": ""
                }
            }
        }
        
        with patch.object(fs, 'info') as mock_info:
            mock_info.return_value = info_response
            
            result = fs.is_online("/test/file.nc")
        
        # Empty strings should be treated as 0
        assert result is False
    
    def test_is_online_error_handling(self, mock_logger, scoutfs_config):
        """Test is_online method error handling."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        with patch.object(fs, 'info') as mock_info:
            mock_info.side_effect = Exception("Info error")
            
            result = fs.is_online("/test/file.nc")
        
        # Should assume online when error occurs
        assert result is True
        mock_logger.warning.assert_called_once()
    
    def test_scoutfs_online_status_success(self, scoutfs_config):
        """Test _scoutfs_online_status method success."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        info_response = {
            "scoutfs_info": {
                "/file": {
                    "onlineblocks": "75",
                    "offlineblocks": "25"
                }
            }
        }
        
        with patch.object(fs, 'info') as mock_info:
            mock_info.return_value = info_response
            
            result = fs._scoutfs_online_status("/test/file.nc")
        
        assert isinstance(result, Text)
        # Check that the text contains the expected information
        result_str = str(result)
        assert "/test/file.nc" in result_str
        assert "online_blocks: 75" in result_str
        assert "offline_blocks: 25" in result_str
    
    def test_scoutfs_online_status_error(self, scoutfs_config):
        """Test _scoutfs_online_status method error handling."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        with patch.object(fs, 'info') as mock_info:
            mock_info.side_effect = Exception("Status error")
            
            result = fs._scoutfs_online_status("/test/file.nc")
        
        assert isinstance(result, str)
        assert "/test/file.nc" in result
        assert "Error: Status error" in result


class TestScoutFSFileOpening:
    """Test ScoutFS file opening with staging."""
    
    def test_open_write_mode_no_staging(self, scoutfs_config):
        """Test opening file in write mode skips staging."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        mock_file = Mock()
        with patch('fsspec.implementations.sftp.SFTPFileSystem.open') as mock_super_open:
            mock_super_open.return_value = mock_file
            
            result = fs.open("/test/file.nc", mode="w")
        
        assert result == mock_file
        mock_super_open.assert_called_once_with("/test/file.nc", mode="w", callback=None)
    
    def test_open_no_staging_disabled(self, scoutfs_config):
        """Test opening file with staging disabled."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        mock_file = Mock()
        with patch('fsspec.implementations.sftp.SFTPFileSystem.open') as mock_super_open:
            mock_super_open.return_value = mock_file
            
            result = fs.open("/test/file.nc", mode="r", stage_before_opening=False)
        
        assert result == mock_file
        mock_super_open.assert_called_once_with("/test/file.nc", mode="r", callback=None)
    
    def test_open_file_not_found_read_mode(self, scoutfs_config):
        """Test opening non-existent file in read mode."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        with patch.object(fs, 'info') as mock_info:
            mock_info.side_effect = FileNotFoundError("File not found")
            
            with pytest.raises(FileNotFoundError):
                fs.open("/test/nonexistent.nc", mode="r")
    
    def test_open_file_not_found_write_mode(self, scoutfs_config):
        """Test opening non-existent file in write mode."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        mock_file = Mock()
        with patch.object(fs, 'info') as mock_info:
            mock_info.side_effect = FileNotFoundError("File not found")
            
            with patch('fsspec.implementations.sftp.SFTPFileSystem.open') as mock_super_open:
                mock_super_open.return_value = mock_file
                
                result = fs.open("/test/newfile.nc", mode="w")
        
        assert result == mock_file
        mock_super_open.assert_called_once_with("/test/newfile.nc", mode="w", callback=None)
    
    def test_open_file_already_online(self, scoutfs_config):
        """Test opening file that's already online."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        mock_file = Mock()
        with patch.object(fs, 'info') as mock_info:
            mock_info.return_value = {"size": 1024}
            
            with patch.object(fs, 'is_online') as mock_is_online:
                mock_is_online.return_value = True
                
                with patch('fsspec.implementations.sftp.SFTPFileSystem.open') as mock_super_open:
                    mock_super_open.return_value = mock_file
                    
                    result = fs.open("/test/file.nc", mode="r")
        
        assert result == mock_file
        mock_super_open.assert_called_once_with("/test/file.nc", mode="r", callback=None)
    
    def test_open_file_needs_staging_success(self, scoutfs_config):
        """Test successful file opening with staging."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        mock_file = Mock()
        mock_callback = Mock()
        mock_callback.set_description = Mock()
        mock_callback.relative_update = Mock()
        
        with patch.object(fs, 'info') as mock_info:
            mock_info.return_value = {"size": 1024}
            
            with patch.object(fs, 'is_online') as mock_is_online:
                # First call: file is offline, second call: file is online
                mock_is_online.side_effect = [False, True]
                
                with patch.object(fs, 'stage') as mock_stage:
                    mock_stage.return_value = {"status": "success"}
                    
                    with patch('fsspec.implementations.sftp.SFTPFileSystem.open') as mock_super_open:
                        mock_super_open.return_value = mock_file
                        
                        result = fs.open("/test/file.nc", mode="r", callback=mock_callback)
        
        assert result == mock_file
        mock_stage.assert_called_once_with("/test/file.nc")
        mock_callback.set_description.assert_called_once_with("Staging /test/file.nc")
        mock_super_open.assert_called_once_with("/test/file.nc", mode="r", callback=mock_callback)
    
    def test_open_file_staging_timeout(self, scoutfs_config):
        """Test file opening with staging timeout."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        with patch.object(fs, 'info') as mock_info:
            mock_info.return_value = {"size": 1024}
            
            with patch.object(fs, 'is_online') as mock_is_online:
                # File never becomes online
                mock_is_online.return_value = False
                
                with patch.object(fs, 'stage') as mock_stage:
                    mock_stage.return_value = {"status": "success"}
                    
                    with patch('time.sleep'):  # Speed up the test
                        with patch('datetime.datetime') as mock_datetime:
                            # Mock time progression to trigger timeout
                            base_time = datetime.datetime(2023, 1, 1, 12, 0, 0)
                            timeout_time = base_time + datetime.timedelta(seconds=10)
                            mock_datetime.now.side_effect = [base_time, timeout_time + datetime.timedelta(seconds=1)]
                            
                            with pytest.raises(TimeoutError) as exc_info:
                                fs.open("/test/file.nc", mode="r", timeout=10)
        
        assert "Timeout while waiting for file" in str(exc_info.value)
    
    def test_open_file_staging_with_progress_updates(self, scoutfs_config):
        """Test file opening with progress callback updates during waiting."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        mock_file = Mock()
        mock_callback = Mock()
        mock_callback.set_description = Mock()
        mock_callback.relative_update = Mock()
        
        with patch.object(fs, 'info') as mock_info:
            mock_info.return_value = {"size": 1024}
            
            with patch.object(fs, 'is_online') as mock_is_online:
                # File becomes online after 2 checks
                mock_is_online.side_effect = [False, False, True]
                
                with patch.object(fs, 'stage') as mock_stage:
                    mock_stage.return_value = {"status": "success"}
                    
                    with patch('time.sleep') as mock_sleep:  # Mock sleep to speed up test
                        with patch('fsspec.implementations.sftp.SFTPFileSystem.open') as mock_super_open:
                            mock_super_open.return_value = mock_file
                            
                            result = fs.open("/test/file.nc", mode="r", callback=mock_callback)
        
        assert result == mock_file
        # Should have called relative_update during waiting
        assert mock_callback.relative_update.call_count >= 1
        assert all(call[0][0] == 0 for call in mock_callback.relative_update.call_args_list)
    
    def test_open_file_custom_timeout(self, scoutfs_config):
        """Test file opening with custom timeout."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        mock_file = Mock()
        
        with patch.object(fs, 'info') as mock_info:
            mock_info.return_value = {"size": 1024}
            
            with patch.object(fs, 'is_online') as mock_is_online:
                mock_is_online.side_effect = [False, True]
                
                with patch.object(fs, 'stage') as mock_stage:
                    with patch('fsspec.implementations.sftp.SFTPFileSystem.open') as mock_super_open:
                        mock_super_open.return_value = mock_file
                        
                        with patch('datetime.datetime') as mock_datetime:
                            base_time = datetime.datetime(2023, 1, 1, 12, 0, 0)
                            mock_datetime.now.return_value = base_time
                            mock_datetime.timedelta.return_value = datetime.timedelta(seconds=300)  # Custom timeout
                            
                            fs.open("/test/file.nc", mode="r", timeout=300)
        
        # Should have used custom timeout
        mock_datetime.timedelta.assert_called_with(seconds=300)


class TestIntegrationScenarios:
    """Test integration scenarios and real-world usage patterns."""
    
    def test_complete_staging_workflow(self, mock_requests, scoutfs_config, sample_filesystem_response, 
                                      sample_file_info, sample_staging_response):
        """Test complete workflow from staging to file access."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        # Mock filesystem lookup
        fs_response = Mock()
        fs_response.json.return_value = sample_filesystem_response
        fs_response.raise_for_status.return_value = None
        
        # Mock file info request
        file_response = Mock()
        file_response.json.return_value = sample_file_info
        file_response.raise_for_status.return_value = None
        
        # Mock staging request
        stage_response = Mock()
        stage_response.json.return_value = sample_staging_response
        stage_response.raise_for_status.return_value = None
        
        mock_requests.get.side_effect = [fs_response, file_response]
        mock_requests.post.return_value = stage_response
        
        mock_file = Mock()
        
        with patch.object(fs, 'is_online') as mock_is_online:
            mock_is_online.side_effect = [False, True]  # Offline then online
            
            with patch('fsspec.implementations.sftp.SFTPFileSystem.open') as mock_super_open:
                mock_super_open.return_value = mock_file
                
                result = fs.open("/test/mount/path/file.nc", mode="r")
        
        assert result == mock_file
        
        # Verify API calls were made in correct order
        assert mock_requests.get.call_count == 2  # filesystem info + file info
        assert mock_requests.post.call_count == 1  # staging request
    
    def test_multiple_files_staging(self, scoutfs_config):
        """Test staging multiple files in sequence."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        files = ["/test/file1.nc", "/test/file2.nc", "/test/file3.nc"]
        staged_files = []
        
        def mock_stage(path):
            staged_files.append(path)
            return {"status": "success", "path": path}
        
        with patch.object(fs, '_scoutfs_request') as mock_request:
            mock_request.side_effect = mock_stage
            
            for file_path in files:
                fs.stage(file_path)
        
        assert staged_files == files
        assert mock_request.call_count == 3
    
    def test_error_recovery_during_staging(self, mock_logger, scoutfs_config):
        """Test error recovery during staging operations."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        with patch.object(fs, 'info') as mock_info:
            mock_info.return_value = {"size": 1024}
            
            with patch.object(fs, 'is_online') as mock_is_online:
                mock_is_online.return_value = False
                
                with patch.object(fs, 'stage') as mock_stage:
                    mock_stage.side_effect = requests.HTTPError("API error")
                    
                    with pytest.raises(requests.HTTPError):
                        fs.open("/test/file.nc", mode="r")
    
    def test_concurrent_staging_requests(self, scoutfs_config):
        """Test handling concurrent staging requests."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        request_count = 0
        
        def mock_request(command, path):
            nonlocal request_count
            request_count += 1
            return {"status": "success", "request_id": f"req_{request_count}"}
        
        with patch.object(fs, '_scoutfs_request') as mock_scoutfs_request:
            mock_scoutfs_request.side_effect = mock_request
            
            # Simulate concurrent requests
            results = []
            for i in range(5):
                result = fs.stage(f"/test/file{i}.nc")
                results.append(result)
        
        assert len(results) == 5
        assert all("request_id" in result for result in results)
        assert len(set(result["request_id"] for result in results)) == 5  # All unique


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_filesystem_info_with_empty_response(self, scoutfs_config):
        """Test filesystem info handling with empty response."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        with patch.object(fs, '_scoutfs_get_filesystems') as mock_get_fs:
            mock_get_fs.return_value = {"fsids": []}
            
            with pytest.raises(AssertionError):
                fs._get_fsid_for_path("/any/path")
    
    def test_online_status_with_missing_blocks_info(self, scoutfs_config):
        """Test online status with missing blocks information."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        info_response = {
            "scoutfs_info": {
                "/file": {}  # Missing onlineblocks and offlineblocks
            }
        }
        
        with patch.object(fs, 'info') as mock_info:
            mock_info.return_value = info_response
            
            result = fs.is_online("/test/file.nc")
        
        # Should return False when blocks info is missing
        assert result is False
    
    def test_staging_with_invalid_path(self, scoutfs_config):
        """Test staging with invalid path that doesn't match any filesystem."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        with patch.object(fs, '_get_fsid_for_path') as mock_get_fsid:
            mock_get_fsid.side_effect = AssertionError("No matching filesystem")
            
            with pytest.raises(AssertionError):
                fs.stage("/invalid/path/file.nc")
    
    def test_token_generation_with_malformed_response(self, mock_requests):
        """Test token generation with malformed API response."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host")
        
        mock_response = Mock()
        mock_response.json.return_value = {}  # Missing "response" key
        mock_response.raise_for_status.return_value = None
        mock_requests.post.return_value = mock_response
        
        token = fs._scoutfs_generate_token()
        
        assert token is None  # Should handle missing key gracefully
    
    def test_open_with_none_timeout(self, scoutfs_config):
        """Test opening file with None timeout uses default."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        mock_file = Mock()
        
        with patch.object(fs, 'info') as mock_info:
            mock_info.return_value = {"size": 1024}
            
            with patch.object(fs, 'is_online') as mock_is_online:
                mock_is_online.side_effect = [False, True]
                
                with patch.object(fs, 'stage'):
                    with patch('fsspec.implementations.sftp.SFTPFileSystem.open') as mock_super_open:
                        mock_super_open.return_value = mock_file
                        
                        with patch('datetime.datetime') as mock_datetime:
                            base_time = datetime.datetime(2023, 1, 1, 12, 0, 0)
                            mock_datetime.now.return_value = base_time
                            mock_datetime.timedelta.return_value = datetime.timedelta(seconds=180)
                            
                            fs.open("/test/file.nc", mode="r", timeout=None)
        
        # Should have used default timeout of 180 seconds
        mock_datetime.timedelta.assert_called_with(seconds=180)
    
    def test_info_with_kwargs_passthrough(self, scoutfs_config, sample_file_info):
        """Test that info method passes through kwargs to parent class."""
        with patch('fsspec.implementations.sftp.SFTPFileSystem.__init__'):
            fs = ScoutFSFileSystem("test-host", scoutfs_config=scoutfs_config)
        
        base_info = {"name": "file.nc", "size": 1048576}
        
        with patch('fsspec.implementations.sftp.SFTPFileSystem.info') as mock_super_info:
            mock_super_info.return_value = base_info
            
            with patch.object(fs, '_scoutfs_file') as mock_scoutfs_file:
                mock_scoutfs_file.return_value = sample_file_info
                
                fs.info("/test/file.nc", detail=True, refresh=True)
        
        mock_super_info.assert_called_once_with("/test/file.nc", detail=True, refresh=True)
    
    def test_registration_with_fsspec(self):
        """Test that ScoutFS filesystem is properly registered with fsspec."""
        # This test verifies the registration call at module level
        with patch('tellus.infrastructure.adapters.scoutfs_filesystem.register_implementation') as mock_register:
            # Re-import module to trigger registration
            import importlib

            import tellus.infrastructure.adapters.scoutfs_filesystem
            importlib.reload(tellus.infrastructure.adapters.scoutfs_filesystem)
            
            mock_register.assert_called_with("scoutfs", ScoutFSFileSystem, clobber=True)