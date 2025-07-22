import pytest

from tellus.location import (
    ALLOWED_LOCATIONS,
    LOCATION_REGISTRY,
    BaseLocationHandler,
    FileServerHandler,
    HPCHandler,
    HSMHandler,
    Location,
    create_location_handler,
)


def test_location_initialization():
    """Test Location dataclass initialization with valid data"""
    config = {"path": "/some/path", "permissions": "readonly"}
    location = Location(name="test_loc", kind="HSM", config=config)

    assert location.name == "test_loc"
    assert location.kind == "HSM"
    assert location.config == config


def test_location_invalid_kind():
    """Test that Location raises ValueError for invalid location kinds"""
    with pytest.raises(ValueError) as excinfo:
        Location(name="test_loc", kind="INVALID", config={})

    assert "is not allowed" in str(excinfo.value)
    for allowed in ALLOWED_LOCATIONS:
        assert allowed in str(excinfo.value)


def test_base_location_handler_abstract_methods():
    """Test that BaseLocationHandler raises NotImplementedError for abstract methods"""
    handler = BaseLocationHandler()

    with pytest.raises(NotImplementedError):
        handler.post("data")

    with pytest.raises(NotImplementedError):
        handler.get("id")

    with pytest.raises(NotImplementedError):
        handler.fetch("id")


def test_hsm_handler_methods(capsys):
    """Test HSMHandler methods"""
    handler = HSMHandler()

    handler.post("test_data")
    captured = capsys.readouterr()
    assert "Storing to HSM..." in captured.out

    handler.get("test_id")
    captured = capsys.readouterr()
    assert "Getting from HSM..." in captured.out

    handler.fetch("test_id")
    captured = capsys.readouterr()
    assert "Fetching from HSM..." in captured.out


def test_hpc_handler_methods(capsys):
    """Test HPCHandler methods"""
    handler = HPCHandler()

    handler.post("test_data")
    captured = capsys.readouterr()
    assert "Storing to HPC..." in captured.out

    handler.get("test_id")
    captured = capsys.readouterr()
    assert "Getting from HPC..." in captured.out

    handler.fetch("test_id")
    captured = capsys.readouterr()
    assert "Fetching from HPC..." in captured.out


def test_file_server_handler_methods(capsys):
    """Test FileServerHandler methods"""
    handler = FileServerHandler()

    handler.post("test_data")
    captured = capsys.readouterr()
    assert "Storing to FileServer..." in captured.out

    handler.get("test_id")
    captured = capsys.readouterr()
    assert "Getting from FileServer..." in captured.out

    handler.fetch("test_id")
    captured = capsys.readouterr()
    assert "Fetching from FileServer..." in captured.out


def test_location_registry():
    """Test that the location registry contains the expected handlers"""
    assert set(LOCATION_REGISTRY.keys()) == {"hsm", "hpc", "fileserver"}
    assert issubclass(LOCATION_REGISTRY["hsm"], BaseLocationHandler)
    assert issubclass(LOCATION_REGISTRY["hpc"], BaseLocationHandler)
    assert issubclass(LOCATION_REGISTRY["fileserver"], BaseLocationHandler)


def test_create_location_handler():
    """Test the create_location_handler factory function"""
    # Test with valid location kinds
    for kind in ALLOWED_LOCATIONS:
        location = Location(name=f"test_{kind}", kind=kind, config={})
        handler = create_location_handler(location)
        assert isinstance(handler, LOCATION_REGISTRY[kind])


def test_allowed_locations_constant():
    """Test that ALLOWED_LOCATIONS contains the expected values"""
    assert isinstance(ALLOWED_LOCATIONS, set)
    assert ALLOWED_LOCATIONS == {"hsm", "hpc", "fileserver"}
