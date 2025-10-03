import pytest
from sabot.cli import load_app_from_spec
from sabot.app import App


def test_load_app_from_spec_valid():
    """Test loading a valid app."""
    app = load_app_from_spec("examples.fraud_app:app")
    assert isinstance(app, App)
    assert app.id is not None


def test_load_app_from_spec_invalid_format():
    """Test invalid spec format raises ValueError."""
    with pytest.raises(ValueError, match="Invalid app specification"):
        load_app_from_spec("invalid_spec")


def test_load_app_from_spec_module_not_found():
    """Test nonexistent module raises ImportError."""
    with pytest.raises(ImportError, match="Cannot import module"):
        load_app_from_spec("nonexistent.module:app")


def test_load_app_from_spec_attribute_not_found():
    """Test nonexistent attribute raises AttributeError."""
    with pytest.raises(AttributeError, match="has no attribute"):
        load_app_from_spec("examples.fraud_app:nonexistent")


def test_load_app_from_spec_not_an_app():
    """Test non-App object raises TypeError."""
    with pytest.raises(TypeError, match="not a Sabot App instance"):
        load_app_from_spec("sys:path")  # sys.path is a list, not App

