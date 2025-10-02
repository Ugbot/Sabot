#!/usr/bin/env python3
"""
Tests for RAFT GPU acceleration integration in Sabot.
"""

import pytest
import numpy as np
import pandas as pd
from unittest.mock import Mock, patch

from sabot.app import App, RAFTStream


class TestRAFTStream:
    """Test RAFT stream functionality."""

    def test_raft_stream_creation(self):
        """Test creating a RAFT stream."""
        app = Mock()
        app.enable_gpu = True
        app._gpu_resources = Mock()  # Mock GPU resources

        raft_stream = RAFTStream("test_stream", app)

        assert raft_stream.name == "test_stream"
        assert raft_stream.app == app
        assert raft_stream._gpu_enabled is True

    def test_raft_stream_cpu_fallback(self):
        """Test RAFT stream falls back to CPU when GPU unavailable."""
        app = Mock()
        app.enable_gpu = False
        app._gpu_resources = None

        raft_stream = RAFTStream("test_stream", app)

        assert raft_stream._gpu_enabled is False

    @patch('sabot.app.RAFT_AVAILABLE', True)
    @patch('sabot.app.cudf')
    @patch('sabot.app.RAFTKMeans')
    def test_kmeans_processor_creation(self, mock_kmeans, mock_cudf):
        """Test creating a K-means clustering processor."""
        app = Mock()
        app.enable_gpu = True
        app._gpu_resources = Mock()

        raft_stream = RAFTStream("test_stream", app)
        processor = raft_stream.kmeans_cluster(n_clusters=3, max_iter=100)

        assert callable(processor)

    @patch('sabot.app.RAFT_AVAILABLE', False)
    def test_kmeans_requires_gpu(self):
        """Test that K-means requires GPU acceleration."""
        app = Mock()
        app.enable_gpu = False
        app._gpu_resources = None

        raft_stream = RAFTStream("test_stream", app)

        with pytest.raises(RuntimeError, match="GPU acceleration required"):
            raft_stream.kmeans_cluster(n_clusters=3)

    def test_gpu_transform_processor(self):
        """Test creating a custom GPU transform processor."""
        app = Mock()
        app.enable_gpu = True
        app._gpu_resources = Mock()

        raft_stream = RAFTStream("test_stream", app)

        def test_transform(df):
            return df * 2

        processor = raft_stream.gpu_transform(test_transform)
        assert callable(processor)


class TestAppRAFTIntegration:
    """Test RAFT integration in the main App class."""

    def test_app_raft_stream_creation(self):
        """Test creating RAFT streams through the app."""
        app = App("test_app", enable_gpu=True)

        raft_stream = app.raft_stream("test_raft")

        assert isinstance(raft_stream, RAFTStream)
        assert raft_stream.name == "test_raft"
        assert raft_stream.app == app

    @patch('sabot.app.RAFT_AVAILABLE', True)
    @patch('sabot.app.DeviceResources')
    def test_gpu_initialization_success(self, mock_device_resources):
        """Test successful GPU initialization."""
        mock_device_resources.return_value = Mock()

        app = App("test_app", enable_gpu=True, gpu_device=0)

        assert app.enable_gpu is True
        assert app.gpu_device == 0
        assert app._gpu_resources is not None
        mock_device_resources.assert_called_once_with(device_id=0)

    @patch('sabot.app.RAFT_AVAILABLE', False)
    def test_gpu_initialization_no_raft(self):
        """Test GPU initialization when RAFT is not available."""
        app = App("test_app", enable_gpu=True)

        assert app.enable_gpu is True
        assert app._gpu_resources is None

    def test_cpu_only_mode(self):
        """Test app works in CPU-only mode."""
        app = App("test_app", enable_gpu=False)

        assert app.enable_gpu is False
        assert app._gpu_resources is None

        # Should still be able to create RAFT streams (CPU mode)
        raft_stream = app.raft_stream("cpu_stream")
        assert isinstance(raft_stream, RAFTStream)
        assert raft_stream._gpu_enabled is False


if __name__ == "__main__":
    pytest.main([__file__])
