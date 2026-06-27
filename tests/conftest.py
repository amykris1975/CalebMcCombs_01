"""
conftest.py
Project Harmony — Shared Pytest Fixtures and Configuration
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Add src/ to Python path for all tests
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Set test environment variables before any imports
os.environ.setdefault("GOOGLE_CLOUD_PROJECT", "test-project")
os.environ.setdefault("BIGQUERY_DATASET", "test_dataset")
os.environ.setdefault("HARMONY_DRONE_BUCKET", "test-drone-bucket")
