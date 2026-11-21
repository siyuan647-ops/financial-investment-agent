import json
import os
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def isolate_environment(monkeypatch):
    for key in list(os.environ):
        if key.startswith("APP_"):
            monkeypatch.delenv(key)


@pytest.fixture
def config():
    return json.loads((Path(__file__).parent / "fixtures" / "settings.json").read_text())
