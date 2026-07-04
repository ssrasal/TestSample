# This is a reference file - actual implementation is at files/engine_worker.py
# Import and re-export for organizational purposes
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from engine_worker import EngineWorker
except ImportError:
    class EngineWorker:
        def __init__(self, *args, **kwargs):
            pass
        async def start(self):
            pass
