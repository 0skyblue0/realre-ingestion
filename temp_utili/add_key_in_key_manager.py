import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from key_manager import KeyManager


manager = KeyManager("secrets/keys.json", passphrase="", auto_persist=True)

# passphrase -> 
manager.set("", "")
# manager.delete("")
