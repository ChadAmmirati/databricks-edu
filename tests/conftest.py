import os
import sys

# Make src/ and extensions/ importable for tests run from the repo root.
HERE = os.path.dirname(os.path.abspath(__file__))
for _rel in ("src", "extensions"):
    _p = os.path.abspath(os.path.join(HERE, "..", _rel))
    if _p not in sys.path:
        sys.path.insert(0, _p)
