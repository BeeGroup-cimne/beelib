"""Perquè els tests s'executin des d'un clon sense instal·lar el paquet (`src/` al path) i sense dependències pesades."""
import os
import sys
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
try:
    import happybase  # noqa: F401
except ImportError:                 # `beelib/__init__` l'importa; els tests no el fan servir
    sys.modules["happybase"] = MagicMock()
