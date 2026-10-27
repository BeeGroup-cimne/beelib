import os
from pathlib import Path

# Fitxer que toca el worker perque la livenessProbe de k8s comprovi que el bucle principal avanca.
HEARTBEAT_FILE = Path(os.getenv("HEARTBEAT_FILE", "/tmp/heartbeat"))


def beat():
    """Actualitza el mtime del fitxer de heartbeat. Mai propaga errors: no ha de tombar el worker."""
    try:
        HEARTBEAT_FILE.touch()
    except OSError:
        pass
