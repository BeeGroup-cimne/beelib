import importlib
import pickle
import pkgutil

import beelib

MODULES = [m.name for m in pkgutil.iter_modules(beelib.__path__, "beelib.")]


def test_all_modules_import():
    assert "beelib.beeredis" in MODULES and "beelib.beerest" in MODULES
    # beelib's own DeprecationWarnings are errors via pyproject's filterwarnings,
    # which also covers the eager imports in beelib/__init__ at collection time
    for name in MODULES:
        importlib.import_module(name)


def test_cassandra_has_reactor():
    import cassandra.cluster
    assert cassandra.cluster.DefaultConnection is not None


def test_kafka_pickle_roundtrip_protocol():
    from base64 import b64decode
    from beelib.beekafka import BeeKafka
    value = {"a": [1, 2.5, "x"], "b": None}
    encoded = BeeKafka.__pickle_encoder__(value)
    raw = b64decode(encoded)
    assert raw[0] == pickle.PROTO[0] and raw[1] <= 5  # readable by 3.10
    assert BeeKafka.__pickle_decoder__(encoded) == value
