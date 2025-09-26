import importlib


def test_import_batch_trips():
    mod = importlib.import_module("dags.batch_trips_dag")
    assert hasattr(mod, "dag")
