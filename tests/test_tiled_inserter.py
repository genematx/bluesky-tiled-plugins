"""Tests for `TiledInserter` -- a callback that mimics Databroker's
`insert` interface, posting plain Bluesky documents to a Mongo-backed
Tiled server and (optionally) buffering them to a backup sink if the
primary insertion fails.
"""

import json

import bluesky.plans as bp
import ophyd.sim
import pytest  # noqa: F401  (pytest fixtures are picked up via conftest.py)

from bluesky_tiled_plugins.writing.tiled_writer import TiledInserter

from examples.render import render_templated_documents


class _FailingClient:
    """A stand-in for `CatalogOfBlueskyRuns` whose `post_document`
    always raises, used to exercise the `_ConditionalBackup` fallback."""

    def post_document(self, name, doc):
        raise RuntimeError("simulated outage")


def _assert_complete_run(store_entries):
    """Assert that `store_entries` -- a list of `{"name", "doc"}` records
    -- contains at least one start and one stop document with consistent
    uids (each `stop` document points back to its `start` via
    `stop["doc"]["run_start"]`)."""
    starts = [e for e in store_entries if e["name"] == "start"]
    stops = [e for e in store_entries if e["name"] == "stop"]
    assert starts, "no start document was backed up"
    assert stops, "no stop document was backed up"
    start_uids = {s["doc"]["uid"] for s in starts}
    stop_refs = {s["doc"]["run_start"] for s in stops}
    assert stop_refs <= start_uids, (
        f"stop documents reference run_start uids {stop_refs} that are not "
        f"among the backed-up start uids {start_uids}"
    )


def _read_jsonl_backup(tmp_path):
    files = list(tmp_path.glob("*.jsonl"))
    assert len(files) == 1, f"expected exactly one backup file, got {files}"
    with open(files[0]) as fh:
        return [json.loads(line) for line in fh if line.strip()]


def test_insert_round_trip(RE, mongo_catalog_client):
    """Documents inserted via TiledInserter must be retrievable from the
    Tiled server that backs the `CatalogOfBlueskyRuns` client."""
    inserter = TiledInserter(name="test", client=mongo_catalog_client)
    RE(bp.count([ophyd.sim.det], 3), inserter)

    runs = list(mongo_catalog_client.values())
    assert len(runs) == 1
    assert runs[0].stop is not None
    assert runs[0].stop["run_start"] == runs[0].start["uid"]


def test_insert_method_alias(RE, mongo_catalog_client):
    """`TiledInserter.insert(name, doc)` should behave the same as
    `TiledInserter.__call__`."""
    inserter = TiledInserter(name="test", client=mongo_catalog_client)
    RE(bp.count([ophyd.sim.det], 3), inserter.insert)

    runs = list(mongo_catalog_client.values())
    assert len(runs) == 1


@pytest.mark.parametrize(
    "fname, skip_keys",
    [
        # `internal_events`: the `empty` data key is declared with shape
        # `[]` but rows have actual shape `(0,)`, which MongoAdapter
        # can't reshape back to a scalar.
        ("internal_events", {"empty"}),
        # `external_assets_legacy`: external data keys reference the
        # `AD_HDF5_SWMR_STREAM` resource spec, for which MongoAdapter's
        # Filler has no handler registered in this test environment.
        ("external_assets_legacy", {"det-key1", "det-key2"}),
    ],
)
def test_insert_rendered_documents(
    mongo_catalog_client, external_assets_folder, fname, skip_keys
):
    """Drive `TiledInserter` with rendered example documents (rather
    than live RE-generated docs) and read every supported data key back
    from the Mongo-backed catalog. `skip_keys` lists data keys that
    `TiledInserter` inserts successfully but that MongoAdapter cannot
    serve on the read side for reasons documented in the parametrize
    block above."""
    inserter = TiledInserter(name="test", client=mongo_catalog_client)
    uid = None
    for item in render_templated_documents(fname + ".json", external_assets_folder):
        if item["name"] == "start":
            uid = item["doc"]["uid"]
        inserter(item["name"], item["doc"])

    assert uid is not None
    run = mongo_catalog_client[uid]
    assert run.start["uid"] == uid
    assert run.stop is not None
    assert run.stop["run_start"] == uid

    # Read back every supported data key from every stream 
    assert len(run.items()) > 0
    for stream in run.values():
        data = stream["data"]
        read_keys = [k for k in data if k not in skip_keys]
        assert len(read_keys) > 0
        for key in read_keys:
            arr = data[key].read()
            assert arr is not None and arr.size > 0


def test_no_backup_when_primary_succeeds(RE, mongo_catalog_client, tmp_path):
    inserter = TiledInserter(
        name="test",
        client=mongo_catalog_client,
        backup_directory=str(tmp_path),
    )
    RE(bp.count([ophyd.sim.det], 3), inserter)

    assert list(tmp_path.glob("*.jsonl")) == []


def test_backup_directory_used_on_failure(RE, tmp_path):
    inserter = TiledInserter(
        name="test",
        client=_FailingClient(),
        backup_directory=str(tmp_path),
    )
    RE(bp.count([ophyd.sim.det], 3), inserter)

    _assert_complete_run(_read_jsonl_backup(tmp_path))


def test_backup_dictionary_used_on_failure(RE, redis_json_dict_store):
    """When the primary client fails and `backup_dictionary` is set,
    documents are flushed to a `JSONDictWriter` writing into that dict.
    Parametrized over `fakeredis` and (if `TEST_REDIS_URI` is set) a
    real Redis backend via the `redis_json_dict_store` fixture."""
    inserter = TiledInserter(
        name="test",
        client=_FailingClient(),
        backup_dictionary=redis_json_dict_store,
    )
    RE(bp.count([ophyd.sim.det], 3), inserter)

    assert len(redis_json_dict_store) == 1
    (entries,) = redis_json_dict_store.values()
    _assert_complete_run(list(entries))


def test_backup_to_both_sinks_on_failure(RE, tmp_path, redis_json_dict_store):
    """Setting both `backup_directory` and `backup_dictionary` should
    flush every document to *both* sinks when the primary client fails."""
    inserter = TiledInserter(
        name="test",
        client=_FailingClient(),
        backup_directory=str(tmp_path),
        backup_dictionary=redis_json_dict_store,
    )
    RE(bp.count([ophyd.sim.det], 3), inserter)

    _assert_complete_run(_read_jsonl_backup(tmp_path))

    assert len(redis_json_dict_store) == 1
    (entries,) = redis_json_dict_store.values()
    _assert_complete_run(list(entries))

