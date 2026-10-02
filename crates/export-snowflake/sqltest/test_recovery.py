"""Regression checks for recovery identity and declared data loss."""

from test_sql import load, warehouse


def landing(kind, txid, body, script="app", incarnation=1):
    return dict(
        kind=kind,
        script=script,
        **{"class": "Room"},
        cell="Room:a",
        facet=None,
        incarnation=incarnation,
        epoch=1,
        txid=txid,
        commit=1,
        committed_at=1790000000000,
        node="n",
        origin="live",
        fragment=1,
        fragments=1,
        body=body,
        source="test",
    )


def setup(w, records):
    load(w, dict(tombstones=[], landing_rows=records, dynamic_tables=[]))


def test_scriptless_recovery_targets_existing_stream(warehouse):
    setup(warehouse, [
        landing("link", 0, dict(start_txid=0, prev_epoch=None, prev_txid=None, mode="fresh")),
        landing("recovered", 5, dict(
            session="dead/1", head=dict(epoch=1, txid=5, commit=0), loss=False, cells=1,
        ), script="", incarnation=0),
    ])
    assert not warehouse.rows("SELECT * FROM CELL_STREAMS WHERE script = ''")
    got = warehouse.rows("SELECT script, incarnation, gap_kind FROM EXPORT_GAPS")
    assert any(r["script"] == "app" and r["incarnation"] == 1 for r in got), got


def test_recovered_loss_below_certification_is_detected(warehouse):
    setup(warehouse, [
        landing("watermark", 10, dict(
            through=dict(epoch=1, txid=10, commit=1), commits=0, records=0,
        )),
        landing("recovered", 5, dict(
            session="dead/1", head=dict(epoch=1, txid=5, commit=0), loss=True, cells=1,
        )),
    ])
    assert len(warehouse.rows("SELECT * FROM CELL_CERTIFIED")) == 1
    got = warehouse.rows("SELECT gap_kind, bound_txid FROM EXPORT_GAPS")
    assert len(got) == 1, got
    assert got[0]["gap_kind"] == "recovered"
    assert got[0]["bound_txid"] == 10


def test_partly_adopted_recovery_placeholder(warehouse):
    """A scriptless stream keeps the `recovered` records no named stream
    adopts, and only those: an adopted record appears once, under its
    adopter."""
    setup(warehouse, [
        landing("link", 0, dict(start_txid=0, prev_epoch=None, prev_txid=None, mode="fresh"),
                incarnation=3),
        # Head epoch 5 reaches incarnation 3: adopted by app/3.
        landing("recovered", 5, dict(
            session="dead/1", head=dict(epoch=5, txid=5, commit=0), loss=False, cells=1,
        ), script="", incarnation=0),
        # Head epoch 1 is below every named incarnation: nobody adopts it.
        landing("recovered", 6, dict(
            session="dead/2", head=dict(epoch=1, txid=6, commit=0), loss=False, cells=1,
        ), script="", incarnation=0),
    ])
    streams = warehouse.rows("SELECT script, incarnation FROM CELL_STREAMS ORDER BY script")
    assert [(r["script"], r["incarnation"]) for r in streams] == [("", 0), ("app", 3)], streams
    got = warehouse.rows(
        "SELECT script, incarnation, txid FROM CELL_META_CURRENT WHERE kind = 'recovered'"
    )
    assert sorted((r["script"], r["incarnation"], r["txid"]) for r in got) == [
        ("", 0, 6),
        ("app", 3, 5),
    ], got
