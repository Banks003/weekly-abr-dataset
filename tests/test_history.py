from __future__ import annotations

from datetime import date

import polars as pl

from abr_extract.history import (
    apply_update_dgr,
    apply_update_main,
    apply_update_trading,
    bootstrap_dgr,
    bootstrap_main,
    bootstrap_trading,
    history_summary,
)


def _main_row(**overrides) -> dict:
    base = {
        "abn": "11111111111",
        "abn_status": "ACT",
        "abn_status_from_date": date(2020, 1, 1),
        "record_last_updated": date(2024, 6, 1),
        "replaced": "N",
        "entity_type_ind": "PRV",
        "entity_type_text": "Australian Private Company",
        "entity_kind": "main",
        "main_name": "ACME PTY LTD",
        "main_name_type": "MN",
        "individual_title": None,
        "individual_given_names": None,
        "individual_family_name": None,
        "individual_name_type": None,
        "state": "NSW",
        "postcode": "2000",
        "asic_number": "123456789",
        "asic_number_type": "undetermined",
        "gst_status": "ACT",
        "gst_status_from_date": date(2020, 1, 1),
    }
    base.update(overrides)
    return base


def _df(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows)


# --- bootstrap ----------------------------------------------------------


def test_bootstrap_main_uses_record_last_updated_for_valid_from():
    snapshot = _df([_main_row(record_last_updated=date(2018, 2, 16))])
    history = bootstrap_main(snapshot, extract_date=date(2026, 5, 6))
    assert history.height == 1
    row = history.to_dicts()[0]
    assert row["valid_from"] == date(2018, 2, 16)
    assert row["valid_to"] is None
    assert row["snapshot_observed_date"] == date(2026, 5, 6)


def test_bootstrap_main_falls_back_to_extract_date_when_record_last_updated_null():
    snapshot = _df([_main_row(abn="22222222222", record_last_updated=None)])
    history = bootstrap_main(snapshot, extract_date=date(2026, 5, 6))
    row = history.to_dicts()[0]
    assert row["valid_from"] == date(2026, 5, 6)
    assert row["valid_to"] is None


def test_bootstrap_trading_stamps_extract_date():
    snapshot = pl.DataFrame(
        [{"abn": "11111111111", "name": "ACME", "name_type": "BN"}]
    )
    history = bootstrap_trading(snapshot, extract_date=date(2026, 5, 6))
    row = history.to_dicts()[0]
    assert row["valid_from"] == date(2026, 5, 6)
    assert row["valid_to"] is None
    assert row["snapshot_observed_date"] == date(2026, 5, 6)


def test_bootstrap_dgr_stamps_extract_date():
    snapshot = pl.DataFrame(
        [
            {
                "abn": "11111111111",
                "dgr_status_from_date": date(2010, 1, 1),
                "dgr_status": "ACT",
                "dgr_name": None,
            }
        ]
    )
    history = bootstrap_dgr(snapshot, extract_date=date(2026, 5, 6))
    row = history.to_dicts()[0]
    assert row["valid_from"] == date(2026, 5, 6)
    assert row["valid_to"] is None


# --- apply_update_main: shape preservation ------------------------------


def test_apply_update_no_change_keeps_history_size_constant():
    snapshot = _df([_main_row(abn="11111111111"), _main_row(abn="22222222222")])
    history = bootstrap_main(snapshot, date(2026, 5, 6))
    updated = apply_update_main(history, snapshot, date(2026, 5, 13))
    assert updated.height == 2
    assert updated.filter(pl.col("valid_to").is_null()).height == 2


def test_apply_update_unchanged_row_keeps_original_valid_from():
    snapshot = _df([_main_row(record_last_updated=date(2018, 2, 16))])
    history = bootstrap_main(snapshot, date(2026, 5, 6))
    updated = apply_update_main(history, snapshot, date(2026, 5, 13))
    assert updated.height == 1
    row = updated.to_dicts()[0]
    assert row["valid_from"] == date(2018, 2, 16)


# --- apply_update_main: change detection ------------------------------


def test_apply_update_changed_field_closes_old_and_opens_new():
    prev_snapshot = _df([_main_row(main_name="ACME PTY LTD")])
    history = bootstrap_main(prev_snapshot, date(2026, 5, 6))
    new_snapshot = _df([_main_row(main_name="ACME REBRAND PTY LTD")])
    updated = apply_update_main(history, new_snapshot, date(2026, 5, 13))

    assert updated.height == 2
    closed = updated.filter(pl.col("valid_to").is_not_null()).to_dicts()[0]
    assert closed["main_name"] == "ACME PTY LTD"
    assert closed["valid_to"] == date(2026, 5, 13)
    new = updated.filter(pl.col("valid_to").is_null()).to_dicts()[0]
    assert new["main_name"] == "ACME REBRAND PTY LTD"
    assert new["valid_from"] == date(2026, 5, 13)


def test_apply_update_added_abn_appended_with_open_version():
    prev_snapshot = _df([_main_row(abn="11111111111")])
    history = bootstrap_main(prev_snapshot, date(2026, 5, 6))
    new_snapshot = _df(
        [
            _main_row(abn="11111111111"),
            _main_row(abn="22222222222", main_name="NEW COMPANY PTY LTD"),
        ]
    )
    updated = apply_update_main(history, new_snapshot, date(2026, 5, 13))
    assert updated.height == 2
    new = updated.filter(pl.col("abn") == "22222222222").to_dicts()[0]
    assert new["valid_from"] == date(2026, 5, 13)
    assert new["valid_to"] is None
    assert new["main_name"] == "NEW COMPANY PTY LTD"


def test_apply_update_removed_abn_closes_open_row():
    prev_snapshot = _df([_main_row(abn="11111111111"), _main_row(abn="22222222222")])
    history = bootstrap_main(prev_snapshot, date(2026, 5, 6))
    new_snapshot = _df([_main_row(abn="11111111111")])
    updated = apply_update_main(history, new_snapshot, date(2026, 5, 13))
    assert updated.height == 2
    closed = updated.filter(pl.col("valid_to").is_not_null()).to_dicts()[0]
    assert closed["abn"] == "22222222222"
    assert closed["valid_to"] == date(2026, 5, 13)
    open_rows = updated.filter(pl.col("valid_to").is_null())
    assert open_rows.height == 1
    assert open_rows.to_dicts()[0]["abn"] == "11111111111"


# --- apply_update: idempotency / multi-week sequences --------------------


def test_apply_update_two_consecutive_runs_idempotent_on_unchanged_data():
    snapshot = _df([_main_row()])
    history = bootstrap_main(snapshot, date(2026, 5, 6))
    once = apply_update_main(history, snapshot, date(2026, 5, 13))
    twice = apply_update_main(once, snapshot, date(2026, 5, 13))
    assert once.equals(twice)


def test_three_week_sequence_with_change_in_week_two():
    s1 = _df([_main_row(state="NSW")])
    history = bootstrap_main(s1, date(2026, 5, 6))
    s2 = _df([_main_row(state="VIC")])
    history = apply_update_main(history, s2, date(2026, 5, 13))
    s3 = _df([_main_row(state="VIC")])  # unchanged from week 2
    history = apply_update_main(history, s3, date(2026, 5, 20))

    closed = history.filter(pl.col("valid_to").is_not_null()).sort("valid_from").to_dicts()
    open_rows = history.filter(pl.col("valid_to").is_null()).to_dicts()

    assert len(closed) == 1
    assert closed[0]["state"] == "NSW"
    assert closed[0]["valid_to"] == date(2026, 5, 13)
    assert len(open_rows) == 1
    assert open_rows[0]["state"] == "VIC"
    assert open_rows[0]["valid_from"] == date(2026, 5, 13)


# --- apply_update_trading: set-membership semantics ---------------------


def test_apply_update_trading_added_creates_open_row():
    prev = bootstrap_trading(
        pl.DataFrame([{"abn": "11111111111", "name": "ACME", "name_type": "BN"}]),
        date(2026, 5, 6),
    )
    new = pl.DataFrame(
        [
            {"abn": "11111111111", "name": "ACME", "name_type": "BN"},
            {"abn": "11111111111", "name": "ACME GLOBAL", "name_type": "BN"},
        ]
    )
    updated = apply_update_trading(prev, new, date(2026, 5, 13))
    assert updated.height == 2
    assert updated.filter(pl.col("valid_to").is_null()).height == 2


def test_apply_update_trading_removed_closes_open_row():
    prev = bootstrap_trading(
        pl.DataFrame(
            [
                {"abn": "11111111111", "name": "ACME", "name_type": "BN"},
                {"abn": "11111111111", "name": "ACME PRODUCTS", "name_type": "BN"},
            ]
        ),
        date(2026, 5, 6),
    )
    new = pl.DataFrame([{"abn": "11111111111", "name": "ACME", "name_type": "BN"}])
    updated = apply_update_trading(prev, new, date(2026, 5, 13))
    closed = updated.filter(pl.col("valid_to").is_not_null()).to_dicts()
    assert len(closed) == 1
    assert closed[0]["name"] == "ACME PRODUCTS"
    assert closed[0]["valid_to"] == date(2026, 5, 13)


# --- apply_update_dgr: content-tracking on (abn, dgr_status_from_date) ---


def test_apply_update_dgr_status_change_closes_old_opens_new():
    prev = bootstrap_dgr(
        pl.DataFrame(
            [
                {
                    "abn": "11111111111",
                    "dgr_status_from_date": date(2010, 1, 1),
                    "dgr_status": "ACT",
                    "dgr_name": None,
                }
            ]
        ),
        date(2026, 5, 6),
    )
    new = pl.DataFrame(
        [
            {
                "abn": "11111111111",
                "dgr_status_from_date": date(2010, 1, 1),
                "dgr_status": "REVOKED",
                "dgr_name": None,
            }
        ]
    )
    updated = apply_update_dgr(prev, new, date(2026, 5, 13))
    assert updated.height == 2
    closed = updated.filter(pl.col("valid_to").is_not_null()).to_dicts()[0]
    assert closed["dgr_status"] == "ACT"
    open_row = updated.filter(pl.col("valid_to").is_null()).to_dicts()[0]
    assert open_row["dgr_status"] == "REVOKED"


# --- summary -------------------------------------------------------------


def test_history_summary_counts():
    snapshot = _df([_main_row()])
    history = bootstrap_main(snapshot, date(2026, 5, 6))
    new_snapshot = _df([_main_row(state="VIC")])
    updated = apply_update_main(history, new_snapshot, date(2026, 5, 13))
    summary = history_summary(updated)
    assert summary["total_rows"] == 2
    assert summary["open_rows"] == 1
    assert summary["closed_rows"] == 1
