"""DAG integrity checks: the DAG imports cleanly and has the expected shape."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
EXPECTED_ORDER = [
    "ensure_schema",
    "extract_to_raw",
    "validate_and_load",
    "upsert_current_orders",
    "dbt_source_freshness",
    "dbt_build",
    "check_volume_anomaly",
    "apply_retention",
]


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(DAGS_DIR))
    assert bag.import_errors == {}
    return bag.get_dag("orders_hourly_pipeline")


def test_dag_is_loaded(dag):
    assert dag is not None
    assert dag.max_active_runs == 1
    assert not dag.catchup


def test_tasks_run_in_the_expected_order(dag):
    assert sorted(dag.task_ids) == sorted(EXPECTED_ORDER)
    for upstream, downstream in zip(EXPECTED_ORDER[:-1], EXPECTED_ORDER[1:], strict=True):
        assert downstream in dag.get_task(upstream).downstream_task_ids


def test_every_task_retries(dag):
    for task in dag.tasks:
        assert task.retries >= 1, task.task_id
