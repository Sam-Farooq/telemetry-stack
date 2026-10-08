"""The operational scripts, run as the shell runs them, against the fixtures.

No service is started and nothing is fetched: every script takes --source, so
a file stands in for the endpoint and the exit codes are the contract. The
fixtures are hand-authored, not captured; fixtures/README.md says how.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from telemetry.sampling import Trace, keep_rate


def run(repo_root: Path, script: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, f"scripts/{script}", *args],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def test_check_budgets_passes_on_the_app_metrics_fixture(repo_root: Path) -> None:
    result = run(repo_root, "check-budgets.py", "--source", "fixtures/app-metrics.txt")
    assert result.returncode == 0, result.stderr
    assert "4 budgets" in result.stdout
    assert "http_server_request_duration_seconds: 4 label sets, budget 1200" in result.stdout
    assert "OVER BUDGET" not in result.stderr


def test_check_budgets_fails_and_names_the_label(repo_root: Path, tmp_path: Path) -> None:
    tight = tmp_path / "budgets.json"
    tight.write_text(
        json.dumps(
            {
                "latency_bucket_boundaries_seconds": [0.1, 1.0],
                "metrics": {
                    "kafka_consumer_lag_records": {
                        "kind": "gauge",
                        "labels": {"service": 1, "topic": 1, "partition": 2},
                        "max_label_sets": 2,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    result = run(
        repo_root,
        "check-budgets.py",
        "--source",
        "fixtures/app-metrics.txt",
        "--budgets",
        str(tight),
        "--quiet",
    )
    assert result.returncode == 1
    assert "OVER BUDGET kafka_consumer_lag_records: 3 label sets against 2" in result.stderr
    assert "Worst label is partition with 3 values" in result.stderr
    assert result.stdout == ""


def test_check_budgets_refuses_an_empty_scrape(repo_root: Path, tmp_path: Path) -> None:
    empty = tmp_path / "empty.txt"
    empty.write_text("# HELP nothing here\n", encoding="utf-8")
    result = run(repo_root, "check-budgets.py", "--source", str(empty))
    assert result.returncode == 1
    assert "no samples" in result.stderr


def test_collector_health_reports_loss_with_exit_code_two(repo_root: Path) -> None:
    result = run(repo_root, "collector-health.py", "--source", "fixtures/collector-metrics.txt")
    assert result.returncode == 2
    assert "61440 items refused by a full queue" in result.stdout
    assert "overall: dropping" in result.stdout
    assert "receiver has refused 41280 spans" in result.stdout


def test_collector_health_can_report_without_failing(repo_root: Path) -> None:
    result = run(
        repo_root,
        "collector-health.py",
        "--source",
        "fixtures/collector-metrics.txt",
        "--exit-zero",
    )
    assert result.returncode == 0
    assert "overall: dropping" in result.stdout


def test_two_identical_scrapes_mean_nothing_is_being_lost_now(repo_root: Path) -> None:
    # The counter is cumulative, so the same file twice is a zero delta.
    result = run(
        repo_root,
        "collector-health.py",
        "--source",
        "fixtures/collector-metrics.txt",
        "--previous",
        "fixtures/collector-metrics.txt",
    )
    # Same scrape, read as a delta: the queue is three quarters full and
    # filling, which is worth looking at and not worth failing a check for.
    assert result.returncode == 0, result.stdout
    assert "overall: filling" in result.stdout
    assert "failed send attempts being retried" in result.stdout


def test_the_sampling_report_prints_what_the_policy_computes(repo_root: Path) -> None:
    result = run(repo_root, "tail-sample-report.py")
    assert result.returncode == 0, result.stderr

    lines = (repo_root / "fixtures" / "traces.jsonl").read_text().splitlines()
    rows = [json.loads(line) for line in lines]
    traces = [
        Trace(
            trace_id=row["trace_id"],
            service=row["service"],
            route=row["route"],
            duration_ms=row["duration_ms"],
            span_count=row["span_count"],
            status_code=row["status_code"],
            error=row["error"],
        )
        for row in rows
    ]
    expected_kept = round(keep_rate(traces) * len(traces))
    assert f"traces      {len(traces)}" in result.stdout
    assert f"kept        {expected_kept} " in result.stdout
    assert "head at 10% would lose 22 of those 26" in result.stdout


def test_a_wider_percentage_keeps_more(repo_root: Path) -> None:
    narrow = run(repo_root, "tail-sample-report.py", "--percentage", "2")
    wide = run(repo_root, "tail-sample-report.py", "--percentage", "50")
    assert narrow.returncode == wide.returncode == 0

    def kept(output: str) -> int:
        line = next(line for line in output.splitlines() if line.startswith("kept"))
        return int(line.split()[1])

    assert kept(narrow.stdout) < kept(wide.stdout)


def test_the_loadgen_sends_nothing_when_asked_for_no_time(repo_root: Path) -> None:
    result = run(repo_root, "loadgen.py", "--seconds", "0", "--base", "http://127.0.0.1:1")
    assert result.returncode == 1
    assert "0 requests in 0s" in result.stdout


def test_the_loadgen_rejects_a_rate_of_zero(repo_root: Path) -> None:
    result = run(repo_root, "loadgen.py", "--rate", "0")
    assert result.returncode == 2
    assert "--rate has to be positive" in result.stderr


def test_the_bootstrap_script_is_valid_shell(repo_root: Path) -> None:
    result = subprocess.run(
        ["bash", "-n", "scripts/bootstrap-elasticsearch.sh"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_the_bootstrap_script_installs_the_policies_the_repo_declares(repo_root: Path) -> None:
    script = (repo_root / "scripts" / "bootstrap-elasticsearch.sh").read_text(encoding="utf-8")
    for name in ("ilm-logs.json", "ilm-traces.json"):
        policy = json.loads((repo_root / "elasticsearch" / name).read_text())["policy"]["_meta"]
        assert f"/_ilm/policy/{policy['policy_name']}" in script
        assert policy["write_alias"] in script
        assert name in script
    for name in ("template-logs.json", "template-traces.json"):
        assert name in script


@pytest.mark.parametrize("script", ["check-budgets.py", "collector-health.py", "loadgen.py"])
def test_every_script_documents_itself(repo_root: Path, script: str) -> None:
    result = run(repo_root, script, "--help")
    assert result.returncode == 0
    assert "--source" in result.stdout or "--base" in result.stdout
