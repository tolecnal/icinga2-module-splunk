from __future__ import annotations

import json
import os
import stat
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from icinga2_splunk.spool import DiskSpool


def lines(*values: Any) -> list[str]:
    return [json.dumps({"event": v}) for v in values]


def events(spool_lines: list[str]) -> list[Any]:
    return [json.loads(line)["event"] for line in spool_lines]


class Clock:
    def __init__(self) -> None:
        self.now = 1_000_000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def make(tmp_path: Path, clock: Clock) -> Iterator[Any]:
    created: list[DiskSpool] = []

    def factory(**kwargs: Any) -> DiskSpool:
        kwargs.setdefault("max_bytes", 1024 * 1024)
        kwargs.setdefault("max_age", 3600)
        spool = DiskSpool(tmp_path / "spool", clock=clock, **kwargs)
        created.append(spool)
        return spool

    yield factory
    for spool in created:
        spool.close()


def segments(tmp_path: Path) -> list[Path]:
    return sorted((tmp_path / "spool").glob("*.jsonl"))


def test_directory_and_file_permissions(make: Any, tmp_path: Path) -> None:
    spool = make()
    spool.append(lines(1))
    spool.close()
    assert stat.S_IMODE((tmp_path / "spool").stat().st_mode) == 0o700
    assert stat.S_IMODE(segments(tmp_path)[0].stat().st_mode) == 0o600


def test_append_read_remove(make: Any) -> None:
    spool = make()
    assert not spool
    assert spool.oldest() is None
    spool.append(lines(1, 2))
    spool.append(lines(3))
    spool.append([])
    assert spool
    path, got = spool.oldest()
    assert events(got) == [1, 2, 3]
    spool.remove(path)
    assert not spool
    assert spool.size_bytes == 0


def test_segments_roll_and_replay_in_order(make: Any, tmp_path: Path) -> None:
    spool = make(segment_max_bytes=20, max_bytes=10_000)
    for i in range(6):
        spool.append(lines(i))
    spool.close()
    assert len(segments(tmp_path)) >= 3
    seen: list[Any] = []
    while (item := spool.oldest()) is not None:
        seen.extend(events(item[1]))
        spool.remove(item[0])
    assert seen == list(range(6))


def test_existing_segments_loaded_on_start(make: Any, tmp_path: Path) -> None:
    first = make()
    first.append(lines("a", "b"))
    first.close()
    (tmp_path / "spool" / "00000000000000000000-empty.jsonl").touch()

    second = make()
    assert second
    _, got = second.oldest()
    assert events(got) == ["a", "b"]
    assert not (tmp_path / "spool" / "00000000000000000000-empty.jsonl").exists()


def test_corrupt_lines_are_skipped(make: Any, tmp_path: Path, caplog: Any) -> None:
    spool = make()
    spool.append(lines(1))
    spool.close()
    with segments(tmp_path)[0].open("ab") as fh:
        fh.write(b'{"event": 2}\n{"event": tor\n\xff\xfe\n')
    second = make()
    _, got = second.oldest()
    assert events(got) == [1, 2]
    assert "2 corrupt line" in caplog.text


def test_size_limit_drops_oldest(make: Any, caplog: Any) -> None:
    spool = make(max_bytes=200, segment_max_bytes=50)
    for i in range(20):
        spool.append(lines(i))
    assert spool.size_bytes <= 200
    assert spool.dropped_count > 0
    remaining: list[Any] = []
    while (item := spool.oldest()) is not None:
        remaining.extend(events(item[1]))
        spool.remove(item[0])
    assert remaining == list(range(20 - len(remaining), 20))
    assert remaining[-1] == 19
    assert "spool size limit reached" in caplog.text


def test_single_append_larger_than_limit(make: Any) -> None:
    spool = make(max_bytes=10)
    spool.append(lines("x" * 100))
    assert spool.size_bytes == 0
    assert spool.dropped_count == 1


def test_age_limit(make: Any, clock: Clock, tmp_path: Path, caplog: Any) -> None:
    spool = make(max_age=100, segment_max_bytes=1)
    spool.append(lines("old"))
    spool.append(lines("new"))  # rolls the "old" segment
    old, new = segments(tmp_path)
    os.utime(old, (clock.now - 200, clock.now - 200))
    os.utime(new, (clock.now - 50, clock.now - 50))

    spool.enforce_limits()
    assert not old.exists()
    assert new.exists()
    assert spool.dropped_count == 1
    assert "older than the spool age limit" in caplog.text


def test_age_limit_applies_to_idle_current_segment(make: Any, clock: Clock, tmp_path: Path) -> None:
    spool = make(max_age=100)
    spool.append(lines("idle"))
    [current] = segments(tmp_path)
    os.utime(current, (clock.now - 500, clock.now - 500))
    spool.enforce_limits()
    assert not spool
    assert spool.dropped_count == 1


def test_externally_deleted_segments_are_tolerated(make: Any, tmp_path: Path) -> None:
    spool = make(segment_max_bytes=1)
    spool.append(lines(1))
    spool.append(lines(2))
    spool.close()
    for path in segments(tmp_path):
        path.unlink()
    spool.enforce_limits()
    assert spool.oldest() is None


def test_deleted_segment_skipped_on_read(make: Any, tmp_path: Path) -> None:
    spool = make(segment_max_bytes=1)
    spool.append(lines(1))
    spool.append(lines(2))
    spool.close()
    segments(tmp_path)[0].unlink()
    _, got = spool.oldest()
    assert events(got) == [2]


def test_unwritable_directory(tmp_path: Path) -> None:
    directory = tmp_path / "ro"
    directory.mkdir(mode=0o500)
    if os.access(directory, os.W_OK):  # running as root
        pytest.skip("cannot test permissions as root")
    with pytest.raises(PermissionError):
        DiskSpool(directory, max_bytes=100, max_age=10)


def test_age_limit_handles_out_of_order_mtimes(make: Any, clock: Clock, tmp_path: Path) -> None:
    spool = make(max_age=100, segment_max_bytes=1)
    for name in ("fresh", "old", "current"):
        spool.append(lines(name))
    fresh, old, _current = segments(tmp_path)
    os.utime(fresh, (clock.now - 10, clock.now - 10))
    os.utime(old, (clock.now - 200, clock.now - 200))
    spool.enforce_limits()
    assert [p.exists() for p in (fresh, old)] == [True, False]
    assert spool.dropped_count == 1


def test_current_segment_deleted_externally(make: Any, tmp_path: Path) -> None:
    spool = make()
    spool.append(lines(1))
    segments(tmp_path)[0].unlink()
    spool.enforce_limits()  # must not raise


def test_drop_of_externally_deleted_segment(make: Any, tmp_path: Path) -> None:
    spool = make(max_bytes=120, segment_max_bytes=30)
    spool.append(lines(1))
    spool.append(lines(2))
    segments(tmp_path)[0].unlink()
    for i in range(10):
        spool.append(lines(i))
    assert spool.size_bytes <= 120
