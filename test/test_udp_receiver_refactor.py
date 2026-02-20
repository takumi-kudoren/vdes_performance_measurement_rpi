"""udp_receiverの挙動不変リファクタ向け回帰テスト。"""

from __future__ import annotations

import socket
import sys
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIRECTORY = PROJECT_ROOT / "src"
if str(SRC_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(SRC_DIRECTORY))

from udp.receive import udp_receiver  # noqa: E402


class _FakeUdpSocket:
    """collect_receive_metrics検証用の簡易ソケット実装。"""

    def __init__(self, responses: list[tuple[bytes, tuple[str, int]]]) -> None:
        self._responses = responses
        self.sockopt_calls: list[tuple[int, int, object]] = []
        self.bound_address: tuple[str, int] | None = None
        self.timeout_seconds: float | None = None
        self.closed = False

    def setsockopt(self, level: int, optname: int, value: object) -> None:
        self.sockopt_calls.append((level, optname, value))

    def bind(self, address: tuple[str, int]) -> None:
        self.bound_address = address

    def settimeout(self, timeout_seconds: float) -> None:
        self.timeout_seconds = timeout_seconds

    def recvfrom(self, _buffer_size: int) -> tuple[bytes, tuple[str, int]]:
        if not self._responses:
            raise TimeoutError()
        return self._responses.pop(0)

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> _FakeUdpSocket:
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        del exc_type, exc, traceback
        return False


class TestUdpReceiverRefactor(unittest.TestCase):
    """分境界終了と受信集計の主要挙動を確認する。"""

    def test_初回受信後は次境界到達で終了する(self) -> None:
        base_utc = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        boundary_utc = base_utc + timedelta(seconds=1)
        tdb_sentence = "!VATDB,1,1,1,200000001,200000002,0,0,11,0*00"
        fake_socket = _FakeUdpSocket([(tdb_sentence.encode("utf-8"), ("192.0.2.10", 50000))])

        with (
            patch("udp.receive.udp_receiver.socket.socket", return_value=fake_socket),
            patch("udp.receive.udp_receiver._resolve_next_minute_boundary_utc", return_value=boundary_utc),
            patch("udp.receive.udp_receiver._current_utc", side_effect=[base_utc, base_utc, boundary_utc]),
        ):
            metrics = udp_receiver.collect_receive_metrics_until_next_minute_boundary()

        self.assertEqual(metrics.first_tdb_received_utc, base_utc)
        self.assertEqual(metrics.measurement_end_utc, boundary_utc)
        self.assertEqual(metrics.rx_tdb_count, 1)
        self.assertEqual(metrics.rx_payload_chars_total, 2)
        self.assertEqual(len(metrics.received_tdb_sentence_records), 1)

        drop_membership_calls = [
            call for call in fake_socket.sockopt_calls if call[1] == socket.IP_DROP_MEMBERSHIP
        ]
        self.assertEqual(len(drop_membership_calls), 1)


if __name__ == "__main__":
    unittest.main()
