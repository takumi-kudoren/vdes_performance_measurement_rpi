"""receive_reportの挙動不変リファクタ向け回帰テスト。"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIRECTORY = PROJECT_ROOT / "src"
if str(SRC_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(SRC_DIRECTORY))

from reporting import receive_report  # noqa: E402
from udp.receive.udp_receiver import ReceivedTdbSentenceRecord  # noqa: E402


class TestReceiveReportRefactor(unittest.TestCase):
    """JSONL構造とUTC表記の回帰を確認する。"""

    def test_jsonlの1行目はsummaryで2行目以降はsentenceになる(self) -> None:
        report_timestamp = datetime(2026, 1, 1, 0, 0, 0)
        record_received_utc = datetime(2026, 1, 1, 0, 0, 1, 123000, tzinfo=UTC)
        sentence_record = ReceivedTdbSentenceRecord(
            received_utc=record_received_utc,
            tdb_sentence="!VATDB,1,1,0,200000001,200000002,0,0,11,0*00",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch("reporting.receive_report.get_project_root", return_value=Path(temp_dir)):
                report_file_path = receive_report.write_receive_report_jsonl(
                    rx_tdb_count=1,
                    rx_payload_chars_total=2,
                    split_reconstruct_success_count=0,
                    split_reconstruct_failure_count=0,
                    received_tdb_sentence_records=[sentence_record],
                    report_timestamp_utc=report_timestamp,
                )

            lines = report_file_path.read_text(encoding="utf-8").splitlines()

        self.assertEqual(len(lines), 2)
        summary_record = json.loads(lines[0])
        sentence_detail_record = json.loads(lines[1])

        self.assertEqual(summary_record["record_type"], "summary")
        self.assertEqual(summary_record["rx_tdb_count"], 1)
        self.assertEqual(sentence_detail_record["record_type"], "sentence")
        self.assertEqual(sentence_detail_record["tdb_sentence"], sentence_record.tdb_sentence)
        self.assertEqual(sentence_detail_record["sentence_received_utc"], "2026-01-01T00:00:01.123Z")


if __name__ == "__main__":
    unittest.main()
