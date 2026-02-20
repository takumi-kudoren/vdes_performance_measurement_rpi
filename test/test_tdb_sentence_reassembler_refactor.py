"""TDB再構成の挙動不変リファクタ向け回帰テスト。"""

from __future__ import annotations

import sys
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIRECTORY = PROJECT_ROOT / "src"
if str(SRC_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(SRC_DIRECTORY))

from reporting.tdb_sentence_reassembler import TdbSentenceReassembler  # noqa: E402


class TestTdbSentenceReassemblerRefactor(unittest.TestCase):
    """分割再構成の成功・失敗件数が維持されることを確認する。"""

    def test_正常系_再構成成功時に成功件数が増える(self) -> None:
        reassembler = TdbSentenceReassembler()
        base_utc = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        sentence_1 = "!VATDB,2,1,1,200000001,200000002,0,0,11,0*00"
        sentence_2 = "!VATDB,2,2,1,200000001,200000002,0,0,22,0*00"

        self.assertEqual(reassembler.reassemble_sentences([sentence_1], base_utc), [])
        results = reassembler.reassemble_sentences([sentence_2], base_utc + timedelta(seconds=1))

        self.assertEqual(len(results), 1)
        self.assertRegex(results[0], r"^!VATDB,1,1,0,200000001,200000002,0,0,.*\*[0-9A-F]{2}$")
        success_count, failure_count = reassembler.get_split_reconstruct_counts()
        self.assertEqual(success_count, 1)
        self.assertEqual(failure_count, 0)

    def test_異常系_重複受信時に失敗件数が増える(self) -> None:
        reassembler = TdbSentenceReassembler()
        base_utc = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        sentence = "!VATDB,2,1,1,200000001,200000002,0,0,11,0*00"

        reassembler.reassemble_sentences([sentence], base_utc)
        reassembler.reassemble_sentences([sentence], base_utc + timedelta(seconds=1))

        success_count, failure_count = reassembler.get_split_reconstruct_counts()
        self.assertEqual(success_count, 0)
        self.assertEqual(failure_count, 1)

    def test_異常系_期限切れ時に失敗件数が増える(self) -> None:
        reassembler = TdbSentenceReassembler()
        base_utc = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        sentence = "!VATDB,2,1,1,200000001,200000002,0,0,11,0*00"

        reassembler.reassemble_sentences([sentence], base_utc)
        reassembler.reassemble_sentences([], base_utc + timedelta(seconds=31))

        success_count, failure_count = reassembler.get_split_reconstruct_counts()
        self.assertEqual(success_count, 0)
        self.assertEqual(failure_count, 1)

    def test_異常系_解析失敗時に失敗件数が増える(self) -> None:
        reassembler = TdbSentenceReassembler()
        base_utc = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        sentence_1 = "!VATDB,2,1,1,200000001,200000002,0,0,11,0*00"
        invalid_sentence = "!VATDB,2,X,1,200000001,200000002,0,0,22,0*00"

        reassembler.reassemble_sentences([sentence_1], base_utc)
        reassembler.reassemble_sentences([invalid_sentence], base_utc + timedelta(seconds=1))

        success_count, failure_count = reassembler.get_split_reconstruct_counts()
        self.assertEqual(success_count, 0)
        self.assertEqual(failure_count, 1)

    def test_異常系_分境界確定時に欠落分割を失敗確定する(self) -> None:
        reassembler = TdbSentenceReassembler()
        base_utc = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        sentence = "!VATDB,2,1,1,200000001,200000002,0,0,11,0*00"

        reassembler.reassemble_sentences([sentence], base_utc)
        pending_count = reassembler.finalize_pending_groups_as_boundary_failure()

        self.assertEqual(pending_count, 1)
        success_count, failure_count = reassembler.get_split_reconstruct_counts()
        self.assertEqual(success_count, 0)
        self.assertEqual(failure_count, 1)


if __name__ == "__main__":
    unittest.main()
