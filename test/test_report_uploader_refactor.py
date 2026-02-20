"""report_uploaderの挙動不変リファクタ向け回帰テスト。"""

from __future__ import annotations

import errno
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIRECTORY = PROJECT_ROOT / "src"
if str(SRC_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(SRC_DIRECTORY))

from sftp import report_uploader  # noqa: E402


class _FakeSftpClientForDirectory:
    """リモートディレクトリ作成検証用の簡易SFTPクライアント。"""

    def __init__(self, existing_paths: set[str]) -> None:
        self._existing_paths = set(existing_paths)
        self.created_paths: list[str] = []

    def stat(self, remote_path: str) -> None:
        if remote_path not in self._existing_paths:
            raise FileNotFoundError(remote_path)

    def mkdir(self, remote_path: str) -> None:
        if remote_path in self._existing_paths:
            error = OSError("already exists")
            error.errno = errno.EEXIST
            raise error
        self._existing_paths.add(remote_path)
        self.created_paths.append(remote_path)


class TestReportUploaderRefactor(unittest.TestCase):
    """リモートディレクトリ作成と再試行挙動の回帰を確認する。"""

    def test_ディレクトリ不足時は不足階層のみ作成する(self) -> None:
        fake_sftp = _FakeSftpClientForDirectory(existing_paths={"/home"})

        report_uploader._ensure_remote_directory(fake_sftp, "/home/admin/vdes_web_sys")

        self.assertEqual(fake_sftp.created_paths, ["/home/admin", "/home/admin/vdes_web_sys"])

    def test_ディレクトリが既に存在する場合は作成しない(self) -> None:
        fake_sftp = _FakeSftpClientForDirectory(existing_paths={"/home", "/home/admin", "/home/admin/vdes_web_sys"})

        report_uploader._ensure_remote_directory(fake_sftp, "/home/admin/vdes_web_sys")

        self.assertEqual(fake_sftp.created_paths, [])

    def test_アップロード失敗時は待機後に再試行する(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            local_report_file_path = Path(temp_dir) / "REPORT_20260101_000000.jsonl"
            local_report_file_path.write_text("{\"record_type\":\"summary\"}\n", encoding="utf-8")

            mock_ssh_client = Mock()
            mock_sftp_client = Mock()

            with (
                patch(
                    "sftp.report_uploader.open_sftp_connection",
                    side_effect=[Exception("first failure"), (mock_ssh_client, mock_sftp_client)],
                ) as open_mock,
                patch("sftp.report_uploader.close_sftp_connection") as close_mock,
                patch("sftp.report_uploader._ensure_remote_directory") as ensure_mock,
                patch("sftp.report_uploader.time.sleep") as sleep_mock,
            ):
                remote_path = report_uploader.upload_report_file_until_success(local_report_file_path)

        self.assertEqual(open_mock.call_count, 2)
        self.assertEqual(close_mock.call_count, 2)
        ensure_mock.assert_called_once()
        sleep_mock.assert_called_once_with(report_uploader.sftp_report_constants.POLL_INTERVAL_SECONDS)
        mock_sftp_client.put.assert_called_once()
        self.assertEqual(
            remote_path,
            f"{report_uploader.sftp_report_constants.REMOTE_REPORT_DIRECTORY}/{local_report_file_path.name}",
        )


if __name__ == "__main__":
    unittest.main()
