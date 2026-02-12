"""SFTP経由で受信側パフォーマンスを取得する処理を提供するモジュール。"""

from __future__ import annotations

import json
import logging
import stat
import time
from dataclasses import dataclass

import paramiko

from consts import sftp_report_constants
from sftp.client import close_sftp_connection, open_sftp_connection

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReceivePerformanceSummary:
    """受信側パフォーマンスの集計結果を保持する。

    引数:
        rx_tdb_count: 受信TDB数。
        rx_payload_chars_total: 受信成功payload総文字数。
        source_remote_jsonl_path: 集計元となったサーバー上のjsonlパス。

    戻り値:
        なし。

    例外:
        なし。
    """

    rx_tdb_count: int
    rx_payload_chars_total: int
    source_remote_jsonl_path: str


# 補助処理
def _join_remote_path(directory_path: str, file_name: str) -> str:
    """サーバー上のディレクトリとファイル名からフルパスを生成する。

    引数:
        directory_path: サーバー上のディレクトリパス。
        file_name: ファイル名。

    戻り値:
        サーバー上のフルパス。

    例外:
        なし。
    """
    return f"{directory_path.rstrip('/')}/{file_name}"


# 補助処理
def _is_target_report_jsonl_file(entry: paramiko.SFTPAttributes) -> bool:
    """対象となる受信側パフォーマンスjsonlかどうかを判定する。

    引数:
        entry: SFTPで取得したディレクトリエントリ。

    戻り値:
        対象ならTrue、対象外ならFalse。

    例外:
        なし。

    補足:
        名前規則と通常ファイル判定の両方を満たすもののみを対象にする。
    """
    file_name = entry.filename
    if not file_name.startswith(sftp_report_constants.REPORT_FILE_PREFIX):
        return False
    if not file_name.endswith(sftp_report_constants.REPORT_FILE_SUFFIX):
        return False

    return stat.S_ISREG(entry.st_mode)


# 補助処理
def _select_latest_report_jsonl_path(sftp_client: paramiko.SFTPClient) -> str:
    """対象ディレクトリから最新の受信側パフォーマンスjsonlを選択する。

    引数:
        sftp_client: SFTPクライアント。

    戻り値:
        最新jsonlのサーバー上フルパス。

    例外:
        FileNotFoundError: 対象ディレクトリにファイルが存在しない場合。

    補足:
        最新判定はファイル名の名前順で実施する。
    """
    directory_entries = sftp_client.listdir_attr(sftp_report_constants.REMOTE_REPORT_JSON_DIRECTORY)

    target_file_names: list[str] = []
    for entry in directory_entries:
        if not _is_target_report_jsonl_file(entry):
            continue
        target_file_names.append(entry.filename)

    if not target_file_names:
        raise FileNotFoundError(
            f"受信側パフォーマンスjsonlが存在しません。directory={sftp_report_constants.REMOTE_REPORT_JSON_DIRECTORY}"
        )

    latest_file_name = max(target_file_names)
    return _join_remote_path(sftp_report_constants.REMOTE_REPORT_JSON_DIRECTORY, latest_file_name)


# 補助処理
def _read_remote_jsonl_text(sftp_client: paramiko.SFTPClient, remote_jsonl_path: str) -> str:
    """サーバー上のjsonlファイル本文を読み込む。

    引数:
        sftp_client: SFTPクライアント。
        remote_jsonl_path: サーバー上のjsonlパス。

    戻り値:
        UTF-8として復号したjsonl本文。

    例外:
        OSError: 読み込みに失敗した場合。
    """
    with sftp_client.file(remote_jsonl_path, mode="rb") as remote_file:
        file_bytes = remote_file.read()

    return file_bytes.decode("utf-8", errors="replace")


# 補助処理
def _extract_required_int_value(record: dict[str, object], key_name: str, line_number: int) -> int | None:
    """JSONレコードから必須整数値を抽出する。

    引数:
        record: 1行分のJSONレコード。
        key_name: 抽出対象のキー名。
        line_number: 行番号。

    戻り値:
        抽出できた整数値。抽出できない場合はNone。

    例外:
        なし。

    補足:
        欠落や型不正を検知した場合は警告ログを出して除外する。
    """
    if key_name not in record:
        logger.warning("異常: 必須キーが存在しないためレコードを除外しました。行番号=%d キー=%s", line_number, key_name)
        return None

    raw_value = record[key_name]
    if isinstance(raw_value, bool):
        logger.warning(
            "異常: 必須キーが真偽値のためレコードを除外しました。行番号=%d キー=%s 値=%s",
            line_number,
            key_name,
            raw_value,
        )
        return None

    try:
        return int(raw_value)
    except (TypeError, ValueError):
        logger.warning(
            "異常: 必須キーが整数へ変換できないためレコードを除外しました。行番号=%d キー=%s 値=%s",
            line_number,
            key_name,
            raw_value,
        )
        return None


# 補助処理
def _parse_latest_valid_summary(jsonl_text: str, source_remote_jsonl_path: str) -> ReceivePerformanceSummary:
    """jsonl本文から最新有効レコードの受信側指標を抽出する。

    引数:
        jsonl_text: jsonlファイル本文。
        source_remote_jsonl_path: 集計元jsonlのサーバー上パス。

    戻り値:
        最新有効レコードから抽出した受信側パフォーマンス集計結果。

    例外:
        ValueError: 有効レコードが1件も存在しない場合。

    補足:
        複数有効行がある場合は、ファイル内の最後の有効行を採用する。
    """
    latest_summary: ReceivePerformanceSummary | None = None

    line_number = 0
    for raw_line in jsonl_text.splitlines():
        line_number += 1
        stripped_line = raw_line.strip()
        if not stripped_line:
            continue

        try:
            parsed_record = json.loads(stripped_line)
        except json.JSONDecodeError:
            logger.warning("異常: JSON形式が不正なためレコードを除外しました。行番号=%d", line_number)
            continue

        if not isinstance(parsed_record, dict):
            logger.warning("異常: JSONがオブジェクト形式ではないためレコードを除外しました。行番号=%d", line_number)
            continue

        rx_tdb_count = _extract_required_int_value(parsed_record, "rx_tdb_count", line_number)
        if rx_tdb_count is None:
            continue

        rx_payload_chars_total = _extract_required_int_value(parsed_record, "rx_payload_chars_total", line_number)
        if rx_payload_chars_total is None:
            continue

        latest_summary = ReceivePerformanceSummary(
            rx_tdb_count=rx_tdb_count,
            rx_payload_chars_total=rx_payload_chars_total,
            source_remote_jsonl_path=source_remote_jsonl_path,
        )

    if latest_summary is None:
        raise ValueError(f"有効な受信側パフォーマンスレコードが存在しません。path={source_remote_jsonl_path}")

    return latest_summary


# メイン処理
def wait_latest_receive_performance_summary() -> ReceivePerformanceSummary:
    """受信側パフォーマンスjsonlを待機取得して集計結果を返す。

    引数:
        なし。

    戻り値:
        取得できた受信側パフォーマンス集計結果。

    例外:
        なし。取得失敗時はログ出力して待機再試行を継続する。

    補足:
        受信側の配置遅延を許容するため、対象取得まで無期限で再試行する。
    """
    logger.info("開始: 受信側パフォーマンス集計の取得を開始します。")

    while True:
        ssh_client: paramiko.SSHClient | None = None
        sftp_client: paramiko.SFTPClient | None = None

        try:
            ssh_client, sftp_client = open_sftp_connection()
            latest_remote_jsonl_path = _select_latest_report_jsonl_path(sftp_client)
            jsonl_text = _read_remote_jsonl_text(sftp_client, latest_remote_jsonl_path)
            summary = _parse_latest_valid_summary(jsonl_text, latest_remote_jsonl_path)
        except Exception:
            logger.exception(
                "異常: 受信側パフォーマンス集計の取得に失敗しました。%d秒後に再試行します。",
                sftp_report_constants.POLL_INTERVAL_SECONDS,
            )
            time.sleep(sftp_report_constants.POLL_INTERVAL_SECONDS)
            continue
        finally:
            close_sftp_connection(ssh_client, sftp_client)

        logger.info(
            "終了: 受信側パフォーマンス集計の取得が完了しました。ファイル=%s 受信TDB数=%d 受信成功payload総文字数=%d",
            summary.source_remote_jsonl_path,
            summary.rx_tdb_count,
            summary.rx_payload_chars_total,
        )
        return summary


# メイン処理
def delete_referenced_jsonl_until_success(remote_jsonl_path: str) -> None:
    """参照した受信側jsonlをサーバーから削除するまで再試行する。

    引数:
        remote_jsonl_path: 削除対象となるサーバー上jsonlパス。

    戻り値:
        なし。

    例外:
        なし。削除失敗時はログ出力して待機再試行を継続する。
    """
    logger.info("開始: 受信側パフォーマンスjsonlの削除を開始します。対象=%s", remote_jsonl_path)

    while True:
        ssh_client: paramiko.SSHClient | None = None
        sftp_client: paramiko.SFTPClient | None = None

        try:
            ssh_client, sftp_client = open_sftp_connection()
            sftp_client.remove(remote_jsonl_path)
        except FileNotFoundError:
            logger.info(
                "終了: 受信側パフォーマンスjsonlは既に存在しないため、削除済みとして処理を継続します。対象=%s",
                remote_jsonl_path,
            )
            return
        except OSError as error:
            if error.errno == 2:
                logger.info(
                    "終了: 受信側パフォーマンスjsonlは既に存在しないため、削除済みとして処理を継続します。対象=%s",
                    remote_jsonl_path,
                )
                return

            logger.exception(
                "異常: 受信側パフォーマンスjsonlの削除に失敗しました。%d秒後に再試行します。対象=%s",
                sftp_report_constants.POLL_INTERVAL_SECONDS,
                remote_jsonl_path,
            )
            time.sleep(sftp_report_constants.POLL_INTERVAL_SECONDS)
            continue
        except Exception:
            logger.exception(
                "異常: 受信側パフォーマンスjsonlの削除に失敗しました。%d秒後に再試行します。対象=%s",
                sftp_report_constants.POLL_INTERVAL_SECONDS,
                remote_jsonl_path,
            )
            time.sleep(sftp_report_constants.POLL_INTERVAL_SECONDS)
            continue
        finally:
            close_sftp_connection(ssh_client, sftp_client)

        logger.info("終了: 受信側パフォーマンスjsonlの削除が完了しました。対象=%s", remote_jsonl_path)
        return
