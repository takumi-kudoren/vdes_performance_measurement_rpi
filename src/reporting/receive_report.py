"""受信集計レポートの作成処理を提供するモジュール。"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path


# 補助処理
def _resolve_report_file_path(report_timestamp_utc: datetime) -> Path:
    """受信レポートの出力先ファイルパスを生成する。

    引数:
        report_timestamp_utc: ファイル名へ埋め込むUTC時刻。

    戻り値:
        `REPORT_YYYYMMDD_HHMMSS.jsonl`形式の出力先パス。

    例外:
        OSError: 出力ディレクトリ作成に失敗した場合。

    補足:
        保存先は`<project_root>/report`で固定する。
    """
    if report_timestamp_utc.tzinfo is None:
        normalized_utc = report_timestamp_utc.replace(tzinfo=UTC)
    else:
        normalized_utc = report_timestamp_utc.astimezone(UTC)

    project_root = Path(__file__).resolve().parent.parent.parent
    report_directory = project_root / "report"
    report_directory.mkdir(parents=True, exist_ok=True)

    report_file_name = normalized_utc.strftime("REPORT_%Y%m%d_%H%M%S.jsonl")
    return report_directory / report_file_name


# メイン処理
def write_receive_report_jsonl(
    rx_tdb_count: int, rx_payload_chars_total: int, report_timestamp_utc: datetime | None = None
) -> Path:
    """受信集計結果を1行JSONLで書き出す。

    引数:
        rx_tdb_count: 受信したTDBセンテンス数。
        rx_payload_chars_total: 受信したpayload総文字数。
        report_timestamp_utc: ファイル名へ埋め込むUTC時刻。未指定時は現在UTCを使用する。

    戻り値:
        出力したレポートファイルパス。

    例外:
        OSError: ファイル書き込みに失敗した場合。

    補足:
        JSONL本文は1行のみとし、項目は2つに固定する。
    """
    if report_timestamp_utc is None:
        output_timestamp_utc = datetime.now(UTC)
    else:
        if report_timestamp_utc.tzinfo is None:
            output_timestamp_utc = report_timestamp_utc.replace(tzinfo=UTC)
        else:
            output_timestamp_utc = report_timestamp_utc.astimezone(UTC)

    report_file_path = _resolve_report_file_path(output_timestamp_utc)
    report_record = {"rx_tdb_count": rx_tdb_count, "rx_payload_chars_total": rx_payload_chars_total}
    report_line = json.dumps(report_record, ensure_ascii=False)
    report_file_path.write_text(report_line + "\n", encoding="utf-8")
    return report_file_path
