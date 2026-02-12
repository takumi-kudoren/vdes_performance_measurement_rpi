"""受信集計レポートの作成処理を提供するモジュール。"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from udp.receive.udp_receiver import ReceivedTdbSentenceRecord


# 補助処理
def _normalize_utc_timestamp(timestamp_utc: datetime) -> datetime:
    """日時をUTCへ正規化する。

    引数:
        timestamp_utc: UTCとして扱う日時。

    戻り値:
        UTCへ正規化した日時。

    例外:
        なし。
    """
    if timestamp_utc.tzinfo is None:
        return timestamp_utc.replace(tzinfo=UTC)
    return timestamp_utc.astimezone(UTC)


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
    normalized_utc = _normalize_utc_timestamp(report_timestamp_utc)

    project_root = Path(__file__).resolve().parent.parent.parent
    report_directory = project_root / "report"
    report_directory.mkdir(parents=True, exist_ok=True)

    report_file_name = normalized_utc.strftime("REPORT_%Y%m%d_%H%M%S.jsonl")
    return report_directory / report_file_name


# メイン処理
def write_receive_report_jsonl(
    rx_tdb_count: int,
    rx_payload_chars_total: int,
    received_tdb_sentence_records: list[ReceivedTdbSentenceRecord],
    report_timestamp_utc: datetime | None = None,
) -> Path:
    """受信集計結果とセンテンス明細を1つのJSONLへ集約して書き出す。

    引数:
        rx_tdb_count: 受信したTDBセンテンス数。
        rx_payload_chars_total: 受信したpayload総文字数。
        received_tdb_sentence_records: 集計対象として採用したTDBセンテンス一覧。
        report_timestamp_utc: ファイル名へ埋め込むUTC時刻。未指定時は現在UTCを使用する。

    戻り値:
        出力したレポートファイルパス。

    例外:
        OSError: ファイル書き込みに失敗した場合。

    補足:
        1行目は集計サマリー、2行目以降はセンテンス明細を出力する。
        行種別は`record_type`で区別する。
    """
    if report_timestamp_utc is None:
        output_timestamp_utc = datetime.now(UTC)
    else:
        output_timestamp_utc = _normalize_utc_timestamp(report_timestamp_utc)

    report_file_path = _resolve_report_file_path(output_timestamp_utc)
    report_jsonl_lines: list[str] = []

    summary_record = {
        "record_type": "summary",
        "rx_tdb_count": rx_tdb_count,
        "rx_payload_chars_total": rx_payload_chars_total,
    }
    report_jsonl_lines.append(json.dumps(summary_record, ensure_ascii=False))

    for sentence_record in received_tdb_sentence_records:
        sentence_detail_record = {
            "record_type": "sentence",
            "sentence_received_utc": _format_utc_text(sentence_record.received_utc),
            "tdb_sentence": sentence_record.tdb_sentence,
        }
        report_jsonl_lines.append(json.dumps(sentence_detail_record, ensure_ascii=False))

    report_file_path.write_text("\n".join(report_jsonl_lines) + "\n", encoding="utf-8")
    return report_file_path


# 補助処理
def _format_utc_text(timestamp_utc: datetime) -> str:
    """UTC日時を`Z`付きISO形式へ変換する。

    引数:
        timestamp_utc: UTCとして扱う日時。

    戻り値:
        `YYYY-MM-DDTHH:MM:SS.sssZ`形式の文字列。

    例外:
        なし。
    """
    normalized_utc = _normalize_utc_timestamp(timestamp_utc)
    return normalized_utc.isoformat(timespec="milliseconds").replace("+00:00", "Z")
