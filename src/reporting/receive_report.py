"""受信集計レポートの作成処理を提供するモジュール。"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from consts import receive_report_constants
from udp.receive.udp_receiver import ReceivedTdbSentenceRecord
from utils.datetime_utils import normalize_utc_datetime
from utils.path_utils import get_project_root


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
    normalized_utc = normalize_utc_datetime(report_timestamp_utc)

    project_root = get_project_root()
    report_directory = project_root / receive_report_constants.REPORT_DIRECTORY_NAME
    report_directory.mkdir(parents=True, exist_ok=True)

    report_file_name = normalized_utc.strftime(receive_report_constants.REPORT_FILE_NAME_FORMAT)
    return report_directory / report_file_name


# メイン処理
def write_receive_report_jsonl(
    rx_tdb_count: int,
    rx_payload_chars_total: int,
    split_reconstruct_success_count: int,
    split_reconstruct_failure_count: int,
    received_tdb_sentence_records: list[ReceivedTdbSentenceRecord],
    report_timestamp_utc: datetime | None = None,
) -> Path:
    """受信集計結果とセンテンス明細を1つのJSONLへ集約して書き出す。

    引数:
        rx_tdb_count: 受信したTDBセンテンス数。
        rx_payload_chars_total: 受信したpayload総文字数。
        split_reconstruct_success_count: 分割グループの再構成成功件数。
        split_reconstruct_failure_count: 分割グループの再構成失敗件数。
        received_tdb_sentence_records: 再構築後を含む集計対象として採用したTDBセンテンス一覧。
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
        output_timestamp_utc = normalize_utc_datetime(report_timestamp_utc)

    report_file_path = _resolve_report_file_path(output_timestamp_utc)
    report_jsonl_lines: list[str] = []

    summary_record = {
        "record_type": receive_report_constants.SUMMARY_RECORD_TYPE,
        "rx_tdb_count": rx_tdb_count,
        "rx_payload_chars_total": rx_payload_chars_total,
        # 一時対応: 再構成成功数/失敗数はJSONLへ記載しない。
        # "split_reconstruct_success_count": split_reconstruct_success_count,
        # "split_reconstruct_failure_count": split_reconstruct_failure_count,
    }
    report_jsonl_lines.append(json.dumps(summary_record, ensure_ascii=False))

    for sentence_record in received_tdb_sentence_records:
        sentence_detail_record = {
            "record_type": receive_report_constants.SENTENCE_RECORD_TYPE,
            "sentence_received_utc": _format_utc_text(sentence_record.received_utc),
            "tdb_sentence": sentence_record.tdb_sentence,
        }
        report_jsonl_lines.append(json.dumps(sentence_detail_record, ensure_ascii=False))

    report_file_path.write_text(
        receive_report_constants.JSONL_LINE_SEPARATOR.join(report_jsonl_lines)
        + receive_report_constants.JSONL_LINE_SEPARATOR,
        encoding=receive_report_constants.JSONL_ENCODING,
    )
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
    normalized_utc = normalize_utc_datetime(timestamp_utc)
    return normalized_utc.isoformat(timespec=receive_report_constants.UTC_TEXT_TIMESPEC).replace(
        receive_report_constants.UTC_OFFSET_TEXT, receive_report_constants.UTC_SUFFIX_TEXT
    )
