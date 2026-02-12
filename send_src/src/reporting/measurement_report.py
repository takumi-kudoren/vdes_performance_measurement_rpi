"""計測レポート生成処理を提供するモジュール。"""

from __future__ import annotations

import logging
from collections import deque
from datetime import UTC, datetime
from multiprocessing import queues
from pathlib import Path
from queue import Empty

from udp.protocol.vetmk_parser import extract_vetmk_flag

logger = logging.getLogger(__name__)
SentenceRecord = tuple[datetime, str, str]


# 補助処理
def collect_sentence_records(sentence_record_queue: queues.Queue) -> list[SentenceRecord]:
    """送受信プロセスからセンテンス記録を回収して時系列順に並べる。

    引数:
        sentence_record_queue: 送受信センテンス記録を受け取るキュー。

    戻り値:
        時刻昇順に並んだセンテンス記録一覧。

    例外:
        なし。形式不正レコードはログ出力して破棄する。

    補足:
        report 出力は計測終了後に実施するため、non-blocking で全件回収してから整列する。
    """
    sentence_records: list[SentenceRecord] = []
    while True:
        try:
            raw_record = sentence_record_queue.get_nowait()
        except Empty:
            break

        if not isinstance(raw_record, tuple) or len(raw_record) != 3:
            logger.warning("異常: センテンス記録の形式が不正なため破棄しました。")
            continue

        record_timestamp_utc, sentence_type, sentence_text = raw_record
        if not isinstance(record_timestamp_utc, datetime):
            logger.warning("異常: センテンス記録の時刻形式が不正なため破棄しました。")
            continue
        if not isinstance(sentence_type, str):
            logger.warning("異常: センテンス記録の種別形式が不正なため破棄しました。")
            continue
        if not isinstance(sentence_text, str):
            logger.warning("異常: センテンス記録の本文形式が不正なため破棄しました。")
            continue

        sentence_records.append((record_timestamp_utc, sentence_type, sentence_text))

    sentence_records.sort(key=lambda record: record[0])
    return sentence_records


# 補助処理
def extract_payload_sixbit_char_count(send_data: str) -> int:
    """VATDBセンテンスから payload の6-bit文字数を抽出する。

    引数:
        send_data: 送信対象の VATDB センテンス文字列。

    戻り値:
        payload 部分の文字数。

    例外:
        ValueError: VATDB の必須要素数を満たさず payload を特定できない場合に送出する。

    補足:
        現行フォーマットでは payload が 9 番目の要素に固定されているため、
        区切り文字で分割して該当位置を取り出す。
    """
    sentence_parts = send_data.split(",")
    payload_index = 8
    if len(sentence_parts) <= payload_index:
        raise ValueError("VATDBセンテンスの形式が不正なため、payloadを抽出できません。")

    payload_text = sentence_parts[payload_index]
    return len(payload_text)


# 補助処理
def calculate_average_tmk1_latency_ms(sentence_records: list[SentenceRecord]) -> float | None:
    """送信から TMK.1 応答受信までの平均時間をミリ秒で算出する。

    引数:
        sentence_records: 時系列順の送受信センテンス記録一覧。

    戻り値:
        平均応答時間（ミリ秒）。TMK.1 応答がない場合は None。

    例外:
        なし。記録形式が不正な行は無視して計算を継続する。

    補足:
        送信と TMK.1 応答は FIFO で対応付け、最初に到着した TMK.1 を先頭送信へ割り当てる。
    """
    pending_send_timestamps: deque[datetime] = deque()
    latency_values_ms: list[float] = []

    for record_timestamp_utc, sentence_type, sentence_text in sentence_records:
        if sentence_type == "送信":
            pending_send_timestamps.append(record_timestamp_utc)
            continue

        if sentence_type != "受信":
            continue

        response_flag = extract_vetmk_flag(sentence_text)
        if response_flag != 1:
            continue

        if not pending_send_timestamps:
            continue

        sent_timestamp_utc = pending_send_timestamps.popleft()
        latency_milliseconds = (record_timestamp_utc - sent_timestamp_utc).total_seconds() * 1000
        latency_values_ms.append(latency_milliseconds)

    if not latency_values_ms:
        return None

    total_latency_ms = sum(latency_values_ms)
    return total_latency_ms / len(latency_values_ms)


# 補助処理
def format_utc_text(timestamp_utc: datetime) -> str:
    """UTC日時をレポート表示用の文字列へ変換する。

    引数:
        timestamp_utc: UTCとして扱う日時。

    戻り値:
        `YYYY-MM-DD HH:MM:SS UTC` 形式の文字列。

    例外:
        なし。

    補足:
        タイムゾーン未設定の日時は UTC とみなし、表示形式を統一する。
    """
    if timestamp_utc.tzinfo is None:
        normalized_utc = timestamp_utc.replace(tzinfo=UTC)
    else:
        normalized_utc = timestamp_utc.astimezone(UTC)

    return normalized_utc.strftime("%Y-%m-%d %H:%M:%S UTC")


# 補助処理
def format_utc_millisecond_text(timestamp_utc: datetime) -> str:
    """UTC日時をミリ秒精度の表示文字列へ変換する。

    引数:
        timestamp_utc: UTCとして扱う日時。

    戻り値:
        `YYYY-MM-DD HH:MM:SS.mmm UTC` 形式の文字列。

    例外:
        なし。

    補足:
        詳細欄では送受信イベントの前後関係を追えるよう、秒未満まで表示する。
    """
    if timestamp_utc.tzinfo is None:
        normalized_utc = timestamp_utc.replace(tzinfo=UTC)
    else:
        normalized_utc = timestamp_utc.astimezone(UTC)

    timestamp_text = normalized_utc.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return f"{timestamp_text} UTC"


# 補助処理
def build_report_file_path(report_timestamp_utc: datetime) -> Path:
    """reportフォルダ配下の基本レポートファイルパスを生成する。

    引数:
        report_timestamp_utc: ファイル名に埋め込むUTC時刻。

    戻り値:
        `REPORT_YYYYMMDD_HHMMSS.log` 形式のファイルパス。

    例外:
        OSError: report フォルダの作成に失敗した場合に送出される。

    補足:
        実行環境差異の影響を避けるため、プロジェクトルート基準で出力先を決定する。
    """
    project_root = Path(__file__).resolve().parent.parent.parent
    report_directory = project_root / "report"
    report_directory.mkdir(parents=True, exist_ok=True)
    report_timestamp = report_timestamp_utc.astimezone(UTC)
    report_file_name = report_timestamp.strftime("REPORT_%Y%m%d_%H%M%S.log")
    return report_directory / report_file_name


# 補助処理
def resolve_non_overwrite_path(base_path: Path) -> Path:
    """既存ファイルを上書きしない保存先パスを決定する。

    引数:
        base_path: 基本となる保存先ファイルパス。

    戻り値:
        既存ファイルと重複しないファイルパス。

    例外:
        なし。

    補足:
        同一秒に複数回出力された場合は `_01`、`_02` の連番を付与して退避する。
    """
    if not base_path.exists():
        return base_path

    base_stem = base_path.stem
    base_suffix = base_path.suffix
    sequence_number = 1
    while True:
        candidate_path = base_path.with_name(f"{base_stem}_{sequence_number:02d}{base_suffix}")
        if not candidate_path.exists():
            return candidate_path
        sequence_number += 1


# 補助処理
def write_measurement_report(
    selected_case: str,
    send_count: int,
    payload_total_char_count: int,
    rx_tdb_count: int,
    rx_payload_chars_total: int,
    send_start_utc: datetime,
    send_end_utc: datetime,
    average_tmk1_latency_ms: float | None,
    sentence_records: list[SentenceRecord],
) -> Path:
    """計測結果を report フォルダへ書き出す。

    引数:
        selected_case: 利用者が選択した送信ケース。
        send_count: 計測中に送信完了した TDB 数。
        payload_total_char_count: 送信 payload の総6-bit文字数。
        rx_tdb_count: 受信TDB数。
        rx_payload_chars_total: 受信成功payload総文字数。
        send_start_utc: 計測開始時刻（UTC）。
        send_end_utc: 計測終了時刻（UTC）。
        average_tmk1_latency_ms: 送信から TMK.1 応答までの平均時間（ミリ秒）。
        sentence_records: 計測中に記録した送受信センテンス一覧。

    戻り値:
        実際に出力したレポートファイルパス。

    例外:
        OSError: ファイル書き込みに失敗した場合に送出される。

    補足:
        ファイル名は送信開始時刻を基準にし、同名ファイルが存在する場合は別名で保存する。
        出力形式は「送信パフォーマンス」「受信パフォーマンス」「送受信センテンス詳細」の
        3セクション構成で固定する。
    """
    if send_start_utc.tzinfo is None:
        report_timestamp_utc = send_start_utc.replace(tzinfo=UTC)
    else:
        report_timestamp_utc = send_start_utc.astimezone(UTC)

    base_report_path = build_report_file_path(report_timestamp_utc)
    output_report_path = resolve_non_overwrite_path(base_report_path)

    report_lines = [
        "【送信パフォーマンス】",
        f"選択したCASE: {selected_case}",
        f"送信TDB数: {send_count}",
        f"送信成功payload総文字数: {payload_total_char_count}",
    ]
    if average_tmk1_latency_ms is None:
        report_lines.append("平均送信時間: N/A")
    else:
        report_lines.append(f"平均送信時間: {average_tmk1_latency_ms:.1f} ms")

    report_lines.extend(
        [
            f"送信開始・終了時刻（UTC）: {format_utc_text(send_start_utc)} ～ {format_utc_text(send_end_utc)}",
            "",
            "【受信パフォーマンス】",
            f"受信TDB数: {rx_tdb_count}",
            f"受信成功payload総文字数: {rx_payload_chars_total}",
            "",
            "【送受信センテンス詳細】",
        ]
    )

    if not sentence_records:
        report_lines.append("記録なし")
    else:
        for record_timestamp_utc, sentence_type, sentence_text in sentence_records:
            formatted_timestamp = format_utc_millisecond_text(record_timestamp_utc)
            report_lines.append(f"[{formatted_timestamp}] {sentence_type}: {sentence_text}")

    report_text = "\n".join(report_lines) + "\n"
    output_report_path.write_text(report_text, encoding="utf-8")
    return output_report_path
