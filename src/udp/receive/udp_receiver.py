"""UDP受信とTDB集計処理を提供するモジュール。"""

from __future__ import annotations

import logging
import socket
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from consts.receive_constants import (
    RECEIVE_BIND_IP,
    RECEIVE_BUFFER_SIZE,
    RECEIVE_INTERFACE_IP,
    RECEIVE_MULTICAST_IP,
    RECEIVE_PORT,
    RECEIVE_SOCKET_TIMEOUT_SECONDS,
)
from consts.tdb_constants import (
    NMEA_CHECKSUM_SEPARATOR,
    NMEA_FIELD_SEPARATOR,
    NMEA_SENTENCE_START_MARK,
    TDB_FIELD_PAYLOAD_POSITION,
    TDB_TOKEN_LENGTH,
    TDB_TOKEN_SUFFIX,
)
from consts.udp_receiver_constants import UDP_DECODE_ENCODING, UDP_DECODE_ERROR_MODE, UTC_LOG_TEXT_FORMAT
from reporting.tdb_sentence_reassembler import TdbSentenceReassembler
from utils.datetime_utils import normalize_utc_datetime

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReceivedTdbSentenceRecord:
    """集計対象として採用したTDBセンテンスの受信情報を保持する。

    引数:
        received_utc: センテンスを受信したUTC時刻。
        tdb_sentence: 受信したTDBセンテンス文字列。

    戻り値:
        なし。

    例外:
        なし。
    """

    received_utc: datetime
    tdb_sentence: str


@dataclass(frozen=True)
class ReceiveMetrics:
    """受信集計の結果を保持する。

    引数:
        rx_tdb_count: 計測期間中に受信したTDB件数。
        rx_payload_chars_total: 計測期間中に受信したpayload総文字数。
        split_reconstruct_success_count: 分割グループの再構成成功件数。
        split_reconstruct_failure_count: 分割グループの再構成失敗件数。
        first_tdb_received_utc: 最初の有効TDBを受信したUTC時刻。
        measurement_end_utc: 計測終了のUTC時刻。
        received_tdb_sentence_records: 集計対象として採用したTDBセンテンス一覧。

    戻り値:
        なし。

    例外:
        なし。
    """

    rx_tdb_count: int
    rx_payload_chars_total: int
    split_reconstruct_success_count: int
    split_reconstruct_failure_count: int
    first_tdb_received_utc: datetime
    measurement_end_utc: datetime
    received_tdb_sentence_records: list[ReceivedTdbSentenceRecord]


@dataclass
class _ReceiveAggregationState:
    """受信ループ中に更新する集計状態を保持する。"""

    rx_tdb_count: int = 0
    rx_payload_chars_total: int = 0
    first_tdb_received_utc: datetime | None = None
    measurement_end_utc: datetime | None = None
    received_tdb_sentence_records: list[ReceivedTdbSentenceRecord] = field(default_factory=list)
    tdb_sentence_reassembler: TdbSentenceReassembler = field(default_factory=TdbSentenceReassembler)


# 補助処理
def _build_membership_request() -> bytes:
    """マルチキャスト参加に使用するパケットを組み立てる。

    引数:
        なし。

    戻り値:
        `IP_ADD_MEMBERSHIP`に設定する8バイトのパケット。

    例外:
        OSError: IPアドレス変換に失敗した場合。
    """
    multicast_group = socket.inet_aton(RECEIVE_MULTICAST_IP)
    interface_address = socket.inet_aton(RECEIVE_INTERFACE_IP)
    return multicast_group + interface_address


# 補助処理
def _current_utc() -> datetime:
    """現在のUTC時刻を返す。"""
    return datetime.now(UTC)


# 補助処理
def _format_utc_log_text(timestamp_utc: datetime) -> str:
    """UTC日時をログ表示向け文字列へ変換する。

    引数:
        timestamp_utc: UTCとして扱う日時。

    戻り値:
        `YYYY-MM-DD HH:MM:SS UTC` 形式の文字列。

    例外:
        なし。
    """
    normalized_utc = normalize_utc_datetime(timestamp_utc)
    return normalized_utc.strftime(UTC_LOG_TEXT_FORMAT)


# 補助処理
def _resolve_next_minute_boundary_utc(timestamp_utc: datetime) -> datetime:
    """指定時刻が属する分の次境界をUTCで返す。

    引数:
        timestamp_utc: 基準となるUTC時刻。

    戻り値:
        次の分境界UTC時刻。

    例外:
        なし。
    """
    normalized_utc = normalize_utc_datetime(timestamp_utc)
    current_minute_floor_utc = normalized_utc.replace(second=0, microsecond=0)
    return current_minute_floor_utc + timedelta(minutes=1)


# 補助処理
def _is_tdb_sentence_token(token: str) -> bool:
    """センテンス種別トークンが`!--TDB`形式か判定する。

    引数:
        token: `!`の直後から`,`までのセンテンス種別トークン。

    戻り値:
        `!--TDB`形式の場合はTrue。その他はFalse。

    例外:
        なし。
    """
    normalized_token = token.strip()
    return len(normalized_token) == TDB_TOKEN_LENGTH and normalized_token.endswith(TDB_TOKEN_SUFFIX)


# 補助処理
def _extract_target_tdb_sentences(received_text: str) -> list[str]:
    """受信テキストから`!--TDB`形式のセンテンス行を抽出する。

    引数:
        received_text: UDP受信データをUTF-8復号した文字列。

    戻り値:
        `!--TDB`形式のセンテンス行一覧。

    例外:
        なし。

    補足:
        1データグラムに複数行を含む場合へ対応するため、行単位で判定する。
    """
    target_sentences: list[str] = []
    for raw_line in received_text.splitlines():
        sentence_start_index = raw_line.find(NMEA_SENTENCE_START_MARK)
        while sentence_start_index >= 0:
            comma_index = raw_line.find(NMEA_FIELD_SEPARATOR, sentence_start_index)
            if comma_index < 0:
                break

            sentence_token = raw_line[sentence_start_index + 1 : comma_index]
            if _is_tdb_sentence_token(sentence_token):
                tdb_sentence = raw_line[sentence_start_index:].strip()
                if tdb_sentence:
                    target_sentences.append(tdb_sentence)
                break

            sentence_start_index = raw_line.find(NMEA_SENTENCE_START_MARK, sentence_start_index + 1)

    return target_sentences


# 補助処理
def _extract_payload_char_count(tdb_sentence: str) -> int | None:
    """`!--TDB`センテンスからpayload文字数を抽出する。

    引数:
        tdb_sentence: `!--TDB`形式の1行センテンス文字列。

    戻り値:
        payload文字数。形式不正の場合はNone。

    例外:
        なし。形式不正時は警告ログを出してNoneを返す。

    補足:
        payloadは`*`より前の9項目目（index=8）を採用する。
    """
    sentence_body = tdb_sentence.split(NMEA_CHECKSUM_SEPARATOR, 1)[0]
    sentence_fields = sentence_body.split(NMEA_FIELD_SEPARATOR)

    if len(sentence_fields) <= TDB_FIELD_PAYLOAD_POSITION:
        logger.warning(
            "異常: TDBセンテンスの項目数が不足しているため集計対象から除外しました。受信データ=%s", tdb_sentence
        )
        return None

    return len(sentence_fields[TDB_FIELD_PAYLOAD_POSITION])


# 補助処理
def _start_measurement_window_if_needed(
    first_tdb_received_utc: datetime | None, measurement_end_utc: datetime | None, sentence_received_utc: datetime
) -> tuple[datetime | None, datetime | None]:
    """未開始の場合のみ計測開始時刻と終了時刻を確定する。

    引数:
        first_tdb_received_utc: 現在の計測開始時刻。未開始時はNone。
        measurement_end_utc: 現在の計測終了時刻。未開始時はNone。
        sentence_received_utc: 今回の受信時刻。

    戻り値:
        計測開始時刻と計測終了時刻のタプル。

    例外:
        なし。

    補足:
        計測開始判定は再構築結果ではなく、最初に受信した`!--TDB`行で行う。
    """
    if first_tdb_received_utc is not None and measurement_end_utc is not None:
        return first_tdb_received_utc, measurement_end_utc

    first_tdb_received_utc = sentence_received_utc
    measurement_end_utc = _resolve_next_minute_boundary_utc(first_tdb_received_utc)
    logger.info(
        "開始: 最初のTDB受信を検知したため計測を開始します。開始時刻=%s 終了時刻=%s",
        _format_utc_log_text(first_tdb_received_utc),
        _format_utc_log_text(measurement_end_utc),
    )
    return first_tdb_received_utc, measurement_end_utc


# 補助処理
def _reassemble_target_tdb_sentence(
    tdb_sentence: str, sentence_received_utc: datetime, tdb_sentence_reassembler: TdbSentenceReassembler
) -> list[str]:
    """受信したTDBセンテンスを再構築し、出力対象センテンスを返す。

    引数:
        tdb_sentence: 受信した`!--TDB`センテンス。
        sentence_received_utc: センテンス受信時刻。
        tdb_sentence_reassembler: 再構築状態を保持する再構築器。

    戻り値:
        集計対象として採用するセンテンス一覧。

    例外:
        なし。再構築失敗時は警告ログを出して空配列を返す。
    """
    try:
        return tdb_sentence_reassembler.reassemble_sentences([tdb_sentence], sentence_received_utc)
    except Exception as exc:
        logger.warning(
            "異常: TDB分割センテンスの再構築に失敗したため受信データを破棄します。sentence=%s reason=%s",
            tdb_sentence,
            exc,
        )
        return []


# 補助処理
def _accumulate_reassembled_sentences(
    reassembled_sentences: list[str],
    sentence_received_utc: datetime,
    rx_tdb_count: int,
    rx_payload_chars_total: int,
    received_tdb_sentence_records: list[ReceivedTdbSentenceRecord],
) -> tuple[int, int]:
    """再構築後センテンスを集計へ反映する。

    引数:
        reassembled_sentences: 再構築済みのセンテンス一覧。
        sentence_received_utc: 集計に採用する受信時刻。
        rx_tdb_count: 現在の受信TDB件数。
        rx_payload_chars_total: 現在のpayload総文字数。
        received_tdb_sentence_records: 明細として保持するセンテンス一覧。

    戻り値:
        更新後の受信TDB件数とpayload総文字数。

    例外:
        なし。形式不正センテンスは警告ログのうえ破棄する。
    """
    for reassembled_sentence in reassembled_sentences:
        payload_char_count = _extract_payload_char_count(reassembled_sentence)
        if payload_char_count is None:
            continue

        rx_tdb_count += 1
        rx_payload_chars_total += payload_char_count
        received_tdb_sentence_records.append(
            ReceivedTdbSentenceRecord(received_utc=sentence_received_utc, tdb_sentence=reassembled_sentence)
        )

    return rx_tdb_count, rx_payload_chars_total


# 補助処理
def _initialize_udp_socket() -> tuple[socket.socket, bytes]:
    """UDP受信ソケットを初期化し、参加要求パケットと併せて返す。"""
    membership_request = b""
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    try:
        udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_socket.bind((RECEIVE_BIND_IP, RECEIVE_PORT))
        membership_request = _build_membership_request()
        udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership_request)
        udp_socket.settimeout(RECEIVE_SOCKET_TIMEOUT_SECONDS)
    except OSError:
        udp_socket.close()
        logger.exception(
            "異常: UDP受信ソケットの初期化に失敗しました。待受=%s:%s グループ=%s",
            RECEIVE_BIND_IP,
            RECEIVE_PORT,
            RECEIVE_MULTICAST_IP,
        )
        raise

    return udp_socket, membership_request


# 補助処理
def _is_measurement_end_reached(measurement_end_utc: datetime | None, current_utc: datetime) -> bool:
    """計測終了時刻へ到達したか判定する。"""
    if measurement_end_utc is None:
        return False
    return current_utc >= measurement_end_utc


# 補助処理
def _receive_datagram(udp_socket: socket.socket) -> tuple[bytes, tuple[str, int]] | None:
    """1データグラム受信を行い、受信失敗時はNoneを返す。"""
    try:
        return udp_socket.recvfrom(RECEIVE_BUFFER_SIZE)
    except TimeoutError:
        return None
    except OSError:
        logger.exception("異常: UDP受信に失敗しました。待受=%s:%s", RECEIVE_BIND_IP, RECEIVE_PORT)
        return None


# 補助処理
def _process_target_tdb_sentence(tdb_sentence: str, state: _ReceiveAggregationState) -> bool:
    """1件のTDBセンテンスを処理し、計測終了時はTrueを返す。"""
    sentence_received_utc = _current_utc()
    if _is_measurement_end_reached(state.measurement_end_utc, sentence_received_utc):
        logger.info(
            "終了: 計測終了時刻到達後のデータを検知したため受信集計を終了します。終了時刻=%s",
            _format_utc_log_text(state.measurement_end_utc),
        )
        return True

    state.first_tdb_received_utc, state.measurement_end_utc = _start_measurement_window_if_needed(
        first_tdb_received_utc=state.first_tdb_received_utc,
        measurement_end_utc=state.measurement_end_utc,
        sentence_received_utc=sentence_received_utc,
    )

    reassembled_sentences = _reassemble_target_tdb_sentence(
        tdb_sentence=tdb_sentence,
        sentence_received_utc=sentence_received_utc,
        tdb_sentence_reassembler=state.tdb_sentence_reassembler,
    )
    if not reassembled_sentences:
        return False

    state.rx_tdb_count, state.rx_payload_chars_total = _accumulate_reassembled_sentences(
        reassembled_sentences=reassembled_sentences,
        sentence_received_utc=sentence_received_utc,
        rx_tdb_count=state.rx_tdb_count,
        rx_payload_chars_total=state.rx_payload_chars_total,
        received_tdb_sentence_records=state.received_tdb_sentence_records,
    )
    return False


# 補助処理
def _process_received_datagram(
    received_data: bytes, sender_address: tuple[str, int], state: _ReceiveAggregationState
) -> bool:
    """1データグラムを処理し、計測終了時はTrueを返す。"""
    received_text = received_data.decode(UDP_DECODE_ENCODING, errors=UDP_DECODE_ERROR_MODE)
    target_sentences = _extract_target_tdb_sentences(received_text)
    if not target_sentences:
        return False

    logger.info(
        "開始: TDB候補データを受信しました。送信元=%s:%s 件数=%d",
        sender_address[0],
        sender_address[1],
        len(target_sentences),
    )

    for tdb_sentence in target_sentences:
        should_stop = _process_target_tdb_sentence(tdb_sentence, state)
        if should_stop:
            return True
    return False


# 補助処理
def _run_receive_loop(udp_socket: socket.socket, state: _ReceiveAggregationState) -> None:
    """受信ループを実行し、終了条件に到達するまで処理を継続する。"""
    while True:
        current_utc = _current_utc()
        if _is_measurement_end_reached(state.measurement_end_utc, current_utc):
            logger.info(
                "終了: 計測終了時刻に到達したため受信集計を終了します。終了時刻=%s",
                _format_utc_log_text(state.measurement_end_utc),
            )
            return

        received_packet = _receive_datagram(udp_socket)
        if received_packet is None:
            continue

        received_data, sender_address = received_packet
        should_stop = _process_received_datagram(received_data, sender_address, state)
        if should_stop:
            return


# 補助処理
def _leave_multicast_group_if_needed(udp_socket: socket.socket, membership_request: bytes) -> None:
    """マルチキャスト参加済みの場合のみグループ離脱を試行する。"""
    if not membership_request:
        return

    try:
        udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, membership_request)
    except OSError:
        logger.exception("異常: マルチキャストグループ離脱に失敗しました。グループ=%s", RECEIVE_MULTICAST_IP)


# 補助処理
def _build_receive_metrics(state: _ReceiveAggregationState) -> ReceiveMetrics:
    """受信ループ完了後の状態から集計結果オブジェクトを構築する。"""
    if state.first_tdb_received_utc is None or state.measurement_end_utc is None:
        raise RuntimeError("受信集計結果を確定できませんでした。")

    state.tdb_sentence_reassembler.finalize_pending_groups_as_boundary_failure()
    split_reconstruct_success_count, split_reconstruct_failure_count = (
        state.tdb_sentence_reassembler.get_split_reconstruct_counts()
    )

    logger.info(
        "終了: UDPマルチキャスト受信処理が完了しました。受信TDB数=%d 受信payload総文字数=%d "
        "分割再構成成功数=%d 分割再構成失敗数=%d",
        state.rx_tdb_count,
        state.rx_payload_chars_total,
        split_reconstruct_success_count,
        split_reconstruct_failure_count,
    )
    return ReceiveMetrics(
        rx_tdb_count=state.rx_tdb_count,
        rx_payload_chars_total=state.rx_payload_chars_total,
        split_reconstruct_success_count=split_reconstruct_success_count,
        split_reconstruct_failure_count=split_reconstruct_failure_count,
        first_tdb_received_utc=state.first_tdb_received_utc,
        measurement_end_utc=state.measurement_end_utc,
        received_tdb_sentence_records=state.received_tdb_sentence_records,
    )


# メイン処理
def collect_receive_metrics_until_next_minute_boundary() -> ReceiveMetrics:
    """TDB受信を集計し、初回受信分の次分境界で終了する。

    引数:
        なし。

    戻り値:
        受信件数・payload総文字数・集計対象センテンス一覧を含む集計結果。

    例外:
        OSError: ソケット初期化に失敗した場合。
        RuntimeError: 計測結果の確定前に予期しない分岐へ到達した場合。

    補足:
        最初の有効`!--TDB`を受信するまでは無期限で待機する。
    """
    logger.info(
        "開始: UDPマルチキャスト受信処理を開始します。待受=%s:%s グループ=%s",
        RECEIVE_BIND_IP,
        RECEIVE_PORT,
        RECEIVE_MULTICAST_IP,
    )

    state = _ReceiveAggregationState()
    udp_socket, membership_request = _initialize_udp_socket()

    with udp_socket:
        try:
            _run_receive_loop(udp_socket, state)
        finally:
            _leave_multicast_group_if_needed(udp_socket, membership_request)

    return _build_receive_metrics(state)
