"""UDP受信とTDB集計処理を提供するモジュール。"""

from __future__ import annotations

import logging
import socket
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from consts.receive_constants import (
    RECEIVE_BIND_IP,
    RECEIVE_BUFFER_SIZE,
    RECEIVE_MULTICAST_IP,
    RECEIVE_PORT,
    RECEIVE_SOCKET_TIMEOUT_SECONDS,
)

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
    first_tdb_received_utc: datetime
    measurement_end_utc: datetime
    received_tdb_sentence_records: list[ReceivedTdbSentenceRecord]


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
    interface_address = socket.inet_aton("0.0.0.0")
    return multicast_group + interface_address


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
    if timestamp_utc.tzinfo is None:
        normalized_utc = timestamp_utc.replace(tzinfo=UTC)
    else:
        normalized_utc = timestamp_utc.astimezone(UTC)

    return normalized_utc.strftime("%Y-%m-%d %H:%M:%S UTC")


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
    if timestamp_utc.tzinfo is None:
        normalized_utc = timestamp_utc.replace(tzinfo=UTC)
    else:
        normalized_utc = timestamp_utc.astimezone(UTC)

    current_minute_floor_utc = normalized_utc.replace(second=0, microsecond=0)
    return current_minute_floor_utc + timedelta(minutes=1)


# 補助処理
def _extract_target_vatdb_sentences(received_text: str) -> list[str]:
    """受信テキストから`!VATDB`を含むセンテンス行を抽出する。

    引数:
        received_text: UDP受信データをUTF-8復号した文字列。

    戻り値:
        `!VATDB`を含むセンテンス行一覧。

    例外:
        なし。

    補足:
        1データグラムに複数行を含む場合へ対応するため、行単位で判定する。
    """
    target_sentences: list[str] = []
    for raw_line in received_text.splitlines():
        sentence_start_index = raw_line.find("!VATDB")
        if sentence_start_index < 0:
            continue

        vatdb_sentence = raw_line[sentence_start_index:].strip()
        if not vatdb_sentence:
            continue
        target_sentences.append(vatdb_sentence)

    return target_sentences


# 補助処理
def _extract_payload_char_count(vatdb_sentence: str) -> int | None:
    """`!VATDB`センテンスからpayload文字数を抽出する。

    引数:
        vatdb_sentence: `!VATDB`を含む1行のセンテンス文字列。

    戻り値:
        payload文字数。形式不正の場合はNone。

    例外:
        なし。形式不正時は警告ログを出してNoneを返す。

    補足:
        payloadは`*`より前の9項目目（index=8）を採用する。
    """
    sentence_body = vatdb_sentence.split("*", 1)[0]
    sentence_fields = sentence_body.split(",")
    payload_index = 8

    if len(sentence_fields) <= payload_index:
        logger.warning(
            "異常: VATDBセンテンスの項目数が不足しているため集計対象から除外しました。受信データ=%s", vatdb_sentence
        )
        return None

    return len(sentence_fields[payload_index])


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
        最初の有効`!VATDB`を受信するまでは無期限で待機する。
    """
    logger.info(
        "開始: UDPマルチキャスト受信処理を開始します。待受=%s:%s グループ=%s",
        RECEIVE_BIND_IP,
        RECEIVE_PORT,
        RECEIVE_MULTICAST_IP,
    )

    membership_request = b""
    rx_tdb_count = 0
    rx_payload_chars_total = 0
    first_tdb_received_utc: datetime | None = None
    measurement_end_utc: datetime | None = None
    received_tdb_sentence_records: list[ReceivedTdbSentenceRecord] = []

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

    with udp_socket:
        try:
            while True:
                current_utc = datetime.now(UTC)
                if measurement_end_utc is not None and current_utc >= measurement_end_utc:
                    logger.info(
                        "終了: 計測終了時刻に到達したため受信集計を終了します。終了時刻=%s",
                        _format_utc_log_text(measurement_end_utc),
                    )
                    break

                try:
                    received_data, sender_address = udp_socket.recvfrom(RECEIVE_BUFFER_SIZE)
                except TimeoutError:
                    continue
                except OSError:
                    logger.exception("異常: UDP受信に失敗しました。待受=%s:%s", RECEIVE_BIND_IP, RECEIVE_PORT)
                    continue

                received_text = received_data.decode("utf-8", errors="replace")
                target_sentences = _extract_target_vatdb_sentences(received_text)
                if not target_sentences:
                    continue

                logger.info(
                    "開始: TDB候補データを受信しました。送信元=%s:%s 件数=%d",
                    sender_address[0],
                    sender_address[1],
                    len(target_sentences),
                )

                for vatdb_sentence in target_sentences:
                    sentence_received_utc = datetime.now(UTC)
                    if measurement_end_utc is not None and sentence_received_utc >= measurement_end_utc:
                        logger.info(
                            "終了: 計測終了時刻到達後のデータを検知したため受信集計を終了します。終了時刻=%s",
                            _format_utc_log_text(measurement_end_utc),
                        )
                        break

                    payload_char_count = _extract_payload_char_count(vatdb_sentence)
                    if payload_char_count is None:
                        continue

                    if first_tdb_received_utc is None:
                        first_tdb_received_utc = sentence_received_utc
                        measurement_end_utc = _resolve_next_minute_boundary_utc(first_tdb_received_utc)
                        logger.info(
                            "開始: 最初のTDB受信を検知したため計測を開始します。開始時刻=%s 終了時刻=%s",
                            _format_utc_log_text(first_tdb_received_utc),
                            _format_utc_log_text(measurement_end_utc),
                        )

                    rx_tdb_count += 1
                    rx_payload_chars_total += payload_char_count
                    received_tdb_sentence_records.append(
                        ReceivedTdbSentenceRecord(received_utc=sentence_received_utc, tdb_sentence=vatdb_sentence)
                    )
                else:
                    continue
                break
        finally:
            if membership_request:
                try:
                    udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, membership_request)
                except OSError:
                    logger.exception(
                        "異常: マルチキャストグループ離脱に失敗しました。グループ=%s", RECEIVE_MULTICAST_IP
                    )

    if first_tdb_received_utc is None or measurement_end_utc is None:
        raise RuntimeError("受信集計結果を確定できませんでした。")

    logger.info(
        "終了: UDPマルチキャスト受信処理が完了しました。受信TDB数=%d 受信payload総文字数=%d",
        rx_tdb_count,
        rx_payload_chars_total,
    )
    return ReceiveMetrics(
        rx_tdb_count=rx_tdb_count,
        rx_payload_chars_total=rx_payload_chars_total,
        first_tdb_received_utc=first_tdb_received_utc,
        measurement_end_utc=measurement_end_utc,
        received_tdb_sentence_records=received_tdb_sentence_records,
    )
