"""UDPマルチキャスト受信処理を提供するモジュール。"""

from __future__ import annotations

import logging
import socket
from datetime import UTC, datetime
from multiprocessing import queues, synchronize
from queue import Full

from consts.receive_constants import (
    RECEIVE_BIND_IP,
    RECEIVE_BUFFER_SIZE,
    RECEIVE_MULTICAST_IP,
    RECEIVE_PORT,
    RECEIVE_SOCKET_TIMEOUT_SECONDS,
)
from udp.protocol.vetmk_parser import extract_vetmk_flag
from utils.logging_config import configure_queue_logging

logger = logging.getLogger(__name__)


# 補助処理
def _build_membership_request() -> bytes:
    """マルチキャスト参加用のパケットを生成する。

    引数:
        なし。

    戻り値:
        IP_ADD_MEMBERSHIP に渡す 8 バイトのパケット。

    例外:
        OSError: IP文字列の変換に失敗した場合に送出される。

    補足:
        受信インターフェースは 0.0.0.0 を指定し、OSの既定経路に従って参加する。
    """
    multicast_group = socket.inet_aton(RECEIVE_MULTICAST_IP)
    interface_address = socket.inet_aton("0.0.0.0")
    return multicast_group + interface_address


# 補助処理
def _record_vetmk_sentence(sentence_record_queue: queues.Queue, received_text: str) -> None:
    """VETMKを含む受信センテンスを report 用キューへ登録する。

    引数:
        sentence_record_queue: report 用のセンテンス記録キュー。
        received_text: 受信したセンテンス文字列。

    戻り値:
        なし。

    例外:
        なし。登録失敗時はログ出力して受信処理を継続する。

    補足:
        report 要件に合わせ、`$VETMK` を含む受信文のみを記録対象とする。
    """
    if "$VETMK" not in received_text:
        return

    try:
        sentence_record_queue.put((datetime.now(UTC), "受信", received_text))
    except Exception:
        logger.exception("異常: 受信センテンス記録の登録に失敗しました。")


# メイン処理
def run_multicast_udp_receive(
    stop_event: synchronize.Event,
    response_flag_queue: queues.Queue,
    log_record_queue: queues.Queue,
    sentence_record_queue: queues.Queue,
) -> None:
    """UDPマルチキャストデータを定期的に受信する。

    引数:
        stop_event: 停止要求を共有するイベント。
        response_flag_queue: 受信した VETMK 応答フラグを送信処理へ連携するキュー。
        log_record_queue: ログ専用プロセスへログレコードを送るキュー。
        sentence_record_queue: report 用のセンテンス記録キュー。

    戻り値:
        なし。

    例外:
        なし。受信時の OSError は捕捉してログへ出力し、処理を継続する。

    補足:
        停止要求の即時反映のため、受信ソケットにはタイムアウトを設定して待機する。
    """
    # Windows の spawn では子プロセスでロガー設定が引き継がれないため、
    # 子プロセス単体でもログ専用プロセスへ送信できるように初期化する。
    configure_queue_logging(log_record_queue)
    logger.info(
        "開始: UDPマルチキャスト受信処理を開始します。待受=%s:%s グループ=%s",
        RECEIVE_BIND_IP,
        RECEIVE_PORT,
        RECEIVE_MULTICAST_IP,
    )

    membership_request = b""

    try:
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_socket.bind((RECEIVE_BIND_IP, RECEIVE_PORT))
        membership_request = _build_membership_request()
        udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership_request)
        udp_socket.settimeout(RECEIVE_SOCKET_TIMEOUT_SECONDS)
    except OSError:
        logger.exception(
            "異常: UDPマルチキャスト受信ソケットの初期化に失敗しました。待受=%s:%s グループ=%s",
            RECEIVE_BIND_IP,
            RECEIVE_PORT,
            RECEIVE_MULTICAST_IP,
        )
        return

    with udp_socket:
        try:
            while True:
                if stop_event.is_set():
                    logger.info("終了: 停止要求を受信したため、UDPマルチキャスト受信処理を終了します。")
                    return

                try:
                    received_data, sender_address = udp_socket.recvfrom(RECEIVE_BUFFER_SIZE)
                except TimeoutError:
                    continue
                except OSError:
                    logger.exception(
                        "異常: UDPマルチキャスト受信に失敗しました。待受=%s:%s", RECEIVE_BIND_IP, RECEIVE_PORT
                    )
                    continue

                received_text = received_data.decode("utf-8", errors="replace")
                logger.info(
                    "終了: UDPマルチキャスト受信が完了しました。送信元=%s:%s バイト数=%d データ=%s",
                    sender_address[0],
                    sender_address[1],
                    len(received_data),
                    received_text,
                )
                _record_vetmk_sentence(sentence_record_queue, received_text)

                response_flag = extract_vetmk_flag(received_text)
                if response_flag is None:
                    continue

                try:
                    response_flag_queue.put_nowait(response_flag)
                except Full:
                    logger.warning("異常: 応答フラグ連携キューが満杯のため登録できませんでした。flag=%d", response_flag)
                else:
                    logger.info("終了: VETMK応答フラグを送信処理へ連携しました。flag=%d", response_flag)
        except KeyboardInterrupt:
            logger.info("終了: Ctrl+C を受信したため、UDPマルチキャスト受信処理を終了します。")
            return
        finally:
            if membership_request:
                try:
                    udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, membership_request)
                except OSError:
                    logger.exception(
                        "異常: マルチキャストグループ離脱に失敗しました。グループ=%s", RECEIVE_MULTICAST_IP
                    )
