"""UDP応答駆動送信処理を提供するモジュール。"""

from __future__ import annotations

import logging
import socket
from datetime import UTC, datetime
from multiprocessing import queues, synchronize
from queue import Empty

from consts.send_constants import SEND_TARGET_IP, SEND_TARGET_PORT
from utils.logging_config import configure_queue_logging

logger = logging.getLogger(__name__)
RESPONSE_FLAG_WAIT_TIMEOUT_SECONDS = 0.2


# 補助処理
def _notify_send_count(send_count_queue: queues.Queue, send_count: int) -> None:
    """送信回数を親プロセスへ通知する。

    引数:
        send_count_queue: 送信回数を親プロセスへ渡すためのキュー。
        send_count: 正常に送信できた回数。

    戻り値:
        なし。

    例外:
        なし。通知失敗時はログ出力のみを行い、例外は送出しない。

    補足:
        停止時にも結果を確認できるよう、終了直前に必ず回数を通知する。
    """
    try:
        send_count_queue.put(send_count)
    except Exception:
        logger.exception("異常: 送信回数の通知に失敗しました。送信回数=%d", send_count)


# 補助処理
def _clear_response_flag_queue(response_flag_queue: queues.Queue) -> None:
    """応答キューの残件を破棄する。

    引数:
        response_flag_queue: 受信プロセスから受け取った応答フラグを保持するキュー。

    戻り値:
        なし。

    例外:
        なし。キューが空になるまで読み出し、残件がない状態を保証する。

    補足:
        送信前に前回の応答を破棄しておくことで、今回送信分の判定と混在しないようにする。
    """
    discarded_count = 0
    while True:
        try:
            response_flag_queue.get_nowait()
        except Empty:
            break
        discarded_count += 1

    if discarded_count > 0:
        logger.info("開始: 応答キューの残件を破棄しました。破棄件数=%d", discarded_count)


# 補助処理
def _record_sentence(sentence_record_queue: queues.Queue, sentence_type: str, sentence_text: str) -> None:
    """送受信センテンス記録キューへ1件登録する。

    引数:
        sentence_record_queue: report 用のセンテンス記録キュー。
        sentence_type: 記録種別（送信/受信）。
        sentence_text: 記録対象のセンテンス本文。

    戻り値:
        なし。

    例外:
        なし。登録失敗時はログ出力して処理を継続する。

    補足:
        report 生成はメインプロセスで実施するため、子プロセスでは事実データの登録のみを行う。
    """
    try:
        sentence_record_queue.put((datetime.now(UTC), sentence_type, sentence_text))
    except Exception:
        logger.exception("異常: センテンス記録の登録に失敗しました。種別=%s", sentence_type)


# 補助処理
def _wait_for_next_send_trigger(stop_event: synchronize.Event, response_flag_queue: queues.Queue) -> int | None:
    """次回送信を開始できる応答フラグを待機する。

    引数:
        stop_event: 停止要求を共有するイベント。
        response_flag_queue: 受信プロセスから受け取った応答フラグを保持するキュー。

    戻り値:
        次回送信のトリガーとなる flag 値。停止要求を受信した場合は None。

    例外:
        なし。停止要求を優先し、キュー待機の継続可否を内部で制御する。

    補足:
        flag=0 はキュー登録完了通知を示すため、送信完了通知が来るまで待機を継続する。
    """
    while True:
        if stop_event.is_set():
            return None

        try:
            response_flag = response_flag_queue.get(timeout=RESPONSE_FLAG_WAIT_TIMEOUT_SECONDS)
        except Empty:
            continue

        if response_flag == 0:
            logger.info("開始: VETMK応答を受信しました。flag=0 のため送信完了待機を継続します。")
            continue

        return response_flag


# メイン処理
def run_periodic_udp_send(
    stop_event: synchronize.Event,
    send_count_queue: queues.Queue,
    response_flag_queue: queues.Queue,
    log_record_queue: queues.Queue,
    send_data: str,
    sentence_record_queue: queues.Queue,
) -> None:
    """UDPデータを応答駆動で送信する。

    引数:
        stop_event: 停止要求を共有するイベント。
        send_count_queue: 送信回数を親プロセスへ渡すためのキュー。
        response_flag_queue: 受信プロセスから受け取った応答フラグを保持するキュー。
        log_record_queue: ログ専用プロセスへログレコードを送るキュー。
        send_data: 送信するセンテンス文字列。
        sentence_record_queue: report 用のセンテンス記録キュー。

    戻り値:
        なし。

    例外:
        なし。送信時の OSError は捕捉してログへ出力し、処理を継続する。

    補足:
        送信完了の判定は VETMK 応答の flag=1 を受信したタイミングで行う。
    """
    # Windows の spawn では子プロセスでロガー設定が引き継がれないため、
    # 子プロセス単体でもログ専用プロセスへ送信できるように初期化する。
    configure_queue_logging(log_record_queue)

    logger.info("開始: UDP応答駆動送信処理を開始します。送信先=%s:%s", SEND_TARGET_IP, SEND_TARGET_PORT)
    send_data_bytes = send_data.encode("utf-8")
    sent_count = 0

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_socket:
            while True:
                if stop_event.is_set():
                    logger.info("終了: 停止要求を受信したため、UDP応答駆動送信処理を終了します。")
                    return

                _clear_response_flag_queue(response_flag_queue)

                try:
                    udp_socket.sendto(send_data_bytes, (SEND_TARGET_IP, SEND_TARGET_PORT))
                except OSError:
                    logger.exception(
                        "異常: UDPデータ送信に失敗しました。送信先=%s:%s", SEND_TARGET_IP, SEND_TARGET_PORT
                    )
                    continue

                _record_sentence(sentence_record_queue, "送信", send_data)
                logger.info(
                    "終了: UDPデータ送信が完了しました。送信先=%s:%s バイト数=%d",
                    SEND_TARGET_IP,
                    SEND_TARGET_PORT,
                    len(send_data_bytes),
                )

                response_flag = _wait_for_next_send_trigger(stop_event, response_flag_queue)
                if response_flag is None:
                    logger.info("終了: 停止要求を受信したため、UDP応答駆動送信処理を終了します。")
                    return

                if response_flag == 1:
                    sent_count += 1
                    logger.info(
                        "終了: VETMK応答で送信完了を確認しました。flag=%d 送信回数=%d", response_flag, sent_count
                    )
                    continue

                logger.warning(
                    "異常: VETMK応答で想定外のflagを受信しました。flag=%d 次の送信へ進みます。", response_flag
                )
    except KeyboardInterrupt:
        logger.info("終了: Ctrl+C を受信したため、UDP応答駆動送信処理を終了します。")
        return
    finally:
        _notify_send_count(send_count_queue, sent_count)
