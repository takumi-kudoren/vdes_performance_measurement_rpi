"""UDP送受信プロセスを起動するエントリーポイント。"""

from __future__ import annotations

import logging
import multiprocessing
import time
from datetime import UTC, datetime, timedelta
from multiprocessing import queues
from queue import Empty

from consts.send_constants import SEND_DATA_BY_CASE, SEND_MEASUREMENT_DURATION_SECONDS
from reporting import (
    calculate_average_tmk1_latency_ms,
    collect_sentence_records,
    extract_payload_sixbit_char_count,
    write_measurement_report,
)
from sftp.receive_performance_collector import (
    delete_referenced_jsonl_until_success,
    wait_latest_receive_performance_summary,
)
from udp.receive.udp_receiver import run_multicast_udp_receive
from udp.send.udp_sender import run_periodic_udp_send
from utils.logging_config import LOG_QUEUE_STOP_SIGNAL, configure_queue_logging, run_log_writer
from utils.measurement_timing import (
    calculate_wait_seconds_until_boundary,
    format_utc_log_text,
    resolve_measurement_start_boundary_utc,
)

logger = logging.getLogger(__name__)


# 補助処理
def configure_logging(log_record_queue: queues.Queue) -> None:
    """ログの基本設定を行う。

    引数:
        log_record_queue: ログレコードをログ専用プロセスへ送るキュー。

    戻り値:
        なし。

    例外:
        なし。

    補足:
        全プロセスのログを単一プロセスへ集約し、ファイル書き込みの混在を防ぐ。
    """
    configure_queue_logging(log_record_queue)


# 補助処理
def collect_send_count(send_count_queue: queues.Queue, send_process: multiprocessing.Process) -> int:
    """送信プロセスから送信回数を受け取る。

    引数:
        send_count_queue: 送信回数を受け取るキュー。
        send_process: 送信プロセス。

    戻り値:
        送信回数。取得できない場合は 0。

    例外:
        なし。取得失敗時はログ出力して 0 を返す。

    補足:
        送信処理の終了理由に関係なく結果表示を行うため、失敗時も継続可能な戻り値とする。
    """
    try:
        return send_count_queue.get_nowait()
    except Empty:
        logger.warning("異常: 送信回数を取得できませんでした。送信プロセス終了コード=%s", send_process.exitcode)
        return 0


# 補助処理
def prompt_send_data_case() -> str:
    """送信センテンスのケースを対話入力で確定する。

    引数:
        なし。

    戻り値:
        確定した送信ケース。値は "A" / "B" / "C" のいずれか。

    例外:
        なし。入力値が不正な場合は再入力を要求して継続する。

    補足:
        ログ設定前でも確実に利用者へ案内を出せるよう、標準入出力で案内する。
    """
    while True:
        selected_case = input("送信センテンスを選択してください（A/B/C）: ").strip().upper()
        if selected_case in SEND_DATA_BY_CASE:
            return selected_case

        print("入力が不正です。A、B、C のいずれかを入力してください。")


# メイン処理
def main() -> None:
    """UDP送受信プロセスを起動して終了まで待機する。

    引数:
        なし。

    戻り値:
        なし。

    例外:
        なし。Ctrl+C は捕捉して停止処理へ切り替える。

    補足:
        Windows の spawn 方式で安全に起動できるよう、処理は関数化して呼び出す。
    """
    selected_case = prompt_send_data_case()
    selected_send_data = SEND_DATA_BY_CASE[selected_case]

    log_record_queue = multiprocessing.Queue()
    log_writer_process = multiprocessing.Process(
        target=run_log_writer, args=(log_record_queue,), name="log_writer_process"
    )
    log_writer_process.start()

    configure_logging(log_record_queue)
    logger.info("開始: メイン処理を開始します。")
    logger.info("開始: 送信センテンスのケースを確定しました。")
    payload_per_tdb_char_count = extract_payload_sixbit_char_count(selected_send_data)

    measurement_start_boundary_utc = resolve_measurement_start_boundary_utc(datetime.now(UTC))
    current_utc = datetime.now(UTC)
    wait_seconds = calculate_wait_seconds_until_boundary(measurement_start_boundary_utc, current_utc)
    boundary_text = format_utc_log_text(measurement_start_boundary_utc)
    if wait_seconds == 0:
        logger.info("開始: UTC分境界ちょうどのため待機せず開始します。開始時刻=%s", boundary_text)
    else:
        current_text = format_utc_log_text(current_utc)
        logger.info(
            "開始: UTC分境界まで待機します。現在=%s 開始予定=%s 待機秒数=%.3f",
            current_text,
            boundary_text,
            wait_seconds,
        )

        try:
            time.sleep(wait_seconds)
        except KeyboardInterrupt:
            logger.info("終了: UTC分境界待機中にCtrl+Cを受信したため、計測を開始せず終了します。")
            logger.info("終了: 境界待機を中断したため、メイン処理を終了します。")
            log_record_queue.put(LOG_QUEUE_STOP_SIGNAL)
            log_writer_process.join()
            return

        logger.info("終了: UTC分境界への待機が完了しました。開始時刻=%s", boundary_text)

    stop_event = multiprocessing.Event()
    send_count_queue = multiprocessing.Queue()
    response_flag_queue = multiprocessing.Queue()
    sentence_record_queue = multiprocessing.Queue()
    send_process = multiprocessing.Process(
        target=run_periodic_udp_send,
        args=(
            stop_event,
            send_count_queue,
            response_flag_queue,
            log_record_queue,
            selected_send_data,
            sentence_record_queue,
        ),
        name="udp_send_process",
    )
    receive_process = multiprocessing.Process(
        target=run_multicast_udp_receive,
        args=(stop_event, response_flag_queue, log_record_queue, sentence_record_queue),
        name="udp_receive_process",
    )

    send_start_utc = measurement_start_boundary_utc
    measurement_end_utc = send_start_utc + timedelta(seconds=SEND_MEASUREMENT_DURATION_SECONDS)

    logger.info("開始: UDP送信プロセスを起動します。")
    send_process.start()
    logger.info("開始: UDP受信プロセスを起動します。")
    receive_process.start()

    interrupted_during_measurement = False
    try:
        logger.info(
            "開始: 計測を開始します。計測時間=%.1f秒 開始時刻=%s 終了予定時刻=%s",
            SEND_MEASUREMENT_DURATION_SECONDS,
            format_utc_log_text(send_start_utc),
            format_utc_log_text(measurement_end_utc),
        )
        while True:
            now_utc = datetime.now(UTC)
            remaining_seconds = (measurement_end_utc - now_utc).total_seconds()
            if remaining_seconds <= 0:
                logger.info("終了: 計測時間が経過したため、全処理を停止します。")
                break

            if stop_event.wait(remaining_seconds):
                logger.info("終了: 停止要求を受信したため、全処理を停止します。")
                break
    except KeyboardInterrupt:
        interrupted_during_measurement = True
        logger.info("異常: Ctrl+C を受信したため、送受信プロセスに停止要求を送信します。")
    finally:
        stop_event.set()
        if interrupted_during_measurement:
            send_end_utc = datetime.now(UTC)
        else:
            send_end_utc = measurement_end_utc
        send_process.join()
        receive_process.join()
        sentence_records = collect_sentence_records(sentence_record_queue)
        send_count = collect_send_count(send_count_queue, send_process)
        payload_total_char_count = send_count * payload_per_tdb_char_count
        average_tmk1_latency_ms = calculate_average_tmk1_latency_ms(sentence_records)
        receive_performance_summary = wait_latest_receive_performance_summary()
        logger.info("開始: 計測レポート出力を開始します。")
        try:
            report_file_path = write_measurement_report(
                selected_case=selected_case,
                send_count=send_count,
                payload_total_char_count=payload_total_char_count,
                rx_tdb_count=receive_performance_summary.rx_tdb_count,
                rx_payload_chars_total=receive_performance_summary.rx_payload_chars_total,
                send_start_utc=send_start_utc,
                send_end_utc=send_end_utc,
                average_tmk1_latency_ms=average_tmk1_latency_ms,
                sentence_records=sentence_records,
            )
        except Exception:
            logger.exception("異常: 計測レポート出力に失敗しました。送信TDB数=%d", send_count)
        else:
            logger.info("終了: 計測レポート出力が完了しました。出力先=%s", report_file_path)
            delete_referenced_jsonl_until_success(receive_performance_summary.source_remote_jsonl_path)
        logger.info("終了: メイン処理を終了します。")
        log_record_queue.put(LOG_QUEUE_STOP_SIGNAL)
        log_writer_process.join()


if __name__ == "__main__":
    main()
