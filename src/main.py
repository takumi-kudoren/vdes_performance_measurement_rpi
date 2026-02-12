"""RPi向け受信処理を起動するエントリーポイント。"""

from __future__ import annotations

import logging
import multiprocessing
import time
from datetime import UTC, datetime
from multiprocessing import queues
from pathlib import Path

from consts.receive_constants import RECEIVE_CYCLE_RESTART_DELAY_SECONDS
from reporting.receive_report import write_receive_report_jsonl
from sftp.report_uploader import upload_report_file_until_success
from udp.receive.udp_receiver import collect_receive_metrics_until_next_minute_boundary
from utils.logging_config import LOG_QUEUE_STOP_SIGNAL, configure_queue_logging, run_log_writer

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
        全処理のログを単一プロセスへ集約し、ログ行の混在を防ぐ。
    """
    configure_queue_logging(log_record_queue)


def delete_uploaded_report_file(local_report_file_path: Path) -> None:
    """送信済みレポートファイルをローカルから削除する。

    引数:
        local_report_file_path: 削除対象のローカルレポートファイルパス。

    戻り値:
        なし。

    例外:
        OSError: ファイル削除に失敗した場合。

    補足:
        アップロード済みファイルを残さない運用のため、送信完了後に削除する。
    """
    logger.info("開始: 送信済みレポートファイル削除を開始します。対象=%s", local_report_file_path)

    if not local_report_file_path.exists():
        logger.warning("異常: 削除対象のレポートファイルが存在しません。対象=%s", local_report_file_path)
        return

    try:
        local_report_file_path.unlink()
    except OSError:
        logger.exception("異常: 送信済みレポートファイルの削除に失敗しました。対象=%s", local_report_file_path)
        raise

    logger.info("終了: 送信済みレポートファイル削除が完了しました。対象=%s", local_report_file_path)


# メイン処理
def run_measurement_cycle(cycle_number: int) -> None:
    """連続運転の1サイクル分の受信計測と送信を実行する。

    引数:
        cycle_number: 連続運転サイクル番号。

    戻り値:
        なし。

    例外:
        Exception: 受信・レポート作成・アップロード処理で失敗した場合。

    補足:
        サイクル内の処理順は「受信集計→レポート作成→アップロード→ローカル削除」で固定する。
    """
    logger.info("開始: 連続運転サイクルを開始します。サイクル番号=%d", cycle_number)

    receive_metrics = collect_receive_metrics_until_next_minute_boundary()
    logger.info(
        "終了: 受信集計が完了しました。受信TDB数=%d 受信payload総文字数=%d",
        receive_metrics.rx_tdb_count,
        receive_metrics.rx_payload_chars_total,
    )

    report_timestamp_utc = datetime.now(UTC)

    logger.info("開始: 集約レポートjsonlファイル作成を開始します。")
    report_file_path = write_receive_report_jsonl(
        rx_tdb_count=receive_metrics.rx_tdb_count,
        rx_payload_chars_total=receive_metrics.rx_payload_chars_total,
        received_tdb_sentence_records=receive_metrics.received_tdb_sentence_records,
        report_timestamp_utc=report_timestamp_utc,
    )
    logger.info("終了: 集約レポートjsonlファイル作成が完了しました。出力先=%s", report_file_path)

    logger.info("開始: 集約レポートjsonlファイルのアップロードを開始します。ローカル=%s", report_file_path)
    remote_report_path = upload_report_file_until_success(report_file_path)
    logger.info("終了: 集約レポートjsonlファイルのアップロードが完了しました。アップロード先=%s", remote_report_path)
    delete_uploaded_report_file(report_file_path)
    logger.info("終了: 連続運転サイクルが完了しました。サイクル番号=%d", cycle_number)


# メイン処理
def main() -> None:
    """RPi向け受信処理を連続運転で起動して終了まで実行する。

    引数:
        なし。

    戻り値:
        なし。

    例外:
        なし。Ctrl+Cは捕捉して安全に終了する。

    補足:
        実行手順は「ログ初期化→サイクル実行→待機」をCtrl+Cまで繰り返す。
    """
    log_record_queue = multiprocessing.Queue()
    log_writer_process = multiprocessing.Process(
        target=run_log_writer, args=(log_record_queue,), name="log_writer_process"
    )
    log_writer_process.start()

    configure_logging(log_record_queue)
    logger.info("開始: 連続運転メイン処理を開始します。")

    try:
        cycle_number = 1
        while True:
            run_measurement_cycle(cycle_number)
            logger.info("開始: 次サイクル開始待機を開始します。待機秒数=%d", RECEIVE_CYCLE_RESTART_DELAY_SECONDS)
            time.sleep(RECEIVE_CYCLE_RESTART_DELAY_SECONDS)
            logger.info("終了: 次サイクル開始待機が完了しました。")
            cycle_number += 1
    except KeyboardInterrupt:
        logger.info("終了: Ctrl+Cを受信したため、連続運転を終了します。")
    except Exception:
        logger.exception("異常: 連続運転メイン処理で予期しない例外が発生しました。")
        raise
    finally:
        logger.info("終了: 連続運転メイン処理を終了します。")
        log_record_queue.put(LOG_QUEUE_STOP_SIGNAL)
        log_writer_process.join()


if __name__ == "__main__":
    main()
