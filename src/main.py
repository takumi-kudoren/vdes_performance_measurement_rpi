"""RPi向け受信処理を起動するエントリーポイント。"""

from __future__ import annotations

import logging
import multiprocessing
from multiprocessing import queues

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


# メイン処理
def main() -> None:
    """RPi向け受信処理を起動して終了まで実行する。

    引数:
        なし。

    戻り値:
        なし。

    例外:
        なし。Ctrl+Cは捕捉して安全に終了する。

    補足:
        実行手順は「ログ初期化→受信集計→レポート作成→SFTPアップロード」で固定する。
    """
    log_record_queue = multiprocessing.Queue()
    log_writer_process = multiprocessing.Process(
        target=run_log_writer, args=(log_record_queue,), name="log_writer_process"
    )
    log_writer_process.start()

    configure_logging(log_record_queue)
    logger.info("開始: メイン処理を開始します。")

    try:
        receive_metrics = collect_receive_metrics_until_next_minute_boundary()
        logger.info(
            "終了: 受信集計が完了しました。受信TDB数=%d 受信payload総文字数=%d",
            receive_metrics.rx_tdb_count,
            receive_metrics.rx_payload_chars_total,
        )

        report_file_path = write_receive_report_jsonl(
            rx_tdb_count=receive_metrics.rx_tdb_count,
            rx_payload_chars_total=receive_metrics.rx_payload_chars_total,
        )
        logger.info("終了: レポートファイル作成が完了しました。出力先=%s", report_file_path)

        remote_report_path = upload_report_file_until_success(report_file_path)
        logger.info("終了: レポートファイルのアップロードが完了しました。アップロード先=%s", remote_report_path)
    except KeyboardInterrupt:
        logger.info("終了: Ctrl+Cを受信したため、メイン処理を終了します。")
    except Exception:
        logger.exception("異常: メイン処理で予期しない例外が発生しました。")
        raise
    finally:
        logger.info("終了: メイン処理を終了します。")
        log_record_queue.put(LOG_QUEUE_STOP_SIGNAL)
        log_writer_process.join()


if __name__ == "__main__":
    main()
