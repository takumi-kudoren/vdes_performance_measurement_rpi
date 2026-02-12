"""ログの出力先設定を提供するモジュール。"""

from __future__ import annotations

import logging
from logging.handlers import QueueHandler
from multiprocessing import queues
from pathlib import Path

LOG_FILE_NAME = "vdes_performance_measurement_rpi.log"
LOG_OUTPUT_FORMAT = "%(asctime)s %(processName)s %(levelname)s %(name)s - %(message)s"
LOG_QUEUE_STOP_SIGNAL = None


# 補助処理
def _get_log_file_path() -> Path:
    """logsフォルダ配下のログファイルパスを返す。

    引数:
        なし。

    戻り値:
        ログ出力先のファイルパス。

    例外:
        なし。

    補足:
        実行環境差異を減らすため、プロジェクトルート基準で出力先を決定する。
    """
    project_root = Path(__file__).resolve().parent.parent.parent
    logs_directory = project_root / "logs"
    logs_directory.mkdir(parents=True, exist_ok=True)
    return logs_directory / LOG_FILE_NAME


# メイン処理
def configure_queue_logging(log_record_queue: queues.Queue) -> None:
    """ログをキュー経由でログ専用プロセスへ送信するよう設定する。

    引数:
        log_record_queue: ログレコードをログ専用プロセスへ送るキュー。

    戻り値:
        なし。

    例外:
        なし。

    補足:
        各処理が直接ファイルへ書き込まず、1プロセスへ集約して混在を防ぐ。
    """
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(QueueHandler(log_record_queue))


# メイン処理
def run_log_writer(log_record_queue: queues.Queue) -> None:
    """キューに集約したログレコードをファイルへ書き込む。

    引数:
        log_record_queue: 各処理が送信するログレコードを受信するキュー。

    戻り値:
        なし。

    例外:
        なし。内部例外時は自プロセスで最低限のログを残して継続する。

    補足:
        ログ停止シグナルを受信するまで待機し、受信後に安全に終了する。
    """
    log_file_path = _get_log_file_path()
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_OUTPUT_FORMAT,
        filename=str(log_file_path),
        filemode="w",
        encoding="utf-8",
        force=True,
    )

    while True:
        log_record = log_record_queue.get()
        if log_record is LOG_QUEUE_STOP_SIGNAL:
            break

        try:
            logging.getLogger(log_record.name).handle(log_record)
        except Exception:
            logging.getLogger(__name__).exception("異常: ログレコードの書き込みに失敗しました。")
