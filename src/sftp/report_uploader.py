"""受信レポートのSFTPアップロード処理を提供するモジュール。"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import paramiko

from consts import sftp_report_constants
from sftp.client import close_sftp_connection, open_sftp_connection

logger = logging.getLogger(__name__)


# 補助処理
def _join_remote_path(directory_path: str, file_name: str) -> str:
    """サーバーディレクトリとファイル名からフルパスを生成する。

    引数:
        directory_path: サーバー上のディレクトリパス。
        file_name: ファイル名。

    戻り値:
        サーバー上のフルパス。

    例外:
        なし。
    """
    return f"{directory_path.rstrip('/')}/{file_name}"


# メイン処理
def upload_report_file_until_success(local_report_file_path: Path) -> str:
    """受信レポートをSFTPでアップロードするまで再試行する。

    引数:
        local_report_file_path: アップロード対象のローカルレポートパス。

    戻り値:
        アップロード先のサーバー上フルパス。

    例外:
        FileNotFoundError: ローカルレポートが存在しない場合。

    補足:
        サーバー接続や転送の失敗時は、成功するまで無期限で再試行する。
    """
    if not local_report_file_path.exists():
        raise FileNotFoundError(f"アップロード対象のレポートが存在しません。path={local_report_file_path}")

    remote_report_path = _join_remote_path(
        sftp_report_constants.REMOTE_REPORT_JSON_DIRECTORY,
        local_report_file_path.name,
    )

    logger.info(
        "開始: 受信レポートのSFTPアップロードを開始します。ローカル=%s リモート=%s",
        local_report_file_path,
        remote_report_path,
    )

    while True:
        ssh_client: paramiko.SSHClient | None = None
        sftp_client: paramiko.SFTPClient | None = None

        try:
            ssh_client, sftp_client = open_sftp_connection()
            sftp_client.put(str(local_report_file_path), remote_report_path)
        except Exception:
            logger.exception(
                "異常: 受信レポートのアップロードに失敗しました。%d秒後に再試行します。ローカル=%s リモート=%s",
                sftp_report_constants.POLL_INTERVAL_SECONDS,
                local_report_file_path,
                remote_report_path,
            )
            time.sleep(sftp_report_constants.POLL_INTERVAL_SECONDS)
            continue
        finally:
            close_sftp_connection(ssh_client, sftp_client)

        logger.info("終了: 受信レポートのSFTPアップロードが完了しました。リモート=%s", remote_report_path)
        return remote_report_path
