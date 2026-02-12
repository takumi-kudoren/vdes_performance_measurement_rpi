"""SFTP接続の確立と終了処理を提供する。"""

import logging
from pathlib import Path

import paramiko

from consts import sftp_constants

LOGGER = logging.getLogger(__name__)


# メイン処理
def open_sftp_connection() -> tuple[paramiko.SSHClient, paramiko.SFTPClient]:
    """SFTP接続を確立してクライアントを返す。

    引数:
        なし。

    戻り値:
        SSHクライアントとSFTPクライアントのタプル。

    例外:
        ValueError: 設定値が不正な場合。
        FileNotFoundError: 秘密鍵ファイルが存在しない場合。
        paramiko.SSHException: 接続に失敗した場合。
        OSError: ソケット接続に失敗した場合。
    """
    ssh_client = open_ssh_connection()

    LOGGER.info("開始: SFTPセッションを開始します。")

    try:
        sftp_client = ssh_client.open_sftp()
    except Exception:
        LOGGER.exception("異常: SFTPセッションの開始に失敗しました。")
        ssh_client.close()
        raise

    LOGGER.info("終了: SFTPセッションの開始が完了しました。")
    return ssh_client, sftp_client


def close_sftp_connection(ssh_client: paramiko.SSHClient | None, sftp_client: paramiko.SFTPClient | None) -> None:
    """SFTP接続を終了する。

    引数:
        ssh_client: SSHクライアント。
        sftp_client: SFTPクライアント。

    戻り値:
        なし。

    例外:
        なし。
    """
    if sftp_client is not None:
        try:
            sftp_client.close()
        except Exception:
            LOGGER.exception("異常: SFTPセッションの終了に失敗しました。")

    if ssh_client is not None:
        try:
            ssh_client.close()
        except Exception:
            LOGGER.exception("異常: SSH接続の終了に失敗しました。")

    LOGGER.info("終了: SFTP接続を終了しました。")


def open_ssh_connection() -> paramiko.SSHClient:
    """SSH接続を確立してクライアントを返す。

    引数:
        なし。

    戻り値:
        SSHクライアント。

    例外:
        ValueError: 設定値が不正な場合。
        FileNotFoundError: 秘密鍵ファイルが存在しない場合。
        paramiko.SSHException: 接続に失敗した場合。
        OSError: ソケット接続に失敗した場合。
    """
    validate_sftp_config()

    host = sftp_constants.HOST
    port = sftp_constants.PORT
    username = sftp_constants.USERNAME
    private_key_path = resolve_private_key_path(sftp_constants.PRIVATE_KEY_PATH)
    passphrase = sftp_constants.PRIVATE_KEY_PASSPHRASE
    timeout_seconds = sftp_constants.CONNECT_TIMEOUT_SECONDS

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    LOGGER.info("開始: SSH接続を開始します。host=%s, port=%s, user=%s", host, port, username)

    try:
        ssh_client.connect(
            hostname=host,
            port=port,
            username=username,
            key_filename=str(private_key_path),
            passphrase=passphrase,
            timeout=timeout_seconds,
            allow_agent=False,
            look_for_keys=False,
        )
    except Exception:
        LOGGER.exception("異常: SSH接続に失敗しました。host=%s, port=%s, user=%s", host, port, username)
        ssh_client.close()
        raise

    LOGGER.info("終了: SSH接続が完了しました。")
    return ssh_client


# 補助処理
def validate_sftp_config() -> None:
    """SFTP接続設定の妥当性を確認する。

    引数:
        なし。

    戻り値:
        なし。

    例外:
        ValueError: 設定値が不正な場合。
    """
    if not sftp_constants.HOST:
        raise ValueError("SFTP接続先のホストが未指定です。")

    if sftp_constants.PORT <= 0 or sftp_constants.PORT > 65535:
        raise ValueError(f"SFTP接続先のポートが範囲外です。port={sftp_constants.PORT}")

    if not sftp_constants.USERNAME:
        raise ValueError("SFTP接続ユーザー名が未指定です。")

    if not sftp_constants.PRIVATE_KEY_PATH:
        raise ValueError("秘密鍵ファイルのパスが未指定です。")


def resolve_private_key_path(private_key_path: str) -> Path:
    """秘密鍵ファイルのパスを解決する。

    引数:
        private_key_path: 秘密鍵ファイルのパス。

    戻り値:
        解決済みのパス。

    例外:
        ValueError: 秘密鍵ファイルの形式が不正な場合。
        FileNotFoundError: 秘密鍵ファイルが存在しない場合。

    補足:
        パスの展開にはチルダも含めて解決する。
    """
    expanded_path = Path(private_key_path).expanduser()

    if expanded_path.suffix.lower() == ".ppk":
        raise ValueError("PPK形式の秘密鍵には未対応です。OpenSSH形式の鍵を指定してください。")

    if not expanded_path.exists():
        raise FileNotFoundError(f"秘密鍵ファイルが存在しません。path={expanded_path}")

    return expanded_path
