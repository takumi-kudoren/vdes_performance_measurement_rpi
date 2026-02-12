"""分境界開始で1分間TDBセンテンスを送信する手動テストスクリプト。"""

from __future__ import annotations

import logging
import socket
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

LOGGER = logging.getLogger(__name__)

SEND_COUNT_PER_MINUTE = 60
SEND_INTERVAL_SECONDS = 1


# 補助処理
def _load_receive_destination() -> tuple[str, int]:
    """受信設定から送信先マルチキャスト情報を読み込む。

    引数:
        なし。

    戻り値:
        送信先マルチキャストIPとポート番号。

    例外:
        ModuleNotFoundError: `src`配下モジュールの読み込みに失敗した場合。
    """
    project_root = Path(__file__).resolve().parent.parent
    src_directory = project_root / "src"
    src_directory_text = str(src_directory)

    if src_directory_text not in sys.path:
        sys.path.insert(0, src_directory_text)

    from consts.receive_constants import RECEIVE_MULTICAST_IP, RECEIVE_PORT

    return RECEIVE_MULTICAST_IP, RECEIVE_PORT


# 補助処理
def _resolve_next_minute_boundary_utc(now_utc: datetime) -> datetime:
    """現在時刻から次の分境界UTC時刻を求める。

    引数:
        now_utc: 現在時刻として扱うUTC日時。

    戻り値:
        次の分境界UTC時刻。

    例外:
        なし。
    """
    if now_utc.tzinfo is None:
        normalized_utc = now_utc.replace(tzinfo=UTC)
    else:
        normalized_utc = now_utc.astimezone(UTC)

    current_minute_floor = normalized_utc.replace(second=0, microsecond=0)
    return current_minute_floor + timedelta(minutes=1)


# 補助処理
def _sleep_until_utc(target_utc: datetime) -> None:
    """指定UTC時刻になるまで待機する。

    引数:
        target_utc: 待機終了時刻のUTC日時。

    戻り値:
        なし。

    例外:
        なし。
    """
    if target_utc.tzinfo is None:
        normalized_target_utc = target_utc.replace(tzinfo=UTC)
    else:
        normalized_target_utc = target_utc.astimezone(UTC)

    while True:
        remaining_seconds = (normalized_target_utc - datetime.now(UTC)).total_seconds()
        if remaining_seconds <= 0:
            break

        # 境界直前のずれを減らすため、短い単位で分割して待機する。
        time.sleep(min(remaining_seconds, 0.2))


# 補助処理
def _format_utc_text(timestamp_utc: datetime) -> str:
    """UTC日時をログ向け文字列へ変換する。

    引数:
        timestamp_utc: 変換対象のUTC日時。

    戻り値:
        `YYYY-MM-DDTHH:MM:SS.ffffffZ`形式の文字列。

    例外:
        なし。
    """
    if timestamp_utc.tzinfo is None:
        normalized_utc = timestamp_utc.replace(tzinfo=UTC)
    else:
        normalized_utc = timestamp_utc.astimezone(UTC)

    return normalized_utc.isoformat(timespec="microseconds").replace("+00:00", "Z")


# 補助処理
def _build_test_vatdb_sentence(sequence: int) -> str:
    """テスト送信用のTDBセンテンスを生成する。

    引数:
        sequence: 送信連番（0始まり）。

    戻り値:
        受信集計条件を満たす`!VATDB`センテンス文字列。

    例外:
        ValueError: 連番が0〜59の範囲外の場合。
    """
    if sequence < 0 or sequence >= SEND_COUNT_PER_MINUTE:
        raise ValueError(f"送信連番は0〜59で指定してください。sequence={sequence}")

    payload_text = f"TEST_PAYLOAD_{sequence:02d}"
    return f"!VATDB,0,0,0,0,0,0,0,{payload_text}*00"


# メイン処理
def send_tdb_for_one_minute() -> None:
    """分境界を起点に1分間、1秒間隔でTDBセンテンスを送信する。

    引数:
        なし。

    戻り値:
        なし。

    例外:
        OSError: UDP送信処理に失敗した場合。
    """
    multicast_ip, port = _load_receive_destination()
    next_minute_boundary_utc = _resolve_next_minute_boundary_utc(datetime.now(UTC))

    LOGGER.info(
        "開始: 分境界送信テストを開始します。送信先=%s:%d 分境界=%s",
        multicast_ip,
        port,
        _format_utc_text(next_minute_boundary_utc),
    )

    _sleep_until_utc(next_minute_boundary_utc)
    LOGGER.info("開始: 分境界に到達したためTDBセンテンス送信を開始します。")

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as udp_socket:
        udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)

        for sequence in range(SEND_COUNT_PER_MINUTE):
            scheduled_send_utc = next_minute_boundary_utc + timedelta(seconds=sequence * SEND_INTERVAL_SECONDS)
            _sleep_until_utc(scheduled_send_utc)

            tdb_sentence = _build_test_vatdb_sentence(sequence)
            udp_socket.sendto(tdb_sentence.encode("utf-8"), (multicast_ip, port))
            LOGGER.info(
                "終了: TDBセンテンス送信が完了しました。連番=%02d/%02d 送信時刻=%s データ=%s",
                sequence + 1,
                SEND_COUNT_PER_MINUTE,
                _format_utc_text(datetime.now(UTC)),
                tdb_sentence,
            )

    LOGGER.info("終了: 分境界送信テストが完了しました。送信件数=%d", SEND_COUNT_PER_MINUTE)


def main() -> None:
    """テストスクリプトの実行入口。"""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    try:
        send_tdb_for_one_minute()
    except KeyboardInterrupt:
        LOGGER.info("終了: Ctrl+Cを受け付けたため分境界送信テストを終了します。")
    except Exception:
        LOGGER.exception("異常: 分境界送信テストで例外が発生しました。")
        raise


if __name__ == "__main__":
    main()
