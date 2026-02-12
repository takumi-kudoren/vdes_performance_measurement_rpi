"""計測開始時刻の境界制御処理を提供するモジュール。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta


# 補助処理
def format_utc_log_text(timestamp_utc: datetime) -> str:
    """UTC日時をログ表示用の文字列へ変換する。

    引数:
        timestamp_utc: UTCとして扱う日時。

    戻り値:
        `YYYY-MM-DD HH:MM:SS UTC` 形式の文字列。

    例外:
        なし。

    補足:
        ログ上で時刻比較しやすいよう、秒精度のUTC表記へ統一する。
    """
    if timestamp_utc.tzinfo is None:
        normalized_utc = timestamp_utc.replace(tzinfo=UTC)
    else:
        normalized_utc = timestamp_utc.astimezone(UTC)

    return normalized_utc.strftime("%Y-%m-%d %H:%M:%S UTC")


# 補助処理
def resolve_measurement_start_boundary_utc(current_utc: datetime) -> datetime:
    """計測開始に使用するUTC分境界時刻を決定する。

    引数:
        current_utc: 判定基準となる現在UTC時刻。

    戻り値:
        計測開始に採用する分境界時刻。

    例外:
        なし。

    補足:
        既に分境界ちょうどの場合はその時刻を返し、境界外の場合は次の分境界を返す。
    """
    if current_utc.tzinfo is None:
        normalized_utc = current_utc.replace(tzinfo=UTC)
    else:
        normalized_utc = current_utc.astimezone(UTC)

    if normalized_utc.second == 0 and normalized_utc.microsecond == 0:
        return normalized_utc

    current_minute_floor = normalized_utc.replace(second=0, microsecond=0)
    return current_minute_floor + timedelta(minutes=1)


# 補助処理
def calculate_wait_seconds_until_boundary(target_boundary_utc: datetime, current_utc: datetime) -> float:
    """指定した分境界までの待機秒数を算出する。

    引数:
        target_boundary_utc: 目標となるUTC分境界時刻。
        current_utc: 算出基準となる現在UTC時刻。

    戻り値:
        待機が必要な秒数。既に到達済みの場合は 0.0。

    例外:
        なし。

    補足:
        時刻差が負になる場合は待機不要のため 0.0 に丸める。
    """
    if target_boundary_utc.tzinfo is None:
        normalized_target_utc = target_boundary_utc.replace(tzinfo=UTC)
    else:
        normalized_target_utc = target_boundary_utc.astimezone(UTC)

    if current_utc.tzinfo is None:
        normalized_current_utc = current_utc.replace(tzinfo=UTC)
    else:
        normalized_current_utc = current_utc.astimezone(UTC)

    wait_seconds = (normalized_target_utc - normalized_current_utc).total_seconds()
    if wait_seconds < 0:
        return 0.0

    return wait_seconds
