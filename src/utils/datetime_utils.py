"""日時処理の共通補助関数を提供する。"""

from __future__ import annotations

from datetime import UTC, datetime


def normalize_utc_datetime(timestamp_utc: datetime) -> datetime:
    """日時をUTCへ正規化する。

    引数:
        timestamp_utc: UTCとして扱う日時。

    戻り値:
        UTCへ正規化した日時。

    例外:
        なし。

    補足:
        タイムゾーン未指定日時は既存挙動に合わせてUTCとして扱う。
    """
    if timestamp_utc.tzinfo is None:
        return timestamp_utc.replace(tzinfo=UTC)
    return timestamp_utc.astimezone(UTC)
