"""パス解決の共通補助関数を提供する。"""

from __future__ import annotations

from pathlib import Path


def get_project_root() -> Path:
    """プロジェクトルートパスを返す。

    引数:
        なし。

    戻り値:
        このリポジトリのルートディレクトリパス。

    例外:
        なし。
    """
    return Path(__file__).resolve().parent.parent.parent
