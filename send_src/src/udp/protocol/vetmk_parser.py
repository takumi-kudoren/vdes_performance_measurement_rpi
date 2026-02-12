"""VETMK 応答解析処理を提供するモジュール。"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def extract_vetmk_flag(received_text: str) -> int | None:
    """受信文字列から VETMK 応答の flag を抽出する。

    引数:
        received_text: UTF-8 で復号した受信文字列。

    戻り値:
        抽出に成功した flag。抽出できない場合は None。

    例外:
        なし。形式不正時は理由をログへ出力して None を返す。

    補足:
        仕様対象は `$VETMK,0,<flag>*CS` のみとし、それ以外は送信トリガーに利用しない。
    """
    sentence_start_index = received_text.find("$VETMK")
    if sentence_start_index < 0:
        return None

    vetmk_sentence = received_text[sentence_start_index:].splitlines()[0]
    vetmk_body = vetmk_sentence.split("*", 1)[0]
    sentence_fields = vetmk_body.split(",")

    if len(sentence_fields) < 3:
        logger.warning("異常: VETMK応答の項目数が不足しているため解析できません。受信データ=%s", vetmk_sentence.strip())
        return None

    message_type = sentence_fields[0].strip()
    if message_type != "$VETMK":
        logger.warning(
            "異常: VETMK応答の識別子が想定外のため解析できません。識別子=%s 受信データ=%s",
            message_type,
            vetmk_sentence.strip(),
        )
        return None

    sequence_number = sentence_fields[1].strip()
    if sequence_number != "0":
        logger.warning(
            "異常: VETMK応答の連番が想定外のため解析できません。連番=%s 受信データ=%s",
            sequence_number,
            vetmk_sentence.strip(),
        )
        return None

    flag_text = sentence_fields[2].strip()
    try:
        return int(flag_text)
    except ValueError:
        logger.warning(
            "異常: VETMK応答のflagが数値ではないため解析できません。flag=%s 受信データ=%s",
            flag_text,
            vetmk_sentence.strip(),
        )
        return None
