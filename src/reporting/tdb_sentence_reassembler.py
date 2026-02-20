"""TDB分割センテンスの再構築処理を提供する。"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from consts.tdb_constants import (
    AIS_CHAR_HIGH_OFFSET,
    AIS_CHAR_LOW_OFFSET,
    AIS_CHAR_RANGE_HIGH_END,
    AIS_CHAR_RANGE_HIGH_START,
    AIS_CHAR_RANGE_LOW_END,
    AIS_CHAR_RANGE_LOW_START,
    AIS_CHAR_SPLIT_THRESHOLD,
    AIS_SIXBIT_CHUNK_SIZE,
    AIS_SIXBIT_MAX_VALUE,
    AIS_SIXBIT_MIN_VALUE,
    NMEA_CHECKSUM_SEPARATOR,
    NMEA_FIELD_SEPARATOR,
    NMEA_SENTENCE_START_MARK,
    TDB_FIELD_DESTINATION_POSITION,
    TDB_FIELD_FILL_POSITION,
    TDB_FIELD_PAYLOAD_POSITION,
    TDB_FIELD_SEGMENT_INDEX_POSITION,
    TDB_FIELD_SEQUENCE_POSITION,
    TDB_FIELD_SOURCE_POSITION,
    TDB_FIELD_TOTAL_POSITION,
    TDB_FILL_MAX_VALUE,
    TDB_FILL_MIN_VALUE,
    TDB_GROUP_EXPIRY_SECONDS,
    TDB_GROUP_IDENTITY_MIN_FIELD_COUNT,
    TDB_MIN_FIELD_COUNT,
    TDB_TOKEN_LENGTH,
    TDB_TOKEN_SUFFIX,
)

LOGGER = logging.getLogger(__name__)


@dataclass
class _TdbSegment:
    """TDBセンテンスの解析結果を保持する。"""

    total: int
    index: int
    src: str
    dst: str
    payload: str
    fill: int


@dataclass
class _TdbSegmentGroup:
    """TDB分割データの集約状態を保持する。"""

    total: int
    src: str
    dst: str
    segments: dict[int, str] = field(default_factory=dict)
    last_received_at: datetime | None = None


class TdbSentenceReassembler:
    """TDB分割センテンスを再構築する。"""

    def __init__(self) -> None:
        self._groups: dict[tuple[int, str, str], _TdbSegmentGroup] = {}
        self._expiry_seconds = TDB_GROUP_EXPIRY_SECONDS
        self._split_reconstruct_success_count: int = 0
        self._split_reconstruct_failure_count: int = 0

    def get_split_reconstruct_counts(self) -> tuple[int, int]:
        """分割再構成（total>=2）の成功件数と失敗件数を返す。"""
        return self._split_reconstruct_success_count, self._split_reconstruct_failure_count

    def finalize_pending_groups_as_boundary_failure(self) -> int:
        """分境界時点で未完了の分割グループを失敗として確定する。"""
        pending_group_keys = list(self._groups.keys())
        for group_key in pending_group_keys:
            group = self._groups.get(group_key)
            if group is None:
                continue
            self._mark_group_failure_with_log(
                group_key,
                "異常: 分境界到達時点で未完了の分割TDBが残っていたため失敗として確定します。total=%s, src=%s, dst=%s",
                group.total,
                group.src,
                group.dst,
            )

        return len(pending_group_keys)

    def reassemble_sentences(self, sentences: list[str], received_at: datetime) -> list[str]:
        """TDB分割センテンスを再構築し、出力対象のセンテンスを返す。

        引数:
            sentences: 受信済みセンテンスのリスト。
            received_at: 受信時刻。

        戻り値:
            再構築後のセンテンスを含むリスト。

        例外:
            ValueError: センテンス一覧が未指定の場合。
            TypeError: センテンス一覧の型が不正な場合。
        """
        if sentences is None:
            raise ValueError("センテンス一覧が未指定です。")

        if not isinstance(sentences, list):
            raise TypeError("センテンス一覧はリストである必要があります。")

        if received_at is None:
            raise ValueError("受信時刻が未指定です。")

        self._expire_groups(received_at)

        results: list[str] = []
        for sentence in sentences:
            if not isinstance(sentence, str):
                LOGGER.warning("異常: センテンスが文字列ではないため破棄します。sentence=%s", sentence)
                continue

            normalized_sentence = sentence.strip()
            if not normalized_sentence:
                LOGGER.warning("異常: センテンスが空のため破棄します。")
                continue

            if not self._is_tdb_sentence(normalized_sentence):
                results.append(normalized_sentence)
                continue

            try:
                segment = self._parse_tdb_sentence(normalized_sentence)
            except Exception as exc:
                group_identity = self._try_extract_group_identity(normalized_sentence)
                if group_identity is not None:
                    total, index, src, dst = group_identity
                    group_key = (total, src, dst)
                    should_mark_failure = False
                    if total >= 2:
                        if index == 1:
                            should_mark_failure = True
                        elif group_key in self._groups:
                            should_mark_failure = True

                    if should_mark_failure:
                        self._mark_group_failure_with_log(
                            group_key,
                            "異常: 解析失敗のため分割TDBを破棄します。total=%s, src=%s, dst=%s",
                            total,
                            src,
                            dst,
                        )
                LOGGER.warning(
                    "異常: TDBセンテンスの解析に失敗したため破棄します。sentence=%s, reason=%s",
                    normalized_sentence,
                    exc,
                )
                continue

            if segment.total < 2:
                results.append(normalized_sentence)
                continue

            reassembled = self._register_segment(segment, received_at)
            if reassembled is None:
                continue
            results.append(reassembled)

        return results

    def _mark_group_success(self, group_key: tuple[int, str, str]) -> None:
        """分割グループ（total>=2）の成功終端を確定し成功件数を加算する。"""
        group = self._groups.pop(group_key, None)
        if group is None:
            return

        if group.total < 2:
            LOGGER.warning(
                "異常: 単行TDBは再構成成功件数へ加算しません。total=%s, src=%s, dst=%s",
                group.total,
                group.src,
                group.dst,
            )
            return

        # 一時対応: 再構成成功数の加算処理は停止する。
        # self._split_reconstruct_success_count += 1

    def _mark_group_failure(self, group_key: tuple[int, str, str]) -> None:
        """分割グループの失敗終端を確定し失敗件数を加算する。"""
        self._groups.pop(group_key, None)
        # 一時対応: 再構成失敗数の加算処理は停止する。
        # self._split_reconstruct_failure_count += 1

    def _mark_group_failure_with_log(self, group_key: tuple[int, str, str], message: str, *args: object) -> None:
        """失敗ログ出力と失敗終端確定を同時に行う。"""
        LOGGER.warning(message, *args)
        self._mark_group_failure(group_key)

    @staticmethod
    def _is_tdb_sentence(sentence: str) -> bool:
        """対象センテンスが`!--TDB`形式か判定する。"""
        if not sentence or not sentence.startswith(NMEA_SENTENCE_START_MARK):
            return False

        comma_index = sentence.find(NMEA_FIELD_SEPARATOR)
        if comma_index <= 1:
            return False

        token = sentence[1:comma_index]
        if len(token) != TDB_TOKEN_LENGTH:
            return False

        return token.endswith(TDB_TOKEN_SUFFIX)

    def _parse_tdb_sentence(self, sentence: str) -> _TdbSegment:
        """TDBセンテンスを解析して必要な情報を抽出する。"""
        body = sentence.split(NMEA_CHECKSUM_SEPARATOR, 1)[0]
        fields = body.split(NMEA_FIELD_SEPARATOR)
        if len(fields) < TDB_MIN_FIELD_COUNT:
            raise ValueError("TDBセンテンスのフィールド数が不足しています。")

        total = self._parse_int(fields[TDB_FIELD_TOTAL_POSITION], "total")
        index = self._parse_int(fields[TDB_FIELD_SEGMENT_INDEX_POSITION], "index")
        self._parse_int(fields[TDB_FIELD_SEQUENCE_POSITION], "seq")

        src = fields[TDB_FIELD_SOURCE_POSITION].strip()
        dst = fields[TDB_FIELD_DESTINATION_POSITION].strip()
        payload = fields[TDB_FIELD_PAYLOAD_POSITION].strip()
        fill = self._parse_int(fields[TDB_FIELD_FILL_POSITION], "fill")

        if total < 1:
            raise ValueError("totalの値が不正です。")

        if index < 1 or index > total:
            raise ValueError("indexの値が範囲外です。")

        if fill < TDB_FILL_MIN_VALUE or fill > TDB_FILL_MAX_VALUE:
            raise ValueError("fillの値が範囲外です。")

        if payload == "":
            raise ValueError("payloadが空です。")

        return _TdbSegment(
            total=total, index=index, src=src, dst=dst, payload=payload, fill=fill
        )

    def _parse_int(self, value: str, field_name: str) -> int:
        """整数フィールドの変換を行う。"""
        normalized_value = value.strip()
        if normalized_value == "":
            raise ValueError(f"{field_name}が空です。")

        try:
            return int(normalized_value)
        except ValueError as exc:
            raise ValueError(f"{field_name}が整数ではありません。") from exc

    def _register_segment(self, segment: _TdbSegment, received_at: datetime) -> str | None:
        """分割センテンスを登録し、再構築が完了した場合は結合結果を返す。"""
        group_key = (segment.total, segment.src, segment.dst)
        if segment.index == 1 and group_key in self._groups:
            self._mark_group_failure_with_log(
                group_key,
                "異常: 未完了の分割TDBが残っていたため破棄します。total=%s, src=%s, dst=%s",
                segment.total,
                segment.src,
                segment.dst,
            )

        group = self._groups.get(group_key)
        if group is None:
            group = _TdbSegmentGroup(total=segment.total, src=segment.src, dst=segment.dst)

        if segment.index in group.segments:
            self._mark_group_failure_with_log(
                group_key,
                "異常: 分割TDBのindexが重複したため破棄します。total=%s, index=%s, src=%s, dst=%s",
                segment.total,
                segment.index,
                segment.src,
                segment.dst,
            )
            return None

        try:
            segment_bits = self._payload_to_bits(segment.payload, segment.fill)
        except Exception as exc:
            self._mark_group_failure_with_log(
                group_key,
                "異常: 分割TDBのpayload変換に失敗したため破棄します。total=%s, index=%s, src=%s, dst=%s, reason=%s",
                segment.total,
                segment.index,
                segment.src,
                segment.dst,
                exc,
            )
            return None

        group.segments[segment.index] = segment_bits
        group.last_received_at = received_at
        self._groups[group_key] = group

        if len(group.segments) < group.total:
            return None

        try:
            reassembled_sentence = self._build_reassembled_sentence(group)
        except Exception as exc:
            self._mark_group_failure_with_log(
                group_key,
                "異常: 分割TDBの再構成結果生成に失敗したため破棄します。total=%s, src=%s, dst=%s, reason=%s",
                group.total,
                group.src,
                group.dst,
                exc,
            )
            return None

        if reassembled_sentence is None:
            self._mark_group_failure(group_key)
            return None

        self._mark_group_success(group_key)
        return reassembled_sentence

    def _expire_groups(self, received_at: datetime) -> None:
        """受信時刻を基準に期限切れのグループを破棄する。"""
        if not self._groups:
            return

        expired_keys: list[tuple[int, str, str]] = []
        for group_key, group in self._groups.items():
            if group.last_received_at is None:
                continue

            if received_at < group.last_received_at:
                LOGGER.warning(
                    "異常: 受信時刻が逆転しているため期限判定をスキップします。total=%s, src=%s, dst=%s",
                    group.total,
                    group.src,
                    group.dst,
                )
                continue

            elapsed = received_at - group.last_received_at
            if elapsed > timedelta(seconds=self._expiry_seconds):
                expired_keys.append(group_key)

        for group_key in expired_keys:
            group = self._groups.get(group_key)
            if group is None:
                continue
            self._mark_group_failure_with_log(
                group_key,
                "異常: 分割TDBが期限切れのため破棄します。total=%s, src=%s, dst=%s, seconds=%s",
                group.total,
                group.src,
                group.dst,
                self._expiry_seconds,
            )

    def _try_extract_group_identity(self, sentence: str) -> tuple[int, int | None, str, str] | None:
        """解析失敗時にグループ識別情報を可能な範囲で抽出する。"""
        if not sentence:
            return None

        body = sentence.split(NMEA_CHECKSUM_SEPARATOR, 1)[0]
        fields = body.split(NMEA_FIELD_SEPARATOR)
        if len(fields) < TDB_GROUP_IDENTITY_MIN_FIELD_COUNT:
            return None

        try:
            total = self._parse_int(fields[TDB_FIELD_TOTAL_POSITION], "total")
        except Exception:
            return None

        index: int | None = None
        if len(fields) > TDB_FIELD_SEGMENT_INDEX_POSITION:
            try:
                index = self._parse_int(fields[TDB_FIELD_SEGMENT_INDEX_POSITION], "index")
            except Exception:
                index = None

        src = fields[TDB_FIELD_SOURCE_POSITION].strip()
        dst = fields[TDB_FIELD_DESTINATION_POSITION].strip()
        if not src or not dst:
            return None

        if total < 1:
            return None

        return (total, index, src, dst)

    def _payload_to_bits(self, payload: str, fill: int) -> str:
        """AIS 6-bit ASCIIをビット列へ変換する。"""
        bit_chunks: list[str] = []
        for char in payload:
            bit_value = self._ais_char_to_sixbit(char)
            bit_chunks.append(f"{bit_value:0{AIS_SIXBIT_CHUNK_SIZE}b}")

        bits = "".join(bit_chunks)
        if fill == 0:
            return bits

        if fill > len(bits):
            raise ValueError("fillがpayloadの長さを超えています。")

        return bits[:-fill]

    def _ais_char_to_sixbit(self, char: str) -> int:
        """AIS 6-bit ASCIIの1文字を6bit値へ変換する。"""
        code = ord(char)
        if AIS_CHAR_RANGE_LOW_START <= code <= AIS_CHAR_RANGE_LOW_END:
            return code - AIS_CHAR_LOW_OFFSET
        if AIS_CHAR_RANGE_HIGH_START <= code <= AIS_CHAR_RANGE_HIGH_END:
            return code - AIS_CHAR_HIGH_OFFSET
        raise ValueError(f"AIS 6-bit ASCII外の文字です。char={char}")

    def _build_reassembled_sentence(self, group: _TdbSegmentGroup) -> str | None:
        """分割データを結合してTDBセンテンスを生成する。"""
        bit_sequences: list[str] = []
        for index in range(1, group.total + 1):
            segment_bits = group.segments.get(index)
            if segment_bits is None:
                LOGGER.warning(
                    "異常: 分割TDBの欠落があるため破棄します。total=%s, src=%s, dst=%s, missing_index=%s",
                    group.total,
                    group.src,
                    group.dst,
                    index,
                )
                return None
            bit_sequences.append(segment_bits)

        combined_bits = "".join(bit_sequences)
        if not combined_bits:
            LOGGER.warning(
                "異常: 分割TDBの結合結果が空のため破棄します。total=%s, src=%s, dst=%s",
                group.total,
                group.src,
                group.dst,
            )
            return None

        fill = (AIS_SIXBIT_CHUNK_SIZE - (len(combined_bits) % AIS_SIXBIT_CHUNK_SIZE)) % AIS_SIXBIT_CHUNK_SIZE
        if fill > 0:
            combined_bits += "0" * fill

        payload = self._bits_to_payload(combined_bits)
        sentence_body = f"VATDB,1,1,0,{group.src},{group.dst},0,0,{payload},{fill}"
        checksum = self._calculate_checksum(sentence_body)
        return f"!{sentence_body}*{checksum}"

    def _bits_to_payload(self, bits: str) -> str:
        """ビット列をAIS 6-bit ASCIIへ変換する。"""
        if len(bits) % AIS_SIXBIT_CHUNK_SIZE != 0:
            raise ValueError("ビット列の長さが6の倍数ではありません。")

        chars: list[str] = []
        for offset in range(0, len(bits), AIS_SIXBIT_CHUNK_SIZE):
            chunk = bits[offset : offset + AIS_SIXBIT_CHUNK_SIZE]
            value = int(chunk, 2)
            chars.append(self._sixbit_to_ais_char(value))

        return "".join(chars)

    def _sixbit_to_ais_char(self, value: int) -> str:
        """6bit値をAIS 6-bit ASCIIの1文字に変換する。"""
        if value < AIS_SIXBIT_MIN_VALUE or value > AIS_SIXBIT_MAX_VALUE:
            raise ValueError("6bit値が範囲外です。")

        if value < AIS_CHAR_SPLIT_THRESHOLD:
            return chr(value + AIS_CHAR_LOW_OFFSET)
        return chr(value + AIS_CHAR_HIGH_OFFSET)

    def _calculate_checksum(self, sentence_body: str) -> str:
        """NMEAチェックサムを計算する。"""
        checksum = 0
        for char in sentence_body:
            checksum ^= ord(char)
        return f"{checksum:02X}"
