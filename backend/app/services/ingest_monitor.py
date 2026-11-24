"""接入监控对账领域服务：从迟报/缺口结果定位可重传区间，并按实际实收结算重传。

职责边界：
- 按「测点 + 事件时间 + 到达顺序」登记服务端实收凭证（接收凭证 ledger），重复上送只增加到达
  记录，不会把同一个采样重复计入实收集合。
- 依据期望采样与实收样本对账，输出缺口区间（可重传区间）、相邻/交叠已收批次的分别对账结果，
  并固化查询快照，供派发与结算回溯“查询时的合理捕获范围”。
- 派发重传时重新读取最新实收：查询之后、派发之前迟到补齐的半段会从待发范围中剔除；重传只
  覆盖查询快照认定的缺口范围，不会把新到数据当成缺口重发。
- 重传落地后按实收凭证结算：只统计实际被接收的采样，半段补齐只更新剩余缺口，不因重传请求
  发送成功就关闭整段。

数据产物（均在 DATA_ROOT 下）：
- receipts/{point}_receipts.jsonl        接收凭证流水，每次上送/重传落地追加一行
- gap_reconcile/{snapshot_id}.json       缺口查询快照（查询时刻的捕获基线）
- replay_settlements/{replay_id}.json    重传申请的派发与结算状态
- replay_settlements/{replay_id}_events.jsonl 重传生命周期审计事件
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


def _parse_ts(value: Any) -> float:
    """把事件时间解析为 epoch 秒；接受 ISO 字符串或数字时间戳，无法解析时抛 ValueError。"""
    if value is None or value == "":
        raise ValueError("事件时间不能为空")
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError as exc:
        raise ValueError(f"无法解析事件时间：{value}") from exc


def _format_ts(epoch: float) -> str:
    return datetime.fromtimestamp(epoch).isoformat(timespec="seconds")


class IngestMonitorService(BaseModuleService):
    """接入监控：接收凭证、实收样本、缺口定位与重传结算。"""

    MODULE = "ingest_monitor"
    ENTITY = "缺口重传对账"

    # -- 接收凭证登记 ----------------------------------------------------

    def register_receipts(
        self,
        point_key: str,
        batch_key: str,
        samples: list[dict[str, Any]],
        arrival_seq: int | None = None,
    ) -> dict[str, Any]:
        """登记一批实际到达的采样（接收凭证）。

        同一采样（测点 + 采样序号/事件时间）重复到达时只追加凭证流水，实收集合在读取时按
        键去重，绝不重复计数。arrival_seq 不给定时按该测点凭证流水自增。
        """
        if not point_key or not str(point_key).strip():
            raise ValueError("测点编号不能为空")
        if not batch_key or not str(batch_key).strip():
            raise ValueError("批次编号不能为空")
        if not isinstance(samples, list):
            raise ValueError("实收样本必须是列表")

        ledger = self._ledger_file(point_key)
        existing = self._read_jsonl(ledger)
        if arrival_seq is None:
            arrival_seq = max((int(r.get("arrival_seq", 0)) for r in existing), default=0) + 1

        records = []
        for order, sample in enumerate(samples):
            ts = sample.get("timestamp") or sample.get("event_time") or sample.get("事件时间")
            epoch = _parse_ts(ts)
            index = sample.get("sample_index") or sample.get("sample_seq")
            records.append({
                "point": point_key,
                "batch": str(batch_key),
                "arrival_seq": int(arrival_seq),
                "arrival_order": len(existing) + order + 1,
                "sample_index": int(index) if index is not None else None,
                "timestamp": _format_ts(epoch) if isinstance(ts, str) and len(str(ts)) <= 19 else str(ts),
                "raw_timestamp": str(ts),
                "value": sample.get("value", sample.get("raw_value", sample.get("raw_value_mV"))),
                "received_at": time.time(),
            })
        written = self._write_jsonl(ledger, records)
        return {
            "point": point_key,
            "batch": str(batch_key),
            "arrival_seq": int(arrival_seq),
            "records": written,
            "ledger_total": len(existing) + written,
        }

    # -- 实收样本读取与去重 ----------------------------------------------

    def _ledger_file(self, point_key: str) -> Path:
        return self._data_dir("receipts") / f"{point_key}_receipts.jsonl"

    def _load_expected(
        self,
        point_key: str,
        start_time: str | None,
        end_time: str | None,
        expected_samples: list[dict[str, Any]] | None,
    ) -> list[dict[str, Any]]:
        """加载期望采样：显式传入优先，否则回退到边缘缓冲采样文件，并按事件时间窗口过滤。"""
        if expected_samples is not None:
            raw = list(expected_samples)
        else:
            edge_file = self._data_dir("edge_samples") / f"{point_key}.jsonl"
            raw = self._read_jsonl(edge_file)

        window: tuple[float, float] | None = None
        if start_time or end_time:
            lo = _parse_ts(start_time) if start_time else float("-inf")
            hi = _parse_ts(end_time) if end_time else float("inf")
            window = (lo, hi)

        expected: list[dict[str, Any]] = []
        seen_indexes: set[int] = set()
        for order, sample in enumerate(raw):
            epoch = _parse_ts(sample.get("timestamp") or sample.get("event_time"))
            if window and not (window[0] <= epoch <= window[1]):
                continue
            index = sample.get("sample_index") or sample.get("sample_seq")
            index = int(index) if index is not None else order + 1
            if index in seen_indexes:  # 期望侧重复定义不参与缺口计算
                continue
            seen_indexes.add(index)
            expected.append({
                "sample_index": index,
                "timestamp": _format_ts(epoch),
                "epoch": epoch,
            })
        expected.sort(key=lambda item: (item["epoch"], item["sample_index"]))
        return expected

    def _load_actual(self, point_key: str) -> list[dict[str, Any]]:
        """读取接收凭证流水，按到达顺序返回；补齐缺失的 sample_index（按事件时间映射）。"""
        rows = self._read_jsonl(self._ledger_file(point_key))
        rows.sort(key=lambda row: (int(row.get("arrival_seq", 0)), int(row.get("arrival_order", 0))))
        return rows

    @staticmethod
    def _receipt_key(
        row: dict[str, Any],
        ts_to_index: dict[str, int],
        epoch_to_index: dict[int, int] | None = None,
    ) -> int | None:
        index = row.get("sample_index")
        if index is not None:
            return int(index)
        mapped = ts_to_index.get(str(row.get("raw_timestamp") or row.get("timestamp")))
        if mapped is not None:
            return mapped
        if epoch_to_index:
            try:
                return epoch_to_index.get(int(_parse_ts(row.get("raw_timestamp"))))
            except ValueError:
                return None
        return None

    def _received_index_set(
        self,
        rows: list[dict[str, Any]],
        ts_to_index: dict[str, int],
        scope: set[int] | None = None,
        epoch_to_index: dict[int, int] | None = None,
    ) -> set[int]:
        """实收集合：同一采样多次到达只计一次。"""
        keys: set[int] = set()
        for row in rows:
            key = self._receipt_key(row, ts_to_index, epoch_to_index)
            if key is None:
                continue
            if scope is not None and key not in scope:
                continue
            keys.add(key)
        return keys

    # -- 批次分别对账（相邻与交叠） --------------------------------------

    def _reconcile_batches(
        self,
        rows: list[dict[str, Any]],
        ts_to_index: dict[str, int],
        index_to_time: dict[int, str],
        epoch_to_index: dict[int, int] | None = None,
    ) -> list[dict[str, Any]]:
        """按批次（凭证）分别对账，标注相邻与交叠关系，互不合并。"""
        grouped: dict[str, dict[str, Any]] = {}
        order_seen: list[str] = []
        for row in rows:
            batch = str(row.get("batch"))
            if batch not in grouped:
                grouped[batch] = {
                    "batch": batch,
                    "arrival_seq": int(row.get("arrival_seq", 0)),
                    "record_count": 0,
                    "indexes": set(),
                }
                order_seen.append(batch)
            info = grouped[batch]
            info["record_count"] += 1
            key = self._receipt_key(row, ts_to_index, epoch_to_index)
            if key is not None:
                info["indexes"].add(key)

        batches = []
        for batch in order_seen:
            info = grouped[batch]
            indexes = sorted(info["indexes"])
            covered_ranges = self._ranges_from_keys(indexes, index_to_time)
            others = [b for b in order_seen if b != batch]
            overlap_with = [
                other for other in others if grouped[other]["indexes"] & info["indexes"]
            ]
            batches.append({
                "batch": batch,
                "arrival_seq": info["arrival_seq"],
                "record_count": info["record_count"],
                "unique_points": len(indexes),
                "duplicate_count": info["record_count"] - len(indexes),
                "covered_indexes": indexes,
                "covered_ranges": covered_ranges,
                "overlap_with": overlap_with,
                "adjacent_with": [
                    other for other in others
                    if other not in overlap_with
                    and self._is_adjacent(grouped[other]["indexes"], info["indexes"])
                ],
            })
        batches.sort(key=lambda item: item["arrival_seq"])
        return batches

    @staticmethod
    def _is_adjacent(left: set[int], right: set[int]) -> bool:
        if not left or not right:
            return False
        for index in right:
            if (index - 1) in left or (index + 1) in left:
                return True
        return False

    # -- 缺口区间压缩 ----------------------------------------------------

    @staticmethod
    def _ranges_from_keys(keys: list[int], index_to_time: dict[int, str]) -> list[dict[str, Any]]:
        """把离散采样序号压缩成连续可重传区间。"""
        ranges: list[dict[str, Any]] = []
        for key in keys:
            if ranges and key == ranges[-1]["end_index"] + 1:
                ranges[-1]["end_index"] = key
                ranges[-1]["end_time"] = index_to_time.get(key, "")
                ranges[-1]["count"] += 1
            else:
                ranges.append({
                    "start_index": key,
                    "end_index": key,
                    "start_time": index_to_time.get(key, ""),
                    "end_time": index_to_time.get(key, ""),
                    "count": 1,
                })
        return ranges

    # -- 缺口查询与快照 --------------------------------------------------

    def query_gaps(
        self,
        point_key: str,
        start_time: str | None = None,
        end_time: str | None = None,
        expected_samples: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """按事件时间窗口对账实收样本，定位剩余缺口（可重传区间）并固化查询快照。"""
        if not point_key or not str(point_key).strip():
            raise ValueError("测点编号不能为空")

        expected = self._load_expected(point_key, start_time, end_time, expected_samples)
        if not expected:
            return {
                "point": point_key,
                "window": {"start_time": start_time, "end_time": end_time},
                "expected_count": 0,
                "actual_count": 0,
                "unique_actual": 0,
                "duplicate_count": 0,
                "missing_count": 0,
                "gap_ranges": [],
                "batches": [],
                "message": "窗口内没有期望采样，无法定位缺口",
            }

        index_to_time = {item["sample_index"]: item["timestamp"] for item in expected}
        ts_to_index = {item["timestamp"]: item["sample_index"] for item in expected}
        epoch_to_index = {int(item["epoch"]): item["sample_index"] for item in expected}
        scope = set(index_to_time)

        rows = self._load_actual(point_key)
        scoped_rows = [
            row for row in rows
            if (key := self._receipt_key(row, ts_to_index, epoch_to_index)) is not None and key in scope
        ]
        received = self._received_index_set(scoped_rows, ts_to_index, scope, epoch_to_index)
        missing = sorted(scope - received)

        # 重复到达：流水行数超出实收去重点数的部分
        duplicate_count = len(scoped_rows) - len(received)
        batches = self._reconcile_batches(
            scoped_rows, ts_to_index, index_to_time, epoch_to_index
        )
        gap_ranges = self._ranges_from_keys(missing, index_to_time)

        snapshot_dir = self._data_dir("gap_reconcile")
        seq = len(list(snapshot_dir.glob(f"GAPQ-{point_key}-*.json"))) + 1
        snapshot_id = f"GAPQ-{point_key}-{seq:03d}"
        snapshot = {
            "snapshot_id": snapshot_id,
            "point": point_key,
            "queried_at": time.time(),
            "window": {"start_time": start_time, "end_time": end_time},
            "expected_indexes": sorted(scope),
            "received_indexes": sorted(received),
            "missing_indexes": missing,
            "expected_count": len(scope),
            "actual_count": len(scoped_rows),
            "unique_actual": len(received),
            "duplicate_count": duplicate_count,
            "gap_ranges": gap_ranges,
            "batches": batches,
        }
        with open(snapshot_dir / f"{snapshot_id}.json", "w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False, indent=2, default=str)

        return {
            "snapshot_id": snapshot_id,
            "point": point_key,
            "window": snapshot["window"],
            "expected_count": len(scope),
            "actual_count": len(scoped_rows),
            "unique_actual": len(received),
            "duplicate_count": duplicate_count,
            "missing_count": len(missing),
            "gap_ranges": gap_ranges,
            "batches": batches,
        }

    def get_snapshot(self, snapshot_id: str) -> dict[str, Any]:
        """读取缺口查询快照（派发与结算的捕获基线）。"""
        path = self._data_dir("gap_reconcile") / f"{snapshot_id}.json"
        if not path.exists():
            raise KeyError(f"缺口查询快照 {snapshot_id} 不存在")
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    # -- 重传派发 --------------------------------------------------------

    def dispatch_replay(
        self,
        snapshot_id: str,
        point_key: str,
        range_indexes: list[int] | None = None,
        replay_key: str | None = None,
    ) -> dict[str, Any]:
        """依据查询快照发起重传，派发前重新对账，剔除查询后迟到补齐的部分。"""
        snapshot = self.get_snapshot(snapshot_id)
        if snapshot["point"] != point_key:
            raise ValueError("快照测点与请求测点不一致，拒绝派发")

        expected_indexes = set(snapshot["expected_indexes"])
        snapshot_missing = set(snapshot["missing_indexes"])
        index_to_time = {
            item["sample_index"]: item["timestamp"]
            for item in self._load_expected(
                point_key,
                snapshot["window"].get("start_time"),
                snapshot["window"].get("end_time"),
                None,
            )
        }
        ts_to_index = {ts: idx for idx, ts in index_to_time.items()}
        epoch_to_index = {int(_parse_ts(ts)): idx for idx, ts in index_to_time.items()}

        # 派发时刻重新读取实收凭证：查询与派发之间的迟到上送在此刻生效
        rows = self._load_actual(point_key)
        received_now = self._received_index_set(rows, ts_to_index, expected_indexes, epoch_to_index)
        remaining_now = sorted(expected_indexes - received_now)

        # 查询之后、派发之前被迟到数据补齐的部分（旧查询里算缺口，此刻已实收）
        late_covered = sorted(snapshot_missing & received_now)

        if range_indexes is not None:
            requested = {int(i) for i in range_indexes}
            outside_query = sorted(requested - snapshot_missing)
            if outside_query:
                raise ValueError(
                    f"请求重传的采样 {outside_query} 不在查询快照缺口范围内，拒绝派发"
                )
            target = sorted(requested - received_now)
        else:
            target = remaining_now

        target_set = set(target)
        unsent = sorted((expected_indexes - received_now) - target_set)
        within_scope = target_set.issubset(snapshot_missing)

        # 在区间重采模块登记重传申请（状态留在服务层，路由不做业务判断）
        replay_rows = store.rows("range_replay")
        replay_id = max((int(r.get("id", 0)) for r in replay_rows), default=0) + 1
        if replay_key is None:
            replay_key = f"RETR-{point_key}-{replay_id:03d}"
        sent_ranges = self._ranges_from_keys(target, index_to_time)
        unsent_ranges = self._ranges_from_keys(unsent, index_to_time)
        entry = {
            "id": replay_id,
            "重采编号": replay_key,
            "测点编号": point_key,
            "起始时刻": sent_ranges[0]["start_time"] if sent_ranges else "",
            "截止时刻": sent_ranges[-1]["end_time"] if sent_ranges else "",
            "重采原因": "接入缺口重传",
            "覆盖方式": "去重覆盖",
            "重采点数": str(len(target)),
            "重采水位": "0",
            "status": "待重采",
            "pending": True,
            "abnormal": False,
        }
        replay_rows.append(entry)
        # 状态流转交给区间重采领域服务（ACTION_RULES 由该模块持有）；重采中仍是待办态
        replay_svc = self._safe_import("app.services.range_replay", "RangeReplayService")()
        replay_svc.run_action(replay_id, "启动重采")
        entry["status"] = "重采中"
        entry["pending"] = True
        entry["abnormal"] = False

        settlement = {
            "replay_id": replay_id,
            "replay_key": replay_key,
            "snapshot_id": snapshot_id,
            "point": point_key,
            "window": snapshot["window"],
            "expected_indexes": sorted(expected_indexes),
            "snapshot_missing_indexes": sorted(snapshot_missing),
            "sent_indexes": target,
            "sent_ranges": sent_ranges,
            "unsent_indexes": unsent,
            "unsent_ranges": unsent_ranges,
            "late_covered_after_query": late_covered,
            "within_query_scope": within_scope,
            "status": "待落地",
            "settled": False,
            "dispatched_at": time.time(),
        }
        settle_dir = self._data_dir("replay_settlements")
        with open(settle_dir / f"{replay_key}.json", "w", encoding="utf-8") as f:
            json.dump(settlement, f, ensure_ascii=False, indent=2, default=str)
        self._write_jsonl(
            settle_dir / f"{replay_key}_events.jsonl",
            [{"event": "dispatch", "at": time.time(), **{
                k: settlement[k] for k in (
                    "replay_key", "snapshot_id", "sent_indexes", "unsent_indexes",
                    "late_covered_after_query",
                )
            }}],
        )

        return {
            "replay_id": replay_id,
            "replay_key": replay_key,
            "snapshot_id": snapshot_id,
            "point": point_key,
            "status": "待重采",
            "sent_count": len(target),
            "sent_indexes": target,
            "sent_ranges": sent_ranges,
            "unsent_count": len(unsent),
            "unsent_indexes": unsent,
            "unsent_ranges": unsent_ranges,
            "late_covered_after_query": late_covered,
            "within_query_scope": within_scope,
        }

    # -- 重传落地结算 ----------------------------------------------------

    def _find_settlement(self, replay_key: str) -> Path | None:
        settle_dir = self._data_dir("replay_settlements")
        if not settle_dir.exists():
            return None
        for path in settle_dir.glob("*.json"):
            if path.name.endswith("_events.jsonl"):
                continue
            try:
                with open(path, "r", encoding="utf-8") as f:
                    doc = json.load(f)
            except (json.JSONDecodeError, OSError):
                continue
            if doc.get("replay_key") == replay_key or str(doc.get("replay_id")) == str(replay_key):
                return path
        return None

    def settle_replay(
        self,
        replay_key: str,
        received_samples: list[dict[str, Any]] | None = None,
        batch: str | None = None,
    ) -> dict[str, Any]:
        """重传落地后按实际实收结算：重复到达不重复计点，半段补齐只更新剩余缺口。"""
        path = self._find_settlement(replay_key)
        if path is None:
            raise KeyError(f"重传申请 {replay_key} 不存在或未派发")
        with open(path, "r", encoding="utf-8") as f:
            settlement = json.load(f)
        if settlement.get("settled"):
            raise ValueError(f"重传申请 {replay_key} 已结算，不能重复结算")

        point_key = settlement["point"]
        # 本次重传实际送达的样本先入接收凭证流水（重复上送也只追加凭证）
        if received_samples:
            self.register_receipts(
                point_key,
                batch or f"{settlement['replay_key']}-落地",
                received_samples,
            )

        expected_indexes = set(settlement["expected_indexes"])
        sent_indexes = set(settlement["sent_indexes"])
        index_to_time = {
            item["sample_index"]: item["timestamp"]
            for item in self._load_expected(
                point_key,
                settlement["window"].get("start_time"),
                settlement["window"].get("end_time"),
                None,
            )
        }
        ts_to_index = {ts: idx for idx, ts in index_to_time.items()}
        epoch_to_index = {int(_parse_ts(ts)): idx for idx, ts in index_to_time.items()}

        rows = self._load_actual(point_key)
        scoped = [
            row for row in rows
            if (key := self._receipt_key(row, ts_to_index, epoch_to_index)) is not None
            and key in expected_indexes
        ]
        received_now = self._received_index_set(scoped, ts_to_index, expected_indexes, epoch_to_index)

        # 重传范围内的实收：以凭证为准，发了但没到位不能划掉缺口
        accepted = sorted(sent_indexes & received_now)
        # 重传范围内重复到达的凭证行数（同一采样的第二次及以后到达）
        seen: set[int] = set()
        duplicate_in_replay = 0
        for row in scoped:
            key = self._receipt_key(row, ts_to_index, epoch_to_index)
            if key is None or key not in sent_indexes:
                continue
            if key in seen:
                duplicate_in_replay += 1
            seen.add(key)

        still_missing = sorted(expected_indexes - received_now)
        remaining_ranges = self._ranges_from_keys(still_missing, index_to_time)
        accepted_ranges = self._ranges_from_keys(accepted, index_to_time)
        fully_closed = not still_missing

        settlement.update({
            "settled": True,
            "settled_at": time.time(),
            "accepted_indexes": accepted,
            "accepted_ranges": accepted_ranges,
            "accepted_count": len(accepted),
            "duplicate_in_replay_count": duplicate_in_replay,
            "remaining_missing_indexes": still_missing,
            "remaining_ranges": remaining_ranges,
            "final_received_points": len(received_now),
            "expected_points": len(expected_indexes),
            "fully_closed": fully_closed,
            "status": "已结清" if fully_closed else "部分补齐",
        })
        with open(path, "w", encoding="utf-8") as f:
            json.dump(settlement, f, ensure_ascii=False, indent=2, default=str)
        self._write_jsonl(
            path.parent / f"{settlement['replay_key']}_events.jsonl",
            [{"event": "settle", "at": time.time(),
              "accepted_count": len(accepted), "duplicate_in_replay_count": duplicate_in_replay,
              "remaining_missing_count": len(still_missing), "fully_closed": fully_closed}],
        )

        # 同步区间重采申请：按实收结算，半段补齐不关闭整段
        replay_svc = self._safe_import("app.services.range_replay", "RangeReplayService")()
        replay_entry = store.find("range_replay", int(settlement["replay_id"]))
        if replay_entry is not None:
            replay_entry["重采点数"] = str(len(accepted))
            replay_entry["重采水位"] = str(len(received_now))
            if fully_closed:
                replay_svc.run_action(int(settlement["replay_id"]), "标记完成")
                replay_entry["status"] = "已完成"
                replay_entry["pending"] = False
            else:
                replay_entry["status"] = "待重采"
                replay_entry["pending"] = True

        return {
            "replay_id": settlement["replay_id"],
            "replay_key": settlement["replay_key"],
            "point": point_key,
            "status": settlement["status"],
            "fully_closed": fully_closed,
            "sent_count": len(sent_indexes),
            "accepted_count": len(accepted),
            "accepted_indexes": accepted,
            "accepted_ranges": accepted_ranges,
            "duplicate_in_replay_count": duplicate_in_replay,
            "remaining_missing_count": len(still_missing),
            "remaining_missing_indexes": still_missing,
            "remaining_ranges": remaining_ranges,
            "final_received_points": len(received_now),
            "expected_points": len(expected_indexes),
        }
