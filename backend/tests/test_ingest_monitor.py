"""接入监控缺口重传对账验收测试。

验收主线：查询与派发之间迟到上送补齐半段 → 发起重传 → 核对实际送出区间、未发送区间，
以及重复到达时最终被接收的点数；重传按实际实收结算，半段补齐不关闭整段。
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from app.services.base import DATA_ROOT


def _sample(index: int, second: int | None = None, point: str = "SENS-MON") -> dict:
    second = index if second is None else second
    return {
        "point": point,
        "sample_index": index,
        "timestamp": f"2026-11-24T10:00:{second:02d}",
        "raw_value_mV": 23.5 + index,
    }


def _write_edge(tmp: Path, point: str, count: int) -> None:
    edge_dir = tmp / "edge_samples"
    edge_dir.mkdir(parents=True, exist_ok=True)
    with open(edge_dir / f"{point}.jsonl", "w", encoding="utf-8") as f:
        for i in range(1, count + 1):
            f.write(json.dumps(_sample(i, point=point), ensure_ascii=False) + "\n")


@pytest.fixture(autouse=True)
def setup_data_dir():
    with tempfile.TemporaryDirectory() as tmp:
        with patch("app.services.base.DATA_ROOT", Path(tmp)):
            yield Path(tmp)


class TestGapQueryAndReceipts:
    """接收凭证、缺口定位与相邻/交叠批次分别对账。"""

    def test_receipts_dedupe_and_gap_ranges_are_located(self, setup_data_dir):
        from app.services.ingest_monitor import IngestMonitorService

        point = "SENS-MON-A"
        _write_edge(setup_data_dir, point, 10)
        svc = IngestMonitorService()

        # 批次A实收 1,2,3,8,9,10 → 缺口 4,5,6,7
        svc.register_receipts(point, "BATCH-A", [_sample(i) for i in [1, 2, 3, 8, 9, 10]])
        # 同一批次重复上送（重复上报）：不能凭“又发了一次”划掉缺口
        dup = svc.register_receipts(
            point, "BATCH-A-DUP", [_sample(i) for i in [1, 2, 3]], arrival_seq=2
        )
        assert dup["records"] == 3

        result = svc.query_gaps(point, "2026-11-24T10:00:00", "2026-11-24T10:00:10")
        assert result["expected_count"] == 10
        assert result["actual_count"] == 9  # 流水 6 + 重复 3
        assert result["unique_actual"] == 6  # 去重后实收仍是 6 个点
        assert result["duplicate_count"] == 3
        assert result["missing_count"] == 4
        assert result["gap_ranges"] == [{
            "start_index": 4, "end_index": 7,
            "start_time": "2026-11-24T10:00:04", "end_time": "2026-11-24T10:00:07",
            "count": 4,
        }]
        # 两个凭证批次分别对账，不合并
        batches = {b["batch"]: b for b in result["batches"]}
        assert set(batches) == {"BATCH-A", "BATCH-A-DUP"}
        assert batches["BATCH-A"]["unique_points"] == 6
        assert batches["BATCH-A-DUP"]["unique_points"] == 3
        assert batches["BATCH-A-DUP"]["overlap_with"] == ["BATCH-A"]

    def test_multiple_gap_ranges_compressed_separately(self, setup_data_dir):
        from app.services.ingest_monitor import IngestMonitorService

        point = "SENS-MON-B"
        _write_edge(setup_data_dir, point, 10)
        svc = IngestMonitorService()
        svc.register_receipts(point, "BATCH-A", [_sample(i) for i in [1, 2, 3, 6, 8, 9, 10]])

        result = svc.query_gaps(point, "2026-11-24T10:00:00", "2026-11-24T10:00:10")
        # 缺口 4,5 与 7 是两段，必须分别给出，不能并成一段
        assert [(r["start_index"], r["end_index"], r["count"]) for r in result["gap_ranges"]] == [
            (4, 5, 2), (7, 7, 1),
        ]

    def test_receipt_without_sample_index_maps_by_event_time(self, setup_data_dir):
        from app.services.ingest_monitor import IngestMonitorService

        point = "SENS-MON-C"
        _write_edge(setup_data_dir, point, 5)
        svc = IngestMonitorService()
        # 凭证只给事件时间不给序号，应按事件时间映射到期望采样序号
        svc.register_receipts(point, "BATCH-T", [
            {"timestamp": _sample(i)["timestamp"]} for i in [1, 2, 3]
        ])
        result = svc.query_gaps(point, "2026-11-24T10:00:00", "2026-11-24T10:00:06")
        assert result["unique_actual"] == 3
        assert result["missing_count"] == 2
        assert result["gap_ranges"][0]["start_index"] == 4

    def test_receipt_with_fractional_timestamp_maps_by_epoch(self, setup_data_dir):
        from app.services.ingest_monitor import IngestMonitorService

        point = "SENS-MON-C2"
        _write_edge(setup_data_dir, point, 5)
        svc = IngestMonitorService()
        # 凭证事件时间带零毫秒且无序号：字符串与期望不一致时按 epoch 回退映射
        svc.register_receipts(point, "BATCH-MS", [
            {"timestamp": f"2026-11-24T10:00:{i:02d}.000000"} for i in [1, 2]
        ])
        result = svc.query_gaps(point, "2026-11-24T10:00:00", "2026-11-24T10:00:06")
        assert result["unique_actual"] == 2
        assert result["missing_count"] == 3
        assert result["batches"][0]["batch"] == "BATCH-MS"


class TestHalfSegmentReplayAcceptance:
    """验收主线：查询与派发之间补到半段，再重传，核对送出/未送区间与重复到达实收点数。"""

    def test_late_half_segment_between_query_and_dispatch(self, setup_data_dir):
        from app.services.ingest_monitor import IngestMonitorService
        from app.store import store

        point = "SENS-MON-D"
        _write_edge(setup_data_dir, point, 10)
        svc = IngestMonitorService()

        # 1) 首次上送：实收 1,2,3,8,9,10，缺口 4,5,6,7
        svc.register_receipts(point, "BATCH-A", [_sample(i) for i in [1, 2, 3, 8, 9, 10]])
        query = svc.query_gaps(point, "2026-11-24T10:00:00", "2026-11-24T10:00:10")
        snapshot_id = query["snapshot_id"]
        assert query["missing_count"] == 4

        # 2) 查询之后、派发之前：迟到上送正好确认缺口的一半（4,5）
        svc.register_receipts(point, "BATCH-LATE", [_sample(4), _sample(5)], arrival_seq=2)

        # 3) 派发重传，只申请覆盖剩余的 6（7 留作未发送区间）
        dispatch = svc.dispatch_replay(snapshot_id, point, range_indexes=[6])
        assert dispatch["within_query_scope"] is True
        assert dispatch["sent_indexes"] == [6]
        assert dispatch["sent_ranges"] == [{
            "start_index": 6, "end_index": 6,
            "start_time": "2026-11-24T10:00:06", "end_time": "2026-11-24T10:00:06",
            "count": 1,
        }]
        assert dispatch["unsent_indexes"] == [7]
        assert dispatch["unsent_ranges"][0]["start_index"] == 7
        # 迟到补齐的半段被识别，且不会被当成缺口再次重发
        assert dispatch["late_covered_after_query"] == [4, 5]
        assert 4 not in dispatch["sent_indexes"] and 5 not in dispatch["sent_indexes"]

        replay_key = dispatch["replay_key"]
        replay_entry = next(
            r for r in store.rows("range_replay") if r["重采编号"] == replay_key
        )
        assert replay_entry["status"] == "重采中"

        # 4) 重传落地：6 实际送达，且重复到达一次（同一采样两份凭证）
        settle = svc.settle_replay(
            replay_key,
            received_samples=[_sample(6), _sample(6)],
            batch="BATCH-RETRY",
        )
        # 重复到达最终只接收 1 个点
        assert settle["accepted_count"] == 1
        assert settle["accepted_indexes"] == [6]
        assert settle["duplicate_in_replay_count"] == 1
        # 半段补齐：剩余缺口只更新为 7，整段没有被关闭
        assert settle["fully_closed"] is False
        assert settle["status"] == "部分补齐"
        assert settle["remaining_missing_indexes"] == [7]
        assert settle["remaining_ranges"][0]["start_index"] == 7
        # 最终实收集合：1,2,3,4,5,6,8,9,10 共 9 个点（重复不重复计入）
        assert settle["final_received_points"] == 9
        assert settle["expected_points"] == 10

        # 区间重采申请按实收结算，发送成功不代表整段关闭
        assert replay_entry["status"] == "待重采"
        assert replay_entry["pending"] is True
        assert replay_entry["重采点数"] == "1"

        # 凭证流水里重复样本确实留痕，但缺口对账不受影响
        ledger = svc._read_jsonl(svc._ledger_file(point))
        assert sum(1 for r in ledger if r["sample_index"] == 6) == 2
        final_query = svc.query_gaps(point, "2026-11-24T10:00:00", "2026-11-24T10:00:10")
        assert final_query["missing_count"] == 1
        assert final_query["gap_ranges"][0]["start_index"] == 7
        # 三个凭证批次分别可对账：相邻关系存在、重复批次保留
        batches = {b["batch"]: b for b in final_query["batches"]}
        assert set(batches) == {"BATCH-A", "BATCH-LATE", "BATCH-RETRY"}
        assert "BATCH-A" in batches["BATCH-LATE"]["adjacent_with"]
        assert batches["BATCH-RETRY"]["duplicate_count"] == 1

    def test_dispatch_full_remaining_excludes_late_data_and_records_unsent(self, setup_data_dir):
        from app.services.ingest_monitor import IngestMonitorService

        point = "SENS-MON-E"
        _write_edge(setup_data_dir, point, 8)
        svc = IngestMonitorService()
        svc.register_receipts(point, "BATCH-A", [_sample(i) for i in [1, 2, 7, 8]])
        query = svc.query_gaps(point, "2026-11-24T10:00:00", "2026-11-24T10:00:08")
        assert query["missing_count"] == 4  # 缺口 3,4,5,6

        # 迟到批次覆盖 3,4（一半），整量派发时只续发剩余 5,6
        svc.register_receipts(point, "BATCH-LATE", [_sample(3), _sample(4)], arrival_seq=2)
        dispatch = svc.dispatch_replay(query["snapshot_id"], point)
        assert dispatch["sent_indexes"] == [5, 6]
        assert dispatch["late_covered_after_query"] == [3, 4]
        assert dispatch["unsent_indexes"] == []

    def test_dispatch_rejects_range_outside_snapshot_gap(self, setup_data_dir):
        from app.services.ingest_monitor import IngestMonitorService

        point = "SENS-MON-F"
        _write_edge(setup_data_dir, point, 6)
        svc = IngestMonitorService()
        svc.register_receipts(point, "BATCH-A", [_sample(1), _sample(2)])
        query = svc.query_gaps(point, "2026-11-24T10:00:00", "2026-11-24T10:00:06")

        # 1、2 在查询时已经实收，不允许当成缺口重发
        with pytest.raises(ValueError, match="不在查询快照缺口范围内"):
            svc.dispatch_replay(query["snapshot_id"], point, range_indexes=[1, 3])


class TestSettleByActualReceipt:
    """重传落地结算：以实收凭证为准，发送成功不关闭整段。"""

    def test_sent_but_not_received_keeps_gap_open(self, setup_data_dir):
        from app.services.ingest_monitor import IngestMonitorService
        from app.store import store

        point = "SENS-MON-G"
        _write_edge(setup_data_dir, point, 6)
        svc = IngestMonitorService()
        svc.register_receipts(point, "BATCH-A", [_sample(i) for i in [1, 2, 3]])
        query = svc.query_gaps(point, "2026-11-24T10:00:00", "2026-11-24T10:00:06")
        dispatch = svc.dispatch_replay(query["snapshot_id"], point)
        assert dispatch["sent_indexes"] == [4, 5, 6]

        # 重传请求已发送但没有任何样本实际落地
        settle = svc.settle_replay(dispatch["replay_key"], received_samples=[])
        assert settle["accepted_count"] == 0
        assert settle["fully_closed"] is False
        assert settle["remaining_missing_indexes"] == [4, 5, 6]
        entry = store.find("range_replay", settle["replay_id"])
        assert entry["status"] == "待重采"

        # 同一申请不能重复结算
        with pytest.raises(ValueError, match="已结算"):
            svc.settle_replay(dispatch["replay_key"], received_samples=[_sample(4)])

    def test_full_settlement_closes_gap_and_completes_replay(self, setup_data_dir):
        from app.services.ingest_monitor import IngestMonitorService
        from app.store import store

        point = "SENS-MON-H"
        _write_edge(setup_data_dir, point, 5)
        svc = IngestMonitorService()
        svc.register_receipts(point, "BATCH-A", [_sample(1), _sample(2)])
        query = svc.query_gaps(point, "2026-11-24T10:00:00", "2026-11-24T10:00:05")
        dispatch = svc.dispatch_replay(query["snapshot_id"], point)

        settle = svc.settle_replay(
            dispatch["replay_key"], received_samples=[_sample(i) for i in [3, 4, 5]]
        )
        assert settle["fully_closed"] is True
        assert settle["status"] == "已结清"
        assert settle["remaining_missing_count"] == 0
        assert settle["final_received_points"] == 5
        entry = store.find("range_replay", settle["replay_id"])
        assert entry["status"] == "已完成"
        assert entry["pending"] is False

    def test_snapshot_persisted_as_capture_baseline(self, setup_data_dir):
        from app.services.ingest_monitor import IngestMonitorService

        point = "SENS-MON-I"
        _write_edge(setup_data_dir, point, 4)
        svc = IngestMonitorService()
        svc.register_receipts(point, "BATCH-A", [_sample(1)])
        query = svc.query_gaps(point, "2026-11-24T10:00:00", "2026-11-24T10:00:04")

        snapshot = svc.get_snapshot(query["snapshot_id"])
        assert snapshot["point"] == point
        assert snapshot["missing_indexes"] == [2, 3, 4]
        assert snapshot["expected_indexes"] == [1, 2, 3, 4]
        snapshot_file = setup_data_dir / "gap_reconcile" / f"{query['snapshot_id']}.json"
        assert snapshot_file.exists()

        with pytest.raises(KeyError):
            svc.get_snapshot("GAPQ-NOT-EXIST-001")
