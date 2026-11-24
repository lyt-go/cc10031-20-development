"""接入监控专用路由：接收凭证登记、缺口查询、重传派发与按实收结算。

这些路径固定且以 /api/ingest-monitor/ 开头，注册顺序在通用资源路由之前，
避免被 /api/{module} 动态路由抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.ingest_monitor import IngestMonitorService

router = APIRouter(prefix="/api/ingest-monitor", tags=["接入监控对账"])
service = IngestMonitorService()


class ReceiptPayload(BaseModel):
    point: str
    batch: str
    arrival_seq: int | None = None
    samples: list[dict[str, Any]] = Field(default_factory=list)


class DispatchPayload(BaseModel):
    snapshot_id: str
    point: str
    range_indexes: list[int] | None = None
    replay_key: str | None = None


class SettlePayload(BaseModel):
    received_samples: list[dict[str, Any]] | None = None
    batch: str | None = None


@router.post("/receipts")
def register_receipts(payload: ReceiptPayload) -> dict[str, Any]:
    """登记一批实际到达样本作为接收凭证；重复到达不重复计点。"""
    try:
        return service.register_receipts(
            payload.point, payload.batch, payload.samples, payload.arrival_seq)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/gaps")
def query_gaps(
    point: str = Query(...),
    start_time: str | None = Query(default=None),
    end_time: str | None = Query(default=None),
) -> dict[str, Any]:
    """按事件时间窗口对账实收样本，定位剩余缺口与可重传区间，并固化查询快照。"""
    try:
        return service.query_gaps(point, start_time, end_time)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/snapshots/{snapshot_id}")
def get_snapshot(snapshot_id: str) -> dict[str, Any]:
    """读取某次缺口查询的捕获快照（派发与结算的基线）。"""
    try:
        return service.get_snapshot(snapshot_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/replays")
def dispatch_replay(payload: DispatchPayload) -> dict[str, Any]:
    """依据查询快照发起重传；派发前重新对账，剔除查询后迟到补齐的部分。"""
    try:
        return service.dispatch_replay(
            payload.snapshot_id, payload.point, payload.range_indexes, payload.replay_key)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/replays/{replay_key}/settle")
def settle_replay(replay_key: str, payload: SettlePayload) -> dict[str, Any]:
    """重传落地后按实际实收结算；半段补齐只更新剩余缺口，不关闭整段。"""
    try:
        return service.settle_replay(replay_key, payload.received_samples, payload.batch)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
