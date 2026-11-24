"""业务路由汇总：固定路径的接入监控路由先注册，通用资源动态路由随后注册。"""
from __future__ import annotations

from app.routers import monitor, resource

ROUTERS = [monitor, resource]
