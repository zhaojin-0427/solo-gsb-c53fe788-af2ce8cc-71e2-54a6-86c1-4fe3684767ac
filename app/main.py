"""FastAPI 应用：任务与结果处理、计划查询、页面托管。"""
from __future__ import annotations

import hashlib
import os
import random
import time

from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import FileResponse

from .demo import build_demo_graph
from .engine import Engine, ImportInvalid, ReportConflict
from .models import ClaimRequest, ImportRequest, ReportRequest, ResultItem
from .store import Store

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


def create_app(db_path: str | None = None) -> FastAPI:
    app = FastAPI(title="增量构建裁决台")
    store = Store(db_path or os.environ.get("BUILDDB", "build.db"))
    engine = Engine(store)
    app.state.engine = engine

    @app.get("/api/health")
    def health():
        return {"ok": True}

    @app.post("/api/graph/import")
    def import_graph(req: ImportRequest):
        try:
            return engine.import_graph(req.tasks)
        except ImportInvalid as e:
            raise HTTPException(400, e.detail)

    @app.get("/api/plan")
    def plan(since: int = 0):
        """增量计划：仅返回 since 版本之后发生变化的任务。"""
        return engine.plan(since)

    @app.get("/api/summary")
    def summary():
        return engine.summary()

    @app.get("/api/tasks")
    def list_tasks(state: str, limit: int = 200, offset: int = 0):
        return engine.list_tasks(state, limit, offset)

    @app.get("/api/tasks/{task_id:path}")
    def task_detail(task_id: str):
        d = engine.task_detail(task_id)
        if d is None:
            raise HTTPException(404, "任务不存在")
        return d

    @app.post("/api/claim")
    def claim(req: ClaimRequest = Body(default_factory=ClaimRequest)):
        return {"claim": engine.claim(req.task_id)}

    @app.post("/api/results")
    def results(req: ReportRequest):
        try:
            return engine.report(req.results)
        except ReportConflict as e:
            # 新依赖成环或引用缺失：整次上报已回滚，状态未变
            raise HTTPException(409, {"kind": e.kind, "detail": e.detail})

    # ---------------- 演示辅助（页面一键体验用） ----------------

    @app.post("/api/demo/seed")
    def demo_seed():
        return engine.import_graph(build_demo_graph())

    @app.post("/api/demo/step")
    def demo_step():
        c = engine.claim()
        if c is None:
            return {"ran": None}
        detail = engine.task_detail(c["task_id"])
        # 60% 概率产物摘要与上次一致（演示下游仍可复用），否则产生新摘要
        art = detail["prev_artifact_digest"]
        if art is None or random.random() < 0.4:
            art = hashlib.sha256(
                f"{c['task_id']}:{time.time_ns()}".encode()
            ).hexdigest()[:16]
        res = engine.report([ResultItem(
            task_id=c["task_id"],
            fingerprint=c["fingerprint"],
            reads=c["deps"],
            artifact_digest=art,
        )])
        return {"ran": c["task_id"], "artifact_digest": art, "result": res}

    @app.get("/")
    def index():
        return FileResponse(os.path.join(STATIC_DIR, "index.html"))

    return app


app = create_app()
