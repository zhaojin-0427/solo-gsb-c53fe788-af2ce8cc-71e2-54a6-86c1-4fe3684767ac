"""性能基准：十万任务 / 二十万依赖下，一千次上报 + 增量计划查询的总耗时。

用法：
    python scripts/benchmark.py [--tasks 100000] [--reports 1000] [--db PATH]

验证点：
1. 导入 10 万任务、20 万依赖；
2. 连续 1000 次（领取 -> 上报 -> 仅返回变动项的计划查询），每次影响不超过十项，
   总耗时不超过 10 秒；
3. 重启（同一数据库重建应用）后裁决结果一致。
"""
from __future__ import annotations

import argparse
import hashlib
import random
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi.testclient import TestClient

from app.main import create_app


def tok(*parts: str) -> str:
    return hashlib.sha256(":".join(parts).encode()).hexdigest()[:16]


def gen_graph(n_tasks: int, n_deps: int, n_changed: int, seed: int = 7):
    """分层 DAG：每层向前一层连边；中层挑 n_changed 个任务变更定义。

    每个中层任务约有 2 个直接下游，单次上报影响不超过十项。
    """
    rng = random.Random(seed)
    per_dep = max(1, n_deps // n_tasks)  # 每个非首层任务的依赖数
    width = 1000
    layers = max(2, n_tasks // width)
    ids = [[f"//pkg{l}:t{i}" for i in range(width)] for l in range(layers)]
    step = max(1, width // max(1, n_changed))
    changed = {f"//pkg{layers // 2}:t{i}" for i in range(0, width, step)}
    tasks = []
    total_deps = 0
    for l in range(layers):
        for i in range(width):
            tid = ids[l][i]
            deps = [] if l == 0 else rng.sample(ids[l - 1], k=min(per_dep, width))
            total_deps += len(deps)
            t = {
                "id": tid,
                "def_digest": tok("def", tid),
                "src_digest": tok("src", tid),
                "static_deps": deps,
                "dyn_deps": [],
                "prev_reads": {d: tok("art", d) for d in deps},
                "prev_artifact_digest": tok("art", tid),
                "prev_def_digest": tok("def", tid) + (":old" if tid in changed else ""),
                "prev_src_digest": tok("src", tid),
            }
            tasks.append(t)
    return tasks, sorted(changed), total_deps


def full_plan_fingerprint(client: TestClient) -> str:
    h = hashlib.sha256()
    plan = client.get("/api/plan").json()
    for c in plan["changes"]:
        h.update(f"{c['id']}|{c['state']}|{c['artifact_digest']}|{c['reason']}\n".encode())
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tasks", type=int, default=100_000)
    ap.add_argument("--deps", type=int, default=200_000)
    ap.add_argument("--reports", type=int, default=1000)
    ap.add_argument("--db", default=None)
    args = ap.parse_args()

    db = args.db or tempfile.mktemp(prefix="bench-", suffix=".db")
    print(f"数据库: {db}")
    print(f"生成任务图: {args.tasks} 任务 / 约 {args.deps} 依赖 …")
    tasks, changed, total_deps = gen_graph(args.tasks, args.deps, args.reports // 2)
    print(f"实际依赖数: {total_deps}，变更任务数: {len(changed)}")

    app = create_app(db)
    with TestClient(app) as client:
        t0 = time.perf_counter()
        r = client.post("/api/graph/import", json={"tasks": tasks})
        assert r.status_code == 200, r.text[:500]
        t_import = time.perf_counter() - t0
        info = r.json()
        print(f"导入完成: {info['tasks']} 任务 / {info['deps']} 依赖，"
              f"耗时 {t_import:.2f}s，状态分布 {info['counts']}")

        version = client.get("/api/summary").json()["version"]
        t0 = time.perf_counter()
        plan_items = 0
        done_reports = 0

        def one_round(tid=None, new_artifact=True):
            nonlocal version, plan_items, done_reports
            c = client.post("/api/claim",
                            json={"task_id": tid} if tid else {}).json()["claim"]
            assert c, f"领取失败: {tid}"
            art = tok("art2", c["task_id"]) if new_artifact else tok("art", c["task_id"])
            r = client.post("/api/results", json={"results": [{
                "task_id": c["task_id"],
                "fingerprint": c["fingerprint"],
                "reads": c["deps"],
                "artifact_digest": art,
            }]})
            assert r.status_code == 200 and r.json()["applied"], r.text[:300]
            p = client.get(f"/api/plan?since={version}").json()
            version = p["version"]
            plan_items += len(p["changes"])
            done_reports += 1

        # 阶段一：上报全部变更任务（新产物摘要，触发下游重估）
        for tid in changed:
            one_round(tid, new_artifact=True)
        # 阶段二：动态领取被解锁的任务，产物摘要与上次一致（下游复用）
        while done_reports < args.reports:
            one_round(None, new_artifact=False)
        t_loop = time.perf_counter() - t0
        print(f"{done_reports} 次上报 + 增量计划查询总耗时: {t_loop:.2f}s "
              f"(计划变动项合计 {plan_items} 条)")
        assert t_loop < 10.0, f"超出 10 秒预算: {t_loop:.2f}s"

        summary1 = client.get("/api/summary").json()
        fp1 = full_plan_fingerprint(client)

    # 模拟重启：同一数据库文件重建应用
    app2 = create_app(db)
    with TestClient(app2) as client2:
        summary2 = client2.get("/api/summary").json()
        fp2 = full_plan_fingerprint(client2)
    assert summary1 == summary2, "重启后汇总不一致"
    assert fp1 == fp2, "重启后计划不一致"
    print("重启后裁决一致 ✓")
    print(f"基准通过：导入 {t_import:.2f}s，上报+查询 {t_loop:.2f}s（预算 10s）")


if __name__ == "__main__":
    main()
