"""裁决语义与 API 行为测试。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import create_app


@pytest.fixture()
def client(tmp_path):
    app = create_app(str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def mk(tid, deps=(), changed=False, never=False, reads=None):
    """构造导入项：默认上次执行记录与本轮一致（可复用）。"""
    item = {
        "id": tid,
        "def_digest": f"def:{tid}",
        "src_digest": f"src:{tid}",
        "static_deps": list(deps),
        "dyn_deps": [],
        "prev_reads": {d: f"art:{d}" for d in deps} if reads is None else reads,
    }
    if not never:
        item["prev_artifact_digest"] = f"art:{tid}"
        item["prev_def_digest"] = f"def:{tid}" + (":old" if changed else "")
        item["prev_src_digest"] = f"src:{tid}"
    return item


def import_ok(client, tasks):
    r = client.post("/api/graph/import", json={"tasks": tasks})
    assert r.status_code == 200, r.text
    return r.json()


def states(client):
    plan = client.get("/api/plan").json()
    return {c["id"]: c["state"] for c in plan["changes"]}


def detail(client, tid):
    r = client.get("/api/tasks/" + tid)
    assert r.status_code == 200, r.text
    return r.json()


def claim(client, tid=None):
    body = {"task_id": tid} if tid else {}
    return client.post("/api/claim", json=body).json()["claim"]


def report(client, results):
    return client.post("/api/results", json={"results": results})


# ---------------------------------------------------------------- 复用判定

def test_reuse_when_all_match(client):
    import_ok(client, [mk("A"), mk("B", deps=["A"]), mk("C", deps=["B"])])
    assert states(client) == {"A": "reusable", "B": "reusable", "C": "reusable"}


def test_never_executed_is_executable_with_reason(client):
    import_ok(client, [mk("A", never=True)])
    d = detail(client, "A")
    assert d["state"] == "executable"
    assert d["reason"] == [{"task": "A", "why": "从未执行"}]


def test_definition_change_makes_executable(client):
    import_ok(client, [mk("A"), mk("B", deps=["A"], changed=True)])
    d = detail(client, "B")
    assert d["state"] == "executable"
    assert d["reason"] == [{"task": "B", "why": "任务定义已变更"}]


def test_source_change_makes_executable(client):
    t = mk("A")
    t["prev_src_digest"] = "src:A:old"
    import_ok(client, [t])
    d = detail(client, "A")
    assert d["state"] == "executable"
    assert d["reason"][0]["why"] == "源文件摘要已变更"


def test_new_dependency_without_prior_read_not_reusable(client):
    import_ok(client, [mk("A"), mk("B", deps=["A"], reads={})])
    d = detail(client, "B")
    assert d["state"] == "executable"
    assert "新增依赖" in d["reason"][0]["why"]


# ---------------------------------------------------------------- 等待与传播

def test_unfinished_upstream_blocks_downstream(client):
    import_ok(client, [mk("A", changed=True), mk("B", deps=["A"]), mk("C", deps=["B"])])
    st = states(client)
    assert st["A"] == "executable"
    assert st["B"] == "waiting"
    assert st["C"] == "waiting"
    # B 的原因链穿过 A：根因 -> B，长度 2
    d = detail(client, "B")
    assert len(d["reason"]) == 2
    assert d["reason"][0] == {"task": "A", "why": "任务定义已变更"}


def test_upstream_same_artifact_keeps_downstream_reusable(client):
    """上游重跑但产物摘要不变 -> 下游仍复用。"""
    import_ok(client, [mk("A", changed=True), mk("B", deps=["A"]), mk("C", deps=["B"])])
    c = claim(client, "A")
    r = report(client, [{"task_id": "A", "fingerprint": c["fingerprint"],
                         "reads": {}, "artifact_digest": "art:A"}])  # 与上次一致
    assert r.status_code == 200 and r.json()["applied"] == ["A"]
    st = states(client)
    assert st["A"] == "done"
    assert st["B"] == "reusable"   # 产物摘要不变，直接复用
    assert st["C"] == "reusable"


def test_upstream_new_artifact_cascades(client):
    import_ok(client, [mk("A", changed=True), mk("B", deps=["A"]), mk("C", deps=["B"])])
    c = claim(client, "A")
    report(client, [{"task_id": "A", "fingerprint": c["fingerprint"],
                     "reads": {}, "artifact_digest": "art:A:2"}])
    st = states(client)
    assert st["B"] == "executable"   # 上游产物变化 -> 可执行
    assert st["C"] == "waiting"      # B 未落定 -> 等待
    # 执行 B（产物不变）-> C 复用
    cb = claim(client, "B")
    assert cb["deps"] == {"A": "art:A:2"}
    report(client, [{"task_id": "B", "fingerprint": cb["fingerprint"],
                     "reads": {"A": "art:A:2"}, "artifact_digest": "art:B"}])
    assert states(client)["C"] == "reusable"


def test_shortest_reason_chain(client):
    """失效原因链取最短：D 同时被长链与短链影响时选短链。"""
    # A 变 -> B -> C -> D；另有 X 变 -> D（短链）
    import_ok(client, [
        mk("A", changed=True), mk("B", deps=["A"]), mk("C", deps=["B"]),
        mk("X", changed=True),
        mk("D", deps=["C", "X"]),
    ])
    # 全部跑完（产物都变）
    for tid in ["A", "X"]:
        c = claim(client, tid)
        report(client, [{"task_id": tid, "fingerprint": c["fingerprint"],
                         "reads": {}, "artifact_digest": f"art:{tid}:2"}])
    for tid in ["B", "C"]:
        c = claim(client, tid)
        report(client, [{"task_id": tid, "fingerprint": c["fingerprint"],
                         "reads": c["deps"], "artifact_digest": f"art:{tid}:2"}])
    d = detail(client, "D")
    assert d["state"] == "executable"
    # 最短链应经由 X（2 跳）而非 A->B->C（4 跳）
    assert len(d["reason"]) == 2
    assert d["reason"][0]["task"] == "X"
    assert d["reason"][1]["task"] == "D"


# ---------------------------------------------------------------- 指纹与过期

def test_wrong_fingerprint_rejected_without_state_change(client):
    import_ok(client, [mk("A", changed=True)])
    before = detail(client, "A")
    r = report(client, [{"task_id": "A", "fingerprint": "bogus",
                         "reads": {}, "artifact_digest": "art:A:9"}])
    assert r.status_code == 200
    assert r.json()["applied"] == []
    assert r.json()["rejected"][0]["task_id"] == "A"
    after = detail(client, "A")
    assert after["state"] == before["state"] == "executable"
    assert after["prev_artifact_digest"] == before["prev_artifact_digest"]


def test_stale_result_rejected_after_reimport(client):
    """领取后任务图被重新导入（输入已变）-> 旧指纹结果过期且不改状态。"""
    import_ok(client, [mk("A", changed=True)])
    c = claim(client, "A")
    # 重新导入：A 的定义再次变化
    t = mk("A")
    t["def_digest"] = "def:A:new"
    t["prev_def_digest"] = "def:A"
    import_ok(client, [t])
    r = report(client, [{"task_id": "A", "fingerprint": c["fingerprint"],
                         "reads": {}, "artifact_digest": "art:A:2"}])
    assert r.json()["applied"] == []
    assert detail(client, "A")["state"] == "executable"


def test_read_digest_mismatch_rejected(client):
    import_ok(client, [mk("A"), mk("B", deps=["A"], changed=True)])
    c = claim(client, "B")
    r = report(client, [{"task_id": "B", "fingerprint": c["fingerprint"],
                         "reads": {"A": "art:A:WRONG"}, "artifact_digest": "art:B:2"}])
    assert r.json()["applied"] == []
    assert detail(client, "B")["state"] == "executable"


def test_double_report_rejected(client):
    import_ok(client, [mk("A", changed=True)])
    c = claim(client, "A")
    r1 = report(client, [{"task_id": "A", "fingerprint": c["fingerprint"],
                          "reads": {}, "artifact_digest": "art:A:2"}])
    assert r1.json()["applied"] == ["A"]
    # 同一指纹再次上报：任务已 done，过期拒绝
    r2 = report(client, [{"task_id": "A", "fingerprint": c["fingerprint"],
                          "reads": {}, "artifact_digest": "art:A:3"}])
    assert r2.json()["applied"] == []
    assert detail(client, "A")["artifact_digest"] == "art:A:2"


# ---------------------------------------------------------------- 动态依赖与回滚

def test_dynamic_deps_updated_by_report(client):
    import_ok(client, [mk("A"), mk("X"), mk("B", deps=["A"], changed=True)])
    c = claim(client, "B")
    r = report(client, [{"task_id": "B", "fingerprint": c["fingerprint"],
                         "reads": {"A": "art:A", "X": "art:X"},
                         "artifact_digest": "art:B:2"}])
    assert r.json()["applied"] == ["B"]
    d = detail(client, "B")
    assert d["dyn_deps"] == ["X"]          # X 不是静态依赖 -> 记为动态前置
    assert d["prev_reads"] == {"A": "art:A", "X": "art:X"}
    assert d["state"] == "done"


def test_missing_dependency_rolls_back_whole_report(client):
    """整次上报回滚：同批已校验通过的结果也不得生效。"""
    import_ok(client, [mk("A", changed=True), mk("B", changed=True)])
    ca, cb = claim(client, "A"), claim(client, "B")
    r = report(client, [
        {"task_id": "A", "fingerprint": ca["fingerprint"], "reads": {},
         "artifact_digest": "art:A:2"},
        {"task_id": "B", "fingerprint": cb["fingerprint"], "reads": {"ghost": "x"},
         "artifact_digest": "art:B:2"},
    ])
    assert r.status_code == 409
    assert r.json()["detail"]["kind"] == "missing_dependency"
    st = states(client)
    assert st["A"] == "executable"   # 已回滚，未变成 done
    assert st["B"] == "executable"


def test_cycle_rolls_back_whole_report(client, tmp_path):
    """新依赖成环 -> 整次上报回滚（白盒构造：不变式之外的临界情形）。"""
    from app.engine import DONE, Engine, ReportConflict
    from app.models import ImportTask, ResultItem
    from app.store import Store

    engine = Engine(Store(str(tmp_path / "cycle.db")))
    engine.import_graph([
        ImportTask(id="X", def_digest="dX", src_digest="sX",
                   prev_def_digest="dX0", prev_src_digest="sX",
                   prev_artifact_digest="art:X"),
        # Y 依赖 X，历史记录显示它读过 X 的旧产物
        ImportTask(id="Y", def_digest="dY", src_digest="sY",
                   prev_def_digest="dY", prev_src_digest="sY",
                   prev_artifact_digest="art:Y",
                   static_deps=["X"], prev_reads={"X": "art:X"}),
    ])
    # 白盒：把 Y 置为已完成（模拟它在 X 失效前已跑完的历史时刻）
    y = engine.tasks["Y"]
    engine.counts[y.state] -= 1
    engine.counts[DONE] += 1
    y.state = DONE
    y.artifact = "art:Y"
    # X 可执行，领取后上报：读取 Y -> 新边 X->Y 与既有 Y->X 成环
    c = engine.claim("X")
    assert c is not None
    with pytest.raises(ReportConflict) as ei:
        engine.report([ResultItem(task_id="X", fingerprint=c["fingerprint"],
                                  reads={"Y": "art:Y"}, artifact_digest="art:X:2")])
    assert ei.value.kind == "cycle"
    # 回滚：X 仍可执行、无新动态依赖、版本未变
    assert engine.tasks["X"].state == "executable"
    assert engine.tasks["X"].dyn_deps == set()
    assert engine.plan(0)["version"] == engine.version


# ---------------------------------------------------------------- 计划查询

def test_plan_since_returns_only_changes(client):
    import_ok(client, [mk("A", changed=True), mk("B"), mk("C")])
    v = client.get("/api/summary").json()["version"]
    c = claim(client, "A")
    report(client, [{"task_id": "A", "fingerprint": c["fingerprint"],
                     "reads": {}, "artifact_digest": "art:A:2"}])
    inc = client.get(f"/api/plan?since={v}").json()
    assert [c_["id"] for c_ in inc["changes"]] == ["A"]
    assert inc["version"] > v
    # 再次查询无新变化
    inc2 = client.get(f"/api/plan?since={inc['version']}").json()
    assert inc2["changes"] == []


def test_summary_counts(client):
    import_ok(client, [mk("A", changed=True), mk("B", deps=["A"]), mk("C")])
    s = client.get("/api/summary").json()
    assert s["counts"] == {"reusable": 1, "waiting": 1, "executable": 1, "done": 0}
    assert s["total"] == 3


# ---------------------------------------------------------------- 导入校验

def test_import_rejects_duplicate_ids(client):
    r = client.post("/api/graph/import", json={"tasks": [mk("A"), mk("A")]})
    assert r.status_code == 400


def test_import_rejects_missing_dep(client):
    r = client.post("/api/graph/import", json={"tasks": [mk("A", deps=["ghost"])]})
    assert r.status_code == 400


def test_import_rejects_cycle(client):
    r = client.post("/api/graph/import",
                    json={"tasks": [mk("A", deps=["B"]), mk("B", deps=["A"])]})
    assert r.status_code == 400


# ---------------------------------------------------------------- 重启一致

def test_restart_consistency(client, tmp_path):
    db = str(tmp_path / "restart.db")
    app1 = create_app(db)
    with TestClient(app1) as c1:
        import_ok(c1, [mk("A", changed=True), mk("B", deps=["A"]), mk("C")])
        cl = claim(c1, "A")
        report(c1, [{"task_id": "A", "fingerprint": cl["fingerprint"],
                     "reads": {}, "artifact_digest": "art:A:2"}])
        plan1 = c1.get("/api/plan").json()
        summary1 = c1.get("/api/summary").json()
    # 模拟重启：同一数据库文件新建应用
    app2 = create_app(db)
    with TestClient(app2) as c2:
        assert c2.get("/api/plan").json() == plan1
        assert c2.get("/api/summary").json() == summary1
        # 裁决可继续：B 现在可执行
        cl2 = claim(c2, "B")
        assert cl2 is not None and cl2["deps"] == {"A": "art:A:2"}
