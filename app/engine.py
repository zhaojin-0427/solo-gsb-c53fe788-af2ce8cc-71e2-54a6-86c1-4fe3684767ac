"""增量构建裁决引擎。

内存中保存任务图与裁决状态用于快速计算，所有变更在同一 SQLite 事务中落库，
重启后从数据库完整恢复，保证裁决一致。

裁决语义：
- 定义、源文件、所读上游产物摘要全部与上次执行一致 -> 可复用（reusable）；
- 有上游未落定（非 reusable/done）-> 待上游（waiting）；
- 其余 -> 可执行（executable）；结果被接受后 -> 已完成（done，本轮终态）。
- 失效任务携带"最短失效原因链"：自身原因长度为 1，否则取原因链最短的上游 + 1。
"""
from __future__ import annotations

import hashlib
import json
import threading
from collections import deque

REUSABLE = "reusable"      # 可复用
WAITING = "waiting"        # 待上游
EXECUTABLE = "executable"  # 可执行
DONE = "done"              # 已完成

MAX_CHAIN = 32  # 原因链最大长度，超出截断

_TASK_SLOTS = (
    "id", "def_digest", "src_digest",
    "has_prev", "prev_def", "prev_src", "prev_artifact",
    "static_deps", "dyn_deps", "prev_reads",
    "state", "artifact", "reason", "version",
)


class ReportConflict(Exception):
    """整次上报必须回滚（新依赖成环或引用缺失）。"""

    def __init__(self, kind: str, detail: str):
        super().__init__(detail)
        self.kind = kind
        self.detail = detail


class ImportInvalid(Exception):
    """导入图非法（重复 id、引用缺失、成环）。"""

    def __init__(self, detail: str):
        super().__init__(detail)
        self.detail = detail


class Task:
    __slots__ = _TASK_SLOTS

    def __init__(self):
        self.id = ""
        self.def_digest = ""
        self.src_digest = ""
        self.has_prev = False
        self.prev_def = None
        self.prev_src = None
        self.prev_artifact = None
        self.static_deps = set()
        self.dyn_deps = set()
        self.prev_reads = {}
        self.state = WAITING
        self.artifact = None
        self.reason = None
        self.version = 0

    def snapshot(self):
        # 所有可变字段只整体替换、从不就地修改，浅拷贝即可安全回滚
        return {k: getattr(self, k) for k in _TASK_SLOTS if k != "id"}

    def restore(self, snap):
        for k, v in snap.items():
            setattr(self, k, v)


class Engine:
    def __init__(self, store: Store):
        self.store = store
        self.conn = store.conn
        self.lock = threading.RLock()
        self.tasks = {}       # id -> Task
        self.dependents = {}  # id -> 依赖它的任务集合（反向边）
        self.executable = set()
        self.counts = {REUSABLE: 0, WAITING: 0, EXECUTABLE: 0, DONE: 0}
        self.version = 0
        self._load()

    # ------------------------------------------------------------------
    # 启动加载（重启后裁决一致）
    # ------------------------------------------------------------------

    def _load(self):
        conn = self.conn
        tasks = {}
        for row in conn.execute("SELECT * FROM tasks"):
            t = Task()
            t.id = row["id"]
            t.def_digest = row["def_digest"]
            t.src_digest = row["src_digest"]
            t.has_prev = bool(row["has_prev"])
            t.prev_def = row["prev_def"]
            t.prev_src = row["prev_src"]
            t.prev_artifact = row["prev_artifact"]
            tasks[t.id] = t
        for row in conn.execute("SELECT task_id, dep_id FROM deps"):
            tasks[row["task_id"]].static_deps.add(row["dep_id"])
        for row in conn.execute("SELECT task_id, dep_id FROM dyn_deps"):
            tasks[row["task_id"]].dyn_deps.add(row["dep_id"])
        for row in conn.execute("SELECT task_id, dep_id, digest FROM prev_reads"):
            tasks[row["task_id"]].prev_reads[row["dep_id"]] = row["digest"]
        for row in conn.execute("SELECT * FROM task_state"):
            t = tasks[row["task_id"]]
            t.state = row["state"]
            t.artifact = row["artifact"]
            t.reason = json.loads(row["reason"]) if row["reason"] else None
            t.version = row["version"]
        dependents = {tid: set() for tid in tasks}
        for t in tasks.values():
            for d in t.static_deps | t.dyn_deps:
                dependents[d].add(t.id)
        self.tasks = tasks
        self.dependents = dependents
        self.executable = {t.id for t in tasks.values() if t.state == EXECUTABLE}
        self.counts = {s: 0 for s in (REUSABLE, WAITING, EXECUTABLE, DONE)}
        for t in tasks.values():
            self.counts[t.state] += 1
        row = conn.execute("SELECT value FROM meta WHERE key='version'").fetchone()
        self.version = row["value"] if row else 0

    # ------------------------------------------------------------------
    # 裁决求值
    # ------------------------------------------------------------------

    @staticmethod
    def _deps_of(t):
        return t.static_deps | t.dyn_deps

    def _evaluate(self, tasks, tid):
        """根据当前图数据计算 (state, reason)。done 为本轮终态，不重算。"""
        t = tasks[tid]
        if t.state == DONE:
            return DONE, t.reason
        self_why = None
        if not t.has_prev:
            self_why = "从未执行"
        elif t.def_digest != t.prev_def:
            self_why = "任务定义已变更"
        elif t.src_digest != t.prev_src:
            self_why = "源文件摘要已变更"
        waiting_on = []
        mismatched = []
        new_dep = None
        for d in t.static_deps | t.dyn_deps:
            pr = t.prev_reads.get(d)
            if pr is None:
                if new_dep is None:
                    new_dep = d
                continue
            dt = tasks[d]
            if dt.artifact is None:
                waiting_on.append(d)
            elif dt.artifact != pr:
                mismatched.append(d)
        if self_why is None and new_dep is not None:
            self_why = f"新增依赖 {new_dep}（无历史读取记录）"
        if self_why is None and not waiting_on and not mismatched:
            return REUSABLE, None
        if self_why is not None:
            reason = [{"task": tid, "why": self_why}]
        else:
            # 最短失效原因链：选择原因链最短的上游，追加一跳
            best_dep = None
            best_reason = None
            best_len = None
            for d in mismatched + waiting_on:
                dr = tasks[d].reason
                ln = len(dr) if dr else 0
                if best_len is None or ln < best_len:
                    best_len, best_dep, best_reason = ln, d, dr
            if best_reason:
                if best_dep in mismatched:
                    why = f"上游 {best_dep} 的产物摘要发生变化"
                else:
                    why = f"等待上游 {best_dep} 完成"
                reason = best_reason + [{"task": tid, "why": why}]
                if len(reason) > MAX_CHAIN:
                    reason = reason[: MAX_CHAIN - 1] + [
                        {"task": tid, "why": "……（链路过长已截断）"}
                    ]
            else:
                # 上游本身可复用，但历史读取记录与其产物不一致
                reason = [{"task": tid, "why": f"记录的 {best_dep} 读取摘要与当前产物不一致"}]
        return (WAITING if waiting_on else EXECUTABLE), reason

    # ------------------------------------------------------------------
    # 导入任务图（整图替换 + 全量重算，单事务）
    # ------------------------------------------------------------------

    def import_graph(self, items):
        with self.lock:
            tasks, dependents = self._build_graph(items)
            new_version = self.version + 1
            executable = set()
            counts = {s: 0 for s in (REUSABLE, WAITING, EXECUTABLE, DONE)}
            # Kahn 拓扑序：依赖先于被依赖者求值
            indegree = {tid: len(t.static_deps | t.dyn_deps) for tid, t in tasks.items()}
            queue = deque([tid for tid, k in indegree.items() if k == 0])
            seen = 0
            while queue:
                tid = queue.popleft()
                seen += 1
                t = tasks[tid]
                state, reason = self._evaluate(tasks, tid)
                t.state = state
                t.reason = reason
                t.artifact = t.prev_artifact if state == REUSABLE else None
                t.version = new_version
                counts[state] += 1
                if state == EXECUTABLE:
                    executable.add(tid)
                for dep in dependents[tid]:
                    indegree[dep] -= 1
                    if indegree[dep] == 0:
                        queue.append(dep)
            if seen != len(tasks):
                raise ImportInvalid("任务图存在环路")
            self._persist_import(tasks, new_version)
            self.tasks = tasks
            self.dependents = dependents
            self.executable = executable
            self.counts = counts
            self.version = new_version
            return {
                "version": new_version,
                "tasks": len(tasks),
                "deps": sum(len(t.static_deps) + len(t.dyn_deps) for t in tasks.values()),
                "counts": dict(counts),
            }

    def _build_graph(self, items):
        tasks = {}
        for it in items:
            if it.id in tasks:
                raise ImportInvalid(f"任务 id 重复: {it.id}")
            t = Task()
            t.id = it.id
            t.def_digest = it.def_digest
            t.src_digest = it.src_digest
            t.has_prev = (
                it.prev_def_digest is not None
                and it.prev_src_digest is not None
                and it.prev_artifact_digest is not None
            )
            t.prev_def = it.prev_def_digest
            t.prev_src = it.prev_src_digest
            t.prev_artifact = it.prev_artifact_digest
            t.static_deps = set(it.static_deps)
            t.dyn_deps = set(it.dyn_deps) - t.static_deps
            t.prev_reads = dict(it.prev_reads)
            tasks[t.id] = t
        dependents = {tid: set() for tid in tasks}
        missing = []
        for t in tasks.values():
            for d in t.static_deps | t.dyn_deps:
                if d not in tasks:
                    missing.append(f"{t.id} -> {d}")
                else:
                    dependents[d].add(t.id)
        if missing:
            raise ImportInvalid("依赖引用了不存在的任务: " + ", ".join(missing[:5]))
        return tasks, dependents

    def _persist_import(self, tasks, version):
        conn = self.conn
        conn.execute("BEGIN")
        try:
            for tbl in ("prev_reads", "dyn_deps", "deps", "task_state", "tasks"):
                conn.execute(f"DELETE FROM {tbl}")
            conn.executemany(
                "INSERT INTO tasks (id, def_digest, src_digest, has_prev,"
                " prev_def, prev_src, prev_artifact) VALUES (?,?,?,?,?,?,?)",
                [
                    (t.id, t.def_digest, t.src_digest, int(t.has_prev),
                     t.prev_def, t.prev_src, t.prev_artifact)
                    for t in tasks.values()
                ],
            )
            conn.executemany(
                "INSERT INTO deps (task_id, dep_id) VALUES (?,?)",
                [(t.id, d) for t in tasks.values() for d in t.static_deps],
            )
            conn.executemany(
                "INSERT INTO dyn_deps (task_id, dep_id) VALUES (?,?)",
                [(t.id, d) for t in tasks.values() for d in t.dyn_deps],
            )
            conn.executemany(
                "INSERT INTO prev_reads (task_id, dep_id, digest) VALUES (?,?,?)",
                [(t.id, d, g) for t in tasks.values() for d, g in t.prev_reads.items()],
            )
            conn.executemany(
                "INSERT INTO task_state (task_id, state, artifact, reason, version)"
                " VALUES (?,?,?,?,?)",
                [
                    (t.id, t.state, t.artifact,
                     json.dumps(t.reason, ensure_ascii=False) if t.reason else None,
                     t.version)
                    for t in tasks.values()
                ],
            )
            conn.execute("UPDATE meta SET value=? WHERE key='version'", (version,))
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------------
    # 领取与输入指纹
    # ------------------------------------------------------------------

    def _fingerprint(self, t):
        """输入指纹：定义摘要 + 源文件摘要 + 各依赖当前产物摘要（按依赖名排序）。"""
        h = hashlib.sha256()
        h.update(t.def_digest.encode())
        h.update(b"\x00")
        h.update(t.src_digest.encode())
        h.update(b"\x00")
        for d in sorted(t.static_deps | t.dyn_deps):
            h.update(d.encode())
            h.update(b"=")
            h.update(self.tasks[d].artifact.encode())
            h.update(b";")
        return h.hexdigest()

    def claim(self, task_id=None):
        with self.lock:
            if task_id is None:
                if not self.executable:
                    return None
                task_id = next(iter(self.executable))
            t = self.tasks.get(task_id)
            if t is None or t.state != EXECUTABLE:
                return None
            return {
                "task_id": t.id,
                "fingerprint": self._fingerprint(t),
                "def_digest": t.def_digest,
                "src_digest": t.src_digest,
                "deps": {d: self.tasks[d].artifact
                         for d in sorted(t.static_deps | t.dyn_deps)},
            }

    # ------------------------------------------------------------------
    # 结果上报：核对指纹后原子更新依赖关系与计划
    # ------------------------------------------------------------------

    def report(self, results):
        with self.lock:
            undo = []        # (tid, snapshot)
            edge_undo = []   # (dep_id, tid, added?)
            mutated = {}
            applied = []
            rejected = []
            new_version = self.version + 1
            self.conn.execute("BEGIN")
            try:
                for r in results:
                    err = self._apply_one(r, undo, edge_undo, mutated)
                    if err is None:
                        applied.append(r.task_id)
                    else:
                        rejected.append({"task_id": r.task_id, "reason": err})
                if applied:
                    self._propagate(applied, undo, mutated)
                if mutated:
                    for t in mutated.values():
                        t.version = new_version
                    self._persist_mutation(mutated, applied, new_version)
                    self.version = new_version
                self.conn.execute("COMMIT")
            except BaseException:
                self.conn.execute("ROLLBACK")
                self._restore(undo, edge_undo)
                raise
            return {"version": self.version, "applied": applied, "rejected": rejected}

    def _apply_one(self, r, undo, edge_undo, mutated):
        """应用单条结果；返回 None 表示接受，否则返回拒绝原因（过期结果不改状态）。

        新依赖成环或引用缺失时抛出 ReportConflict，由调用方回滚整次上报。
        """
        t = self.tasks.get(r.task_id)
        if t is None:
            return "任务不存在"
        if t.state != EXECUTABLE:
            return f"任务状态为 {t.state}，结果已过期"
        if self._fingerprint(t) != r.fingerprint:
            return "输入指纹已过期"
        for d, digest in r.reads.items():
            dt = self.tasks.get(d)
            if dt is None:
                raise ReportConflict(
                    "missing_dependency", f"{t.id} 读取了不存在的依赖 {d}")
            if d == t.id:
                raise ReportConflict("cycle", f"{t.id} 依赖自身")
            if dt.artifact is None or dt.artifact != digest:
                return f"读取的依赖 {d} 产物摘要与当前不一致，结果已过期"
        undo.append((t.id, t.snapshot()))
        new_dyn = set(r.reads) - t.static_deps
        if new_dyn != t.dyn_deps:
            # 先对全部新增边做成环检查，再改图
            for d in new_dyn - t.dyn_deps:
                if d not in t.static_deps and self._reachable(d, t.id):
                    raise ReportConflict(
                        "cycle", f"新依赖 {t.id} -> {d} 会形成环路")
            for d in t.dyn_deps - new_dyn:
                self.dependents[d].discard(t.id)
                edge_undo.append((d, t.id, False))
            for d in new_dyn - t.dyn_deps:
                if d not in t.static_deps:
                    self.dependents[d].add(t.id)
                    edge_undo.append((d, t.id, True))
            t.dyn_deps = new_dyn
        self._transition(t, DONE, t.reason, mutated)  # 保留失效原因作为"为何重跑"
        t.has_prev = True
        t.prev_def = t.def_digest
        t.prev_src = t.src_digest
        t.prev_artifact = r.artifact_digest
        t.prev_reads = dict(r.reads)
        t.artifact = r.artifact_digest
        return None

    def _reachable(self, start, target):
        """沿依赖边从 start 出发能否到达 target（用于新边成环检查）。"""
        if start == target:
            return True
        seen = {start}
        stack = [start]
        while stack:
            x = stack.pop()
            for d in self.tasks[x].static_deps | self.tasks[x].dyn_deps:
                if d == target:
                    return True
                if d not in seen:
                    seen.add(d)
                    stack.append(d)
        return False

    def _propagate(self, seeds, undo, mutated):
        """从被接受的结果出发，沿反向边增量重算受影响的下游任务。"""
        in_queue = set()
        queue = []
        for tid in seeds:
            for dep in self.dependents.get(tid, ()):
                if dep not in in_queue:
                    in_queue.add(dep)
                    queue.append(dep)
        while queue:
            x = queue.pop()
            in_queue.discard(x)
            t = self.tasks[x]
            if t.state == DONE:
                continue
            old_pair = (t.artifact is not None, t.artifact)
            new_state, reason = self._evaluate(self.tasks, x)
            if new_state == t.state and reason == t.reason:
                continue
            undo.append((x, t.snapshot()))
            self._transition(t, new_state, reason, mutated)
            t.artifact = t.prev_artifact if new_state == REUSABLE else None
            new_pair = (t.artifact is not None, t.artifact)
            if new_pair != old_pair:
                for dep in self.dependents.get(x, ()):
                    if dep not in in_queue:
                        in_queue.add(dep)
                        queue.append(dep)

    def _transition(self, t, new_state, reason, mutated):
        old = t.state
        self.counts[old] -= 1
        self.counts[new_state] += 1
        if old == EXECUTABLE:
            self.executable.discard(t.id)
        if new_state == EXECUTABLE:
            self.executable.add(t.id)
        t.state = new_state
        t.reason = reason
        mutated[t.id] = t

    def _restore(self, undo, edge_undo):
        for tid, snap in reversed(undo):
            t = self.tasks[tid]
            self.counts[t.state] -= 1
            self.counts[snap["state"]] += 1
            if t.state == EXECUTABLE:
                self.executable.discard(tid)
            if snap["state"] == EXECUTABLE:
                self.executable.add(tid)
            t.restore(snap)
        for dep_id, tid, added in reversed(edge_undo):
            if added:
                self.dependents[dep_id].discard(tid)
            else:
                self.dependents[dep_id].add(tid)

    def _persist_mutation(self, mutated, applied_ids, version):
        conn = self.conn
        applied = [self.tasks[tid] for tid in applied_ids]
        # 已执行任务的"上次执行记录"随结果原子更新
        conn.executemany(
            "UPDATE tasks SET has_prev=1, prev_def=def_digest,"
            " prev_src=src_digest, prev_artifact=? WHERE id=?",
            [(t.prev_artifact, t.id) for t in applied],
        )
        conn.executemany("DELETE FROM prev_reads WHERE task_id=?",
                         [(t.id,) for t in applied])
        conn.executemany(
            "INSERT INTO prev_reads (task_id, dep_id, digest) VALUES (?,?,?)",
            [(t.id, d, g) for t in applied for d, g in t.prev_reads.items()],
        )
        conn.executemany("DELETE FROM dyn_deps WHERE task_id=?",
                         [(t.id,) for t in applied])
        conn.executemany(
            "INSERT INTO dyn_deps (task_id, dep_id) VALUES (?,?)",
            [(t.id, d) for t in applied for d in t.dyn_deps],
        )
        conn.executemany(
            "UPDATE task_state SET state=?, artifact=?, reason=?, version=?"
            " WHERE task_id=?",
            [
                (t.state, t.artifact,
                 json.dumps(t.reason, ensure_ascii=False) if t.reason else None,
                 version, t.id)
                for t in mutated.values()
            ],
        )
        conn.execute("UPDATE meta SET value=? WHERE key='version'", (version,))

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def plan(self, since=0):
        """返回 since 版本之后发生变化的任务（增量计划）。"""
        with self.lock:
            version = self.version
            rows = self.conn.execute(
                "SELECT task_id, state, artifact, reason, version FROM task_state"
                " WHERE version > ? ORDER BY version, task_id",
                (since,),
            ).fetchall()
            return {
                "version": version,
                "changes": [
                    {
                        "id": r["task_id"],
                        "state": r["state"],
                        "artifact_digest": r["artifact"],
                        "reason": json.loads(r["reason"]) if r["reason"] else None,
                        "version": r["version"],
                    }
                    for r in rows
                ],
            }

    def summary(self):
        with self.lock:
            return {
                "version": self.version,
                "total": len(self.tasks),
                "counts": dict(self.counts),
            }

    def list_tasks(self, state, limit=200, offset=0):
        with self.lock:
            rows = self.conn.execute(
                "SELECT task_id FROM task_state WHERE state=?"
                " ORDER BY task_id LIMIT ? OFFSET ?",
                (state, limit, offset),
            ).fetchall()
            return {"state": state, "tasks": [r["task_id"] for r in rows]}

    def task_detail(self, tid):
        with self.lock:
            t = self.tasks.get(tid)
            if t is None:
                return None
            d = {
                "id": t.id,
                "state": t.state,
                "def_digest": t.def_digest,
                "src_digest": t.src_digest,
                "prev_def_digest": t.prev_def,
                "prev_src_digest": t.prev_src,
                "prev_artifact_digest": t.prev_artifact,
                "artifact_digest": t.artifact,
                "static_deps": sorted(t.static_deps),
                "dyn_deps": sorted(t.dyn_deps),
                "prev_reads": dict(t.prev_reads),
                "reason": t.reason,
                "version": t.version,
            }
            if t.state == EXECUTABLE:
                d["fingerprint"] = self._fingerprint(t)
            return d
