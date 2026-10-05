"""演示用任务图生成器（供页面一键体验）。"""
from __future__ import annotations

import hashlib
import random

from .models import ImportTask


def _tok(*parts: str) -> str:
    return hashlib.sha256(":".join(parts).encode()).hexdigest()[:16]


def build_demo_graph(seed: int = 42, layers: int = 8, width: int = 25) -> list[ImportTask]:
    """分层随机 DAG：约 18% 任务定义/源文件变更，5% 从未执行。"""
    rng = random.Random(seed)
    tasks: list[ImportTask] = []
    ids_by_layer = [[f"//demo/L{l}/t{i}" for i in range(width)] for l in range(layers)]
    for l in range(layers):
        for i in range(width):
            tid = ids_by_layer[l][i]
            deps = [] if l == 0 else rng.sample(ids_by_layer[l - 1], k=rng.randint(1, 3))
            def_d = _tok("def", tid)
            src_d = _tok("src", tid)
            roll = rng.random()
            never = roll < 0.05
            changed = not never and roll < 0.23
            item = ImportTask(
                id=tid,
                def_digest=def_d,
                src_digest=src_d,
                static_deps=deps,
                dyn_deps=[],
                prev_reads={d: _tok("art", d) for d in deps},
            )
            if not never:
                item.prev_artifact_digest = _tok("art", tid)
                item.prev_def_digest = def_d + ":old" if changed else def_d
                item.prev_src_digest = src_d
            tasks.append(item)
    return tasks
