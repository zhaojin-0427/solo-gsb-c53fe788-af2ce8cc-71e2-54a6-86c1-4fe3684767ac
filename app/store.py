"""SQLite 持久化层：保存任务图、依赖关系与裁决状态。"""
from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
  id            TEXT PRIMARY KEY,
  def_digest    TEXT NOT NULL,   -- 本轮任务定义摘要
  src_digest    TEXT NOT NULL,   -- 本轮源文件摘要
  has_prev      INTEGER NOT NULL DEFAULT 0,
  prev_def      TEXT,            -- 上次执行时的定义摘要
  prev_src      TEXT,            -- 上次执行时的源文件摘要
  prev_artifact TEXT             -- 上次执行的产物摘要
);
CREATE TABLE IF NOT EXISTS deps (
  task_id TEXT NOT NULL,
  dep_id  TEXT NOT NULL,
  PRIMARY KEY (task_id, dep_id)
);
CREATE TABLE IF NOT EXISTS dyn_deps (
  task_id TEXT NOT NULL,
  dep_id  TEXT NOT NULL,
  PRIMARY KEY (task_id, dep_id)
);
CREATE TABLE IF NOT EXISTS prev_reads (
  task_id TEXT NOT NULL,
  dep_id  TEXT NOT NULL,
  digest  TEXT NOT NULL,         -- 上次执行时读到的上游产物摘要
  PRIMARY KEY (task_id, dep_id)
);
CREATE TABLE IF NOT EXISTS task_state (
  task_id  TEXT PRIMARY KEY,
  state    TEXT NOT NULL,        -- reusable / waiting / executable / done
  artifact TEXT,                 -- 当前生效的产物摘要（未落定为空）
  reason   TEXT,                 -- 最短失效原因链（JSON），可复用时为空
  version  INTEGER NOT NULL      -- 最近一次状态变更的全局版本号
);
CREATE INDEX IF NOT EXISTS idx_task_state_version ON task_state(version);
CREATE INDEX IF NOT EXISTS idx_task_state_state   ON task_state(state);
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value INTEGER NOT NULL
);
"""


class Store:
    """单连接 SQLite（WAL）。所有写操作由 Engine 在锁内、显式事务中完成。"""

    def __init__(self, path: str):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.isolation_level = None  # 自动提交；事务由引擎显式控制
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA)
        self.conn.execute("INSERT OR IGNORE INTO meta (key, value) VALUES ('version', 0)")

    def close(self):
        self.conn.close()
