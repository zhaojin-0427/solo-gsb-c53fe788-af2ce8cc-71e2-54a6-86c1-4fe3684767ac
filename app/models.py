"""API 请求模型。"""
from __future__ import annotations

from pydantic import BaseModel, Field


class ImportTask(BaseModel):
    """任务图导入项：本轮与上次执行的定义、摘要与依赖信息。"""

    id: str
    def_digest: str                              # 本轮任务定义摘要
    src_digest: str                              # 本轮源文件摘要
    prev_def_digest: str | None = None           # 上次执行时的定义摘要
    prev_src_digest: str | None = None           # 上次执行时的源文件摘要
    prev_artifact_digest: str | None = None      # 上次执行的产物摘要
    static_deps: list[str] = Field(default_factory=list)   # 静态前置任务
    dyn_deps: list[str] = Field(default_factory=list)      # 上次发现的动态前置任务
    prev_reads: dict[str, str] = Field(default_factory=dict)  # 上次执行读到的上游产物摘要


class ImportRequest(BaseModel):
    tasks: list[ImportTask]


class ClaimRequest(BaseModel):
    task_id: str | None = None  # 为空则领取任意一个可执行任务


class ResultItem(BaseModel):
    """单条执行结果：领取时的输入指纹 + 实际读取的依赖及摘要 + 新产物摘要。"""

    task_id: str
    fingerprint: str
    reads: dict[str, str] = Field(default_factory=dict)
    artifact_digest: str


class ReportRequest(BaseModel):
    results: list[ResultItem]
