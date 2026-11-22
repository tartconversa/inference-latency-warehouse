"""源交付装载与版本划分。

``sources/`` 下每个 JSON 是一次交付（文件名前缀为到达序号）。本模块只做
装载、结构校验和按到达批次切分，不做任何业务解释，保证后续阶段拿到的是
不可变的原件视图。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional


@dataclass(frozen=True)
class Delivery:
    intake: int
    table: str
    records: tuple[dict, ...]
    path: str

    def by_id(self, record_id: str) -> dict:
        for row in self.records:
            if row["id"] == record_id:
                return row
        raise KeyError(f"{self.table} 内不存在记录 {record_id}")


@dataclass(frozen=True)
class SourceBundle:
    """某一资料版本（截至第 version 批到达）的全部原件。"""

    version: int
    deliveries: tuple[Delivery, ...]

    def table(self, name: str) -> tuple[dict, ...]:
        rows: list[dict] = []
        for delivery in self.deliveries:
            if delivery.table == name:
                rows.extend(delivery.records)
        return tuple(rows)

    def find_record(self, table: str, record_id: str) -> Optional[dict]:
        for row in self.table(table):
            if row["id"] == record_id:
                return row
        return None


def load_deliveries(source_dir: Path) -> list[Delivery]:
    """按文件名顺序装载全部交付，并做轻量结构校验。"""
    deliveries: list[Delivery] = []
    for path in sorted(Path(source_dir).glob("*.json")):
        with path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
        if not {"intake", "table", "records"} <= set(payload):
            raise ValueError(f"{path.name} 缺少 intake/table/records 外层字段")
        deliveries.append(
            Delivery(
                intake=int(payload["intake"]),
                table=payload["table"],
                records=tuple(payload["records"]),
                path=str(path),
            )
        )
    deliveries.sort(key=lambda item: (item.intake, item.table))
    _validate_uniqueness(deliveries)
    return deliveries


def _validate_uniqueness(deliveries: list[Delivery]) -> None:
    seen: set[tuple[str, str]] = set()
    for delivery in deliveries:
        for row in delivery.records:
            key = (delivery.table, row["id"])
            if key in seen:
                raise ValueError(f"记录 id 在表内重复：{key}")
            seen.add(key)


def iter_versions(source_dir: Path) -> Iterator[SourceBundle]:
    """逐批产生版本视图：v1=首批，v2=前两批，依此类推。"""
    deliveries = load_deliveries(source_dir)
    intakes = sorted({delivery.intake for delivery in deliveries})
    for index, intake_no in enumerate(intakes, start=1):
        bundle = tuple(d for d in deliveries if d.intake <= intake_no)
        yield SourceBundle(version=index, deliveries=bundle)


def load_version(source_dir: Path, version: Optional[int] = None) -> SourceBundle:
    """装载指定版本；默认装载到最后一批。"""
    versions = list(iter_versions(source_dir))
    if not versions:
        raise ValueError(f"{source_dir} 内没有任何交付")
    if version is None:
        return versions[-1]
    if not 1 <= version <= len(versions):
        raise ValueError(f"版本 {version} 超出范围 1..{len(versions)}")
    return versions[version - 1]
