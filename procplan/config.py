from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List


@dataclass(frozen=True)
class GPU:
    """Single GPU definition for a node."""

    id: str
    kind: str


@dataclass(frozen=True)
class Node:
    """Node metadata loaded from configuration."""

    id: str
    name: str
    gpus: List[GPU]

    @property
    def gpu_count(self) -> int:
        return len(self.gpus)


@dataclass(frozen=True)
class Config:
    """Application configuration derived from JSON file."""

    nodes: List[Node]

    @property
    def nodes_by_id(self) -> Dict[str, Node]:
        return {node.id: node for node in self.nodes}


def load_config(path: str | Path) -> Config:
    with open(path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)

    nodes: List[Node] = []
    seen_ids: set[str] = set()
    for node_obj in raw.get("nodes", []):
        node_id = str(node_obj.get("id", "")).strip()
        if not node_id:
            raise ValueError("Each node object in configuration must include a non-empty 'id'")
        if node_id in seen_ids:
            raise ValueError(f"Duplicate node id '{node_id}' in configuration")
        seen_ids.add(node_id)

        name = str(node_obj.get("name") or node_id)
        gpu_defs = node_obj.get("gpus") or []
        gpus: List[GPU] = []
        seen_gpu_ids: set[str] = set()
        for gpu_obj in gpu_defs:
            gpu_id = str(gpu_obj.get("id", "")).strip()
            if not gpu_id:
                raise ValueError(f"Node '{node_id}' contains a GPU without an 'id'")
            if gpu_id in seen_gpu_ids:
                raise ValueError(f"Duplicate GPU id '{gpu_id}' for node '{node_id}'")
            seen_gpu_ids.add(gpu_id)
            kind = str(gpu_obj.get("kind") or "unspecified")
            gpus.append(GPU(id=gpu_id, kind=kind))
        nodes.append(Node(id=node_id, name=name, gpus=gpus))

    if not nodes:
        raise ValueError("Configuration must include at least one node")

    return Config(nodes=nodes)

