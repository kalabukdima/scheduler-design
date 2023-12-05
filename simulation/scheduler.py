from dataclasses import dataclass
from typing import Dict, List, FrozenSet
import logging
from collections import defaultdict
from hashlib import md5

from utils import argmin


WorkerId = int
ChunkId = int

@dataclass
class Logs:
    access_frequencies: Dict[ChunkId, int]

@dataclass
class Assignment:
    workers: Dict[WorkerId, FrozenSet[ChunkId]]
    chunks: Dict[ChunkId, FrozenSet[WorkerId]]

    def __init__(self, workers: Dict[WorkerId, FrozenSet[ChunkId]]) -> None:
        self.workers = workers
        self.chunks = defaultdict(set)
        for worker, chunks in workers.items():
            for chunk in chunks:
                self.chunks[chunk].add(worker)

# Doesn't use logs. Simply distributes chunks evenly between workers
def assign_plain(
    workers: List[WorkerId],
    chunks: List[ChunkId],
    logs: Logs = Logs(access_frequencies={}),
):
    def get_hash(chunk: ChunkId, worker: WorkerId):
        # return md5(f"{chunk}:{worker}".encode("utf-8")).digest()
        return hash(f"{chunk}:{worker}")

    # Rendezvous hashing
    assigned_chunks = defaultdict(list)
    for chunk in chunks:
        hashes = [get_hash(chunk, worker) for worker in workers]
        chosen_worker = workers[argmin(hashes)]
        assigned_chunks[chosen_worker].append(chunk)
    
    logging.debug("Transposing assignment")
    return Assignment(workers={k: set(v) for k, v in assigned_chunks.items()})