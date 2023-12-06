from dataclasses import dataclass
from typing import Dict, List, Literal, Any
import logging
from collections import defaultdict
from hashlib import md5
from bisect import bisect_left

from utils import argmin


WorkerId = int
ChunkId = int

@dataclass
class Logs:
    access_frequencies: Dict[ChunkId, int]

@dataclass
class Assignment:
    workers: Dict[WorkerId, List[ChunkId]]
    chunks: Dict[ChunkId, List[WorkerId]]

    def __init__(self, workers: Dict[WorkerId, List[ChunkId]]) -> None:
        self.workers = workers
        self.chunks = defaultdict(list)
        for worker, chunks in workers.items():
            for chunk in chunks:
                self.chunks[chunk].append(worker)

# Doesn't use logs for now. Simply distributes chunks evenly between workers
def assign(
    workers: List[WorkerId],
    chunks: List[ChunkId],
    logs: Logs = Logs(access_frequencies={}),
    strategy: Literal["rendezvous", "circle"] = "rendezvous",
):
    assigned_chunks = defaultdict(list)

    def get_hash(s: Any):
        # return int.from_bytes(md5(str(s).encode("utf-8")).digest())
        return hash(str(s))

    if strategy == "rendezvous":
        # https://en.wikipedia.org/wiki/Rendezvous_hashing
        for chunk in chunks:
            hashes = [get_hash(f"{chunk}:{worker}") for worker in workers]
            chosen_worker = workers[argmin(hashes)]
            assigned_chunks[chosen_worker].append(chunk)
    
    elif strategy == "circle":
        # https://en.wikipedia.org/wiki/Consistent_hashing
        worker_pos = sorted((get_hash(id), id) for id in workers)
        logging.debug("worker_pos: %s", worker_pos)
        hashes = []
        for chunk in chunks:
            h = get_hash(chunk)
            hashes.append(h)
            if h > worker_pos[-1][0]:
                chosen_worker = worker_pos[0][1]
            else:
                chosen_worker = worker_pos[bisect_left(worker_pos, (h, 0))][1]
            assigned_chunks[chosen_worker].append(chunk)

    return Assignment(workers=assigned_chunks)
