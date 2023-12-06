from dataclasses import dataclass, field
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
    access_frequencies: Dict[ChunkId, int] = field(default_factory=lambda: defaultdict(int))

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

def distribute_chunks(
    chunks: List[ChunkId],
    weights: List[int],
    average: float,
    lower: int,
    upper: int,
) -> Dict[ChunkId, int]:
    assert len(chunks) == len(weights)

    if average <= lower:
        return {id: lower for id in chunks}
    if average >= upper:
        return {id: upper for id in chunks}
    if sum(weights) == 0:
        return {id: round(average) for id in chunks}

    base = len(chunks) * lower
    redundance = average * len(chunks) - base
    k = redundance / sum(weights)
    distribution = {id: lower + round(weights[i] * k) for i, id in enumerate(chunks)}
    maximum = max(distribution.values())

    logging.debug("Distributing chunks across workers. Maximum replication: %d", maximum)
    if maximum > upper:
        logging.warn("Couldn't distribute chunks fairly")
        return {id: min(replicas, upper) for id, replicas in distribution.items()}
    return distribution

def assign(
    workers: List[WorkerId],
    chunks: List[ChunkId],
    logs: Logs = Logs(),
    strategy: Literal["rendezvous", "circle"] = "rendezvous",
    replication_factor: float = 3, # how many workers should download a chunk on average
):
    chunks_replicas = distribute_chunks(
        chunks, weights=[logs.access_frequencies.get(id, 0) for id in chunks],
        lower=1, upper=len(workers), average=replication_factor,
    )

    assigned_chunks = defaultdict(list)

    def get_hash(s: Any):
        # return int.from_bytes(md5(str(s).encode("utf-8")).digest())
        return hash(str(s))

    if strategy == "rendezvous":
        # https://en.wikipedia.org/wiki/Rendezvous_hashing
        for chunk in chunks:
            replicas = chunks_replicas[chunk]
            hashes = sorted((get_hash(f"{chunk}:{worker}"), worker) for worker in workers)
            for _, chosen_worker in hashes[:replicas]:
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
                worker_index = 0
            else:
                worker_index = bisect_left(worker_pos, (h, 0))

            for _ in range(chunks_replicas[chunk]):
                worker = worker_pos[worker_index][1]
                assigned_chunks[worker].append(chunk)
                worker_index = (worker_index + 1) % len(worker_pos)

    return Assignment(workers=assigned_chunks)
