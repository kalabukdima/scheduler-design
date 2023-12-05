from typing import List, FrozenSet, Dict
from collections import defaultdict
import numpy as np
from numpy import random
import logging
from dataclasses import dataclass
import matplotlib.pyplot as plt

from scheduler import assign_plain, WorkerId, ChunkId, Logs

DEBUG = True


class Metrics:
    def __init__(self) -> None:
        self.fetched_by_worker: Dict[WorkerId, int] = defaultdict(int)

    def report_worker_fetched(self, worker: WorkerId, new_chunks: int, dropped_chunks: int) -> None:
        self.fetched_by_worker[worker] += new_chunks

    def report_query_processed(self, worker: WorkerId, chunk: ChunkId) -> None:
        pass

    def dump(self) -> None:
        mean = np.mean(list(self.fetched_by_worker.values()))
        plt.bar(self.fetched_by_worker.keys(), self.fetched_by_worker.values())
        plt.hlines(mean, 0, len(self.fetched_by_worker), colors="red", linestyles="dashed")
        plt.show(block=True)

    def reset(self) -> None:
        self.fetched_by_worker = defaultdict(int)


class Worker:
    def __init__(self, id: WorkerId, metrics: Metrics) -> None:
        self.id: WorkerId = id
        self.metrics: Metrics = metrics
        self.chunks: FrozenSet[ChunkId] = set()
    
    def assign(self, assignment: FrozenSet[ChunkId]) -> None:
        to_fetch = assignment.difference(self.chunks)
        to_drop = self.chunks.difference(assignment)
        self.metrics.report_worker_fetched(self.id, len(to_fetch), len(to_drop))
        self.chunks = assignment

    def query(self, chunk: ChunkId) -> None:
        if DEBUG:
            assert chunk in self.chunks
        self.metrics.report_query_processed(self.id, chunk)


class Client:
    def __init__(self) -> None:
        pass

    def generate_query(self, chunks: List[ChunkId]) -> List[ChunkId]:
        return list(random.choice(chunks, 50))


@dataclass
class SimulationParams:
    workers_num: int = 100
    chunks_num: int = 1000000
    clients_num: int = 1000
    new_chunks_per_epoch: int = 0

def simulate(params: SimulationParams) -> None:
    metrics = Metrics()

    worker_ids = [WorkerId(i) for i in range(params.workers_num)]
    workers = {id: Worker(id, metrics) for id in worker_ids}
    chunks = [ChunkId(i) for i in range(params.chunks_num)]
    clients = [Client() for _ in range(params.clients_num)]

    for epoch in range(10):
        print("Epoch #", epoch)

        logging.debug("Generating assignment")
        assignment = assign_plain(worker_ids, chunks)

        logging.debug("Assigning chunks to workers")
        for id, worker in workers.items():
            worker.assign(assignment.workers.get(id, set()))
        
        logging.debug("Simulating client requests")
        for client in clients:
            for chunk in client.generate_query(chunks):
                worker: WorkerId = random.choice(list(assignment.chunks[chunk]))
                workers[worker].query(chunk)

        logging.debug("Reporting summary")
        metrics.dump()
        metrics.reset()

        logging.debug("Simulating environment changes")
        chunks.extend(ChunkId(i) for i in range(len(chunks), len(chunks) + params.new_chunks_per_epoch))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(message)s",
    )
    simulate(SimulationParams(chunks_num=100000, new_chunks_per_epoch=1000))