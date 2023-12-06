from typing import List, FrozenSet, Dict, Literal
from collections import defaultdict
import numpy as np
import random
import logging
from dataclasses import dataclass
import matplotlib.pyplot as plt

from scheduler import assign, WorkerId, ChunkId, Logs

DEBUG = True


class Metrics:
    def __init__(self) -> None:
        self.fetched_by_worker: Dict[WorkerId, int] = defaultdict(int)
        self.queries_by_worker: Dict[WorkerId, int] = defaultdict(int)
        self.queries_by_chunks: Dict[WorkerId, int] = defaultdict(int)

    def report_worker_fetched(self, worker: WorkerId, new_chunks: int, dropped_chunks: int) -> None:
        self.fetched_by_worker[worker] += new_chunks

    def report_query_processed(self, worker: WorkerId, chunk: ChunkId) -> None:
        self.queries_by_worker[worker] += 1
        self.queries_by_chunks[chunk] += 1

    def dump(self, mode: Literal["fetches", "queries"] = "fetches") -> None:
        if mode == "fetches":
            mean = np.mean(list(self.fetched_by_worker.values()))
            plt.bar(self.fetched_by_worker.keys(), self.fetched_by_worker.values())
            plt.hlines(mean, 0, max(self.fetched_by_worker.keys()), colors="red", linestyles="dashed")
            plt.title("Fetches from persistent storage")
            plt.show(block=True)
        elif mode == "queries":
            mean = np.mean(list(self.queries_by_worker.values()))
            _, (plt1, plt2) = plt.subplots(1, 2)
            plt1.bar(self.queries_by_worker.keys(), self.queries_by_worker.values())
            plt1.hlines(mean, 0, max(self.queries_by_worker.keys()), colors="red", linestyles="dashed")
            plt1.set_xlabel("Worker")
            plt1.set_ylabel("Chunks queried")
            plt2.bar(self.queries_by_chunks.keys(), self.queries_by_chunks.values())
            plt2.set_xlabel("Chunk")
            plt2.set_ylabel("Queries")
            plt.title("Egress with uneven chunk requests")
            plt.show(block=True)

    def reset(self) -> None:
        self.fetched_by_worker.clear()
        self.queries_by_worker.clear()
        self.queries_by_chunks.clear()


class Worker:
    def __init__(self, id: WorkerId, metrics: Metrics) -> None:
        self.id: WorkerId = id
        self.metrics: Metrics = metrics
        self.chunks: FrozenSet[ChunkId] = set()
    
    def assign(self, assignment: List[ChunkId]) -> None:
        assignment = set(assignment)
        to_fetch = assignment.difference(self.chunks)
        to_drop = self.chunks.difference(assignment)
        self.metrics.report_worker_fetched(self.id, len(to_fetch), len(to_drop))
        self.chunks = assignment

    def query(self, chunk: ChunkId) -> None:
        if DEBUG:
            assert chunk in self.chunks
        self.metrics.report_query_processed(self.id, chunk)


class Client:
    def __init__(self, chunk_popularity_factor: float = 1) -> None:
        self.chunk_popularity_factor = chunk_popularity_factor
        self.weights = [1.0]
        pass

    def generate_query(self, chunks: List[ChunkId], num: int = 50) -> List[ChunkId]:
        while len(self.weights) < len(chunks):
            self.weights.append(self.weights[-1] * self.chunk_popularity_factor)

        return random.choices(chunks, k=num, weights=self.weights[:len(chunks)])


@dataclass
class SimulationParams:
    workers_num: int = 100
    chunks_num: int = 1000000
    clients_num: int = 1000
    epochs_num: int = 100
    new_chunks_per_epoch: int = 0
    chunk_popularity_factor: float = 1.001

def simulate(params: SimulationParams) -> None:
    metrics = Metrics()

    worker_ids = [WorkerId(i) for i in range(params.workers_num)]
    workers = {id: Worker(id, metrics) for id in worker_ids}
    chunks = [ChunkId(i) for i in range(params.chunks_num)]
    client = Client(chunk_popularity_factor=params.chunk_popularity_factor)
    # clients = [client for _ in range(params.clients_num)]

    for epoch in range(params.epochs_num):
        print("Epoch #", epoch)

        logging.debug("Generating assignment")
        assignment = assign(worker_ids, chunks, strategy="rendezvous")

        logging.debug("Assigning chunks to workers")
        for id, worker in workers.items():
            worker.assign(assignment.workers.get(id, []))
        
        logging.debug("Simulating client requests")
        for _ in range(params.clients_num):
            for chunk in client.generate_query(chunks):
                worker: WorkerId = random.choice(assignment.chunks[chunk])
                workers[worker].query(chunk)

        logging.debug("Simulating environment changes")
        chunks.extend(ChunkId(i) for i in range(len(chunks), len(chunks) + params.new_chunks_per_epoch))

    logging.debug("Reporting summary")
    metrics.dump("queries")
    metrics.reset()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(message)s",
    )
    simulate(SimulationParams(chunks_num=100000, new_chunks_per_epoch=0, epochs_num=10))