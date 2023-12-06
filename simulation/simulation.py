from typing import List, FrozenSet, Dict, Literal
from collections import defaultdict
import numpy as np
import random
import logging
from dataclasses import dataclass
import matplotlib.pyplot as plt

from scheduler import assign, WorkerId, ChunkId, Logs, Assignment

DEBUG = True


class Metrics:
    def __init__(self) -> None:
        self.fetched_by_worker: Dict[WorkerId, int] = defaultdict(int)
        self.dropped_by_worker: Dict[WorkerId, int] = defaultdict(int)
        self.queries_by_worker: Dict[WorkerId, int] = defaultdict(int)
        self.queries_by_chunks: Dict[WorkerId, int] = defaultdict(int)

    def report_assignment(self, worker: WorkerId, chunks: List[ChunkId]) -> None:
        pass

    def report_worker_fetched(self, worker: WorkerId, new_chunks: int, dropped_chunks: int) -> None:
        self.fetched_by_worker[worker] += new_chunks
        self.dropped_by_worker[worker] += dropped_chunks

    def report_query_processed(self, worker: WorkerId, chunk: ChunkId) -> None:
        self.queries_by_worker[worker] += 1
        self.queries_by_chunks[chunk] += 1

    def reset(self) -> None:
        self.fetched_by_worker.clear()
        self.dropped_by_worker.clear()
        self.queries_by_worker.clear()
        self.queries_by_chunks.clear()


class Worker:
    def __init__(self, id: WorkerId, metrics: Metrics) -> None:
        self.id: WorkerId = id
        self.metrics: Metrics = metrics
        self.chunks: FrozenSet[ChunkId] = set()
    
    def assign(self, assignment: List[ChunkId]) -> None:
        self.metrics.report_assignment(self.id, assignment)
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
    chunk_popularity_factor: float = 1.0

def simulate(params: SimulationParams) -> None:
    metrics = Metrics()
    logs = Logs()

    worker_ids = [WorkerId(i) for i in range(params.workers_num)]
    workers = {id: Worker(id, metrics) for id in worker_ids}
    chunks = [ChunkId(i) for i in range(params.chunks_num)]
    client = Client(chunk_popularity_factor=params.chunk_popularity_factor)

    for epoch in range(params.epochs_num):
        print("Epoch #", epoch)

        logging.debug("Generating assignment")
        assignment = assign(worker_ids, chunks, strategy="rendezvous", logs=logs)

        logging.debug("Assigning chunks to workers")
        for id, worker in workers.items():
            worker.assign(assignment.workers.get(id, []))
        
        logging.debug("Simulating client requests")
        for _ in range(params.clients_num):
            for chunk in client.generate_query(chunks):
                worker: WorkerId = random.choice(assignment.chunks[chunk])
                workers[worker].query(chunk)
                logs.access_frequencies[chunk] += 1

        logging.debug("Simulating environment changes")
        chunks.extend(ChunkId(i) for i in range(len(chunks), len(chunks) + params.new_chunks_per_epoch))

        if epoch <= 1:
            metrics.reset()

    logging.debug("Reporting summary")
    show_data(metrics, assignment, epoch)


def show_data(metrics: Metrics, assignment: Assignment, epoch: int):
    fig, ((plt1, plt2), (plt3, plt4)) = plt.subplots(2, 2)

    fetches_mean = np.mean(list(metrics.fetched_by_worker.values()))
    plt1.bar(metrics.fetched_by_worker.keys(), metrics.fetched_by_worker.values())
    plt1.bar(metrics.dropped_by_worker.keys(), [-v for v in metrics.dropped_by_worker.values()], color="darkred")
    plt1.hlines(fetches_mean, 0, max(metrics.fetched_by_worker.keys()), colors="orange", linestyles="dashed")
    plt1.set_title("Fetches from persistent storage")

    plt2.bar(assignment.chunks.keys(), [len(workers) for workers in assignment.chunks.values()])
    plt2.set_title("Chunk replicas distribution")

    queries_mean = np.mean(list(metrics.queries_by_worker.values()))
    plt3.bar(metrics.queries_by_worker.keys(), metrics.queries_by_worker.values())
    plt3.hlines(queries_mean, 0, max(metrics.queries_by_worker.keys()), colors="orange", linestyles="dashed")
    plt3.set_xlabel("Worker")
    plt3.set_ylabel("Chunks queried")
    plt3.set_title("Workers egress")

    plt4.bar(metrics.queries_by_chunks.keys(), metrics.queries_by_chunks.values())
    plt4.set_xlabel("Chunk")
    plt4.set_ylabel("Queries")
    plt4.set_title("Chunk popularity")

    fig.suptitle(f"Epoch {epoch}")
    plt.show(block=True)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(message)s",
    )
    simulate(SimulationParams(
        chunks_num=100000,
        clients_num=1000,
        new_chunks_per_epoch=100,
        epochs_num=100,
        chunk_popularity_factor=1.001,
    ))