"""Message transport: Kafka in production, an in-memory twin for fast tests.

Both implementations share the semantics the worker depends on:
- messages with the same key land on the same partition, in order;
- a consumer only advances when it commits;
- anything uncommitted is delivered again after a restart.
The Kafka suite (tests/test_kafka.py) runs the same scenarios on a real broker.
"""

from __future__ import annotations

import asyncio
import json
import zlib
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Protocol

FRONTIER = "crawl.frontier"
DLQ = "crawl.dlq"


@dataclass(frozen=True)
class Message:
    topic: str
    partition: int
    offset: int
    key: str
    value: dict[str, Any]


class Producer(Protocol):
    async def send(self, topic: str, key: str, value: dict[str, Any]) -> None: ...
    async def close(self) -> None: ...


class Consumer(Protocol):
    async def poll(self, max_records: int = 50, timeout: float = 0.5) -> list[Message]: ...
    async def commit(self, messages: list[Message]) -> None: ...
    async def close(self) -> None: ...


def partition_for(key: str, partitions: int) -> int:
    return zlib.crc32(key.encode()) % partitions


# -- in-memory -------------------------------------------------------------


@dataclass
class InMemoryBroker:
    partitions: int = 4
    log: dict[tuple[str, int], list[Message]] = field(default_factory=lambda: defaultdict(list))
    committed: dict[tuple[str, str, int], int] = field(default_factory=dict)  # (group, topic, p) -> next
    produced: int = 0

    def producer(self) -> InMemoryProducer:
        return InMemoryProducer(self)

    def consumer(self, group: str, topics: list[str]) -> InMemoryConsumer:
        return InMemoryConsumer(self, group, topics)


class InMemoryProducer:
    def __init__(self, broker: InMemoryBroker) -> None:
        self.broker = broker

    async def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        p = partition_for(key, self.broker.partitions)
        log = self.broker.log[(topic, p)]
        # Round-trip through JSON so tests catch anything Kafka could not serialise.
        log.append(Message(topic, p, len(log), key, json.loads(json.dumps(value))))
        self.broker.produced += 1

    async def close(self) -> None:
        return None


class InMemoryConsumer:
    def __init__(self, broker: InMemoryBroker, group: str, topics: list[str]) -> None:
        self.broker, self.group, self.topics = broker, group, topics
        # A fresh consumer starts from the group's committed offsets, like Kafka.
        self.position = {
            (t, p): broker.committed.get((group, t, p), 0) for t in topics for p in range(broker.partitions)
        }

    async def poll(self, max_records: int = 50, timeout: float = 0.5) -> list[Message]:
        out: list[Message] = []
        for tp, pos in self.position.items():
            batch = self.broker.log[tp][pos : pos + max_records - len(out)]
            out.extend(batch)
            self.position[tp] = pos + len(batch)
            if len(out) >= max_records:
                break
        if not out:
            await asyncio.sleep(min(timeout, 0.05))
        return out

    async def commit(self, messages: list[Message]) -> None:
        for m in messages:
            key = (self.group, m.topic, m.partition)
            self.broker.committed[key] = max(self.broker.committed.get(key, 0), m.offset + 1)

    async def close(self) -> None:
        return None


# -- Kafka -----------------------------------------------------------------


class KafkaProducer:
    def __init__(self, bootstrap: str) -> None:
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap,
            acks="all",
            enable_idempotence=True,
            key_serializer=str.encode,
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        self._started = False

    async def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        if not self._started:
            await self._producer.start()
            self._started = True
        await self._producer.send_and_wait(topic, value=value, key=key)

    async def close(self) -> None:
        if self._started:
            await self._producer.stop()


class KafkaConsumer:
    def __init__(self, bootstrap: str, group: str, topics: list[str]) -> None:
        from aiokafka import AIOKafkaConsumer

        self._consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap,
            group_id=group,
            enable_auto_commit=False,  # commit only after the page is safely stored
            auto_offset_reset="earliest",
            key_deserializer=lambda b: b.decode() if b else "",
            value_deserializer=lambda b: json.loads(b),
        )
        self._started = False

    async def _ensure(self) -> None:
        if not self._started:
            await self._consumer.start()
            self._started = True

    async def poll(self, max_records: int = 50, timeout: float = 0.5) -> list[Message]:
        await self._ensure()
        batches = await self._consumer.getmany(timeout_ms=int(timeout * 1000), max_records=max_records)
        return [
            Message(r.topic, r.partition, r.offset, r.key, r.value)
            for records in batches.values()
            for r in records
        ]

    async def commit(self, messages: list[Message]) -> None:
        from aiokafka import TopicPartition

        offsets: dict[Any, int] = {}
        for m in messages:
            tp = TopicPartition(m.topic, m.partition)
            offsets[tp] = max(offsets.get(tp, 0), m.offset + 1)
        if offsets:
            await self._consumer.commit(offsets)

    async def close(self) -> None:
        if self._started:
            await self._consumer.stop()


async def ensure_topics(bootstrap: str, partitions: int = 4) -> None:
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.create_topics([NewTopic(t, partitions, 1) for t in (FRONTIER, DLQ)])
    except TopicAlreadyExistsError:
        pass
    finally:
        await admin.close()
