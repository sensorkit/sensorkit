import asyncio
import logging
import os
from datetime import datetime
from enum import StrEnum
from typing import override

import nats
import nats.errors
import nats.js.errors
import nats.micro
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.js.api import ConsumerConfig, DeliverPolicy, StreamConfig
from nats.js.kv import KV_DEL, KV_PURGE, KeyValue

from sensorkit.backend.base import (
    BackendError,
    BackendImpl,
    Entity,
    KeyNotFound,
    KVEntry,
    KVError,
    RequestCallback,
    RevisionError,
    ServiceInfo,
    SpecialProperty,
    StreamMessage,
    Subject,
    UnregisteredResponder,
)
from sensorkit.common.aio import cleanup_future

SUBJECT_PREFIX = "sensorkit"
DELETE_OPS = (KV_DEL, KV_PURGE)

logging.getLogger("nats").setLevel(
    logging.WARNING if os.environ.get("SENSORKIT_DEBUG") else logging.CRITICAL
)


class Method(StrEnum):
    """NATS subject namespace identifiers for the three backend communication channels."""

    REQ = "request"
    STR = "stream"
    KV = "kv"

    def with_prefix(self):
        """Return the fully-qualified NATS subject prefix for this method (e.g. 'sensorkit.request')."""
        return SUBJECT_PREFIX + "." + str(self)


def _subject_to_nats(method: Method, target: Subject):
    tokens = [method.with_prefix(), *target.path]

    match target.prop:
        case SpecialProperty.ALL_DESCENDANTS:
            tokens.append(">")
        case SpecialProperty.ALL_PROPERTIES:
            tokens.append("*")
        case SpecialProperty.EVENTS:
            tokens.append("_EVENTS")
        case SpecialProperty.NONE:
            pass
        case _:
            tokens.append(target.prop)

    return ".".join(tokens)


def _subject_from_nats(method: Method, subject: str):
    tokens = subject.split(".")

    if tokens[0] != SUBJECT_PREFIX or tokens[1] != str(method):
        raise ValueError()

    if tokens[-1] == "_EVENTS":
        tokens[-1] = SpecialProperty.EVENTS

    return Subject(tuple[str](tokens[2:-1]), tokens[-1])


def _kv_entry(entry: KeyValue.Entry, *, key: Subject = None):
    return KVEntry(
        key=key if key else _subject_from_nats(Method.KV, entry.key),
        value=entry.value if entry.operation not in DELETE_OPS else KVEntry.DELETED,
        revision=entry.revision,
    )


class NATSBackendImpl(BackendImpl):
    """BackendImpl backed by a live NATS JetStream cluster."""

    @override
    @classmethod
    async def create(cls, *args, **kwargs):
        # Connect to the NATS broker.
        nc = await nats.connect(*args, **kwargs)

        # Create JetStream resources.
        js = nc.jetstream()
        info = await js.add_stream(
            name="sensorkit",
            subjects=[f"{Method.STR.with_prefix()}.>"],
        )
        # TODO: When limit_marker_ttl support is added to nats-py, watchers should be able to
        #       detect key expiration.
        #kv = await js.create_key_value(bucket="sensorkit", limit_marker_ttl=1)
        kv = await js.create_key_value(bucket="sensorkit")

        return cls(
            client=nc,
            stream=info.config,
            key_value=kv,
        )

    def __init__(self, client: nats.NATS, stream: StreamConfig, key_value: KeyValue):
        self._nc = client
        self._kv = key_value
        self._stream = stream
        self._js = client.jetstream()
        self._subs: dict[Subject, Subscription] = {}

    @override
    async def register_service(self, info: ServiceInfo):
        try:
            await nats.micro.add_service(
                self._nc,
                name=info.name,
                version=info.version,
            )
        except nats.errors.Error as e:
            raise BackendError() from e

    @override
    async def request_invoke(self, target: Subject, payload: bytes):
        try:
            msg = await self._nc.request(
                _subject_to_nats(Method.REQ, target),
                payload,
            )
        except nats.errors.NoRespondersError as e:
            raise UnregisteredResponder(target) from e
        except nats.errors.Error as e:
            raise BackendError() from e

        return msg.data

    @override
    async def request_listen(self, target: Subject, coro: RequestCallback):
        async def _handle_request(msg: Msg):
            resp = await coro(msg.data)

            if resp is None:
                resp = b""

            await msg.respond(resp)

        async def _receive_request(msg: Msg):
            t = asyncio.create_task(_handle_request(msg))
            t.add_done_callback(cleanup_future)

        try:
            self._subs[target] = await self._nc.subscribe(
                _subject_to_nats(Method.REQ, target),
                cb=_receive_request,
            )
        except nats.errors.Error as e:
            raise BackendError() from e

    @override
    async def request_purge_listeners(self):
        req_prefix = Method.REQ.with_prefix()
        subjects = tuple(
            key for key, sub in self._subs.items() if sub.subject.startswith(req_prefix)
        )

        subscriptions = (self._subs.pop(subject) for subject in subjects)
        unsubscribes = (subscription.unsubscribe() for subscription in subscriptions)

        await asyncio.gather(*unsubscribes, return_exceptions=True)

    @override
    async def stream_list(self, entity: Entity):
        try:
            info = await self._js.stream_info(
                self._stream.name,
                subjects_filter=_subject_to_nats(
                    Method.STR,
                    entity.subject(SpecialProperty.ALL_PROPERTIES),
                ),
            )
            return [entity.subject(key) for key in info.state.subjects.keys()]
        except nats.errors.Error as e:
            raise BackendError() from e

    @override
    async def stream_publish(self, target: Subject, payload: bytes):
        try:
            await self._js.publish(
                subject=_subject_to_nats(Method.STR, target),
                payload=payload,
                stream=self._stream.name,
            )
        except nats.errors.Error as e:
            raise BackendError() from e

    @staticmethod
    async def _stream_iterator(sub: Subscription):
        try:
            async for msg in sub.messages:
                try:
                    yield StreamMessage(
                        subject=_subject_from_nats(Method.STR, msg.subject),
                        sequence=msg.metadata.sequence.stream,
                        timestamp=msg.metadata.timestamp,
                        data=msg.data,
                    )
                finally:
                    await msg.ack()
        except nats.errors.Error as e:
            raise BackendError() from e
        finally:
            await sub.unsubscribe()

    @override
    async def stream_consume(
        self,
        target: Subject,
        *,
        durable_name: str | None = None,
        start_at: int | datetime | None = None,
        include_latest: bool = False,
    ):
        config = ConsumerConfig()
        config.stream = self._stream.name
        config.durable_name = durable_name

        if start_at is not None:
            if include_latest:
                raise RuntimeError("Cannot specify both `start_at` and `include_latest`")

            match start_at:
                case datetime():
                    config.deliver_policy = DeliverPolicy.BY_START_TIME
                    config.opt_start_time = start_at.isoformat()  # type: ignore
                case int():
                    if start_at <= 0:
                        config.deliver_policy = DeliverPolicy.ALL
                    else:
                        config.deliver_policy = DeliverPolicy.BY_START_SEQUENCE
                        config.opt_start_seq = start_at
        elif include_latest:
            config.deliver_policy = DeliverPolicy.LAST_PER_SUBJECT
        else:
            config.deliver_policy = DeliverPolicy.NEW

        try:
            sub = await self._js.subscribe(
                _subject_to_nats(Method.STR, target),
                config=config,
            )
        except nats.errors.Error as e:
            raise BackendError() from e

        return self._stream_iterator(sub)

    @staticmethod
    async def _kv_iterator(watcher: KeyValue.KeyWatcher):
        try:
            # Collect initial entries until the watcher yields None (end-of-initial sentinel).
            initial = []
            async for entry in watcher:
                if entry is None:
                    break
                initial.append(_kv_entry(entry))
            yield initial

            # Stream live updates.
            async for entry in watcher:
                if entry is not None:
                    yield [_kv_entry(entry)]
        finally:
            await watcher.stop()

    @override
    async def kv_monitor(self, target: Subject):
        try:
            watcher = await self._kv.watch(
                keys=_subject_to_nats(Method.KV, target),
                include_history=True,
            )
        except nats.js.errors.KeyValueError as e:
            raise KVError() from e
        except nats.errors.Error as e:
            raise BackendError() from e

        return self._kv_iterator(watcher)

    @override
    async def kv_get(self, target: Subject, revision: int | None = None) -> KVEntry:
        try:
            entry = await self._kv.get(key=_subject_to_nats(Method.KV, target), revision=revision)
        except nats.js.errors.KeyNotFoundError as e:
            raise KeyNotFound(target, deleted=e.op in DELETE_OPS) from e
        except nats.js.errors.KeyValueError as e:
            raise KVError() from e
        except nats.errors.Error as e:
            raise BackendError() from e

        return _kv_entry(entry, key=target)

    @override
    async def kv_create(self, target: Subject, payload: bytes, ttl: float | None = None):
        # Presently NATS (or nats-py, as of v2.13) supports only whole second TTLs.
        if ttl:
            ttl = max(round(ttl), 1)

        try:
            rev = await self._kv.create(
                key=_subject_to_nats(Method.KV, target),
                value=payload,
                msg_ttl=ttl,
            )
        except nats.js.errors.KeyWrongLastSequenceError as e:
            raise RevisionError() from e
        except nats.js.errors.KeyValueError as e:
            raise KVError() from e
        except nats.errors.Error as e:
            raise BackendError() from e

        return KVEntry(target, payload, rev)

    @override
    async def kv_update(
        self,
        target: Subject,
        payload: bytes,
        revision: int | None = None,
        ttl: float | None = None,
    ):
        key = _subject_to_nats(Method.KV, target)

        # Presently NATS (or nats-py, as of v2.13) supports only whole second TTLs.
        if ttl:
            ttl = max(round(ttl), 1)

        try:
            if revision is None:
                if ttl is not None:
                    raise BackendError("Revision required for update with TTL")

                rev = await self._kv.put(key, payload)
            else:
                rev = await self._kv.update(key, payload, revision, msg_ttl=ttl)
        except nats.js.errors.KeyWrongLastSequenceError as e:
            raise RevisionError() from e
        except nats.js.errors.KeyValueError as e:
            raise KVError() from e
        except nats.errors.Error as e:
            raise BackendError() from e

        return KVEntry(target, payload, rev)

    @override
    async def kv_delete(self, target: Subject, revision: int | None = None):
        try:
            await self._kv.delete(
                key=_subject_to_nats(Method.KV, target),
                last=revision,
            )
        except nats.js.errors.KeyWrongLastSequenceError as e:
            raise RevisionError() from e
        except nats.errors.Error as e:
            raise BackendError() from e
