"""Dedup stage — idempotent ingestion across redeliveries and restarts."""

import asyncio
from datetime import UTC, datetime

from lorekeeper.app import build_handler
from lorekeeper.models import InboundMessage, MessageType
from lorekeeper.pipeline.dedup import MessageDeduplicator
from lorekeeper.pipeline.enricher import MessageEnricher


def _msg(msg_id: str, conversation_id: str = "g") -> InboundMessage:
    return InboundMessage(
        id=msg_id,
        conversation_id=conversation_id,
        sender_id="u",
        type=MessageType.TEXT,
        text="hi",
        timestamp=datetime(2026, 1, 1, 9, 0, tzinfo=UTC),
    )


async def test_second_delivery_is_a_duplicate():
    d = MessageDeduplicator()
    assert not await d.is_duplicate(_msg("1"))
    assert await d.is_duplicate(_msg("1"))  # LINE redelivery of the same event
    assert not await d.is_duplicate(_msg("2"))


async def test_same_id_in_another_conversation_is_distinct():
    # Telegram message_ids are only unique within a chat
    d = MessageDeduplicator()
    assert not await d.is_duplicate(_msg("7", "chat-a"))
    assert not await d.is_duplicate(_msg("7", "chat-b"))


async def test_blank_id_is_never_a_duplicate():
    d = MessageDeduplicator()
    assert not await d.is_duplicate(_msg(""))
    assert not await d.is_duplicate(_msg(""))


async def test_window_is_bounded_oldest_forgotten_first():
    d = MessageDeduplicator(max_ids=2)
    for i in "123":
        await d.is_duplicate(_msg(i))
    assert not await d.is_duplicate(_msg("1"))  # evicted
    assert await d.is_duplicate(_msg("3"))


async def test_concurrent_redeliveries_pass_exactly_once(tmp_path):
    d = MessageDeduplicator(state_path=str(tmp_path / "seen.txt"))
    results = await asyncio.gather(*(d.is_duplicate(_msg("1")) for _ in range(50)))
    assert results.count(False) == 1


async def test_seen_ids_survive_restart(tmp_path):
    path = str(tmp_path / "seen.txt")
    before = MessageDeduplicator(state_path=path)
    assert not await before.is_duplicate(_msg("1"))

    after = MessageDeduplicator(state_path=path)  # a fresh process
    assert await after.is_duplicate(_msg("1"))
    assert not await after.is_duplicate(_msg("2"))


async def test_state_file_stays_bounded(tmp_path):
    path = tmp_path / "seen.txt"
    d = MessageDeduplicator(max_ids=3, state_path=str(path))
    for i in range(10):
        await d.is_duplicate(_msg(str(i)))

    lines = path.read_text(encoding="utf-8").split()
    assert len(lines) <= 6  # never more than 2 × max_ids lines on disk
    reloaded = MessageDeduplicator(max_ids=3, state_path=str(path))
    assert await reloaded.is_duplicate(_msg("9"))
    assert not await reloaded.is_duplicate(_msg("0"))


async def test_handler_drops_redelivery_before_aggregation():
    added: list[str] = []

    class FakeAggregator:
        async def add(self, conversation_id: str, message: InboundMessage) -> None:
            added.append(message.id)

    handle = build_handler(MessageDeduplicator(), MessageEnricher(), FakeAggregator())
    await handle(_msg("1"))
    await handle(_msg("1"))
    await handle(_msg("2"))
    assert added == ["1", "2"]
