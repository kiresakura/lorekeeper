"""Pipeline stage 0 — drop redelivered messages so ingestion is idempotent.

Webhook sources retry: LINE redelivers an event when the endpoint doesn't answer
200 in time, Telegram re-sends an Update until it's acknowledged. Every attempt
carries the same message id, so remembering the most recent
`(conversation_id, message_id)` keys is enough to process each message once.

Keys live in a bounded in-memory window and, optionally, in a plain text file
(one key per line) so a restart — the usual reason a 200 goes missing — doesn't
let the redeliveries through. File I/O runs in a worker thread; nothing blocks
the event loop.
"""

import asyncio
import logging
from collections import OrderedDict
from pathlib import Path

from lorekeeper.models import InboundMessage

logger = logging.getLogger(__name__)


class MessageDeduplicator:
    """Check-and-record store of recently seen message keys."""

    def __init__(self, max_ids: int = 10_000, state_path: str = ""):
        self.max_ids = max_ids
        self._path = Path(state_path) if state_path else None
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._io_lock = asyncio.Lock()
        self._unflushed = 0  # 自上次重寫狀態檔以來追加的行數
        if self._path is not None:
            self._load()

    @staticmethod
    def key_for(msg: InboundMessage) -> str:
        # Telegram 的 message_id 只在單一 chat 內唯一，所以一律加上 conversation_id
        return f"{msg.conversation_id}:{msg.id}"

    async def is_duplicate(self, msg: InboundMessage) -> bool:
        """Return True if this message was seen before; otherwise record it.

        The check and the insert happen without an `await` in between, so
        concurrent deliveries of the same message can't both pass.
        """
        if not msg.id:
            return False  # 沒有 id 就無從判斷，寧可放行
        key = self.key_for(msg)
        if key in self._seen:
            self._seen.move_to_end(key)
            return True
        self._seen[key] = None
        while len(self._seen) > self.max_ids:
            self._seen.popitem(last=False)
        if self._path is not None:
            await self._persist(key)
        return False

    # --- persistence ---------------------------------------------------------

    async def _persist(self, key: str) -> None:
        async with self._io_lock:
            self._unflushed += 1
            if self._unflushed > self.max_ids:
                # 檔案長到 2 × max_ids 行就整個重寫，保持有界
                self._unflushed = 0
                await asyncio.to_thread(self._rewrite, list(self._seen))
            else:
                await asyncio.to_thread(self._append, key)

    def _load(self) -> None:
        if not self._path.exists():
            return
        keys = [k for k in self._path.read_text(encoding="utf-8").split("\n") if k]
        for key in keys:
            self._seen[key] = None
            self._seen.move_to_end(key)
        while len(self._seen) > self.max_ids:
            self._seen.popitem(last=False)
        if len(keys) > len(self._seen):
            self._rewrite(list(self._seen))
        logger.info(f"載入 {len(self._seen)} 個已處理訊息 id: {self._path}")

    def _append(self, key: str) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a", encoding="utf-8") as f:
            f.write(key + "\n")

    def _rewrite(self, keys: list[str]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_name(self._path.name + ".tmp")
        tmp.write_text("".join(k + "\n" for k in keys), encoding="utf-8")
        tmp.replace(self._path)  # 原子替換，不會留下寫到一半的檔
