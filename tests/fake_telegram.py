"""A small simulated Telegram: World + Actors + FakeTelegramClient.

The world owns chats, messages, users, sticker sets and fault injection.
A client speaks enough Telethon against the world for the real tools to run
unmodified — get_client / ensure_connected / resolve_entity / ops journal /
pending events all execute for real; only the wire is fake.

    world = FakeTelegram()
    me = world.add_account("owner")          # the account the client runs as
    alice = world.add_user("alice")          # a simulated remote user
    dm = world.private_chat(me, alice)
    group = world.group_chat("team", me, alice)

    client = world.client_for(me, flood_state=FloodWaitState(path))
    runtime.clients["default"] = client      # mutate, don't rebind

    await messages.send_message(chat_id=dm.id, message="hi")
    await alice.say(dm, "hello @owner")      # fires real incoming handlers

Faults live on the world: world.flood(seconds), world.partition(),
world.drop_response(), client.fail_next(exc). A second client_for() with the
same account and state files is a restarted process.

TL requests the tools send are dispatched through _RPC; anything unhandled is
recorded in world.unhandled (so coverage gaps are visible) and gets a benign
empty result rather than crashing the scenario.
"""

from __future__ import annotations

import datetime
import time
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

from telethon.errors import FloodWaitError
from telethon.tl.types import UpdateMessageID

from telegram_mcp.floodgate import FloodWaitState


class FakeMessage:
    """Enough of telethon.tl.types.Message for the tools to format."""

    def __init__(
        self,
        id: int,
        chat_id: int,
        sender,
        text: str,
        *,
        date=None,
        reply_to=None,
        mentioned: bool = False,
        media=None,
        out: bool = False,
        scheduled: bool = False,
        schedule_date=None,
    ):
        self.id = id
        self.peer_id = chat_id
        self.sender = sender
        self.sender_id = getattr(sender, "id", None)
        self.from_id = sender
        self.message = text
        self.date = date or datetime.datetime.now(datetime.timezone.utc)
        self.reply_to = reply_to
        self.mentioned = mentioned
        self.media = media
        self.out = out
        self.scheduled = scheduled
        self.schedule_date = schedule_date
        self.is_reply = reply_to is not None and not scheduled
        self.entities = None
        self.grouped_id = None

    @property
    def is_scheduled(self):
        return self.scheduled


class Chat:
    def __init__(self, id: int, title: str, kind: str):
        self.id = id
        self.title = title
        self.username = None
        self.kind = kind  # "dm" | "group" | "forum"
        self.participants: List[Actor] = []
        self.messages: List[FakeMessage] = []
        self.pinned_ids: List[int] = []
        self.draft: Optional[FakeMessage] = None
        self.topics: Dict[int, str] = {}

    # Entity-shape attributes used by resolve_entity/is_chat_allowed.
    @property
    def first_name(self):
        return self.title if self.kind == "dm" else None

    @property
    def last_name(self):
        return None


class Actor:
    """A simulated Telegram user — including the account's own identity."""

    def __init__(self, world: "FakeTelegram", id: int, username: str):
        self.world = world
        self.id = id
        self.username = username
        self.usernames = []
        self.first_name = username.title()
        self.last_name = None
        self.bot = False
        self.is_self = False
        self.premium = False
        self.phone = None
        self.title = None

    async def say(
        self,
        chat: Chat,
        text: str,
        *,
        reply_to: Optional[FakeMessage] = None,
        topic_id: Optional[int] = None,
        scheduled: bool = False,
    ) -> FakeMessage:
        """Post a message into a chat; delivered to every member's client."""
        msg = self.world.post(chat, self, text, reply_to=reply_to, topic_id=topic_id)
        await self.world.broadcast(msg)
        return msg

    def __repr__(self):
        return f"<Actor @{self.username}#{self.id}>"


class FakeTelegram:
    """The simulated Telegram backend shared by clients and actors."""

    def __init__(self):
        self.users: Dict[int, Actor] = {}
        self.usernames: Dict[str, Actor] = {}
        self.chats: Dict[int, Chat] = {}
        self.sticker_sets: Dict[str, SimpleNamespace] = {}
        self.clients: List["FakeTelegramClient"] = []
        self.unhandled: List[Any] = []  # requests we don't simulate yet
        self._next_id = 1
        self._next_msg_id = 1000
        self.offline = False

    # --- topology ---------------------------------------------------------------

    def _id(self) -> int:
        self._next_id += 1
        return self._next_id

    def _msg_id(self) -> int:
        self._next_msg_id += 1
        return self._next_msg_id - 1

    def add_user(self, username: str) -> Actor:
        u = Actor(self, self._id(), username)
        self.users[u.id] = u
        self.usernames[username] = u
        return u

    def add_account(self, username: str) -> Actor:
        return self.add_user(username)

    def private_chat(self, a: Actor, b: Actor) -> Chat:
        chat = Chat(self._id(), b.username, "dm")
        chat.participants = [a, b]
        self.chats[chat.id] = chat
        return chat

    def group_chat(self, title: str, *members: Actor, forum: bool = False) -> Chat:
        chat = Chat(-self._id(), title, "forum" if forum else "group")
        chat.participants = list(members)
        if forum:
            chat.topics = {1: "General"}
        self.chats[chat.id] = chat
        return chat

    def add_sticker_set(self, short_name: str, *emojis: str) -> None:
        docs = [
            SimpleNamespace(
                id=100 + i,
                access_hash=200 + i,
                file_reference=bytes([i + 1]) * 4,
                attributes=[SimpleNamespace(alt=e)],
            )
            for i, e in enumerate(emojis)
        ]
        self.sticker_sets[short_name] = SimpleNamespace(
            set=SimpleNamespace(title=f"{short_name} Set"), documents=docs
        )

    # --- message flow -------------------------------------------------------------

    def post(
        self,
        chat: Chat,
        sender: Actor,
        text: str,
        *,
        reply_to: Optional[FakeMessage] = None,
        topic_id: Optional[int] = None,
        scheduled: bool = False,
        schedule_date=None,
        media=None,
    ) -> FakeMessage:
        reply_header = None
        if reply_to is not None:
            reply_header = SimpleNamespace(
                reply_to_msg_id=reply_to.id,
                reply_to_top_id=topic_id,
                forum_topic=topic_id is not None,
            )
        elif topic_id is not None:
            reply_header = SimpleNamespace(
                reply_to_msg_id=topic_id,
                reply_to_top_id=topic_id,
                forum_topic=True,
            )
        msg = FakeMessage(
            self._msg_id(),
            chat.id,
            sender,
            text,
            reply_to=reply_header,
            mentioned=False,  # computed per recipient in broadcast
            media=media,
            out=False,
            scheduled=scheduled,
            schedule_date=schedule_date,
        )
        chat.messages.append(msg)
        return msg

    async def broadcast(self, msg: FakeMessage) -> None:
        """Deliver a posted message as a NewMessage-style event to clients."""
        chat = self.chats[msg.peer_id]
        for client in list(self.clients):
            if client.me.id not in {p.id for p in chat.participants}:
                continue
            event = client._make_event(chat, msg)
            await client._fire(event)

    def client_for(
        self,
        account: Actor,
        session_string: str = "fake-session-1",
        flood_state: Optional[FloodWaitState] = None,
    ) -> "FakeTelegramClient":
        """A client for this account. Same session_string + state files as a
        previous client = that client restarted."""
        return FakeTelegramClient(
            self, account, session_string=session_string, flood_state=flood_state
        )

    # --- fault injection ------------------------------------------------------------

    def partition(self) -> None:
        """Every request raises ConnectionError until go_online()."""
        self.offline = True

    def go_online(self) -> None:
        self.offline = False


class FakeTelegramClient:
    """Speaks Telethon against a FakeTelegram world."""

    def __init__(
        self,
        world: FakeTelegram,
        me: Actor,
        session_string: str = "fake-session-1",
        flood_state: Optional[FloodWaitState] = None,
    ):
        self.world = world
        self.me = me
        self.session = SimpleNamespace(filename=None, save=lambda: session_string)
        self._connected = False
        self._authorized = True
        self._handlers: list = []
        self.sent: List[Any] = []  # outbound requests/method calls that hit the wire
        self.fail_with: Optional[BaseException] = None
        self._drop_next = False  # apply the effect, lose the response
        self.flood_state = flood_state
        self._flood_until = 0.0
        # Like Telethon, handlers attached pre-connect deliver once connected —
        # the sim registers for delivery immediately.
        world.clients.append(self)

    # --- lifecycle ------------------------------------------------------------------

    async def connect(self):
        self._connected = True
        if self not in self.world.clients:
            self.world.clients.append(self)

    async def disconnect(self):
        self._connected = False
        if self in self.world.clients:
            self.world.clients.remove(self)

    def is_connected(self):
        return self._connected

    async def is_user_authorized(self):
        return self._authorized

    async def get_me(self):
        return self.me

    def set_authorized(self, value: bool):
        self._authorized = value

    async def get_dialogs(self, *a, **kw):
        return [c for c in self.world.chats.values() if self.me in c.participants]

    async def get_entity(self, identifier):
        if isinstance(identifier, str):
            name = identifier.lstrip("@")
            if name in self.world.usernames:
                return self.world.usernames[name]
            raise ValueError(f"Cannot find entity {identifier!r}")
        ident = int(identifier)
        if ident in self.world.chats:
            return self.world.chats[ident]
        if ident in self.world.users:
            return self.world.users[ident]
        raise ValueError(f"Cannot find entity {identifier!r}")

    # --- fault injection --------------------------------------------------------------

    def flood(self, seconds: float):
        if self.flood_state is not None:
            self.flood_state.record(seconds)
        else:
            self._flood_until = max(self._flood_until, time.time() + seconds)

    def fail_next(self, exc: BaseException):
        self.fail_with = exc

    def drop_response(self):
        """Next send applies its effect but the response never arrives —
        the ambiguous-failure case the journal must record as 'unknown'."""
        self._drop_next = True

    def _check_faults(self, request=None):
        wait = (
            self.flood_state.retry_after()
            if self.flood_state is not None
            else max(0.0, self._flood_until - time.time())
        )
        if wait > 0:
            raise FloodWaitError(request=request, capture=max(1, int(wait)))
        if self.world.offline:
            raise ConnectionError("telegram: offline")
        if self.fail_with is not None:
            exc, self.fail_with = self.fail_with, None
            raise exc

    # --- incoming events -----------------------------------------------------------------

    def add_event_handler(self, fn, event_type=None):
        self._handlers.append(fn)

    def _make_event(self, chat: Chat, msg: FakeMessage):
        client = self
        own = msg.sender_id == self.me.id
        mentioned = f"@{self.me.username}" in (msg.message or "")
        sender = msg.sender
        if own:
            # The handler drops own messages via sender.is_self — mirror that.
            sender = SimpleNamespace(**{**vars(msg.sender), "is_self": True})

        async def get_sender():
            return sender

        async def get_reply_message():
            if msg.reply_to and msg.reply_to.reply_to_msg_id:
                reply_id = msg.reply_to.reply_to_msg_id
                for m in chat.messages:
                    if m.id == reply_id:
                        return m
            return None

        return SimpleNamespace(
            is_private=chat.kind == "dm",
            chat_id=chat.id,
            client=client,
            message=SimpleNamespace(
                id=msg.id,
                mentioned=mentioned,
                is_reply=msg.is_reply,
                reply_to=msg.reply_to,
            ),
            get_sender=get_sender,
            get_reply_message=get_reply_message,
        )

    async def _fire(self, event):
        for handler in self._handlers:
            await handler(event)

    # --- high-level client API ---------------------------------------------------------

    async def _parse_message_text(self, message, parse_mode):
        return message, []

    def _resolve_chat(self, entity) -> Chat:
        """Entities can be chats or users — a user resolves to the private chat."""
        cid = _chat_id(entity)
        if cid in self.world.chats:
            return self.world.chats[cid]
        for c in self.world.chats.values():
            if c.kind == "dm" and {p.id for p in c.participants} == {self.me.id, cid}:
                return c
        raise ValueError(f"no chat for entity {entity!r}")

    async def send_message(self, entity, message, **kwargs):
        self._check_faults(message)
        chat = self._resolve_chat(entity)
        schedule = kwargs.get("schedule")
        msg = self.world.post(
            chat,
            self.me,
            message,
            media=kwargs.get("file"),
            scheduled=bool(schedule),
            schedule_date=schedule,
        )
        msg.out = True
        self.sent.append(("send_message", chat.id, message, kwargs))
        await self.world.broadcast(msg)
        if self._drop_next:
            self._drop_next = False
            raise ConnectionError("telegram: response lost after send")
        return msg

    async def send_file(self, entity, file, **kwargs):
        self._check_faults(file)
        chat = self._resolve_chat(entity)
        if isinstance(file, list):  # album
            msgs = [
                self.world.post(chat, self.me, "", media=SimpleNamespace(kind="file"))
                for _ in file
            ]
            for m in msgs:
                m.out = True
            self.sent.append(("send_file", chat.id, file, kwargs))
            if self._drop_next:
                self._drop_next = False
                raise ConnectionError("telegram: response lost after send")
            return msgs
        media = SimpleNamespace(kind="file", file=str(file)[:60])
        msg = self.world.post(chat, self.me, kwargs.get("caption") or "", media=media)
        msg.out = True
        self.sent.append(("send_file", chat.id, file, kwargs))
        if self._drop_next:
            self._drop_next = False
            raise ConnectionError("telegram: response lost after send")
        return msg

    async def edit_message(self, entity, message_id, text, **kwargs):
        self._check_faults(entity)
        chat = self._resolve_chat(entity)
        for m in chat.messages:
            if m.id == message_id:
                m.message = text
                return m
        raise ValueError(f"message {message_id} not found")

    async def get_messages(self, entity, **kwargs):
        self._check_faults(entity)
        chat = self._resolve_chat(entity)
        msgs = list(chat.messages)
        flt = kwargs.get("filter")
        if flt is not None and type(flt).__name__ == "InputMessagesFilterMyMentions":
            msgs = [m for m in msgs if f"@{self.me.username}" in (m.message or "")]
        ids = kwargs.get("ids")
        if ids is not None:
            wanted = ids if isinstance(ids, list) else [ids]
            return [m for m in msgs if m.id in wanted]
        limit = kwargs.get("limit")
        if limit:
            msgs = msgs[-limit:]
        return list(reversed(msgs))  # Telethon returns newest first

    async def download_media(self, *a, **kw):
        return None

    async def iter_messages(self, entity, **kwargs):
        return iter(await self.get_messages(entity, **kwargs))

    # --- low-level RPC -------------------------------------------------------------------

    async def __call__(self, request, **kwargs):
        self._check_faults(request)
        name = type(request).__name__
        handler = self._RPC.get(name)
        if handler is None:
            self.world.unhandled.append(name)
            return SimpleNamespace(updates=[])
        result = await handler(self, request)
        if self._drop_next:
            self._drop_next = False
            raise ConnectionError("telegram: response lost after request")
        return result

    async def _rpc_send_message(self, request):
        chat = self._resolve_chat(request.peer)
        text = request.message
        scheduled = getattr(request, "schedule_date", None)
        msg = self.world.post(
            chat,
            self.me,
            text,
            topic_id=getattr(request, "reply_to", None)
            and getattr(request.reply_to, "reply_to_msg_id", None),
            scheduled=bool(scheduled),
            schedule_date=scheduled,
        )
        msg.out = True
        self.sent.append(request)
        await self.world.broadcast(msg)
        return SimpleNamespace(
            updates=[UpdateMessageID(id=msg.id, random_id=request.random_id)],
            date=int(msg.date.timestamp()),
        )

    async def _rpc_send_media(self, request):
        chat = self._resolve_chat(request.peer)
        msg = self.world.post(chat, self.me, request.message or "", media=request.media)
        msg.out = True
        self.sent.append(request)
        return SimpleNamespace(updates=[UpdateMessageID(id=msg.id, random_id=request.random_id)])

    async def _rpc_nearest_dc(self, request):
        return SimpleNamespace(this_dc=1, nearest_dc=1)

    async def _rpc_sticker_set(self, request):
        short = getattr(getattr(request, "stickerset", None), "short_name", "")
        s = self.world.sticker_sets.get(short)
        if s is None:
            raise ValueError(f"STICKERSET_INVALID: {short}")
        return s

    async def _rpc_all_stickers(self, request):
        return SimpleNamespace(sets=[s.set for s in self.world.sticker_sets.values()])

    async def _rpc_scheduled_history(self, request):
        chat = self._resolve_chat(request.peer)
        return SimpleNamespace(messages=[m for m in chat.messages if m.scheduled])

    async def _rpc_save_draft(self, request):
        chat = self._resolve_chat(request.peer)
        chat.draft = FakeMessage(0, chat.id, self.me, request.message)
        return SimpleNamespace()

    async def _rpc_all_drafts(self, request):
        return SimpleNamespace(
            drafts=[
                SimpleNamespace(peer=SimpleNamespace(channel_id=c.id), draft=c.draft)
                for c in self.world.chats.values()
                if c.draft
            ]
        )

    async def _rpc_delete_messages(self, request):
        ids = set(request.id if isinstance(request.id, list) else [request.id])
        for chat in self.world.chats.values():
            chat.messages = [m for m in chat.messages if m.id not in ids]
        return SimpleNamespace()

    async def _rpc_edit_message(self, request):
        for chat in self.world.chats.values():
            for m in chat.messages:
                if m.id == request.id:
                    m.message = request.message
                    return SimpleNamespace(updates=[SimpleNamespace(message=m)])
        return SimpleNamespace(updates=[])


def _chat_id(entity) -> int:
    """Peer objects arrive as entity, InputPeer, or bare int — get the chat id."""
    return int(getattr(entity, "id", entity))


FakeTelegramClient._RPC = {
    "SendMessageRequest": FakeTelegramClient._rpc_send_message,
    "SendMediaRequest": FakeTelegramClient._rpc_send_media,
    "GetNearestDcRequest": FakeTelegramClient._rpc_nearest_dc,
    "GetStickerSetRequest": FakeTelegramClient._rpc_sticker_set,
    "GetAllStickersRequest": FakeTelegramClient._rpc_all_stickers,
    "GetScheduledHistoryRequest": FakeTelegramClient._rpc_scheduled_history,
    "SaveDraftRequest": FakeTelegramClient._rpc_save_draft,
    "GetAllDraftsRequest": FakeTelegramClient._rpc_all_drafts,
    "DeleteMessagesRequest": FakeTelegramClient._rpc_delete_messages,
    "EditMessageRequest": FakeTelegramClient._rpc_edit_message,
}
