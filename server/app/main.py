"""SCS-MATCH: HTTPS + SSE. One join shape serves every game."""


from __future__ import annotations

import asyncio
import json
import pathlib as _path
import random
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .api.health import router as health_router
from .api.chat_proxy import router as chat_proxy_router
from .settings import settings
from .multiplayer.protocol import (
    AuthorityConfig, Capabilities, CharacterSchema, CharacterState,
    ClientReportedSkillProvider, GameSessionConfig,
    JoinRequest, JoinResponse,
    MatchmakingConfig, SkillProvider, SkillRating,
    TurnAction, TurnActionResponse, TurnConfig, TurnState,
)
from .multiplayer.authority import AuthorityManager
from .multiplayer.dirty import DirtyFilter, FreshnessMatrix

from datastar_py.fastapi import DatastarResponse
from datastar_py import ServerSentEventGenerator as SSE

from demos import needs_server_gems, gem_target, is_turn_based


def _now_ms() -> int:
    return int(time.time() * 1000)


def get_skill_provider() -> SkillProvider:
    return ClientReportedSkillProvider()


class TurnManager:
    def __init__(self, room_id: str, player_ids: List[str],
                 config: GameSessionConfig,
                 validator: Optional[Any] = None):
        self.room_id = room_id
        self.config = config
        self.validator = validator
        order = list(player_ids)
        if config.turn.turn_order_strategy == "random":
            random.shuffle(order)
        initial = validator.initial_state(order, config) if validator else {}
        self.state = TurnState(
            room_id=room_id, game_id=config.game_id, phase="in_progress" if order else "waiting",
            current_player_id=order[0] if order else None, turn_number=1,
            turn_order=order, state=initial,
        )

    def apply_action(self, client_id: str, action: TurnAction) -> TurnActionResponse:
        st = self.state
        if st.phase != "in_progress":
            return TurnActionResponse(ok=True, accepted=False, reason="game_finished", turn_state=st)
        if action.action_type == "pass":
            if not self.config.turn.allow_pass:
                return TurnActionResponse(ok=True, accepted=False, reason="pass_not_allowed", turn_state=st)
            self._advance(reason="pass")
            return TurnActionResponse(ok=True, accepted=True, reason="ok", turn_state=st)
        if action.action_type == "resign":
            if not self.config.turn.allow_resign:
                return TurnActionResponse(ok=True, accepted=False, reason="resign_not_allowed", turn_state=st)
            self._finish(winner_id=self._other_player(client_id), end_reason="resign")
            return TurnActionResponse(ok=True, accepted=True, reason="ok", turn_state=st)
        if st.current_player_id != client_id:
            return TurnActionResponse(ok=True, accepted=False, reason="not_your_turn", turn_state=st)
        if self.validator is not None:
            accepted, reason, new_state = self.validator.validate(st.state, client_id, action, self.config)
            if not accepted or new_state is None:
                return TurnActionResponse(ok=True, accepted=False, reason=reason, turn_state=st)
            st.state = new_state
            st.last_action = {"player_id": client_id, "action_type": action.action_type,
                              "payload": action.payload, "timestamp": _now_ms()}
            winner, is_draw, end_reason = self.validator.check_terminal(new_state, self.config)
            if winner or is_draw:
                self._finish(winner_id=winner, is_draw=is_draw, end_reason=end_reason or "win")
            elif self.config.turn.max_turns is not None and st.turn_number >= self.config.turn.max_turns:
                self._finish(winner_id=None, is_draw=True, end_reason="max_turns")
            else:
                self._advance()
            return TurnActionResponse(ok=True, accepted=True, reason="ok", turn_state=st)
        if action.proposed_state is not None:
            if not isinstance(action.proposed_state, dict):
                return TurnActionResponse(ok=True, accepted=False, reason="invalid_proposed_state", turn_state=st)
            st.state = action.proposed_state
        st.last_action = {"player_id": client_id, "action_type": action.action_type,
                          "payload": action.payload, "timestamp": _now_ms()}
        if action.claimed_winner_id or action.claimed_is_draw:
            self._finish(winner_id=action.claimed_winner_id, is_draw=action.claimed_is_draw,
                         end_reason=action.claimed_end_reason or "win")
        elif self.config.turn.max_turns is not None and st.turn_number >= self.config.turn.max_turns:
            self._finish(winner_id=None, is_draw=True, end_reason="max_turns")
        else:
            self._advance()
        return TurnActionResponse(ok=True, accepted=True, reason="ok", turn_state=st)

    def on_player_left(self, client_id: str) -> Optional[TurnActionResponse]:
        if self.state.phase != "in_progress":
            return None
        self._finish(winner_id=self._other_player(client_id), end_reason="opponent_left")
        return TurnActionResponse(ok=True, accepted=True, reason="opponent_left",
                                  turn_state=self.state)

    def _advance(self, reason: str = "move") -> None:
        st = self.state
        if not st.turn_order:
            return
        idx = st.turn_order.index(st.current_player_id) if st.current_player_id in st.turn_order else -1
        st.current_player_id = st.turn_order[(idx + 1) % len(st.turn_order)]
        st.turn_number += 1
        st.timestamp = _now_ms()

    def _finish(self, winner_id: Optional[str], is_draw: bool = False,
                end_reason: Optional[str] = None) -> None:
        st = self.state
        st.phase = "finished"
        st.winner_id = winner_id
        st.is_draw = is_draw
        st.end_reason = end_reason
        st.current_player_id = None
        st.timestamp = _now_ms()

    def _other_player(self, client_id: str) -> Optional[str]:
        others = [p for p in self.state.turn_order if p != client_id]
        return others[0] if others else None


# ---------------------------------------------------------------------------
# Match-making
# ---------------------------------------------------------------------------

@dataclass
class QueuedPlayer:
    client_id: str
    session_id: str
    game_id: str
    config: GameSessionConfig
    skill: SkillRating
    enqueued_at_ms: int = field(default_factory=_now_ms)
    fingerprint: str = ""


class Matchmaker:
    """Skill-aware queue. Grouping key = config fingerprint; ranking key =
    skill gap, widened by wait time so outliers eventually match.

    No DB knowledge: it only compares ``SkillRating.rating`` values that
    the ``SkillProvider`` already resolved.
    """

    def __init__(self, skill_provider: SkillProvider):
        self.skill_provider = skill_provider
        self.queues: Dict[str, List[QueuedPlayer]] = {}  # fingerprint -> waiters

    async def enqueue(self, client_id: str, session_id: str,
                      req: JoinRequest, skill: SkillRating) -> QueuedPlayer:
        config = req.config or GameSessionConfig.default(req.game_id)
        qp = QueuedPlayer(client_id=client_id, session_id=session_id,
                          game_id=req.game_id, config=config, skill=skill,
                          fingerprint=config.fingerprint())
        self.queues.setdefault(qp.fingerprint, []).append(qp)
        return qp

    def cancel(self, client_id: str) -> bool:
        for key, waiters in list(self.queues.items()):
            for i, qp in enumerate(waiters):
                if qp.client_id == client_id:
                    waiters.pop(i)
                    if not waiters:
                        self.queues.pop(key, None)
                    return True
        return False

    def queue_position(self, client_id: str) -> Optional[int]:
        for waiters in self.queues.values():
            for i, qp in enumerate(waiters):
                if qp.client_id == client_id:
                    return i + 1
        return None

    def try_form_matches(self, fingerprint: str) -> List[List[QueuedPlayer]]:
        waiters = self.queues.get(fingerprint, [])
        if not waiters:
            return []
        config = waiters[0].config
        need = config.max_players_per_room
        now = _now_ms()
        formed: List[List[QueuedPlayer]] = []
        pool = sorted(waiters, key=lambda q: q.enqueued_at_ms)
        while len(pool) >= config.min_players_per_room:
            oldest = pool[0]
            tolerance = config.matchmaking.tolerance_for_wait_ms(now - oldest.enqueued_at_ms)
            candidates = [q for q in pool[1:]
                          if abs(q.skill.rating - oldest.skill.rating) <= tolerance]
            candidates.sort(key=lambda q: abs(q.skill.rating - oldest.skill.rating))
            group = [oldest] + candidates[: need - 1]
            if len(group) < config.min_players_per_room:
                break
            for q in group:
                pool.remove(q)
            formed.append(group)
        if pool:
            self.queues[fingerprint] = pool
        else:
            self.queues.pop(fingerprint, None)
        return formed


# ---------------------------------------------------------------------------
# Core session & room
# ---------------------------------------------------------------------------

@dataclass
class ClientSession:
    client_id: str
    session_id: str
    environment: str
    character_name: str
    is_synchronizer: bool
    game_id: str = "default"
    player_id: str = ""
    room_id: Optional[str] = None
    config: Optional[GameSessionConfig] = None
    skill: Optional[SkillRating] = None
    seat: Optional[int] = None
    match_status: str = "ready"
    created_at: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    sse_queue: Optional[asyncio.Queue] = None
    # --- Disconnection (event-driven, no timer loop) ---
    # Last ms a packet was received from this client. Updated on every
    # inbound packet (see _touch). Checked lazily on the next packet
    # from ANY client (see _sweep_expired_locked).
    last_seen_ms: int = field(default_factory=_now_ms)
    # Per-client budget from GameSessionConfig.disconnection_time_sec.
    # Default 60. 0 == never expire.
    disconnection_time_sec: float = 60.0


@dataclass
class Room:
    room_id: str
    game_id: str
    config: GameSessionConfig
    player_ids: List[str] = field(default_factory=list)
    status: str = "waiting"
    custom: Dict[str, Any] = field(default_factory=dict)
    turn_manager: Optional[TurnManager] = None
    created_at_ms: int = field(default_factory=_now_ms)

    @property
    def is_full(self) -> bool:
        return len(self.player_ids) >= self.config.max_players_per_room


class MultiplayerRegistry:
    VALIDATORS: Dict[str, Any] = {}

    def __init__(self, skill_provider: Optional[SkillProvider] = None):
        self.clients: Dict[str, ClientSession] = {}
        self.sessions: Dict[str, str] = {}
        self.authority = AuthorityManager()
        self.dirty_filter = DirtyFilter()
        self.freshness = FreshnessMatrix()
        self.synchronizers: Dict[str, str] = {}
        self.character_cache: Dict[str, CharacterState] = {}
        self.item_cache: Dict[str, Dict[str, Any]] = {}
        self.item_environments: Dict[str, str] = {}
        self.skill_provider = skill_provider or get_skill_provider()
        self.matchmaker = Matchmaker(self.skill_provider)
        self.rooms: Dict[str, Room] = {}
        self._lock = asyncio.Lock()
        self.running = True
        # --- Empty-game cleanup (event-driven, no timer loop) ---
        # env -> ms when it became empty (last client left / timed out).
        # Deleted lazily on the next packet from ANY client once
        # now - empty_since > ttl. See _sweep_empty_games_locked.
        self.env_empty_since: Dict[str, int] = {}
        # env -> TTL (sec) captured from the last client that was in it.
        # Default 300s (5 min). Overridable per-game via config.
        self.env_empty_ttl_sec: Dict[str, float] = {}
        # --- Character schema (one per game_id, stored with game data) ---
        # game_id -> stored CharacterSchema. First join stores it; later
        # joins must match it exactly or get 409 plus the stored schema.
        # Same schema for all rooms of the same game, including private
        # rooms. Kept while the game is empty inside empty_game_ttl_sec,
        # deleted with game data when the TTL expires.
        self.game_character_schemas: Dict[str, CharacterSchema] = {}
        # client_id -> server-set fields that arrived before the client
        # had a full stored state (e.g. score set on gem collection
        # before the first character patch). Merged into the full state
        # on the next client patch, then cleared.
        self._character_server_pending: Dict[str, Dict[str, Any]] = {}
        # env <-> game_id bookkeeping so _delete_game_data_locked can
        # delete the schema for the game(s) that used that env, once no
        # env still uses them and no client with that game_id remains.
        self._env_to_game_ids: Dict[str, set] = {}
        self._game_to_envs: Dict[str, set] = {}
        # --- Server-authoritative game data ---
        # Gem games: server spawns gems, not the synchronizer client.
        # Per-env gem counter so instanceIds never collide / get reused.
        self._top_down_gem_counter: Dict[str, int] = {}
        # How long an empty game keeps its data before lazy deletion.
        self.EMPTY_GAME_TTL_SEC = 300.0

    async def start(self):
        print("[SCS-MP-SYNC] Registry started")

    async def stop(self):
        self.running = False
        print("[SCS-MP-SYNC] Registry stopped")

    # -- helpers ---------------------------------------------------------
    def _schema_for(self, session: ClientSession) -> CharacterSchema:
        """Stored schema for a session's game_id, falling back to the
        session's own config (pre-join / legacy paths)."""
        stored = self.game_character_schemas.get(session.game_id)
        if stored is not None:
            return stored
        if session.config is not None and session.config.character_schema is not None:
            return session.config.character_schema
        return CharacterSchema()

    # -- server-authoritative game data + empty-game cleanup --------
    # The server owns game data from the moment the FIRST player for a
    # particular game connects. Clients never spawn authoritative state:
    # they send character-state + collections, the server spawns / respawns
    # items, keeps the field full, and hands the full snapshot to late
    # joiners via _mp_for_client bootstrap. If the last client leaves,
    # the game data is kept for EMPTY_GAME_TTL_SEC (5 min) so a quick
    # reconnect resumes the same game, then deleted lazily on the next
    # packet from ANY client. No background task, no sleep, no timer:
    # deletion only happens because a packet arrived.
    def _is_top_down_game(self, game_id: str = "", env_name: str = "") -> bool:
        # Strictly the 2D top-down game. Babylon is NOT a top-down
        # game — it is a 3D third-person game that happens to share
        # the gem mechanic. Do not add it here.
        return game_id == "top-down-demo" or env_name == "top-down-demo"

    def _is_gem_game(self, game_id: str = "", env_name: str = "") -> bool:
        # Delegates to demos.py — no per-game list here anymore.
        return needs_server_gems(game_id, env_name)

    def _delete_game_data_locked(self, env_name: str, now_ms: int) -> List[str]:
        """Delete ALL stored data for one empty game (env). Caller holds lock."""
        deleted: List[str] = []
        # Items for this env.
        for iid in [k for k, v in list(self.item_environments.items()) if v == env_name]:
            deleted.append(iid)
            self.item_environments.pop(iid, None)
            self.item_cache.pop(iid, None)
            try:
                self.dirty_filter.cache.pop(iid, None)
            except Exception:
                pass
            try:
                if iid in getattr(self.authority, "itemOwners", {}):
                    del self.authority.itemOwners[iid]
            except Exception:
                pass
        # Any item_cache rows that carry environment==env but were never bound.
        for iid in [k for k, v in list(self.item_cache.items())
                    if isinstance(v, dict) and v.get("environment") == env_name]:
            deleted.append(iid)
            self.item_cache.pop(iid, None)
            self.item_environments.pop(iid, None)
            try:
                self.dirty_filter.cache.pop(iid, None)
            except Exception:
                pass
        # Characters for this env (should already be gone on leave).
        for cid in [cid for cid, st in list(self.character_cache.items())
                    if cid in self.clients and self.clients[cid].environment == env_name]:
            self.character_cache.pop(cid, None)
        # Server-pending fields for clients in this env.
        for cid in [cid for cid in list(self._character_server_pending)
                    if cid in self.clients and self.clients[cid].environment == env_name]:
            self._character_server_pending.pop(cid, None)
        # Rooms with no players that belong to this env.
        for rid in [rid for rid, room in list(self.rooms.items())
                    if not room.player_ids and (
                        getattr(room, "_join_key", "") == f"direct:{env_name}"
                        or getattr(room, "_join_key", "") == env_name
                        or room.game_id == env_name)]:
            self.rooms.pop(rid, None)
        # Env authority / synchronizer pointers.
        try:
            if hasattr(self.authority, "envAuthority"):
                self.authority.envAuthority.pop(env_name, None)
        except Exception:
            pass
        self.synchronizers.pop(env_name, None)
        self._top_down_gem_counter.pop(env_name, None)
        self.env_empty_since.pop(env_name, None)
        self.env_empty_ttl_sec.pop(env_name, None)
        # Character schema for the game(s) that used this env. The schema
        # lives with game data: it is deleted here, with everything else,
        # once the TTL has expired. It survives while the game is empty
        # inside the TTL because this method is only called after the TTL.
        _game_ids = list(self._env_to_game_ids.get(env_name, set()))
        if env_name in self.game_character_schemas and env_name not in _game_ids:
            _game_ids.append(env_name)
        self._env_to_game_ids.pop(env_name, None)
        for gid in _game_ids:
            envs = self._game_to_envs.get(gid)
            if envs is not None:
                envs.discard(env_name)
                if envs:
                    continue
                self._game_to_envs.pop(gid, None)
            if not any(c.game_id == gid for c in self.clients.values()):
                self.game_character_schemas.pop(gid, None)
        return list(dict.fromkeys(deleted))

    def _sweep_empty_games_locked(self, now_ms: int) -> List[str]:
        """Delete game data for envs empty longer than their TTL. Caller holds lock."""
        deleted_envs: List[str] = []
        for env_name, since in list(self.env_empty_since.items()):
            # Still someone here? Should not happen, but be safe.
            if any(c.environment == env_name for c in self.clients.values()):
                self.env_empty_since.pop(env_name, None)
                self.env_empty_ttl_sec.pop(env_name, None)
                continue
            ttl = self.env_empty_ttl_sec.get(env_name, self.EMPTY_GAME_TTL_SEC)
            if ttl is None or ttl <= 0:
                continue  # 0 == keep forever
            if (now_ms - since) > ttl * 1000:
                self._delete_game_data_locked(env_name, now_ms)
                deleted_envs.append(env_name)
        return deleted_envs

    def _spawn_top_down_gems_locked(self, env_name: str, count: int, now_ms: int) -> List[Dict[str, Any]]:
        """Server-spawn `count` gems for one gem-game env. Caller holds lock."""
        import random as _random
        out: List[Dict[str, Any]] = []
        for _ in range(max(0, count)):
            self._top_down_gem_counter[env_name] = self._top_down_gem_counter.get(env_name, 0) + 1
            n = self._top_down_gem_counter[env_name]
            iid = f"{env_name}:gem-{n}"
            x = 80 + _random.random() * (2000 - 160)
            y = 80 + _random.random() * (2000 - 160)
            row: Dict[str, Any] = {
                "instanceId": iid, "itemName": "gem",
                "pos": [x, y], "rot": [0.0, 0.0, 0.0, 1.0],
                "isCollected": False, "collectedByClientId": None,
                "ownerClientId": None, "environment": env_name,
                "timestamp": now_ms, "server_spawned": True,
            }
            self.item_cache[iid] = row
            self.item_environments[iid] = env_name
            try:
                self.dirty_filter.cache[iid] = dict(row)
            except Exception:
                pass
            out.append(dict(row))
        return out

    def _ensure_game_data_locked(self, env_name: str, game_id: str, now_ms: int) -> List[Dict[str, Any]]:
        """Ensure server-owned data exists for a game. Caller holds lock.

        Gem games (top-down-demo, babylon-demo): keep
        TOP_DOWN_GEM_TARGET live gems. Other games: no-op today (their
        state is character + turn + custom, already server-held).
        Called on first join, on env switch, and after every collection
        (to top up).
        """
        target = gem_target(game_id, env_name)
        if not self._is_gem_game(game_id, env_name):
            return []
        live = sum(1 for v in self.item_cache.values()
                if isinstance(v, dict) and v.get("environment") == env_name
                and not v.get("isCollected"))
        if live >= target:
            return []
        return self._spawn_top_down_gems_locked(env_name, target - live, now_ms)

    # -- disconnection: event-driven, no timers -------------------------
    def _is_expired(self, session: ClientSession, now_ms: int) -> bool:
        dt = session.disconnection_time_sec
        if dt is None or dt <= 0:
            return False
        return (now_ms - session.last_seen_ms) > dt * 1000

    def _touch(self, session: ClientSession, now_ms: int) -> None:
        session.last_seen_ms = now_ms
        session.last_seen = now_ms / 1000.0

    async def _sweep_expired_locked(self, now_ms: int, exclude: Optional[str] = None) -> List[str]:
        """Evict every client silent longer than its own budget.

        MUST be called with self._lock held. Call it at the top of every
        packet handler, after touching the sender. No timers: the sweep
        only runs because a packet arrived.
        """
        expired = [
            cid for cid, s in list(self.clients.items())
            if cid != exclude and self._is_expired(s, now_ms)
        ]
        for cid in expired:
            await self._remove_client_locked(cid, reason="timeout", now_ms=now_ms)
        # Empty-game cleanup rides on the same packet. An env that just
        # became empty has age 0, so it is NOT deleted here; it is only
        # deleted once a LATER packet arrives after its TTL has passed.
        self._sweep_empty_games_locked(now_ms)
        return expired

    async def _check_sender_alive_locked(self, client_id: str, now_ms: int) -> Optional[ClientSession]:
        """If sender is already expired, evict + return None. Else touch + sweep others."""
        sess = self.clients.get(client_id)
        if sess is None:
            return None
        if self._is_expired(sess, now_ms):
            await self._remove_client_locked(client_id, reason="timeout", now_ms=now_ms)
            return None
        self._touch(sess, now_ms)
        await self._sweep_expired_locked(now_ms, exclude=client_id)
        return sess

    async def handle_heartbeat(self, client_id: str) -> Dict[str, Any]:
        """Explicit keep-alive packet. Counts as a packet like any other."""
        async with self._lock:
            now = _now_ms()
            sess = self.clients.get(client_id)
            if not sess:
                raise HTTPException(403, "Unknown client")
            if self._is_expired(sess, now):
                await self._remove_client_locked(client_id, reason="timeout", now_ms=now)
                raise HTTPException(403, "disconnected: timeout")
            self._touch(sess, now)
            expired = await self._sweep_expired_locked(now, exclude=client_id)
            return {"ok": True, "expired": expired, "server_timestamp": now,
                    "disconnection_time_sec": sess.disconnection_time_sec}

    # -- join ----------------------------------------------------------
    async def join(self, req: JoinRequest) -> JoinResponse:
        req.normalize()
        config = req.config or GameSessionConfig.default(req.game_id)
        async with self._lock:
            now = _now_ms()
            # New arrival is itself a packet: sweep stale clients first
            # so existing_clients / synchronizer election are accurate.
            await self._sweep_expired_locked(now)
            # --- Character schema: one per game_id, first join wins. ---
            # Must run before any session is created, so a rejected join
            # leaves no session, no synchronizer, and no room behind.
            declared = config.character_schema or CharacterSchema()
            stored = self.game_character_schemas.get(req.game_id)
            if stored is None:
                self.game_character_schemas[req.game_id] = declared
                stored = declared
            elif stored.canonical_dict() != declared.canonical_dict():
                # Different schema for the same game_id, even with a
                # different room_code / room_id. Do not create a second
                # bucket. Return the stored schema so the client can
                # rejoin with it.
                raise HTTPException(409, detail={
                    "error": "character_schema_mismatch",
                    "game_id": req.game_id,
                    "character_schema": stored.to_dict(),
                    "message": "game_id already has a character schema; rejoin with the stored schema",
                })
            # Effective schema for this join is the stored one.
            config.character_schema = stored
            # Track env <-> game_id so empty-game deletion can delete
            # the schema with the game data once the TTL expires.
            self._env_to_game_ids.setdefault(req.environment_name, set()).add(req.game_id)
            self._game_to_envs.setdefault(req.game_id, set()).add(req.environment_name)
            client_id = uuid.uuid4().hex[:8]
            session_id = uuid.uuid4().hex[:16]
            skill = await self.skill_provider.resolve_skill(
                player_id=req.player_id or client_id, game_id=req.game_id,
                client_reported=req.skill)
            is_sync = req.environment_name not in self.synchronizers
            session = ClientSession(
                client_id=client_id, session_id=session_id,
                environment=req.environment_name, character_name=req.character_name,
                is_synchronizer=is_sync, game_id=req.game_id,
                player_id=req.player_id or client_id, config=config, skill=skill,
                last_seen_ms=now, disconnection_time_sec=config.disconnection_time_sec)
            self.clients[client_id] = session
            self.sessions[session_id] = client_id
            # Game is no longer empty: cancel any pending empty-game deletion.
            self.env_empty_since.pop(req.environment_name, None)
            self.env_empty_ttl_sec.pop(req.environment_name, None)
            if is_sync:
                self.synchronizers[req.environment_name] = client_id
            # Register with the authority manager so get_resolved_owner()
            # resolves to the first player. Previously join never called
            # client_arrives (only switch_environment did), so the first
            # player's item updates were silently dropped and only the
            # spawner saw gems locally (optimistic local add).
            try:
                _auth_ev = self.authority.client_arrives(client_id, req.environment_name)
            except Exception:
                _auth_ev = {}
            # Server owns game data from the FIRST join. Spawn it now so
            # the joining client's bootstrap already contains the field.
            # For gem games this creates 24 gems; for other games no-op.
            try:
                self._ensure_game_data_locked(req.environment_name, req.game_id, now)
            except Exception:
                pass
            if config.requires_matchmaking:
                session.match_status = "queued"
                await self.matchmaker.enqueue(client_id, session_id, req, skill)
                pos = self.matchmaker.queue_position(client_id) or 1
                await self._send_to_client(client_id, "matchmaking-queued",
                                           {"eventType": "matchmaking_queued", "clientId": client_id,
                                            "gameId": req.game_id, "queuePosition": pos,
                                            "timestamp": _now_ms()})
                await self._try_fill_queues(config.fingerprint())
                after = self.clients.get(client_id)
                status = after.match_status if after else "queued"
                room_id = after.room_id if after else None
                return JoinResponse(client_id=client_id, session_id=session_id,
                                    is_synchronizer=is_sync, existing_clients=0,
                                    room_id=room_id, game_id=req.game_id, match_status=status,
                                    queue_position=pos if status == "queued" else None,
                                    seat=after.seat if after else None,
                                    skill_source=skill.source, config=config,
                                    character_schema=stored,
                                    disconnection_time_sec=config.disconnection_time_sec)
            room = self._get_or_create_direct_room(req, config)
            seat = len(room.player_ids)
            room.player_ids.append(client_id)
            session.room_id = room.room_id
            session.seat = seat
            session.match_status = "ready"
            await self._on_room_changed(room)
            return JoinResponse(client_id=client_id, session_id=session_id,
                                is_synchronizer=is_sync,
                                existing_clients=len(room.player_ids) - 1,
                                room_id=room.room_id, game_id=req.game_id,
                                match_status="ready", seat=seat, player_index=seat,
                                skill_source=skill.source, config=config,
                                character_schema=stored,
                                disconnection_time_sec=config.disconnection_time_sec)

    async def _try_fill_queues(self, fingerprint: str) -> None:
        for group in self.matchmaker.try_form_matches(fingerprint):
            config = group[0].config
            room = Room(room_id=uuid.uuid4().hex[:12], game_id=group[0].game_id, config=config)
            self.rooms[room.room_id] = room
            for seat, qp in enumerate(group):
                session = self.clients.get(qp.client_id)
                if session is None:
                    continue
                room.player_ids.append(qp.client_id)
                session.room_id = room.room_id
                session.seat = seat
                session.match_status = "ready"
            await self._on_room_changed(room)

    def _get_or_create_direct_room(self, req: JoinRequest, config: GameSessionConfig) -> Room:
        """Direct: one room per environment. Explicit room_id / room_code
        lets friend / private rooms share a key instead."""
        key = req.room_id or req.room_code or f"direct:{req.environment_name or config.game_id}"
        for room in self.rooms.values():
            if room.room_id == key or getattr(room, "_join_key", None) == key:
                return room
        room = Room(room_id=key if req.room_id else uuid.uuid4().hex[:12],
                    game_id=config.game_id, config=config)
        room._join_key = key  # type: ignore[attr-defined]
        self.rooms[room.room_id] = room
        return room

    async def _on_room_changed(self, room: Room) -> None:
        """Room filled / changed: start turn management when configured."""
        if room.status == "in_progress":
            return
        if len(room.player_ids) < room.config.min_players_per_room:
            room.status = "waiting"
            return
        # Room is playable.
        if room.config.requires_turn_based and room.turn_manager is None:
            validator = self.VALIDATORS.get(room.game_id)
            order = list(room.player_ids)
            if room.config.turn.turn_order_strategy == "skill_order":
                order.sort(key=lambda cid: (self.clients[cid].skill.rating
                                            if self.clients.get(cid) and self.clients[cid].skill else 1000.0))
            room.turn_manager = TurnManager(room.room_id, order, room.config, validator)
            room.status = "in_progress"
        else:
            room.status = "in_progress"
        await self._broadcast_room_state(room)

    # -- match-making helpers ---------------------------
    async def cancel_matchmaking(self, client_id: str) -> bool:
        async with self._lock:
            session = self.clients.get(client_id)
            if session:
                # Cancel is a packet: touch, then sweep others.
                now = _now_ms()
                if self._is_expired(session, now):
                    await self._remove_client_locked(client_id, reason="timeout", now_ms=now)
                    return False
                self._touch(session, now)
                await self._sweep_expired_locked(now, exclude=client_id)
                session.match_status = "cancelled"
            return self.matchmaker.cancel(client_id)

    # -- leave / switch -------------------------------------------------
    async def leave(self, client_id: str):
        async with self._lock:
            await self._remove_client_locked(client_id, reason="leave")

    async def _remove_client_locked(self, client_id: str, reason: str = "leave",
                                    now_ms: Optional[int] = None) -> bool:
        """Core removal. Caller must hold self._lock.

        reason: "leave" | "timeout" | "env_switch" | "disconnect".
        For "timeout" the broadcast client-left carries reason="timeout"
        so demos can show 'dropped: timeout'.
        """
        now = now_ms if now_ms is not None else _now_ms()
        session = self.clients.pop(client_id, None)
        if not session:
            return False
        self.sessions.pop(session.session_id, None)
        self.matchmaker.cancel(client_id)
        if session.sse_queue:
            self._enqueue_signal(session.sse_queue, "__close__", {})
        session.sse_queue = None
        if self.synchronizers.get(session.environment) == client_id:
            remaining = [c for c in self.clients.values() if c.environment == session.environment]
            if remaining:
                new_sync = remaining[0].client_id
                self.synchronizers[session.environment] = new_sync
                for c in remaining:
                    c.is_synchronizer = (c.client_id == new_sync)
                await self._broadcast_signal("synchronizer-changed",
                                             {"newSynchronizerId": new_sync, "reason": "disconnection",
                                              "timestamp": now},
                                             environment=session.environment)
            else:
                self.synchronizers.pop(session.environment, None)
        events = self.authority.client_leaves(client_id)
        for ev in events:
            if ev["type"] == "env-authority-changed":
                await self._broadcast_signal("env-authority-changed", ev,
                                             environment=ev.get("environmentName", session.environment))
            elif ev["type"] == "item-authority-changed":
                await self._broadcast_signal("item-authority-changed", ev, environment=session.environment)
        self.freshness.client_leaves(session.environment, client_id)
        # Room + turn bookkeeping.
        room = self.rooms.get(session.room_id) if session.room_id else None
        if room and client_id in room.player_ids:
            room.player_ids.remove(client_id)
            if room.turn_manager and room.turn_manager.state.phase == "in_progress":
                fired = room.turn_manager.on_player_left(client_id)
                if fired:
                    room.status = "finished"
                    await self._broadcast_to_room(room.room_id, "game-over",
                                                 {"roomId": room.room_id,
                                                  "winnerId": room.turn_manager.state.winner_id,
                                                  "isDraw": room.turn_manager.state.is_draw,
                                                  "endReason": "opponent_left",
                                                  "turnState": room.turn_manager.state.to_dict(),
                                                  "timestamp": now})
            if not room.player_ids:
                self.rooms.pop(room.room_id, None)
        # Character state for this client is gone with the client.
        # The schema stays: it lives with game data, not with the client.
        self.character_cache.pop(client_id, None)
        self._character_server_pending.pop(client_id, None)
        # If this env is now empty, start the 5-minute grace clock.
        # Data is NOT deleted here; it is deleted lazily by
        # _sweep_empty_games_locked on the next packet after the TTL.
        # A reconnect within 5 min resumes the same game, including the
        # same character schema.
        _remaining = [c for c in self.clients.values() if c.environment == session.environment]
        if not _remaining:
            ttl = 300.0
            try:
                if session.config is not None:
                    ttl = float(getattr(session.config, "empty_game_ttl_sec", 300.0) or 300.0)
            except Exception:
                ttl = 300.0
            # Only start the clock if we actually have data worth keeping.
            # If there is no data, deletion is a no-op anyway.
            self.env_empty_since[session.environment] = now
            self.env_empty_ttl_sec[session.environment] = ttl
        else:
            self.env_empty_since.pop(session.environment, None)
            self.env_empty_ttl_sec.pop(session.environment, None)
        await self._broadcast_signal("client-left",
                                    {"eventType": "left", "clientId": client_id,
                                     "totalClients": sum(s.environment == session.environment
                                                         for s in self.clients.values()),
                                     "reason": reason,
                                     "timestamp": now},
                                    environment=session.environment)
        return True

    async def get_client_by_session(self, session_id: str) -> Optional[ClientSession]:
        client_id = self.sessions.get(session_id)
        if not client_id:
            return None
        return self.clients.get(client_id)

    async def switch_environment(self, client_id: str, environment: str):
        # Rooms stay where they are.
        async with self._lock:
            client = self.clients.get(client_id)
            if not client:
                raise HTTPException(403, "Unknown client")
            now = _now_ms()
            if self._is_expired(client, now):
                await self._remove_client_locked(client_id, reason="timeout", now_ms=now)
                raise HTTPException(403, "disconnected: timeout")
            self._touch(client, now)
            await self._sweep_expired_locked(now, exclude=client_id)
            if not environment or environment == client.environment:
                return client.environment
            previous_environment = client.environment
            previous_clients = sum(s.environment == previous_environment
                                   for s in self.clients.values()) - 1
            authority_events = self.authority.client_leaves(client_id)
            self.character_cache.pop(client_id, None)
            self._character_server_pending.pop(client_id, None)
            self.freshness.client_leaves(previous_environment, client_id)
            client.environment = environment
            # New env is no longer empty; old env may have just become empty.
            # The character schema is per game_id, not per env, so it
            # survives the switch. Track the new env <-> game_id mapping
            # so empty-game deletion still finds the schema later.
            self.env_empty_since.pop(environment, None)
            self.env_empty_ttl_sec.pop(environment, None)
            self._env_to_game_ids.setdefault(environment, set()).add(client.game_id)
            self._game_to_envs.setdefault(client.game_id, set()).add(environment)
            _old_remaining = [c for c in self.clients.values() if c.environment == previous_environment]
            if not _old_remaining:
                self.env_empty_since[previous_environment] = _now_ms()
                self.env_empty_ttl_sec[previous_environment] = 300.0
            # Server owns data for the new env too: spawn if first in.
            try:
                self._ensure_game_data_locked(environment, client.game_id, _now_ms())
            except Exception:
                pass
            authority_event = self.authority.client_arrives(client_id, environment)
            self.freshness.client_enters(environment, client_id, list(self.item_cache))
            await self._broadcast_signal("client-left",
                                        {"eventType": "left", "clientId": client_id,
                                         "totalClients": previous_clients, "reason": "env_switch",
                                         "timestamp": _now_ms()},
                                        environment=previous_environment)
            for event in authority_events:
                if event["type"] == "env-authority-changed":
                    event["reason"] = "env_switch"
                    await self._broadcast_signal("env-item-authority-changed", event,
                                                 environment=previous_environment)
                elif event["type"] == "item-authority-changed":
                    event["reason"] = "env_switch"
                    await self._broadcast_signal("item-authority-changed", event,
                                                 environment=previous_environment)
            await self._broadcast_signal("client-joined",
                                        {"eventType": "joined", "clientId": client_id,
                                         "environment": environment, "character": client.character_name,
                                         "totalClients": sum(s.environment == environment
                                                             for s in self.clients.values()),
                                         "reason": "env_switch", "timestamp": _now_ms()},
                                        environment=environment)
            if authority_event.get("became_authority"):
                await self._broadcast_signal("env-item-authority-changed",
                                             {"environmentName": environment, "previousAuthorityId": None,
                                              "newAuthorityId": client_id, "reason": "env_switch",
                                              "timestamp": _now_ms()},
                                             environment=environment)
            if client.sse_queue:
                for signal_name, payload in self._bootstrap_for_client(client):
                    self._enqueue_signal(client.sse_queue, signal_name, payload)
            return environment

    # -- SSE plumbing: one mp signal per client ---
    def _mp_patch_for(self, signal_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        mp: Dict[str, Any] = {"timestamp": payload.get("timestamp", _now_ms())}
        if signal_name == "matchmaking-queued":
            mp["match"] = {"status": "queued", "queuePosition": payload.get("queuePosition"),
                           "gameId": payload.get("gameId")}
        elif signal_name == "match-found":
            mp["match"] = {"status": "matched", "roomId": payload.get("roomId"),
                           "gameId": payload.get("gameId"), "playerIds": payload.get("playerIds"),
                           "seat": payload.get("seat"), "turnOrder": payload.get("turnOrder")}
            mp["room"] = {"roomId": payload.get("roomId"), "gameId": payload.get("gameId"),
                          "playerIds": payload.get("playerIds")}
            if payload.get("turnState") is not None:
                mp["turnState"] = payload.get("turnState")
        elif signal_name == "turn-state-update":
            mp["turnState"] = payload.get("turnState")
        elif signal_name == "game-over":
            mp["turnState"] = payload.get("turnState")
            mp["match"] = {"status": "finished", "roomId": payload.get("roomId")}
        elif signal_name == "room-ready":
            mp["room"] = {"roomId": payload.get("roomId"), "gameId": payload.get("gameId"),
                          "playerIds": payload.get("playerIds"), "turnOrder": payload.get("turnOrder")}
        elif signal_name == "character-state-update":
            mp["characters"] = payload.get("updates")
        elif signal_name == "item-state-update":
            mp["items"] = {"updates": payload.get("updates"), "collections": payload.get("collections")}
        elif signal_name == "client-joined":
            mp["clientJoined"] = payload
        elif signal_name == "client-left":
            mp["clientLeft"] = payload
        elif signal_name == "synchronizer-changed":
            mp["synchronizer"] = payload
        elif signal_name == "item-authority-changed":
            mp["itemAuthority"] = payload
        elif signal_name == "env-item-authority-changed":
            mp["envAuthority"] = payload
        elif signal_name == "effects-state-update":
            mp["effects"] = payload
        elif signal_name == "custom-state-update":
            mp["custom"] = payload.get("custom")
            mp["room"] = {"roomId": payload.get("roomId")}
        else:
            mp[signal_name] = payload
        return mp

    def _mp_for_client(self, client: ClientSession) -> Dict[str, Any]:
        now = _now_ms()
        env_name = client.environment
        room = self.rooms.get(client.room_id) if client.room_id else None
        # Stored schema for this game_id, so a late joiner / resync learns
        # the schema before sending any character patch. This is the
        # first bootstrap event on GET /stream.
        schema = self.game_character_schemas.get(client.game_id)
        if schema is None and client.config is not None and client.config.character_schema is not None:
            schema = client.config.character_schema
        if schema is None:
            schema = CharacterSchema()
        mp: Dict[str, Any] = {
            "timestamp": now,
            "connection": {"clientId": client.client_id, "sessionId": client.session_id,
                           "isSynchronizer": client.is_synchronizer, "environment": env_name,
                           "roomId": client.room_id, "gameId": client.game_id,
                           "matchStatus": client.match_status,
                           "disconnectionTimeSec": client.disconnection_time_sec,
                           "emptyGameTtlSec": self.env_empty_ttl_sec.get(env_name, 300.0)},
            "characterSchema": schema.to_dict(),
            "clients": [{"clientId": c.client_id, "character": c.character_name,
                         "environment": c.environment}
                        for c in self.clients.values() if c.environment == env_name],
            "characters": [st.to_dict() for oid, st in self.character_cache.items()
                           if oid in self.clients and self.clients[oid].environment == env_name],
            "items": {"updates": [row for row in self.item_cache.values()
                                  if self._item_belongs_to_environment(row.get("instanceId", ""), env_name)
                                  or row.get("environment") == env_name],
                      "collections": []},
        }
        if room:
            mp["room"] = {"roomId": room.room_id, "gameId": room.game_id,
                          "playerIds": list(room.player_ids)}
            mp["custom"] = dict(room.custom)
            if room.turn_manager:
                mp["turnState"] = room.turn_manager.state.to_dict()
        if client.match_status == "queued":
            mp["match"] = {"status": "queued",
                           "queuePosition": self.matchmaker.queue_position(client.client_id),
                           "gameId": client.game_id, "roomId": client.room_id}
        elif room:
            mp["match"] = {"status": "matched", "roomId": room.room_id, "gameId": room.game_id,
                           "playerIds": list(room.player_ids), "seat": client.seat}
        else:
            mp["match"] = {"status": client.match_status, "roomId": client.room_id,
                           "gameId": client.game_id}
        return mp

    @staticmethod
    def _enqueue_signal(queue: asyncio.Queue, signal_name: str, payload: Dict[str, Any]):
        try:
            queue.put_nowait((signal_name, payload))
        except asyncio.QueueFull:
            try:
                queue.get_nowait()
                queue.put_nowait((signal_name, payload))
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass

    async def _send_to_client(self, client_id: str, signal_name: str, payload: Dict[str, Any]):
        client = self.clients.get(client_id)
        if client and client.sse_queue:
            if signal_name in ("__close__", "__resync__"):
                self._enqueue_signal(client.sse_queue, signal_name, payload)
            else:
                self._enqueue_signal(client.sse_queue, "mp", self._mp_patch_for(signal_name, payload))

    async def attach_stream(self, client_id: str):
        async with self._lock:
            client = self.clients.get(client_id)
            if not client:
                raise HTTPException(401, "Unknown client_id")
            now = _now_ms()
            if self._is_expired(client, now):
                await self._remove_client_locked(client_id, reason="timeout", now_ms=now)
                raise HTTPException(403, "disconnected: timeout")
            self._touch(client, now)
            await self._sweep_expired_locked(now, exclude=client_id)
            if client.sse_queue:
                self._enqueue_signal(client.sse_queue, "__close__", {})
            queue = asyncio.Queue(maxsize=64)
            client.sse_queue = queue
            bootstrap = [("mp", self._mp_for_client(client))]
            return queue, bootstrap

    async def detach_stream(self, client_id: str, queue: asyncio.Queue):
        async with self._lock:
            client = self.clients.get(client_id)
            if client and client.sse_queue is queue:
                client.sse_queue = None

    def _bootstrap_for_client(self, client: ClientSession):
        return [("mp", self._mp_for_client(client))]

    # -- items: binding ---------------------------------------------------
    def _item_env_allows(self, instance_id: str, env_name: str) -> bool:
        """True if this item may live in env_name.

        Bound items must match their bound env. Unbound items bind on
        first sight (first writer wins), unless the id is prefixed with
        a *known other* env (e.g. "other-env:gem-1" in "top-down-demo").
        This is what makes bare ids like "gem-1" work: they bind to the
        sender's env instead of being dropped for missing the prefix.
        """
        bound = self.item_environments.get(str(instance_id))
        if bound:
            return bound == env_name
        if ":" in str(instance_id):
            prefix = str(instance_id).split(":", 1)[0]
            if prefix != env_name and prefix in self.envAuthority:
                return False
        return True

    def _ensure_item_env(self, instance_id: str, env_name: str) -> None:
        self.item_environments.setdefault(str(instance_id), env_name)

    def _item_belongs_to_environment(self, instance_id: str, environment: str) -> bool:
        # Used for bootstrap filtering: unbound items are not yet assigned,
        # so they do not belong to any env until first bound.
        item_environment = self.item_environments.get(str(instance_id))
        if item_environment:
            return item_environment == environment
        return str(instance_id).startswith(environment + ":")

    def _bind_item_environment(self, instance_id: str, environment: str):
        self.item_environments.setdefault(str(instance_id), environment)

    async def _broadcast_signal(self, signal_name: str, payload: Dict[str, Any],
                                environment: Optional[str] = None):
        for client in list(self.clients.values()):
            if environment is not None and client.environment != environment:
                continue
            if client.sse_queue:
                self._enqueue_signal(client.sse_queue, "mp", self._mp_patch_for(signal_name, payload))

    async def _broadcast_to_room(self, room_id: str, signal_name: str, payload: Dict[str, Any]):
        room = self.rooms.get(room_id)
        if not room:
            return
        for client_id in room.player_ids:
            await self._send_to_client(client_id, signal_name, payload)

    async def _broadcast_room_state(self, room) -> None:
        for client_id in list(room.player_ids):
            client = self.clients.get(client_id)
            if not client or not client.sse_queue:
                continue
            mp: Dict[str, Any] = {"timestamp": _now_ms()}
            mp["match"] = {"status": "matched", "roomId": room.room_id, "gameId": room.game_id,
                           "playerIds": list(room.player_ids), "seat": client.seat}
            mp["room"] = {"roomId": room.room_id, "gameId": room.game_id,
                          "playerIds": list(room.player_ids)}
            if room.turn_manager:
                mp["turnState"] = room.turn_manager.state.to_dict()
            self._enqueue_signal(client.sse_queue, "mp", mp)

    # -- authority gating ------------------------------------------------
    def _authority_covers(self, client: ClientSession, scope: str, resource_type: str = "*") -> bool:
        """Per-room authority."""
        config = client.config or GameSessionConfig.default(client.game_id)
        return config.authority.covers(scope, resource_type)

    # -- state handlers ------------------------
    async def handle_character_state(self, client_id: str, updates: List[Dict]) -> Dict[str, Any]:
        """PATCH /character-state. Caller is the route; lock held here.

        Each patch is validated against the stored schema for this
        game_id (client source: server fields dropped, unknown fields
        rejected), merged into the stored full state, validated as a
        full state (defaults filled, required enforced), stored, and
        broadcast as the merged result. The response echoes dropped
        server fields so the client can debug the drop.
        """
        async with self._lock:
            now = _now_ms()
            sess = self.clients.get(client_id)
            if sess is None:
                raise HTTPException(403, "Unknown client")
            if self._is_expired(sess, now):
                await self._remove_client_locked(client_id, reason="timeout", now_ms=now)
                raise HTTPException(403, "disconnected: timeout")
            self._touch(sess, now)
            await self._sweep_expired_locked(now, exclude=client_id)
            schema = self._schema_for(sess)
            if sess.game_id not in self.game_character_schemas:
                self.game_character_schemas[sess.game_id] = schema
            parsed_updates: List[CharacterState] = []
            dropped_all: Dict[str, List[str]] = {}
            for update in updates:
                try:
                    state, dropped = CharacterState.from_payload(update, schema, source="client")
                except (TypeError, ValueError) as ex:
                    raise HTTPException(422, str(ex))
                if state.clientId != client_id:
                    raise HTTPException(403, f"clientId mismatch: {state.clientId} != {client_id}")
                if dropped:
                    dropped_all[state.clientId] = dropped
                # Merge: pending server fields, then stored full state,
                # then this patch. Client patches never contain server
                # fields (dropped above), so they cannot clobber them.
                base: Dict[str, Any] = {}
                base.update(self._character_server_pending.get(client_id, {}))
                existing = self.character_cache.get(client_id)
                if existing is not None:
                    base.update(existing.state)
                base.update(state.state)
                try:
                    merged = schema.validate_full_state(base)
                except ValueError as ex:
                    raise HTTPException(422, str(ex))
                merged_state = CharacterState(clientId=client_id, state=merged,
                                              timestamp=now, source="client")
                self.character_cache[client_id] = merged_state
                self._character_server_pending.pop(client_id, None)
                parsed_updates.append(merged_state)
            if parsed_updates:
                await self._broadcast_signal("character-state-update",
                                            {"updates": [s.to_dict() for s in parsed_updates],
                                             "timestamp": now},
                                            environment=sess.environment)
            return {"ok": True,
                    "accepted": [s.to_dict() for s in parsed_updates],
                    "dropped_server_fields": dropped_all,
                    "server_timestamp": now}

    # -- server-set character path (server code only, never HTTP) -----
    # The synchronizer is still a client. It must go through
    # handle_character_state, which drops server fields. Only server
    # code calls these methods, e.g. gem collection setting score.
    async def _apply_server_character_patch_locked(
        self, client_id: str, patch: Dict[str, Any], now_ms: int,
    ) -> Optional[CharacterState]:
        """Apply a server patch. Caller holds self._lock.

        Validates via validate_server_patch, merges into the stored
        full state, validates the merged full state, stores it, and
        broadcasts with source server. If the client has no stored
        full state yet, the fields are stashed in
        _character_server_pending and broadcast as a partial update;
        they are merged into the full state on the next client patch.
        """
        session = self.clients.get(client_id)
        if session is None:
            return None
        schema = self._schema_for(session)
        accepted = schema.validate_server_patch(patch)
        existing = self.character_cache.get(client_id)
        if existing is not None:
            merged = schema.validate_full_state({**existing.state, **accepted})
            new_state = CharacterState(clientId=client_id, state=merged,
                                       timestamp=now_ms, source="server")
            self.character_cache[client_id] = new_state
            self._character_server_pending.pop(client_id, None)
            await self._broadcast_signal("character-state-update",
                                        {"updates": [new_state.to_dict()],
                                         "timestamp": now_ms},
                                        environment=session.environment)
            return new_state
        pending = dict(self._character_server_pending.get(client_id, {}))
        pending.update(accepted)
        self._character_server_pending[client_id] = pending
        partial = CharacterState(clientId=client_id, state=dict(pending),
                                 timestamp=now_ms, source="server")
        await self._broadcast_signal("character-state-update",
                                    {"updates": [partial.to_dict()],
                                     "timestamp": now_ms},
                                    environment=session.environment)
        return partial

    async def set_server_character_state(
        self, client_id: str, patch: Dict[str, Any],
    ) -> Optional[CharacterState]:
        """Server-code entry point for setting character fields.

        Not exposed over HTTP. Game adapters / turn validators call
        this, e.g. to set a server-authoritative score. Raises
        ValueError on an invalid patch (a server bug, not a client
        error, so it is not turned into a 422 here).
        """
        async with self._lock:
            return await self._apply_server_character_patch_locked(client_id, patch, _now_ms())

    async def _increment_server_score_locked(
        self, client_id: str, now_ms: int, amount: int = 1,
    ) -> Optional[CharacterState]:
        """Increment a server-authoritative score field. Caller holds lock.

        No-op when the game's schema has no score field, or when score
        is client-authoritative. Used by gem collection in
        handle_item_state.
        """
        session = self.clients.get(client_id)
        if session is None:
            return None
        schema = self._schema_for(session)
        spec = schema.fields.get("score")
        if spec is None or spec.authority != "server":
            return None
        existing = self.character_cache.get(client_id)
        if existing is not None:
            current = existing.state.get("score", 0)
        else:
            current = self._character_server_pending.get(client_id, {}).get("score", 0)
        if isinstance(current, bool) or not isinstance(current, (int, float)):
            current = 0
        new_score = int(current) + amount
        if spec.max_value is not None:
            new_score = min(new_score, int(spec.max_value))
        if spec.min_value is not None:
            new_score = max(new_score, int(spec.min_value))
        return await self._apply_server_character_patch_locked(
            client_id, {"score": new_score}, now_ms)

    async def handle_item_state(self, client_id: str, updates: List[Dict], collections: List[Dict]):
        async with self._lock:
            now = _now_ms()
            session = self.clients.get(client_id)
            if not session:
                raise HTTPException(403, "Unknown client")
            if self._is_expired(session, now):
                await self._remove_client_locked(client_id, reason="timeout", now_ms=now)
                raise HTTPException(403, "disconnected: timeout")
            self._touch(session, now)
            await self._sweep_expired_locked(now, exclude=client_id)
            env_name = session.environment
            accepted_updates, accepted_collections = [], []
            for row in updates:
                iid = row.get("instanceId")
                if not iid:
                    continue
                # Binding: first sight wins. Bare ids bind to sender env.
                if not self._item_env_allows(iid, env_name):
                    continue
                self._ensure_item_env(iid, env_name)
                if row.get("environment") not in (None, env_name):
                    continue
                # Authority-gated: rooms without items coverage relay nothing.
                if not self._authority_covers(session, "items", "item"):
                    continue
                # Server-authoritative gems: clients never move / spawn them.
                # A top-down client that still tries to spawn gems (old demo)
                # must be dropped, otherwise it would duplicate the
                # server field. Only collections are accepted for gems.
                _cached_pre = self.item_cache.get(iid)
                if self._is_gem_game(session.game_id, env_name):
                    if (_cached_pre and _cached_pre.get("server_spawned")) or row.get("itemName") in ("gem", ""):
                        continue
                resolved = self.authority.get_resolved_owner(iid, env_name)
                if resolved is None:
                    # No owner yet (e.g. first creator). Synchronizer wins;
                    # otherwise drop. This fixes the old bug where join
                    # never registered authority so EVERY update was dropped.
                    if session.is_synchronizer:
                        try:
                            self.authority.client_arrives(client_id, env_name)
                        except Exception:
                            pass
                        resolved = client_id
                    else:
                        continue
                if resolved != client_id:
                    continue  # silently drop per §7.5
                cached = self.item_cache.get(iid)
                if cached and cached.get("isCollected"):
                    # Collected gems stay collected unless the env
                    # authority explicitly respawns them.
                    if row.get("isCollected") is False:
                        # Respawn allowed only for the authority.
                        pass
                    else:
                        row = dict(row)
                        row["isCollected"] = True
                        row["collectedByClientId"] = cached.get("collectedByClientId", "")
                row = dict(row)
                row["environment"] = env_name
                if self.dirty_filter.is_dirty(iid, row):
                    accepted_updates.append(row)
                self.item_cache[iid] = row
                self._bind_item_environment(iid, env_name)
                all_clients_in_env = [cid for cid, s in self.clients.items() if s.environment == env_name]
                self.freshness.mark_dirty(env_name, iid, client_id, all_clients_in_env)
                if iid in self.authority.itemOwners:
                    self.authority.refresh(iid, client_id)
            for coll in collections:
                iid = coll.get("instanceId")
                if not iid:
                    continue
                if not self._item_env_allows(iid, env_name):
                    continue
                self._ensure_item_env(iid, env_name)
                if coll.get("environment") not in (None, env_name):
                    continue
                if not self._authority_covers(session, "items", "item"):
                    continue
                cached = self.item_cache.get(iid)
                if cached and cached.get("isCollected"):
                    continue
                if coll.get("collectedByClientId") != client_id:
                    continue
                accepted_collection = dict(coll)
                accepted_collection["isCollected"] = True
                accepted_collection["environment"] = env_name
                accepted_collections.append(accepted_collection)
                if iid in self.item_cache:
                    self.item_cache[iid]["isCollected"] = True
                    self.item_cache[iid]["collectedByClientId"] = client_id
                    self.item_cache[iid]["environment"] = env_name
                else:
                    self.item_cache[iid] = {"instanceId": iid, "isCollected": True,
                                            "collectedByClientId": client_id, "environment": env_name}
                self._bind_item_environment(iid, env_name)
                # Keep the dirty cache in sync so a later update cannot resurrect it.
                self.dirty_filter.cache[iid] = dict(self.item_cache[iid])
                # Server keeps the top-down field full: one new gem per
                # collection, included in the SAME broadcast as the collection.
                if self._is_gem_game(session.game_id, env_name):
                    _respawned = self._spawn_top_down_gems_locked(env_name, 1, now)
                    accepted_updates.extend(_respawned)
                    # Server-authoritative score: gem collection sets it.
                    # This goes through the server-set path, never through
                    # a client patch, and broadcasts with source server.
                    # No-op when the schema has no server score field.
                    try:
                        await self._increment_server_score_locked(client_id, now)
                    except Exception:
                        pass
            if accepted_updates or accepted_collections:
                await self._broadcast_signal("item-state-update",
                                            {"updates": accepted_updates, "collections": accepted_collections,
                                             "timestamp": _now_ms()},
                                            environment=env_name)

    async def handle_item_claim(self, client_id: str, claim: Dict) -> Dict:
        async with self._lock:
            now = _now_ms()
            session = self.clients.get(client_id)
            if not session:
                raise HTTPException(403, "Unknown client")
            if self._is_expired(session, now):
                await self._remove_client_locked(client_id, reason="timeout", now_ms=now)
                raise HTTPException(403, "disconnected: timeout")
            self._touch(session, now)
            await self._sweep_expired_locked(now, exclude=client_id)
            iid = claim.get("instanceId")
            if not iid:
                raise HTTPException(400, "Missing instanceId")
            if not self._authority_covers(session, "items", "item"):
                return {"ok": False, "accepted": False, "instanceId": iid, "reason": "authority_not_enabled"}
            if not session.config.authority.allow_client_claims:
                return {"ok": False, "accepted": False, "instanceId": iid, "reason": "client_claims_disabled"}
            if not self._item_env_allows(iid, session.environment):
                return {"ok": False, "accepted": False, "instanceId": iid, "reason": "wrong_environment"}
            self._ensure_item_env(iid, session.environment)
            result = self.authority.claim(iid, client_id)
            if result["accepted"]:
                if not result.get("already_owned"):
                    await self._broadcast_signal("item-authority-changed",
                                                {"instanceId": iid, "previousOwnerId": result.get("previous"),
                                                 "newOwnerId": result.get("new"), "reason": result.get("reason", "claim"),
                                                 "timestamp": _now_ms()},
                                                environment=session.environment)
                return {"ok": True, "accepted": True, "instanceId": iid,
                        "ownerClientId": result.get("new"), "serverTimestamp": _now_ms()}
            return {"ok": True, "accepted": False, "instanceId": iid,
                    "currentOwnerId": result.get("currentOwnerId"), "serverTimestamp": _now_ms()}

    async def handle_item_release(self, client_id: str, release: Dict) -> Dict:
        async with self._lock:
            now = _now_ms()
            session = self.clients.get(client_id)
            if not session:
                raise HTTPException(403, "Unknown client")
            if self._is_expired(session, now):
                await self._remove_client_locked(client_id, reason="timeout", now_ms=now)
                raise HTTPException(403, "disconnected: timeout")
            self._touch(session, now)
            await self._sweep_expired_locked(now, exclude=client_id)
            iid = release.get("instanceId")
            if not iid:
                raise HTTPException(400, "Missing instanceId")
            if not self._item_env_allows(iid, session.environment):
                return {"ok": False, "released": False, "instanceId": iid, "reason": "wrong_environment"}
            result = self.authority.release(iid, client_id)
            if result.get("released"):
                await self._broadcast_signal("item-authority-changed",
                                            {"instanceId": iid, "previousOwnerId": result.get("previous"),
                                             "newOwnerId": None, "reason": "release", "timestamp": _now_ms()},
                                            environment=session.environment)
            return {"ok": True, "released": result.get("released", False),
                    "instanceId": iid, "serverTimestamp": _now_ms()}

    async def handle_custom_state(self, client_id: str, state: Dict[str, Any]) -> Dict[str, Any]:
        async with self._lock:
            now = _now_ms()
            session = self.clients.get(client_id)
            if not session:
                raise HTTPException(403, "Unknown client")
            if self._is_expired(session, now):
                await self._remove_client_locked(client_id, reason="timeout", now_ms=now)
                raise HTTPException(403, "disconnected: timeout")
            self._touch(session, now)
            await self._sweep_expired_locked(now, exclude=client_id)
            if not session.room_id or session.room_id not in self.rooms:
                raise HTTPException(404, "Client is not in a room")
            room = self.rooms[session.room_id]
            if isinstance(state, dict):
                room.custom.update(state)
            await self._broadcast_to_room(room.room_id, "custom-state-update",
                                          {"roomId": room.room_id, "custom": dict(room.custom),
                                           "timestamp": _now_ms()})
            return dict(room.custom)

    # -- turn-based handlers (generic) -----------------------------------
    async def handle_turn_action(self, client_id: str, action: TurnAction) -> TurnActionResponse:
        async with self._lock:
            now = _now_ms()
            session = self.clients.get(client_id)
            if not session:
                raise HTTPException(403, "Unknown client")
            if self._is_expired(session, now):
                await self._remove_client_locked(client_id, reason="timeout", now_ms=now)
                raise HTTPException(403, "disconnected: timeout")
            self._touch(session, now)
            await self._sweep_expired_locked(now, exclude=client_id)
            if not session.room_id or session.room_id not in self.rooms:
                raise HTTPException(404, "Client is not in a room")
            room = self.rooms[session.room_id]
            if not room.turn_manager:
                raise HTTPException(409, "Room is not turn-based")
            if not self._authority_covers(session, "turns"):
                return TurnActionResponse(ok=False, accepted=False, reason="authority_not_enabled",
                                          turn_state=room.turn_manager.state)
            resp = room.turn_manager.apply_action(client_id, action)
            if resp.accepted:
                await self._broadcast_to_room(room.room_id, "turn-state-update",
                                             {"turnState": room.turn_manager.state.to_dict(),
                                              "timestamp": _now_ms()})
                if room.turn_manager.state.phase == "finished":
                    room.status = "finished"
                    await self._broadcast_to_room(room.room_id, "game-over",
                                                 {"roomId": room.room_id,
                                                  "winnerId": room.turn_manager.state.winner_id,
                                                  "isDraw": room.turn_manager.state.is_draw,
                                                  "endReason": room.turn_manager.state.end_reason,
                                                  "turnState": room.turn_manager.state.to_dict(),
                                                  "timestamp": _now_ms()})
            return resp

    async def get_turn_state(self, client_id: str) -> TurnState:
        # GET counts as a packet for liveness (it carries X-Client-ID).
        async with self._lock:
            now = _now_ms()
            session = self.clients.get(client_id)
            if not session:
                raise HTTPException(403, "Unknown client")
            if self._is_expired(session, now):
                await self._remove_client_locked(client_id, reason="timeout", now_ms=now)
                raise HTTPException(403, "disconnected: timeout")
            self._touch(session, now)
            await self._sweep_expired_locked(now, exclude=client_id)
            if not session.room_id or session.room_id not in self.rooms:
                raise HTTPException(404, "Client is not in a room")
            room = self.rooms[session.room_id]
            if not room.turn_manager:
                raise HTTPException(409, "Room is not turn-based")
            return room.turn_manager.state

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.mp = MultiplayerRegistry()
    await app.state.mp.start()
    yield
    await app.state.mp.stop()


app = FastAPI(
    title="SCS Multiplayer",
    version="0.2.0",
    description="Match-making, rooms, turn-based state.",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.get_allowed_origins_list() or ["*"],
    allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)
app.include_router(health_router)
app.include_router(chat_proxy_router)


def _client_id_from(request: Request, body: Optional[BaseModel] = None) -> Optional[str]:
    return (request.headers.get("X-Client-ID")
            or (getattr(body, "client_id", None) if body else None)
            or request.query_params.get("client_id"))


def _parse_skill(raw: Any, *, player_id: str, game_id: str) -> Optional[SkillRating]:
    if raw is None:
        return None
    try:
        return SkillRating.from_client(raw, player_id=player_id, game_id=game_id)
    except ValueError:
        return None


def _join_request_from_body(body: "JoinBody") -> JoinRequest:
    raw = body.model_dump(exclude_none=False)
    config = None
    if raw.get("config") is not None:
        config = GameSessionConfig.from_payload(raw["config"])
    elif any(raw.get(k) is not None for k in (
            "requires_matchmaking", "requires_turn_based", "max_players_per_room",
            "min_players_per_room", "matchmaking", "turn", "authority",
            "character_schema", "characterSchema")):
        # Flat form: config fields sent alongside environment_name.
        # character_schema may arrive as snake_case or camelCase.
        # Only pass keys the client actually set: from_payload() treats
        # a present None (e.g. max_players_per_room=None) as a value
        # and int(None) crashes, so None keys must be omitted.
        flat_schema = raw.get("character_schema") or raw.get("characterSchema")
        flat: Dict[str, Any] = {
            "game_id": raw.get("game_id") or raw.get("environment_name") or "default",
        }
        for _k in ("requires_matchmaking", "requires_turn_based",
                   "max_players_per_room", "min_players_per_room",
                   "matchmaking", "turn", "authority"):
            if raw.get(_k) is not None:
                flat[_k] = raw.get(_k)
        if flat_schema is not None:
            flat["character_schema"] = flat_schema
        config = GameSessionConfig.from_payload(flat)
    # Flat disconnectionTime alongside the body (without nested config).
    if config is None and (raw.get("disconnectionTime") is not None
                           or raw.get("character_schema") is not None
                           or raw.get("characterSchema") is not None):
        flat2: Dict[str, Any] = {
            "game_id": raw.get("game_id") or raw.get("environment_name") or "default",
        }
        if raw.get("disconnectionTime") is not None:
            flat2["disconnectionTime"] = raw.get("disconnectionTime")
        flat_schema2 = raw.get("character_schema") or raw.get("characterSchema")
        if flat_schema2 is not None:
            flat2["character_schema"] = flat_schema2
        config = GameSessionConfig.from_payload(flat2)
    game_id = raw.get("game_id") or raw.get("environment_name") or "default"
    return JoinRequest(
        environment_name=raw.get("environment_name") or game_id,
        character_name=raw.get("character_name") or "player",
        game_id=game_id,
        player_id=raw.get("player_id") or "",
        display_name=raw.get("display_name") or raw.get("character_name") or "player",
        skill=_parse_skill(raw.get("skill"), player_id=raw.get("player_id") or "", game_id=game_id),
        config=config, room_id=raw.get("room_id"), room_code=raw.get("room_code"),
        metadata=dict(raw.get("metadata") or {}))


# ---------- Routes ----------

bgs_router = APIRouter(prefix="/api/multiplayer")


class JoinBody(BaseModel):
    environment_name: str = "default"
    character_name: str = "player"
    # All optional.
    game_id: Optional[str] = None
    player_id: Optional[str] = None
    display_name: Optional[str] = None
    skill: Optional[Any] = None  # number or {rating, deviation, confidence}
    config: Optional[Dict[str, Any]] = None
    # Flat aliases for config (so clients need not nest it).
    requires_matchmaking: Optional[bool] = None
    requires_turn_based: Optional[bool] = None
    max_players_per_room: Optional[int] = None
    min_players_per_room: Optional[int] = None
    matchmaking: Optional[Dict[str, Any]] = None
    turn: Optional[Dict[str, Any]] = None
    authority: Optional[Any] = None  # list of scopes or {scopes, resource_types, ...}
    # Flat character schema, snake_case or camelCase.
    character_schema: Optional[Dict[str, Any]] = None
    characterSchema: Optional[Dict[str, Any]] = None
    room_id: Optional[str] = None
    room_code: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    disconnectionTime: Optional[float] = None
    disconnection_time_sec: Optional[float] = None


class EnvironmentSwitchBody(BaseModel):
    environment_name: str


@bgs_router.post("/join")
async def join(request: Request, body: JoinBody):
    registry: MultiplayerRegistry = request.app.state.mp
    try:
        req = _join_request_from_body(body)
    except ValueError as ex:
        raise HTTPException(422, str(ex))
    res = await registry.join(req)
    return {"client_id": res.client_id, "session_id": res.session_id,
            "is_synchronizer": res.is_synchronizer, "existing_clients": res.existing_clients,
            "room_id": res.room_id, "game_id": res.game_id, "match_status": res.match_status,
            "queue_position": res.queue_position, "seat": res.seat,
            "player_index": res.player_index, "turn_order": res.turn_order,
            "skill_source": res.skill_source,
            "character_schema": res.character_schema.to_dict() if res.character_schema else None,
            "disconnection_time_sec": res.disconnection_time_sec}


@bgs_router.post("/leave")
async def leave(request: Request, body: Optional[Dict[str, Any]] = None):
    cid = _client_id_from(request)
    if not cid:
        raise HTTPException(400, "Missing client_id")
    registry: MultiplayerRegistry = request.app.state.mp
    await registry.leave(cid)
    return {"ok": True}


@bgs_router.post("/heartbeat")
async def heartbeat(request: Request):
    """Keep-alive packet. Updates last_seen, sweeps expired clients.

    Send this when idle instead of spamming character-state. No timers
    on the server: this handler is what triggers the sweep.
    """
    cid = _client_id_from(request)
    if not cid:
        raise HTTPException(400, "Missing X-Client-ID")
    registry: MultiplayerRegistry = request.app.state.mp
    return await registry.handle_heartbeat(cid)


@bgs_router.post("/matchmaking/cancel")
async def cancel_matchmaking(request: Request):
    cid = _client_id_from(request)
    if not cid:
        raise HTTPException(400, "Missing X-Client-ID")
    registry: MultiplayerRegistry = request.app.state.mp
    cancelled = await registry.cancel_matchmaking(cid)
    return {"ok": True, "cancelled": cancelled}


@bgs_router.get("/matchmaking/status")
async def matchmaking_status(request: Request):
    cid = _client_id_from(request)
    if not cid:
        raise HTTPException(400, "Missing X-Client-ID")
    registry: MultiplayerRegistry = request.app.state.mp
    session = registry.clients.get(cid)
    if not session:
        raise HTTPException(403, "Unknown client")
    return {"client_id": cid, "match_status": session.match_status,
            "queue_position": registry.matchmaker.queue_position(cid),
            "room_id": session.room_id, "game_id": session.game_id}


@bgs_router.post("/switch-environment")
async def switch_environment(request: Request, body: EnvironmentSwitchBody):
    cid = _client_id_from(request)
    if not cid:
        raise HTTPException(400, "Missing X-Client-ID")
    registry: MultiplayerRegistry = request.app.state.mp
    environment = await registry.switch_environment(cid, body.environment_name)
    return {"ok": True, "environment": environment}


@bgs_router.get("/stream")
async def stream(request: Request):
    sid = request.headers.get("X-Session-ID") or request.query_params.get("session_id")
    if not sid:
        raise HTTPException(400, "Missing session_id")
    registry: MultiplayerRegistry = request.app.state.mp
    client = await registry.get_client_by_session(sid)
    if not client:
        raise HTTPException(401, "Unknown session_id")
    same_game = [c for c in registry.clients.values() if c.game_id == client.game_id and c.sse_queue]
    if len(same_game) >= 64:
        raise HTTPException(503, "Too many clients")
    queue, bootstrap = await registry.attach_stream(client.client_id)

    async def event_generator():
        try:
            for signal_name, payload in bootstrap:
                yield SSE.patch_signals({signal_name: payload})
            while True:
                signal_name, payload = await queue.get()
                if signal_name == "__close__":
                    break
                yield SSE.patch_signals({signal_name: payload})
        except asyncio.CancelledError:
            pass
        finally:
            await registry.detach_stream(client.client_id, queue)

    return DatastarResponse(event_generator())


class CharacterStateBody(BaseModel):
    updates: List[Dict[str, Any]]
    timestamp: int


@bgs_router.patch("/character-state")
async def character_state(request: Request, body: CharacterStateBody):
    cid = _client_id_from(request)
    if not cid:
        raise HTTPException(400, "Missing X-Client-ID")
    registry: MultiplayerRegistry = request.app.state.mp
    return await registry.handle_character_state(cid, body.updates)


class ItemStateBody(BaseModel):
    updates: List[Dict[str, Any]] = []
    collections: List[Dict[str, Any]] = []
    timestamp: int


@bgs_router.patch("/item-state")
async def item_state(request: Request, body: ItemStateBody):
    cid = _client_id_from(request)
    if not cid:
        raise HTTPException(400, "Missing X-Client-ID")
    registry: MultiplayerRegistry = request.app.state.mp
    await registry.handle_item_state(cid, body.updates, body.collections)
    return {"ok": True}


class ClaimBody(BaseModel):
    instanceId: str
    clientPosition: Optional[Dict[str, float]] = None
    reason: Optional[str] = None
    timestamp: int


@bgs_router.patch("/item-authority-claim")
async def item_claim(request: Request, body: ClaimBody):
    cid = _client_id_from(request)
    if not cid:
        raise HTTPException(400, "Missing X-Client-ID")
    registry: MultiplayerRegistry = request.app.state.mp
    return await registry.handle_item_claim(cid, body.model_dump())


class ReleaseBody(BaseModel):
    instanceId: str
    reason: Optional[str] = None
    timestamp: int


@bgs_router.patch("/item-authority-release")
async def item_release(request: Request, body: ReleaseBody):
    cid = _client_id_from(request)
    if not cid:
        raise HTTPException(400, "Missing X-Client-ID")
    registry: MultiplayerRegistry = request.app.state.mp
    return await registry.handle_item_release(cid, body.model_dump())


class CustomStateBody(BaseModel):
    state: Dict[str, Any] = {}
    timestamp: Optional[int] = None


@bgs_router.patch("/custom-state")
async def custom_state(request: Request, body: CustomStateBody):
    cid = _client_id_from(request)
    if not cid:
        raise HTTPException(400, "Missing X-Client-ID")
    registry: MultiplayerRegistry = request.app.state.mp
    return {"ok": True, "custom": await registry.handle_custom_state(cid, body.state)}


# ---------- Generic turn-based routes ----------

class TurnActionBody(BaseModel):
    action_type: str = "move"  # move | pass | resign
    payload: Dict[str, Any] = {}
    # Client-authoritative games only; ignored when a validator is registered.
    proposed_state: Optional[Dict[str, Any]] = None
    claimed_winner_id: Optional[str] = None
    claimed_is_draw: bool = False
    claimed_end_reason: Optional[str] = None
    timestamp: Optional[int] = None


@bgs_router.post("/turn/action")
async def turn_action(request: Request, body: TurnActionBody):
    cid = _client_id_from(request)
    if not cid:
        raise HTTPException(400, "Missing X-Client-ID")
    registry: MultiplayerRegistry = request.app.state.mp
    resp = await registry.handle_turn_action(
        cid, TurnAction(player_id=cid, action_type=body.action_type,
                        payload=body.payload, proposed_state=body.proposed_state,
                        claimed_winner_id=body.claimed_winner_id,
                        claimed_is_draw=body.claimed_is_draw,
                        claimed_end_reason=body.claimed_end_reason,
                        timestamp=body.timestamp or _now_ms()))
    return {"ok": resp.ok, "accepted": resp.accepted, "reason": resp.reason,
            "turn_state": resp.turn_state.to_dict() if resp.turn_state else None,
            "server_timestamp": resp.server_timestamp}


@bgs_router.get("/turn/state")
async def turn_state(request: Request):
    cid = _client_id_from(request)
    if not cid:
        raise HTTPException(400, "Missing X-Client-ID")
    registry: MultiplayerRegistry = request.app.state.mp
    st = await registry.get_turn_state(cid)
    return {"turn_state": st.to_dict()}


@bgs_router.post("/turn/pass")
async def turn_pass(request: Request):
    cid = _client_id_from(request)
    if not cid:
        raise HTTPException(400, "Missing X-Client-ID")
    registry: MultiplayerRegistry = request.app.state.mp
    resp = await registry.handle_turn_action(
        cid, TurnAction(player_id=cid, action_type="pass", timestamp=_now_ms()))
    return {"ok": resp.ok, "accepted": resp.accepted, "reason": resp.reason,
            "turn_state": resp.turn_state.to_dict() if resp.turn_state else None}


@bgs_router.post("/turn/resign")
async def turn_resign(request: Request):
    cid = _client_id_from(request)
    if not cid:
        raise HTTPException(400, "Missing X-Client-ID")
    registry: MultiplayerRegistry = request.app.state.mp
    resp = await registry.handle_turn_action(
        cid, TurnAction(player_id=cid, action_type="resign", timestamp=_now_ms()))
    return {"ok": resp.ok, "accepted": resp.accepted, "reason": resp.reason,
            "turn_state": resp.turn_state.to_dict() if resp.turn_state else None}


# Effects, lights, sky (base synchronizer only) - simplified
class EffectsBody(BaseModel):
    particle_effects: Optional[List[Dict]] = None
    environment_particles: Optional[List[Dict]] = None
    timestamp: int


@bgs_router.patch("/effects-state")
async def effects_state(request: Request, body: EffectsBody):
    cid = _client_id_from(request)
    if not cid:
        raise HTTPException(400, "Missing X-Client-ID")
    registry: MultiplayerRegistry = request.app.state.mp
    _sess = registry.clients.get(cid)
    if not _sess or registry.synchronizers.get(_sess.environment) != cid:
        raise HTTPException(403, "Not synchronizer")
    session = registry.clients.get(cid)
    if not session:
        raise HTTPException(403, "Unknown client")
    # Effects is a packet: touch + sweep like the other handlers.
    async with registry._lock:
        now = _now_ms()
        if registry._is_expired(session, now):
            await registry._remove_client_locked(cid, reason="timeout", now_ms=now)
            raise HTTPException(403, "disconnected: timeout")
        registry._touch(session, now)
        await registry._sweep_expired_locked(now, exclude=cid)
    await registry._broadcast_signal("effects-state-update", body.model_dump(),
                                     environment=session.environment)
    return {"ok": True}


app.include_router(bgs_router)


# ---------- Demo client ----------


def _serve_demo_file(name: str):
    """Shared helper for /tic-tac-toe and /demo/{name}."""
    from fastapi.responses import HTMLResponse
    safe = "".join(c for c in name if c.isalnum() or c in ("-", "_"))
    if not safe or safe != name:
        raise HTTPException(400, "Invalid demo name")
    demo_file = _path.Path(__file__).parent / "demo" / f"{safe}.html"
    if not demo_file.exists():
        raise HTTPException(404, f"No demo client named '{safe}'")
    return HTMLResponse(demo_file.read_text(encoding="utf-8"))


@app.get("/tic-tac-toe", include_in_schema=False)
async def tic_tac_toe_demo_page():
    """Demo client entry point."""
    return _serve_demo_file("tic-tac-toe")


@app.get("/top-down", include_in_schema=False)
async def top_down_demo_page():
    """Top-down demo — same file as /demo/top-down.

    This is the URL you deployed: https://matchgod.onrender.com/top-down
    Previously only /demo/top-down existed, so /top-down returned 404.
    """
    return _serve_demo_file("top-down")


@app.get("/demo/{name}", include_in_schema=False)
async def demo_page(name: str):
    """Generic demo loader."""
    return _serve_demo_file(name)


@app.get("/api/v1/capabilities")
async def capabilities():
    return Capabilities().__dict__


@app.get("/extension/manifest.json")
async def manifest():
    mp = _path.Path(__file__).parent.parent / "extension" / "manifest.json"
    if mp.exists():
        return JSONResponse(json.loads(mp.read_text()))
    return JSONResponse({"id": "scs.multiplayer", "version": "0.2.0"})


@app.get("/")
async def root():
    return {
        "service": "SCS Multiplayer",
        "version": "0.2.0",
        "spec": "https://github.com/EricEisaman/babylon-game-starter/blob/main/MULTIPLAYER_SYNCH.md",
        "transport": "HTTPS + SSE (Datastar)",
        "endpoints": {
            "join": "POST /api/multiplayer/join {environment_name, character_name, game_id?, player_id?, skill?, config?}",
            "join_flat": "POST /api/multiplayer/join {environment_name, requires_matchmaking, requires_turn_based, max_players_per_room, authority, skill}",
            "leave": "POST /api/multiplayer/leave X-Client-ID",
            "heartbeat": "POST /api/multiplayer/heartbeat X-Client-ID (keep-alive, triggers disconnect sweep)",
            "matchmaking_status": "GET /api/multiplayer/matchmaking/status X-Client-ID",
            "matchmaking_cancel": "POST /api/multiplayer/matchmaking/cancel X-Client-ID",
            "stream": "GET /api/multiplayer/stream?session_id=... (signal: mp)",
            "character-state": "PATCH /api/multiplayer/character-state X-Client-ID {updates, timestamp}",
            "item-state": "PATCH /api/multiplayer/item-state X-Client-ID {updates, collections, timestamp}",
            "item-claim": "PATCH /api/multiplayer/item-authority-claim X-Client-ID {instanceId, reason, timestamp}",
            "item-release": "PATCH /api/multiplayer/item-authority-release X-Client-ID {instanceId, reason, timestamp}",
            "turn_action": "POST /api/multiplayer/turn/action X-Client-ID {action_type: move|pass|resign, payload}",
            "turn_state": "GET /api/multiplayer/turn/state X-Client-ID",
            "tic_tac_toe_demo": "GET /tic-tac-toe -> demo client page (HTML, NOT an API)",
            "demo_generic": "GET /demo/{name} -> static demo client for any game",
            "top_down_demo": "GET /top-down -> top-down realtime demo (gems, squares, health bars)",
            "server_authoritative": "First join spawns server game data; clients send character-state + collections only",
            "empty_game_cleanup": "Game data deleted lazily on next packet after 5 min empty (no timer loop)",
            "top_down_demo_alias": "GET /demo/top-down -> same file",
        },
        "signals": ["mp"],
        "python_api": "extensions.multiplayer.MultiplayerClient",
    }


# ---------------------------------------------------------------------------
# ADDED ONLY: Babylon demo file serving. Nothing above changed.
# Protocol untouched: no join / stream / character / item / turn / match
# logic touched, no _is_top_down_game change, no gem logic change.
# The old _serve_demo_file() only serves *.html. Babylon needs its binary
# hulk.glb too, and babylon.html loads "hulk.glb" relatively. So this
# block adds a second helper for binary files + new routes only.
# ---------------------------------------------------------------------------
from fastapi.responses import FileResponse as _BabylonFileResponse

_BABYLON_DEMO_MIME = {
    ".html": "text/html; charset=utf-8",
    ".glb": "model/gltf-binary",
    ".gltf": "model/gltf+json",
    ".bin": "application/octet-stream",
    ".js": "text/javascript; charset=utf-8",
    ".json": "application/json",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".css": "text/css; charset=utf-8",
}


def _serve_demo_binary_file(filename: str):
    """Serve one file from demo/ with the right MIME. No traversal."""
    safe = str(filename or "").strip()
    if not safe or "/" in safe or "\\" in safe or ".." in safe:
        raise HTTPException(400, "Invalid demo file")
    if any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-" for c in safe):
        raise HTTPException(400, "Invalid demo file")
    suffix = _path.Path(safe).suffix.lower()
    if suffix not in _BABYLON_DEMO_MIME:
        raise HTTPException(404, "Demo file not found")
    demo_file = _path.Path(__file__).parent / "demo" / safe
    if not demo_file.exists() or not demo_file.is_file():
        raise HTTPException(404, "Demo file not found")
    headers = {"Cache-Control": "no-cache"} if suffix == ".html" else {"Cache-Control": "public, max-age=3600", "Accept-Ranges": "bytes"}
    return _BabylonFileResponse(str(demo_file), media_type=_BABYLON_DEMO_MIME[suffix], headers=headers)


@app.get("/babylon", include_in_schema=False)
async def babylon_demo_page():
    """Babylon demo — same file as /demo/babylon."""
    return _serve_demo_binary_file("babylon.html")


@app.get("/babylon.html", include_in_schema=False)
async def babylon_demo_page_html():
    return _serve_demo_binary_file("babylon.html")


@app.get("/hulk.glb", include_in_schema=False)
async def hulk_model():
    """hulk.glb for /babylon (relative 'hulk.glb' -> /hulk.glb)."""
    return _serve_demo_binary_file("hulk.glb")


@app.get("/demo/hulk.glb", include_in_schema=False)
async def demo_hulk_model():
    """hulk.glb for /demo/babylon (relative 'hulk.glb' -> /demo/hulk.glb)."""
    return _serve_demo_binary_file("hulk.glb")


@app.get("/demo/babylon.html", include_in_schema=False)
async def demo_babylon_page_html():
    return _serve_demo_binary_file("babylon.html")


# /demo/{name} is registered before this block, so Starlette would match
# /demo/hulk.glb and /demo/babylon.html against it first. Move only these
# two new demo routes ahead of it. No existing route is edited or removed.
_demo_new = [r for r in app.routes if getattr(r, "path", "") in ("/demo/hulk.glb", "/demo/babylon.html")]
_demo_rest = [r for r in app.routes if getattr(r, "path", "") not in ("/demo/hulk.glb", "/demo/babylon.html")]
# FIX: app.routes is a read-only property (no setter) — assigning it crashes on import.
# Mutate the underlying list in place instead. Same order, no route edited/removed.
app.router.routes[:] = _demo_new + _demo_rest
