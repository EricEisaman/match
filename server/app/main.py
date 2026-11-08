"""
SCS Datastar Backend with MP-SYNC Python Implementation — v2.

Normative mp spec: https://github.com/EricEisaman/babylon-game-starter/blob/main/MULTIPLAYER_SYNCH.md
Normative match-making spec: this file + multiplayer/protocol.py (v2).

Short name: SCS-MATCH
Transport: HTTPS + SSE (Datastar)
Encoding: JSON UTF-8

Implements (legacy, unchanged behaviour):
- Session lifecycle: join, leave, stream (SSE)
- State messages: character-state, item-state, item-authority-claim/release
- Authority: itemOwners + envArrivalOrder + envAuthority (per §4.7, §4.8)
- Signals: character-state-update, item-state-update, client-joined, client-left,
  synchronizer-changed, item-authority-changed, env-item-authority-changed
- Dirty filter + freshness matrix (simplified for platformer)

Implements (v2, new):
- Configurable sessions: ``GameSessionConfig`` on join — requires_matchmaking,
  requires_turn_based, max/minPlayersPerRoom, authority scopes, matchmaking
  + turn tuning, opaque custom_rules. See protocol.py.
- Match-making: skill-aware queue (``Matchmaker``). Skill is resolved through
  a ``SkillProvider`` — client-reported today, DB tomorrow (one-line swap in
  ``get_skill_provider``). Queue widens its skill tolerance with wait time.
- Rooms: match-made players land in a room; direct (non-matchmade) joins land
  in a room keyed by environment_name, exactly as before. Broadcasts are
  room-scoped for match-made / turn-based rooms, environment-scoped for legacy.
- Turn-based state: generic ``TurnManager`` (whose turn, deadlines, pass /
  resign, terminal detection). Game rules live in ``TurnValidator``s.
  generic turn machinery + ````. The reference example
  for adding any future turn-based game.

Deliberately NOT in this change (see reply for discussion):
persistence / DB skill provider, reconnect-resume, spectators, bots,
ranked ladders, anti-cheat, parties, ELO updates after a match.
"""

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
    AuthorityConfig, Capabilities, CharacterState,
    ClientReportedSkillProvider, DatabaseSkillProvider, GameSessionConfig,
    JoinRequest, JoinResponse,
    MatchmakingConfig, SkillProvider, SkillRating,
    TurnAction, TurnActionResponse, TurnConfig, TurnState,
)
from .multiplayer.authority import AuthorityManager
from .multiplayer.dirty import DirtyFilter, FreshnessMatrix

from datastar_py.fastapi import DatastarResponse
from datastar_py import ServerSentEventGenerator as SSE


def _now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Skill provider wiring — THE one place to swap client -> DB authority.
# ---------------------------------------------------------------------------

def get_skill_provider() -> SkillProvider:
    """Today: trust what the client sends on join.

    Tomorrow: ``return DatabaseSkillProvider(db)`` — nothing else changes:
    ``Matchmaker`` and the routes only ever see a ``SkillRating``.
    """
    return ClientReportedSkillProvider()
    # return DatabaseSkillProvider(db)  # <- future swap


# ---------------------------------------------------------------------------
# Turn-based state management (generic — no game rules in here)
# ---------------------------------------------------------------------------

class TurnManager:
    """Enforces turn order, deadlines, pass/resign, terminal state.

    Game rules are delegated to a ``TurnValidator``. The manager owns only:
    whose turn it is, when the turn expires, and whether the game is over.
    """

    def __init__(self, room_id: str, player_ids: List[str],
                 config: GameSessionConfig,
                 validator: Optional[Any] = None):
        self.room_id = room_id
        self.config = config
        self.validator = validator
        order = list(player_ids)
        if config.turn.turn_order_strategy == "random":
            random.shuffle(order)
        # "skill_order" is applied by the caller (it has the ratings);
        # anything else keeps join order.
        # No validator == client-authoritative game: initial state is
        # empty until the first mover sends proposed_state. The server
        # never invents game state.
        initial = validator.initial_state(order, config) if validator else {}
        self.state = TurnState(
            room_id=room_id, game_id=config.game_id, phase="in_progress" if order else "waiting",
            current_player_id=order[0] if order else None, turn_number=1,
            turn_order=order,
            turn_deadline_ms=(_now_ms() + config.turn.turn_timeout_ms)
            if config.turn.turn_timeout_ms else None,
            state=initial,
        )

    # -- actions ------------------------------------------------------
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
        # --- Branch A: server-validated game (validator registered) ---
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
        # --- Branch B: client-authoritative (default, 1000s of games) ---
        # Server enforces turn/phase only. It does NOT interpret payload.
        # The mover's proposed_state becomes canonical; claimed terminal
        # results are accepted at face value and broadcast. Clients that
        # disagree can detect divergence from the action stream (all
        # moves are broadcast, so the full history is auditable).
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

    def check_timeout(self, now_ms: int) -> Optional[TurnActionResponse]:
        """Called from the tick loop. Returns a response if a deadline fired."""
        st = self.state
        if st.phase != "in_progress" or not st.turn_deadline_ms or now_ms < st.turn_deadline_ms:
            return None
        current = st.current_player_id or ""
        if self.config.turn.timeout_action == "forfeit":
            self._finish(winner_id=self._other_player(current), end_reason="timeout")
        elif self.config.turn.timeout_action == "pass":
            self._advance(reason="timeout")
        else:  # "none" — just extend the deadline, never auto-act
            st.turn_deadline_ms = now_ms + (self.config.turn.turn_timeout_ms or 30_000)
            return None
        return TurnActionResponse(ok=True, accepted=True, reason="timeout", turn_state=st)

    def on_player_left(self, client_id: str) -> Optional[TurnActionResponse]:
        """A leaver forfeits the game (if it was in progress)."""
        if self.state.phase != "in_progress":
            return None
        self._finish(winner_id=self._other_player(client_id), end_reason="opponent_left")
        return TurnActionResponse(ok=True, accepted=True, reason="opponent_left",
                                  turn_state=self.state)

    # -- internals ----------------------------------------------------
    def _advance(self, reason: str = "move") -> None:
        st = self.state
        if not st.turn_order:
            return
        idx = st.turn_order.index(st.current_player_id) if st.current_player_id in st.turn_order else -1
        st.current_player_id = st.turn_order[(idx + 1) % len(st.turn_order)]
        st.turn_number += 1
        st.turn_deadline_ms = (_now_ms() + self.config.turn.turn_timeout_ms) \
            if self.config.turn.turn_timeout_ms else None
        st.timestamp = _now_ms()

    def _finish(self, winner_id: Optional[str], is_draw: bool = False,
                end_reason: Optional[str] = None) -> None:
        st = self.state
        st.phase = "finished"
        st.winner_id = winner_id
        st.is_draw = is_draw
        st.end_reason = end_reason
        st.current_player_id = None
        st.turn_deadline_ms = None
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
        config = req.config or GameSessionConfig.from_legacy(req.game_id)
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
        """Greedy best-fit: oldest waiter first, pair/group with the
        closest-skill waiters inside the (wait-widened) tolerance.
        Returns full groups ready to become rooms."""
        waiters = self.queues.get(fingerprint, [])
        if not waiters:
            return []
        config = waiters[0].config
        need = config.max_players_per_room  # rooms fill to max; min is a floor for starting
        # A room may start early once min is reached AND everyone in it is
        # within tolerance — the caller decides "start now vs keep waiting".
        # Here: only form when we can fill to max, or min is met and the
        # oldest waiter has waited past half of max_wait.
        now = _now_ms()
        formed: List[List[QueuedPlayer]] = []
        # Work on a copy, oldest first.
        pool = sorted(waiters, key=lambda q: q.enqueued_at_ms)
        while len(pool) >= config.min_players_per_room:
            oldest = pool[0]
            tolerance = config.matchmaking.tolerance_for_wait_ms(now - oldest.enqueued_at_ms)
            candidates = [q for q in pool[1:]
                          if abs(q.skill.rating - oldest.skill.rating) <= tolerance]
            candidates.sort(key=lambda q: abs(q.skill.rating - oldest.skill.rating))
            group = [oldest] + candidates[: need - 1]
            can_fill = len(group) >= need
            waited_long = (now - oldest.enqueued_at_ms) >= config.matchmaking.max_wait_ms // 2
            if not (can_fill or (len(group) >= config.min_players_per_room and waited_long)):
                break  # oldest cannot match yet — nobody newer will beat them
            for q in group:
                pool.remove(q)
            formed.append(group)
        # Persist the remainder.
        if pool:
            self.queues[fingerprint] = pool
        else:
            self.queues.pop(fingerprint, None)
        return formed

    def timed_out(self, fingerprint: str, now_ms: int) -> List[QueuedPlayer]:
        """Waiters past max_wait — caller signals timeout and drops them."""
        waiters = self.queues.get(fingerprint, [])
        if not waiters:
            return []
        config = waiters[0].config
        expired = [q for q in waiters if now_ms - q.enqueued_at_ms >= config.matchmaking.max_wait_ms]
        if expired:
            self.queues[fingerprint] = [q for q in waiters if q not in expired]
            if not self.queues[fingerprint]:
                self.queues.pop(fingerprint, None)
        return expired


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
    # v2
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


@dataclass
class Room:
    room_id: str
    game_id: str
    config: GameSessionConfig
    player_ids: List[str] = field(default_factory=list)
    status: str = "waiting"  # waiting | in_progress | finished
    turn_manager: Optional[TurnManager] = None
    created_at_ms: int = field(default_factory=_now_ms)

    @property
    def is_full(self) -> bool:
        return len(self.player_ids) >= self.config.max_players_per_room


class MultiplayerRegistry:
    # Validators by game_id — register a new turn-based game here, one line.
    VALIDATORS: Dict[str, Any] = {}

    def __init__(self, skill_provider: Optional[SkillProvider] = None):
        self.clients: Dict[str, ClientSession] = {}
        self.sessions: Dict[str, str] = {}
        self.authority = AuthorityManager()
        self.dirty_filter = DirtyFilter()
        self.freshness = FreshnessMatrix()
        self.base_synchronizer: Optional[str] = None
        self.character_cache: Dict[str, CharacterState] = {}
        self.item_cache: Dict[str, Dict[str, Any]] = {}
        self.item_environments: Dict[str, str] = {}
        # v2
        self.skill_provider = skill_provider or get_skill_provider()
        self.matchmaker = Matchmaker(self.skill_provider)
        self.rooms: Dict[str, Room] = {}
        self._lock = asyncio.Lock()
        self._tick_task: Optional[asyncio.Task] = None
        self.running = True

    async def start(self):
        self._tick_task = asyncio.create_task(self._tick_loop())
        print("[SCS-MP-SYNC] Registry started")

    async def stop(self):
        self.running = False
        if self._tick_task:
            self._tick_task.cancel()
            try:
                await self._tick_task
            except asyncio.CancelledError:
                pass
        print("[SCS-MP-SYNC] Registry stopped")

    async def _tick_loop(self):
        while self.running:
            await asyncio.sleep(1 / 20)  # 20Hz tick
            async with self._lock:
                # Turn deadlines.
                for room in list(self.rooms.values()):
                    if room.turn_manager:
                        fired = room.turn_manager.check_timeout(_now_ms())
                        if fired:
                            await self._broadcast_to_room(
                                room.room_id, "turn-state-update",
                                {"turnState": room.turn_manager.state.to_dict(),
                                 "timestamp": _now_ms()})
                            if room.turn_manager.state.phase == "finished":
                                room.status = "finished"
                                await self._broadcast_to_room(
                                    room.room_id, "game-over",
                                    {"roomId": room.room_id, "winnerId": room.turn_manager.state.winner_id,
                                     "isDraw": room.turn_manager.state.is_draw,
                                     "endReason": room.turn_manager.state.end_reason,
                                     "turnState": room.turn_manager.state.to_dict(),
                                     "timestamp": _now_ms()})
                # Match-making timeouts.
                for fingerprint in list(self.matchmaker.queues.keys()):
                    for qp in self.matchmaker.timed_out(fingerprint, _now_ms()):
                        session = self.clients.get(qp.client_id)
                        if session:
                            session.match_status = "timeout"
                            await self._send_to_client(
                                qp.client_id, "matchmaking-timeout",
                                {"eventType": "matchmaking_timeout", "clientId": qp.client_id,
                                 "gameId": qp.game_id, "reason": "no_match_within_max_wait",
                                 "timestamp": _now_ms()})

    # -- join ----------------------------------------------------------
    async def join(self, req: JoinRequest) -> JoinResponse:
        req.normalize()
        config = req.config or GameSessionConfig.from_legacy(req.game_id)
        async with self._lock:
            client_id = uuid.uuid4().hex[:8]
            session_id = uuid.uuid4().hex[:16]
            # Skill: resolved through the provider (client today, DB tomorrow).
            skill = await self.skill_provider.resolve_skill(
                player_id=req.player_id or client_id, game_id=req.game_id,
                client_reported=req.skill)
            is_sync = self.base_synchronizer is None
            session = ClientSession(
                client_id=client_id, session_id=session_id,
                environment=req.environment_name, character_name=req.character_name,
                is_synchronizer=is_sync, game_id=req.game_id,
                player_id=req.player_id or client_id, config=config, skill=skill)
            self.clients[client_id] = session
            self.sessions[session_id] = client_id
            if is_sync:
                self.base_synchronizer = client_id

            if config.requires_matchmaking:
                session.match_status = "queued"
                await self.matchmaker.enqueue(client_id, session_id, req, skill)
                pos = self.matchmaker.queue_position(client_id) or 1
                await self._send_to_client(client_id, "matchmaking-queued",
                                           {"eventType": "matchmaking_queued", "clientId": client_id,
                                            "gameId": req.game_id, "queuePosition": pos,
                                            "timestamp": _now_ms()})
                await self._try_fill_queues(config.fingerprint())
                return JoinResponse(client_id=client_id, session_id=session_id,
                                    is_synchronizer=is_sync, existing_clients=0,
                                    game_id=req.game_id, match_status="queued",
                                    queue_position=pos, skill_source=skill.source, config=config)
            # Direct join — legacy path, now room-backed.
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
                                skill_source=skill.source, config=config)

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
                await self._send_to_client(
                    qp.client_id, "match-found",
                    {"eventType": "match_found", "roomId": room.room_id, "gameId": room.game_id,
                     "playerIds": [q.client_id for q in group], "seat": seat,
                     "turnOrder": [q.client_id for q in group], "timestamp": _now_ms()})
            await self._on_room_changed(room)

    def _get_or_create_direct_room(self, req: JoinRequest, config: GameSessionConfig) -> Room:
        """Legacy: one room per environment. Explicit room_id / room_code
        lets friend / private rooms share a key instead."""
        key = req.room_id or req.room_code or f"legacy:{req.environment_name or config.game_id}"
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
        # Room is playable. For turn-based rooms, start the turn manager
        # once the room is full (or min is met and no more are expected —
        # direct rooms start at min; match-made rooms always arrive full).
        if room.config.requires_turn_based and room.turn_manager is None:
            validator = self.VALIDATORS.get(room.game_id)
            order = list(room.player_ids)
            if room.config.turn.turn_order_strategy == "skill_order":
                order.sort(key=lambda cid: (self.clients[cid].skill.rating
                                            if self.clients.get(cid) and self.clients[cid].skill else 1000.0))
            room.turn_manager = TurnManager(room.room_id, order, room.config, validator)
            room.status = "in_progress"
            await self._broadcast_to_room(room.room_id, "turn-state-update",
                                         {"turnState": room.turn_manager.state.to_dict(),
                                          "timestamp": _now_ms()})
            await self._broadcast_to_room(room.room_id, "room-ready",
                                         {"roomId": room.room_id, "gameId": room.game_id,
                                          "playerIds": order,
                                          "turnOrder": room.turn_manager.state.turn_order,
                                          "timestamp": _now_ms()})
        else:
            room.status = "in_progress"
            await self._broadcast_to_room(room.room_id, "room-ready",
                                         {"roomId": room.room_id, "gameId": room.game_id,
                                          "playerIds": list(room.player_ids),
                                          "timestamp": _now_ms()})

    # -- match-making helpers used by routes ---------------------------
    async def cancel_matchmaking(self, client_id: str) -> bool:
        async with self._lock:
            session = self.clients.get(client_id)
            if session:
                session.match_status = "cancelled"
            return self.matchmaker.cancel(client_id)

    # -- leave / switch -------------------------------------------------
    async def leave(self, client_id: str):
        async with self._lock:
            session = self.clients.pop(client_id, None)
            if not session:
                return
            self.sessions.pop(session.session_id, None)
            self.matchmaker.cancel(client_id)
            if session.sse_queue:
                self._enqueue_signal(session.sse_queue, "__close__", {})
            session.sse_queue = None
            if self.base_synchronizer == client_id:
                if self.clients:
                    new_sync = next(iter(self.clients))
                    self.base_synchronizer = new_sync
                    await self._broadcast_signal("synchronizer-changed",
                                                 {"newSynchronizerId": new_sync, "reason": "disconnection",
                                                  "timestamp": _now_ms()})
                else:
                    self.base_synchronizer = None
            events = self.authority.client_leaves(client_id)
            for ev in events:
                if ev["type"] == "env-authority-changed":
                    await self._broadcast_signal("env-item-authority-changed", ev,
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
                                                      "timestamp": _now_ms()})
                if not room.player_ids:
                    self.rooms.pop(room.room_id, None)
            await self._broadcast_signal("client-left",
                                        {"eventType": "left", "clientId": client_id,
                                         "totalClients": sum(s.environment == session.environment
                                                             for s in self.clients.values()),
                                         "timestamp": _now_ms()},
                                        environment=session.environment)

    async def get_client_by_session(self, session_id: str) -> Optional[ClientSession]:
        client_id = self.sessions.get(session_id)
        if not client_id:
            return None
        return self.clients.get(client_id)

    async def switch_environment(self, client_id: str, environment: str):
        # Legacy behaviour preserved verbatim (see git history); rooms are
        # not moved by this call — use leave + join with a new config.
        async with self._lock:
            client = self.clients.get(client_id)
            if not client:
                raise HTTPException(403, "Unknown client")
            if not environment or environment == client.environment:
                return client.environment
            previous_environment = client.environment
            previous_clients = sum(s.environment == previous_environment
                                   for s in self.clients.values()) - 1
            authority_events = self.authority.client_leaves(client_id)
            self.character_cache.pop(client_id, None)
            self.freshness.client_leaves(previous_environment, client_id)
            client.environment = environment
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

    # -- SSE plumbing ---------------------------------------------------
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
            self._enqueue_signal(client.sse_queue, signal_name, payload)

    async def attach_stream(self, client_id: str):
        """Atomically attach a live queue and capture the matching snapshot."""
        async with self._lock:
            client = self.clients.get(client_id)
            if not client:
                raise HTTPException(401, "Unknown client_id")
            if client.sse_queue:
                self._enqueue_signal(client.sse_queue, "__close__", {})
            queue = asyncio.Queue(maxsize=64)
            client.sse_queue = queue
            bootstrap = self._bootstrap_for_client(client)
            return queue, bootstrap

    async def detach_stream(self, client_id: str, queue: asyncio.Queue):
        async with self._lock:
            client = self.clients.get(client_id)
            if client and client.sse_queue is queue:
                client.sse_queue = None

    def _bootstrap_for_client(self, client: ClientSession):
        env_name = client.environment
        now = _now_ms()
        bootstrap = [("connectionState",
                      {"connectionState": "connected", "clientId": client.client_id,
                       "sessionId": client.session_id, "isSynchronizer": client.is_synchronizer,
                       "environment": env_name, "roomId": client.room_id, "gameId": client.game_id,
                       "matchStatus": client.match_status,
                       "serverTick": 0, "datastarConnected": True})]
        for existing in self.clients.values():
            if existing.environment == env_name and existing.client_id != client.client_id:
                bootstrap.append(("client-joined",
                                  {"eventType": "joined", "clientId": existing.client_id,
                                   "environment": env_name, "character": existing.character_name,
                                   "totalClients": sum(s.environment == env_name for s in self.clients.values()),
                                   "timestamp": now}))
        env_authority = self.authority.envAuthority.get(env_name)
        if env_authority:
            bootstrap.append(("envAuthority",
                              {"environmentName": env_name, "previousAuthorityId": None,
                               "newAuthorityId": env_authority, "reason": "arrival", "timestamp": now}))
        for instance_id, owner in self.authority.itemOwners.items():
            if self._item_belongs_to_environment(instance_id, env_name):
                bootstrap.append(("itemAuthority",
                                  {"instanceId": instance_id, "previousOwnerId": None,
                                   "newOwnerId": owner["ownerClientId"], "reason": "claim", "timestamp": now}))
        characters = [state.to_dict() for owner_id, state in self.character_cache.items()
                      if owner_id in self.clients and self.clients[owner_id].environment == env_name]
        if characters:
            bootstrap.append(("character-state-update", {"updates": characters, "timestamp": now}))
        items = [row for instance_id, row in self.item_cache.items()
                 if self._item_belongs_to_environment(instance_id, env_name)]
        bootstrap.append(("item-state-update", {"updates": items, "collections": [], "timestamp": now}))
        # v2: turn state for the client's room, if it has one.
        room = self.rooms.get(client.room_id) if client.room_id else None
        if room and room.turn_manager:
            bootstrap.append(("turn-state-update",
                              {"turnState": room.turn_manager.state.to_dict(), "timestamp": now}))
        return bootstrap

    def _item_belongs_to_environment(self, instance_id: str, environment: str) -> bool:
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
                self._enqueue_signal(client.sse_queue, signal_name, payload)

    async def _broadcast_to_room(self, room_id: str, signal_name: str, payload: Dict[str, Any]):
        room = self.rooms.get(room_id)
        if not room:
            return
        for client_id in room.player_ids:
            await self._send_to_client(client_id, signal_name, payload)

    # -- authority gating ------------------------------------------------
    def _authority_covers(self, client: ClientSession, scope: str, resource_type: str = "*") -> bool:
        """Per-room authority: a room whose config excludes a scope gets
        relay-only behaviour for it — never adjudication."""
        config = client.config or GameSessionConfig.from_legacy(client.game_id)
        return config.authority.covers(scope, resource_type)

    # -- state handlers (legacy, authority-gated) ------------------------
    async def handle_character_state(self, client_id: str, updates: List[Dict]):
        async with self._lock:
            if client_id not in self.clients:
                raise HTTPException(403, "Unknown client")
            session = self.clients[client_id]
            parsed_updates = []
            for update in updates:
                try:
                    state = CharacterState.from_payload(update)
                except (TypeError, ValueError) as ex:
                    raise HTTPException(422, str(ex))
                if state.clientId != client_id:
                    raise HTTPException(403, f"clientId mismatch: {state.clientId} != {client_id}")
                parsed_updates.append(state)
            for state in parsed_updates:
                self.character_cache[client_id] = state
            await self._broadcast_signal("character-state-update",
                                        {"updates": [s.to_dict() for s in parsed_updates],
                                         "timestamp": _now_ms()},
                                        environment=session.environment)

    async def handle_item_state(self, client_id: str, updates: List[Dict], collections: List[Dict]):
        async with self._lock:
            session = self.clients.get(client_id)
            if not session:
                raise HTTPException(403, "Unknown client")
            env_name = session.environment
            accepted_updates, accepted_collections = [], []
            for row in updates:
                iid = row.get("instanceId")
                if not iid:
                    continue
                if iid not in self.item_environments and row.get("environment") == env_name:
                    self._bind_item_environment(iid, env_name)
                if not self._item_belongs_to_environment(iid, env_name):
                    continue
                if row.get("environment") not in (None, env_name):
                    continue
                # Authority-gated: rooms that don't cover "items" relay nothing.
                if not self._authority_covers(session, "items", "item"):
                    continue
                resolved = self.authority.get_resolved_owner(iid, env_name)
                if resolved != client_id:
                    continue  # silently drop per §7.5
                cached = self.item_cache.get(iid)
                if cached and cached.get("isCollected"):
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
                if iid not in self.item_environments and coll.get("environment") == env_name:
                    self._bind_item_environment(iid, env_name)
                if not self._item_belongs_to_environment(iid, env_name):
                    continue
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
            if accepted_updates or accepted_collections:
                await self._broadcast_signal("item-state-update",
                                            {"updates": accepted_updates, "collections": accepted_collections,
                                             "timestamp": _now_ms()},
                                            environment=env_name)

    async def handle_item_claim(self, client_id: str, claim: Dict) -> Dict:
        async with self._lock:
            session = self.clients.get(client_id)
            if not session:
                raise HTTPException(403, "Unknown client")
            iid = claim.get("instanceId")
            if not iid:
                raise HTTPException(400, "Missing instanceId")
            if not self._authority_covers(session, "items", "item"):
                return {"ok": False, "accepted": False, "instanceId": iid, "reason": "authority_not_enabled"}
            if not session.config.authority.allow_client_claims:
                return {"ok": False, "accepted": False, "instanceId": iid, "reason": "client_claims_disabled"}
            if not self._item_belongs_to_environment(iid, session.environment):
                return {"ok": False, "accepted": False, "instanceId": iid, "reason": "wrong_environment"}
            result = self.authority.claim(iid, client_id, idle_timeout_ms=10000)
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
            session = self.clients.get(client_id)
            if not session:
                raise HTTPException(403, "Unknown client")
            iid = release.get("instanceId")
            if not iid:
                raise HTTPException(400, "Missing instanceId")
            if not self._item_belongs_to_environment(iid, session.environment):
                return {"ok": False, "released": False, "instanceId": iid, "reason": "wrong_environment"}
            result = self.authority.release(iid, client_id)
            if result.get("released"):
                await self._broadcast_signal("item-authority-changed",
                                            {"instanceId": iid, "previousOwnerId": result.get("previous"),
                                             "newOwnerId": None, "reason": "release", "timestamp": _now_ms()},
                                            environment=session.environment)
            return {"ok": True, "released": result.get("released", False),
                    "instanceId": iid, "serverTimestamp": _now_ms()}

    # -- turn-based handlers (generic) -----------------------------------
    async def handle_turn_action(self, client_id: str, action: TurnAction) -> TurnActionResponse:
        async with self._lock:
            session = self.clients.get(client_id)
            if not session:
                raise HTTPException(403, "Unknown client")
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
        session = self.clients.get(client_id)
        if not session or not session.room_id or session.room_id not in self.rooms:
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
    title="SCS Multiplayer - BGS-MP-SYNC Python",
    version="0.2.0",
    description="Python implementation of Babylon Game Starter Multiplayer Sync - SCS-MP-SYNC, generalized: match-making, rooms, turn-based state.",
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
        return None  # bad skill payload must never fail a join


def _join_request_from_body(body: "JoinBody") -> JoinRequest:
    raw = body.model_dump(exclude_none=False)
    config = None
    if raw.get("config") is not None:
        config = GameSessionConfig.from_payload(raw["config"])
    elif any(raw.get(k) is not None for k in (
            "requires_matchmaking", "requires_turn_based", "max_players_per_room",
            "min_players_per_room", "matchmaking", "turn", "authority")):
        # Flat form: config fields sent alongside environment_name.
        config = GameSessionConfig.from_payload({
            "game_id": raw.get("game_id") or raw.get("environment_name") or "default",
            "requires_matchmaking": raw.get("requires_matchmaking", False),
            "requires_turn_based": raw.get("requires_turn_based", False),
            "max_players_per_room": raw.get("max_players_per_room"),
            "min_players_per_room": raw.get("min_players_per_room"),
            "matchmaking": raw.get("matchmaking") or {},
            "turn": raw.get("turn") or {},
            "authority": raw.get("authority") or {},
        })
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


# ---------- BGS / generalized routes ----------

bgs_router = APIRouter(prefix="/api/multiplayer")


class JoinBody(BaseModel):
    environment_name: str = "default"
    character_name: str = "player"
    # v2 — all optional, legacy clients send none of these.
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
    room_id: Optional[str] = None
    room_code: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


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
            "skill_source": res.skill_source}


@bgs_router.post("/leave")
async def leave(request: Request, body: Optional[Dict[str, Any]] = None):
    cid = _client_id_from(request)
    if not cid:
        raise HTTPException(400, "Missing client_id")
    registry: MultiplayerRegistry = request.app.state.mp
    await registry.leave(cid)
    return {"ok": True}


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
    if sum(bool(s.sse_queue) for s in registry.clients.values()) >= 64:
        raise HTTPException(503, "Too many clients")
    queue, bootstrap = await registry.attach_stream(client.client_id)

    async def event_generator():
        try:
            for signal_name, payload in bootstrap:
                yield SSE.patch_signals({signal_name: payload})
                if signal_name == "character-state-update":
                    yield SSE.patch_signals({"gameSnapshot": payload})
            while True:
                signal_name, payload = await queue.get()
                if signal_name == "__close__":
                    break
                yield SSE.patch_signals({signal_name: payload})
                if signal_name == "character-state-update":
                    yield SSE.patch_signals({"gameSnapshot": payload})
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
    await registry.handle_character_state(cid, body.updates)
    return {"ok": True}


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


# ---------- Generic turn-based routes ----------

class TurnActionBody(BaseModel):
    action_type: str = "move"  # move | pass | resign
    payload: Dict[str, Any] = {}  # opaque, game-specific — server never reads it
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
    if registry.base_synchronizer != cid:
        raise HTTPException(403, "Not base synchronizer")
    session = registry.clients.get(cid)
    if not session:
        raise HTTPException(403, "Unknown client")
    await registry._broadcast_signal("effects-state-update", body.model_dump(),
                                     environment=session.environment)
    return {"ok": True}


app.include_router(bgs_router)


# ---------- Demo client: GET /tic-tac-toe is a PAGE, not an API ----------
# The API above is fully generic — it contains no game-specific routes,
# types, or logic. The ONLY tic-tac-toe code in the repo is the static
# demo client in ``demo/tic-tac-toe.html``, which plays tic-tac-toe by
# calling only the generic endpoints (join with a turn-based config,
# then turn/action + turn/state). Adding a 1001st game means adding a
# 1001st demo/client file — never touching the API.
#
# This route serves that demo file. It holds no game logic: no board,
# no win detection, no move validation. All of that lives in the
# browser, in demo/tic-tac-toe.html.


def _serve_demo_file(name: str):
    """Shared helper for /tic-tac-toe and /demo/{name}. Generic: the
    server never knows what game a demo file implements."""
    from fastapi.responses import HTMLResponse
    safe = "".join(c for c in name if c.isalnum() or c in ("-", "_"))
    if not safe or safe != name:
        raise HTTPException(400, "Invalid demo name")
    # Demo files live next to main.py: server/app/demo/<name>.html
    demo_file = _path.Path(__file__).parent / "demo" / f"{safe}.html"
    if not demo_file.exists():
        raise HTTPException(404, f"No demo client named '{safe}'")
    return HTMLResponse(demo_file.read_text(encoding="utf-8"))


@app.get("/tic-tac-toe", include_in_schema=False)
async def tic_tac_toe_demo_page():
    """Demo client entry point. Serves static HTML — not an API.

    The page itself is the only tic-tac-toe-specific thing in the
    system. Everything it calls is generic: POST /api/multiplayer/join,
    POST /api/multiplayer/turn/action, GET /api/multiplayer/turn/state,
    GET /api/multiplayer/matchmaking/status, POST /api/multiplayer/leave.
    """
    return _serve_demo_file("tic-tac-toe")


@app.get("/demo/{name}", include_in_schema=False)
async def demo_page(name: str):
    """Generic demo loader: /demo/<name> serves demo/<name>.html.

    /demo/chess would serve demo/chess.html with zero API changes.
    Path is sanitised to block traversal.
    """
    return _serve_demo_file(name)


@app.get("/api/v1/capabilities")
async def capabilities():
    return Capabilities().__dict__


@app.get("/extension/manifest.json")
async def manifest():
    mp = _path.Path(__file__).parent.parent / "extension" / "manifest.json"
    if mp.exists():
        return JSONResponse(json.loads(mp.read_text()))
    return JSONResponse({"id": "scs.bgs.platformer", "version": "0.2.0"})


@app.get("/")
async def root():
    return {
        "service": "SCS Multiplayer - SCS-MP-SYNC (BGS Python)",
        "version": "0.2.0",
        "spec": "https://github.com/EricEisaman/babylon-game-starter/blob/main/MULTIPLAYER_SYNCH.md",
        "transport": "HTTPS + SSE (Datastar)",
        "endpoints": {
            "join": "POST /api/multiplayer/join {environment_name, character_name, game_id?, player_id?, skill?, config?}",
            "join_flat": "POST /api/multiplayer/join {environment_name, requires_matchmaking, requires_turn_based, max_players_per_room, authority, skill}",
            "leave": "POST /api/multiplayer/leave X-Client-ID",
            "matchmaking_status": "GET /api/multiplayer/matchmaking/status X-Client-ID",
            "matchmaking_cancel": "POST /api/multiplayer/matchmaking/cancel X-Client-ID",
            "stream": "GET /api/multiplayer/stream?session_id=... (signals: matchmaking-queued, match-found, turn-state-update, game-over, ...)",
            "character-state": "PATCH /api/multiplayer/character-state X-Client-ID {updates, timestamp}",
            "item-state": "PATCH /api/multiplayer/item-state X-Client-ID {updates, collections, timestamp}",
            "item-claim": "PATCH /api/multiplayer/item-authority-claim X-Client-ID {instanceId, reason, timestamp}",
            "item-release": "PATCH /api/multiplayer/item-authority-release X-Client-ID {instanceId, reason, timestamp}",
            "turn_action": "POST /api/multiplayer/turn/action X-Client-ID {action_type: move|pass|resign, payload}",
            "turn_state": "GET /api/multiplayer/turn/state X-Client-ID",
            "tic_tac_toe_demo": "GET /tic-tac-toe -> demo client page (HTML, NOT an API)",
            "demo_generic": "GET /demo/{name} -> static demo client for any game",
        },
        "signals": ["character-state-update", "item-state-update", "client-joined", "client-left",
                    "synchronizer-changed", "item-authority-changed", "env-item-authority-changed",
                    "matchmaking-queued", "match-found", "matchmaking-timeout",
                    "turn-state-update", "room-ready", "game-over"],
        "python_api": "extensions.multiplayer.MultiplayerClient",
    }
