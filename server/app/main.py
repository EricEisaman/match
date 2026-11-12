# server/app/main.py
# ---------------------------------------------------------------------------
# MATCH Multiplayer API — full server for the top-down + Babylon demos.
#
# What this file is:
#   FastAPI service that owns ALL game state for every environment
#   (top-down-demo, babylon-demo, and any caller-supplied game_id /
#   environment_name). Clients never trust each other; they trust this
#   file. Every mutation goes through the in-memory MultiplayerRegistry
#   below, and every mutation is broadcast to every subscriber in the
#   same environment as a Datastar SSE signal patch.
#
# Wire protocol (the thing babylon.html / top-down.html actually speak):
#   POST  /api/multiplayer/join            -> {client_id, session_id, ...}
#   POST  /api/multiplayer/leave           -> {ok: true}
#   PATCH /api/multiplayer/character-state -> {ok: true} (+ broadcast)
#   PATCH /api/multiplayer/item-state      -> {ok: true} (+ broadcast)
#   POST  /api/multiplayer/heartbeat       -> {ok: true} (+ sweep)
#   GET   /api/multiplayer/stream?session_id=...  (SSE, Datastar patches)
#   POST  /api/multiplayer/match-action    -> {ok: true} (+ broadcast)
#   POST  /api/multiplayer/environment     -> {ok: true} (+ broadcast)
#   GET   /api/multiplayer/character-state, /item-state, /state, /games ...
#   POST  /api/multiplayer/turn           -> turn-based games
#   POST  /api/multiplayer/reset           -> reset one environment
#
# SSE signal patches — THIS is how game state is handled:
#   The server NEVER expects clients to poll. On every join / leave /
#   character-state / item-state / heartbeat / match-action / turn /
#   timeout-sweep, the registry calls _broadcast_locked(env_key, mp_patch)
#   and every subscribed SSE stream in that environment receives:
#       event: datastar-patch-signals
#       data: signals {"mp": { ...patch... }}
#   The client (babylon.html) does:
#       stream.addEventListener("datastar-patch-signals", ev =>
#           handleStreamData(ev.data))
#       handleStreamData() strips the "signals " prefix, JSON-parses it,
#       and applyMp() applies mp.connection / mp.characters / mp.items /
#       mp.clientJoined / mp.clientLeft / mp.synchronizer.
#   On subscribe the server FIRST sends a full snapshot patch:
#       {"mp": {"connection": {...},
#               "characters": [ ...every character... ],
#               "items": {"updates": [ ...every uncollected gem... ],
#                         "collections": []},
#               "synchronizer": "...", "turn_state": ..., "environment": ...}}
#   so a late joiner sees the whole world immediately, then only deltas.
#   A patch may carry any subset of those keys; clients merge, never replace.
#
# Remotes / cloning (client side, documented here because the server
# makes it possible):
#   The server sends one character record per clientId. babylon.html
#   turns EACH record into its own hulk.glb clone via
#   cloneRemoteHulk(clientId): a fresh SceneLoader.ImportMesh of the
#   same hulk.glb file, which gives that remote its own skeleton +
#   AnimationGroups (idle / jump / run). The server does NOT send meshes;
#   it sends the character records that the client clones from.
#
# Gems:
#   _is_gem_game() is true for top-down-demo and babylon-demo. On first
#   join / first ensure, the server spawns 24 gems in 2000-space.
#   On every collection it respawns exactly 1 gem, at a random 2000-space
#   position. Clients never spawn gems locally — that was the bug where
#   only the spawner saw gems.
#
# Disconnect:
#   The server stores last_seen per client and disconnectionTime from
#   the join config (default 60s). On EVERY inbound packet from ANY
#   client it sweeps expired clients and broadcasts
#   mp.clientLeft = {clientId, reason: "timeout"} for each one. There
#   is no timer loop. A late packet from an expired client is rejected
#   with 403 disconnected: timeout instead of reviving it.
#   If nobody sends a packet, an idle client stays listed until the
#   next packet arrives. Empty game data is deleted 5 min after the
#   last reset, on the next packet.
#
# Demo files:
#   GET /top-down, /babylon, /hulk.glb, /demo/{name}
#   babylon.html lives in server/app/demo/babylon.html, hulk.glb lives
#   next to it in server/app/demo/hulk.glb. babylon.html loads hulk.glb
#   relatively ("hulk.glb"), so /demo/babylon -> /demo/hulk.glb and
#   /babylon -> /hulk.glb. Both are served by _serve_demo_file().
# ---------------------------------------------------------------------------
from __future__ import annotations

import asyncio
import json
import math
import queue
import random
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Request, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field, field_validator

from server.app.config import settings
from server.app.services.multiplayer import (
    CharacterState,
    MultiplayerConfig,
    MultiplayerConfigOut,
    MultiplayerException,
    MultiplayerService,
    PlayerInfo,
)

security = HTTPBearer(auto_error=False)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_DISCONNECTION_TIME_SEC = 60.0
EMPTY_GAME_TTL_SEC = 300.0
MAX_PLAYERS_PER_ROOM_DEFAULT = 8
MIN_PLAYERS_PER_ROOM_DEFAULT = 1
GEM_COUNT = 24
GEM_WORLD_MIN = 80
GEM_WORLD_MAX = 1920
GEM_GRID_COLS = 6
GEM_GRID_X0 = 80
GEM_GRID_X_STEP = 320
GEM_GRID_Y0 = 120
GEM_GRID_Y_STEP = 380
GEM_JITTER_X = 120
GEM_JITTER_Y = 120
SSE_KEEPALIVE_SEC = 15
SSE_SUBSCRIBER_QUEUE_MAX = 200
SESSION_ID_LEN = 36
CLIENT_ID_PREFIX = "client-"
GEM_GAME_IDS = ("top-down-demo", "babylon-demo")
GEM_GAME_ENVS = ("top-down-demo", "babylon-demo")
DEMO_DIR_NAME = "demo"
ALLOWED_DEMO_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")
MAX_BODY_UPDATES = 500
MAX_SCORE = 1000000
MAX_HEALTH = 100
MIN_HEALTH = 0

# ---------------------------------------------------------------------------
# Small helpers — time, ids, clamping, parsing
# ---------------------------------------------------------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _new_id(prefix: str = "") -> str:
    return f"{prefix}{uuid4()}"


def _new_session_id() -> str:
    return str(uuid4())


def _new_client_id(player_id: str) -> str:
    safe = "".join(c for c in str(player_id or "player") if c.isalnum() or c in ("-", "_")) or "player"
    return f"{CLIENT_ID_PREFIX}{safe}-{uuid4().hex[:8]}"


def _clamp(value: Any, lo: float, hi: float, default: float = 0.0) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(v) or math.isinf(v):
        return default
    return max(lo, min(hi, v))


def _clamp_int(value: Any, lo: int, hi: int, default: int = 0) -> int:
    try:
        v = int(value)
    except (TypeError, ValueError):
        return default
    return max(lo, min(hi, v))


def _as_float_list(value: Any, length: int = 2) -> Optional[list[float]]:
    if not isinstance(value, (list, tuple)):
        return None
    if len(value) < length:
        return None
    out: list[float] = []
    for item in value[:length]:
        try:
            out.append(float(item))
        except (TypeError, ValueError):
            return None
    return out


def _as_pos(value: Any) -> Optional[list[float]]:
    # Accept [x, y] in 2000-space. Reject NaN / Inf / absurd values.
    pos = _as_float_list(value, 2)
    if pos is None:
        return None
    for v in pos:
        if math.isnan(v) or math.isinf(v):
            return None
        if abs(v) > 100000:
            return None
    return pos


def _clean_str(value: Any, default: str = "", max_len: int = 200) -> str:
    if value is None:
        return default
    s = str(value)
    if len(s) > max_len:
        s = s[:max_len]
    return s


def _clean_name(value: Any, default: str = "player") -> str:
    s = _clean_str(value, default, 80).strip()
    return s or default


def _jsonable(value: Any) -> Any:
    # Round-trip through json so datetimes / sets / tuples become JSON-safe.
    try:
        return json.loads(json.dumps(value, default=str))
    except (TypeError, ValueError):
        return str(value)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value))


def _merge_dict(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    out = dict(base or {})
    for k, v in (patch or {}).items():
        if v is not None:
            out[k] = v
    return out


def _get_config_value(config: Any, *keys: str, default: Any = None) -> Any:
    if not isinstance(config, dict):
        return default
    for key in keys:
        if key in config and config[key] is not None:
            return config[key]
    return default


def _disconnection_time_from_config(config: Any) -> float:
    raw = _get_config_value(config, "disconnection_time_sec", "disconnectionTime", default=None)
    if raw is None:
        return DEFAULT_DISCONNECTION_TIME_SEC
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return DEFAULT_DISCONNECTION_TIME_SEC
    if not math.isfinite(v):
        return DEFAULT_DISCONNECTION_TIME_SEC
    return max(0.0, v)


def _empty_ttl_from_config(config: Any) -> float:
    raw = _get_config_value(config, "empty_game_ttl_sec", "emptyGameTtlSec", default=None)
    if raw is None:
        return EMPTY_GAME_TTL_SEC
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return EMPTY_GAME_TTL_SEC
    return max(0.0, v) if math.isfinite(v) else EMPTY_GAME_TTL_SEC


def _max_players_from_config(config: Any) -> int:
    raw = _get_config_value(config, "max_players_per_room", "maxPlayersPerRoom", default=None)
    if raw is None:
        return MAX_PLAYERS_PER_ROOM_DEFAULT
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return MAX_PLAYERS_PER_ROOM_DEFAULT


def _min_players_from_config(config: Any) -> int:
    raw = _get_config_value(config, "min_players_per_room", "minPlayersPerRoom", default=None)
    if raw is None:
        return MIN_PLAYERS_PER_ROOM_DEFAULT
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return MIN_PLAYERS_PER_ROOM_DEFAULT


# ---------------------------------------------------------------------------
# Pydantic request / response models
# Every model allows extra fields (extra="allow") so older / newer demo
# clients that send one more field do not get a 422. Handlers read only
# the fields they understand and ignore the rest.
# ---------------------------------------------------------------------------
class JoinRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    environment_name: str = Field(default="default")
    character_name: str = Field(default="player")
    game_id: str = Field(default="default")
    player_id: str = Field(default="player-1")
    display_name: Optional[str] = None
    skill: dict[str, Any] = Field(default_factory=dict)
    config: Optional[dict[str, Any]] = None
    metadata: Optional[dict[str, Any]] = None

    @field_validator("environment_name", "game_id", "character_name", "player_id", mode="before")
    @classmethod
    def _coerce_str(cls, v: Any) -> str:
        if v is None:
            return ""
        return str(v)


class JoinResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    client_id: str
    session_id: str
    room_id: Optional[str] = None
    is_synchronizer: bool = False
    disconnection_time_sec: float = DEFAULT_DISCONNECTION_TIME_SEC
    environment_name: Optional[str] = None
    game_id: Optional[str] = None


class LeaveRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    client_id: Optional[str] = None
    reason: Optional[str] = None


class CharacterStateUpdate(BaseModel):
    model_config = ConfigDict(extra="allow")
    clientId: str
    characterModelId: Optional[str] = None
    position: Optional[list[float]] = None
    pos: Optional[list[float]] = None
    velocity: Optional[list[float]] = None
    animationState: Optional[str] = None
    animationFrame: Optional[int] = None
    isJumping: Optional[bool] = None
    facing: Optional[float] = None
    rotation: Optional[float] = None
    score: Optional[int] = None
    health: Optional[int] = None
    onGround: Optional[bool] = None
    timestamp: Optional[int] = None
    metadata: Optional[dict[str, Any]] = None


class CharacterStateRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    updates: list[CharacterStateUpdate] = Field(default_factory=list)
    timestamp: Optional[int] = None
    game_id: Optional[str] = None
    environment_name: Optional[str] = None


class ItemUpdate(BaseModel):
    model_config = ConfigDict(extra="allow")
    instanceId: str
    itemId: Optional[str] = None
    pos: Optional[list[float]] = None
    position: Optional[list[float]] = None
    isCollected: Optional[bool] = None
    collectedBy: Optional[str] = None
    collectedByClientId: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None


class ItemCollection(BaseModel):
    model_config = ConfigDict(extra="allow")
    instanceId: str
    collectedByClientId: Optional[str] = None
    environment: Optional[str] = None
    timestamp: Optional[int] = None


class ItemStateRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    updates: list[ItemUpdate] = Field(default_factory=list)
    collections: list[ItemCollection] = Field(default_factory=list)
    timestamp: Optional[int] = None
    game_id: Optional[str] = None
    environment_name: Optional[str] = None


class ItemStateResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    ok: bool = True
    item_state: list[dict[str, Any]] = Field(default_factory=list)


class MatchActionRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    type: str
    data: Optional[dict[str, Any]] = None
    client_id: Optional[str] = None
    game_id: Optional[str] = None
    environment_name: Optional[str] = None


class TurnActionRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    type: str = Field(default="move")
    data: Optional[dict[str, Any]] = None
    client_id: Optional[str] = None
    game_id: Optional[str] = None
    environment_name: Optional[str] = None
    turn_index: Optional[int] = None


class EnvironmentRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    environment: Optional[dict[str, Any]] = None
    game_id: Optional[str] = None
    environment_name: Optional[str] = None


class HeartbeatRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    client_id: Optional[str] = None


class ResetRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    game_id: Optional[str] = None
    environment_name: Optional[str] = None
    reason: Optional[str] = None


class ConfigUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    game_id: Optional[str] = None
    environment_name: Optional[str] = None
    config: dict[str, Any] = Field(default_factory=dict)


class CharacterStateResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    ok: bool = True
    characters: list[dict[str, Any]] = Field(default_factory=list)


class GameStateResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    ok: bool = True
    game_id: Optional[str] = None
    environment_name: Optional[str] = None
    room_id: Optional[str] = None
    characters: list[dict[str, Any]] = Field(default_factory=list)
    item_state: list[dict[str, Any]] = Field(default_factory=list)
    turn_state: Optional[dict[str, Any]] = None
    synchronizer: Optional[str] = None
    environment: Optional[dict[str, Any]] = None
    turn_count: int = 0
    created_at: Optional[str] = None
    last_update_at: Optional[str] = None


class ListGamesResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    ok: bool = True
    games: list[dict[str, Any]] = Field(default_factory=list)


class ErrorResponse(BaseModel):
    model_config = ConfigDict(extra="allow")
    ok: bool = False
    detail: str = ""
    status_code: int = 400


# ---------------------------------------------------------------------------
# Wire serializers — one place that decides what a character / item /
# game looks like on the SSE patch and on the REST responses.
# Clients (babylon.html applyMp) read exactly these field names:
#   character: clientId, characterModelId, position, pos, velocity,
#              animationState, animationFrame, facing, rotation,
#              score, health, isJumping, onGround, metadata, last_seen
#   item: instanceId, itemId, pos, position, isCollected, collectedBy
# ---------------------------------------------------------------------------
def _character_to_wire(char: dict[str, Any]) -> dict[str, Any]:
    pos = char.get("position") or char.get("pos") or [0, 0]
    if not isinstance(pos, (list, tuple)) or len(pos) < 2:
        pos = [0, 0]
    metadata = char.get("metadata") or {}
    if not isinstance(metadata, dict):
        metadata = {}
    # Keep metadata.health / .name / .color / .y in sync with top-level fields
    # so clients that read either place (top-down reads top-level, Babylon
    # reads metadata.y for jump height) both see the same values.
    merged_meta = dict(metadata)
    if char.get("health") is not None:
        merged_meta.setdefault("health", char.get("health"))
    if char.get("name"):
        merged_meta.setdefault("name", char.get("name"))
    if char.get("color"):
        merged_meta.setdefault("color", char.get("color"))
    if char.get("metadata") and isinstance(char["metadata"], dict) and "y" in char["metadata"]:
        merged_meta["y"] = char["metadata"]["y"]
    return {
        "clientId": char.get("clientId"),
        "player_id": char.get("player_id"),
        "characterModelId": char.get("characterModelId"),
        "characterName": char.get("characterName") or char.get("characterModelId"),
        "displayName": char.get("displayName") or char.get("name"),
        "name": char.get("name") or char.get("displayName"),
        "score": _clamp_int(char.get("score"), 0, MAX_SCORE, 0),
        "health": _clamp_int(char.get("health"), MIN_HEALTH, MAX_HEALTH, 100),
        "position": [float(pos[0]), float(pos[1])],
        "pos": [float(pos[0]), float(pos[1])],
        "velocity": char.get("velocity") or [0, 0],
        "animationState": char.get("animationState") or "idle",
        "animationFrame": char.get("animationFrame") or 0,
        "facing": char.get("facing") if _is_number(char.get("facing")) else 0,
        "rotation": char.get("rotation") if _is_number(char.get("rotation")) else 0,
        "isJumping": bool(char.get("isJumping", False)),
        "onGround": bool(char.get("onGround", True)),
        "metadata": merged_meta,
        "skill": char.get("skill") or {},
        "skillRating": char.get("skillRating"),
        "rating": char.get("rating"),
        "elo": char.get("elo"),
        "last_seen": char.get("last_seen"),
        "joined_at": char.get("joined_at"),
    }


def _item_to_wire(item: dict[str, Any]) -> dict[str, Any]:
    pos = item.get("pos") or item.get("position") or [0, 0]
    if not isinstance(pos, (list, tuple)) or len(pos) < 2:
        pos = [0, 0]
    return {
        "instanceId": item.get("instanceId"),
        "itemId": item.get("itemId") or "gem",
        "pos": [float(pos[0]), float(pos[1])],
        "position": [float(pos[0]), float(pos[1])],
        "isCollected": bool(item.get("isCollected", False)),
        "collectedBy": item.get("collectedBy"),
        "metadata": item.get("metadata") or {},
    }


def _game_summary(game_data: dict[str, Any]) -> dict[str, Any]:
    chars = game_data.get("characters") or {}
    return {
        "game_id": game_data.get("game_id"),
        "environment_name": game_data.get("environment_name"),
        "room_id": game_data.get("room_id"),
        "player_count": len(chars),
        "item_count": len(game_data.get("item_state") or []),
        "synchronizer": game_data.get("synchronizer"),
        "turn_count": game_data.get("turn_count") or 0,
        "created_at": game_data.get("created_at"),
        "last_update_at": game_data.get("last_update_at"),
        "last_player_action_at": game_data.get("last_player_action_at"),
    }


def _snapshot_patch(game_data: dict[str, Any], for_client_id: Optional[str] = None) -> dict[str, Any]:
    # Full snapshot: every character + every uncollected gem + turn + env.
    # Sent as the FIRST SSE patch on subscribe. Clients merge it exactly
    # like any other patch (applyMp handles full and delta identically).
    chars = [_character_to_wire(c) for c in (game_data.get("characters") or {}).values()]
    items = [_item_to_wire(i) for i in (game_data.get("item_state") or []) if not i.get("isCollected")]
    is_sync = (game_data.get("synchronizer") == for_client_id) if for_client_id else False
    return {
        "connection": {
            "isSynchronizer": bool(is_sync),
            "roomId": game_data.get("room_id"),
            "game_id": game_data.get("game_id"),
            "environment_name": game_data.get("environment_name"),
        },
        "characters": chars,
        "items": {"updates": items, "collections": []},
        "synchronizer": game_data.get("synchronizer"),
        "turn_state": game_data.get("turn_state"),
        "environment": game_data.get("environment"),
        "turn_count": game_data.get("turn_count") or 0,
    }


def _characters_patch(chars: list[dict[str, Any]]) -> dict[str, Any]:
    return {"characters": [_character_to_wire(c) for c in chars]}


def _items_patch(updates: list[dict[str, Any]], collections: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "items": {
            "updates": [_item_to_wire(u) for u in updates],
            "collections": [_jsonable(c) for c in collections],
        }
    }


def _client_joined_patch(client_id: str, character: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    patch: dict[str, Any] = {"clientJoined": {"clientId": client_id}}
    if character is not None:
        patch["characters"] = [_character_to_wire(character)]
    return patch


def _client_left_patch(client_id: str, reason: str = "left") -> dict[str, Any]:
    return {"clientLeft": {"clientId": client_id, "reason": reason}}


def _synchronizer_patch(synchronizer: Optional[str]) -> dict[str, Any]:
    return {"synchronizer": synchronizer, "connection": {"isSynchronizer": False, "synchronizer": synchronizer}}


def _turn_patch(game_data: dict[str, Any]) -> dict[str, Any]:
    return {"turn_state": game_data.get("turn_state"), "turn_count": game_data.get("turn_count") or 0}


def _environment_patch(game_data: dict[str, Any]) -> dict[str, Any]:
    return {"environment": game_data.get("environment")}


def _merge_patches(*patches: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for p in patches:
        for k, v in (p or {}).items():
            if k == "items" and "items" in merged and isinstance(v, dict):
                merged_items = merged["items"]
                if isinstance(merged_items, dict):
                    merged_items["updates"] = list(merged_items.get("updates") or []) + list(v.get("updates") or [])
                    merged_items["collections"] = list(merged_items.get("collections") or []) + list(v.get("collections") or [])
                    continue
            if k == "characters" and "characters" in merged and isinstance(v, list):
                merged["characters"] = list(merged["characters"]) + list(v)
                continue
            merged[k] = v
    return merged


# ---------------------------------------------------------------------------
# In-memory registry — the single source of truth for game state.
# All methods that mutate state are called with the lock held, or acquire
# it themselves. Broadcasts are queued, never blocking: if a subscriber's
# queue is full the oldest patch is dropped for that subscriber, not for
# everyone else. No timers here — sweeps happen on inbound packets.
# ---------------------------------------------------------------------------
class MultiplayerRegistry:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._games: dict[str, dict[str, Any]] = {}
        self._sessions: dict[str, dict[str, Any]] = {}
        self._subscribers: dict[str, list[queue.Queue]] = {}
        self._gem_counter: dict[str, int] = {}
        self._top_down_gem_counter: dict[str, int] = self._gem_counter

    # --- gem games ---------------------------------------------------------
    def _is_top_down_game(self, game_id: str = "", env_name: str = "") -> bool:
        return self._is_gem_game(game_id, env_name)

    def _is_gem_game(self, game_id: str = "", env_name: str = "") -> bool:
        # Gem games: top-down-demo (original) and babylon-demo (3D).
        # Both share the 2000-space, 24 gems, respawn-1-per-collection.
        return game_id in GEM_GAME_IDS or env_name in GEM_GAME_ENVS

    def _next_gem_id(self, env_key: str) -> str:
        self._gem_counter[env_key] = self._gem_counter.get(env_key, 0) + 1
        return f"gem-{env_key}-{self._gem_counter[env_key]}"

    def _spawn_top_down_gems_locked(self, game_data: dict[str, Any], env_key: str) -> list[dict[str, Any]]:
        return self._spawn_gems_locked(game_data, env_key)

    def _spawn_gems_locked(self, game_data: dict[str, Any], env_key: str) -> list[dict[str, Any]]:
        # Spawn 24 gems in a 6x4 grid with jitter, 2000-space.
        # Idempotent: if gems already exist, do nothing.
        game_data.setdefault("item_state", [])
        if game_data["item_state"]:
            return []
        items: list[dict[str, Any]] = []
        for i in range(GEM_COUNT):
            instance_id = self._next_gem_id(env_key)
            x = (i % GEM_GRID_COLS) * GEM_GRID_X_STEP + GEM_GRID_X0 + (i * 37) % GEM_JITTER_X
            y = (i // GEM_GRID_COLS) * GEM_GRID_Y_STEP + GEM_GRID_Y0 + (i * 53) % GEM_JITTER_Y
            items.append({
                "instanceId": instance_id,
                "itemId": "gem",
                "pos": [x, y],
                "position": [x, y],
                "isCollected": False,
                "collectedBy": None,
                "metadata": {},
            })
        game_data["item_state"] = items
        return items

    def _spawn_one_gem_locked(self, game_data: dict[str, Any], env_key: str) -> dict[str, Any]:
        # Respawn exactly 1 gem per collection, random 2000-space position.
        new_id = self._next_gem_id(env_key)
        x = random.randint(GEM_WORLD_MIN, GEM_WORLD_MAX)
        y = random.randint(GEM_WORLD_MIN, GEM_WORLD_MAX)
        item = {
            "instanceId": new_id,
            "itemId": "gem",
            "pos": [x, y],
            "position": [x, y],
            "isCollected": False,
            "collectedBy": None,
            "metadata": {},
        }
        game_data.setdefault("item_state", []).append(item)
        return item

    # --- game-data lifecycle -------------------------------------------------
    def _delete_game_data_locked(self, game_id: str, env_key: str) -> None:
        game_data = self._games.get(env_key)
        if game_data is not None:
            del self._games[env_key]
        if self._games.get(game_id) is game_data:
            del self._games[game_id]
        # Drop sessions that pointed at this game so their streams end.
        for sid in [s for s, info in self._sessions.items() if info.get("env_key") == env_key]:
            del self._sessions[sid]

    def _create_game_data(self, game_id: str, env_key: str, config: Any = None) -> dict[str, Any]:
        now = _now_iso()
        game_data: dict[str, Any] = {
            "id": game_id,
            "game_id": game_id,
            "environment_name": env_key,
            "created_at": now,
            "characters": {},
            "item_state": [],
            "character_order": [],
            "character_updates": [],
            "turn_state": None,
            "room_id": str(uuid4()),
            "turn_count": 0,
            "last_reset_at": now,
            "last_player_action_at": now,
            "environment": None,
            "item_state_updates": [],
            "last_update_at": now,
            "synchronizer": None,
            "config": config if isinstance(config, dict) else {},
            "match_history": [],
            "turn_history": [],
        }
        self._games[env_key] = game_data
        self._games[game_id] = game_data
        return game_data

    def _ensure_game_data_locked(self, game_id: str, env_key: str, config: Any = None) -> dict[str, Any]:
        game_data = self._games.get(env_key)
        if game_data is None:
            game_data = self._games.get(game_id)
            if game_data is not None:
                self._games[env_key] = game_data
        if game_data is None:
            game_data = self._create_game_data(game_id, env_key, config)
        if config is not None and isinstance(config, dict):
            game_data["config"] = _merge_dict(game_data.get("config") or {}, config)
        if self._is_gem_game(game_id, env_key) and not game_data.get("item_state"):
            self._spawn_gems_locked(game_data, env_key)
        return game_data

    def get_or_create_game(self, game_id: str, env_key: str, config: Any = None) -> dict[str, Any]:
        with self._lock:
            return self._ensure_game_data_locked(game_id, env_key, config)

    def get_game(self, game_id: str, env_key: str) -> Optional[dict[str, Any]]:
        with self._lock:
            return self._games.get(env_key) or self._games.get(game_id)

    def list_games(self) -> list[dict[str, Any]]:
        with self._lock:
            seen: set[int] = set()
            out: list[dict[str, Any]] = []
            for gd in self._games.values():
                if id(gd) in seen:
                    continue
                seen.add(id(gd))
                out.append(_game_summary(gd))
            return out

    # --- config / disconnect helpers ----------------------------------------
    def _get_disconnection_time_locked(self, game_data: dict[str, Any]) -> float:
        return _disconnection_time_from_config(game_data.get("config") or {})

    def _get_empty_ttl_locked(self, game_data: dict[str, Any]) -> float:
        return _empty_ttl_from_config(game_data.get("config") or {})

    def _should_delete_game_data_locked(self, game_data: dict[str, Any], now: datetime) -> bool:
        if game_data.get("characters"):
            return False
        last_reset = _parse_iso(game_data.get("last_reset_at"))
        if last_reset is None:
            return False
        return (now - last_reset).total_seconds() > self._get_empty_ttl_locked(game_data)

    def _enforce_disconnection_locked(self, game_data: dict[str, Any]) -> list[str]:
        # Sweep expired clients. Returns the expired clientIds.
        now = _now_dt()
        disconnection_time = self._get_disconnection_time_locked(game_data)
        expired: list[str] = []
        for client_id, char in list((game_data.get("characters") or {}).items()):
            last_seen = _parse_iso(char.get("last_seen"))
            if last_seen is None:
                expired.append(client_id)
            elif (now - last_seen).total_seconds() > disconnection_time:
                expired.append(client_id)
        for client_id in expired:
            game_data["characters"].pop(client_id, None)
            if client_id in (game_data.get("character_order") or []):
                game_data["character_order"].remove(client_id)
        return expired

    def _sweep_and_broadcast_locked(self, game_data: dict[str, Any], env_key: str) -> list[str]:
        # Sweep + broadcast clientLeft for each expired client + synchronizer
        # change if the synchronizer was among the expired. Called on
        # EVERY inbound packet from ANY client (see join / character-state /
        # item-state / heartbeat below). This is the ONLY disconnect path —
        # there is no timer loop anywhere in this file.
        expired = self._enforce_disconnection_locked(game_data)
        for client_id in expired:
            self._broadcast_locked(env_key, _client_left_patch(client_id, "timeout"))
        if expired:
            if self._sync_synchronizer_locked(game_data):
                self._broadcast_locked(env_key, _synchronizer_patch(game_data.get("synchronizer")))
            if self._should_delete_game_data_locked(game_data, _now_dt()):
                self._delete_game_data_locked(game_data.get("game_id") or "", env_key)
        return expired

    def _get_next_synchronizer_locked(self, game_data: dict[str, Any]) -> Optional[str]:
        order = game_data.get("character_order") or []
        return order[0] if order else None

    def _sync_synchronizer_locked(self, game_data: dict[str, Any]) -> bool:
        nxt = self._get_next_synchronizer_locked(game_data)
        if game_data.get("synchronizer") != nxt:
            game_data["synchronizer"] = nxt
            return True
        return False

    # --- sessions + SSE subscribers -----------------------------------------
    def register_session(self, session_id: str, client_id: str, game_id: str, env_key: str) -> None:
        with self._lock:
            self._sessions[session_id] = {
                "session_id": session_id,
                "client_id": client_id,
                "game_id": game_id,
                "env_key": env_key,
                "created_at": _now_iso(),
            }

    def resolve_session(self, session_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            info = self._sessions.get(session_id)
            return dict(info) if info else None

    def find_session_for_client(self, client_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            for info in self._sessions.values():
                if info.get("client_id") == client_id:
                    return dict(info)
            return None

    def subscribe(self, env_key: str) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=SSE_SUBSCRIBER_QUEUE_MAX)
        with self._lock:
            self._subscribers.setdefault(env_key, []).append(q)
        return q

    def unsubscribe(self, env_key: str, q: queue.Queue) -> None:
        with self._lock:
            subs = self._subscribers.get(env_key) or []
            if q in subs:
                subs.remove(q)
            if not subs:
                self._subscribers.pop(env_key, None)

    def subscriber_count(self, env_key: str) -> int:
        with self._lock:
            return len(self._subscribers.get(env_key) or [])

    def _broadcast_locked(self, env_key: str, mp_patch: dict[str, Any]) -> None:
        # THE SSE FAN-OUT. Every mutation in this registry ends here.
        # mp_patch is a dict like {"characters": [...]} or
        # {"items": {"updates": [...], "collections": [...]}} or
        # {"clientLeft": {...}} or a merged combination.
        # Each subscriber's queue gets the patch; if its queue is full we
        # drop the patch for THAT subscriber only (put_nowait + Full).
        if not mp_patch:
            return
        subs = list(self._subscribers.get(env_key) or [])
        for q in subs:
            try:
                q.put_nowait(_jsonable(mp_patch))
            except queue.Full:
                try:
                    q.get_nowait()
                except queue.Empty:
                    pass
                try:
                    q.put_nowait(_jsonable(mp_patch))
                except queue.Full:
                    pass

    def broadcast(self, env_key: str, mp_patch: dict[str, Any]) -> None:
        with self._lock:
            self._broadcast_locked(env_key, mp_patch)

    def snapshot_for(self, env_key: str, client_id: Optional[str] = None) -> Optional[dict[str, Any]]:
        with self._lock:
            game_data = self._games.get(env_key)
            if game_data is None:
                return None
            return _snapshot_patch(game_data, client_id)

    # --- join / leave -------------------------------------------------------
    def join_player(self, game_id: str, env_key: str, player_id: str, character_name: str,
                    display_name: Optional[str], skill: Any, config: Any = None) -> tuple[dict[str, Any], str, str]:
        # Returns (game_data, client_id, session_id). Broadcasts:
        #   clientJoined + the new character record + synchronizer if changed.
        # Also sweeps expired clients first (every inbound packet sweeps).
        with self._lock:
            game_data = self._ensure_game_data_locked(game_id, env_key, config)
            self._sweep_and_broadcast_locked(game_data, env_key)
            # Re-fetch: sweep may have deleted empty game data.
            if self._games.get(env_key) is None:
                game_data = self._ensure_game_data_locked(game_id, env_key, config)
            max_players = _max_players_from_config(game_data.get("config") or {})
            if len(game_data.get("characters") or {}) >= max_players:
                raise MultiplayerException("room full", status_code=409)
            now = _now_iso()
            client_id = _new_client_id(player_id)
            session_id = _new_session_id()
            skill_dict = skill if isinstance(skill, dict) else {}
            rating = skill_dict.get("rating") if isinstance(skill_dict.get("rating"), (int, float)) else None
            character = {
                "clientId": client_id,
                "player_id": player_id,
                "characterModelId": character_name or "player",
                "characterName": character_name or "player",
                "displayName": display_name or character_name or "player",
                "name": character_name or "player",
                "score": 0,
                "health": MAX_HEALTH,
                "position": [0, 0],
                "pos": [0, 0],
                "velocity": [0, 0],
                "animationState": "idle",
                "animationFrame": 0,
                "facing": 0,
                "rotation": 0,
                "isJumping": False,
                "onGround": True,
                "metadata": {"health": MAX_HEALTH, "name": character_name or "player", "color": None, "y": 0},
                "last_seen": now,
                "joined_at": now,
                "skill": skill_dict,
                "skillRating": rating,
                "rating": rating,
                "elo": rating,
                "color": None,
            }
            # Deterministic-ish colour from client_id so the name bar / remote
            # clone tint is stable for the life of the session.
            hue = sum(ord(c) for c in client_id) % 360
            character["color"] = f"hsl({hue} 70% 55%)"
            character["metadata"]["color"] = character["color"]
            game_data.setdefault("characters", {})[client_id] = character
            game_data.setdefault("character_order", []).append(client_id)
            game_data["last_player_action_at"] = now
            game_data["last_update_at"] = now
            sync_changed = self._sync_synchronizer_locked(game_data)
            self.register_session(session_id, client_id, game_id, env_key)
            self._broadcast_locked(env_key, _client_joined_patch(client_id, character))
            if sync_changed:
                self._broadcast_locked(env_key, _synchronizer_patch(game_data.get("synchronizer")))
            return game_data, client_id, session_id

    def get_character_state(self, game_id: str, env_key: str) -> tuple[Optional[dict[str, Any]], Optional[dict[str, Any]]]:
        with self._lock:
            game_data = self._games.get(env_key) or self._games.get(game_id)
            if game_data is None:
                return None, None
            game_data["last_update_at"] = _now_iso()
            return game_data, game_data.get("characters") or {}

    def get_match_state(self, game_id: str, env_key: str) -> Optional[dict[str, Any]]:
        with self._lock:
            game_data = self._games.get(env_key) or self._games.get(game_id)
            if game_data is None:
                return None
            game_data["last_update_at"] = _now_iso()
            return game_data

    def remove_player(self, client_id: str) -> Optional[dict[str, Any]]:
        # Broadcasts clientLeft + synchronizer change. Returns the game.
        with self._lock:
            for env_key, game_data in list(self._games.items()):
                if not isinstance(game_data, dict) or "characters" not in game_data:
                    continue
                if client_id in (game_data.get("characters") or {}):
                    del game_data["characters"][client_id]
                    if client_id in (game_data.get("character_order") or []):
                        game_data["character_order"].remove(client_id)
                    game_data["last_update_at"] = _now_iso()
                    sync_changed = self._sync_synchronizer_locked(game_data)
                    self._broadcast_locked(env_key, _client_left_patch(client_id, "left"))
                    if sync_changed:
                        self._broadcast_locked(env_key, _synchronizer_patch(game_data.get("synchronizer")))
                    # Drop sessions for this client so streams end.
                    for sid in [s for s, info in self._sessions.items() if info.get("client_id") == client_id]:
                        del self._sessions[sid]
                    if not game_data.get("characters"):
                        game_data["last_reset_at"] = _now_iso()
                    return game_data
            return None

    def _check_not_expired_locked(self, game_data: dict[str, Any], char: dict[str, Any]) -> None:
        # A late packet from an expired client is 403, never a revive.
        last_seen = _parse_iso(char.get("last_seen"))
        if last_seen is None:
            return
        disconnection_time = self._get_disconnection_time_locked(game_data)
        if (_now_dt() - last_seen).total_seconds() > disconnection_time:
            raise MultiplayerException("disconnected: timeout", status_code=403)

    def handle_character_state(self, game_id: str, env_key: str, client_id: str, update: dict[str, Any]) -> Optional[dict[str, Any]]:
        # Applies one character-state update and broadcasts
        # {"characters": [updated character]} as an SSE patch.
        # Also sweeps expired clients and broadcasts their clientLeft.
        with self._lock:
            game_data = self._games.get(env_key) or self._games.get(game_id)
            if game_data is None:
                return None
            self._sweep_and_broadcast_locked(game_data, env_key)
            if self._games.get(env_key) is None:
                return None
            game_data = self._games.get(env_key) or self._games.get(game_id)
            char = (game_data.get("characters") or {}).get(client_id)
            if char is None:
                return game_data
            self._check_not_expired_locked(game_data, char)
            now = _now_iso()
            char["last_seen"] = now
            game_data["last_player_action_at"] = now
            game_data["last_update_at"] = now
            pos = _as_pos(update.get("position") if update.get("position") is not None else update.get("pos"))
            if pos is not None:
                char["position"] = pos
                char["pos"] = pos
            vel = _as_float_list(update.get("velocity"), 2)
            if vel is not None:
                char["velocity"] = vel
            if update.get("animationState") is not None:
                char["animationState"] = _clean_str(update.get("animationState"), "idle", 40)
            if update.get("animationFrame") is not None:
                char["animationFrame"] = _clamp_int(update.get("animationFrame"), 0, 100000, 0)
            if update.get("facing") is not None and _is_number(update.get("facing")):
                char["facing"] = float(update.get("facing"))
            if update.get("rotation") is not None and _is_number(update.get("rotation")):
                char["rotation"] = float(update.get("rotation"))
            if update.get("score") is not None:
                char["score"] = _clamp_int(update.get("score"), 0, MAX_SCORE, char.get("score") or 0)
            if update.get("health") is not None:
                char["health"] = _clamp_int(update.get("health"), MIN_HEALTH, MAX_HEALTH, char.get("health") or 100)
            if update.get("isJumping") is not None:
                char["isJumping"] = bool(update.get("isJumping"))
            if update.get("onGround") is not None:
                char["onGround"] = bool(update.get("onGround"))
            if update.get("characterModelId"):
                char["characterModelId"] = _clean_str(update.get("characterModelId"), "player", 80)
                char["characterName"] = char["characterModelId"]
            meta = update.get("metadata")
            if isinstance(meta, dict):
                existing = char.get("metadata") or {}
                if not isinstance(existing, dict):
                    existing = {}
                for k, v in meta.items():
                    if v is not None:
                        existing[k] = v
                char["metadata"] = existing
                if existing.get("health") is not None:
                    char["health"] = _clamp_int(existing.get("health"), MIN_HEALTH, MAX_HEALTH, char["health"])
                if existing.get("name"):
                    char["name"] = _clean_str(existing.get("name"), char.get("name") or "player", 80)
                if existing.get("color"):
                    char["color"] = str(existing.get("color"))
            # Keep metadata.health in sync with top-level health.
            if isinstance(char.get("metadata"), dict):
                char["metadata"]["health"] = char.get("health")
            game_data.setdefault("character_updates", []).append(_jsonable(char))
            if len(game_data["character_updates"]) > MAX_BODY_UPDATES:
                game_data["character_updates"] = game_data["character_updates"][-MAX_BODY_UPDATES:]
            self._broadcast_locked(env_key, _characters_patch([char]))
            return game_data

    def handle_item_state(self, game_id: str, env_key: str, updates: list[dict[str, Any]], collections: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
        # Applies item updates + collections and broadcasts
        # {"items": {"updates": [...], "collections": [...]}} as one SSE patch.
        # Gem games: each collection also spawns 1 new gem, included in
        # the SAME patch as an update, so every client sees the replacement.
        with self._lock:
            game_data = self._games.get(env_key) or self._games.get(game_id)
            if game_data is None:
                return None
            self._sweep_and_broadcast_locked(game_data, env_key)
            if self._games.get(env_key) is None:
                return None
            game_data = self._games.get(env_key) or self._games.get(game_id)
            game_data["last_player_action_at"] = _now_iso()
            game_data["last_update_at"] = _now_iso()
            game_data.setdefault("item_state", [])
            new_updates: list[dict[str, Any]] = []
            new_collections: list[dict[str, Any]] = []
            for u in updates or []:
                if not isinstance(u, dict):
                    continue
                instance_id = u.get("instanceId")
                if instance_id is None:
                    continue
                existing = next((x for x in game_data["item_state"] if x.get("instanceId") == instance_id), None)
                if existing is None:
                    new_item = dict(u)
                    pos = _as_pos(new_item.get("pos") if new_item.get("pos") is not None else new_item.get("position"))
                    if pos is not None:
                        new_item["pos"] = pos
                        new_item["position"] = pos
                    new_item.setdefault("itemId", "gem")
                    new_item.setdefault("isCollected", False)
                    game_data["item_state"].append(new_item)
                    new_updates.append(new_item)
                else:
                    pos = _as_pos(u.get("pos") if u.get("pos") is not None else u.get("position"))
                    if pos is not None:
                        existing["pos"] = pos
                        existing["position"] = pos
                    for k, v in u.items():
                        if k in ("pos", "position"):
                            continue
                        if v is not None:
                            existing[k] = v
                    new_updates.append(existing)
            for c in collections or []:
                if not isinstance(c, dict):
                    continue
                instance_id = c.get("instanceId")
                if instance_id is None:
                    continue
                existing = next((x for x in game_data["item_state"] if x.get("instanceId") == instance_id), None)
                if existing is None:
                    continue
                if existing.get("isCollected"):
                    continue
                existing["isCollected"] = True
                existing["collectedBy"] = c.get("collectedByClientId") or c.get("collectedBy")
                new_collections.append({
                    "instanceId": instance_id,
                    "collectedByClientId": c.get("collectedByClientId") or c.get("collectedBy"),
                })
                if self._is_gem_game(game_id, env_key):
                    new_item = self._spawn_one_gem_locked(game_data, env_key)
                    new_updates.append(new_item)
            if new_updates or new_collections:
                game_data.setdefault("item_state_updates", []).append({"updates": _jsonable(new_updates), "collections": _jsonable(new_collections)})
                if len(game_data["item_state_updates"]) > MAX_BODY_UPDATES:
                    game_data["item_state_updates"] = game_data["item_state_updates"][-MAX_BODY_UPDATES:]
                self._broadcast_locked(env_key, _items_patch(new_updates, new_collections))
            return game_data

    def handle_heartbeat(self, client_id: str) -> Optional[dict[str, Any]]:
        # Touches last_seen, sweeps expired clients, broadcasts clientLeft.
        with self._lock:
            for env_key, game_data in list(self._games.items()):
                if not isinstance(game_data, dict) or "characters" not in game_data:
                    continue
                char = (game_data.get("characters") or {}).get(client_id)
                if char is not None:
                    self._check_not_expired_locked(game_data, char)
                    now = _now_iso()
                    char["last_seen"] = now
                    game_data["last_player_action_at"] = now
                    game_data["last_update_at"] = now
                    self._sweep_and_broadcast_locked(game_data, env_key)
                    return game_data
            return None

    def handle_disconnection(self, game_id: str, env_key: str, now: datetime) -> tuple[Optional[dict[str, Any]], list[str]]:
        with self._lock:
            game_data = self._games.get(env_key) or self._games.get(game_id)
            if game_data is None:
                return None, []
            expired = self._sweep_and_broadcast_locked(game_data, env_key)
            if self._games.get(env_key) is None:
                return None, expired
            return game_data, expired

    def handle_match_action(self, game_id: str, env_key: str, action: dict[str, Any], client_id: str) -> Optional[dict[str, Any]]:
        # Match actions: end / start / pause / resume / custom. Broadcasts
        # the resulting match state so every client sees it via SSE.
        with self._lock:
            game_data = self._games.get(env_key) or self._games.get(game_id)
            if game_data is None:
                return None
            self._sweep_and_broadcast_locked(game_data, env_key)
            if self._games.get(env_key) is None:
                return None
            game_data = self._games.get(env_key) or self._games.get(game_id)
            action_type = str((action or {}).get("type") or "")
            data = (action or {}).get("data") or {}
            now = _now_iso()
            game_data["last_player_action_at"] = now
            game_data["last_update_at"] = now
            entry = {"type": action_type, "data": _jsonable(data), "client_id": client_id, "at": now}
            game_data.setdefault("match_history", []).append(entry)
            if action_type == "end":
                game_data["last_reset_at"] = now
                game_data["turn_state"] = None
                game_data["turn_count"] = 0
            elif action_type == "start":
                game_data["turn_count"] = 0
                game_data["turn_state"] = {"phase": "started", "turn_index": 0, "current_player": None, "started_at": now}
            elif action_type == "pause":
                if isinstance(game_data.get("turn_state"), dict):
                    game_data["turn_state"]["phase"] = "paused"
            elif action_type == "resume":
                if isinstance(game_data.get("turn_state"), dict):
                    game_data["turn_state"]["phase"] = "running"
            self._broadcast_locked(env_key, _merge_patches(
                {"match_action": entry},
                _turn_patch(game_data),
            ))
            return game_data

    def handle_turn_action(self, game_id: str, env_key: str, action: dict[str, Any], client_id: str) -> Optional[dict[str, Any]]:
        # Turn-based games: advance turn_state, broadcast it via SSE.
        with self._lock:
            game_data = self._games.get(env_key) or self._games.get(game_id)
            if game_data is None:
                return None
            self._sweep_and_broadcast_locked(game_data, env_key)
            if self._games.get(env_key) is None:
                return None
            game_data = self._games.get(env_key) or self._games.get(game_id)
            now = _now_iso()
            order = list(game_data.get("character_order") or [])
            current = game_data.get("turn_state") or {}
            turn_index = current.get("turn_index") or 0
            action_type = str((action or {}).get("type") or "move")
            if action_type == "advance":
                turn_index += 1
            elif action_type == "reset":
                turn_index = 0
            current_player = order[turn_index % len(order)] if order else None
            turn_state = {
                "phase": "running",
                "turn_index": turn_index,
                "current_player": current_player,
                "last_action": {"type": action_type, "data": _jsonable((action or {}).get("data") or {}), "client_id": client_id},
                "updated_at": now,
            }
            game_data["turn_state"] = turn_state
            game_data["turn_count"] = (game_data.get("turn_count") or 0) + 1
            game_data["last_player_action_at"] = now
            game_data["last_update_at"] = now
            game_data.setdefault("turn_history", []).append(_jsonable(turn_state))
            self._broadcast_locked(env_key, _turn_patch(game_data))
            return game_data

    def handle_environment(self, game_id: str, env_key: str, environment: Any) -> Optional[dict[str, Any]]:
        # Environment updates (map / settings / weather): broadcast via SSE.
        with self._lock:
            game_data = self._games.get(env_key) or self._games.get(game_id)
            if game_data is None:
                return None
            self._sweep_and_broadcast_locked(game_data, env_key)
            if self._games.get(env_key) is None:
                return None
            game_data = self._games.get(env_key) or self._games.get(game_id)
            if isinstance(environment, dict):
                game_data["environment"] = environment
            game_data["last_player_action_at"] = _now_iso()
            game_data["last_update_at"] = _now_iso()
            self._broadcast_locked(env_key, _environment_patch(game_data))
            return game_data

    def reset_game(self, game_id: str, env_key: str, reason: str = "") -> Optional[dict[str, Any]]:
        # Clears characters + items + turn, keeps the room. Broadcasts a
        # full snapshot patch so clients resync to an empty world.
        with self._lock:
            game_data = self._games.get(env_key) or self._games.get(game_id)
            if game_data is None:
                return None
            now = _now_iso()
            game_data["characters"] = {}
            game_data["character_order"] = []
            game_data["turn_state"] = None
            game_data["turn_count"] = 0
            game_data["last_reset_at"] = now
            game_data["last_update_at"] = now
            game_data["last_player_action_at"] = now
            # Gems: clear and respawn the full set for gem games.
            game_data["item_state"] = []
            if self._is_gem_game(game_id, env_key):
                self._spawn_gems_locked(game_data, env_key)
            game_data["synchronizer"] = None
            self._broadcast_locked(env_key, _snapshot_patch(game_data, None))
            return game_data

    def update_config(self, game_id: str, env_key: str, config: dict[str, Any]) -> Optional[dict[str, Any]]:
        with self._lock:
            game_data = self._ensure_game_data_locked(game_id, env_key, None)
            game_data["config"] = _merge_dict(game_data.get("config") or {}, config or {})
            game_data["last_update_at"] = _now_iso()
            return game_data


registry = MultiplayerRegistry()


def _get_mp_service() -> MultiplayerService:
    return MultiplayerService(registry)


# ---------------------------------------------------------------------------
# Auth + per-request helpers
# ---------------------------------------------------------------------------
async def _verify_token(credentials: HTTPAuthorizationCredentials = Security(security)):
    if settings.multiplayer_auth_mode == "none":
        return None
    if credentials is None:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    if settings.multiplayer_api_key and credentials.credentials != settings.multiplayer_api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return credentials


async def _get_client_id(request: Request) -> str:
    client_id = request.headers.get("X-Client-ID")
    if client_id is None:
        client_id = request.query_params.get("client_id")
    if client_id is None:
        # Body fallback for clients that POST {client_id} instead of a header.
        try:
            body = await request.json()
            if isinstance(body, dict):
                client_id = body.get("client_id") or body.get("clientId")
        except Exception:
            pass
    if client_id is None:
        raise HTTPException(status_code=400, detail="Missing X-Client-ID header")
    return str(client_id)


async def _get_session_id(request: Request) -> str:
    session_id = request.query_params.get("session_id")
    if session_id is None:
        session_id = request.headers.get("X-Session-ID")
    if session_id is None:
        raise HTTPException(status_code=400, detail="Missing session_id")
    return str(session_id)


def _resolve_game(request: Request, body: Any = None) -> tuple[str, str]:
    # Precedence: query > body > defaults. Both game_id and environment_name
    # are accepted; environment_name is the SSE / broadcast key.
    game_id = request.query_params.get("game_id") or ""
    env_key = request.query_params.get("environment_name") or request.query_params.get("environment") or ""
    if body is not None and isinstance(body, dict):
        game_id = game_id or str(body.get("game_id") or "")
        env_key = env_key or str(body.get("environment_name") or body.get("environment") or "")
    if not game_id:
        game_id = "default"
    if not env_key:
        env_key = game_id
    return game_id, env_key


def _resolve_game_from_model(request: Request, model: Any) -> tuple[str, str]:
    game_id = request.query_params.get("game_id") or ""
    env_key = request.query_params.get("environment_name") or ""
    if model is not None:
        game_id = game_id or str(getattr(model, "game_id", "") or "")
        env_key = env_key or str(getattr(model, "environment_name", "") or "")
    if not game_id:
        game_id = "default"
    if not env_key:
        env_key = game_id
    return game_id, env_key


def _error(status_code: int, detail: str) -> HTTPException:
    return HTTPException(status_code=status_code, detail=detail)


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="MATCH Multiplayer API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "match-multiplayer"}


@app.get("/api/multiplayer/config", response_model=MultiplayerConfigOut)
async def get_config():
    return MultiplayerConfigOut(
        game_id="match-multiplayer",
        requires_matchmaking=False,
        requires_turn_based=False,
        max_players_per_room=MAX_PLAYERS_PER_ROOM_DEFAULT,
        min_players_per_room=MIN_PLAYERS_PER_ROOM_DEFAULT,
    )


@app.get("/")
async def root():
    return {
        "service": "match-multiplayer",
        "endpoints": {
            "health": "/health",
            "config": "/api/multiplayer/config",
            "join": "POST /api/multiplayer/join",
            "leave": "POST /api/multiplayer/leave",
            "character_state": "PATCH /api/multiplayer/character-state",
            "character_state_get": "GET /api/multiplayer/character-state?environment_name=...",
            "item_state": "PATCH /api/multiplayer/item-state",
            "item_state_get": "GET /api/multiplayer/item-state?environment_name=...",
            "heartbeat": "POST /api/multiplayer/heartbeat",
            "stream": "GET /api/multiplayer/stream?session_id=...",
            "state": "GET /api/multiplayer/state?environment_name=...",
            "games": "GET /api/multiplayer/games",
            "match_action": "POST /api/multiplayer/match-action",
            "turn": "POST /api/multiplayer/turn",
            "environment": "POST /api/multiplayer/environment",
            "reset": "POST /api/multiplayer/reset",
            "top_down_demo": "/top-down",
            "top_down_demo_alt": "/demo/top-down",
            "babylon_demo": "/babylon",
            "babylon_demo_alt": "/demo/babylon",
            "babylon_model": "/hulk.glb (also /demo/hulk.glb)",
        },
    }


# ---------------------------------------------------------------------------
# Demo files — serves files from server/app/demo/
#   /demo/babylon -> babylon.html, /demo/hulk.glb -> hulk.glb
#   /babylon      -> babylon.html, /hulk.glb      -> hulk.glb
# babylon.html loads hulk.glb relatively ("hulk.glb"), so whichever page
# URL is used, the model resolves to the same directory as the page.
# ---------------------------------------------------------------------------
_DEMO_MIME = {
    ".html": "text/html; charset=utf-8",
    ".glb": "model/gltf-binary",
    ".gltf": "model/gltf+json",
    ".bin": "application/octet-stream",
    ".js": "text/javascript; charset=utf-8",
    ".json": "application/json",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".css": "text/css; charset=utf-8",
    ".txt": "text/plain; charset=utf-8",
}


def _serve_demo_file(name: str):
    demo_dir = Path(__file__).parent / DEMO_DIR_NAME
    safe = str(name or "").strip()
    if not safe or safe in (".", "..") or "/" in safe or "\\" in safe or ".." in safe:
        raise HTTPException(status_code=400, detail="Invalid demo file")
    if any(c not in ALLOWED_DEMO_CHARS for c in safe):
        raise HTTPException(status_code=400, detail="Invalid demo file")
    suffix = Path(safe).suffix.lower()
    if suffix not in _DEMO_MIME:
        raise HTTPException(status_code=404, detail="Demo file not found")
    demo_file = demo_dir / safe
    if not demo_file.exists() or not demo_file.is_file():
        raise HTTPException(status_code=404, detail="Demo file not found")
    # Long cache for .glb: the model is versioned by filename in practice,
    # and re-importing it per remote clone must be cheap.
    headers = {}
    if suffix == ".glb":
        headers["Cache-Control"] = "public, max-age=3600"
    return FileResponse(str(demo_file), media_type=_DEMO_MIME[suffix], headers=headers)


@app.get("/top-down", include_in_schema=False)
async def top_down_demo_page():
    return _serve_demo_file("top-down.html")


@app.get("/babylon", include_in_schema=False)
async def babylon_demo_page():
    return _serve_demo_file("babylon.html")


@app.get("/hulk.glb", include_in_schema=False)
async def hulk_model():
    # /babylon loads "hulk.glb" relative to / -> /hulk.glb
    return _serve_demo_file("hulk.glb")


@app.get("/demo/{name}", include_in_schema=False)
async def demo_file(name: str):
    # /demo/babylon -> babylon.html, /demo/hulk.glb -> hulk.glb
    if name == "babylon":
        return _serve_demo_file("babylon.html")
    if name == "top-down":
        return _serve_demo_file("top-down.html")
    return _serve_demo_file(name)


# ---------------------------------------------------------------------------
# Multiplayer endpoints
# Every mutation broadcasts an SSE signal patch via the registry — see
# _broadcast_locked(). The PATCH endpoints return {ok: true} immediately;
# the authoritative new state arrives on the stream, including to the
# sender itself, so every client (including the sender) applies the same
# patch path and never diverges.
# ---------------------------------------------------------------------------
@app.post("/api/multiplayer/join")
async def join_game(body: JoinRequest, request: Request, token: Any = Depends(_verify_token)):
    try:
        game_id = body.game_id or "default"
        env_key = body.environment_name or game_id
        game_data, client_id, session_id = registry.join_player(
            game_id, env_key, body.player_id, body.character_name,
            body.display_name or body.character_name, body.skill, body.config,
        )
        return {
            "client_id": client_id,
            "session_id": session_id,
            "room_id": game_data.get("room_id"),
            "is_synchronizer": game_data.get("synchronizer") == client_id,
            "disconnection_time_sec": registry._get_disconnection_time_locked(game_data),
            "environment_name": env_key,
            "game_id": game_id,
        }
    except MultiplayerException as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)


@app.post("/api/multiplayer/leave")
async def leave_game(request: Request, body: Optional[LeaveRequest] = None, token: Any = Depends(_verify_token)):
    try:
        client_id = await _get_client_id(request)
    except HTTPException:
        if body is not None and body.client_id:
            client_id = str(body.client_id)
        else:
            raise
    registry.remove_player(client_id)
    return {"ok": True}


@app.patch("/api/multiplayer/character-state")
async def update_character_state(body: CharacterStateRequest, request: Request, token: Any = Depends(_verify_token)):
    game_id, env_key = _resolve_game_from_model(request, body)
    for update in body.updates:
        try:
            registry.handle_character_state(game_id, env_key, update.clientId, update.model_dump(exclude_unset=True))
        except MultiplayerException as e:
            raise HTTPException(status_code=e.status_code, detail=e.message)
    return {"ok": True}


@app.get("/api/multiplayer/character-state")
async def get_character_state(request: Request, token: Any = Depends(_verify_token)):
    game_id, env_key = _resolve_game(request)
    game_data, chars = registry.get_character_state(game_id, env_key)
    if game_data is None:
        raise HTTPException(status_code=404, detail="Game not found")
    return {"ok": True, "characters": [_character_to_wire(c) for c in (chars or {}).values()]}


@app.patch("/api/multiplayer/item-state")
async def update_item_state(body: ItemStateRequest, request: Request, token: Any = Depends(_verify_token)):
    game_id, env_key = _resolve_game_from_model(request, body)
    if not body.collections and not body.updates:
        return {"ok": True}
    game_data = registry.handle_item_state(
        game_id, env_key,
        [u.model_dump(exclude_unset=True) for u in body.updates],
        [c.model_dump(exclude_unset=True) for c in body.collections],
    )
    if game_data is None:
        raise HTTPException(status_code=404, detail="Game not found")
    return {"ok": True, "item_state": [_item_to_wire(i) for i in game_data.get("item_state") or []]}


@app.get("/api/multiplayer/item-state")
async def get_item_state(request: Request, token: Any = Depends(_verify_token)):
    game_id, env_key = _resolve_game(request)
    game_data = registry.get_game(game_id, env_key)
    if game_data is None:
        raise HTTPException(status_code=404, detail="Game not found")
    return {"ok": True, "item_state": [_item_to_wire(i) for i in game_data.get("item_state") or []]}


@app.post("/api/multiplayer/heartbeat")
async def heartbeat(request: Request, token: Any = Depends(_verify_token)):
    client_id = await _get_client_id(request)
    try:
        game_data = registry.handle_heartbeat(client_id)
    except MultiplayerException as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)
    if game_data is None:
        raise HTTPException(status_code=404, detail="Client not found")
    return {"ok": True}


@app.get("/api/multiplayer/stream")
async def stream_game(request: Request, session_id: str = Depends(_get_session_id), token: Any = Depends(_verify_token)):
    # ------------------------------------------------------------------
    # SSE GAME STATE — THIS is where game state is handled.
    # The client opens ONE EventSource to this endpoint after join:
    #   new EventSource("/api/multiplayer/stream?session_id=" + sessionId)
    # and listens for "datastar-patch-signals" events:
    #   stream.addEventListener("datastar-patch-signals",
    #       ev => handleStreamData(ev.data))
    # Each patch is:
    #   event: datastar-patch-signals
    #   data: signals {"mp": { ...patch... }}
    # handleStreamData() strips "signals ", JSON-parses, and applyMp()
    # applies mp.connection / mp.characters / mp.items /
    # mp.clientJoined / mp.clientLeft / mp.synchronizer.
    #
    # This endpoint:
    #   1. Resolves session_id -> (client_id, game_id, env_key) via the
    #      registry. A session_id from join() is required — an unknown
    #      session_id is 404, so a stray EventSource cannot snoop a game.
    #   2. Subscribes this connection to that env_key's broadcast queue.
    #   3. FIRST sends a full snapshot patch (all characters + all gems +
    #      turn + environment + connection.isSynchronizer for THIS client)
    #      so a late joiner sees the whole world immediately.
    #   4. Then forwards every broadcast patch for that environment,
    #      in order, until the client disconnects.
    #   5. Sends a ": keep-alive" comment every SSE_KEEPALIVE_SEC while
    #      idle so proxies / browsers do not close the stream.
    #   6. Unsubscribes on disconnect.
    # There is NO polling anywhere: clients get state only from this
    # stream, and the server pushes only when state actually changes
    # (plus the keep-alive comment).
    # ------------------------------------------------------------------
    info = registry.resolve_session(session_id)
    if info is None:
        raise HTTPException(status_code=404, detail="Session not found")
    client_id = info["client_id"]
    env_key = info["env_key"]
    game_id = info["game_id"]
    q = registry.subscribe(env_key)

    async def gen():
        try:
            # 1. Full snapshot first — the client needs the world, not just deltas.
            snapshot = registry.snapshot_for(env_key, client_id)
            if snapshot is not None:
                payload = json.dumps({"mp": snapshot}, default=str)
                yield f"event: datastar-patch-signals\ndata: signals {payload}\n\n"
            else:
                yield "event: datastar-patch-signals\ndata: signals {\"mp\": {}}\n\n"
            # 2. Forward broadcast patches until disconnect.
            while True:
                if await request.is_disconnected():
                    break
                try:
                    mp_patch = await asyncio.to_thread(q.get, True, SSE_KEEPALIVE_SEC)
                except queue.Empty:
                    yield ": keep-alive\n\n"
                    continue
                except Exception:
                    yield ": keep-alive\n\n"
                    continue
                if mp_patch is None:
                    continue
                # Fix up connection.isSynchronizer for THIS client on
                # synchronizer patches so each client knows if it is "you".
                try:
                    if isinstance(mp_patch, dict) and "synchronizer" in mp_patch:
                        game_data = registry.get_game(game_id, env_key)
                        if game_data is not None:
                            mp_patch = dict(mp_patch)
                            mp_patch["connection"] = {
                                "isSynchronizer": game_data.get("synchronizer") == client_id,
                                "roomId": game_data.get("room_id"),
                                "synchronizer": game_data.get("synchronizer"),
                            }
                except Exception:
                    pass
                payload = json.dumps({"mp": mp_patch}, default=str)
                yield f"event: datastar-patch-signals\ndata: signals {payload}\n\n"
        finally:
            registry.unsubscribe(env_key, q)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/multiplayer/state")
async def get_game_state(request: Request, token: Any = Depends(_verify_token)):
    game_id, env_key = _resolve_game(request)
    game_data = registry.get_game(game_id, env_key)
    if game_data is None:
        raise HTTPException(status_code=404, detail="Game not found")
    return {
        "ok": True,
        "game_id": game_id,
        "environment_name": env_key,
        "room_id": game_data.get("room_id"),
        "characters": [_character_to_wire(c) for c in (game_data.get("characters") or {}).values()],
        "item_state": [_item_to_wire(i) for i in game_data.get("item_state") or []],
        "turn_state": game_data.get("turn_state"),
        "synchronizer": game_data.get("synchronizer"),
        "environment": game_data.get("environment"),
        "turn_count": game_data.get("turn_count") or 0,
        "created_at": game_data.get("created_at"),
        "last_update_at": game_data.get("last_update_at"),
    }


@app.get("/api/multiplayer/games")
async def list_games(token: Any = Depends(_verify_token)):
    return {"ok": True, "games": registry.list_games()}


@app.post("/api/multiplayer/match-action")
async def match_action(body: MatchActionRequest, request: Request, token: Any = Depends(_verify_token)):
    client_id = await _get_client_id(request)
    game_id, env_key = _resolve_game_from_model(request, body)
    game_data = registry.handle_match_action(game_id, env_key, body.model_dump(exclude_unset=True), client_id)
    if game_data is None:
        raise HTTPException(status_code=404, detail="Game not found")
    return {"ok": True}


@app.post("/api/multiplayer/turn")
async def turn_action(body: TurnActionRequest, request: Request, token: Any = Depends(_verify_token)):
    client_id = await _get_client_id(request)
    game_id, env_key = _resolve_game_from_model(request, body)
    game_data = registry.handle_turn_action(game_id, env_key, body.model_dump(exclude_unset=True), client_id)
    if game_data is None:
        raise HTTPException(status_code=404, detail="Game not found")
    return {"ok": True, "turn_state": game_data.get("turn_state")}


@app.post("/api/multiplayer/environment")
async def update_environment(body: EnvironmentRequest, request: Request, token: Any = Depends(_verify_token)):
    game_id, env_key = _resolve_game_from_model(request, body)
    game_data = registry.handle_environment(game_id, env_key, body.environment)
    if game_data is None:
        raise HTTPException(status_code=404, detail="Game not found")
    return {"ok": True}


@app.post("/api/multiplayer/reset")
async def reset_game(body: ResetRequest, request: Request, token: Any = Depends(_verify_token)):
    game_id, env_key = _resolve_game_from_model(request, body)
    game_data = registry.reset_game(game_id, env_key, body.reason or "")
    if game_data is None:
        raise HTTPException(status_code=404, detail="Game not found")
    return {"ok": True}


@app.post("/api/multiplayer/config")
async def update_config(body: ConfigUpdateRequest, request: Request, token: Any = Depends(_verify_token)):
    game_id, env_key = _resolve_game_from_model(request, body)
    game_data = registry.update_config(game_id, env_key, body.config or {})
    if game_data is None:
        raise HTTPException(status_code=404, detail="Game not found")
    return {"ok": True, "config": game_data.get("config")}


# ---------------------------------------------------------------------------
# Compatibility aliases — older clients used these paths.
# They forward to the canonical handlers above, same broadcast behaviour.
# ---------------------------------------------------------------------------
@app.post("/api/multiplayer/character-state/batch")
async def update_character_state_batch(body: CharacterStateRequest, request: Request, token: Any = Depends(_verify_token)):
    return await update_character_state(body, request, token)


@app.get("/api/multiplayer/players")
async def list_players_alias(request: Request, token: Any = Depends(_verify_token)):
    return await get_character_state(request, token)


@app.get("/api/multiplayer/items")
async def list_items_alias(request: Request, token: Any = Depends(_verify_token)):
    return await get_item_state(request, token)


