"""Match-making + multiplayer protocol.

Transport: HTTPS + SSE (Datastar)
Encoding: JSON UTF-8

Protocol version: 1 (alpha). The version stays at 1 until the project
leaves alpha. Breaking changes during alpha are noted in the changelog,
not signalled by a version bump. Clients must check
``Capabilities.features["characterSchema"]``, not the version number.

Character state is schema-declared. The only universal top-level
character field is ``clientId``. Every other field is declared in
``GameSessionConfig.character_schema`` by the first connecting client
for a given ``game_id``, stored once per ``game_id`` (same schema for
all rooms of that game), kept with the game data through
``empty_game_ttl_sec``, and enforced for every later join. A field may
be declared ``authority: "server"``; clients cannot set it, only
server code can.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Literal, Optional, Protocol, Tuple

# ---------------------------------------------------------------------------
# Skill
# ---------------------------------------------------------------------------

SkillSource = Literal["client_reported", "database", "default"]


@dataclass
class SkillRating:
    """One number the match-maker can compare.

    ``rating`` is on whatever scale the game uses (Elo-like 0-3000 is the
    convention, but any monotonic scale works as long as every client for
    a given ``game_id`` uses the same one). ``deviation`` / ``confidence``
    let a DB provider express uncertainty without changing the
    match-maker: a high-deviation player matches more loosely.
    """

    rating: float = 1000.0
    deviation: float = 350.0  # high == unknown / uncalibrated
    confidence: float = 0.0  # 0.0 (guess) .. 1.0 (many rated games)
    source: SkillSource = "client_reported"
    player_id: str = ""
    game_id: str = ""
    updated_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))

    def validate(self) -> None:
        if isinstance(self.rating, bool) or not isinstance(self.rating, (int, float)):
            raise ValueError("skill.rating must be a number")
        if not math.isfinite(self.rating) or not 0 <= self.rating <= 10000:
            raise ValueError("skill.rating must be in [0, 10000]")
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError("skill.confidence must be in [0, 1]")

    @classmethod
    def from_client(cls, payload: Any, *, player_id: str = "", game_id: str = "") -> "SkillRating":
        """Parse the client-reported shape. Accepts a bare number or an object.

        ``{"skill": 1250}`` / ``{"skill": {"rating": 1250}}`` /
        ``{"skill": {"rating": 1250, "deviation": 80}}`` are all accepted.
        Anything unparseable -> default rating, source still client_reported
        (keep match-making running on a bad skill payload).
        """
        if isinstance(payload, (int, float)) and not isinstance(payload, bool):
            out = cls(rating=float(payload), source="client_reported",
                      player_id=player_id, game_id=game_id)
        elif isinstance(payload, dict):
            rating = payload.get("rating", payload.get("elo", payload.get("mmr", 1000.0)))
            try:
                rating = float(rating)
            except (TypeError, ValueError):
                rating = 1000.0
            out = cls(
                rating=rating,
                deviation=float(payload.get("deviation", 350.0)),
                confidence=float(payload.get("confidence", 0.0)),
                source="client_reported", player_id=player_id, game_id=game_id,
            )
        elif payload is None:
            out = cls(source="client_reported", player_id=player_id, game_id=game_id)
        else:
            raise ValueError("skill must be a number or an object")
        out.validate()
        return out

    @classmethod
    def default(cls, *, player_id: str = "", game_id: str = "") -> "SkillRating":
        return cls(source="default", player_id=player_id, game_id=game_id)


class SkillProvider(Protocol):
    """The seam where skill comes from. Match-making depends on this
    interface only.

    ``ClientReportedSkillProvider`` trusts ``JoinRequest.skill``.
    ``DatabaseSkillProvider`` looks the player up by
    ``player_id`` + ``game_id`` and returns the DB rating, ignoring (or
    cross-checking) the client value.
    """

    async def resolve_skill(
        self, *, player_id: str, game_id: str, client_reported: Optional[SkillRating]
    ) -> SkillRating:
        ...


class ClientReportedSkillProvider:
    """Current behaviour: the client sends its skill level on join."""

    async def resolve_skill(self, *, player_id, game_id, client_reported):
        if client_reported is not None:
            client_reported.player_id = player_id or client_reported.player_id
            client_reported.game_id = game_id or client_reported.game_id
            return client_reported
        return SkillRating.default(player_id=player_id, game_id=game_id)


class DatabaseSkillProvider:
    """DB-backed authority for skill.

    Intended shape:

        async def resolve_skill(...):
            row = await db.fetchone(
                "SELECT rating, deviation, games_played FROM player_skill "
                "WHERE player_id = ? AND game_id = ?", player_id, game_id)
            if row is None: return SkillRating.default(...)
            return SkillRating(rating=row.rating, ..., source="database", ...)

    Design decisions:
      * the provider is chosen in ONE place (``main.py: get_skill_provider``);
      * if the DB is unreachable the provider falls back to the
        client-reported value and marks ``source="client_reported"``;
      * the DB value, when present, wins over the client value.
    """

    def __init__(self, db: Any = None):
        self._db = db

    async def resolve_skill(self, *, player_id, game_id, client_reported):
        raise NotImplementedError("DatabaseSkillProvider is a stub.")


# ---------------------------------------------------------------------------
# Session configuration — what the application declares on connect
# ---------------------------------------------------------------------------

AuthorityScope = Literal["items", "environment", "character_state", "turns", "score", "custom"]


@dataclass
class MatchmakingConfig:
    """Tuning for the queue. Only read when ``requires_matchmaking``."""

    enabled: bool = False
    # Players start matchable if |rating gap| <= this, then the window
    # widens by ``gap_widen_per_sec`` for each second waited.
    max_initial_skill_gap: float = 100.0
    gap_widen_per_sec: float = 5.0
    max_skill_gap: float = 500.0
    # Optional grouping keys. Players only match within the same bucket.
    game_mode: str = "default"  # e.g. "ranked", "casual", "blitz"
    region: Optional[str] = None
    allow_bots: bool = False

    def tolerance_for_wait_ms(self, wait_ms: int) -> float:
        return min(
            self.max_skill_gap,
            self.max_initial_skill_gap + self.gap_widen_per_sec * (wait_ms / 1000.0),
        )


@dataclass
class TurnConfig:
    """Tuning for turn-based rooms. Only read when ``requires_turn_based``."""

    enabled: bool = False
    max_turns: Optional[int] = None  # None == unlimited (e.g. chess-like)
    allow_pass: bool = True
    allow_resign: bool = True
    # "join_order" | "random" | "skill_order" (lowest rating moves first)
    turn_order_strategy: str = "join_order"

    def validate(self) -> None:
        pass


@dataclass
class AuthorityConfig:
    """Which subsystems the server is authoritative for, for THIS session.

    This is the answer to "what items are to be handled by the authority
    system" — asked per-application, per-connect, not hard-coded.

    ``scopes``: subsystems the server enforces (claims / turns / scores
    outside the scope are relayed).
    ``resource_types``: within those scopes, which resource kinds the
    authority covers, e.g. ``["item", "character", "score"]`` or a
    game-specific ``["custom:power_up"]``. ``["*"]`` == everything.
    """

    scopes: List[str] = field(default_factory=lambda: ["items", "environment"])
    resource_types: List[str] = field(default_factory=lambda: ["*"])
    # If False, clients may not claim authority at all — the server (or
    # the turn manager) assigns it.
    allow_client_claims: bool = True

    def covers(self, scope: str, resource_type: str = "*") -> bool:
        if scope not in self.scopes:
            return False
        return "*" in self.resource_types or resource_type in self.resource_types


# ---------------------------------------------------------------------------
# Character schema — what a game's per-player state looks like
# ---------------------------------------------------------------------------
#
# There are no built-in character fields. The only universal top-level
# character field is ``clientId``. Everything else is declared here, by
# the first connecting client for a given ``game_id``, and stored once
# per ``game_id`` in the registry (same schema for every room of that
# game, including private rooms). A later join that declares a different
# schema for the same ``game_id`` is rejected with 409 plus the stored
# schema; it does not create a second bucket.
#
# The schema is kept with the game data: it survives the last client
# leaving, and it is deleted when game data is deleted after
# ``empty_game_ttl_sec``. To change a schema, use a new ``game_id`` or
# wait for the TTL to clear the old game data.
#
# Example — top-down demo:
#   {"version": 1, "fields": {
#     "position": {"type": "vec2", "required": True, "authority": "client",
#                  "semantic": "position"},
#     "health": {"type": "int", "min": 0, "max": 100, "authority": "client",
#                "semantic": "health"},
#     "score": {"type": "int", "min": 0, "authority": "server",
#               "semantic": "score"}}}
# Example — tic-tac-toe: {"version": 1, "fields": {}} (no character state).
#
# ``semantic`` is a hint for generic tools (lobby, spectator, debug
# view). It is never required. A generic tool looks for
# ``semantic: "position"`` or ``semantic: "health"`` when present and
# renders nothing when absent. It must not be confused with a required
# top-level field.

MAX_CHARACTER_SCHEMA_FIELDS = 64
MAX_CHARACTER_STATE_KEYS = 64
MAX_CHARACTER_STATE_BYTES = 8192

CharacterFieldType = Literal["string", "int", "number", "bool", "enum", "vec2", "vec3"]
CharacterFieldAuthority = Literal["client", "server"]
_CHARACTER_FIELD_TYPES = ("string", "int", "number", "bool", "enum", "vec2", "vec3")
_RESERVED_CHARACTER_FIELDS = ("clientId", "state", "timestamp", "source")


@dataclass
class CharacterField:
    """One declared character field.

    ``default`` is None when the field has no default. None is never a
    valid value for any field type, so None unambiguously means
    "no default". ``min`` / ``max`` are numeric bounds for ``int`` /
    ``number``, per-component bounds for ``vec2`` / ``vec3``, and
    length bounds for ``string``. They are not meaningful for ``bool``
    or ``enum``. ``values`` is only allowed for ``enum``.
    """

    type: str = "number"
    required: bool = False
    default: Any = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    values: Optional[List[Any]] = None
    authority: CharacterFieldAuthority = "client"
    semantic: Optional[str] = None

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if self.type not in _CHARACTER_FIELD_TYPES:
            raise ValueError(f"character field type must be one of {list(_CHARACTER_FIELD_TYPES)}")
        if not isinstance(self.required, bool):
            raise ValueError("character field required must be a boolean")
        if self.authority not in ("client", "server"):
            raise ValueError('character field authority must be "client" or "server"')
        if self.semantic is not None and (not isinstance(self.semantic, str) or not self.semantic):
            raise ValueError("character field semantic must be a non-empty string")
        if self.min_value is not None and self.max_value is not None and self.min_value > self.max_value:
            raise ValueError("character field min must be <= max")
        if self.type == "enum":
            if not isinstance(self.values, list) or not self.values:
                raise ValueError("character field of type enum must declare non-empty values")
            for v in self.values:
                if isinstance(v, bool) or not isinstance(v, (str, int, float)):
                    raise ValueError("character field enum values must be strings or numbers")
                if isinstance(v, float) and not math.isfinite(v):
                    raise ValueError("character field enum values must be finite")
        else:
            if self.values is not None:
                raise ValueError("character field values is only allowed for type enum")
            if self.type in ("bool", "enum") and (self.min_value is not None or self.max_value is not None):
                raise ValueError("character field min/max are not meaningful for bool or enum")
        if self.default is not None:
            self.validate_value(self.default, field_name="<default>")

    def validate_value(self, value: Any, *, field_name: str = "") -> Any:
        """Validate one value, returning the normalized form.

        Raises ValueError on any mismatch. Normalization: ``number``
        becomes float, ``vec2`` / ``vec3`` become lists of floats.
        Everything else is returned as given (enum values are not
        coerced, so 1 and 1.0 stay distinct only if the schema says so
        through its declared values; in practice declare one form).
        """
        label = f"character field {field_name!r}" if field_name else "character field"
        if value is None:
            raise ValueError(f"{label} must not be null")
        if self.type == "string":
            if not isinstance(value, str):
                raise ValueError(f"{label} must be a string")
            if self.min_value is not None and len(value) < self.min_value:
                raise ValueError(f"{label} is shorter than min length {self.min_value}")
            if self.max_value is not None and len(value) > self.max_value:
                raise ValueError(f"{label} is longer than max length {self.max_value}")
            return value
        if self.type == "int":
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError(f"{label} must be an integer")
            if self.min_value is not None and value < self.min_value:
                raise ValueError(f"{label} must be >= {self.min_value}")
            if self.max_value is not None and value > self.max_value:
                raise ValueError(f"{label} must be <= {self.max_value}")
            return value
        if self.type == "number":
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError(f"{label} must be a finite number")
            out = float(value)
            if self.min_value is not None and out < self.min_value:
                raise ValueError(f"{label} must be >= {self.min_value}")
            if self.max_value is not None and out > self.max_value:
                raise ValueError(f"{label} must be <= {self.max_value}")
            return out
        if self.type == "bool":
            if not isinstance(value, bool):
                raise ValueError(f"{label} must be a boolean")
            return value
        if self.type == "enum":
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError(f"{label} must be a finite enum value")
            if value not in (self.values or []):
                raise ValueError(f"{label} must be one of {self.values}")
            return value
        if self.type in ("vec2", "vec3"):
            want = 2 if self.type == "vec2" else 3
            if not isinstance(value, (list, tuple)) or len(value) != want:
                raise ValueError(f"{label} must contain exactly {want} numbers")
            out = []
            for v in value:
                if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
                    raise ValueError(f"{label} must contain finite numbers")
                f = float(v)
                if self.min_value is not None and f < self.min_value:
                    raise ValueError(f"{label} components must be >= {self.min_value}")
                if self.max_value is not None and f > self.max_value:
                    raise ValueError(f"{label} components must be <= {self.max_value}")
                out.append(f)
            return out
        raise ValueError(f"{label} has unknown type {self.type!r}")

    @classmethod
    def from_payload(cls, payload: Any) -> "CharacterField":
        if not isinstance(payload, dict):
            raise ValueError("character field must be an object")
        raw = dict(payload)
        if "type" not in raw:
            raise ValueError("character field must declare type")
        min_value = raw.get("min", raw.get("min_value", raw.get("minimum")))
        max_value = raw.get("max", raw.get("max_value", raw.get("maximum")))
        authority = raw.get("authority", "client")
        if "server_only" in raw:
            authority = "server" if bool(raw["server_only"]) else "client"
        values = raw.get("values", raw.get("enum_values"))
        if values is not None and not isinstance(values, list):
            raise ValueError("character field values must be a list")
        unknown = set(raw) - {
            "type", "required", "default", "min", "min_value", "minimum",
            "max", "max_value", "maximum", "values", "enum_values",
            "authority", "server_only", "semantic",
        }
        if unknown:
            raise ValueError("unsupported character field keys: " + ", ".join(sorted(unknown)))
        return cls(
            type=str(raw["type"]),
            required=bool(raw.get("required", False)),
            default=raw.get("default"),
            min_value=None if min_value is None else float(min_value),
            max_value=None if max_value is None else float(max_value),
            values=list(values) if values is not None else None,
            authority=authority,  # type: ignore[arg-type]
            semantic=raw.get("semantic"),
        )

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"type": self.type, "required": bool(self.required),
                               "authority": self.authority}
        if self.default is not None:
            out["default"] = self.default
        if self.min_value is not None:
            out["min"] = self.min_value
        if self.max_value is not None:
            out["max"] = self.max_value
        if self.values is not None:
            out["values"] = list(self.values)
        if self.semantic is not None:
            out["semantic"] = self.semantic
        return out


@dataclass
class CharacterSchema:
    """The declared shape of per-player state for one ``game_id``.

    ``version`` is part of the schema identity: same fields but a
    different version is a mismatch (409). To change a schema, use a
    new ``game_id`` or wait for the old game data to expire.
    ``allow_unknown`` defaults to False (closed schema). A game may
    opt into an open schema, but then unknown fields are relayed
    unvalidated, so this is for prototyping only.
    """

    version: int = 1
    fields: Dict[str, CharacterField] = field(default_factory=dict)
    allow_unknown: bool = False

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if isinstance(self.version, bool) or not isinstance(self.version, int) or self.version < 1:
            raise ValueError("character_schema.version must be an integer >= 1")
        if not isinstance(self.fields, dict):
            raise ValueError("character_schema.fields must be an object")
        if len(self.fields) > MAX_CHARACTER_SCHEMA_FIELDS:
            raise ValueError(f"character_schema has too many fields (max {MAX_CHARACTER_SCHEMA_FIELDS})")
        if not isinstance(self.allow_unknown, bool):
            raise ValueError("character_schema.allow_unknown must be a boolean")
        for name, f in self.fields.items():
            if not isinstance(name, str) or not name:
                raise ValueError("character_schema field names must be non-empty strings")
            if name in _RESERVED_CHARACTER_FIELDS:
                raise ValueError(f"character_schema field name {name!r} is reserved")
            if not isinstance(f, CharacterField):
                raise ValueError(f"character_schema field {name!r} must be a CharacterField")
            f.validate()

    def is_empty(self) -> bool:
        return not self.fields

    def canonical_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "fields": {name: self.fields[name].to_dict() for name in sorted(self.fields)},
            "allow_unknown": bool(self.allow_unknown),
        }

    def to_dict(self) -> Dict[str, Any]:
        return self.canonical_dict()

    def fingerprint_hash(self) -> str:
        """Canonical hash for GameSessionConfig.fingerprint().

        Sorting and compact separators make the hash independent of
        key order and whitespace in the client's JSON.
        """
        payload = json.dumps(self.canonical_dict(), sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @classmethod
    def from_payload(cls, payload: Any) -> "CharacterSchema":
        if payload is None:
            return cls()
        if isinstance(payload, CharacterSchema):
            return payload
        if not isinstance(payload, dict):
            raise ValueError("character_schema must be an object")
        raw = dict(payload)
        unknown = set(raw) - {"version", "fields", "properties", "allow_unknown", "allowUnknown"}
        if unknown:
            raise ValueError("unsupported character_schema keys: " + ", ".join(sorted(unknown)))
        version = raw.get("version", 1)
        if isinstance(version, bool) or not isinstance(version, int):
            raise ValueError("character_schema.version must be an integer")
        fields_raw = raw.get("fields", raw.get("properties", {}))
        if not isinstance(fields_raw, dict):
            raise ValueError("character_schema.fields must be an object")
        fields = {str(name): CharacterField.from_payload(spec) for name, spec in fields_raw.items()}
        allow_unknown = raw.get("allow_unknown", raw.get("allowUnknown", False))
        return cls(version=version, fields=fields, allow_unknown=bool(allow_unknown))

    def _check_size(self, patch: Dict[str, Any]) -> None:
        if len(patch) > MAX_CHARACTER_STATE_KEYS:
            raise ValueError(f"character state has too many keys (max {MAX_CHARACTER_STATE_KEYS})")
        try:
            size = len(json.dumps(patch, default=str).encode("utf-8"))
        except (TypeError, ValueError):
            raise ValueError("character state must be JSON-encodable")
        if size > MAX_CHARACTER_STATE_BYTES:
            raise ValueError(f"character state too large (max {MAX_CHARACTER_STATE_BYTES} bytes)")

    def validate_client_patch(self, patch: Any) -> Tuple[Dict[str, Any], List[str]]:
        """Validate a client PATCH. Returns (accepted, dropped_server_fields).

        Patches are partial: missing ``required`` fields are NOT an
        error here. Required-ness is enforced on the merged full state
        (see ``validate_full_state``), so a client can send one field
        at a time. Server-authoritative fields are dropped, never
        broadcast. Unknown fields are rejected unless ``allow_unknown``.
        """
        if patch is None:
            return {}, []
        if not isinstance(patch, dict):
            raise ValueError("character state must be an object")
        self._check_size(patch)
        accepted: Dict[str, Any] = {}
        dropped: List[str] = []
        for name, value in patch.items():
            spec = self.fields.get(str(name))
            if spec is None:
                if self.allow_unknown:
                    accepted[str(name)] = value
                    continue
                raise ValueError(f"unknown character field: {name}")
            if spec.authority == "server":
                dropped.append(str(name))
                continue
            accepted[str(name)] = spec.validate_value(value, field_name=str(name))
        return accepted, dropped

    def validate_server_patch(self, patch: Any) -> Dict[str, Any]:
        """Validate a server-side patch. Server code may set any field.

        Used by the registry's internal server-set path only. The
        synchronizer is still a client and must go through
        ``validate_client_patch``.
        """
        if patch is None:
            return {}
        if not isinstance(patch, dict):
            raise ValueError("character state must be an object")
        self._check_size(patch)
        accepted: Dict[str, Any] = {}
        for name, value in patch.items():
            spec = self.fields.get(str(name))
            if spec is None:
                if self.allow_unknown:
                    accepted[str(name)] = value
                    continue
                raise ValueError(f"unknown character field: {name}")
            accepted[str(name)] = spec.validate_value(value, field_name=str(name))
        return accepted

    def apply_defaults(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Return ``state`` with defaults filled for missing fields."""
        out = dict(state)
        for name, spec in self.fields.items():
            if name not in out and spec.default is not None:
                out[name] = spec.validate_value(spec.default, field_name=name)
        return out

    def validate_full_state(self, state: Any) -> Dict[str, Any]:
        """Validate a complete merged state: defaults, types, required.

        The registry calls this after merging a patch into the stored
        full state. It is the only place ``required`` is enforced.
        """
        if not isinstance(state, dict):
            raise ValueError("character state must be an object")
        self._check_size(state)
        out: Dict[str, Any] = {}
        for name, value in state.items():
            spec = self.fields.get(str(name))
            if spec is None:
                if self.allow_unknown:
                    out[str(name)] = value
                    continue
                raise ValueError(f"unknown character field: {name}")
            out[str(name)] = spec.validate_value(value, field_name=str(name))
        out = self.apply_defaults(out)
        missing = [name for name, spec in self.fields.items()
                   if spec.required and name not in out]
        if missing:
            raise ValueError("missing required character fields: " + ", ".join(sorted(missing)))
        return out


@dataclass
class GameSessionConfig:
    """Everything an application declares when it first connects."""

    game_id: str = "default"
    requires_matchmaking: bool = False
    requires_turn_based: bool = False
    max_players_per_room: int = 8
    min_players_per_room: int = 1
    matchmaking: MatchmakingConfig = field(default_factory=MatchmakingConfig)
    turn: TurnConfig = field(default_factory=TurnConfig)
    authority: AuthorityConfig = field(default_factory=AuthorityConfig)
    # Schema-declared character state for this game_id. Empty schema
    # means the game sends no character state (e.g. tic-tac-toe).
    # The first join stores it; later joins must match it exactly.
    character_schema: CharacterSchema = field(default_factory=CharacterSchema)
    # Public rooms can be discovered / matchmade; private rooms need a code.
    visibility: Literal["public", "private"] = "public"
    room_code: Optional[str] = None  # join key for private rooms
    # Opaque, game-specific rules. Validators / game adapters read them.
    custom_rules: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    # --- Disconnection ---
    # Seconds of silence before the server drops the client.
    # The client game declares this per game_id on connect, e.g.
    #   config: { game_id: "top-down-demo", disconnectionTime: 60 }
    # Default 60. 0 == never expire.
    #
    # Enforcement is EVENT-DRIVEN, not a timer: there is no background
    # loop / sleep. On every inbound packet from ANY client the server
    # checks last_seen vs now for every client and evicts expired ones
    # with reason="timeout". A packet is any client -> server HTTP
    # request with X-Client-ID (join, character-state, item-state,
    # claim/release, turn/action, custom-state, switch-environment,
    # heartbeat, stream attach). If nobody sends a packet, an idle
    # client stays listed until the next packet arrives. A late packet
    # from an already-expired client is rejected (403) instead of
    # reviving it.
    disconnection_time_sec: float = 60.0
    # Seconds an empty game keeps its data before lazy deletion.
    # Default 300 (5 min). 0 == keep forever. Deletion is event-driven:
    # checked on the next packet from ANY client, never by a timer.
    # A reconnect within the TTL resumes the same game (same gems,
    # same character schema).
    empty_game_ttl_sec: float = 300.0

    def validate(self) -> None:
        if not self.game_id:
            raise ValueError("config.game_id must be non-empty")
        if not 1 <= self.min_players_per_room <= self.max_players_per_room <= 64:
            raise ValueError("need 1 <= min_players_per_room <= max_players_per_room <= 64")
        if self.requires_turn_based and self.max_players_per_room < 1:
            raise ValueError("turn-based rooms need max_players_per_room >= 1")
        if self.disconnection_time_sec is None:
            self.disconnection_time_sec = 60.0
        try:
            self.disconnection_time_sec = float(self.disconnection_time_sec)
        except (TypeError, ValueError):
            raise ValueError("config.disconnection_time_sec must be a number")
        if not 0 <= self.disconnection_time_sec <= 86400:
            raise ValueError("config.disconnection_time_sec must be in [0, 86400]")
        if self.empty_game_ttl_sec is None:
            self.empty_game_ttl_sec = 300.0
        try:
            self.empty_game_ttl_sec = float(self.empty_game_ttl_sec)
        except (TypeError, ValueError):
            raise ValueError("config.empty_game_ttl_sec must be a number")
        if not 0 <= self.empty_game_ttl_sec <= 86400:
            raise ValueError("config.empty_game_ttl_sec must be in [0, 86400]")
        if self.character_schema is None:
            self.character_schema = CharacterSchema()
        if isinstance(self.character_schema, dict):
            self.character_schema = CharacterSchema.from_payload(self.character_schema)
        self.character_schema.validate()
        self.turn.validate()
        if self.requires_matchmaking:
            self.matchmaking.enabled = True

    def fingerprint(self) -> str:
        """Match-making bucket: players only match if fingerprints agree.

        Skill, region, and game_mode are part of the bucket; skill *value*
        is not (that is the thing we match *on*, not *by*).
        disconnection_time_sec is deliberately NOT part of the bucket:
        enforcement is per-client, so two players with different budgets
        can still match.
        character_schema is part of the bucket via its canonical hash,
        so two clients that declared different schemas never matchmake
        together. The registry additionally enforces one schema per
        game_id (see CharacterSchema): a later join for the same
        game_id with a different schema gets 409, even if its
        fingerprint would otherwise differ.
        """
        return json.dumps({
            "game_id": self.game_id,
            "max": self.max_players_per_room,
            "min": self.min_players_per_room,
            "turn": self.requires_turn_based,
            "authority": sorted(self.authority.scopes),
            "mode": self.matchmaking.game_mode,
            "region": self.matchmaking.region,
            "visibility": self.visibility,
            "custom_rules": self.custom_rules,
            "character_schema": self.character_schema.fingerprint_hash()
            if self.character_schema is not None else CharacterSchema().fingerprint_hash(),
        }, sort_keys=True, default=str)

    @classmethod
    def default(cls, game_id: str = "") -> "GameSessionConfig":
        return cls(game_id=game_id or "default")

    @classmethod
    def from_payload(cls, payload: Any) -> "GameSessionConfig":
        """Parse the ``config`` object from a JoinRequest body.

        Accepts the flat aliases clients actually send
        (``maxPlayersPerRoom``) as well as the nested shape.
        Accepts ``disconnectionTime`` / ``disconnection_time_sec`` /
        ``disconnection_time`` / ``disconnect_timeout_sec`` /
        ``disconnectTimeout`` for the disconnect budget.
        Accepts ``character_schema`` / ``characterSchema`` /
        ``character_state_schema`` / ``characterStateSchema`` for the
        character schema. Omitting it declares an empty schema.
        """
        if payload is None:
            return cls()
        if not isinstance(payload, dict):
            raise ValueError("config must be an object")
        raw = dict(payload)

        def _pop(*names: str, default: Any = None) -> Any:
            for name in names:
                if name in raw:
                    return raw.pop(name)
            return default

        matchmaking_raw = _pop("matchmaking", default={}) or {}
        turn_raw = _pop("turn", "turn_based", "turnBased", default={}) or {}
        authority_raw = _pop("authority", default={}) or {}
        if not isinstance(matchmaking_raw, dict) or not isinstance(turn_raw, dict) \
                or not isinstance(authority_raw, dict):
            raise ValueError("config.matchmaking / turn / authority must be objects")

        requires_mm = _pop("requires_matchmaking", "requiresMatchmaking",
                           "matchmaking_required", "matchmaking", default=False)
        if isinstance(requires_mm, dict):  # client sent matchmaking:{enabled:...} only
            requires_mm = requires_mm.get("enabled", False)
        requires_turn = _pop("requires_turn_based", "requiresTurnBased",
                             "turn_based", "turnBased", default=False)
        if isinstance(requires_turn, dict):
            requires_turn = requires_turn.get("enabled", False)

        # Character schema — top-level config field.
        schema_raw = _pop("character_schema", "characterSchema",
                          "character_state_schema", "characterStateSchema",
                          default=None)
        if schema_raw is None:
            character_schema = CharacterSchema()
        elif isinstance(schema_raw, CharacterSchema):
            character_schema = schema_raw
        elif isinstance(schema_raw, dict):
            character_schema = CharacterSchema.from_payload(schema_raw)
        else:
            raise ValueError("config.character_schema must be an object")

        # Disconnection budget — top-level config field.
        raw_disconnect = _pop(
            "disconnection_time_sec", "disconnectionTime",
            "disconnection_time", "disconnect_timeout_sec",
            "disconnectTimeout", default=60.0,
        )
        # Also accept it nested under matchmaking for clients that put it there.
        if isinstance(matchmaking_raw, dict) and raw_disconnect == 60.0:
            for k in ("disconnection_time_sec", "disconnectionTime", "disconnection_time"):
                if k in matchmaking_raw:
                    raw_disconnect = matchmaking_raw.get(k)
                    break
        try:
            disconnection_time_sec = float(raw_disconnect) if raw_disconnect is not None else 60.0
        except (TypeError, ValueError):
            disconnection_time_sec = 60.0
        raw_empty_ttl = _pop(
            "empty_game_ttl_sec", "emptyGameTtlSec",
            "empty_game_ttl", "emptyGameTtl", default=300.0,
        )
        try:
            empty_game_ttl_sec = float(raw_empty_ttl) if raw_empty_ttl is not None else 300.0
        except (TypeError, ValueError):
            empty_game_ttl_sec = 300.0

        # Authority may arrive as a bare list: authority: ["turns", "score"]
        if isinstance(authority_raw, list):
            authority_raw = {"scopes": authority_raw}
        scopes = authority_raw.get("scopes", authority_raw.get("items", None))
        if scopes is None:
            scopes = ["items", "environment"]
        if isinstance(scopes, str):
            scopes = [scopes]

        cfg = cls(
            game_id=str(_pop("game_id", "gameId", "app_id", "appId",
                             default=raw.get("environment_name", "default")) or "default"),
            requires_matchmaking=bool(requires_mm),
            requires_turn_based=bool(requires_turn),
            max_players_per_room=int(_pop("max_players_per_room", "maxPlayersPerRoom",
                                          default=8)),
            min_players_per_room=int(_pop("min_players_per_room", "minPlayersPerRoom",
                                          default=1)),
            matchmaking=MatchmakingConfig(
                enabled=bool(requires_mm),
                max_initial_skill_gap=float(matchmaking_raw.get("max_initial_skill_gap", 100.0)),
                gap_widen_per_sec=float(matchmaking_raw.get("gap_widen_per_sec", 5.0)),
                max_skill_gap=float(matchmaking_raw.get("max_skill_gap", 500.0)),
                game_mode=str(matchmaking_raw.get("game_mode", "default")),
                region=matchmaking_raw.get("region"),
                allow_bots=bool(matchmaking_raw.get("allow_bots", False)),
            ),
            turn=TurnConfig(
                enabled=bool(requires_turn),
                max_turns=turn_raw.get("max_turns"),
                allow_pass=bool(turn_raw.get("allow_pass", True)),
                allow_resign=bool(turn_raw.get("allow_resign", True)),
                turn_order_strategy=str(turn_raw.get("turn_order_strategy", "join_order")),
            ),
            authority=AuthorityConfig(
                scopes=[str(s) for s in scopes],
                resource_types=[str(r) for r in authority_raw.get(
                    "resource_types", authority_raw.get("resourceTypes", ["*"]))],
                allow_client_claims=bool(authority_raw.get("allow_client_claims", True)),
            ),
            character_schema=character_schema,
            visibility=str(_pop("visibility", default="public")),
            room_code=_pop("room_code", "roomCode", default=None),
            custom_rules=dict(_pop("custom_rules", "customRules", default={}) or {}),
            metadata=dict(_pop("metadata", default={}) or {}),
            disconnection_time_sec=disconnection_time_sec,
            empty_game_ttl_sec=empty_game_ttl_sec,
        )
        if raw:  # unknown top-level keys: keep for forward-compat
            cfg.metadata.setdefault("unknown_config_keys", sorted(raw.keys()))
        cfg.validate()
        return cfg


# Backwards-friendly aliases — one concept, several natural names.
RoomConfig = GameSessionConfig
ConnectionConfig = GameSessionConfig
SessionConfig = GameSessionConfig

# ---------- Session lifecycle ----------


@dataclass
class JoinRequest:
    # Base fields.
    environment_name: str = "default"
    character_name: str = "player"
    # Extended fields.
    game_id: str = ""  # defaults to environment_name in normalize()
    player_id: str = ""  # stable per-player id for skill lookup; "" == anonymous
    display_name: str = ""
    skill: Optional[SkillRating] = None  # client-reported; see SkillProvider
    config: Optional[GameSessionConfig] = None
    room_id: Optional[str] = None  # join this specific room (private / friend)
    room_code: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def normalize(self) -> "JoinRequest":
        if not self.game_id:
            self.game_id = (self.config.game_id if self.config else "") \
                or self.environment_name or "default"
        if self.config is None:
            self.config = GameSessionConfig.default(self.game_id)
        else:
            self.config.game_id = self.game_id
            self.config.validate()
        if not self.display_name:
            self.display_name = self.character_name
        return self


@dataclass
class JoinResponse:
    client_id: str
    session_id: str
    is_synchronizer: bool
    existing_clients: int
    # Optional fields.
    room_id: Optional[str] = None
    game_id: str = "default"
    match_status: Literal["ready", "queued", "waiting_for_players"] = "ready"
    queue_position: Optional[int] = None
    seat: Optional[int] = None  # index into the room's player list
    player_index: Optional[int] = None  # alias of seat
    turn_order: List[str] = field(default_factory=list)
    skill_source: SkillSource = "client_reported"
    config: Optional[GameSessionConfig] = None
    # Effective character schema for this game_id, echoed so a client
    # that declared the wrong schema can fail fast. This is the stored
    # schema, not necessarily the one the client declared: on a 409
    # the registry returns the stored schema instead of a JoinResponse.
    character_schema: Optional[CharacterSchema] = None
    # Echo of the effective disconnect budget for this client.
    disconnection_time_sec: float = 60.0


@dataclass
class ClientJoinedSignal:
    eventType: Literal["joined"] = "joined"
    clientId: str = ""
    environment: str = ""
    character: str = ""
    totalClients: int = 0
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class ClientLeftSignal:
    eventType: Literal["left"] = "left"
    clientId: str = ""
    totalClients: int = 0
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))
    reason: Optional[str] = None  # "leave" | "timeout" | "env_switch" | "disconnect"


@dataclass
class ClientDisconnectedSignal:
    """Sent when the server evicts a client for silence.

    Broadcast as client-left with reason="timeout", plus this shape
    is available for clients that want an explicit disconnect event.
    """

    eventType: Literal["disconnected"] = "disconnected"
    clientId: str = ""
    reason: Literal["timeout"] = "timeout"
    disconnection_time_sec: float = 60.0
    last_seen_ms: int = 0
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class SynchronizerChangedSignal:
    newSynchronizerId: str
    reason: str  # "disconnection", etc.
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))


# ---------- Match-making signals (sent over the existing SSE stream) ----------


@dataclass
class MatchmakingQueuedSignal:
    eventType: Literal["matchmaking_queued"] = "matchmaking_queued"
    clientId: str = ""
    gameId: str = ""
    queuePosition: int = 0
    estimatedWaitMs: Optional[int] = None
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class MatchFoundSignal:
    eventType: Literal["match_found"] = "match_found"
    roomId: str = ""
    gameId: str = ""
    playerIds: List[str] = field(default_factory=list)
    seat: int = 0
    turnOrder: List[str] = field(default_factory=list)
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))


# ---------- Turn-based state management (generic) ----------


@dataclass
class TurnState:
    """The core's view of a turn-based room. ``state`` is opaque to the
    core — only the game's ``TurnValidator`` reads/writes it."""

    room_id: str = ""
    game_id: str = ""
    phase: Literal["waiting", "in_progress", "finished"] = "waiting"
    current_player_id: Optional[str] = None
    turn_number: int = 0
    turn_order: List[str] = field(default_factory=list)
    state: Dict[str, Any] = field(default_factory=dict)  # game-specific
    last_action: Optional[Dict[str, Any]] = None
    winner_id: Optional[str] = None
    is_draw: bool = False
    end_reason: Optional[str] = None  # "win" | "draw" | "resign" | "max_turns"
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))

    def is_players_turn(self, client_id: str) -> bool:
        return self.phase == "in_progress" and self.current_player_id == client_id

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TurnAction:
    """One move, from one player, in one room. ``payload`` is opaque to
    the core — e.g. ``{"cell": 4}`` for one game, ``{"from": "e2", "to": "e4"}``
    for another. The core treats it as opaque.

    Two ways to run a game, both supported:

    1. Client-authoritative (default, scales to 1000s of games):
       the mover also sends ``proposed_state`` (the full game state after
       its move) and, if the game ended, ``claimed_winner`` /
       ``claimed_is_draw`` / ``claimed_end_reason``. The server checks
       only turn / phase, accepts the proposed state as
       canonical, and broadcasts it. Every client recomputes the same
       state from the same action stream.
    2. Server-validated (opt-in, for anti-cheat):
       register a ``TurnValidator`` for the ``game_id``. The server then
       ignores ``proposed_state`` / claimed results and derives the next
       state from the validator instead.
    """

    player_id: str = ""
    action_type: str = "move"  # "move" | "pass" | "resign"
    payload: Dict[str, Any] = field(default_factory=dict)
    # Client-authoritative mode only; ignored when a validator is registered.
    proposed_state: Optional[Dict[str, Any]] = None
    claimed_winner_id: Optional[str] = None
    claimed_is_draw: bool = False
    claimed_end_reason: Optional[str] = None  # "win" | "draw" | ...
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class TurnActionResponse:
    ok: bool = True
    accepted: bool = False
    reason: Optional[str] = None  # "not_your_turn" | "game_finished" | "invalid_move" | ...
    turn_state: Optional[TurnState] = None
    server_timestamp: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class TurnChangedSignal:
    eventType: Literal["turn_changed"] = "turn_changed"
    roomId: str = ""
    turnState: Optional[TurnState] = None
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))


class TurnValidator(Protocol):
    """OPTIONAL server-side game rules.

    The default path is client-authoritative (see TurnAction): the server
    enforces turns and broadcasts moves without understanding them, so a
    new game needs zero server code. Implement this interface only when
    a game needs the server to adjudicate moves (anti-cheat, ranked
    play). Register it with ``register_turn_validator(game_id, validator)``
    in main.py — one line, no core changes.
    """

    def initial_state(self, player_ids: List[str],
                      config: GameSessionConfig) -> Dict[str, Any]:
        """Fresh opaque state for a room that just filled up."""
        ...

    def validate(self, state: Dict[str, Any], player_id: str,
                 action: TurnAction, config: GameSessionConfig
                 ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """Return (accepted, reason, new_state). ``new_state`` is the
        state *after* the move, or None if rejected. Must be pure:
        no I/O, no randomness the server cannot reproduce."""
        ...

    def check_terminal(self, state: Dict[str, Any],
                       config: GameSessionConfig
                       ) -> Tuple[Optional[str], bool, Optional[str]]:
        """Return (winner_id, is_draw, end_reason). (None, False, None)
        means the game continues. ``winner_id`` here is a *player id*;
        validators that work in seats/marks map them via state."""
        ...


# ---------- No game-specific types below this line ----------
# Game state is an opaque ``Dict[str, Any]`` carried in
# ``TurnState.state`` / ``TurnAction.proposed_state``. Character state
# is schema-declared (see CharacterSchema above): the server validates
# it against the game's schema without understanding what any field
# means. The only game-specific code is (a) an optional
# ``TurnValidator`` for a game that wants server-side adjudication, and
# (b) demo clients, which live outside the API entirely.


# ---------- Character state ----------


@dataclass
class CharacterState:
    """Per-player state, owned by client_id.

    ``state`` is a PATCH in flight and a full state at rest. A client
    PATCH may be partial: it can send one field at a time, and
    ``required`` is not enforced on a patch. The registry merges the
    patch into the stored full state, fills defaults, then enforces
    ``required`` on the merged result (see
    ``CharacterSchema.validate_full_state``).

    ``source`` is ``"client"`` for client patches and ``"server"`` for
    patches applied by server code (for example a server-authoritative
    ``score`` set on gem collection). Clients must not set ``source``;
    the server stamps it. The synchronizer is still a client.
    """

    clientId: str
    state: Dict[str, Any] = field(default_factory=dict)
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))
    source: Literal["client", "server"] = "client"

    @classmethod
    def from_payload(
        cls, payload: Dict[str, Any], schema: "CharacterSchema",
        *, source: Literal["client", "server"] = "client",
    ) -> Tuple["CharacterState", List[str]]:
        """Parse one character update against ``schema``.

        Returns (character_state, dropped_server_fields). A client
        patch that includes a server-authoritative field has that
        field dropped, never broadcast. A server patch may set any
        field. Unknown fields are rejected unless the schema opts
        into ``allow_unknown``.

        Legacy shapes are rejected outright: top-level ``position``,
        ``health``, ``velocity``, ``score``, ``metadata``, ``pos``,
        and friends are not fields. Send them inside ``state`` and
        declare them in the schema.

        ``timestamp`` from the client is ignored: the server stamps
        receipt time. ``source`` from the client is ignored: the
        caller decides ``source``.
        """
        if not isinstance(payload, dict):
            raise ValueError("character update must be an object")
        if schema is None:
            raise ValueError("character update requires a CharacterSchema")
        allowed = {"clientId", "state", "timestamp", "source"}
        unknown = set(payload) - allowed
        if unknown:
            legacy = sorted(set(payload) & {
                "position", "health", "velocity", "score", "metadata", "pos",
                "characterModelId", "animationState", "animationFrame",
                "isJumping", "facing", "onGround",
            })
            hint = ""
            if legacy:
                hint = (
                    f" Legacy top-level fields {legacy} are no longer supported: "
                    "declare them in character_schema and send them inside state."
                )
            raise ValueError("unsupported character fields: " + ", ".join(sorted(unknown)) + hint)
        if not isinstance(payload.get("clientId"), str) or not payload["clientId"]:
            raise ValueError("clientId must be a non-empty string")
        state_raw = payload.get("state", {})
        if state_raw is None:
            state_raw = {}
        if not isinstance(state_raw, dict):
            raise ValueError("state must be an object")
        if source == "server":
            accepted = schema.validate_server_patch(state_raw)
            dropped: List[str] = []
        else:
            accepted, dropped = schema.validate_client_patch(state_raw)
        return cls(
            clientId=payload["clientId"],
            state=accepted,
            timestamp=int(time.time() * 1000),
            source=source,
        ), dropped

    def to_dict(self) -> Dict[str, Any]:
        return {
            "clientId": self.clientId,
            "state": dict(self.state),
            "timestamp": self.timestamp,
            "source": self.source,
        }


@dataclass
class CharacterStateUpdate:
    updates: List[CharacterState]
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))

    @classmethod
    def from_payload(
        cls, payload: Any, schema: "CharacterSchema",
        *, source: Literal["client", "server"] = "client",
    ) -> Tuple["CharacterStateUpdate", Dict[str, List[str]]]:
        """Parse a PATCH /character-state body against ``schema``.

        Returns (update, dropped_server_fields_by_client_id). The
        caller (registry) merges each patch into the stored full
        state, fills defaults, enforces ``required``, and broadcasts
        the merged result. The PATCH timestamp is ignored; the server
        stamps receipt time.
        """
        if not isinstance(payload, dict):
            raise ValueError("character-state update must be an object")
        raw_updates = payload.get("updates", [])
        if not isinstance(raw_updates, list):
            raise ValueError("character-state updates must be a list")
        updates: List[CharacterState] = []
        dropped: Dict[str, List[str]] = {}
        for item in raw_updates:
            parsed, item_dropped = CharacterState.from_payload(item, schema, source=source)
            updates.append(parsed)
            if item_dropped:
                dropped[parsed.clientId] = item_dropped
        return cls(updates=updates, timestamp=int(time.time() * 1000)), dropped


@dataclass
class CharacterStateUpdateResponse:
    """PATCH /character-state response.

    ``dropped_server_fields`` maps clientId to the server-authoritative
    field names that were dropped from that client's patch. Dropped
    fields are never broadcast, so the client can debug the drop
    without the room breaking.
    """

    ok: bool = True
    accepted: List[CharacterState] = field(default_factory=list)
    dropped_server_fields: Dict[str, List[str]] = field(default_factory=dict)
    server_timestamp: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class ItemInstanceState:
    """Pose-only transport. pos [x,y], rot quaternion [x,y,z,w]."""
    instanceId: str
    itemName: str = ""
    pos: List[float] = field(default_factory=lambda: [0.0, 0.0])  # world pos [x, y]
    rot: List[float] = field(default_factory=lambda: [0.0, 0.0, 0.0, 1.0])  # quaternion
    isCollected: bool = False
    collectedByClientId: Optional[str] = None
    ownerClientId: Optional[str] = None
    environment: Optional[str] = None  # env this item is bound to; set on first sight
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class ItemCollectionEvent:
    instanceId: str
    collectedByClientId: str
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class ItemStateUpdate:
    updates: List[ItemInstanceState] = field(default_factory=list)
    collections: List[ItemCollectionEvent] = field(default_factory=list)
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))


# ---------- Authority ----------


@dataclass
class ItemAuthorityClaim:
    instanceId: str
    clientPosition: Optional[Dict[str, float]] = None  # {"x":.., "y":.., "z":..}
    reason: Optional[str] = None  # proximity-enter, refresh, etc.
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class ItemAuthorityClaimResponse:
    ok: bool = True
    accepted: bool = False
    instanceId: str = ""
    ownerClientId: Optional[str] = None
    currentOwnerId: Optional[str] = None
    serverTimestamp: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class ItemAuthorityRelease:
    instanceId: str
    reason: Optional[str] = None
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class ItemAuthorityChangedSignal:
    instanceId: str
    previousOwnerId: Optional[str]
    newOwnerId: Optional[str]
    reason: Literal["claim", "release", "disconnect", "transfer", "env_switch"]
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class EnvItemAuthorityChangedSignal:
    environmentName: str
    previousAuthorityId: Optional[str]
    newAuthorityId: Optional[str]
    reason: Literal["arrival", "failover", "env_switch", "disconnect"]
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class AuthorityClaim:
    """Generic form of ItemAuthorityClaim: any resource, any scope.

    ``resource_type`` is what ``AuthorityConfig.resource_types`` gates on
    (``"item"``, ``"score"``, ``"custom:power_up"`` …). Item claims keep
    working — they are ``AuthorityClaim(resource_type="item")``.
    """

    resource_id: str
    resource_type: str = "item"
    scope: str = "items"
    client_position: Optional[Dict[str, float]] = None
    reason: Optional[str] = None
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))

    @classmethod
    def from_item_claim(cls, claim: ItemAuthorityClaim) -> "AuthorityClaim":
        return cls(resource_id=claim.instanceId, resource_type="item", scope="items",
                   client_position=claim.clientPosition, reason=claim.reason,
                   timestamp=claim.timestamp)


# ---------- Global world state (base synchronizer only) ----------


@dataclass
class LightState:
    lightId: str
    isEnabled: bool = True
    intensity: float = 1.0
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))


# ---------- Capabilities ----------


@dataclass
class Capabilities:
    protocolVersion: int = 1
    transport: str = "datastar-sse"
    features: Dict[str, bool] = field(default_factory=lambda: {
        "characterState": True, "characterSchema": True,
        "itemState": True, "itemAuthority": True,
        "envAuthority": True, "synchronizer": True,
        "matchmaking": True, "turnBased": True,
        "skillProviders": True, "rooms": True, "customGames": True,
        "disconnectionTime": True, "heartbeat": True,
        "websocket": False, "sse": True,
    })
    # Defaults only — each room's GameSessionConfig overrides these.
    limits: Dict[str, int] = field(default_factory=lambda: {
        "maxClients": 16, "maxPlayersPerRoom": 8,
        "maxSkillGap": 500, "disconnectionTimeSec": 60,
    })
    skill_sources: List[str] = field(default_factory=lambda: ["client_reported"])
