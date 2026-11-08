"""
SCS-MP-SYNC v2 — generalized match-making + multiplayer protocol.

Normative (legacy): https://github.com/EricEisaman/babylon-game-starter/blob/main/MULTIPLAYER_SYNCH.md
Transport: HTTPS + SSE (Datastar)
Encoding: JSON UTF-8

What changed in v2
------------------
The old protocol assumed ONE kind of game: a realtime platformer where
everyone in an `environment_name` is in the same room, the first arrival
is the synchronizer / env authority, and only character + item state is
synced.

v2 keeps that behaviour as the *default* (so existing clients keep
working) but makes the session configurable at connect time:

  * ``GameSessionConfig`` — sent on the initial ``JoinRequest`` — declares
    what this application needs:
      - ``requires_matchmaking`` — queue players instead of dropping them
        straight into a named room.
      - ``requires_turn_based`` — the room is driven by a ``TurnManager``
        (one player to move at a time, deadlines, pass / resign, game-over).
      - ``max_players_per_room`` / ``min_players_per_room`` — room size.
      - ``authority`` — *which* subsystems the server is authoritative
        for (``items``, ``environment``, ``turns``, ``score``, ``custom:*``)
        and which resource types that covers.
      - ``matchmaking`` / ``turn`` — tuning for those subsystems.
      - ``custom_rules`` — opaque, game-specific rules the core never
        interprets. Games that need their own rules pass them here instead
        of forking the protocol.

  * Skill is client-reported *today* but resolved through a
    ``SkillProvider``. Swap ``ClientReportedSkillProvider`` for
    ``DatabaseSkillProvider`` (stub in this file) when the DB lands —
    match-making code never sees where the number came from, only a
    ``SkillRating`` + ``source``. See ``SkillProvider`` below.

  * Turn-based games are *generic*: the core only enforces whose turn it
    is, deadlines, and terminal state. Game rules live in the client
    by default. A game that wants server-side adjudication may
    optionally register a ``TurnValidator`` — no core changes.

  * Rooms are first-class. Legacy ``environment_name`` maps to a direct
    (non-matchmade) room, so old clients get exactly the old semantics.

Backward compatibility
----------------------
All new fields on ``JoinRequest`` / ``JoinResponse`` / ``Capabilities``
are optional with legacy defaults. A v1 client that sends only
``{environment_name, character_name}`` gets:
``requires_matchmaking=False, requires_turn_based=False,
max_players_per_room=8, authority.scopes=["items", "environment"]``.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Literal, Optional, Protocol, Tuple

# ---------------------------------------------------------------------------
# Skill — client-reported today, DB-authoritative tomorrow
# ---------------------------------------------------------------------------

SkillSource = Literal["client_reported", "database", "default"]


@dataclass
class SkillRating:
    """One number the match-maker can compare. Deliberately minimal.

    ``rating`` is on whatever scale the game uses (Elo-like 0-3000 is the
    convention, but any monotonic scale works as long as every client for
    a given ``game_id`` uses the same one). ``deviation`` / ``confidence``
    let a future DB provider express uncertainty without changing the
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
        (never crash match-making on a bad skill payload).
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
    interface only — never on where the number was typed.

    Today: ``ClientReportedSkillProvider`` trusts ``JoinRequest.skill``.
    Tomorrow: ``DatabaseSkillProvider`` looks the player up by
    ``player_id`` + ``game_id`` and returns the DB rating, ignoring (or
    cross-checking) the client value. Swapping providers must not touch
    ``Matchmaker`` or any route.
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
    """FUTURE — DB-backed authority for skill. Not wired up yet.

    Intended shape (do not invent a schema here — agree it with the DB
    owner first, see the ideation notes in the reply):

        async def resolve_skill(...):
            row = await db.fetchone(
                "SELECT rating, deviation, games_played FROM player_skill "
                "WHERE player_id = ? AND game_id = ?", player_id, game_id)
            if row is None: return SkillRating.default(...)
            return SkillRating(rating=row.rating, ..., source="database", ...)

    Design decisions already baked in so the swap is mechanical:
      * the provider is chosen in ONE place (``main.py: get_skill_provider``);
      * if the DB is unreachable the provider should fall back to the
        client-reported value and mark ``source="client_reported"`` rather
        than failing the join — match-making degrades, it does not go down;
      * the DB value, when present, always wins over the client value.
    """

    def __init__(self, db: Any = None):
        self._db = db

    async def resolve_skill(self, *, player_id, game_id, client_reported):
        raise NotImplementedError(
            "DatabaseSkillProvider is a stub: wire it to the real DB, then "
            "swap it in as the provider in main.py. See class docstring."
        )


# ---------------------------------------------------------------------------
# Session configuration — what the application declares on connect
# ---------------------------------------------------------------------------

AuthorityScope = Literal["items", "environment", "character_state", "turns", "score", "custom"]


@dataclass
class MatchmakingConfig:
    """Tuning for the queue. Only read when ``requires_matchmaking``."""

    enabled: bool = False
    # Players start matchable if |rating gap| <= this, then the window
    # widens by ``gap_widen_per_sec`` for each second waited (so nobody
    # waits forever because they are an outlier).
    max_initial_skill_gap: float = 100.0
    gap_widen_per_sec: float = 5.0
    max_skill_gap: float = 500.0
    max_wait_ms: int = 60_000  # -> matchmaking-timeout signal, client retries
    # Optional grouping keys. Players only match within the same bucket.
    game_mode: str = "default"  # e.g. "ranked", "casual", "blitz"
    region: Optional[str] = None
    allow_bots: bool = False  # future: fill a room with bots, not built yet

    def tolerance_for_wait_ms(self, wait_ms: int) -> float:
        return min(
            self.max_skill_gap,
            self.max_initial_skill_gap + self.gap_widen_per_sec * (wait_ms / 1000.0),
        )


@dataclass
class TurnConfig:
    """Tuning for turn-based rooms. Only read when ``requires_turn_based``."""

    enabled: bool = False
    turn_timeout_ms: Optional[int] = 30_000  # None == no deadline
    timeout_action: Literal["pass", "forfeit", "none"] = "pass"
    max_turns: Optional[int] = None  # None == unlimited (e.g. chess-like)
    allow_pass: bool = True
    allow_resign: bool = True
    # "join_order" | "random" | "skill_order" (lowest rating moves first)
    turn_order_strategy: str = "join_order"

    def validate(self) -> None:
        if self.turn_timeout_ms is not None and self.turn_timeout_ms <= 0:
            raise ValueError("turn.turn_timeout_ms must be > 0 or null")
        if self.timeout_action not in ("pass", "forfeit", "none"):
            raise ValueError("turn.timeout_action must be pass/forfeit/none")


@dataclass
class AuthorityConfig:
    """Which subsystems the server is authoritative for, for THIS session.

    This is the answer to "what items are to be handled by the authority
    system" — asked per-application, per-connect, not hard-coded.

    ``scopes``: subsystems the server enforces (claims / turns / scores
    outside the scope are relayed, never adjudicated).
    ``resource_types``: within those scopes, which resource kinds the
    authority covers, e.g. ``["item", "character", "score"]`` or a
    game-specific ``["custom:power_up"]``. ``["*"]`` == everything.
    """

    scopes: List[str] = field(default_factory=lambda: ["items", "environment"])
    resource_types: List[str] = field(default_factory=lambda: ["*"])
    claim_idle_timeout_ms: int = 10_000
    # If False, clients may not claim authority at all — the server (or
    # the turn manager) assigns it.
    allow_client_claims: bool = True

    def covers(self, scope: str, resource_type: str = "*") -> bool:
        if scope not in self.scopes:
            return False
        return "*" in self.resource_types or resource_type in self.resource_types


@dataclass
class GameSessionConfig:
    """Everything an application declares when it first connects.

    This is the single generalization point: a platformer, a card game,
    and a turn-based board game all speak the same join, they just send
    different configs.
    """

    game_id: str = "default"  # == legacy environment_name when unset
    requires_matchmaking: bool = False
    requires_turn_based: bool = False
    max_players_per_room: int = 8
    min_players_per_room: int = 1
    matchmaking: MatchmakingConfig = field(default_factory=MatchmakingConfig)
    turn: TurnConfig = field(default_factory=TurnConfig)
    authority: AuthorityConfig = field(default_factory=AuthorityConfig)
    # Public rooms can be discovered / matchmade; private rooms need a code.
    visibility: Literal["public", "private"] = "public"
    room_code: Optional[str] = None  # join key for private rooms
    # Opaque, game-specific rules. Core code MUST NOT branch on these —
    # validators / game adapters read them.
    custom_rules: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        if not self.game_id:
            raise ValueError("config.game_id must be non-empty")
        if not 1 <= self.min_players_per_room <= self.max_players_per_room <= 64:
            raise ValueError("need 1 <= min_players_per_room <= max_players_per_room <= 64")
        if self.requires_turn_based and self.max_players_per_room < 1:
            raise ValueError("turn-based rooms need max_players_per_room >= 1")
        self.turn.validate()
        if self.requires_matchmaking:
            self.matchmaking.enabled = True

    def fingerprint(self) -> str:
        """Match-making bucket: players only match if fingerprints agree.

        Skill, region, and game_mode are part of the bucket; skill *value*
        is not (that is the thing we match *on*, not *by*).
        """
        import json

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
        }, sort_keys=True, default=str)

    @classmethod
    def from_legacy(cls, environment_name: str) -> "GameSessionConfig":
        """v1 client: environment == game, no match-making, no turns."""
        return cls(game_id=environment_name or "default")

    @classmethod
    def from_payload(cls, payload: Any) -> "GameSessionConfig":
        """Parse the ``config`` object from a JoinRequest body.

        Accepts the flat aliases clients actually send
        (``maxPlayersPerRoom``) as well as the nested shape.
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
                max_wait_ms=int(matchmaking_raw.get("max_wait_ms", 60_000)),
                game_mode=str(matchmaking_raw.get("game_mode", "default")),
                region=matchmaking_raw.get("region"),
                allow_bots=bool(matchmaking_raw.get("allow_bots", False)),
            ),
            turn=TurnConfig(
                enabled=bool(requires_turn),
                turn_timeout_ms=turn_raw.get("turn_timeout_ms",
                                             turn_raw.get("turnTimeoutMs", 30_000)),
                timeout_action=str(turn_raw.get("timeout_action", "pass")),
                max_turns=turn_raw.get("max_turns"),
                allow_pass=bool(turn_raw.get("allow_pass", True)),
                allow_resign=bool(turn_raw.get("allow_resign", True)),
                turn_order_strategy=str(turn_raw.get("turn_order_strategy", "join_order")),
            ),
            authority=AuthorityConfig(
                scopes=[str(s) for s in scopes],
                resource_types=[str(r) for r in authority_raw.get(
                    "resource_types", authority_raw.get("resourceTypes", ["*"]))],
                claim_idle_timeout_ms=int(authority_raw.get(
                    "claim_idle_timeout_ms", authority_raw.get("claimIdleTimeoutMs", 10_000))),
                allow_client_claims=bool(authority_raw.get("allow_client_claims", True)),
            ),
            visibility=str(_pop("visibility", default="public")),
            room_code=_pop("room_code", "roomCode", default=None),
            custom_rules=dict(_pop("custom_rules", "customRules", default={}) or {}),
            metadata=dict(_pop("metadata", default={}) or {}),
        )
        if raw:  # unknown top-level keys: keep, don't crash — forward-compat
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
    # Legacy fields (v1 clients send only these two).
    environment_name: str = "default"
    character_name: str = "player"
    # v2 fields.
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
            self.config = GameSessionConfig.from_legacy(self.game_id)
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
    # v2 fields (all optional so v1 parsing keeps working).
    room_id: Optional[str] = None
    game_id: str = "default"
    match_status: Literal["ready", "queued", "waiting_for_players"] = "ready"
    queue_position: Optional[int] = None
    seat: Optional[int] = None  # index into the room's player list
    player_index: Optional[int] = None  # alias of seat
    turn_order: List[str] = field(default_factory=list)
    skill_source: SkillSource = "client_reported"
    config: Optional[GameSessionConfig] = None


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


@dataclass
class MatchmakingTimeoutSignal:
    eventType: Literal["matchmaking_timeout"] = "matchmaking_timeout"
    clientId: str = ""
    gameId: str = ""
    reason: str = "no_match_within_max_wait"
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
    turn_deadline_ms: Optional[int] = None
    state: Dict[str, Any] = field(default_factory=dict)  # game-specific
    last_action: Optional[Dict[str, Any]] = None
    winner_id: Optional[str] = None
    is_draw: bool = False
    end_reason: Optional[str] = None  # "win" | "draw" | "resign" | "timeout" | "max_turns"
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))

    def is_players_turn(self, client_id: str) -> bool:
        return self.phase == "in_progress" and self.current_player_id == client_id

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TurnAction:
    """One move, from one player, in one room. ``payload`` is opaque to
    the core — e.g. ``{"cell": 4}`` for one game, ``{"from": "e2", "to": "e4"}``
    for another. The core never interprets it.

    Two ways to run a game, both supported:

    1. Client-authoritative (default, scales to 1000s of games):
       the mover also sends ``proposed_state`` (the full game state after
       its move) and, if the game ended, ``claimed_winner`` /
       ``claimed_is_draw`` / ``claimed_end_reason``. The server checks
       only turn / phase / deadline, accepts the proposed state as
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
    """OPTIONAL server-side game rules. Most games never implement this.

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
# Deliberately: there is no game-specific state type in this file.
# Game state is an opaque ``Dict[str, Any]`` carried in
# ``TurnState.state`` / ``TurnAction.proposed_state``. The server does
# not know, and must not know, what any game's state means. The only
# game-specific code that may ever exist is (a) an optional
# ``TurnValidator`` for a game that wants server-side adjudication, and
# (b) demo clients, which live outside the API entirely.


# ---------- Character state (per-player, legacy — unchanged) ----------


@dataclass
class CharacterState:
    """Authoritative per-player state, owned by client_id. Mirrors BGS CharacterState but 2D."""
    clientId: str
    characterModelId: str = "platformer-default"
    position: List[float] = field(default_factory=lambda: [0.0, 0.0])  # [x, y]
    velocity: List[float] = field(default_factory=lambda: [0.0, 0.0])  # [vx, vy]
    animationState: str = "idle"  # idle, run, jump, fall
    animationFrame: float = 0.0
    isJumping: bool = False
    facing: int = 1  # -1 left, 1 right
    score: int = 0
    onGround: bool = False
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]):
        if not isinstance(payload, dict):
            raise ValueError("character update must be an object")
        allowed = set(cls.__dataclass_fields__)
        unknown = set(payload) - allowed
        if unknown:
            raise ValueError("unsupported character fields: " + ", ".join(sorted(unknown)))
        if "metadata" in payload and not isinstance(payload["metadata"], dict):
            raise ValueError("metadata must be an object")
        if not isinstance(payload.get("clientId"), str) or not payload["clientId"]:
            raise ValueError("clientId must be a non-empty string")
        values = dict(payload)
        for name in ("position", "velocity"):
            vector = values.get(name, [0.0, 0.0])
            if not isinstance(vector, (list, tuple)) or len(vector) != 2:
                raise ValueError(name + " must contain exactly two numbers")
            if any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v)
                   for v in vector):
                raise ValueError(name + " must contain finite numbers")
            values[name] = [float(v) for v in vector]
        if "facing" in values and (isinstance(values["facing"], bool) or not isinstance(values["facing"], int)
                                   or values["facing"] not in (-1, 1)):
            raise ValueError("facing must be -1 or 1")
        if "score" in values and (isinstance(values["score"], bool) or not isinstance(values["score"], int)):
            raise ValueError("score must be an integer")
        for name in ("isJumping", "onGround"):
            if name in values and not isinstance(values[name], bool):
                raise ValueError(name + " must be a boolean")
        for name in ("characterModelId", "animationState"):
            if name in values and not isinstance(values[name], str):
                raise ValueError(name + " must be a string")
        if "animationFrame" in values:
            frame = values["animationFrame"]
            if isinstance(frame, bool) or not isinstance(frame, (int, float)) or not math.isfinite(frame):
                raise ValueError("animationFrame must be a finite number")
            values["animationFrame"] = float(frame)
        if "timestamp" in values and (isinstance(values["timestamp"], bool) or not isinstance(values["timestamp"], int)):
            raise ValueError("timestamp must be an integer")
        return cls(**values)

    def to_dict(self):
        payload = asdict(self)
        if not self.metadata:
            payload.pop("metadata")
        return payload


@dataclass
class CharacterStateUpdate:
    updates: List[CharacterState]
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))


@dataclass
class ItemInstanceState:
    """Pose-only transport per BGS Invariant P. pos [x,y], rot quaternion [x,y,z,w]."""
    instanceId: str
    itemName: str = ""
    pos: List[float] = field(default_factory=lambda: [0.0, 0.0])  # world pos [x, y]
    rot: List[float] = field(default_factory=lambda: [0.0, 0.0, 0.0, 1.0])  # quaternion
    isCollected: bool = False
    collectedByClientId: Optional[str] = None
    ownerClientId: Optional[str] = None
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
    reason: Literal["claim", "release", "disconnect", "idle_timeout", "env_switch"]
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
    protocolVersion: int = 2
    transport: str = "datastar-sse"
    features: Dict[str, bool] = field(default_factory=lambda: {
        "characterState": True, "itemState": True, "itemAuthority": True,
        "envAuthority": True, "baseSynchronizer": True,
        "matchmaking": True, "turnBased": True,
        "skillProviders": True, "rooms": True, "customGames": True,
        "websocket": False, "sse": True,
    })
    # Defaults only — each room's GameSessionConfig overrides these.
    limits: Dict[str, int] = field(default_factory=lambda: {
        "maxClients": 16, "maxPlayersPerRoom": 8, "tickHz": 20,
        "snapshotHz": 15, "claimIdleTimeoutMs": 10000,
        "maxSkillGap": 500, "defaultTurnTimeoutMs": 30000,
    })
    skill_sources: List[str] = field(default_factory=lambda: ["client_reported"])
    # ^ becomes ["database"] when DatabaseSkillProvider is wired in.
