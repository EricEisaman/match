![MATCH — Open-source multiplayer matchmaking](https://raw.githubusercontent.com/EricEisaman/match/main/docs/hero.png)

# MATCH

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688.svg)](https://fastapi.tiangolo.com/)
[![Transport](https://img.shields.io/badge/Transport-REST%20%2B%20SSE-8A2BE2.svg)](#api-reference)
[![Timeouts](https://img.shields.io/badge/Timeouts-Event--Driven%20%E2%80%94%20No%20Timers-00C853.svg)](#disconnect-time--disconnection_time_sec)
[![Matchmaking](https://img.shields.io/badge/Matchmaking-Skill--Aware%20%2B%20Direct%20Rooms-FF6D00.svg)](#how-matching-works)
[![Modes](https://img.shields.io/badge/Modes-Turn--Based%20%7C%20Realtime-2962FF.svg)](#turn-based-or-not--choosing-a-mode)
[![PRs Welcome](https://img.shields.io/badge/PRs-Welcome-brightgreen.svg)](https://github.com/EricEisaman/match/pulls)
[![Stars](https://img.shields.io/github/stars/EricEisaman/match?style=social)](https://github.com/EricEisaman/match/stargazers)

> **One join shape serves every game.**
>
> MATCH is an open-source multiplayer backend for small-to-medium realtime and turn-based games. Clients `join` once with a `GameSessionConfig`, then send character state, item state, custom state, and turn actions over REST — and receive the whole game over a single Server-Sent Events (SSE) stream. The server is authoritative for rooms, turns, items, and game data from the first join.

No WebSockets to manage. No background timer loop. No per-game server code for the common cases — tic-tac-toe, top-down collectors, lobby games, and turn-based board games all speak the same protocol.

**Live demos in this repo:** `GET /tic-tac-toe` · `GET /top-down` · `GET /demo/{name}`

---

## Table of Contents

- [Why MATCH?](#why-match)
- [Features](#features)
- [Quickstart](#quickstart)
- [How it works — architecture](#how-it-works--architecture)
- [Project structure](#project-structure)
- [API Reference](#api-reference)
- [Client protocol — the 60-second version](#client-protocol--the-60-second-version)
- [How Matching Works](#how-matching-works)
- [Turn-Based or Not — Choosing a Mode](#turn-based-or-not--choosing-a-mode)
- [Disconnect Time — `disconnection_time_sec`](#disconnect-time--disconnection_time_sec)
- [Server-authoritative game data & empty-game cleanup](#server-authoritative-game-data--empty-game-cleanup)
- [Config reference](#config-reference)
- [Demos](#demos)
- [Deployment](#deployment)
- [Security & limits](#security--limits)
- [Contributing](#contributing)
- [License](#license)

---

## Why MATCH?

Most multiplayer tutorials stop at “echo positions to everyone.” Real games need more:

| Real-game need | MATCH answer |
|---|---|
| Find fair opponents | Skill-aware matchmaking with widening tolerance, plus direct / private rooms |
| Turn order without cheating | Server-validated `TurnManager`: only the player whose turn it is can act |
| Shared items without duplication | Server-spawned items, authority-gated updates, exactly-once collections |
| Players who vanish mid-game | Event-driven disconnect eviction + synchronizer / authority failover |
| Late joiners | Full bootstrap snapshot on `GET /stream` — characters, items, room, turn state |
| Idle games leaking memory | Empty-game data deleted lazily after `empty_game_ttl_sec` (default 5 min) |
| One backend, many games | `game_id` + `GameSessionConfig` selects matchmaking, turns, authority, TTLs |

**Design principles**

1. **Server-authoritative.** Clients are renderers + input. The server owns rooms, turns, items, and game data.
2. **Event-driven, never timers.** No `sleep`, no background sweeper. Disconnect and empty-game cleanup run lazily when the *next* packet from *any* client arrives. This makes behaviour deterministic, testable, and cheap on serverless / Render free tiers.
3. **One protocol.** `join` → `stream` → `character-state` / `item-state` / `custom-state` / `turn/action`. If you can `fetch()`, you can multiplayer.
4. **Boring transport.** REST for client → server, SSE for server → client. Works through proxies, load balancers, and browsers with no WebSocket upgrade.

---

## Features

- **Skill-aware matchmaking** — rating + widening tolerance, FIFO fairness, `min_players_per_room` / `max_players_per_room`
- **Direct + private rooms** — `room_id` / `room_code`, `visibility: public | private`
- **Turn-based engine** — server-validated actions, pass / resign, draw / win, opponent-left handling
- **Realtime engine** — character-state broadcast, dirty filtering, freshness tracking
- **Item authority** — env authority + per-item owners, claim / release, exactly-once collections
- **Server-spawned game data** — e.g. top-down gems: 24 live, respawn 1 per collection, same field for every player
- **Disconnect handling** — per-client `disconnection_time_sec`, heartbeat endpoint, `403 disconnected: timeout`, failover
- **Empty-game cleanup** — game data deleted after 5 min empty (lazy, on next packet)
- **Custom state & effects** — opaque per-room `custom` + `effects-state` for game-specific rules
- **Demos** — turn-based tic-tac-toe and realtime top-down, both against the same API
- **Capabilities endpoint** — `GET /api/v1/capabilities` for clients to feature-detect

---

## Quickstart

### 1. Run locally

```bash
git clone https://github.com/EricEisaman/match.git
cd match
pip install -r requirements.txt   # or: pip install fastapi uvicorn
uvicorn server.app.main:app --reload --port 8000
```

Open:

- http://localhost:8000/ — API index
- http://localhost:8000/tic-tac-toe — turn-based demo
- http://localhost:8000/top-down — realtime demo

### 2. Join a game (realtime, no matchmaking)

```bash
curl -s -X POST http://localhost:8000/api/multiplayer/join \
  -H "Content-Type: application/json" \
  -d '{
    "environment_name": "top-down-demo",
    "character_name": "Billy",
    "game_id": "top-down-demo",
    "player_id": "demo-billy",
    "skill": {"rating": 1000},
    "config": {
      "game_id": "top-down-demo",
      "requires_matchmaking": false,
      "requires_turn_based": false,
      "max_players_per_room": 8,
      "min_players_per_room": 1,
      "disconnection_time_sec": 60,
      "empty_game_ttl_sec": 300
    }
  }'
```

Response (abridged):

```json
{
  "client_id": "a1b2c3d4",
  "session_id": "9f8e7d6c5b4a3921",
  "is_synchronizer": true,
  "room_id": "3224f87452ae",
  "game_id": "top-down-demo",
  "match_status": "ready",
  "disconnection_time_sec": 60.0
}
```

### 3. Stream the game

```bash
curl -N "http://localhost:8000/api/multiplayer/stream?session_id=9f8e7d6c5b4a3921"
```

The first SSE event is a full bootstrap: `connection`, `clients`, `characters`, `items`, `room`, `match`, `turnState`. Every later event is a patch: `item-state-update`, `character-state-update`, `client-joined`, `client-left`, `synchronizer-changed`, `env-item-authority-changed`, `game-over`, and so on.

### 4. Send input

```bash
# Move (realtime)
curl -s -X PATCH http://localhost:8000/api/multiplayer/character-state \
  -H "Content-Type: application/json" -H "X-Client-ID: a1b2c3d4" \
  -d '{"updates":[{"clientId":"a1b2c3d4","pos":[120,340],"health":100}],"timestamp":1710000000000}'

# Collect a gem (exactly-once)
curl -s -X PATCH http://localhost:8000/api/multiplayer/item-state \
  -H "Content-Type: application/json" -H "X-Client-ID: a1b2c3d4" \
  -d '{"updates":[],"collections":[{"instanceId":"top-down-demo:gem-1","collectedByClientId":"a1b2c3d4","environment":"top-down-demo"}],"timestamp":1710000000000}'

# Keep-alive (also triggers lazy disconnect / empty-game sweeps)
curl -s -X POST http://localhost:8000/api/multiplayer/heartbeat \
  -H "Content-Type: application/json" -H "X-Client-ID: a1b2c3d4" -d '{}'
```

> All client → server calls after `join` carry `X-Client-ID: <client_id>`. The `stream` call carries `?session_id=<session_id>` instead.

### 5. Join a turn-based game (tic-tac-toe)

```bash
curl -s -X POST http://localhost:8000/api/multiplayer/join \
  -H "Content-Type: application/json" \
  -d '{
    "environment_name": "tic-tac-toe",
    "character_name": "X",
    "game_id": "tic-tac-toe",
    "player_id": "player-x",
    "config": {
      "game_id": "tic-tac-toe",
      "requires_matchmaking": true,
      "requires_turn_based": true,
      "max_players_per_room": 2,
      "min_players_per_room": 2,
      "turn": {"max_turns": 9, "allow_pass": false, "allow_resign": true}
    }
  }'
```

Then play with `POST /api/multiplayer/turn/action` — see [Turn-Based or Not](#turn-based-or-not--choosing-a-mode).

---

## How it works — architecture

```
                    ┌────────────────────────────┐
  Browser / Client  │  join()                      │
  ───────────────►  │  POST /api/multiplayer/join │
                    │  → room + authority +       │
                    │    server game data         │
                    └──────────────┬─────────────┘
                                   │ bootstrap
                    ┌──────────────▼─────────────┐
                    │  GET /api/multiplayer/stream │
                    │  SSE: full snapshot, then   │
                    │  patches (Datastar signals) │
                    └──────────────┬─────────────┘
                                   │
        ┌──────────────────────────┼──────────────────────────┐
        │                          │                          │
  PATCH /character-state   PATCH /item-state          POST /turn/action
  PATCH /custom-state      PATCH /item-authority-*    POST /turn/pass|resign
  PATCH /effects-state     POST /heartbeat            POST /switch-environment
        │                          │                          │
        └──────────────────────────┼──────────────────────────┘
                                   ▼
                    ┌────────────────────────────┐
                    │  MultiplayerRegistry       │
                    │  clients · rooms · authority│
                    │  character_cache · item_cache│
                    │  TurnManager · Matchmaker  │
                    │  DirtyFilter · Freshness   │
                    │  ────────────────────────── │
                    │  On EVERY packet:           │
                    │  1. touch sender            │
                    │  2. sweep expired clients   │
                    │  3. sweep empty games       │
                    │  4. apply + broadcast       │
                    └────────────────────────────┘
```

**Key invariants**

- The server never trusts a client about *who* may act, *whose* turn it is, or *whether* an item exists. It validates and then broadcasts.
- The first packet for a game creates the game data. The last client leaving starts a 5-minute grace clock. The next packet after the clock expires deletes the data.
- Every packet is a “tick.” There is no wall-clock loop.

### Project structure

```
match/
├── README.md
├── docs/
│   └── hero.png                  # 1280×320 hero (raw GitHub link at top of README)
├── server/
│   └── app/
│       ├── main.py               # FastAPI app, demos, /api/multiplayer router, registry glue
│       ├── multiplayer/
│       │   ├── protocol.py       # GameSessionConfig, JoinRequest/Response, signals, validation
│       │   ├── authority.py      # AuthorityManager — env + per-item ownership, failover
│       │   ├── matchmaking.py    # Matchmaker — queues, fingerprint, skill tolerance
│       │   ├── turns.py          # TurnManager — phases, order, pass/resign, win/draw
│       │   └── ...               # DirtyFilter, FreshnessMatrix, SkillProvider
│       └── demo/
│           ├── tic-tac-toe.html  # turn-based demo
│           └── top-down.html     # realtime demo (server-spawned gems)
└── extension/
    └── manifest.json
```

> Your checkout may have additional `api/` / `chat` / `health` routers in `main.py`. The multiplayer surface below is the stable contract; other routers are auxiliary.

---

## API Reference

Base: `https://<your-host>/api/multiplayer`

All timestamps are **milliseconds since epoch** (`_now_ms()`). All client → server calls after `join` require `X-Client-ID`. Errors are FastAPI `HTTPException` JSON: `{"detail": "..."}`.

### Session & matchmaking

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/join` | Create a client, assign room / queue, register authority, ensure server game data. Returns `client_id`, `session_id`, `is_synchronizer`, `room_id`, `disconnection_time_sec`. |
| `POST` | `/leave` | Remove client, fail over synchronizer / authority, start empty-game clock if env is now empty. |
| `POST` | `/heartbeat` | Explicit keep-alive. Touches `last_seen_ms`, sweeps expired + empty games. Returns `403` if already evicted. |
| `POST` | `/matchmaking/cancel` | Remove client from queue. |
| `GET` | `/matchmaking/status?client_id=…` | Queue position / status for a queued client. |
| `POST` | `/switch-environment` | Move client to another `environment_name`. Old env may become empty; new env gets game data + authority. |
| `GET` | `/stream?session_id=…` | SSE stream. First event = full bootstrap. Later events = patches. |

### Realtime state

| Method | Path | Purpose |
|---|---|---|
| `PATCH` | `/character-state` | Broadcast position / health / score / custom metadata for one or more clients. Server relays, does not simulate physics. |
| `PATCH` | `/item-state` | `updates` (move / spawn — authority-gated) + `collections` (exactly-once, any player). For server-spawned gems, updates are dropped; only collections are accepted. |
| `PATCH` | `/item-authority-claim` | Claim authority over a single item (if `allow_client_claims`). |
| `PATCH` | `/item-authority-release` | Release a claim. |
| `PATCH` | `/custom-state` | Opaque per-room `custom` dict — game-specific rules, scores, timers, flags. |
| `PATCH` | `/effects-state` | Visual / audio effects state (particles, shakes, flashes) broadcast to the room. |

### Turn-based

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/turn/action` | Submit a turn action. Server validates it is your turn, the room is `in_progress`, and the action is legal for your validator. |
| `GET` | `/turn/state?room_id=…` | Current `TurnState`: phase, turn order, current player, winner, draw. |
| `POST` | `/turn/pass` | Pass your turn (if `allow_pass`). |
| `POST` | `/turn/resign` | Resign. Opponent wins (if in progress). |

### Meta / demos

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/v1/capabilities` | Feature flags the client can detect (`disconnectionTime`, `heartbeat`, `turns`, etc.). |
| `GET` | `/` | API index / health. |
| `GET` | `/tic-tac-toe` | Turn-based demo. |
| `GET` | `/top-down` | Realtime demo (also at `/demo/top-down`). |
| `GET` | `/demo/{name}` | Any demo in `server/app/demo/{name}.html`. |
| `GET` | `/extension/manifest.json` | Extension manifest. |

### `POST /join` — request shape

```jsonc
{
  "environment_name": "top-down-demo",   // namespace: one game = one environment
  "character_name": "Billy",
  "game_id": "top-down-demo",
  "player_id": "stable-player-id",        // for skill lookup; "" = anonymous
  "display_name": "Billy",
  "skill": { "rating": 1000 },
  "room_id": null,                       // or join a specific room
  "room_code": null,                     // or join a private room by code
  "config": {
    "game_id": "top-down-demo",
    "requires_matchmaking": false,
    "requires_turn_based": false,
    "max_players_per_room": 8,
    "min_players_per_room": 1,
    "visibility": "public",               // "public" | "private"
    "room_code": null,
    "matchmaking": {
      "max_initial_skill_gap": 100.0,
      "gap_widen_per_sec": 5.0,
      "max_skill_gap": 500.0
    },
    "turn": {
      "max_turns": null,
      "allow_pass": true,
      "allow_resign": true,
      "turn_order_strategy": "join_order"
    },
    "authority": {
      "scopes": ["items", "environment", "character_state"],
      "resource_types": ["*"],
      "allow_client_claims": true
    },
    "disconnection_time_sec": 60,
    "empty_game_ttl_sec": 300,
    "custom_rules": {},
    "metadata": {}
  }
}
```

`environment_name` defaults to `game_id` if omitted. `config` is parsed by `GameSessionConfig.from_payload()` and accepts flat aliases (`maxPlayersPerRoom`, `disconnectionTime`, `emptyGameTtlSec`, …) as well as nested shapes.

### `GET /stream` — SSE wire format

`Content-Type: text/event-stream`. Events are Datastar `datastar-patch-signals`:

```
event: datastar-patch-signals
data: signals {"mp": {"connection": {...}, "characters": [...], "items": {...}, "room": {...}}}
```

Clients should handle two forms:

- **Bootstrap** (first event): full snapshot — `connection`, `clients`, `characters`, `items.updates`, `room`, `custom`, `turnState`, `match`.
- **Patch** (later events): a single signal name — `item-state-update`, `character-state-update`, `client-joined`, `client-left`, `synchronizer-changed`, `env-item-authority-changed`, `item-authority-changed`, `game-over`, `matchmaking-queued`, etc.

Demo clients parse with:

```js
const stream = new EventSource(`/api/multiplayer/stream?session_id=${sessionId}`);
stream.addEventListener("datastar-patch-signals", (ev) => handleStreamData(ev.data));
stream.onmessage = (ev) => handleStreamData(ev.data);
```

### `PATCH /item-state` — shape

```jsonc
{
  "updates": [
    {
      "instanceId": "top-down-demo:gem-1",
      "itemName": "gem",
      "pos": [120.5, 340.0],
      "rot": [0, 0, 0, 1],
      "isCollected": false,
      "environment": "top-down-demo"
    }
  ],
  "collections": [
    {
      "instanceId": "top-down-demo:gem-1",
      "collectedByClientId": "<client_id>",
      "environment": "top-down-demo"
    }
  ],
  "timestamp": 1710000000000
}
```

- `instanceId` should be namespaced: `<environment>:<local-id>`. Bare ids bind to the sender’s env on first sight.
- `updates` are **authority-gated** — non-owners are silently dropped (per spec §7.5). No error, no broadcast.
- `collections` are **any-player, exactly-once** — the first valid collection wins; later duplicates are dropped; a collected item cannot be resurrected by a stale update (except by the env authority respawning it, or by the server respawning a gem).
- For `top-down-demo`, the server owns gems: client `updates` for gems are dropped, `collections` trigger a server respawn in the same broadcast.

---

## Client protocol — the 60-second version

1. **Join** — `POST /join` with `GameSessionConfig`. Save `client_id` + `session_id`.
2. **Stream** — `GET /stream?session_id=…`. Render the bootstrap, then apply patches.
3. **Send input on your own cadence** — e.g. `requestAnimationFrame` for realtime, on-click for turn-based. No server tick to keep up with.
4. **Heartbeat** — if your game has long idle periods (lobby, thinking, paused), `POST /heartbeat` at least once per `disconnection_time_sec / 2`.
5. **Leave** — `POST /leave` on unload / quit. If you don’t, the server evicts you on the next packet after your budget expires.

```js
// Minimal JS client (see server/app/demo/top-down.html for a full one)
const joinRes = await fetch("/api/multiplayer/join", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    environment_name: "my-game",
    character_name: "Ada",
    game_id: "my-game",
    config: { game_id: "my-game", requires_matchmaking: false, requires_turn_based: false }
  })
});
const { client_id, session_id } = await joinRes.json();

const stream = new EventSource(`/api/multiplayer/stream?session_id=${session_id}`);
stream.addEventListener("datastar-patch-signals", (e) => applyPatch(JSON.parse(e.data.slice("signals ".length))));

await fetch("/api/multiplayer/character-state", {
  method: "PATCH",
  headers: { "Content-Type": "application/json", "X-Client-ID": client_id },
  body: JSON.stringify({ updates: [{ clientId: client_id, pos: [10, 20] }], timestamp: Date.now() })
});
```

---

## How Matching Works

Matching answers one question: **which players should share a room, and in what order?**

MATCH supports two paths that share the same `join` shape. The choice is `requires_matchmaking` in `GameSessionConfig`.

### The two paths

|  | Direct rooms (`requires_matchmaking: false`) | Matchmaking (`requires_matchmaking: true`) |
|---|---|---|
| **Use for** | Friends joining a lobby, private codes, single-player + spectators, demos, any game where *who* you play with matters more than fairness | Ranked / casual queues, strangers, skill-balanced rooms |
| **Room creation** | First joiner creates a room for `environment_name` immediately. Later joiners append until `max_players_per_room`. | Joiner is queued. Rooms form only when enough queued players are mutually compatible. |
| **Room id** | Deterministic from `environment_name` (`direct:<env>`) | New UUID per formed match |
| **Latency to play** | Immediate | Waits for a match (see tolerance widening below) |
| **Example** | `top-down-demo` (8 players, drop-in) | `tic-tac-toe` (2 players, 1v1) |

Both paths converge on the same `Room` object: `player_ids`, `status: waiting | in_progress | finished`, `custom`, `turn_manager` (if turn-based).

### Fingerprint — what makes two players matchable?

`GameSessionConfig.fingerprint()` is the matchmaking key. Two players can only be matched if their fingerprints are **identical**:

- `game_id`
- `requires_matchmaking`, `requires_turn_based`
- `max_players_per_room`, `min_players_per_room`
- `matchmaking` tolerances + `game_mode` + `region`
- `turn` rules (`max_turns`, `allow_pass`, `allow_resign`, `turn_order_strategy`)
- `authority` scopes / resource types
- `visibility`, `room_code`, `custom_rules`

Deliberately **excluded**: `disconnection_time_sec` and `empty_game_ttl_sec`. Those are per-client / per-env hygiene budgets, not match compatibility. Two players with different disconnect budgets can still play together — each is evicted on their own budget.

> If two players “should” match but don’t, check the fingerprint first. A single differing field (`turn_order_strategy`, `game_mode`, `room_code`) silently puts them in different queues.

### Skill-aware matching — the algorithm

Each player has a `SkillRating` (`rating`, e.g. 1000). The source is `SkillProvider.resolve_skill()` — client-reported by default, swappable for a server-side rating store.

`Matchmaker.try_form_matches(fingerprint)` runs on every enqueue / queue fill:

```
1. Pool = queued players for this fingerprint, sorted by enqueued_at_ms (FIFO).
2. While len(pool) >= min_players_per_room:
     oldest = pool[0]                                   # longest waiter is anchor
     waited_ms = now - oldest.enqueued_at_ms
     tolerance = tolerance_for_wait_ms(waited_ms)        # widens over time
     candidates = [p in pool[1:] if |p.skill - oldest.skill| <= tolerance]
     candidates.sort(by |skill diff|)
     group = [oldest] + candidates[:need - 1]            # need = max_players_per_room
     if len(group) < min_players_per_room: break         # wait for more players
     remove group from pool, form a Room for group
3. Leftover pool stays queued.
```

#### Tolerance widening

```python
# MatchmakingConfig
tolerance_for_wait_ms(waited_ms) = min(
    max_skill_gap,
    max_initial_skill_gap + gap_widen_per_sec * (waited_ms / 1000)
)
```

| Config | Default | Meaning |
|---|---|---|
| `max_initial_skill_gap` | `100.0` | At `t = 0`, only players within ±100 rating can match |
| `gap_widen_per_sec` | `5.0` | Tolerance grows +5 rating per second waited |
| `max_skill_gap` | `500.0` | Hard cap — never match players >500 apart, however long they wait |

**Worked example** — `min = 2, max = 2`, `max_initial_gap = 100`, `widen = 5/s`, `max_gap = 500`:

- `t = 0s`: Alice (1000) + Bob (1050) → gap 50 ≤ 100 → **match immediately**.
- `t = 0s`: Carol (1000) + Dave (1300) → gap 300 > 100 → **both wait**.
- `t = 40s`: tolerance = 100 + 5×40 = 300 → gap 300 ≤ 300 → **match**.
- `t = 100s`: tolerance would be 600, capped at 500 → a 600-gap pair **never** matches.

This is the classic fairness-vs-wait tradeoff: tight at first for fair games, looser over time so no one waits forever, capped so a beginner never faces a grandmaster.

#### FIFO fairness + closest-skill selection

- **Anchor = oldest waiter.** The longest-waiting player is never skipped. This prevents starvation: a 900-rated player at the back of a 1500-rated queue cannot be leapfrogged forever.
- **Candidates = closest skill first.** Among players within tolerance, the closest ratings are picked first, so the formed room is as balanced as possible.
- **Group size = up to `max_players_per_room`.** For 2-player games, `group = [oldest, closest]`. For 4-player free-for-alls, the anchor + 3 closest within tolerance.

#### Seats, turn order, and queue position

- `seat` = index in `room.player_ids` (join / formation order). For turn-based games, `TurnManager` derives `turn_order` from `seat` by default (`turn_order_strategy: join_order`).
- `GET /matchmaking/status` returns `queuePosition` (1-based). Position is FIFO, not skill — a high-rated player behind you does not jump you.
- `POST /matchmaking/cancel` removes you from the queue. If you disconnect while queued, the next packet’s sweep cancels you automatically.

### Private rooms & room codes

- `visibility: "private"` + `room_code: "friends-only"` — only joiners who supply the same `room_code` share a room. The fingerprint includes `room_code`, so `room_code: "a"` and `room_code: "b"` are different queues / rooms even for the same `game_id`.
- `room_id` — join a specific existing room (friend invite). If the room is full or finished, join is rejected / a new room is formed depending on the router logic — clients should handle `match_status: queued` vs `ready`.
- `custom_rules` — opaque game-specific rules (e.g. `{"board_size": 3, "time_control": "blitz"}`) are part of the fingerprint, so “3×3 blitz” and “5×5 bullet” never mix.

### Failure modes to design for

| Symptom | Likely cause | Fix |
|---|---|---|
| Two players never match | Fingerprint mismatch (different `turn` / `game_mode` / `room_code` / `custom_rules`) | Log `config.fingerprint()` on both clients, diff them |
| Long queue, no match | Tolerance cap too low, or `min_players_per_room` > queue size | Lower `max_skill_gap` cap is a *maximum*, not a target — raise it, or lower `min_players` |
| Unfair match | `max_initial_skill_gap` too wide | Tighten to 50–100 for ranked, 200+ for casual |
| Player stuck in queue after tab close | No packet arrived to sweep them | Queue is swept on the next packet from any client; heartbeat from other players unblocks it |
| Same player matched twice | Double `join` (e.g. React StrictMode) | Dedupe by `player_id` on the client, or call `leave` before re-joining |

### Tuning cheat-sheet

| Game type | `min` / `max` | `max_initial_gap` | `gap_widen_per_sec` | `max_skill_gap` |
|---|---|---|---|---|
| 1v1 ranked (chess-like) | 2 / 2 | 75 | 2 | 200 |
| 1v1 casual | 2 / 2 | 150 | 8 | 400 |
| 4-player FFA | 4 / 4 | 100 | 5 | 300 |
| Co-op (skill matters less) | 2 / 4 | 300 | 10 | 500 |
| Party / friends | — use direct rooms, not matchmaking — | — | — | — |

---

## Turn-Based or Not — Choosing a Mode

`requires_turn_based` is the single most consequential option in `GameSessionConfig`. It changes what the server validates, what the client must send, and what “cheating” even means.

### At a glance

|  | Realtime (`requires_turn_based: false`) | Turn-based (`requires_turn_based: true`) |
|---|---|---|
| **Examples** | Top-down collector, shooters, racing, sandbox, party games | Tic-tac-toe, chess, checkers, card games, word games |
| **Who decides order?** | Nobody — everyone acts whenever they want | Server — `TurnManager` enforces `turn_order` |
| **Client sends** | `character-state` every frame / tick, `item-state` collections | `turn/action` only on your turn, plus `turn/pass`, `turn/resign` |
| **Server validates** | Authority for items, disconnect, env binding | + Is it your turn? Is the room `in_progress`? Is the action legal for your validator? |
| **State broadcast** | Patches as they arrive | `turnState` patch + `game-over` when finished |
| **Idle players** | Kicked by `disconnection_time_sec` if silent | Same — but see “thinking time” below |
| **Best for** | Continuous movement, reflexes, simultaneous play | Discrete moves, strategy, async / slow play, anti-cheat |

You can mix them: a turn-based game can still send `character-state` for cursors / hover / avatars, and a realtime game can use `custom-state` for scores. The flag only controls whether `TurnManager` gates actions.

### Turn-based in detail

#### `TurnConfig`

```python
TurnConfig(
    enabled=True,                  # == requires_turn_based
    max_turns=None,                # None = unlimited; 9 for tic-tac-toe
    allow_pass=True,               # can a player pass instead of moving?
    allow_resign=True,             # can a player resign?
    turn_order_strategy="join_order"  # join_order | skill | random | custom
)
```

#### `TurnManager` lifecycle

```
waiting ──(min_players join)──▶ in_progress ──(win / draw / resign / max_turns)──▶ finished
                                  ▲
                                  │  turn/action (valid)
                                  │  turn/pass (if allow_pass)
                                  └────────────────────────────────────────────
```

- **`waiting`**: Room exists, not enough players yet. `turn/action` is rejected.
- **`in_progress`**: `current_player_id` = `turn_order[current_index]`. Only that player’s `turn/action` is accepted. After a valid action, the index advances (wrapping), `turn_count += 1`.
- **`finished`**: `winner_id` or `is_draw` is set. Further actions are rejected. `game-over` is broadcast with `turnState`, `winnerId`, `isDraw`, `endReason`.

`GET /turn/state` returns the current `TurnState` — clients should render from this, not from local move history.

#### What is a “turn action”?

```bash
POST /api/multiplayer/turn/action
X-Client-ID: <client_id>

{
  "room_id": "abc123",
  "action": {"cell": 4, "mark": "X"},   # game-specific, opaque to MATCH
  "timestamp": 1710000000000
}
```

MATCH validates *turn ownership*, not game rules — unless you plug in a validator. `MultiplayerRegistry.VALIDATORS` maps `game_id → validator(action, turnState, custom)`. The built-in tic-tac-toe validator checks: cell in `0..8`, cell empty, mark matches the player’s seat. If you don’t register a validator, MATCH accepts any action from the player whose turn it is — suitable for prototyping, not for ranked play.

> **Rule of thumb:** If a move can be faked to win (e.g. “I place X in all 9 cells”), register a validator. If the move is just “I ended my turn,” the turn gate alone is enough.

#### Pass / resign

| Endpoint | When to use | Effect |
|---|---|---|
| `POST /turn/pass` | “I have no move” (card games, blocked positions) | Advances turn without an action, if `allow_pass`. Otherwise `400`. |
| `POST /turn/resign` | “I give up” | Room → `finished`, opponent wins, `endReason: resign`, `game-over` broadcast. If `allow_resign` is false, rejected. |

#### Opponent leaves mid-game

If a player leaves / times out while `turnState.phase == in_progress`, `TurnManager.on_player_left()` fires:

- Remaining player wins, `endReason: opponent_left`, `game-over` broadcast.
- The room is removed (if empty) and the empty-game clock starts.
- If the game was `waiting`, no winner is declared — the room just shrinks.

This is why turn-based games should still send `heartbeat` or `character-state` while thinking — otherwise a long think can look like a disconnect (see [Disconnect Time](#disconnect-time--disconnection_time_sec)).

### Realtime in detail

No `TurnManager`. Clients send `character-state` at their own cadence (the demos use `requestAnimationFrame`, throttled to ~20–30 Hz). The server:

- Relays updates to everyone in the env (dirty-filtered — unchanged positions are not rebroadcast).
- Tracks `FreshnessMatrix` so a late joiner gets the latest state, not a replay of every frame.
- Gates `item-state` updates by authority (see below).

Realtime games still need `custom-state` for scores, timers, and win conditions — e.g. top-down stores `score` in `character-state.metadata` and `custom` for room-level flags.

### Which mode should you pick?

**Pick turn-based if:**

- Moves are discrete (place a piece, play a card, choose a word).
- Order matters and simultaneous moves would be ambiguous.
- You want async play (players can take minutes / hours per move — but see disconnect tuning).
- You want the server to be the anti-cheat referee.

**Pick realtime if:**

- Movement is continuous (WASD, joystick, drag).
- Players act simultaneously (racing, shooters, collectors).
- Latency matters more than strict ordering.
- Your game is a sandbox / party game with no “turns” at all.

**Pick both / hybrid if:**

- Turn-based core + realtime avatars (e.g. chess with live cursors).
- Realtime movement + turn-based combat (e.g. top-down with turn-based duels).
- Lobby is realtime, match is turn-based — switch `requires_turn_based` per `game_id`, not per room.

### Config examples

```jsonc
// Turn-based: tic-tac-toe, 1v1, ranked
{
  "game_id": "tic-tac-toe",
  "requires_matchmaking": true,
  "requires_turn_based": true,
  "max_players_per_room": 2,
  "min_players_per_room": 2,
  "turn": { "max_turns": 9, "allow_pass": false, "allow_resign": true, "turn_order_strategy": "join_order" }
}

// Realtime: top-down, drop-in, 8 players
{
  "game_id": "top-down-demo",
  "requires_matchmaking": false,
  "requires_turn_based": false,
  "max_players_per_room": 8,
  "min_players_per_room": 1
}

// Realtime + matchmaking: 4-player FFA, skill-balanced
{
  "game_id": "arena-ffa",
  "requires_matchmaking": true,
  "requires_turn_based": false,
  "max_players_per_room": 4,
  "min_players_per_room": 4,
  "matchmaking": { "max_initial_skill_gap": 100, "gap_widen_per_sec": 5, "max_skill_gap": 300 }
}
```

### Migration: realtime → turn-based

1. Set `requires_turn_based: true` in `config` (new fingerprint → old clients won’t match new ones, which is what you want).
2. Stop sending `character-state` as moves; send `turn/action` on click / submit.
3. Render from `turnState` + `custom`, not from local move history.
4. Register a validator for your game’s rules.
5. Handle `game-over` — disable input, show winner / draw, offer rematch (new `join`).

---

## Disconnect Time — `disconnection_time_sec`

`disconnection_time_sec` answers: **how long can a client be silent before the server drops it?**

It is declared by the *client game* on `join`, per `game_id`, in `GameSessionConfig`. It is **not** a server-wide timeout.

### The option

```jsonc
// In join → config
{
  "disconnection_time_sec": 60,   // default 60
  "disconnectionTime": 60,       // alias — also accepted
  "disconnectTimeout": 60,        // alias — also accepted
  "disconnection_time": 60        // alias — also accepted
}
```

| Value | Meaning |
|---|---|
| `60` (default) | Drop after 60s of silence |
| `10` | Fast, twitchy games — drop quickly so the room isn’t stuck waiting |
| `300` | Slow / thinking games — 5 min to think, reconnect, or come back from a phone call |
| `0` | **Never expire.** The client stays listed until it explicitly `leave`s. Use for lobbies, spectators, async / correspondence games. |
| `< 0` or `> 86400` | Rejected by `validate()` — must be in `[0, 86400]` (24h) |

It is also echoed in the `JoinResponse` (`disconnection_time_sec`) and in the `stream` bootstrap (`connection.disconnectionTimeSec`) so the client can display “Drop if silent: 60s” and schedule heartbeats.

### What counts as “a packet”?

Any client → server HTTP request with `X-Client-ID`:

- `join`, `leave`, `heartbeat`
- `character-state`, `item-state`, `item-authority-claim/release`
- `custom-state`, `effects-state`
- `turn/action`, `turn/pass`, `turn/resign`
- `switch-environment`, `stream` attach

`GET /stream` itself is *not* a heartbeat — the SSE connection staying open does not prove the client is alive. A tab can hold an SSE connection open while its JS is frozen. That is why the server tracks `last_seen_ms` from *inbound* packets, not from SSE liveness.

### How enforcement works — event-driven, no timers

```
Client sends packet
  → server _touch(sender): last_seen_ms = now
  → server _sweep_expired_locked(now):
      for every client != sender:
        if now - last_seen_ms > disconnection_time_sec * 1000:
          _remove_client_locked(client, reason="timeout")
          broadcast client-left {reason: "timeout"}
          fail over synchronizer + authority
  → server applies the sender's packet (if sender is not itself expired)

If sender is itself expired:
  → _remove_client_locked(sender, reason="timeout")
  → return 403 {detail: "disconnected: timeout"}
  → client must re-join; its old client_id is dead
```

**There is no background loop.** No `asyncio.sleep`, no `setInterval`, no cron. Consequences, stated plainly:

- **If packets keep arriving**, eviction is prompt (within one packet of the deadline).
- **If no packet arrives from anyone**, an idle client stays listed until the next packet arrives. A completely silent server does not evict anyone on its own.
- **A late packet does not revive you.** If you were already expired, your next packet gets `403` instead of resurrecting your session. This prevents “zombie” players reappearing after failover.

This is intentional: it makes the server cheap on Render / serverless (no always-on loop), deterministic in tests (you control `now` by sending a packet), and honest about what “timeout” means in a stateless HTTP world.

### What happens when you are evicted?

1. `client-left` broadcast with `reason: "timeout"` — demos show `player X dropped: timeout`.
2. If you were synchronizer, the next client in the env becomes synchronizer (`synchronizer-changed`).
3. If you held env / item authority, `AuthorityManager.client_leaves()` promotes the next client (`env-item-authority-changed`, `item-authority-changed`).
4. If you were in a turn-based game in progress, `TurnManager.on_player_left()` ends the game (opponent wins, `game-over`).
5. If the env is now empty, the empty-game clock starts (see below). Your character state is removed; your items (if any) are *not* — they belong to the game, not to you.

### Heartbeat — the client’s job

If your game can be silent longer than `disconnection_time_sec / 2`, send a heartbeat:

```js
// In the demo: Heartbeat button + auto-heartbeat while joined
async function sendHeartbeat() {
  const res = await fetch("/api/multiplayer/heartbeat", {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Client-ID": clientId },
    body: "{}"
  });
  if (res.status === 403) {
    // You were already evicted — re-join, don't retry heartbeat
    await leave(); await join();
  }
}
// Good cadence: every disconnection_time_sec / 3, or on visibilitychange / focus
setInterval(() => { if (clientId) sendHeartbeat(); }, 20000); // for 60s budget
```

> The demo’s `requestAnimationFrame` loop *already* sends `character-state` while playing, so it acts as a heartbeat. Heartbeat matters most in lobbies, turn-based thinking, paused games, and background tabs.

### Choosing a value — by genre

| Game | Recommended `disconnection_time_sec` | Why |
|---|---|---|
| Fast realtime (shooter, racer, top-down) | `15–30` | A frozen player should not block the field. Others should see them vanish quickly. |
| Casual realtime (party, sandbox) | `60` (default) | Forgiving for tab switches, phone calls, slow networks. |
| Turn-based, live (blitz chess, tic-tac-toe) | `60–120` | Enough to think + reconnect, short enough that the opponent isn’t stuck. |
| Turn-based, slow / correspondence | `0` (never) or `3600+` | Players may take hours. Eviction would end games unfairly. Use `0` + explicit `leave` / `resign`. |
| Lobby / spectator | `0` | Spectators are silent by design. Never evict them. |
| Mobile / flaky network | `120–180` | Mobile radios drop for 30–60s routinely. Tight budgets feel like random kicks. |
| Tests / bots | `5–10` | Fast failure makes tests fast. |

**Two budgets, two purposes — don’t confuse them:**

| Option | What it deletes | When |
|---|---|---|
| `disconnection_time_sec` (default 60) | A *client* (one player) | After that client is silent that long |
| `empty_game_ttl_sec` (default 300) | A *game’s data* (items, rooms, authority) | After the *game* is empty that long |

A game can be empty for 5 minutes even though every client was evicted after 60s of silence. The first clock removes players; the second clock removes the game they left behind.

### Example: per-game budgets in one server

```js
// Fast game — tight budget
await join({ game_id: "arena", config: { disconnection_time_sec: 15 } });

// Slow game — generous budget, same server, same code
await join({ game_id: "chess-correspondence", config: { disconnection_time_sec: 0 } });
```

Because `disconnection_time_sec` is excluded from the matchmaking fingerprint, a `15s` player and a `60s` player can still match in the same room — each is evicted on their own budget. If you *want* budgets to be part of match compatibility, put the budget in `custom_rules` instead (which *is* fingerprinted).

---

## Server-authoritative game data & empty-game cleanup

### Who owns what?

| Data | Owner | Client can… |
|---|---|---|
| Room, turn order, winner | Server | Request actions, never set |
| Items (e.g. gems) | Server (top-down) | Collect only; never spawn / move |
| Character position | Client (its own) | Send its own `character-state`; server relays |
| Custom rules / scores | Server (via `custom-state`) | Send `custom-state` if authority allows |
| Authority | Server (`AuthorityManager`) | Claim / release if `allow_client_claims` |

**Top-down example, end to end:**

1. Billy joins first → server spawns 24 gems (`server_spawned: True`, `instanceId: top-down-demo:gem-N`).
2. Jill joins → bootstrap contains the *same* 24 gems. No client spawn, no duplication.
3. Billy collects `gem-1` → server marks collected, spawns `gem-25`, broadcasts `collections: [gem-1]` + `updates: [gem-25]` in one packet. Both clients see `-1 +1`.
4. Billy leaves → gems stay. Jill keeps playing. If Jill is now synchronizer, she is a label — the server still owns gems.
5. Both leave → env empty → 5-minute grace clock starts.
6. Someone rejoins within 5 min → same gems (minus collected, plus respawns) resume.
7. No one rejoins for 5 min → next packet from *any* game deletes the game’s data. Next join gets a fresh 24.

### Authority — why it matters

`AuthorityManager` tracks:

- **Env authority** — one client per env who may spawn / move items in that env (usually the first joiner / synchronizer).
- **Item owners** — one client per `instanceId` who may move that item.

`handle_item_state` drops any `update` where `resolved_owner != sender`. This is silent by design — a stale client sending old positions should not error the whole room, it should just be ignored.

For server-spawned games (`top-down-demo`), the server bypasses this check entirely — it writes `item_cache` directly. Client updates for server gems are always dropped.

Failover: when the authority holder leaves / times out, `client_leaves()` promotes the next client and broadcasts `env-item-authority-changed`. The new authority does *not* need to re-spawn anything — the server already holds the game.

### Empty-game cleanup — `empty_game_ttl_sec`

```jsonc
{ "empty_game_ttl_sec": 300 }   // default 300 = 5 min, 0 = keep forever
```

- Set on `join` → `config`. Captured from the last client in the env when it becomes empty.
- Deletion is **lazy, event-driven**: `_sweep_empty_games_locked(now)` runs on every packet (inside `_sweep_expired_locked`). If `now - empty_since > ttl`, `_delete_game_data_locked(env)` removes items, `item_environments`, dirty cache, empty rooms, env authority, gem counter.
- If *no* packet arrives, deletion waits — same tradeoff as disconnect. A single heartbeat from any game is enough to trigger cleanup for all expired empty games.

To test quickly, set `empty_game_ttl_sec: 10` in the demo, leave all players, wait 11s, send any packet (join another game, heartbeat), then rejoin — you should get a fresh field.

---

## Config reference

### `GameSessionConfig`

| Field | Type | Default | Notes |
|---|---|---|---|
| `game_id` | `str` | `"default"` | Also `environment_name` if `game_id` empty |
| `requires_matchmaking` | `bool` | `false` | `true` = queue, `false` = direct room |
| `requires_turn_based` | `bool` | `false` | `true` = `TurnManager` gates actions |
| `max_players_per_room` | `int` | `8` | Room fills at this size |
| `min_players_per_room` | `int` | `1` | Match forms / game starts at this size |
| `matchmaking` | `MatchmakingConfig` | — | See below |
| `turn` | `TurnConfig` | — | See below |
| `authority` | `AuthorityConfig` | — | See below |
| `visibility` | `"public" \| "private"` | `"public"` | Private needs `room_code` |
| `room_code` | `str \| null` | `null` | Private room join key, fingerprinted |
| `custom_rules` | `dict` | `{}` | Game-specific, fingerprinted |
| `metadata` | `dict` | `{}` | Opaque, not fingerprinted |
| `disconnection_time_sec` | `float` | `60.0` | `[0, 86400]`, `0` = never expire. Aliases: `disconnectionTime`, `disconnection_time`, `disconnect_timeout_sec`, `disconnectTimeout` |
| `empty_game_ttl_sec` | `float` | `300.0` | `[0, 86400]`, `0` = keep forever. Aliases: `emptyGameTtlSec`, `emptyGameTtl` |

### `MatchmakingConfig`

| Field | Default | Notes |
|---|---|---|
| `enabled` | `requires_matchmaking` | — |
| `max_initial_skill_gap` | `100.0` | Tolerance at `t = 0` |
| `gap_widen_per_sec` | `5.0` | Tolerance growth per second waited |
| `max_skill_gap` | `500.0` | Hard cap |
| `game_mode` | `"default"` | Fingerprinted |
| `region` | `null` | Fingerprinted if set |
| `allow_bots` | `false` | Future / custom |

### `TurnConfig`

| Field | Default | Notes |
|---|---|---|
| `enabled` | `requires_turn_based` | — |
| `max_turns` | `null` | `null` = unlimited |
| `allow_pass` | `true` | — |
| `allow_resign` | `true` | — |
| `turn_order_strategy` | `"join_order"` | `join_order` \| `skill` \| `random` \| `custom` |

### `AuthorityConfig`

| Field | Default | Notes |
|---|---|---|
| `scopes` | `["items", "environment"]` | What authority covers |
| `resource_types` | `["*"]` | Which resources |
| `allow_client_claims` | `true` | Whether clients may claim items |

---

## Demos

### Tic-tac-toe — turn-based, matchmade

`GET /tic-tac-toe`

- `requires_matchmaking: true`, `requires_turn_based: true`, `2 / 2` players.
- Click a cell → `POST /turn/action`. Server validates turn + cell empty, advances turn, broadcasts `turnState`.
- `Pass` disabled (`allow_pass: false`), `Resign` enabled.
- Opponent leaves → `game-over`, `endReason: opponent_left`.

### Top-down — realtime, direct room

`GET /top-down`

- `requires_matchmaking: false`, `requires_turn_based: false`, `8 / 1` players.
- WASD / arrows to move, walk over gems to collect.
- Gems are server-spawned: 24 live, respawn 1 per collection.
- HUD: `Gems`, `Players`, `Gems on map`, `Synchronizer`, `Drop if silent: 60s`, `Game data deleted if empty 5 min`.
- `Heartbeat` button sends an explicit keep-alive. `Leave` removes you; if you just close the tab, you’re evicted on the next packet after 60s of silence.

Both demos are single HTML files in `server/app/demo/` — read them as the canonical client implementation.

---

## Deployment

### Local

```bash
uvicorn server.app.main:app --reload --port 8000
# or
python -m server.app.main
```

### Render (as in this repo’s history)

- **Build:** `pip install -r requirements.txt`
- **Start:** `uvicorn server.app.main:app --host 0.0.0.0 --port $PORT`
- **Health:** `GET /` should return JSON with endpoints.
- **Check demos:** `GET /top-down` and `GET /demo/top-down` return the same HTML. If `/top-down` 404s, you deployed an old `main.py` without the `def top_down_demo_page` route — redeploy.
- **No background workers needed.** There is no timer loop to keep alive. A free Render instance that sleeps is fine — the next packet after wake-up sweeps expired clients + empty games.

### Environment

| Variable | Purpose |
|---|---|
| `PORT` | Render / hosting port (default `8000` locally) |
| `PYTHONUNBUFFERED=1` | Recommended for logs on Render |

### Scaling notes

- This is an **in-memory** registry (`MultiplayerRegistry`). It is correct for one process. For multi-process / multi-instance deployments, you need a shared store (Redis) for `clients`, `rooms`, `item_cache`, and a shared `now` — or sticky sessions per `environment_name`.
- SSE connections are long-lived. Put a proxy timeout > your longest expected game, or reconnect on `stream.onerror` (demos do this via `EventSource` retry).

---

## Security & limits

- **No auth in the demo protocol.** `player_id` is client-supplied. For production, put MATCH behind your own auth (e.g. verify a JWT in a middleware and set `player_id` from the token, not from the body).
- **Rate-limit `join` / `character-state`.** A client sending 1000 Hz `character-state` will spam every other client. Throttle on the client (20–30 Hz) *and* on the server (per-client rate limit).
- **Validate `custom_rules` / `custom` size.** They are opaque dicts — cap keys / bytes to avoid memory bloat.
- **Don’t trust `skill.rating`.** It is client-reported by default. For ranked play, resolve skill server-side in `SkillProvider`.
- **CORS.** If your game is on another origin, add `CORSMiddleware` with your game’s origin only.
- **InstanceIds.** Namespaced ids (`<env>:<id>`) prevent cross-game collisions. Bare ids are bound to the sender’s env on first sight — don’t reuse a bare id across games.

---

## Contributing

```bash
git clone https://github.com/EricEisaman/match.git
cd match
git checkout -b feat/my-game-adapter
# Add a validator in MultiplayerRegistry.VALIDATORS, or a demo in server/app/demo/
# Keep the event-driven rule: no timers, no sleeps, no background loops.
# Add a test for your path if you add server logic.
git commit -m "feat: add <game> validator"
git push origin feat/my-game-adapter
# Open a PR — please include: what game, what config, how you tested (2 tabs? 2 players?)
```

Good PRs: a new game validator, a new demo, a `SkillProvider` implementation, docs fixes. Please don’t add a timer loop — if you think you need one, open an issue and describe the packet that should trigger the work instead.

---

## License

MIT — see `LICENSE`. Use it in your game, your jam, your class, your startup. If you ship something with MATCH, a link back is appreciated but not required.

---

*Hero image: `docs/hero.png` (1280×320) — place the file at `docs/hero.png` in the repo root so the raw GitHub link at the top of this README resolves. The image is generic to the open-source MATCH project and does not reference any deployment.*

