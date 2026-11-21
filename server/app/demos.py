"""
Demo registry - the ONLY place that knows about individual demos.

Adding a demo = adding one entry here. main.py never changes.

Each entry:
  game_id:      must match what the client sends in join (game_id + environment_name)
  file:         HTML file in server/app/demo/ served by GET /demo/{name}
  extra_files:  sibling files the HTML imports (e.g. viper.html imports
                sidewinder.js). main.py must serve these from the same
                demo folder at GET /demo/{extra_file}. Omit when the demo
                is a single HTML file.
  server_gems:  True => server spawns + respawns gems for this game, clients only send collections
  gem_target:   how many live gems to keep (0 = no gems)
  world:        server 2D space [W, H] - all demos use 2000x2000 today
  turn_based:   True => room needs a TurnManager (tic-tac-toe)
  max_players:  room size
  character:    what the client sends in metadata.characterModelId (docs only)
"""

DEMOS = {
    "top-down-demo": {
        "game_id": "top-down-demo",
        "file": "top-down.html",
        "server_gems": True,
        "gem_target": 24,
        "world": [2000, 2000],
        "turn_based": False,
        "max_players": 8,
        "character": "top-down-sprite",
        "notes": "2D canvas. Server owns gems.",
    },
    "babylon-demo": {
        "game_id": "babylon-demo",
        "file": "babylon.html",
        "server_gems": True,
        "gem_target": 24,
        "world": [2000, 2000],
        "turn_based": False,
        "max_players": 8,
        "character": "character-basic",
        "notes": "Babylon 3D, same 2000-space, same gems - OWN gem set babylon-demo:gem-N, NOT top-down's.",
    },
    "bjs-jets-demo": {
        "game_id": "bjs-jets-demo",
        "file": "bjs-jets.html",
        "server_gems": False,
        "gem_target": 0,
        "world": [2000, 2000],
        "turn_based": False,
        "max_players": 8,
        "character": "jet",
        "notes": "Jets. No gems. Players only. Radar + fullscreen.",
    },
    "viper-demo": {
        "game_id": "viper-demo",
        "file": "viper.html",
        "extra_files": ["sidewinder.js"],
        "server_gems": False,
        "gem_target": 0,
        "world": [2000, 2000],
        "turn_based": False,
        "max_players": 8,
        "character": "viper-jet",
        "notes": (
            "Viper: bjs-jets + Sidewinders. L = radar lock (visible + within 1000 m), "
            "SPACE = fire at locked target. Each jet carries 4 Sidewinders, 2 under each wing, "
            "no replenish in a life (respawn rearms). Missiles, lock_target, alive, "
            "missiles_remaining, destroyed_by ride in character-state, so every client "
            "sees every missile. Red border when under lock, orange diamond for in-flight "
            "missiles on radar, explosion on hit, end screen when destroyed."
        ),
    },
    "tic-tac-toe": {
        "game_id": "tic-tac-toe",
        "file": "tic-tac-toe.html",
        "server_gems": False,
        "gem_target": 0,
        "world": [2000, 2000],  # unused, turn-based only
        "turn_based": True,
        "max_players": 2,
        "character": None,
        "notes": "Turn-based. No character-state, no gems. Uses turnState only.",
    },
}

# Helpers main.py uses - 2 lines, no per-demo if/else.
def get_demo(game_id="", env_name=""):
    """Return DEMOS entry for a game_id or env_name, or None."""
    if game_id in DEMOS:
        return DEMOS[game_id]
    if env_name in DEMOS:
        return DEMOS[env_name]
    return None

def needs_server_gems(game_id="", env_name=""):
    d = get_demo(game_id, env_name)
    return bool(d and d["server_gems"])

def gem_target(game_id="", env_name=""):
    d = get_demo(game_id, env_name)
    return int(d["gem_target"]) if d else 0

def is_turn_based(game_id="", env_name=""):
    d = get_demo(game_id, env_name)
    return bool(d and d["turn_based"])

def extra_files(game_id="", env_name=""):
    """Sibling files to serve alongside the demo HTML (e.g. sidewinder.js)."""
    d = get_demo(game_id, env_name)
    if not d:
        return []
    files = d.get("extra_files") or []
    return list(files)
