// multiplayer.js — shared MATCH client for ALL demos.
// Demo HTML only does: scene + gameId + metadata. This file does join /
// character-state / item-state / heartbeat / stream / leave.
// Schema (agreed): top-level ONLY clientId / position / health / metadata.
// Everything game-specific lives in metadata — never top-level pos / anim / animation_state.

function createMultiplayer({ gameId, envName, apiBase = "" }) {
  const GAME_ID = gameId;
  const ENV = envName || gameId;
  let clientId = "", sessionId = "", roomId = "";
  let joined = false, isSynchronizer = false;
  let stream = null, lastPacketMs = 0, lastSentMs = 0, lastSentAnim = null;
  let onMp = null, onLog = () => {};

  const log = (s) => onLog(s);
  async function api(path, opts = {}) {
    return fetch(apiBase + path, { ...opts, headers: { "Content-Type": "application/json", "X-Client-ID": clientId, ...(opts.headers || {}) } });
  }
  function colorFor(id) {
    let h = 0; for (const c of String(id)) h = (h * 31 + c.charCodeAt(0)) >>> 0;
    const hue = (h % 360) / 360;
    const f = (n) => { const k = (((n + hue) % 1) + 1) % 1; if (k < 1/6) return 0.55 + 0.45*6*k; if (k < 1/2) return 1.0; if (k < 2/3) return 0.55 + 0.45*(2/3-k)*6; return 0.55; };
    const r = Math.round(255*f(1/3)), g = Math.round(255*f(0)), b = Math.round(255*f(-1/3));
    return "#" + [r,g,b].map(v => v.toString(16).padStart(2,"0")).join("");
  }

  async function join({ name, skill = 1000, disconnectSec = 60, extraConfig = {} }) {
    const res = await api("/api/multiplayer/join", { method: "POST", body: JSON.stringify({
      environment_name: ENV, character_name: name, game_id: GAME_ID,
      player_id: "demo-" + String(name).toLowerCase().replace(/\W+/g,"-") + "-" + Math.random().toString(36).slice(2,6),
      display_name: name, skill: { rating: skill },
      config: { game_id: GAME_ID, requires_matchmaking: false, requires_turn_based: false,
        max_players_per_room: 8, min_players_per_room: 1,
        disconnectionTime: disconnectSec, disconnection_time_sec: disconnectSec,
        empty_game_ttl_sec: 300,
        authority: { scopes: ["items","environment","character_state"], resource_types: ["*"], allow_client_claims: true },
        ...extraConfig },
    })});
    const data = await res.json().catch(() => ({}));
    if (!res.ok) { log("join failed: " + res.status + " " + JSON.stringify(data)); return null; }
    clientId = data.client_id || data.clientId || "";
    sessionId = data.session_id || data.sessionId || "";
    roomId = data.room_id || data.roomId || "";
    joined = true; lastPacketMs = performance.now();
    log("joined: " + clientId + " room " + roomId);
    connectStream();
    return { clientId, sessionId, roomId, data };
  }

  async function leave() {
    joined = false;
    try { await api("/api/multiplayer/leave", { method: "POST", body: "{}" }); } catch(e) {}
    if (stream) stream.close(); stream = null;
  }

  // character-state: ONLY clientId / position / health / metadata.
  async function sendCharacterState(me, force = false) {
    if (!clientId || !joined) return;
    const now = performance.now(); if (!force && now - lastSentMs < 50) return;
    lastSentMs = now; lastPacketMs = now;
    const res = await api("/api/multiplayer/character-state", { method: "PATCH", body: JSON.stringify({
      updates: [{ clientId, position: [me.x, me.y], health: me.health,
        metadata: { schemaVersion: 1, ...me.metadata } }],
      timestamp: Date.now(),
    })});
    if (res.status === 403) { const d = await res.json().catch(()=>({})); log("character-state rejected: " + JSON.stringify(d)); await leave(); }
  }

  // Gems / items: server-owned gems => clients ONLY send collections, never updates.
  // updates: [] + collections: [{instanceId, collectedByClientId, environment}]
  async function collectGem(instanceId) {
    if (!clientId || !joined) return;
    const res = await api("/api/multiplayer/item-state", { method: "PATCH", body: JSON.stringify({
      updates: [], collections: [{ instanceId, collectedByClientId: clientId, environment: ENV }], timestamp: Date.now(),
    })});
    lastPacketMs = performance.now();
    return res.ok;
  }

  async function sendHeartbeat() {
    if (!clientId) return;
    const res = await api("/api/multiplayer/heartbeat", { method: "POST", body: "{}" });
    lastPacketMs = performance.now();
    if (res.status === 403) { log("heartbeat rejected — re-join."); await leave(); }
  }

  function handleStreamData(raw) {
    try {
      let text = String(raw || "").trim(); if (text.startsWith("signals ")) text = text.slice("signals ".length);
      const data = JSON.parse(text); const signals = data.signals || data;
      const mp = signals.mp || (signals.match || signals.turnState || signals.characters || signals.items ? signals : null);
      if (mp) {
        if (mp.connection) {
          if (typeof mp.connection.isSynchronizer === "boolean") isSynchronizer = mp.connection.isSynchronizer;
          if (mp.connection.roomId) roomId = mp.connection.roomId;
        }
        if (onMp) onMp(mp);
      }
    } catch(e) { log("stream parse: " + e); }
  }
  function connectStream() {
    if (!sessionId) return;
    if (stream) stream.close();
    stream = new EventSource(`/api/multiplayer/stream?session_id=${encodeURIComponent(sessionId)}`);
    stream.addEventListener("datastar-patch-signals", (ev) => handleStreamData(ev.data));
    stream.onmessage = (ev) => handleStreamData(ev.data);
    stream.onerror = () => { log("stream error — EventSource will retry"); };
  }

  // Per-frame helper: send on anim-change, else on move, else heartbeat every 10s.
  function tick(me, moved) {
    if (!joined) return;
    const anim = me.metadata?.animationState;
    if (anim !== lastSentAnim) { lastSentAnim = anim; sendCharacterState(me, true); }
    else if (moved) sendCharacterState(me, false);
    else if (performance.now() - lastPacketMs > 10000) sendHeartbeat();
  }

  return {
    get clientId() { return clientId; }, get sessionId() { return sessionId; },
    get roomId() { return roomId; }, get joined() { return joined; },
    get isSynchronizer() { return isSynchronizer; }, get GAME_ID() { return GAME_ID; }, get ENV() { return ENV; },
    set onMp(fn) { onMp = fn; }, set onLog(fn) { onLog = fn; },
    join, leave, sendCharacterState, collectGem, sendHeartbeat, tick, colorFor,
  };
}
