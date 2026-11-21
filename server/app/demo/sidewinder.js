// sidewinder.js
// AIM-9 style short-range missile for the Viper demo.
//
// One Sidewinder instance is one physical missile. Each jet carries four:
// two on the underside of the left wing, two on the underside of the right
// wing. A missile is either docked (attached to its jet), flying (tracking
// a target with proportional navigation), hit, or expired.
//
// The Viper demo owns the multiplayer part: it serializes each missile with
// serialize() into the per-player `missiles` character field, and rebuilds
// remote missiles from that same string. This file owns only flight,
// steering, and the mesh. It never touches fetch, SSE, or the DOM.
//
// Usage in viper.html:
//   import { Sidewinder, MOUNT_OFFSETS, SIDEWINDER_CONFIG } from "./sidewinder.js";
//   const missiles = MOUNT_OFFSETS.map((offset, slot) => new Sidewinder({
//     scene, slot, ownerId, ownerColor, mountOffset: offset, parentJet: jet
//   }));

const B = (typeof window !== "undefined" && window.BABYLON) ? window.BABYLON : null;

export const SIDEWINDER_CONFIG = {
  MAX_COUNT: 4,
  MAX_SPEED: 230,
  ACCEL: 140,
  INITIAL_SPEED: 60,
  TURN_RATE: 3.6,
  MAX_LIFE_SEC: 6.0,
  HIT_RADIUS: 9,
  START_OFFSET_M: 2.5
};

// Mount points under the wings. Two per wing, bottom of the wing.
// x is sideways, y is below the wing, z is along the fuselage (nose is +z).
export const MOUNT_OFFSETS = [
  { x: -1.8, y: -0.28, z: 0.4 },
  { x: -3.4, y: -0.28, z: -0.9 },
  { x: 1.8, y: -0.28, z: 0.4 },
  { x: 3.4, y: -0.28, z: -0.9 }
];

function clamp01(v) { return Math.max(-1, Math.min(1, v)); }

export class Sidewinder {
  constructor(opts) {
    const o = opts || {};
    this.scene = o.scene;
    this.slot = typeof o.slot === "number" ? o.slot : 0;
    this.ownerId = o.ownerId || "";
    this.ownerColor = o.ownerColor || "#ff713d";
    this.parentJet = o.parentJet || null;
    const m = o.mountOffset || MOUNT_OFFSETS[this.slot] || { x: 0, y: -0.28, z: 0 };
    this.mountOffset = new B.Vector3(m.x, m.y, m.z);
    this.status = "docked";
    this.targetId = "";
    this.age = 0;
    this.velocity = new B.Vector3(0, 0, 0);
    this.trail = [];
    this.trailTimer = 0;
    this.lastTargetPos = null;
    this.node = new B.TransformNode("sidewinder_" + this.slot + "_" + Math.random().toString(36).slice(2, 6), this.scene);
    this.node.parent = this.parentJet;
    this.node.position.copyFrom(this.mountOffset);
    this._buildMesh();
  }

  _mat(name, color, emissive) {
    const mat = new B.StandardMaterial(name + "_" + this.slot + "_" + Math.random().toString(36).slice(2, 5), this.scene);
    mat.diffuseColor = B.Color3.FromHexString(color);
    mat.specularColor = new B.Color3(0.4, 0.4, 0.4);
    if (emissive) mat.emissiveColor = B.Color3.FromHexString(emissive);
    return mat;
  }

  _buildMesh() {
    const bodyMat = this._mat("sw_body", "#dfe6ee");
    const darkMat = this._mat("sw_dark", "#243247");
    const tipMat = this._mat("sw_tip", "#ff713d", "#7a2a00");
    const body = B.MeshBuilder.CreateCylinder("sw_body", { height: 2.0, diameterTop: 0.26, diameterBottom: 0.26, tessellation: 16 }, this.scene);
    body.rotation.x = Math.PI / 2;
    body.parent = this.node;
    body.material = bodyMat;
    const nose = B.MeshBuilder.CreateCylinder("sw_nose", { height: 0.6, diameterTop: 0, diameterBottom: 0.26, tessellation: 16 }, this.scene);
    nose.rotation.x = Math.PI / 2;
    nose.position.set(0, 0, 1.3);
    nose.parent = this.node;
    nose.material = tipMat;
    const tail = B.MeshBuilder.CreateCylinder("sw_tail", { height: 0.25, diameterTop: 0.28, diameterBottom: 0.28, tessellation: 16 }, this.scene);
    tail.rotation.x = Math.PI / 2;
    tail.position.set(0, 0, -1.05);
    tail.parent = this.node;
    tail.material = darkMat;
    for (let i = 0; i < 4; i++) {
      const fin = B.MeshBuilder.CreateBox("sw_fin", { width: 0.04, height: 0.45, depth: 0.5 }, this.scene);
      const a = (i / 4) * Math.PI * 2;
      fin.position.set(Math.cos(a) * 0.2, Math.sin(a) * 0.2, -0.85);
      fin.rotation.z = a;
      fin.parent = this.node;
      fin.material = darkMat;
    }
    const flameMat = this._mat("sw_flame", "#ff9b35", "#ff6515");
    flameMat.disableLighting = true;
    this.exhaust = B.MeshBuilder.CreateSphere("sw_flame", { diameter: 0.35, segments: 10 }, this.scene);
    this.exhaust.position.set(0, 0, -1.35);
    this.exhaust.parent = this.node;
    this.exhaust.material = flameMat;
    this.exhaust.setEnabled(false);
    this.bodyMeshes = [body, nose, tail];
  }

  getWorldPosition() {
    try {
      const p = this.node.getAbsolutePosition();
      return new B.Vector3(p.x, p.y, p.z);
    } catch (e) {
      return this.node.position.clone();
    }
  }

  isDocked() { return this.status === "docked"; }
  isFlying() { return this.status === "flying"; }
  isDone() { return this.status === "hit" || this.status === "expired"; }

  mount() {
    this.status = "docked";
    this.targetId = "";
    this.age = 0;
    this.velocity.set(0, 0, 0);
    this.lastTargetPos = null;
    this._clearTrail();
    this.node.parent = this.parentJet;
    this.node.position.copyFrom(this.mountOffset);
    this.node.rotation.set(0, 0, 0);
    this.node.setEnabled(true);
    if (this.exhaust) this.exhaust.setEnabled(false);
    return this;
  }

  launch(targetId, startPos, startVel) {
    this.status = "flying";
    this.targetId = targetId || "";
    this.age = 0;
    this.node.parent = null;
    if (startPos) this.node.position.copyFrom(startPos);
    if (startVel) this.velocity.copyFrom(startVel);
    if (this.velocity.length() < 1) this.velocity.set(0, 0, SIDEWINDER_CONFIG.INITIAL_SPEED);
    this._orientToVelocity();
    this.node.setEnabled(true);
    if (this.exhaust) this.exhaust.setEnabled(true);
    return this;
  }

  _orientToVelocity() {
    const v = this.velocity;
    const speed = Math.max(1, v.length());
    const yaw = Math.atan2(v.x, v.z);
    const pitch = Math.asin(clamp01(v.y / speed));
    this.node.rotation.set(-pitch, yaw, 0);
  }

  _spawnTrailPuff() {
    if (!this.scene) return;
    const mat = new B.StandardMaterial("sw_trail_" + Math.random().toString(36).slice(2, 6), this.scene);
    mat.diffuseColor = new B.Color3(0.85, 0.85, 0.88);
    mat.emissiveColor = new B.Color3(0.35, 0.35, 0.35);
    mat.alpha = 0.55;
    const puff = B.MeshBuilder.CreateSphere("sw_trail", { diameter: 0.7, segments: 8 }, this.scene);
    puff.position.copyFrom(this.node.position);
    puff.material = mat;
    puff.scaling.set(0.6, 0.6, 0.6);
    this.trail.push({ mesh: puff, mat, age: 0 });
  }

  _updateTrail(dt) {
    for (let i = this.trail.length - 1; i >= 0; i--) {
      const t = this.trail[i];
      t.age += dt;
      const s = 0.6 + t.age * 2.2;
      t.mesh.scaling.set(s, s, s);
      t.mat.alpha = Math.max(0, 0.55 - t.age * 0.8);
      if (t.age > 0.7) {
        try { t.mesh.dispose(); } catch (e) {}
        try { t.mat.dispose(); } catch (e) {}
        this.trail.splice(i, 1);
      }
    }
  }

  _clearTrail() {
    for (const t of this.trail) {
      try { t.mesh.dispose(); } catch (e) {}
      try { t.mat.dispose(); } catch (e) {}
    }
    this.trail = [];
  }

  // Proper follow pattern: proportional navigation toward a predicted
  // intercept point, with a capped turn rate so the missile cannot snap
  // instantly onto the target. Returns the new status.
  update(dt, targetPos, targetVel) {
    if (this.status !== "flying") {
      this._updateTrail(dt);
      return this.status;
    }
    this.age += dt;
    if (this.age > SIDEWINDER_CONFIG.MAX_LIFE_SEC) {
      this.status = "expired";
      if (this.exhaust) this.exhaust.setEnabled(false);
      return this.status;
    }
    if (targetPos) this.lastTargetPos = targetPos.clone();
    const speed = Math.min(SIDEWINDER_CONFIG.MAX_SPEED, this.velocity.length() + SIDEWINDER_CONFIG.ACCEL * dt);
    let desired = null;
    const aimAt = targetPos || this.lastTargetPos;
    if (aimAt) {
      const toTarget = aimAt.subtract(this.node.position);
      const dist = Math.max(1, toTarget.length());
      const closing = Math.max(20, speed);
      const timeToGo = dist / closing;
      const predicted = aimAt.clone();
      if (targetVel) predicted.addInPlace(targetVel.scale(timeToGo * 0.9));
      desired = predicted.subtract(this.node.position);
      if (desired.length() < 0.001) desired = toTarget;
      desired.normalize();
    }
    if (desired) {
      const current = this.velocity.clone();
      if (current.length() < 0.001) current.copyFrom(desired);
      current.normalize();
      const dot = clamp01(B.Vector3.Dot(current, desired));
      const angle = Math.acos(dot);
      const maxTurn = SIDEWINDER_CONFIG.TURN_RATE * dt;
      let nextDir = desired;
      if (angle > 0.0001 && angle > maxTurn) {
        const t = maxTurn / angle;
        nextDir = B.Vector3.Lerp(current, desired, t);
        nextDir.normalize();
      }
      this.velocity.copyFrom(nextDir.scale(speed));
    } else {
      this.velocity.normalize();
      this.velocity.scaleInPlace(speed);
    }
    this.node.position.addInPlace(this.velocity.scale(dt));
    this._orientToVelocity();
    this.trailTimer += dt;
    if (this.trailTimer > 0.03) {
      this.trailTimer = 0;
      this._spawnTrailPuff();
    }
    this._updateTrail(dt);
    return this.status;
  }

  checkHit(targetPos) {
    if (this.status !== "flying" || !targetPos) return false;
    const d = B.Vector3.Distance(this.node.position, targetPos);
    return d < SIDEWINDER_CONFIG.HIT_RADIUS;
  }

  markHit() {
    if (this.status === "flying") this.status = "hit";
    if (this.exhaust) this.exhaust.setEnabled(false);
    return this.getWorldPosition();
  }

  markExpired() {
    if (this.status === "flying") this.status = "expired";
    if (this.exhaust) this.exhaust.setEnabled(false);
    return this;
  }

  hide() {
    this.node.setEnabled(false);
    this._clearTrail();
    return this;
  }

  // Compact, readable shape for the multiplayer `missiles` string field.
  // Docked missiles send no position: remotes render them on the wing.
  serialize() {
    if (this.status === "docked") {
      return { slot: this.slot, target: "", x: 0, y: 0, z: 0, age: 0, status: "docked" };
    }
    const p = this.node.position;
    return { slot: this.slot, target: this.targetId, x: Math.round(p.x * 10) / 10, y: Math.round(p.y * 10) / 10, z: Math.round(p.z * 10) / 10, age: Math.round(this.age * 100) / 100, status: this.status };
  }

  dispose() {
    this._clearTrail();
    try { this.node.dispose(); } catch (e) {}
  }
}

export function parseMissilesString(str) {
  if (!str || typeof str !== "string") return [];
  try {
    const arr = JSON.parse(str);
    if (!Array.isArray(arr)) return [];
    return arr.filter((m) => m && typeof m.slot === "number" && typeof m.status === "string");
  } catch (e) {
    return [];
  }
}

export function createSidewinders(scene, parentJet, ownerId, ownerColor) {
  return MOUNT_OFFSETS.map((offset, slot) => new Sidewinder({
    scene, slot, ownerId, ownerColor, mountOffset: offset, parentJet
  }));
}
