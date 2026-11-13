// radar.js — circular, heading-up radar. Shared by babylon / jets / any 2D-space demo.
// Usage: const radar = createRadar(canvasEl, { range: 1200 });
//        radar.draw({ yaw, meX, meZ, others: [{x, z, color, name, label}] });
// World: X east, Z south (same as to3D). Forward = (sin yaw, cos yaw). Right = (cos yaw, -sin yaw).

function createRadar(canvas, { range = 1200 } = {}) {
  const ctx = canvas.getContext("2d");
  const S = canvas.width, C = S / 2, R = C - 8;
  function draw({ yaw = 0, meX = 0, meZ = 0, others = [], selfLabel = "YOU" }) {
    ctx.clearRect(0, 0, S, S);
    ctx.strokeStyle = "rgba(120,220,255,.35)"; ctx.lineWidth = 1;
    for (const r of [R, R*0.66, R*0.33]) { ctx.beginPath(); ctx.arc(C, C, r, 0, Math.PI*2); ctx.stroke(); }
    ctx.beginPath(); ctx.moveTo(C, 6); ctx.lineTo(C, S-6); ctx.moveTo(6, C); ctx.lineTo(S-6, C); ctx.stroke();
    ctx.fillStyle = "#9fd8ff"; ctx.font = "bold 18px monospace"; ctx.textAlign = "center"; ctx.fillText("N", C, 24);
    const drawDot = (dx, dz, color, isSelf, label) => {
      const right = dx*Math.cos(yaw) - dz*Math.sin(yaw);
      const fwd = dx*Math.sin(yaw) + dz*Math.cos(yaw);
      let rx = right/range*R, ry = -fwd/range*R;
      const len = Math.hypot(rx, ry);
      if (len > R-6) { const s = (R-6)/len; rx *= s; ry *= s; }
      const x = C+rx, y = C+ry;
      if (isSelf) {
        ctx.beginPath(); ctx.moveTo(x, y-9); ctx.lineTo(x-6, y+6); ctx.lineTo(x+6, y+6); ctx.closePath();
        ctx.fillStyle = "#fff"; ctx.fill();
      } else {
        ctx.beginPath(); ctx.arc(x, y, 5, 0, Math.PI*2); ctx.fillStyle = color; ctx.fill();
        ctx.strokeStyle = "rgba(255,255,255,.85)"; ctx.lineWidth = 1; ctx.stroke();
      }
      if (label) { ctx.fillStyle = "#e9f7ff"; ctx.font = "11px monospace"; ctx.textAlign = "center"; ctx.fillText(label, x, y+16); }
    };
    for (const o of others) drawDot(o.x - meX, o.z - meZ, o.color || "#fff", false, o.name);
    drawDot(0, 0, "#fff", true, selfLabel);
  }
  return { draw, canvas };
}
