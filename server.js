"use strict";
/**
 * Project Void — Party Combat WebSocket Server
 * Deploy on Railway. Connects to Firebase RTDB via Admin SDK.
 *
 * Flow:
 *   1. Client connects via WebSocket, sends { type:"auth", idToken, uid }
 *   2. Server verifies token with Firebase Auth
 *   3. Party leader sends { type:"start_combat", partyId, enemyList }
 *   4. Server loads all party member saves from Firebase
 *   5. Server runs real-time combat loop (500ms tick)
 *   6. All party members receive live state pushes
 *   7. On victory/death, server writes results back to Firebase saves
 *
 * Environment variables (set in Railway dashboard):
 *   FIREBASE_PROJECT_ID        — e.g. project-void-login
 *   FIREBASE_DATABASE_URL      — e.g. https://project-void-login-default-rtdb.firebaseio.com
 *   FIREBASE_SERVICE_ACCOUNT   — full service account JSON as a single string
 *   PORT                       — set automatically by Railway
 */

const http    = require("http");
const { WebSocketServer } = require("ws");
const admin   = require("firebase-admin");

// ── Firebase init ─────────────────────────────────────────────────────────────
const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT || "{}");
admin.initializeApp({
  credential:  admin.credential.cert(serviceAccount),
  databaseURL: process.env.FIREBASE_DATABASE_URL,
});
const db   = admin.database();
const auth = admin.auth();

// ── Game constants (mirrors index.js) ────────────────────────────────────────
const ACTIONS_DB = {
  basic_attack:   { name:"Basic Attack",   damage:[5,10],  difficulty:20, cooldown:0 },
  quick_strike:   { name:"Quick Strike",   damage:[1,7],   difficulty:5,  cooldown:15000, requiresWeaponType:"melee" },
  reckless_swing: { name:"Reckless Swing", damage:[10,15], difficulty:40, cooldown:20000, requiresWeaponType:"melee" },
  focused_shot:   { name:"Focused Shot",   damage:[1,7],   difficulty:5,  cooldown:15000, requiresWeaponType:"ranged" },
  overdraw_shot:  { name:"Overdraw Shot",  damage:[10,15], difficulty:40, cooldown:20000, requiresWeaponType:"ranged" },
  fireball:       { name:"Fireball",       damage:[7,8],   difficulty:25, cooldown:50000, requiresWeaponType:"magic", burnEffect:true },
  bite:            { name:"Bite",           damage:[2,5],   difficulty:25, cooldown:0 },
  goblin_bonk:    { name:"Bonk",           damage:[4,10],  difficulty:25, cooldown:0 },
  flee:           { name:"Flee",           difficulty:1,  isFlee:true },
};

const ENEMIES_DB = {
  goblin: { name:"Goblin", maxHp:40, actions:["goblin_bonk"], loot:[{ type:"gold", qty:[4,10], chance:100 }] },
  wolf:   { name:"Wolf",   maxHp:20, actions:["bite"],        loot:[{ type:"gold", qty:[2,5],  chance:100 }] },
};

// ── Zone definitions (for server-side enemy selection) ─────────────────────
const ZONES = {
  duskwatch:     { safe:true,  enemies:[] },
  goblin_forest: { safe:false, enemies:["goblin","wolf"] },
};

// ── Item databases (for server-side buy/sell validation) ──────────────────
const ALL_ITEMS = {};
// Equipment
[
  {id:"dagger",name:"Dagger",cost:100,type:"weapon",weaponType:"melee",atk:3,acc:2,zones:["duskwatch"]},
  {id:"short_bow",name:"Short Bow",cost:100,type:"weapon",weaponType:"ranged",atk:1,acc:6,zones:["duskwatch"]},
  {id:"wand",name:"Wand",cost:100,type:"weapon",weaponType:"magic",atk:5,zones:["duskwatch"]},
  {id:"adventurers_robe",name:"Adventurer's Robe",cost:100,type:"armor",armorType:"light",atk:1,maxHp:20,zones:["duskwatch"]},
  {id:"adventurers_cuirass",name:"Adventurer's Cuirass",cost:100,type:"armor",armorType:"medium",acc:2,maxHp:30,zones:["duskwatch"]},
  {id:"adventurers_platemail",name:"Adventurer's Platemail",cost:100,type:"armor",armorType:"heavy",maxHp:40,zones:["duskwatch"]},
].forEach(i => ALL_ITEMS[i.id] = i);
// Provisions
[
  {id:"health_potion",name:"Health Potion",cost:25,type:"provision",healHp:50,zones:["duskwatch"]},
].forEach(i => ALL_ITEMS[i.id] = i);
// Actions (learnable)
[
  {id:"quick_strike",name:"Quick Strike",cost:100,category:"action",zones:["duskwatch"]},
  {id:"reckless_swing",name:"Reckless Swing",cost:100,category:"action",zones:["duskwatch"]},
  {id:"focused_shot",name:"Focused Shot",cost:100,category:"action",zones:["duskwatch"]},
  {id:"overdraw_shot",name:"Overdraw Shot",cost:100,category:"action",zones:["duskwatch"]},
  {id:"fireball",name:"Fireball",cost:500,category:"action",zones:["duskwatch"]},
].forEach(i => ALL_ITEMS[i.id] = i);

function getItemSellValue(item) {
  if (item.sellValue != null) return item.sellValue;
  return Math.floor((item.cost || 0) * 0.50);
}
function getInvKey(item) {
  if (item.type === "provision") return "provisions";
  if (item.type === "material") return "materials";
  return item.type === "armor" ? "armor" : item.type === "weapon" ? "weapons" : "accessories";
}

// ── Fish database (for server-side catch validation) ──────────────────────
const MATERIALS_DB = {
  trout:      {id:"trout",      name:"Trout",      rarity:"common",   type:"material", sellValue:1},
  perch:      {id:"perch",      name:"Perch",      rarity:"uncommon", type:"material", sellValue:5},
  bluegill:   {id:"bluegill",   name:"Bluegill",   rarity:"rare",     type:"material", sellValue:10},
  shadow_eel: {id:"shadow_eel", name:"Shadow Eel", rarity:"epic",     type:"material", sellValue:50},
};
const VALID_FISH = new Set(Object.keys(MATERIALS_DB));
// Track last catch time per player for cooldown enforcement
const _fishCooldowns = new Map(); // uid -> lastCatchTs
const FISH_COOLDOWN_MS = 8000; // Minimum 8 seconds between catches (minigame takes ~10-30s)
// Track epic fish frequency for anomaly detection
const _fishEpicLog = new Map(); // uid -> [timestamps of epic rarity catches]

// ── Anomaly flagging ─────────────────────────────────────────────────────
// Logs suspicious activity. In production, could write to Firebase or external service.
const _anomalyLog = [];
function flagAnomaly(uid, reason, details) {
  const entry = { uid, reason, details, ts: Date.now() };
  _anomalyLog.push(entry);
  if (_anomalyLog.length > 5000) _anomalyLog.shift();
  console.warn(`[ANOMALY] uid=${uid} reason=${reason}`, details || "");
  // Write to Firebase for persistent tracking
  db.ref(`pv/anomalies/${uid}`).push(entry).catch(() => {});
}

const TICK_MS        = 1000;
const ENERGY_PER_TICK = 10;
const ENERGY_DELAY_MS = 1000;
const ENERGY_TO_ACT   = 80;
const ENERGY_TO_PLAYER= 80;

const roll  = (a,b) => Math.floor(Math.random()*(b-a+1))+a;
const rng   = ()    => Math.floor(Math.random()*100)+1;
const clamp = (v,a,b) => Math.max(a,Math.min(b,v));

// ── Admin UIDs ───────────────────────────────────────────────────────────
const ADMIN_UIDS = new Set(["PyzoMFRK9bUi337iMR7Dym7maFK2"]);

// ── Connected clients: uid → { ws, uid, username } ───────────────────────────
const clients = new Map();

// ── Active combat rooms: roomId → CombatRoom ─────────────────────────────────
const rooms = new Map();

// ── HTTP server (Railway health check) ───────────────────────────────────────
const server = http.createServer((req, res) => {
  res.writeHead(200);
  res.end("Project Void Combat Server OK");
});

const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
  let clientUid = null;
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    try {
    // Log admin message attempts for debugging
    if (msg.type && msg.type.startsWith("admin_")) {
      console.log(`[WS] admin msg type="${msg.type}" clientUid=${clientUid} isAdmin=${clientUid ? ADMIN_UIDS.has(clientUid) : false}`);
    }
    switch (msg.type) {

      // ── Auth ────────────────────────────────────────────────────────────────
      case "auth": {
        try {
          const decoded = await auth.verifyIdToken(msg.idToken);
          clientUid = decoded.uid;
          // Single-session enforcement: kick previous connection if exists
          const existingClient = clients.get(clientUid);
          if (existingClient && existingClient.ws !== ws && existingClient.ws.readyState === 1) {
            send(existingClient.ws, { type:"kicked", reason:"logged_in_elsewhere" });
            existingClient.ws.close();
            console.log(`[AUTH] kicked previous session for uid=${clientUid}`);
          }
          clients.set(clientUid, { ws, uid: clientUid, username: msg.username || "" });
          // Check if banned
          const banSnap = await db.ref(`pv/saves/${clientUid}/banned`).get();
          if (banSnap.exists() && banSnap.val() === true) {
            send(ws, { type:"banned" });
            console.log(`[AUTH] BANNED uid=${clientUid}`);
            return;
          }
          send(ws, { type:"auth_ok", uid: clientUid, admin: ADMIN_UIDS.has(clientUid) });
          console.log(`[AUTH] uid=${clientUid} username=${msg.username}`);
          // Check if this player is in an active combat room — send rejoin signal
          for (const [roomId, room] of rooms) {
            if (room.ended) continue;
            const member = room.members.find(m => m.uid === clientUid);
            if (member) {
              console.log(`[REJOIN] uid=${clientUid} rejoining room=${roomId}`);
              send(ws, { type:"combat_rejoin", roomId });
              break;
            }
          }
        } catch (e) {
          send(ws, { type:"auth_fail", reason: e.message });
        }
        break;
      }

      // ── Start combat (leader only) ─────────────────────────────────────────
      case "start_combat": {
        if (!clientUid) { send(ws, { type:"error", reason:"not_authed" }); return; }
        if (rooms.size >= MAX_ROOMS) { send(ws, { type:"error", reason:"server_full" }); return; }
        if (!_rateOk(clientUid, "start_combat")) { send(ws, { type:"error", reason:"rate_limited" }); return; }
        const { memberUids, zoneId } = msg;
        // Server picks enemies from zone pool — client cannot choose enemies
        const zone = ZONES[zoneId];
        if (!zone || zone.safe || !zone.enemies.length) {
          send(ws, { type:"error", reason:"invalid_zone" }); return;
        }
        const count = roll(1, 4);
        const enemyList = Array.from({length: count}, (_, i) => ({
          uid: `e_${i}_${Date.now()}`,
          type: zone.enemies[Math.floor(Math.random() * zone.enemies.length)]
        }));
        // Allow solo: auto-generate a partyId if none provided
        const partyId = msg.partyId || `solo_${clientUid}_${Date.now()}`;
        // Solo combat: just the player themselves
        const memberUidList = memberUids?.length ? memberUids : [clientUid];
        try {
          const room = await CombatRoom.create(partyId, memberUidList, enemyList);
          rooms.set(partyId, room);
          room.start();
          console.log(`[ROOM] created partyId=${partyId} members=${memberUidList.length} enemies=${enemyList.length} zone=${zoneId}`);
        } catch (e) {
          console.error("[ROOM] create error:", e);
          send(ws, { type:"error", reason:"start_failed", detail: e.message });
        }
        break;
      }

      // ── Rejoin combat (reconnecting player) ─────────────────────────────────
      case "rejoin_combat": {
        if (!clientUid) return;
        const { roomId } = msg;
        const room = rooms.get(roomId);
        if (room && !room.ended) {
          const member = room.members.find(m => m.uid === clientUid);
          if (member) {
            room.sendFullState(clientUid);
            console.log(`[REJOIN] sent full_state to uid=${clientUid} room=${roomId}`);
          }
        } else {
          send(ws, { type:"no_active_combat" });
        }
        break;
      }

      // ── Player action ──────────────────────────────────────────────────────
      case "action": {
        if (!clientUid) return;
        if (!_rateOk(clientUid, "action")) return;
        const room = findRoomForUid(clientUid);
        if (!room) { send(ws, { type:"error", reason:"not_in_combat" }); return; }
        room.handleAction(clientUid, msg);
        break;
      }

      // ── Flee ───────────────────────────────────────────────────────────────
      case "flee": {
        if (!clientUid) return;
        if (!_rateOk(clientUid, "action")) return;
        const room = findRoomForUid(clientUid);
        if (room) room.handleFlee(clientUid);
        break;
      }

      // ── Reconnect to existing room ─────────────────────────────────────────
      case "rejoin": {
        if (!clientUid) return;
        const room = findRoomForUid(clientUid);
        if (room) {
          // Update ws reference in case they reconnected
          const client = clients.get(clientUid);
          if (client) client.ws = ws;
          room.sendFullState(clientUid);
        } else {
          send(ws, { type:"no_active_combat" });
        }
        break;
      }

      // ── Server-side buy ─────────────────────────────────────────────────────
      case "buy_item": {
        if (!clientUid) { send(ws, { type:"error", reason:"not_authed" }); return; }
        if (!_rateOk(clientUid, "action")) { send(ws, { type:"error", reason:"rate_limited" }); return; }
        const { itemId, qty: buyQty } = msg;
        const itemDef = ALL_ITEMS[itemId];
        if (!itemDef) { send(ws, { type:"buy_fail", reason:"unknown_item" }); return; }
        const cost = itemDef.cost * (buyQty || 1);
        try {
          const snap = await db.ref(`pv/saves/${clientUid}`).get();
          if (!snap.exists()) { send(ws, { type:"buy_fail", reason:"no_save" }); return; }
          const save = snap.val();
          const p = save.player;
          if (!p || p.gold < cost) {
            flagAnomaly(clientUid, "buy_insufficient_gold", { itemId, gold: p?.gold, cost });
            send(ws, { type:"buy_fail", reason:"not_enough_gold" }); return;
          }
          p.gold -= cost;
          if (itemDef.category === "action") {
            const learned = p.learnedActions || [];
            if (learned.includes(itemId)) { send(ws, { type:"buy_fail", reason:"already_learned" }); return; }
            p.learnedActions = [...learned, itemId];
          } else {
            const inv = p.inventory || {};
            const key = getInvKey(itemDef);
            const arr = inv[key] || [];
            if (itemDef.type === "provision" || itemDef.type === "material") {
              const idx = arr.findIndex(i => i.id === itemId);
              const bq = buyQty || 1;
              if (idx >= 0) arr[idx] = { ...arr[idx], qty: (arr[idx].qty || 1) + bq };
              else arr.push({ ...itemDef, qty: bq });
            } else {
              for (let i = 0; i < (buyQty || 1); i++) arr.push({ ...itemDef });
            }
            inv[key] = arr;
            p.inventory = inv;
          }
          p.stats = p.stats || {};
          p.stats.goldForest = (p.stats.goldForest || 0);
          await db.ref(`pv/saves/${clientUid}/player`).update(p);
          send(ws, { type:"buy_ok", itemId, gold: p.gold, player: p });
          console.log(`[BUY] uid=${clientUid} item=${itemId} cost=${cost} gold=${p.gold}`);
        } catch (e) {
          console.error("[BUY] error:", e.message);
          send(ws, { type:"buy_fail", reason:"server_error" });
        }
        break;
      }

      // ── Server-side sell ────────────────────────────────────────────────────
      case "sell_item": {
        if (!clientUid) { send(ws, { type:"error", reason:"not_authed" }); return; }
        if (!_rateOk(clientUid, "action")) { send(ws, { type:"error", reason:"rate_limited" }); return; }
        const { itemId: sellItemId, invKey: sellKey, qty: sellQty } = msg;
        if (!sellItemId || !sellKey) { send(ws, { type:"sell_fail", reason:"bad_request" }); return; }
        try {
          const snap = await db.ref(`pv/saves/${clientUid}`).get();
          if (!snap.exists()) { send(ws, { type:"sell_fail", reason:"no_save" }); return; }
          const save = snap.val();
          const p = save.player;
          const inv = p.inventory || {};
          const arr = inv[sellKey] || [];
          const idx = arr.findIndex(i => i.id === sellItemId);
          if (idx < 0) {
            flagAnomaly(clientUid, "sell_item_not_found", { sellItemId, sellKey });
            send(ws, { type:"sell_fail", reason:"item_not_found" }); return;
          }
          const item = arr[idx];
          const sv = item.sellValue != null ? item.sellValue : Math.floor((item.cost || 0) * 0.50);
          const sq = sellQty || 1;
          // Validate qty
          if (item.qty != null && item.qty < sq) {
            send(ws, { type:"sell_fail", reason:"not_enough_qty" }); return;
          }
          const gold = sv * sq;
          // Check if item is equipped
          const eq = p.equipment || {};
          if ((eq.weapon && eq.weapon.id === sellItemId) ||
              (eq.armor && eq.armor.id === sellItemId) ||
              ((eq.accessories || []).some(a => a && a.id === sellItemId))) {
            send(ws, { type:"sell_fail", reason:"equipped" }); return;
          }
          // Remove from inventory
          if (item.qty != null) {
            if (item.qty <= sq) arr.splice(idx, 1);
            else arr[idx] = { ...item, qty: item.qty - sq };
          } else { arr.splice(idx, 1); }
          inv[sellKey] = arr;
          p.inventory = inv;
          p.gold = (p.gold || 0) + gold;
          p.stats = p.stats || {};
          p.stats.goldFromSelling = (p.stats.goldFromSelling || 0) + gold;
          await db.ref(`pv/saves/${clientUid}/player`).update(p);
          send(ws, { type:"sell_ok", itemId: sellItemId, gold: p.gold, earnedGold: gold, player: p });
          console.log(`[SELL] uid=${clientUid} item=${sellItemId} qty=${sq} earned=${gold} gold=${p.gold}`);
        } catch (e) {
          console.error("[SELL] error:", e.message);
          send(ws, { type:"sell_fail", reason:"server_error" });
        }
        break;
      }

      // ── Server-side fish catch ─────────────────────────────────────────────
      case "fish_catch": {
        if (!clientUid) { send(ws, { type:"error", reason:"not_authed" }); return; }
        if (!_rateOk(clientUid, "action")) { send(ws, { type:"error", reason:"rate_limited" }); return; }
        const { fishId: caughtFishId } = msg;
        // Validate fish exists
        if (!caughtFishId || !VALID_FISH.has(caughtFishId)) {
          flagAnomaly(clientUid, "invalid_fish", { fishId: caughtFishId });
          send(ws, { type:"fish_fail", reason:"invalid_fish" }); return;
        }
        // Enforce cooldown
        const now = Date.now();
        const lastCatch = _fishCooldowns.get(clientUid) || 0;
        if (now - lastCatch < FISH_COOLDOWN_MS) {
          flagAnomaly(clientUid, "fish_cooldown_bypass", { elapsed: now - lastCatch, min: FISH_COOLDOWN_MS });
          send(ws, { type:"fish_fail", reason:"cooldown" }); return;
        }
        _fishCooldowns.set(clientUid, now);
        // Track epic rarity catches for anomaly detection
        const fishDef = MATERIALS_DB[caughtFishId];
        if (fishDef.rarity === "epic") {
          if (!_fishEpicLog.has(clientUid)) _fishEpicLog.set(clientUid, []);
          const log = _fishEpicLog.get(clientUid);
          log.push(now);
          // Keep last hour only
          const hourAgo = now - 3600000;
          while (log.length && log[0] < hourAgo) log.shift();
          // More than 10 epic rarity fish per hour is suspicious
          if (log.length > 10) {
            flagAnomaly(clientUid, "excessive_epic_fish", { count: log.length, window: "1h" });
          }
        }
        try {
          const snap = await db.ref(`pv/saves/${clientUid}`).get();
          if (!snap.exists()) { send(ws, { type:"fish_fail", reason:"no_save" }); return; }
          const save = snap.val();
          const p = save.player;
          const mats = p.inventory?.materials || [];
          const idx = mats.findIndex(m => m.id === caughtFishId);
          if (idx >= 0) mats[idx] = { ...mats[idx], qty: (mats[idx].qty || 1) + 1 };
          else mats.push({ ...fishDef, qty: 1 });
          p.inventory = p.inventory || {};
          p.inventory.materials = mats;
          p.stats = p.stats || {};
          p.stats.fishCaught = (p.stats.fishCaught || 0) + 1;
          p.stats.fishCounts = p.stats.fishCounts || {};
          p.stats.fishCounts[caughtFishId] = (p.stats.fishCounts[caughtFishId] || 0) + 1;
          await db.ref(`pv/saves/${clientUid}/player`).update(p);
          send(ws, { type:"fish_ok", fishId: caughtFishId, player: p });
          console.log(`[FISH] uid=${clientUid} caught=${caughtFishId} rarity=${fishDef.rarity}`);
        } catch (e) {
          console.error("[FISH] error:", e.message);
          send(ws, { type:"fish_fail", reason:"server_error" });
        }
        break;
      }

      // ── Admin commands ─────────────────────────────────────────────────────
      case "admin_get_flags": {
        if (!clientUid || !ADMIN_UIDS.has(clientUid)) { send(ws, { type:"admin_flags", data:{} }); break; }
        console.log(`[ADMIN] get_flags requested by uid=${clientUid}`);
        try {
          const snap = await db.ref("pv/anomalies").get();
          const raw = snap.exists() ? snap.val() : {};
          // Only send last 5000 entries per uid
          const data = {};
          for (const [uid, entries] of Object.entries(raw)) {
            if (entries && typeof entries === "object") {
              const keys = Object.keys(entries);
              if (keys.length > 5000) {
                const trimmed = {};
                keys.slice(-5000).forEach(k => { trimmed[k] = entries[k]; });
                data[uid] = trimmed;
              } else {
                data[uid] = entries;
              }
            }
          }
          console.log(`[ADMIN] get_flags returning ${Object.keys(data).length} UIDs`);
          send(ws, { type:"admin_flags", data });
        } catch (e) {
          console.error("[ADMIN] get_flags error:", e.message);
          send(ws, { type:"admin_flags", data: {} });
        }
        break;
      }

      case "admin_get_player": {
        if (!clientUid || !ADMIN_UIDS.has(clientUid)) return;
        const { targetUid } = msg;
        if (!targetUid) return;
        try {
          const snap = await db.ref(`pv/saves/${targetUid}`).get();
          const player = snap.exists() ? snap.val()?.player : null;
          const banned = snap.exists() ? snap.val()?.banned || false : false;
          const accSnap = await db.ref("pv/accounts").orderByChild("uid").equalTo(targetUid).limitToFirst(1).get();
          let username = null;
          if (accSnap.exists()) { const entries = accSnap.val(); username = Object.keys(entries)[0]; }
          send(ws, { type:"admin_player_data", targetUid, username, player, banned });
        } catch (e) {
          send(ws, { type:"admin_player_data", targetUid, player: null });
        }
        break;
      }

      case "admin_lookup_name": {
        if (!clientUid || !ADMIN_UIDS.has(clientUid)) return;
        const { charName } = msg;
        if (!charName) return;
        try {
          const cnSnap = await db.ref(`pv/charnames/${charName.toLowerCase()}`).get();
          if (!cnSnap.exists()) { send(ws, { type:"admin_lookup_result", found:false, charName }); return; }
          const entry = cnSnap.val();
          const uid = entry.uid;
          const username = entry.username;
          if (!uid) { send(ws, { type:"admin_lookup_result", found:false, charName }); return; }
          const saveSnap = await db.ref(`pv/saves/${uid}`).get();
          const player = saveSnap.exists() ? saveSnap.val()?.player : null;
          const banned = saveSnap.exists() ? saveSnap.val()?.banned || false : false;
          send(ws, { type:"admin_player_data", targetUid:uid, username, player, banned });
        } catch (e) {
          send(ws, { type:"admin_lookup_result", found:false, charName });
        }
        break;
      }

      case "admin_ban": {
        if (!clientUid || !ADMIN_UIDS.has(clientUid)) return;
        const { targetUid: banUid, banned } = msg;
        if (!banUid) return;
        try {
          await db.ref(`pv/saves/${banUid}/banned`).set(banned !== false);
          if (banned !== false) {
            await db.ref(`pv/saves/${banUid}/bannedAt`).set(Date.now());
            // Kick the banned player if they're connected
            const bannedClient = clients.get(banUid);
            if (bannedClient && bannedClient.ws.readyState === 1) {
              send(bannedClient.ws, { type:"banned" });
              bannedClient.ws.close();
            }
          } else {
            await db.ref(`pv/saves/${banUid}/bannedAt`).remove();
          }
          send(ws, { type:"admin_action_ok", action: banned !== false ? "ban" : "unban", targetUid: banUid });
          flagAnomaly(banUid, banned !== false ? "admin_ban" : "admin_unban", { by: clientUid });
          console.log(`[ADMIN] ${banned!==false?"ban":"unban"} uid=${banUid} by=${clientUid}`);
        } catch (e) {
          send(ws, { type:"admin_action_fail", action:"ban", reason: e.message });
        }
        break;
      }

      case "admin_warn": {
        if (!clientUid || !ADMIN_UIDS.has(clientUid)) return;
        const { targetUid: warnUid, message: warnMsg } = msg;
        if (!warnUid || !warnMsg) return;
        try {
          const key = `admin_warn_${Date.now()}`;
          await db.ref(`pv/inbox/${warnUid}/system/${key}`).set({ m: warnMsg, t: Date.now(), from:"SYSTEM" });
          flagAnomaly(warnUid, "admin_warn", { by: clientUid, message: warnMsg });
          send(ws, { type:"admin_action_ok", action:"warn", targetUid: warnUid });
          console.log(`[ADMIN] warn uid=${warnUid} msg="${warnMsg}" by=${clientUid}`);
        } catch (e) {
          send(ws, { type:"admin_action_fail", action:"warn", reason: e.message });
        }
        break;
      }

      case "admin_clear_flags": {
        if (!clientUid || !ADMIN_UIDS.has(clientUid)) return;
        const { targetUid: cfUid } = msg;
        if (!cfUid) return;
        try {
          await db.ref(`pv/anomalies/${cfUid}`).remove();
          send(ws, { type:"admin_action_ok", action:"clear_flags", targetUid: cfUid });
          console.log(`[ADMIN] clear_flags uid=${cfUid} by=${clientUid}`);
        } catch (e) {
          send(ws, { type:"admin_action_fail", action:"clear_flags", reason: e.message });
        }
        break;
      }

      case "admin_get_banned": {
        if (!clientUid || !ADMIN_UIDS.has(clientUid)) { send(ws, { type:"admin_banned_list", players:[] }); break; }
        console.log(`[ADMIN] get_banned requested by uid=${clientUid}`);
        try {
          const savesSnap = await db.ref("pv/saves").get();
          const players = [];
          if (savesSnap.exists()) {
            const saves = savesSnap.val();
            for (const uid of Object.keys(saves)) {
              const save = saves[uid];
              if (save && save.banned === true) {
                const name = (save.player && save.player.name) || null;
                players.push({ uid, name, bannedAt: save.bannedAt || null });
              }
            }
          }
          console.log(`[ADMIN] get_banned found=${players.length}`);
          send(ws, { type:"admin_banned_list", players });
        } catch (e) {
          console.error("[ADMIN] get_banned error:", e.message);
          send(ws, { type:"admin_banned_list", players: [] });
        }
        break;
      }

      // ── Save audit: validate client save before accepting ───────────────────
      case "save_audit": {
        if (!clientUid) return;
        // Client sends its save snapshot for comparison — server checks for anomalies
        const { gold, hp, maxHp } = msg;
        try {
          const snap = await db.ref(`pv/saves/${clientUid}/player`).get();
          if (!snap.exists()) return;
          const serverP = snap.val();
          // Check gold discrepancy
          if (gold != null && serverP.gold != null) {
            const diff = gold - serverP.gold;
            if (diff > 0 && diff !== 0) {
              // Client claims more gold than server has — possible cheat
              flagAnomaly(clientUid, "gold_discrepancy", { clientGold: gold, serverGold: serverP.gold, diff });
            }
          }
          // Check HP above maxHp
          if (hp != null && maxHp != null && hp > maxHp) {
            flagAnomaly(clientUid, "hp_exceeds_max", { hp, maxHp });
          }
        } catch (e) {}
        break;
      }
      default:
        if (clientUid) console.log(`[WS] unknown msg type="${msg.type}" from uid=${clientUid}`);
        break;
    }
    } catch (topErr) {
      console.error(`[WS] UNHANDLED ERROR in msg type="${msg.type}" uid=${clientUid}:`, topErr.message, topErr.stack);
    }
  });

  ws.on("close", () => {
    if (clientUid) {
      console.log(`[DISCONNECT] uid=${clientUid}`);
      clients.delete(clientUid);
      // Room persists — player can rejoin. Room ends when combat resolves.
    }
  });

  ws.on("error", () => {});
});

// ── Helpers ───────────────────────────────────────────────────────────────────
function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function findRoomForUid(uid) {
  for (const room of rooms.values()) {
    if (room.hasMember(uid)) return room;
  }
  return null;
}

// ── CombatRoom ────────────────────────────────────────────────────────────────
class CombatRoom {
  constructor(partyId, members, enemies) {
    this.partyId    = partyId;
    this.members    = members;   // [{uid, name, hp, maxHp, energy, cooldowns, weaponAtk, weaponAcc, weaponType, alive}]
    this.enemies    = enemies;   // [{uid, type, name, hp, maxHp, energy, cooldowns, attackDelay, alive}]
    this.logs       = [];
    this.events     = [];
    this.lgSeq      = 0;
    this.evSeq      = 0;
    this.combatAt   = Date.now();
    this.lastTickTs = this.combatAt;
    this.ticker     = null;
    this.ended      = false;
    this.burns      = [];        // [{uid (enemy uid), ticks:[ts,...]}]
    // Threat table: { enemyUid: { memberUid: totalDmg } }
    // Used by enemy AI to target whoever dealt the most damage to them
    this.threat     = {};
    // Last-sent snapshots for delta diffing — keyed by uid
    this._snapM     = {}; // { uid: {h, mx, en, al} }
    this._snapE     = {}; // { uid: {h, al} }
  }

  // ── Static factory — loads saves from Firebase ────────────────────────────
  static async create(partyId, memberUids, enemyList) {
    const members = [];
    for (const uid of memberUids) {
      const snap = await db.ref(`pv/saves/${uid}`).get();
      const save = snap.val();
      const p    = save?.player || {};
      // Derive weapon stats from equipment (mirrors client calcStats)
      const eq      = p.equipment || {};
      const weapon  = eq.weapon    || null;
      const armor   = eq.armor     || null;
      const accessories = (eq.accessories || []).filter(Boolean);
      const weaponAtk = ((weapon?.atk)||0) + ((armor?.atk)||0);
      const weaponAcc = ((weapon?.acc)||0) + ((armor?.acc)||0);
      const armorMhp  = (armor?.maxHp||0) + accessories.reduce((s,a)=>s+((a?.maxHp)||0),0);
      const baseMaxHp = p.baseMaxHp || 100;
      const maxHp     = baseMaxHp + armorMhp;
      members.push({
        uid,
        name:        p.name        || "Adventurer",
        hp:          Math.min(p.hp || maxHp, maxHp),
        maxHp,
        baseMaxHp,
        energy:      0,
        cooldowns:   {},
        weaponAtk,
        weaponAcc,
        weaponType:  weapon?.weaponType || null,
        learnedActions: p.learnedActions || [],
        inventory:   p.inventory    || [],
        gold:        p.gold         || 0,
        respawnZone: p.respawnZone  || "duskwatch",
        alive:       true,
      });
    }

    // Build enemy objects with unique uids
    const enemies = enemyList.map((e, i) => {
      const def = ENEMIES_DB[e.type] || ENEMIES_DB.goblin;
      return {
        uid:         e.uid || `${e.type}_${i}`,
        type:        e.type,
        name:        def.name,
        hp:          def.maxHp,
        maxHp:       def.maxHp,
        energy:      0,
        cooldowns:   {},
        attackDelay: null,
        alive:       true,
      };
    });

    return new CombatRoom(partyId, members, enemies);
  }

  hasMember(uid) {
    return this.members.some(m => m.uid === uid);
  }

  // ── Broadcast to all connected party members ──────────────────────────────
  broadcast(obj) {
    const str = JSON.stringify(obj);
    for (const m of this.members) {
      const client = clients.get(m.uid);
      if (client?.ws.readyState === 1) client.ws.send(str);
    }
  }

  // ── Send full current state to a single reconnecting member ──────────────
  sendFullState(uid) {
    const client = clients.get(uid);
    if (!client) return;
    send(client.ws, {
      type:    "full_state",
      members: this.members.map(m => this._projectMember(m)),
      enemies: this.enemies.map(e => this._projectEnemy(e)),
      logs:    this.logs,
      events:  this.events,
      combatAt: this.combatAt,
    });
  }

  // Full projection — used for combat_start and full_state only
  _projectMember(m) {
    return { uid:m.uid, name:m.name, h:m.hp, mx:m.maxHp, en:m.energy, al:m.alive?1:0, fl:m.fled?1:0 };
  }
  _projectEnemy(e) {
    return { uid:e.uid, type:e.type, name:e.name, h:e.hp, mx:e.maxHp, al:e.alive?1:0 };
  }

  // Delta projection — only fields that changed since last broadcast
  _deltaMember(m) {
    const s = this._snapM[m.uid] || {};
    const d = { uid: m.uid };
    let changed = false;
    if (s.h  !== m.hp)         { d.h  = m.hp;         s.h  = m.hp;         changed = true; }
    if (s.mx !== m.maxHp)      { d.mx = m.maxHp;       s.mx = m.maxHp;      changed = true; }
    if (s.en !== m.energy)     { d.en = m.energy;      s.en = m.energy;     changed = true; }
    if (s.al !== (m.alive?1:0)){ d.al = m.alive?1:0;   s.al = m.alive?1:0;  changed = true; }
    if (s.fl !== (m.fled?1:0)) { d.fl = m.fled?1:0;    s.fl = m.fled?1:0;   changed = true; }
    this._snapM[m.uid] = s;
    return changed ? d : null;
  }
  _deltaEnemy(e) {
    const s = this._snapE[e.uid] || {};
    const d = { uid: e.uid };
    let changed = false;
    if (s.h  !== e.hp)         { d.h  = e.hp;         s.h  = e.hp;         changed = true; }
    if (s.al !== (e.alive?1:0)){ d.al = e.alive?1:0;  s.al = e.alive?1:0;  changed = true; }
    this._snapE[e.uid] = s;
    return changed ? d : null;
  }

  seqLog(type, text) {
    this.lgSeq++;
    const entry = { sq:this.lgSeq, tp:type, tx:text };
    this.logs.push(entry);
    return entry;
  }

  seqEvent(ev) {
    this.evSeq++;
    const entry = { sq:this.evSeq, ...ev };
    this.events.push(entry);
    return entry;
  }

  // ── Start the combat tick loop ────────────────────────────────────────────
  start() {
    // Send initial state to all members
    this.broadcast({
      type:    "combat_start",
      members: this.members.map(m => this._projectMember(m)),
      enemies: this.enemies.map(e => this._projectEnemy(e)),
      combatAt: this.combatAt,
    });
    console.log(`[COMBAT] tick loop starting for partyId=${this.partyId} members=${this.members.length} enemies=${this.enemies.length}`);
    this._tickCount = 0;
    this.ticker = setInterval(() => this._tick(), TICK_MS);
  }

  // ── Main tick: runs every 500ms ───────────────────────────────────────────
  _tick() {
    if (this.ended) return;
    this._tickCount = (this._tickCount || 0) + 1;
    // Log first 15 ticks and then every 30 ticks
    if (this._tickCount <= 15 || this._tickCount % 30 === 0) {
      const eSum = this.enemies.map(e => `${e.name}:hp=${e.hp},en=${Math.round(e.energy)},al=${e.alive}`).join(', ');
      const mSum = this.members.map(m => `${m.name}:hp=${m.hp},en=${Math.round(m.energy)},al=${m.alive}`).join(', ');
      console.log(`[TICK#${this._tickCount}] E:[${eSum}] M:[${mSum}]`);
    }

    const now     = Date.now();
    const elapsed = now - this.lastTickTs;
    this.lastTickTs = now;

    const newLogs   = [];
    const newEvents = [];
    const _log   = (tp, tx) => { const e = this.seqLog(tp, tx);   newLogs.push(e);   };
    const _event = (ev)      => { const e = this.seqEvent(ev);     newEvents.push(e); };

    // Debug: log energy status every 5 ticks for first 50 ticks
    if (this._tickCount <= 50 && this._tickCount % 5 === 0) {
      const eSums = this.enemies.filter(e=>e.alive).map(e => `${e.name}:en=${Math.round(e.energy)}`).join(' ');
      const mSums = this.members.filter(m=>m.alive).map(m => `${m.name}:en=${Math.round(m.energy)}`).join(' ');
      _log("system", `[DBG t${this._tickCount}] E:[${eSums}] M:[${mSums}]`);
    }

    // ── 1. Burn ticks ────────────────────────────────────────────────────────
    const keepBurns = [];
    this.burns.forEach(burn => {
      const pending = burn.ticks.filter(t => t <= now);
      const future  = burn.ticks.filter(t => t > now);
      pending.forEach(() => {
        const e = this.enemies.find(e => e.uid === burn.uid && e.alive);
        if (!e) return;
        const bdmg = roll(1, 4);
        e.hp = Math.max(0, e.hp - bdmg);
        // Attribute threat to the player who cast the burn
        if (burn.caster) {
          if (!this.threat[burn.uid]) this.threat[burn.uid] = {};
          this.threat[burn.uid][burn.caster] = (this.threat[burn.uid][burn.caster] || 0) + bdmg;
        }
        _log("ph", `${e.name} burns for ${bdmg} damage.`);
        _event({ k:"player_attack", au:burn.caster||"__burn__", vu:e.uid, d:bdmg, h:1, an:"Burn" });
      });
      if (future.length > 0) keepBurns.push({ ...burn, ticks:future });
    });
    this.burns = keepBurns;

    // ── 2. Energy regen (all alive members + enemies, after delay) ───────────
    const combatAge = now - this.combatAt;
    if (combatAge >= ENERGY_DELAY_MS) {
      const ticksThisFrame = Math.max(1, Math.floor(elapsed / TICK_MS));
      const gain = ENERGY_PER_TICK * ticksThisFrame;

      this.members.forEach(m => {
        if (!m.alive) return;
        m.energy = clamp(m.energy + gain, 0, 100);
      });

      this.enemies.forEach(e => {
        if (!e.alive) return;
        e.energy = clamp(e.energy + gain, 0, 100);
      });
    }

    // ── 3. Enemy AI attacks ───────────────────────────────────────────────────
    const alivePlayers = this.members.filter(m => m.alive);
    this.enemies.forEach(e => {
      if (!e.alive) return;
      if (e.energy < ENERGY_TO_ACT) return;

      // Set random attack delay on first reaching threshold (0–4 seconds)
      if (!e.attackDelay) { e.attackDelay = now + Math.floor(Math.random() * 4001); return; }
      if (now < e.attackDelay) return;

      const aId      = (ENEMIES_DB[e.type]?.actions[0]) || "basic_attack";
      const cd       = e.cooldowns[aId];
      if (cd && now < cd) return;

      const ability  = ACTIONS_DB[aId] || ACTIONS_DB.basic_attack;
      e.energy       = Math.max(0, e.energy - ENERGY_TO_ACT);
      e.attackDelay  = null;
      e.cooldowns[aId] = now + (ability.cooldown || 0);

      // Pick target based on threat (damage dealt to this enemy)
      // Priority: 1. Highest damage dealt  2. Highest current HP  3. Random
      if (alivePlayers.length === 0) return;
      const eThreat = this.threat[e.uid] || {};
      let target;
      if (Object.keys(eThreat).length === 0) {
        // No one has attacked this enemy yet — pick highest HP, then random
        const maxHp = Math.max(...alivePlayers.map(m => m.hp));
        const topHp = alivePlayers.filter(m => m.hp === maxHp);
        target = topHp[Math.floor(Math.random() * topHp.length)];
      } else {
        // Find alive players sorted by threat to this enemy
        const withThreat = alivePlayers.map(m => ({ m, dmg: eThreat[m.uid] || 0 }));
        const maxDmg = Math.max(...withThreat.map(t => t.dmg));
        const topDmg = withThreat.filter(t => t.dmg === maxDmg);
        if (topDmg.length === 1) {
          target = topDmg[0].m;
        } else {
          // Tie on damage — pick highest current HP
          const maxHp = Math.max(...topDmg.map(t => t.m.hp));
          const topHp = topDmg.filter(t => t.m.hp === maxHp);
          if (topHp.length === 1) {
            target = topHp[0].m;
          } else {
            // Tie on HP too — random among tied
            target = topHp[Math.floor(Math.random() * topHp.length)].m;
          }
        }
      }

      const hit = ability.difficulty < rng();
      if (hit) {
        const dmg = Math.max(1, roll(ability.damage[0], ability.damage[1]));
        target.hp = Math.max(0, target.hp - dmg);
        if (target.hp === 0) target.alive = false;
        console.log(`[AI-ATK] ${e.name} hits ${target.name} for ${dmg} (hp now ${target.hp})`);
        _log("system", `[DBG] ${e.name} ATK ${target.name} dmg=${dmg} hp=${target.hp} vu=${target.uid}`);
        _log("eh", `${e.name}'s ${ability.name} hits ${target.name} for ${dmg} damage.`);
        _event({ k:"enemy_attack", auid:e.uid, vu:target.uid, vun:target.name, d:dmg, h:1, an:ability.name });
      } else {
        console.log(`[AI-ATK] ${e.name} misses ${target.name}`);
        _log("system", `[DBG] ${e.name} MISS ${target.name} vu=${target.uid}`);
        _log("em", `${e.name}'s ${ability.name} misses ${target.name}.`);
        _event({ k:"enemy_attack", auid:e.uid, vu:target.uid, vun:target.name, d:0, h:0, an:ability.name });
      }
    });

    // ── 4. Death checks ───────────────────────────────────────────────────────
    // Enemy deaths
    this.enemies.forEach(e => {
      if (e.hp <= 0 && e.alive) {
        e.alive = false;
        _log("ed", `${e.name} is slain!`);
      }
    });

    const aliveEnemies  = this.enemies.filter(e => e.alive);
    const aliveMembers  = this.members.filter(m => m.alive);

    // ── 5. Broadcast tick delta to all clients ────────────────────────────────
    const dm = this.members.map(m => this._deltaMember(m)).filter(Boolean);
    const de = this.enemies.map(e => this._deltaEnemy(e)).filter(Boolean);
    // Skip broadcast entirely if nothing changed
    if (dm.length > 0 || de.length > 0 || newLogs.length > 0 || newEvents.length > 0) {
      const pkt = { type: "tick" };
      if (dm.length > 0)         pkt.members = dm;
      if (de.length > 0)         pkt.enemies = de;
      if (newLogs.length > 0)    pkt.logs    = newLogs;
      if (newEvents.length > 0)  pkt.events  = newEvents;
      if (newEvents.length > 0) console.log(`[BROADCAST] ${newEvents.length} events: ${newEvents.map(e=>(e.k||"?")+":"+(e.d||0)).join(", ")}`);
      this.broadcast(pkt);
    }

    // ── 6. Victory ────────────────────────────────────────────────────────────
    if (aliveEnemies.length === 0 && this.enemies.length > 0) {
      this._endCombat("victory");
      return;
    }

    // ── 7. Total party kill ───────────────────────────────────────────────────
    if (aliveMembers.length === 0) {
      this._endCombat("death");
    }
  }

  // ── Handle player action ──────────────────────────────────────────────────
  handleAction(uid, msg) {
    if (this.ended) return;

    const member = this.members.find(m => m.uid === uid);
    if (!member || !member.alive) return;

    const aId     = msg.a || "basic_attack";
    const ability = ACTIONS_DB[aId] || ACTIONS_DB.basic_attack;
    if (ability.isFlee) return; // use flee message type instead

    // Check energy
    if (member.energy < ENERGY_TO_PLAYER) return;

    // Check cooldown
    const cd = member.cooldowns[aId];
    if (cd && Date.now() < cd) return;

    // Check weapon type requirement
    if (ability.requiresWeaponType && ability.requiresWeaponType !== "magic") {
      if (member.weaponType !== ability.requiresWeaponType) return;
    }

    // Find target
    const aliveEnemies = this.enemies.filter(e => e.alive);
    if (aliveEnemies.length === 0) return;
    const target = aliveEnemies.find(e => e.uid === msg.tu) || aliveEnemies[0];

    // Spend energy + set cooldown
    member.energy = Math.max(0, member.energy - ENERGY_TO_PLAYER);
    member.cooldowns[aId] = Date.now() + (ability.cooldown || 0);

    const newLogs   = [];
    const newEvents = [];
    const _log   = (tp, tx) => { const e = this.seqLog(tp, tx);   newLogs.push(e);   };
    const _event = (ev)      => { const e = this.seqEvent(ev);     newEvents.push(e); };

    const hit = (ability.difficulty - member.weaponAcc) < rng();
    if (hit) {
      const dmg = Math.max(1, roll(ability.damage[0], ability.damage[1]) + member.weaponAtk);
      const eIdx = this.enemies.findIndex(e => e.uid === target.uid);
      if (eIdx >= 0) {
        this.enemies[eIdx].hp = Math.max(0, this.enemies[eIdx].hp - dmg);
        if (this.enemies[eIdx].hp === 0) this.enemies[eIdx].alive = false;
      }
      // Record threat — enemy remembers who hurt them
      if (!this.threat[target.uid]) this.threat[target.uid] = {};
      this.threat[target.uid][member.uid] = (this.threat[target.uid][member.uid] || 0) + dmg;
      _log("ph", `${member.name}'s ${ability.name} hits ${target.name} for ${dmg} damage.`);
      _event({ k:"player_attack", au:member.uid, vu:target.uid, d:dmg, h:1, an:ability.name });

      if (ability.burnEffect && this.enemies[eIdx]?.alive) {
        const t = Date.now();
        this.burns.push({ uid:target.uid, caster:member.uid, ticks:[t+2000, t+4000, t+6000] });
      }
    } else {
      _log("pm", `${member.name}'s ${ability.name} misses ${target.name}.`);
      _event({ k:"player_attack", au:member.uid, vu:target.uid, d:0, h:0, an:ability.name });
    }

    // Broadcast action result immediately (don't wait for next tick)
    const _dm = this.members.map(m => this._deltaMember(m)).filter(Boolean);
    const _de = this.enemies.map(e => this._deltaEnemy(e)).filter(Boolean);
    const _pkt = { type: "tick" };
    if (_dm.length > 0)        _pkt.members = _dm;
    if (_de.length > 0)        _pkt.enemies = _de;
    if (newLogs.length > 0)    _pkt.logs    = newLogs;
    if (newEvents.length > 0)  _pkt.events  = newEvents;
    this.broadcast(_pkt);

    // Check if this killed the last enemy
    if (this.enemies.filter(e => e.alive).length === 0) {
      this._endCombat("victory");
    }
  }

  // ── Handle flee attempt ───────────────────────────────────────────────────
  handleFlee(uid) {
    if (this.ended) return;
    const member = this.members.find(m => m.uid === uid);
    if (!member || !member.alive) return;

    // Deduct energy — flee costs the same as any action
    if (member.energy < ENERGY_TO_PLAYER) return;
    member.energy = Math.max(0, member.energy - ENERGY_TO_PLAYER);

    const escaped = (ACTIONS_DB.flee.difficulty || 35) < rng();
    const newLogs = [];
    const _log = (tp, tx) => { const e = this.seqLog(tp, tx); newLogs.push(e); };

    if (escaped) {
      _log("ci", `${member.name} flees from combat!`);
      member.alive = false;
      member.fled = true; // distinguish escaped vs dead
      const _fm1 = this.members.map(m => this._deltaMember(m)).filter(Boolean);
      const _fp1 = { type: "tick" };
      if (_fm1.length > 0) _fp1.members = _fm1;
      if (newLogs.length > 0) _fp1.logs = newLogs;
      _fp1.events = [];
      this.broadcast(_fp1);
      // Send personal fled message
      const client = clients.get(uid);
      if (client) send(client.ws, { type:"fled" });
      // If everyone fled, end combat
      if (this.members.filter(m => m.alive).length === 0) this._endCombat("flee");
    } else {
      _log("ff", `${member.name} failed to flee!`);
      const _fm2 = this.members.map(m => this._deltaMember(m)).filter(Boolean);
      const _fp2 = { type: "tick" };
      if (_fm2.length > 0) _fp2.members = _fm2;
      if (newLogs.length > 0) _fp2.logs = newLogs;
      _fp2.events = [];
      this.broadcast(_fp2);
    }
  }

  // ── End combat — distribute loot, write results to Firebase ──────────────
  async _endCombat(outcome) {
    if (this.ended) return;
    this.ended = true;
    clearInterval(this.ticker);

    console.log(`[ROOM] end partyId=${this.partyId} outcome=${outcome}`);

    // Calculate kills and per-member individual loot rolls
    const kills = {};
    // Each alive member rolls loot independently — loot is NOT shared or split
    const memberLoot = {}; // uid -> { gold }
    if (outcome === "victory") {
      this.enemies.forEach(e => {
        kills[e.type] = (kills[e.type] || 0) + 1;
      });
      const recipients = this.members.filter(m => m.alive);
      recipients.forEach(m => {
        let memberGold = 0;
        this.enemies.forEach(e => {
          const def = ENEMIES_DB[e.type];
          if (!def) return;
          def.loot.forEach(entry => {
            if (rng() <= entry.chance && entry.type === "gold") {
              memberGold += roll(entry.qty[0], entry.qty[1]);
            }
          });
        });
        memberLoot[m.uid] = { gold: memberGold };
      });
    }

    // Broadcast outcome — each member gets their own individual loot amount
    // For death (TPK): use leader's respawnZone so entire party respawns together
    // members[0] is always the leader (first in memberUids sent by start_combat)
    const leaderRespawn = this.members[0]?.respawnZone || "duskwatch";
    const _baseMsg = {
      type: "combat_end",
      outcome,
      kills,
      respawnZone: (outcome === "death" || outcome === "flee") ? leaderRespawn : undefined,
      members: this.members.map(mb => this._projectMember(mb)),
      enemies: this.enemies.map(e => this._projectEnemy(e)),
    };
    this.members.forEach(m => {
      const client = clients.get(m.uid);
      if (!client || client.ws.readyState !== 1) return;
      const loot = memberLoot[m.uid] || { gold: 0 };
      client.ws.send(JSON.stringify({ ..._baseMsg, goldEach: loot.gold }));
    });

    // Write results back to each member's Firebase save
    await Promise.all(this.members.map(async (m) => {
      try {
        const snap = await db.ref(`pv/saves/${m.uid}`).get();
        const save = snap.val();
        if (!save?.player) return;

        const p = { ...save.player };

        // Ensure respawnZone always exists — defaults to duskwatch for old saves
        if (!p.respawnZone) p.respawnZone = "duskwatch";

        // Update HP:
        // - Death (TPK): all members respawn with full HP
        // - Victory: alive members keep current HP (min 1), dead members revive with 1 HP
        p.hp = outcome === "death" ? (p.maxHp || m.maxHp || 100) : Math.max(1, m.hp);

        // On death, set respawnZone to leader's respawnZone so all members respawn together
        if (outcome === "death") {
          p.lastZone = leaderRespawn;
        }

        // Award individual loot to this member
        if (outcome === "victory" && memberLoot[m.uid]) {
          const loot = memberLoot[m.uid];
          p.gold = (p.gold || 0) + loot.gold;
        }

        // Update kill stats
        if (outcome === "victory") {
          p.stats = p.stats || {};
          p.stats.kills = p.stats.kills || {};
          Object.entries(kills).forEach(([type, n]) => {
            p.stats.kills[type] = (p.stats.kills[type] || 0) + n;
          });
        }

        await db.ref(`pv/saves/${m.uid}/player`).update(p);
        console.log(`[SAVE] uid=${m.uid} hp=${p.hp} gold=${p.gold} loot=${JSON.stringify(memberLoot[m.uid]||{})}`);
      } catch (e) {
        console.error(`[SAVE] error uid=${m.uid}:`, e.message);
      }
    }));

    // Clean up room after a short delay
    setTimeout(() => rooms.delete(this.partyId), 10000);
  }
}

// ── Start server ──────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Project Void combat server listening on port ${PORT}`);
});

// ── H3: Hard cap on concurrent combat rooms ───────────────────────────────────
const MAX_ROOMS = 500;

// ── H2: Per-uid rate limiter ──────────────────────────────────────────────────
// start_combat: max 1 per 5 s.  action/flee: max 30 per 5 s.
const _RL_WINDOW  = 5000;
const _RL_MAX_ACT = 30;
const _rateBuckets = new Map(); // uid -> { ts: number[], lastCombat: number }
function _rateOk(uid, type) {
  const now = Date.now();
  if (!_rateBuckets.has(uid)) _rateBuckets.set(uid, { ts: [], lastCombat: 0 });
  const b = _rateBuckets.get(uid);
  if (type === "start_combat") {
    if (now - b.lastCombat < _RL_WINDOW) return false;
    b.lastCombat = now; return true;
  }
  b.ts = b.ts.filter(t => now - t < _RL_WINDOW);
  if (b.ts.length >= _RL_MAX_ACT) return false;
  b.ts.push(now); return true;
}
// Purge stale buckets every 10 min so Map doesn't grow unbounded
setInterval(() => {
  const live = new Set(clients.keys());
  for (const uid of _rateBuckets.keys()) { if (!live.has(uid)) _rateBuckets.delete(uid); }
}, 10 * 60 * 1000);

// ── H1: Ping/pong — evict dead WebSocket connections every 20 s ───────────────
// ws.isAlive is set true on connect and reset to true on each pong response.
// Any socket that doesn't pong within 20 s is terminated → fires "close" → removed from clients.
const _pingInterval = setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 20000);

// ── M3: Graceful shutdown ─────────────────────────────────────────────────────
async function _gracefulShutdown(signal) {
  console.log(`[SHUTDOWN] ${signal} received`);
  clearInterval(_pingInterval);
  for (const room of rooms.values()) {
    if (!room.ended) { room.ended = true; clearInterval(room.ticker); }
  }
  wss.close();
  server.close(() => { console.log("[SHUTDOWN] clean exit"); process.exit(0); });
  setTimeout(() => process.exit(1), 10000); // force-kill after 10 s
}
process.on("SIGTERM", () => _gracefulShutdown("SIGTERM"));
process.on("SIGINT",  () => _gracefulShutdown("SIGINT"));

// ── Stale party cleanup ───────────────────────────────────────────────────────
// Runs every 5 minutes. Deletes party/partyts/partyvote nodes that are older
// than 10 minutes AND have no active combat room on this server.
// Prevents pile-up from clients that crashed or disconnected without disbanding.
const STALE_PARTY_MS  = 10 * 60 * 1000; // 10 minutes
const CLEANUP_INTERVAL = 5 * 60 * 1000;  // run every 5 minutes

async function _cleanStaleParties() {
  try {
    const snap = await db.ref("pv/party").get();
    if (!snap.exists()) return;
    const now = Date.now();
    const parties = snap.val();
    const deletes = [];
    for (const [partyId, data] of Object.entries(parties)) {
      // Skip if there's an active combat room for this party on this server
      if (rooms.has(partyId)) continue;
      const ts = (data && data.ts) || 0;
      if (now - ts > STALE_PARTY_MS) {
        deletes.push(partyId);
      }
    }
    if (deletes.length === 0) return;
    console.log(`[CLEANUP] Deleting ${deletes.length} stale party node(s):`, deletes);
    await Promise.all(deletes.map(id => Promise.all([
      db.ref(`pv/party/${id}`).remove(),
      db.ref(`pv/partyts/${id}`).remove(),
      db.ref(`pv/partyvote/${id}`).remove(),
    ])));
  } catch (e) {
    console.error("[CLEANUP] stale party error:", e.message);
  }
}

setInterval(_cleanStaleParties, CLEANUP_INTERVAL);
// Also run once shortly after boot to clean up anything left from a previous crash
setTimeout(_cleanStaleParties, 30000);
