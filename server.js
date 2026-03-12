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
  bite:           { name:"Bite",           damage:[2,6],   difficulty:20, cooldown:0 },
  goblin_bonk:    { name:"Bonk",           damage:[3,10],  difficulty:25, cooldown:0 },
  flee:           { name:"Flee",           difficulty:35,  isFlee:true },
};

const ENEMIES_DB = {
  goblin: { name:"Goblin", maxHp:40, actions:["goblin_bonk"], loot:[{ type:"gold", qty:[2,8],  chance:100 }] },
  wolf:   { name:"Wolf",   maxHp:20, actions:["bite"],        loot:[{ type:"gold", qty:[1,4],  chance:100 }] },
};

const TICK_MS        = 500;
const ENERGY_PER_TICK = 5;
const ENERGY_DELAY_MS = 3000;
const ENERGY_TO_ACT   = 60;
const ENERGY_TO_PLAYER= 80;

const roll  = (a,b) => Math.floor(Math.random()*(b-a+1))+a;
const rng   = ()    => Math.floor(Math.random()*100)+1;
const clamp = (v,a,b) => Math.max(a,Math.min(b,v));

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

  ws.on("message", async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      // ── Auth ────────────────────────────────────────────────────────────────
      case "auth": {
        try {
          const decoded = await auth.verifyIdToken(msg.idToken);
          clientUid = decoded.uid;
          clients.set(clientUid, { ws, uid: clientUid, username: msg.username || "" });
          send(ws, { type:"auth_ok", uid: clientUid });
          console.log(`[AUTH] uid=${clientUid} username=${msg.username}`);
        } catch (e) {
          send(ws, { type:"auth_fail", reason: e.message });
        }
        break;
      }

      // ── Start combat (leader only) ─────────────────────────────────────────
      case "start_combat": {
        if (!clientUid) { send(ws, { type:"error", reason:"not_authed" }); return; }
        const { partyId, enemyList, memberUids } = msg;
        if (!partyId || !enemyList?.length) {
          send(ws, { type:"error", reason:"bad_start_combat" }); return;
        }
        // Load all member saves from Firebase
        const memberUidList = memberUids?.length ? memberUids : [clientUid];
        try {
          const room = await CombatRoom.create(partyId, memberUidList, enemyList);
          rooms.set(partyId, room);
          room.start();
          console.log(`[ROOM] created partyId=${partyId} members=${memberUidList.length} enemies=${enemyList.length}`);
        } catch (e) {
          console.error("[ROOM] create error:", e);
          send(ws, { type:"error", reason:"start_failed", detail: e.message });
        }
        break;
      }

      // ── Player action ──────────────────────────────────────────────────────
      case "action": {
        if (!clientUid) return;
        const room = findRoomForUid(clientUid);
        if (!room) { send(ws, { type:"error", reason:"not_in_combat" }); return; }
        room.handleAction(clientUid, msg);
        break;
      }

      // ── Flee ───────────────────────────────────────────────────────────────
      case "flee": {
        if (!clientUid) return;
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

  _projectMember(m) {
    return { uid:m.uid, name:m.name, hp:m.hp, maxHp:m.maxHp, energy:m.energy, alive:m.alive };
  }

  _projectEnemy(e) {
    return { uid:e.uid, type:e.type, name:e.name, hp:e.hp, maxHp:e.maxHp, alive:e.alive };
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

    this.ticker = setInterval(() => this._tick(), TICK_MS);
  }

  // ── Main tick: runs every 500ms ───────────────────────────────────────────
  _tick() {
    if (this.ended) return;

    const now     = Date.now();
    const elapsed = now - this.lastTickTs;
    this.lastTickTs = now;

    const newLogs   = [];
    const newEvents = [];
    const _log   = (tp, tx) => { const e = this.seqLog(tp, tx);   newLogs.push(e);   };
    const _event = (ev)      => { const e = this.seqEvent(ev);     newEvents.push(e); };

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
        _log("ph", `${e.name} burns for ${bdmg} damage.`);
        _event({ k:"player_attack", au:"__burn__", vu:e.uid, d:bdmg, h:1, an:"Burn" });
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

      // Set attack delay on first reaching threshold
      if (!e.attackDelay) { e.attackDelay = now; return; }
      if (now < e.attackDelay) return;

      const aId      = (ENEMIES_DB[e.type]?.actions[0]) || "basic_attack";
      const cd       = e.cooldowns[aId];
      if (cd && now < cd) return;

      const ability  = ACTIONS_DB[aId] || ACTIONS_DB.basic_attack;
      e.energy       = Math.max(0, e.energy - ENERGY_TO_ACT);
      e.attackDelay  = null;
      e.cooldowns[aId] = now + (ability.cooldown || 0);

      // Pick a random alive player as target
      if (alivePlayers.length === 0) return;
      const target = alivePlayers[Math.floor(Math.random() * alivePlayers.length)];

      const hit = ability.difficulty < rng();
      if (hit) {
        const dmg = Math.max(1, roll(ability.damage[0], ability.damage[1]));
        target.hp = Math.max(0, target.hp - dmg);
        if (target.hp === 0) target.alive = false;
        _log("eh", `${e.name}'s ${ability.name} hits ${target.name} for ${dmg} damage.`);
        _event({ k:"enemy_attack", auid:e.uid, vu:target.uid, d:dmg, h:1, an:ability.name });
      } else {
        _log("em", `${e.name}'s ${ability.name} misses ${target.name}.`);
        _event({ k:"enemy_attack", auid:e.uid, vu:target.uid, d:0, h:0, an:ability.name });
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
    this.broadcast({
      type:    "tick",
      members: this.members.map(m => this._projectMember(m)),
      enemies: this.enemies.map(e => this._projectEnemy(e)),
      logs:    newLogs,
      events:  newEvents,
    });

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
      _log("ph", `${member.name}'s ${ability.name} hits ${target.name} for ${dmg} damage.`);
      _event({ k:"player_attack", au:uid, vu:target.uid, d:dmg, h:1, an:ability.name });

      if (ability.burnEffect && this.enemies[eIdx]?.alive) {
        const t = Date.now();
        this.burns.push({ uid:target.uid, ticks:[t+2000, t+4000, t+6000] });
      }
    } else {
      _log("pm", `${member.name}'s ${ability.name} misses ${target.name}.`);
      _event({ k:"player_attack", au:uid, vu:target.uid, d:0, h:0, an:ability.name });
    }

    // Broadcast action result immediately (don't wait for next tick)
    this.broadcast({
      type:    "tick",
      members: this.members.map(m => this._projectMember(m)),
      enemies: this.enemies.map(e => this._projectEnemy(e)),
      logs:    newLogs,
      events:  newEvents,
    });

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

    const escaped = rng() <= 35;
    const newLogs = [];
    const _log = (tp, tx) => { const e = this.seqLog(tp, tx); newLogs.push(e); };

    if (escaped) {
      _log("ci", `${member.name} flees from combat!`);
      member.alive = false; // remove from fight
      this.broadcast({
        type: "tick",
        members: this.members.map(m => this._projectMember(m)),
        enemies: this.enemies.map(e => this._projectEnemy(e)),
        logs: newLogs, events: [],
      });
      // Send personal fled message
      const client = clients.get(uid);
      if (client) send(client.ws, { type:"fled" });
      // If everyone fled, end combat
      if (this.members.filter(m => m.alive).length === 0) this._endCombat("flee");
    } else {
      _log("ff", `${member.name} failed to flee!`);
      this.broadcast({
        type: "tick",
        members: this.members.map(m => this._projectMember(m)),
        enemies: this.enemies.map(e => this._projectEnemy(e)),
        logs: newLogs, events: [],
      });
    }
  }

  // ── End combat — distribute loot, write results to Firebase ──────────────
  async _endCombat(outcome) {
    if (this.ended) return;
    this.ended = true;
    clearInterval(this.ticker);

    console.log(`[ROOM] end partyId=${this.partyId} outcome=${outcome}`);

    // Calculate loot on victory
    let rawGold = 0;
    const kills = {};
    if (outcome === "victory") {
      this.enemies.forEach(e => {
        kills[e.type] = (kills[e.type] || 0) + 1;
        const def = ENEMIES_DB[e.type];
        if (!def) return;
        def.loot.forEach(entry => {
          if (rng() <= entry.chance && entry.type === "gold") {
            rawGold += roll(entry.qty[0], entry.qty[1]) * 2;
          }
        });
      });
    }

    // Split gold evenly among alive members (or all if death)
    const recipients = outcome === "victory"
      ? this.members.filter(m => m.alive)
      : this.members;
    const goldEach = recipients.length > 0 ? Math.floor(rawGold / recipients.length) : 0;

    // Broadcast outcome
    this.broadcast({
      type: "combat_end",
      outcome,
      rawGold,
      goldEach,
      kills,
      members: this.members.map(m => this._projectMember(m)),
      enemies: this.enemies.map(e => this._projectEnemy(e)),
    });

    // Write results back to each member's Firebase save
    await Promise.all(this.members.map(async (m) => {
      try {
        const snap = await db.ref(`pv/saves/${m.uid}`).get();
        const save = snap.val();
        if (!save?.player) return;

        const p = { ...save.player };

        // Update HP — clamp to 1 on death (respawn)
        p.hp = outcome === "death" ? 1 : Math.max(1, m.hp);

        // Award gold to recipients
        if (outcome === "victory" && recipients.find(r => r.uid === m.uid)) {
          p.gold = (p.gold || 0) + goldEach;
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
        console.log(`[SAVE] uid=${m.uid} hp=${p.hp} gold=${p.gold}`);
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
