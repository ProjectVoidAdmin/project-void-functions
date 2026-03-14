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
  flee:           { name:"Flee",           difficulty:1,  isFlee:true },
};

const ENEMIES_DB = {
  goblin: { name:"Goblin", maxHp:40, actions:["goblin_bonk"], loot:[{ type:"gold", qty:[2,8],  chance:100 }] },
  wolf:   { name:"Wolf",   maxHp:20, actions:["bite"],        loot:[{ type:"gold", qty:[1,4],  chance:100 }] },
};

const TICK_MS        = 100;
const ENERGY_PER_TICK = 1;
const ENERGY_DELAY_MS = 1000;
const ENERGY_TO_ACT   = 80;
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
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

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
        if (rooms.size >= MAX_ROOMS) { send(ws, { type:"error", reason:"server_full" }); return; }
        if (!_rateOk(clientUid, "start_combat")) { send(ws, { type:"error", reason:"rate_limited" }); return; }
        const { enemyList, memberUids } = msg;
        // Allow solo: auto-generate a partyId if none provided
        const partyId = msg.partyId || `solo_${clientUid}_${Date.now()}`;
        if (!enemyList?.length) {
          send(ws, { type:"error", reason:"bad_start_combat" }); return;
        }
        // Solo combat: just the player themselves
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
        _log("eh", `${e.name}'s ${ability.name} hits ${target.name} for ${dmg} damage.`);
        _event({ k:"enemy_attack", auid:e.uid, vu:target.uid, vun:target.name, d:dmg, h:1, an:ability.name });
      } else {
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
      respawnZone: outcome === "death" ? leaderRespawn : undefined,
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
        // - Death (TPK): all members respawn with 1 HP
        // - Victory: alive members keep current HP (min 1), dead members revive with 1 HP
        p.hp = outcome === "death" ? 1 : Math.max(1, m.hp);

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
