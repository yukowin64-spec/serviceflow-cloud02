/**
 * ServiceFlow Cloud — Zeabur + PostgreSQL 後端
 * ServiceFlow Cloud — Zeabur + PostgreSQL backend
 *
 * 功能 Features:
 *  - 派靜態前端 (public/index.html)  Serve static frontend
 *  - GET  /api/health   健康檢查      Health check
 *  - GET  /api/state    讀取整包雲端資料  Read full cloud state
 *  - PUT  /api/state    儲存整包雲端資料(伺服器合併,防止互相覆蓋)Save full cloud state (server-side merge)
 *  - GET  /api/version  輕量版本號 Lightweight version check
 *  - POST /api/orders   即時儲存單一新工單 Save one new order atomically
 *  - GET/POST /api/backups, GET /api/backups/:id  雲端自動備份(每12小時,保留7日)Auto backups (12h, keep 7 days)
 *  - POST /api/send-email  透過 SMTP 發郵件  Send email via SMTP
 *  - 伺服器端每日排程(24小時自動運行,無需任何人登入)
 *    Server-side daily scheduler (runs 24/7, no login required)
 *
 * 資料存喺 PostgreSQL 一張表 app_state 入面,config / store 各一個 JSONB 欄位。
 * Data is stored in a single PostgreSQL table `app_state`, with config / store as JSONB columns.
 */
const express = require('express');
const path = require('path');
const { Pool } = require('pg');
const nodemailer = require('nodemailer');

const app = express();
const PORT = process.env.PORT || 8080;

// ---- 資料庫連線 Database connection ----
// Zeabur 會自動提供 DATABASE_URL(連好 PostgreSQL 服務後)。
// Zeabur automatically injects DATABASE_URL once you link the PostgreSQL service.
const connectionString = process.env.DATABASE_URL;
if (!connectionString) {
  console.warn('[warn] DATABASE_URL 未設定 — 請喺 Zeabur 連接 PostgreSQL 服務。DATABASE_URL not set — link a PostgreSQL service on Zeabur.');
}
const pool = new Pool({
  connectionString,
  // Zeabur 內部連線唔需要 SSL;若外部連線需要,設 PGSSL=1
  // Internal Zeabur connections don't need SSL; for external set PGSSL=1
  ssl: process.env.PGSSL === '1' ? { rejectUnauthorized: false } : false,
});

// ---- 中介 Middleware ----
app.use(express.json({ limit: '15mb' })); // 資料可能較大(含 logo base64) State can be large (logo base64)
app.use(express.static(path.join(__dirname, 'public')));

// ---- 建表 Ensure schema ----
async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS app_state (
      workspace   TEXT PRIMARY KEY,
      config      JSONB NOT NULL DEFAULT '{}'::jsonb,
      store       JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS app_backups (
      id          SERIAL PRIMARY KEY,
      workspace   TEXT NOT NULL,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
      reason      TEXT NOT NULL DEFAULT 'auto',
      config      JSONB NOT NULL DEFAULT '{}'::jsonb,
      store       JSONB NOT NULL DEFAULT '{}'::jsonb
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS app_order_seq (
      workspace   TEXT NOT NULL,
      day_prefix  TEXT NOT NULL,
      n           INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (workspace, day_prefix)
    );
  `);
  await pool.query('CREATE INDEX IF NOT EXISTS app_backups_ws_time ON app_backups (workspace, created_at DESC)');
  console.log('[db] app_state / app_backups 表已就緒 tables ready');
}

// 預設 workspace(單一團隊共用一份資料)。將來要多團隊可用 ?ws= 分開。
// Default workspace (one shared dataset). For multi-team, split by ?ws=.
function wsKey(req) {
  const ws = (req.query.ws || req.get('X-Workspace') || 'default').toString().trim();
  return ws || 'default';
}

// ---- 健康檢查 Health ----
app.get('/api/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ ok: true, db: true, time: new Date().toISOString() });
  } catch (e) {
    res.status(500).json({ ok: false, db: false, error: String(e.message || e) });
  }
});

// ---- 讀取整包資料 Read state ----
app.get('/api/state', async (req, res) => {
  try {
    const ws = wsKey(req);
    const r = await pool.query('SELECT config, store, updated_at FROM app_state WHERE workspace = $1', [ws]);
    if (r.rows.length === 0) {
      return res.json({ found: false, config: null, store: null, updated_at: null });
    }
    const row = r.rows[0];
    res.json({ found: true, config: row.config, store: row.store, updated_at: row.updated_at });
  } catch (e) {
    console.error('[GET /api/state]', e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

// ================= 伺服器端合併 Server-side merge (v1.9.7) =================
// 防止多部裝置同時儲存時互相覆蓋,導致工單「消失」。
// Prevents devices from overwriting each other's writes (which made orders "disappear").
function _arr(x){ return Array.isArray(x) ? x : []; }
function _deletedSet(inner){
  // 刪除記錄 tombstones:deletedOrders + 舊版「刪除工單」日誌 legacy delete_order logs
  const m = new Map();
  _arr(inner && inner.deletedOrders).forEach(t => { if (t && t.no) m.set(String(t.no), Math.max(Number(t.ts) || 0, m.get(String(t.no)) || 0)); });
  _arr(inner && inner.logs).forEach(l => { if (l && l.action === 'delete_order' && l.target) { const k = String(l.target); if (!m.has(k)) m.set(k, 0); } });
  return m;
}
// 舊版刪除日誌時間(香港時間字串)→ 毫秒 legacy delete-log time (HK string) -> ms
function _hkTsMs(s) { const t = Date.parse(String(s || '').replace(' ', 'T') + (String(s || '').length <= 16 ? ':00' : '') + '+08:00'); return isNaN(t) ? 0 : t; }
// 刪除時間:tombstone 或刪除日誌(取最新)deletion time from tombstone or delete log (latest)
function _deletedAt(inner, k) {
  let ts = -1;
  _arr(inner && inner.deletedOrders).forEach(t => { if (t && String(t.no) === k) ts = Math.max(ts, Number(t.ts) || 0); });
  _arr(inner && inner.logs).forEach(l => { if (l && l.action === 'delete_order' && String(l.target) === k) ts = Math.max(ts, _hkTsMs(l.ts)); });
  return ts; // -1 = 從未刪除 never deleted
}
// 兩張係咪唔同嘅單(撞號)whether two records with the same no are actually different orders (number clash)
function _distinctOrder(a, b) {
  if (!a || !b) return false;
  return String(a.created || '') !== String(b.created || '') || String(a.machineId || '') !== String(b.machineId || '') || String(a.kind || 'repair') !== String(b.kind || 'repair');
}
function _hkDate() { const d = new Date(Date.now() + 8 * 3600 * 1000); return d.toISOString().slice(0, 10).replace(/-/g, ''); }
function _freshNo(inner, no) {
  const pre = (String(no).match(/^([A-Z]+)-/) || [null, 'SR'])[1] + '-' + _hkDate() + '-';
  const used = new Set(); _arr(inner.orders).forEach(o => o && used.add(String(o.no)));
  _arr(inner.deletedOrders).forEach(t => t && used.add(String(t.no))); _arr(inner.logs).forEach(l => l && l.target && used.add(String(l.target)));
  for (let i = 0; i < 3000; i++) { const n = pre + String(Math.floor(Math.random() * (i < 1500 ? 900 : 9000)) + (i < 1500 ? 100 : 1000)); if (!used.has(n)) return n; }
  return pre + String(Date.now()).slice(-6);
}
// v2.0.3:順序單號(每日每種由 001 開始)sequential order numbers per prefix per HK day, starting at 001
function _usedNos(inner) {
  const used = new Set(); _arr(inner.orders).forEach(o => o && o.no && used.add(String(o.no)));
  _arr(inner.deletedOrders).forEach(t => t && t.no && used.add(String(t.no))); _arr(inner.logs).forEach(l => l && l.target && used.add(String(l.target)));
  return used;
}
async function _seqNo(client, ws, inner, no) {
  const pre = (String(no).match(/^([A-Z]+)-/) || [null, 'SR'])[1] + '-' + _hkDate() + '-';
  const used = _usedNos(inner);
  for (let i = 0; i < 5000; i++) {
    const r = await client.query(`INSERT INTO app_order_seq (workspace, day_prefix, n) VALUES ($1, $2, 1)
      ON CONFLICT (workspace, day_prefix) DO UPDATE SET n = app_order_seq.n + 1 RETURNING n`, [ws, pre]);
    const cand = pre + String(r.rows[0].n).padStart(3, '0');
    if (!used.has(cand)) return cand;
  }
  return _freshNo(inner, no);
}
// 已改號嘅舊單號 → 新單號 provisional number -> assigned number
function _renameMap(orders) { const m = new Map(); _arr(orders).forEach(o => { if (o && o._renamedFrom && o.no) m.set(String(o._renamedFrom), o); }); return m; }
function mergeInner(dbInner, inInner) {
  dbInner = dbInner || {}; inInner = inInner || {};
  const out = Object.assign({}, dbInner, inInner); // 其他資料:以送來嘅為準 other arrays: incoming wins (unchanged behaviour)
  let merged = false;
  // 刪除記錄合併 merge tombstones (keep 120 days)
  const tmap = new Map();
  _arr(dbInner.deletedOrders).concat(_arr(inInner.deletedOrders)).forEach(t => {
    if (!t || !t.no) return; const k = String(t.no); const ts = Number(t.ts) || 0;
    if (!tmap.has(k) || ts > tmap.get(k).ts) tmap.set(k, { no: k, ts });
  });
  const cutoff = Date.now() - 120 * 86400000;
  out.deletedOrders = Array.from(tmap.values()).filter(t => t.ts >= cutoff).slice(-3000);
  // 工單:按單號合併,較新 _ts 勝;刪除記錄會移除工單 orders: merge by no, newest _ts wins, tombstones remove
  const dbOrders = _arr(dbInner.orders), inOrders = _arr(inInner.orders);
  const legacyDel = new Map([..._deletedSet(dbInner), ..._deletedSet(inInner)]);
  const map = new Map();
  dbOrders.forEach(o => { if (o && o.no) map.set(String(o.no), o); });
  const inKeys = new Set();
  const rmap = _renameMap(dbOrders);
  inOrders.forEach(o => {
    if (!o || !o.no) return; const k = String(o.no); inKeys.add(k);
    const cur = map.get(k);
    // 裝置仲用緊臨時單號,但伺服器已派咗正式順序單號:唔好再加多張 device still has the provisional number of an order that was already renumbered
    if (!cur && rmap.has(k) && !_distinctOrder(rmap.get(k), o)) { merged = true; return; }
    if (!cur) {
      // 已刪除嘅舊單唔好被舊裝置「復活」;但刪除之後先開嘅新單(撞號)照樣保留
      // don't resurrect a deleted order, but KEEP a newer order that merely reuses the number
      if (legacyDel.has(k)) { const da = Math.max(_deletedAt(dbInner, k), _deletedAt(inInner, k)); if ((Number(o._ts) || 0) <= da) { merged = true; return; } }
      map.set(k, o);
    } else if (_distinctOrder(cur, o)) {
      // 撞號:兩張都保留,送來嗰張用後備單號 number clash: keep BOTH; the incoming one gets a stable alternate number
      const alt = k + '-' + String(Math.abs([...String(o.created || '') + String(o.machineId || '')].reduce((a, c) => ((a << 5) - a + c.charCodeAt(0)) | 0, 7)) % 1000).padStart(3, '0');
      const ex = map.get(alt);
      if (!ex || (Number(o._ts) || 0) >= (Number(ex._ts) || 0)) map.set(alt, Object.assign({}, o, { no: alt, _clashOf: k }));
      merged = true;
    } else if ((Number(o._ts) || 0) >= (Number(cur._ts) || 0)) { map.set(k, o); }
    else merged = true;
  });
  for (const k of map.keys()) if (!inKeys.has(k)) merged = true; // 伺服器有、裝置冇 → 保留 kept server-only orders
  // 套用刪除記錄 apply tombstones
  for (const [k, t] of tmap) { const o = map.get(k); if (o && (Number(o._ts) || 0) <= t.ts) map.delete(k); }
  // 保持原有次序:新單在前 keep newest-first order
  const order = [];
  const seen = new Set();
  inOrders.forEach(o => { if (o && o.no && map.has(String(o.no)) && !seen.has(String(o.no))) { order.push(map.get(String(o.no))); seen.add(String(o.no)); } });
  const extra = [];
  for (const [k, o] of map) if (!seen.has(k)) extra.push(o);
  out.orders = extra.concat(order);
  // 日誌 logs:按 ts|action|target 去重 dedupe, newest first, keep 1000
  const lk = x => ((x && x.ts) || '') + '|' + ((x && x.action) || '') + '|' + ((x && x.target) || '');
  const _fix = x => { if (x && x.target && rmap.has(String(x.target)) && !map.has(String(x.target))) return Object.assign({}, x, { target: rmap.get(String(x.target)).no }); return x; };
  const lmap = new Map(); _arr(dbInner.logs).forEach(x => lmap.set(lk(x), x)); _arr(inInner.logs).forEach(x => { x = _fix(x); lmap.set(lk(x), x); });
  if (lmap.size !== _arr(inInner.logs).length) merged = true;
  out.logs = Array.from(lmap.values()).sort((a, b) => ((b.ts || '') < (a.ts || '') ? -1 : ((b.ts || '') > (a.ts || '') ? 1 : 0))).slice(0, 1000);
  // 通知 / 郵件記錄:按 id 合併 notifications / emailLog by id
  ['notifications', 'emailLog'].forEach(key => {
    const m = new Map(); _arr(dbInner[key]).forEach(x => x && x.id && m.set(x.id, x)); _arr(inInner[key]).forEach(x => x && x.id && m.set(x.id, x));
    if (m.size !== _arr(inInner[key]).length) merged = true;
    out[key] = Array.from(m.values()).sort((a, b) => ((b.ts || '') < (a.ts || '') ? -1 : ((b.ts || '') > (a.ts || '') ? 1 : 0))).slice(0, 300);
  });
  return { inner: out, merged };
}
function mergeStoreObj(dbStore, inStore) {
  dbStore = dbStore || {}; inStore = inStore || {};
  const dseq = dbStore.seq || {}, iseq = inStore.seq || {};
  const seq = {
    uidSeq: Math.max(Number(dseq.uidSeq) || 0, Number(iseq.uidSeq) || 0),
    cidSeq: Math.max(Number(dseq.cidSeq) || 0, Number(iseq.cidSeq) || 0),
  };
  const r = mergeInner(dbStore.store, inStore.store);
  return { store: { seq, store: r.inner }, merged: r.merged };
}

// 前端版本號(用嚟提示舊頁面自動重新載入)Frontend version, so stale open pages can auto-reload
let APP_VERSION_SERVED = null;
try { const _m = require('fs').readFileSync(path.join(__dirname, 'public', 'index.html'), 'utf8').match(/const APP_VERSION\s*=\s*'([^']+)'/); if (_m) APP_VERSION_SERVED = _m[1]; } catch (e) {}
// ---- 輕量版本號 Lightweight version check (poll without downloading everything) ----
app.get('/api/version', async (req, res) => {
  try {
    const r = await pool.query('SELECT updated_at FROM app_state WHERE workspace = $1', [wsKey(req)]);
    res.set('Cache-Control', 'no-store');
    res.json({ found: r.rows.length > 0, updated_at: r.rows.length ? r.rows[0].updated_at : null, appVersion: APP_VERSION_SERVED });
  } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
});

// ---- 查詢單一工單 Look up ONE order (repair-station links; no full download) ----
app.get('/api/orders/:no', async (req, res) => {
  try {
    const r = await pool.query('SELECT store FROM app_state WHERE workspace = $1', [wsKey(req)]);
    res.set('Cache-Control', 'no-store');
    if (!r.rows.length) return res.json({ found: false });
    const inner = (r.rows[0].store && r.rows[0].store.store) || {};
    const k = String(req.params.no || '').trim();
    const o = _arr(inner.orders).find(x => x && String(x.no) === k);
    if (o) return res.json({ found: true, order: o });
    const tomb = _arr(inner.deletedOrders).find(t => t && String(t.no) === k);
    const dlog = _arr(inner.logs).find(l => l && l.action === 'delete_order' && String(l.target) === k);
    res.json({ found: false, deleted: (tomb || dlog) ? { ts: tomb ? tomb.ts : null, at: dlog ? dlog.ts : null, user: dlog ? dlog.user : null } : null });
  } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
});

// ---- v2.0.5:更新現有工單 — 直接喺數據庫最新版本上改,唔用裝置舊副本 ----
// Update an existing order: apply only the changed fields onto the CURRENT database record (never a device's stale copy),
// then return the record as written in the database so the device can confirm.
app.post('/api/orders/:no/patch', async (req, res) => {
  const ws = wsKey(req);
  const k = String(req.params.no || '').trim();
  const { patch, log, notification } = req.body || {};
  if (!k || !patch || typeof patch !== 'object') return res.status(400).json({ ok: false, error: 'missing patch' });
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const r = await client.query('SELECT store FROM app_state WHERE workspace = $1 FOR UPDATE', [ws]);
    if (!r.rows.length) { await client.query('ROLLBACK'); return res.status(409).json({ ok: false, error: 'no state yet' }); }
    const st = r.rows[0].store || {}; const inner = Object.assign({}, st.store || {});
    const orders = _arr(inner.orders).slice();
    const idx = orders.findIndex(x => x && String(x.no) === k);
    if (idx < 0) {
      await client.query('ROLLBACK');
      const del = _deletedAt(inner, k);
      return res.status(404).json({ ok: false, found: false, deleted: del >= 0 });
    }
    const cur = Object.assign({}, orders[idx]);
    const PROTECT = new Set(['no', 'created', '_renamedFrom', '_seqNo', '_clashOf']);
    Object.keys(patch).forEach(f => {
      if (PROTECT.has(f) || f === '_ts') return;
      if (patch[f] === null || patch[f] === undefined) delete cur[f]; else cur[f] = patch[f];
    });
    cur._ts = Math.max(Date.now(), (Number(orders[idx]._ts) || 0) + 1, Number(patch._ts) || 0);
    orders[idx] = cur; inner.orders = orders;
    if (log && log.ts) {
      const lk = x => ((x && x.ts) || '') + '|' + ((x && x.action) || '') + '|' + ((x && x.target) || '');
      const logs = _arr(inner.logs); if (!logs.some(x => lk(x) === lk(log))) inner.logs = [log].concat(logs).slice(0, 1000);
    }
    if (notification && notification.id) {
      const ns = _arr(inner.notifications); if (!ns.some(x => x && x.id === notification.id)) inner.notifications = [notification].concat(ns).slice(0, 300);
    }
    const newStore = Object.assign({}, st, { store: inner });
    const w = await client.query('UPDATE app_state SET store = $2::jsonb, updated_at = now() WHERE workspace = $1 RETURNING updated_at, store', [ws, JSON.stringify(newStore)]);
    await client.query('COMMIT');
    // 由數據庫寫入結果讀返出嚟確認 read back from the written row to confirm
    const saved = _arr(((w.rows[0].store || {}).store || {}).orders).find(x => x && String(x.no) === k) || null;
    res.json({ ok: !!saved, order: saved, updated_at: w.rows[0].updated_at });
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    console.error('[PATCH order]', e);
    res.status(500).json({ ok: false, error: String(e.message || e) });
  } finally { client.release(); }
});

// ---- 即時儲存單一新工單 Save ONE new order atomically (front page submits) ----
// 工單先存入資料庫,成功後前端先發 Telegram/Email,確保「有通知就一定有單」。
// The order is saved first; only after success does the client send notifications.
app.post('/api/orders', async (req, res) => {
  const ws = wsKey(req);
  const { order, log, notification, dedupeInspection } = req.body || {};
  if (!order || !order.no) return res.status(400).json({ ok: false, error: 'missing order' });
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const r = await client.query('SELECT store FROM app_state WHERE workspace = $1 FOR UPDATE', [ws]);
    if (!r.rows.length) { await client.query('ROLLBACK'); return res.status(409).json({ ok: false, error: 'no state yet' }); }
    const st = r.rows[0].store || {}; const inner = Object.assign({}, st.store || {});
    const orders = _arr(inner.orders).slice();
    const k = String(order.no);
    const idx = orders.findIndex(o => o && String(o.no) === k);
    if (idx < 0 && dedupeInspection && order.machineId) {
      const dup = orders.find(o => o && o.kind === 'inspection' && o.machineId === order.machineId && o.status !== 'completed' && o.status !== 'cancelled');
      if (dup) { await client.query('ROLLBACK'); return res.json({ ok: true, duplicate: true, existing: dup.no }); }
    }
    const delAt = _deletedAt(inner, k);
    const clash = (idx >= 0 && _distinctOrder(orders[idx], order)) || (idx < 0 && delAt >= 0);
    let renamed = null;
    // 之前已經改過號(回應遺失後重試)previously renamed (retry after a lost response) -> reuse that record
    const prev = (idx < 0 || clash) ? orders.find(o => o && o._renamedFrom === k && !_distinctOrder(o, order)) : null;
    const seqNew = !prev && req.body.isNew && (idx < 0 || clash || !orders[idx]._seqNo);
    if (prev) { order.no = prev.no; order._renamedFrom = k; }
    if (prev) { /* fallthrough: treat as existing */ }
    else if (seqNew || (clash && idx >= 0)) {
      // 撞號(已有另一張單,或者呢個單號曾經刪除):改用新單號,兩張都保留
      // number clash (another order has it, or it was deleted before): assign a fresh number, keep both
      // v2.0.3:新單一律由伺服器派順序單號 new orders always get the next sequential number from the server
      if (idx >= 0 && !clash) { orders.splice(idx, 1); inner.logs = _arr(inner.logs).map(l => (l && l.target === k) ? Object.assign({}, l, { target: '__tmp__' }) : l); }
      renamed = await _seqNo(client, ws, inner, k);
      if (idx >= 0 && !clash) inner.logs = _arr(inner.logs).map(l => (l && l.target === '__tmp__') ? Object.assign({}, l, { target: renamed }) : l);
      order.no = renamed; order._seqNo = true; if (renamed !== k) order._renamedFrom = k;
      if (log && log.target === k) log.target = renamed;
      if (notification && notification.orderNo === k) { notification.orderNo = renamed; if (typeof notification.text === 'string') notification.text = notification.text.split(k).join(renamed); }
    } else if (idx < 0 && delAt >= 0 && (Number(order._ts) || 0) <= delAt) {
      // 補回嘅舊副本,但已被手動刪除 a rescued old copy of a manually deleted order
      await client.query('ROLLBACK'); return res.json({ ok: true, deleted: true });
    }
    const k2 = String(order.no);
    const idx2 = renamed ? -1 : (prev ? orders.indexOf(prev) : idx);
    if (prev) renamed = prev.no;
    let existed = idx2 >= 0;
    if (existed) { if ((Number(order._ts) || 0) >= (Number(orders[idx2]._ts) || 0)) orders[idx2] = order; }
    else orders.unshift(order);
    inner.orders = orders;
    if (log && log.ts) {
      const lk = x => ((x && x.ts) || '') + '|' + ((x && x.action) || '') + '|' + ((x && x.target) || '');
      const logs = _arr(inner.logs); if (!logs.some(x => lk(x) === lk(log))) inner.logs = [log].concat(logs).slice(0, 1000);
    }
    if (notification && notification.id) {
      const ns = _arr(inner.notifications); if (!ns.some(x => x && x.id === notification.id)) inner.notifications = [notification].concat(ns).slice(0, 300);
    }
    const newStore = Object.assign({}, st, { store: inner });
    const w = await client.query('UPDATE app_state SET store = $2::jsonb, updated_at = now() WHERE workspace = $1 RETURNING updated_at', [ws, JSON.stringify(newStore)]);
    await client.query('COMMIT');
    res.json({ ok: true, existed, renamed, no: String(order.no), updated_at: w.rows[0].updated_at });
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    console.error('[POST /api/orders]', e);
    res.status(500).json({ ok: false, error: String(e.message || e) });
  } finally { client.release(); }
});

// ---- 儲存整包資料 Save state ----
// body: { config?: {...}, store?: {...} }  只更新提供咗嘅部分 Only provided parts are updated
app.put('/api/state', async (req, res) => {
  try {
    const ws = wsKey(req);
    const { config, replace } = req.body || {};
    let { store } = req.body || {};
    if (config === undefined && store === undefined) {
      return res.status(400).json({ error: 'config 或 store 至少要有一個 need at least one of config/store' });
    }
    // v1.9.7:喺交易入面鎖住資料列,同資料庫最新版本合併後先寫入(還原備份 replace=true 例外)。
    // v1.9.7: lock the row in a transaction and merge with the latest DB copy before writing (except backup restore, replace=true).
    const client = await pool.connect();
    let merged = false, r;
    try {
      await client.query('BEGIN');
      if (store !== undefined && !replace) {
        const cur = await client.query('SELECT store FROM app_state WHERE workspace = $1 FOR UPDATE', [ws]);
        if (cur.rows.length && cur.rows[0].store && cur.rows[0].store.store) {
          const m = mergeStoreObj(cur.rows[0].store, store); store = m.store; merged = m.merged;
        }
      } else if (store !== undefined && replace) {
        // 還原備份:先自動備份一次;未完成而備份入面冇嘅工單照樣保留,唔會被還原刪走
        // Restore: snapshot first; unfinished orders missing from the backup are KEPT (never removed by a restore)
        const cur = await client.query('SELECT store FROM app_state WHERE workspace = $1 FOR UPDATE', [ws]);
        if (cur.rows.length && cur.rows[0].store && cur.rows[0].store.store) {
          await client.query(`INSERT INTO app_backups (workspace, reason, config, store) SELECT workspace, 'before_restore', config, store FROM app_state WHERE workspace = $1`, [ws]);
          const inInner = (store && store.store) || {};
          const have = new Set(_arr(inInner.orders).map(o => o && String(o.no)));
          const keep = _arr(cur.rows[0].store.store.orders).filter(o => o && o.no && !have.has(String(o.no)) && o.status !== 'completed' && o.status !== 'cancelled');
          if (keep.length) { store = Object.assign({}, store, { store: Object.assign({}, inInner, { orders: keep.concat(_arr(inInner.orders)) }) }); merged = true; }
        }
      }
    r = await client.query(
      `INSERT INTO app_state (workspace, config, store, updated_at)
       VALUES ($1, COALESCE($2::jsonb, '{}'::jsonb), COALESCE($3::jsonb, '{}'::jsonb), now())
       ON CONFLICT (workspace) DO UPDATE SET
         config = COALESCE($2::jsonb, app_state.config),
         store  = COALESCE($3::jsonb, app_state.store),
         updated_at = now()
       RETURNING updated_at`,
      [ws, config === undefined ? null : JSON.stringify(config), store === undefined ? null : JSON.stringify(store)]
    );
      await client.query('COMMIT');
    } catch (e) { try { await client.query('ROLLBACK'); } catch (_) {} throw e; }
    finally { client.release(); }
    res.json({ ok: true, updated_at: r.rows[0].updated_at, merged });
  } catch (e) {
    console.error('[PUT /api/state]', e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

// ---- 共用:SMTP 寄信 Shared: send one email via SMTP ----
// 回傳 Promise<{ok:boolean, reason?, detail?}>
async function smtpSend(cfg, to, subject, text) {
  const host = cfg.smtpHost;
  if (!host || String(host).includes('@')) return { ok:false, reason:'config', detail:'SMTP host invalid' };
  const recips = String(to||'').split(/[,;\s]+/).map(s=>s.trim()).filter(Boolean);
  if (!recips.length) return { ok:false, reason:'recipient', detail:'no recipient' };
  const p = Number(cfg.smtpPort)||587;
  const transporter = nodemailer.createTransport({
    host, port: p, secure: p===465,
    auth: (cfg.smtpUser||cfg.smtpPass) ? { user: cfg.smtpUser, pass: cfg.smtpPass } : undefined,
    connectionTimeout: 12000, greetingTimeout: 12000, socketTimeout: 15000,
    tls: { rejectUnauthorized: false },
  });
  const info = await transporter.sendMail({
    from: cfg.smtpFrom || cfg.smtpUser,
    to: recips.join(', '),
    subject: subject || '(no subject)',
    text: text || '',
  });
  return { ok:true, messageId: info.messageId };
}

// ---- 發送郵件 Send email via SMTP (前端呼叫) ----
// body: { host, port, user, pass, from, to, subject, text }
// 回傳 { ok:true } 或 { ok:false, reason, detail }
// reason 值: connection | auth | recipient | config | unknown
app.post('/api/send-email', async (req, res) => {
  const { host, port, user, pass, from, to, subject, text } = req.body || {};
  // 基本檢查 Basic validation
  if (!host)  return res.status(400).json({ ok:false, reason:'config', detail:'缺少 SMTP 主機 Missing SMTP host' });
  // 主機名唔應該含有 @(常見錯誤:填成 email)。Host must NOT contain @ (common mistake: entering an email).
  if (String(host).includes('@')) {
    return res.status(400).json({ ok:false, reason:'config',
      detail:'SMTP 主機格式錯誤:主機是網域(例 mail.yourdomain.com),不應含有 @。SMTP host is invalid: it should be a domain like mail.yourdomain.com, not an email (no @).' });
  }
  if (!to)    return res.status(400).json({ ok:false, reason:'recipient', detail:'缺少收件人 Missing recipient' });
  const recips = String(to).split(',').map(s=>s.trim()).filter(Boolean);
  const bad = recips.filter(r=>!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(r));
  if (recips.length===0) return res.status(400).json({ ok:false, reason:'recipient', detail:'收件人地址為空 Recipient empty' });
  if (bad.length)        return res.status(400).json({ ok:false, reason:'recipient', detail:'收件人地址格式不正確 Invalid recipient address: '+bad.join(', ') });

  const p = Number(port)||587;
  try {
    const transporter = nodemailer.createTransport({
      host,
      port: p,
      secure: p===465,                 // 465=SSL, 587/25=STARTTLS
      auth: (user||pass) ? { user, pass } : undefined,
      connectionTimeout: 12000,
      greetingTimeout: 12000,
      socketTimeout: 15000,
      tls: { rejectUnauthorized: false },
    });
    const info = await transporter.sendMail({
      from: from || user,
      to: recips.join(', '),
      subject: subject || '(no subject)',
      text: text || '',
    });
    return res.json({ ok:true, messageId: info.messageId, accepted: info.accepted, rejected: info.rejected });
  } catch (e) {
    // 將 SMTP 錯誤分類為具體原因 Map SMTP error to a specific reason
    const code = (e && (e.code || e.responseCode)) || '';
    const msg  = String((e && e.message) || e);
    let reason = 'unknown';
    if (['ECONNREFUSED','ETIMEDOUT','ESOCKET','ECONNECTION','EDNS','ENOTFOUND','EHOSTUNREACH'].includes(code) ||
        /connect|timeout|getaddrinfo|ENOTFOUND|socket/i.test(msg)) reason='connection';
    else if (code==='EAUTH' || e.responseCode===535 || e.responseCode===534 || /auth|username|password|535|534|credential/i.test(msg)) reason='auth';
    else if (code==='EENVELOPE' || e.responseCode===550 || e.responseCode===553 || e.responseCode===501 || /recipient|mailbox|550|553|no such user|address/i.test(msg)) reason='recipient';
    return res.status(502).json({ ok:false, reason, code:String(code||''), detail: msg });
  }
});

/* ============================================================================
 * 伺服器端每日排程 Server-side daily scheduler
 * ----------------------------------------------------------------------------
 * 由伺服器 24 小時自己計時,到達你系統設定的「每日發送時間(香港,24小時制)」
 * 就自動發送每日未完成工單提示 — 完全唔需要任何人開住個網頁或登入。
 * The server runs 24/7 and fires the daily unfinished-orders digest exactly at the
 * HK hour you set in Settings (config.digestHour). No browser/login required.
 *
 * 時間來源:讀 app_state.config.digestHour(前端「每日發送時間」欄位)。
 * 若要改時間,直接喺系統設定改,伺服器下一分鐘就會採用新設定。
 * The send-time comes from your Settings (config.digestHour). Change it in the app
 * and the server picks it up on its next minute tick — nothing else to configure.
 * ==========================================================================*/

const ORDER_STATUS = { new:'新建立 New', in_progress:'處理中 In Progress', completed:'已完成 Completed', cancelled:'已取消 Cancelled', scheduled:'已預約 Scheduled', waiting_parts:'等待零件 Waiting Parts', pending:'待處理 Pending' };

// 取得香港時間的各部分 Get HK time parts (year/month/day/hour/minute)
function hkNow() {
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Hong_Kong', year:'numeric', month:'2-digit', day:'2-digit',
    hour:'2-digit', minute:'2-digit', hour12:false,
  });
  const parts = {};
  fmt.formatToParts(new Date()).forEach(p => { if (p.type !== 'literal') parts[p.type] = p.value; });
  let hh = parts.hour; if (hh === '24') hh = '00';
  return { date: `${parts.year}-${parts.month}-${parts.day}`, hour: Number(hh), minute: Number(parts.minute) };
}

// 透過 n8n 轉發 Telegram 匯總 Relay a Telegram summary via n8n webhook
async function relayViaN8n(cfg, payload) {
  try {
    if (!cfg.n8nEnabled || !cfg.n8nUrl) return;
    const body = Object.assign({}, payload, {
      channel: cfg.n8nChannel || 'both',
      tgToken: cfg.n8nTgToken || '',
      waToken: cfg.n8nWaToken || '',
      tg_master_chat_id: cfg.n8nTgMasterChat || '',
    });
    await fetch(cfg.n8nUrl, {
      method: 'POST',
      headers: Object.assign({ 'Content-Type': 'application/json' }, cfg.n8nSecret ? { 'X-Secret': cfg.n8nSecret } : {}),
      body: JSON.stringify(body),
    });
  } catch (e) {
    console.warn('[scheduler] n8n relay 失敗 relay failed:', String(e.message || e));
  }
}

// 執行一次每日摘要(伺服器版,對應前端 runDailyDigest)
// Run the daily digest once (server-side mirror of the client runDailyDigest)
async function runDailyDigestServer(ws, config, store) {
  const bn = config.brandName || 'Sunshine Laundry Group';
  const t = hkNow();
  const today = t.date;
  const _inner = (store && store.store) || store || {}; // 資料喺 store.store 入面 data lives in store.store
  const orders = _inner.orders || [];
  const companies = _inner.companies || [];
  const allUnfinished = orders.filter(o => o.status !== 'completed' && o.status !== 'cancelled');

  let companiesSent = 0, ordersCovered = 0;
  const noEmailCompanies = [];
  const perCompanySummary = [];

  for (const c of companies) {
    const list = allUnfinished.filter(o => o.companyId === c.id);
    if (!list.length) continue; // 該公司無未完成工單就唔發 skip companies with none
    ordersCovered += list.length;
    const cn = (c.name_zh || '') + (c.name_en ? (' ' + c.name_en) : '');
    const lines = list.map((o, i) =>
      (i + 1) + '. 工單編號 Order No.  ：' + o.no + '\n' +
      '   設備 Machine        ：' + o.machineId + '\n' +
      '   狀態 Status         ：' + (ORDER_STATUS[o.status] || o.status) + '\n' +
      '   下單時間 Created   ：' + (o.created || '') + '\n' +
      (o.issue ? ('   問題 Issue          ：' + o.issue + '\n') : '')
    ).join('────────────────────\n');
    const subject = '【' + bn + '】每日未完成工單提示 Daily Unfinished Orders · ' + cn + ' · ' + list.length + ' 張';
    const bodyText =
      '負責人您好 Dear Owner，\n\n' +
      '貴公司「' + cn + '」現時尚有 ' + list.length + ' 張工單未完成，詳情如下：\nYour company "' + cn + '" has ' + list.length + ' unfinished order(s):\n' +
      '════════════════════\n' +
      lines +
      '════════════════════\n\n' +
      '合共 Total：' + list.length + ' 張未完成工單 unfinished order(s)。\n' +
      '（此郵件由系統於每日 ' + (config.digestHour == null ? 9 : config.digestHour) + ' 點自動發送。This email is auto-sent daily at ' + (config.digestHour == null ? 9 : config.digestHour) + ':00 HK time.）\n\n' +
      '此郵件由系統自動發送。This email was sent automatically.';

    let to = (c.notifyEmail || '').trim();
    if (!to) to = (config.brandOwnerEmail || config.smtpAlertTo || '').trim();
    if (to && config.smtpEnabled) {
      try {
        const r = await smtpSend(config, to, subject, bodyText);
        if (r.ok) companiesSent++;
        else console.warn('[scheduler] email 發送失敗 send failed for', cn, r.reason, r.detail);
      } catch (e) {
        console.warn('[scheduler] email 例外 exception for', cn, String(e.message || e));
      }
    } else {
      noEmailCompanies.push(cn);
    }
    perCompanySummary.push({ company: cn, companyId: c.id, unfinishedCount: list.length });
  }

  // N8N Telegram:逐間有未完成工單嘅公司，各自發一則到佢自己嘅 tgGroupId(只列該公司未完成工單)。
  // Per company: send one Telegram to each company's OWN group (listing only that company's unfinished orders).
  // 主群預設關閉，只有 config.tgMasterEnabled===true 才額外發一則合併匯總到主群。Master off by default.
  let tgSent = 0;
  const digestHour = (config.digestHour == null ? 9 : config.digestHour);
  for (const c of companies) {
    const list = allUnfinished.filter(o => o.companyId === c.id);
    if (!list.length) continue;
    const g = (c.tgGroupId ? String(c.tgGroupId).trim() : '');
    const wg = (c.waGroupId ? String(c.waGroupId).trim() : '');
    if (!g && !wg) continue; // Telegram 及 WhatsApp 群都未設就跳過 skip companies without any group id
    const cn = (c.name_zh || '') + (c.name_en ? (' ' + c.name_en) : '');
    const tlines = list.map((o, i) =>
      (i + 1) + '. ' + o.no + ' · ' + o.machineId + ' · ' + (ORDER_STATUS[o.status] || o.status) + (o.issue ? (' · ' + o.issue) : '')
    ).join('\n');
    const cMsg = '⏰ 【' + bn + '】每日未完成工單 Daily Unfinished · ' + cn + ' · ' + today + '\n' +
      '負責人您好 Dear Owner，貴公司現時尚有 ' + list.length + ' 張工單未完成：\n' +
      '────────────────────\n' + tlines + '\n' +
      '────────────────────\n' +
      '合共 Total：' + list.length + ' 張。（每日 ' + digestHour + ' 點自動發送 auto-sent daily at ' + digestHour + ':00 HK）';
    await relayViaN8n(config, {
      event: 'daily_digest', kind: 'daily_digest', date: today, brand: bn,
      company_id: c.id, company_zh: c.name_zh || '', company_en: c.name_en || '',
      unfinishedCount: list.length,
      subject: '每日未完成工單 Daily Unfinished · ' + cn,
      text: cMsg, message: cMsg,
      tg_targets: g ? [{ chat_id: g, role: 'company', company_id: c.id, company_zh: c.name_zh || '', company_en: c.name_en || '' }] : [],
      wa_targets: wg ? [{ group_id: wg, role: 'company', company_id: c.id, company_zh: c.name_zh || '', company_en: c.name_en || '' }] : [],
    });
    tgSent++;
  }
  // 主群合併匯總(僅在啟用時)。Combined summary to master group (only when enabled).
  const mg = (config.n8nTgMasterChat || '').trim();
  if (mg && config.tgMasterEnabled === true) {
    const tgLines = perCompanySummary.length
      ? perCompanySummary.map(s => '• ' + s.company + '：' + s.unfinishedCount + ' 張未完成').join('\n')
      : '（今日各公司皆無未完成工單 None）';
    const tgMsg = '【' + bn + '】每日未完成工單匯總 Daily Digest · ' + today + '\n共 ' + allUnfinished.length + ' 張未完成 across ' + perCompanySummary.length + ' 間公司：\n' + tgLines;
    await relayViaN8n(config, {
      event: 'daily_digest', kind: 'daily_digest', date: today, brand: bn,
      unfinishedCount: allUnfinished.length, companies: perCompanySummary.length,
      subject: '每日未完成工單匯總 Daily Digest · ' + today,
      text: tgMsg, message: tgMsg, perCompany: perCompanySummary,
      tg_targets: [{ chat_id: mg, role: 'master' }],
      wa_targets: [],
    });
  }

  // 寫回 digestLastSent,並加一條日誌到 store.logs
  // Persist digestLastSent + append a log entry to store.logs
  const newConfig = Object.assign({}, config, { digestLastSent: today });
  try {
    const logEntry = {
      id: 'log' + Date.now(),
      ts: today + ' ' + String(t.hour).padStart(2,'0') + ':' + String(t.minute).padStart(2,'0'),
      type: 'notify',
      action: '每日報告(伺服器自動) Daily Digest (server)',
      detail: '未完成 ' + allUnfinished.length + ' 張 · 發郵件 ' + companiesSent + ' 間公司 · 覆蓋 ' + ordersCovered + ' 張' +
        (noEmailCompanies.length ? (' · 未設郵箱 ' + noEmailCompanies.join('/')) : '') +
        ' · Telegram ' + tgSent + ' 間公司群' +
        ' · N8N ' + (config.n8nEnabled && config.n8nUrl ? '已轉發 relayed' : '未啟用 off'),
      actor: 'system',
    };
    // v1.9.7:重新讀取最新資料再寫入,唔會覆蓋發送期間新落嘅工單。Re-read latest inside a transaction so orders created meanwhile are kept.
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const cur = await client.query('SELECT config, store FROM app_state WHERE workspace = $1 FOR UPDATE', [ws]);
      if (cur.rows.length) {
        const cfg2 = Object.assign({}, cur.rows[0].config || {}, { digestLastSent: today });
        const st2 = cur.rows[0].store || {};
        if (st2.logs) delete st2.logs; // 清走舊版放錯位置嘅日誌 remove legacy misplaced logs
        const inner2 = Object.assign({}, st2.store || {});
        logEntry.user = 'system'; logEntry.target = logEntry.action; logEntry.action = 'notify';
        inner2.logs = [logEntry].concat(_arr(inner2.logs)).slice(0, 1000);
        st2.store = inner2;
        await client.query('UPDATE app_state SET config=$2::jsonb, store=$3::jsonb, updated_at=now() WHERE workspace=$1', [ws, JSON.stringify(cfg2), JSON.stringify(st2)]);
      }
      await client.query('COMMIT');
    } catch (e2) { try { await client.query('ROLLBACK'); } catch (_) {} throw e2; }
    finally { client.release(); }
  } catch (e) {
    console.warn('[scheduler] 寫回 digestLastSent 失敗 persist failed:', String(e.message || e));
  }

  console.log('[scheduler] ✅ 每日摘要已發送 digest sent · ws=' + ws + ' · 未完成 ' + allUnfinished.length + ' · 郵件 ' + companiesSent + ' 間公司 · ' + today + ' ' + String(t.hour).padStart(2,'0') + ':00 HK');
}

// 每分鐘檢查一次:是否到達設定時段且今日未發。
// Check every minute: is it the configured HK hour and not yet sent today?
let _schedulerBusy = false;
async function schedulerTick() {
  if (_schedulerBusy) return;
  _schedulerBusy = true;
  try {
    // 掃描所有 workspace(通常只有 default)Scan all workspaces (usually just default)
    const r = await pool.query('SELECT workspace, config, store FROM app_state');
    const t = hkNow();
    for (const row of r.rows) {
      const config = row.config || {};
      const store = row.store || {};
      if (!config.dailyDigestEnabled) continue;                 // 未啟用 not enabled
      const hour = (config.digestHour == null ? 9 : Number(config.digestHour));
      if (t.hour !== hour) continue;                            // 只喺設定嗰個鐘頭 only within configured hour
      if (config.digestLastSent === t.date) continue;           // 今日已發 already sent today
      await runDailyDigestServer(row.workspace, config, store);
    }
  } catch (e) {
    console.warn('[scheduler] tick 失敗 tick error:', String(e.message || e));
  } finally {
    _schedulerBusy = false;
  }
}

function startScheduler() {
  // 立即跑一次(以防重啟時剛好喺設定時段),之後每分鐘一次。
  // Run once immediately (in case a restart lands inside the window), then every minute.
  setTimeout(() => { schedulerTick(); }, 8000);
  setInterval(schedulerTick, 60 * 1000);
  console.log('[scheduler] 伺服器每日排程已啟動(每分鐘檢查一次,按系統設定時間發送) server-side daily scheduler started (checks every minute, fires at config.digestHour HK)');
}

// ================= 自動備份 Auto backup (v2.0.0) =================
// 每 12 小時喺資料庫存一份完整快照,保留 7 日(約 14 份)。
// Every 12 hours store a full snapshot in the DB; keep 7 days (~14 copies).
const BACKUP_EVERY_MS = 12 * 3600 * 1000;
const BACKUP_KEEP_DAYS = 7;
function _bkCounts(store) {
  const s = (store && store.store) || {};
  return { companies: _arr(s.companies).length, machines: _arr(s.machines).length, orders: _arr(s.orders).length, users: _arr(s.users).length };
}
async function createBackup(ws, reason) {
  const r = await pool.query(
    `INSERT INTO app_backups (workspace, reason, config, store)
     SELECT workspace, $2, config, store FROM app_state WHERE workspace = $1
     RETURNING id, created_at`, [ws, reason || 'auto']);
  return r.rows[0] || null;
}
async function pruneBackups() {
  await pool.query(`DELETE FROM app_backups WHERE created_at < now() - ($1 || ' days')::interval`, [String(BACKUP_KEEP_DAYS)]);
}
let _backupBusy = false;
async function backupTick() {
  if (_backupBusy) return; _backupBusy = true;
  try {
    const r = await pool.query('SELECT workspace FROM app_state');
    for (const row of r.rows) {
      const last = await pool.query(`SELECT created_at FROM app_backups WHERE workspace = $1 AND reason = 'auto' ORDER BY created_at DESC LIMIT 1`, [row.workspace]);
      const lastT = last.rows.length ? new Date(last.rows[0].created_at).getTime() : 0;
      if (Date.now() - lastT >= BACKUP_EVERY_MS - 60 * 1000) {
        const b = await createBackup(row.workspace, 'auto');
        if (b) console.log('[backup] ✅ 自動備份完成 auto backup #' + b.id + ' · ws=' + row.workspace);
      }
    }
    await pruneBackups();
  } catch (e) {
    console.warn('[backup] 失敗 failed:', String(e.message || e));
  } finally { _backupBusy = false; }
}
function startBackupScheduler() {
  setTimeout(backupTick, 20 * 1000);          // 啟動後檢查一次 check shortly after boot
  setInterval(backupTick, 10 * 60 * 1000);    // 之後每 10 分鐘檢查係咪夠 12 小時 then every 10 min
  console.log('[backup] 自動備份已啟動:每 12 小時一次,保留 7 日 auto backup: every 12h, keep 7 days');
}
// 列出備份 List backups (no payload)
app.get('/api/backups', async (req, res) => {
  try {
    const ws = wsKey(req);
    const r = await pool.query(`SELECT id, created_at, reason, store FROM app_backups WHERE workspace = $1 ORDER BY created_at DESC LIMIT 50`, [ws]);
    res.json({ ok: true, everyHours: 12, keepDays: BACKUP_KEEP_DAYS,
      backups: r.rows.map(x => ({ id: x.id, created_at: x.created_at, reason: x.reason, counts: _bkCounts(x.store) })) });
  } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
});
// 立即備份 Backup now
app.post('/api/backups', async (req, res) => {
  try {
    const b = await createBackup(wsKey(req), 'manual');
    if (!b) return res.status(409).json({ ok: false, error: 'no state' });
    await pruneBackups();
    res.json({ ok: true, id: b.id, created_at: b.created_at });
  } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
});
// 下載備份(同「匯出備份」格式,可直接用「匯入備份」還原)Download in the same format as Export Backup
app.get('/api/backups/:id', async (req, res) => {
  try {
    const r = await pool.query('SELECT id, created_at, reason, config, store FROM app_backups WHERE id = $1 AND workspace = $2', [Number(req.params.id) || 0, wsKey(req)]);
    if (!r.rows.length) return res.status(404).json({ ok: false, error: 'not found' });
    const x = r.rows[0]; const st = x.store || {};
    const hk = new Date(new Date(x.created_at).getTime() + 8 * 3600 * 1000).toISOString().slice(0, 19);
    const body = { _type: 'sunshine_laundry_backup', _version: 1, _exportedAt: hk.replace('T', ' '),
      _exportedBy: (x.reason === 'manual' ? '雲端手動備份 cloud manual backup' : '雲端自動備份 cloud auto backup') + ' #' + x.id,
      seq: st.seq || {}, config: x.config || {}, store: st.store || {} };
    res.setHeader('Content-Disposition', 'attachment; filename="sunshine_cloud_backup_' + hk.replace(/[:T]/g, '-') + '.json"');
    res.json(body);
  } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e) }); }
});

// ---- 前端 fallback(SPA):其他路徑派 index.html ----
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ---- 啟動 Start ----
initDb()
  .then(() => {
    app.listen(PORT, () => console.log(`[server] ServiceFlow Cloud 運行中 running on :${PORT}`));
    startScheduler();
    startBackupScheduler();
  })
  .catch((e) => {
    console.error('[fatal] 資料庫初始化失敗 DB init failed:', e);
    // 即使 DB 未 ready 都啟動,方便前端顯示錯誤 + 健康檢查
    // Start anyway so the frontend loads and health check reports the issue
    app.listen(PORT, () => console.log(`[server] 運行中(DB 未就緒)running on :${PORT} (DB not ready)`));
  });
