/**
 * ServiceFlow Cloud — Zeabur + PostgreSQL 後端
 * ServiceFlow Cloud — Zeabur + PostgreSQL backend
 *
 * 功能 Features:
 *  - 派靜態前端 (public/index.html)  Serve static frontend
 *  - GET  /api/health   健康檢查      Health check
 *  - GET  /api/state    讀取整包雲端資料  Read full cloud state
 *  - PUT  /api/state    儲存整包雲端資料  Save full cloud state
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
  console.log('[db] app_state 表已就緒 table ready');
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

// ---- 儲存整包資料 Save state ----
// body: { config?: {...}, store?: {...} }  只更新提供咗嘅部分 Only provided parts are updated
app.put('/api/state', async (req, res) => {
  try {
    const ws = wsKey(req);
    const { config, store } = req.body || {};
    if (config === undefined && store === undefined) {
      return res.status(400).json({ error: 'config 或 store 至少要有一個 need at least one of config/store' });
    }
    // Upsert:存在就更新提供咗嘅欄位,唔存在就插入。
    // Upsert: update provided columns if row exists, else insert.
    const r = await pool.query(
      `INSERT INTO app_state (workspace, config, store, updated_at)
       VALUES ($1, COALESCE($2::jsonb, '{}'::jsonb), COALESCE($3::jsonb, '{}'::jsonb), now())
       ON CONFLICT (workspace) DO UPDATE SET
         config = COALESCE($2::jsonb, app_state.config),
         store  = COALESCE($3::jsonb, app_state.store),
         updated_at = now()
       RETURNING updated_at`,
      [ws, config === undefined ? null : JSON.stringify(config), store === undefined ? null : JSON.stringify(store)]
    );
    res.json({ ok: true, updated_at: r.rows[0].updated_at });
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
  const orders = (store && store.orders) || [];
  const companies = (store && store.companies) || [];
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
    const newStore = Object.assign({}, store, { logs: [logEntry].concat((store && store.logs) || []).slice(0, 5000) });
    await pool.query(
      `INSERT INTO app_state (workspace, config, store, updated_at)
       VALUES ($1, $2::jsonb, $3::jsonb, now())
       ON CONFLICT (workspace) DO UPDATE SET config=$2::jsonb, store=$3::jsonb, updated_at=now()`,
      [ws, JSON.stringify(newConfig), JSON.stringify(newStore)]
    );
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

// ---- 前端 fallback(SPA):其他路徑派 index.html ----
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ---- 啟動 Start ----
initDb()
  .then(() => {
    app.listen(PORT, () => console.log(`[server] ServiceFlow Cloud 運行中 running on :${PORT}`));
    startScheduler();
  })
  .catch((e) => {
    console.error('[fatal] 資料庫初始化失敗 DB init failed:', e);
    // 即使 DB 未 ready 都啟動,方便前端顯示錯誤 + 健康檢查
    // Start anyway so the frontend loads and health check reports the issue
    app.listen(PORT, () => console.log(`[server] 運行中(DB 未就緒)running on :${PORT} (DB not ready)`));
  });
