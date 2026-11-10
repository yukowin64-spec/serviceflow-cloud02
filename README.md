# Sunshine Laundry Group 維修報單系統 — 雲端同步版
# ServiceFlow Cloud — Zeabur + PostgreSQL

呢個版本將原本「本機儲存」升級成「**雲端資料庫同步**」。
所有裝置(電腦 / 手機 / 唔同瀏覽器)登入後都連同一個 PostgreSQL 資料庫,
**還原一次,全部裝置即時生效**,唔會再「變返未還原」。

This version upgrades the app from browser-local storage to **cloud database sync**.
Every device (desktop / phone / different browsers) talks to the same PostgreSQL database,
so a restore applies everywhere and data no longer reverts.

---

## 架構 Architecture

```
瀏覽器前端 (public/index.html)  ──HTTP──▶  Node/Express 後端 (server.js)  ──▶  PostgreSQL
Browser frontend                          Backend API                         Database
```

- 前端 UI 完全保留原樣(下拉選單建單、公司/門店/設備/工單/使用者全部不變)。
- 後端只做兩件事:`GET /api/state`(攞成包資料)、`PUT /api/state`(存成包資料)。
- 資料存喺 PostgreSQL 一張表 `app_state`,自動建立,唔使你手動建表。

---

## 需要準備 What you need

1. 一個 **GitHub** 帳號(免費)— https://github.com
2. 一個 **Zeabur** 帳號(免費起)— https://zeabur.com
3. (可選)本機裝咗 **Git**,方便上傳程式碼。冇都得,可以用 GitHub 網頁上傳。

---

## 部署步驟 Deployment Steps

### 步驟 1️⃣ 上傳程式碼去 GitHub  Push code to GitHub

**做法 A:用網頁上傳(最簡單,唔使裝 Git)**

1. 去 https://github.com/new 開一個新 repository(例如 `serviceflow-cloud`),設為 Private。
2. 開好之後,喺 repo 頁面撳 **"uploading an existing file"**。
3. 將呢個資料夾入面**所有檔案**(`server.js`、`package.json`、`.gitignore`、`README.md`、`.env.example` 同埋成個 `public/` 資料夾)拉入去上傳。
4. 撳 **Commit changes**。

**做法 B:用 Git 指令(如果你識用)**

```bash
cd serviceflow-cloud
git init
git add .
git commit -m "ServiceFlow cloud sync"
git branch -M main
git remote add origin https://github.com/<你的帳號>/serviceflow-cloud.git
git push -u origin main
```

> ⚠️ 唔好上傳 `node_modules/` 同 `.env`(`.gitignore` 已經幫你排除咗)。

---

### 步驟 2️⃣ 喺 Zeabur 建立專案  Create a Zeabur project

1. 登入 https://zeabur.com,撳 **Create Project**,揀一個地區(建議 `Hong Kong` 或最近你嘅地區)。
2. 撳 **Add Service** ▶ **Deploy from GitHub**。
3. 授權 Zeabur 連你嘅 GitHub,揀返頭先個 `serviceflow-cloud` repo。
4. Zeabur 會自動偵測到係 Node.js 專案,自動用 `npm install` + `npm start` 部署。**唔使你設定 build 指令。**

---

### 步驟 3️⃣ 加 PostgreSQL 資料庫  Add PostgreSQL

1. 喺同一個 Project 入面,再撳 **Add Service** ▶ **Prebuilt** ▶ **PostgreSQL**。
2. Zeabur 會自動開好一個 PostgreSQL 服務。
3. **重點:** 撳返你個 app 服務 ▶ **Variables(環境變數)** ▶ 撳 **Add Variable** 或 **Reference**,
   將 PostgreSQL 服務嘅 `DATABASE_URL` 連過去 app。
   - 最簡單做法:喺 app 嘅 Variables 撳 **"Add Reference / 引用"**,揀 PostgreSQL 服務嘅 `DATABASE_URL`。
   - 或者手動加一個變數 `DATABASE_URL`,值填 PostgreSQL 服務畀你嘅連線字串。

> Zeabur 同一 Project 內嘅服務可以直接互相引用變數,毋須自己抄 IP。

---

### 步驟 4️⃣ 設定對外網址  Expose a public domain

1. 撳你個 app 服務 ▶ **Networking / Domains**。
2. 撳 **Generate Domain**,Zeabur 會畀你一個免費網址(例如 `serviceflow-cloud.zeabur.app`)。
3. (可選)你可以綁自己買嘅網域。

---

### 步驟 5️⃣ 開網址測試  Open and test

1. 開個 Zeabur 網址。
2. 撳右下角 **管理登入 Admin Login**,用你嘅帳號密碼登入。
3. 睇右上角應該顯示 **「雲端已連線 Cloud connected」** 或 **「雲端已同步 Cloud synced」**。
4. 新增一間公司 ▶ 用手機開同一個網址登入 ▶ 應該即刻見到同一份資料。✅

---

## 雲端狀態指示 Cloud status badge

App 右上角會顯示同步狀態:

| 顯示 Text | 意思 Meaning |
|---|---|
| 雲端已連線 Cloud connected | 已成功由雲端載入最新資料 |
| 雲端已同步 Cloud synced | 你嘅修改已成功存上雲端 |
| 雲端已更新 Cloud updated | 偵測到另一部機有新資料,已自動拉落嚟(唔使重載)|
| 雲端已初始化 Cloud initialized | 第一次用,已將現有資料上傳雲端 |
| 離線(僅本機)Offline (local only) | 連唔到後端,暫時只存本機 |

---

## 本次更新 What's new (2026-07)

### 1. 全面即時同步 Full real-time sync
- **修正:** 之前「建立訂單、建立使用者」冇即時上傳雲端,喺另一部機睇唔到。而家**所有寫入(訂單 / 使用者 / 公司 / 機器 / 零件 / 日誌)都會即時存上雲端**。
- **新增自動輪詢 Auto-poll:** 每 5 秒自動由雲端拉最新資料。**A 機新增嘅訂單 / 使用者,B 機唔使重新整理都會自動出現。**
- **防覆蓋合併 Anti-clobber merge:** 兩部機同時改嘢,推送前會先合併雲端最新資料(按編號去重),唔會再互相覆蓋、遺失記錄。

Fixes: order & user creation now push to the cloud instantly; a 5-second auto-poll pulls new data on every device without reload; and a merge-before-push step prevents two devices from overwriting each other's records.

### 2. 建立訂單:三種選機器方式 Three ways to pick a machine
喺前台「選擇設備 Select Machine」上方新增三個分頁,任揀一種:
1. **下拉選單 Dropdown** — 保留原本嘅下拉選單揀機器(不變)。
2. **輸入編號搜尋 Search ID** — 直接輸入機器編號(或名稱)搜尋;多過一個結果會列出畀你點選。
3. **公司 → 門店 → 機器 By Company** — 先揀公司,再揀門店,再揀機器,撳「確定」先顯示報單。

三種方式揀完之後,下面嘅報單表格同提交流程完全一樣。

Three tabbed methods above "Select Machine": keep the original dropdown, search by machine ID/name, or cascade Company → Branch → Machine. All three lead to the same order form.

---

## 首次資料遷移 First-time data migration

- 第一次有裝置登入時,若雲端係空,app 會**自動將該裝置嘅現有資料上傳做初始資料**。
- ⚠️ **建議:** 用你「資料最齊、最新」嗰部機第一個登入新網址,咁上到雲端就係正確嗰份。
- 之後所有裝置都以雲端為準,還原/修改任何一部都會同步。

---

## 本機開發(進階,可略過)Local development (optional)

```bash
npm install
# 準備一個本機 PostgreSQL,設 DATABASE_URL
cp .env.example .env   # 填返你嘅 DATABASE_URL
node server.js         # 開 http://localhost:8080
```

---

## 常見問題 FAQ

**Q：登入帳號密碼點解仲係喺前端?**
A：登入驗證維持原本嘅前端邏輯(帳號資料一樣存喺雲端 `users`)。若日後想要更高安全性,可以將登入改成後端驗證 + JWT,我可以再幫你做。

**Q：資料會唔會唔見?**
A：資料存喺 Zeabur 嘅 PostgreSQL,獨立於任何裝置。建議定期喺 app 內用「備份」功能匯出一份 JSON 留底。

**Q：可唔可以多間分公司各自一份資料?**
A：後端已預留 `workspace` 機制(預設 `default`)。若要分開,可在網址加 `?ws=公司代號`,我可以再幫你接埋 UI。
