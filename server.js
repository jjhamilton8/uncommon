// ─── Env ─────────────────────────────────────────────────────────────────────
const fs   = require('fs');
const path = require('path');
const envPath = path.join(__dirname, '.env');
if (fs.existsSync(envPath)) {
  fs.readFileSync(envPath, 'utf8').split('\n').forEach(line => {
    const [k, ...v] = line.split('=');
    if (k && v.length && !process.env[k.trim()]) process.env[k.trim()] = v.join('=').trim();
  });
}

const express  = require('express');
const session  = require('express-session');
const bcrypt   = require('bcryptjs');
const app      = express();

const PORT           = process.env.PORT || 3000;
const AT_TOKEN       = process.env.AIRTABLE_TOKEN;
const BASE_ID        = 'appZGDhMjhSLr07vP';
const SESSION_SECRET = process.env.SESSION_SECRET || 'sfm-life-mastery-2026';
const ADMIN_PIN      = process.env.ADMIN_PIN || 'sfmadmin2026';
const GHL_TOKEN      = process.env.GHL_TOKEN || '';
const GHL_LOCATION   = process.env.GHL_LOCATION_ID || '';

const TABLES = {
  clients:     'tblatTqpi7TdF7wL7',
  goals:       'tbla7tj9N0xuaCX6L',
  submissions: 'tbltsb1orgYQ7lKZX',
  testimonials:'tblWhDx0hA7Qo4JCx',
  onboarding:  'tblUSfmCrP1qA8kh9',
};

const AT_HEADERS = {
  Authorization:  `Bearer ${AT_TOKEN}`,
  'Content-Type': 'application/json',
};

// ─── Middleware ───────────────────────────────────────────────────────────────
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use(session({
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: { maxAge: 7 * 24 * 60 * 60 * 1000 },
}));

const requireAuth = (req, res, next) =>
  req.session.clientId ? next() : res.redirect('/login');

// ─── Airtable Helpers ─────────────────────────────────────────────────────────
async function fetchAll(tableId, filterFormula = null) {
  const records = []; let offset;
  do {
    const p = new URLSearchParams();
    if (filterFormula) p.set('filterByFormula', filterFormula);
    if (offset) p.set('offset', offset);
    const r = await fetch(`https://api.airtable.com/v0/${BASE_ID}/${tableId}?${p}`, { headers: AT_HEADERS });
    if (!r.ok) throw new Error(`AT ${r.status}: ${await r.text()}`);
    const d = await r.json();
    records.push(...d.records);
    offset = d.offset;
  } while (offset);
  return records;
}

async function fetchRecord(tableId, recordId) {
  const r = await fetch(`https://api.airtable.com/v0/${BASE_ID}/${tableId}/${recordId}`, { headers: AT_HEADERS });
  if (!r.ok) throw new Error(`AT ${r.status}`);
  return r.json();
}

async function patchRecord(tableId, recordId, fields) {
  const r = await fetch(`https://api.airtable.com/v0/${BASE_ID}/${tableId}/${recordId}`, {
    method: 'PATCH', headers: AT_HEADERS, body: JSON.stringify({ fields }),
  });
  if (!r.ok) throw new Error(`AT PATCH ${r.status}: ${await r.text()}`);
  return r.json();
}

async function createRecord(tableId, fields) {
  const r = await fetch(`https://api.airtable.com/v0/${BASE_ID}/${tableId}`, {
    method: 'POST', headers: AT_HEADERS, body: JSON.stringify({ fields }),
  });
  if (!r.ok) throw new Error(`AT POST ${r.status}: ${await r.text()}`);
  return r.json();
}

// ─── Data Helpers ─────────────────────────────────────────────────────────────
const fld  = (r, n) => r?.fields?.[n] ?? null;
const lk   = (r, n) => { const v = fld(r,n); return Array.isArray(v) ? (v[0]??null) : v; };

function goalsForClient(goals, clientId, onboardingIds) {
  return goals.filter(g =>
    (g.fields['Client Name']||[]).some(id => onboardingIds.has(id)) ||
    (g.fields['Clients']||[]).includes(clientId)
  );
}

function buildGoalsByClient(goals, clients) {
  const onbMap = {};
  for (const c of clients)
    for (const id of (c.fields['Onboarding response']||[])) onbMap[id] = c.id;
  const map = {};
  for (const g of goals) {
    const cid = (g.fields['Client Name']?.[0] && onbMap[g.fields['Client Name'][0]]) || g.fields['Clients']?.[0];
    if (cid) (map[cid] = map[cid]||[]).push(g);
  }
  return map;
}

function calcLifeScore(sub) {
  if (!sub) return null;
  const f = sub.fields;
  const energy    = f['Energy level'] || 0;
  const vision    = f['Vision clarity'] || 0;
  const ease      = f['Ease and grace'] || 0;
  const stress    = f['Stress level'] || 0;
  const intimacy  = f['Intimacy'] || 0;
  const comm      = f['Communication quality'] || 0;
  const presence  = f['Presence'] || 0;
  const rel = (intimacy + comm + presence) / 3;
  return Math.round(((energy + vision + ease + (10 - stress) + rel) / 5 / 10) * 100);
}

function daysSinceCheckin(submissions) {
  if (!submissions.length) return null;
  const latest = submissions.reduce((a, b) =>
    new Date(fld(a,'Submission date')) > new Date(fld(b,'Submission date')) ? a : b
  );
  const ms = Date.now() - new Date(fld(latest,'Submission date'));
  return Math.floor(ms / (1000 * 60 * 60 * 24));
}

function flagColor(days) {
  if (days === null) return { color:'#ef4444', bg:'rgba(239,68,68,0.12)', label:'Never checked in' };
  if (days <= 35)    return { color:'#22c55e', bg:'rgba(34,197,94,0.12)',  label:`${days}d ago` };
  if (days <= 65)    return { color:'#eab308', bg:'rgba(234,179,8,0.12)',  label:`${days}d ago ⚠️` };
  return              { color:'#ef4444', bg:'rgba(239,68,68,0.12)',       label:`${days}d ago 🔴` };
}

// ─── Design Constants ─────────────────────────────────────────────────────────
const CATS = {
  Health:        { emoji:'🏋️', color:'#22c55e' },
  HEALTH:        { emoji:'🏋️', color:'#22c55e' },
  Wealth:        { emoji:'💰', color:'#eab308' },
  Relationships: { emoji:'❤️',  color:'#ec4899' },
  Relationship:  { emoji:'❤️',  color:'#ec4899' },
  Purpose:       { emoji:'🎯', color:'#a855f7' },
};
const CAT_ORDER = [
  { label:'Health',        aliases:['Health','HEALTH']              },
  { label:'Wealth',        aliases:['Wealth']                       },
  { label:'Relationships', aliases:['Relationships','Relationship'] },
  { label:'Purpose',       aliases:['Purpose']                      },
];
const STATUS_COLORS = {
  Active:   { text:'#22c55e', bg:'rgba(34,197,94,0.12)'  },
  Achieved: { text:'#a855f7', bg:'rgba(168,85,247,0.12)' },
  Pivoted:  { text:'#eab308', bg:'rgba(234,179,8,0.12)'  },
  Paused:   { text:'#888',    bg:'rgba(136,136,136,0.12)'},
};

// ─── Shared CSS ───────────────────────────────────────────────────────────────
const CSS = `
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#060608;--surface:#0f0f13;--surface2:#16161c;--surface3:#1c1c24;
  --border:#22222e;--border2:#2a2a38;--text:#f0f0f0;--muted:#666;--muted2:#888;
  --gold:#C9A84C;--gold-light:#E8C96D;--gold-dim:rgba(201,168,76,0.15);
  --green:#22c55e;--yellow:#eab308;--red:#ef4444;--purple:#a855f7;--pink:#ec4899;
}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text);min-height:100vh;line-height:1.5}
a{color:inherit;text-decoration:none}

/* Header */
.header{background:rgba(10,10,14,0.95);backdrop-filter:blur(20px);border-bottom:1px solid var(--border);padding:.9rem 2rem;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}
.header-logo{display:flex;align-items:center;gap:.85rem}
.header-logo img{height:36px;width:auto;object-fit:contain}
.logo{font-size:1rem;font-weight:800;color:var(--gold);letter-spacing:2.5px;text-transform:uppercase}
.logo-sub{font-size:.6rem;color:var(--muted);letter-spacing:3px;text-transform:uppercase;margin-top:2px}

/* Layout */
.container{max-width:1400px;margin:0 auto;padding:2rem}

/* Cards */
.card{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:1.5rem;transition:border-color .2s}
.card:hover{border-color:var(--border2)}
.card-glow{box-shadow:0 0 0 1px var(--border),0 4px 24px rgba(0,0,0,.4)}

/* Labels */
.label{font-size:.6rem;font-weight:700;text-transform:uppercase;letter-spacing:2.5px;color:var(--muted)}

/* Buttons */
.btn{display:inline-flex;align-items:center;gap:.4rem;padding:.55rem 1.1rem;border-radius:8px;font-weight:600;font-size:.8rem;cursor:pointer;border:none;transition:all .2s;text-decoration:none;white-space:nowrap}
.btn-gold{background:linear-gradient(135deg,var(--gold),#b8932a);color:#000;box-shadow:0 2px 12px rgba(201,168,76,.25)}
.btn-gold:hover{background:linear-gradient(135deg,var(--gold-light),var(--gold));box-shadow:0 4px 20px rgba(201,168,76,.4);transform:translateY(-1px)}
.btn-ghost{background:transparent;color:var(--muted2);border:1px solid var(--border2)}
.btn-ghost:hover{background:var(--surface2);color:var(--text);border-color:var(--border2)}
.btn-red{background:rgba(239,68,68,.12);color:#f87171;border:1px solid rgba(239,68,68,.25)}.btn-red:hover{background:rgba(239,68,68,.22)}
.btn-green{background:rgba(34,197,94,.12);color:#4ade80;border:1px solid rgba(34,197,94,.25)}.btn-green:hover{background:rgba(34,197,94,.22)}

/* Pills & Status */
.status-pill{display:inline-block;padding:.2rem .65rem;border-radius:9999px;font-size:.6rem;font-weight:700;text-transform:uppercase;letter-spacing:.5px}

/* Inputs */
input,textarea,select{background:var(--surface2);border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:.9rem;outline:none;transition:border-color .15s,box-shadow .15s;font-family:inherit}
input:focus,textarea:focus,select:focus{border-color:var(--gold);box-shadow:0 0 0 3px rgba(201,168,76,.12)}
input[type=email],input[type=text],input[type=password],input[type=number]{width:100%;padding:.7rem 1rem}
textarea{width:100%;padding:.7rem 1rem;resize:vertical}
select{width:100%;padding:.7rem 1rem}

/* Score Ring */
.score-ring{display:inline-flex;align-items:center;justify-content:center;width:58px;height:58px;border-radius:50%;font-size:1.1rem;font-weight:800;border:3px solid;position:relative}
.score-ring::after{content:'';position:absolute;inset:-6px;border-radius:50%;opacity:.12;background:currentColor;filter:blur(8px)}

/* Notification badge */
.badge{display:inline-flex;align-items:center;justify-content:center;background:var(--red);color:#fff;border-radius:9999px;font-size:.6rem;font-weight:700;min-width:18px;height:18px;padding:0 .35rem}

/* Alert banner */
.alert-banner{padding:.75rem 1.5rem;font-size:.82rem;font-weight:600;display:flex;align-items:center;gap:.65rem;border-bottom:1px solid}
.alert-gold{background:rgba(201,168,76,.08);border-color:rgba(201,168,76,.2);color:var(--gold)}
.alert-green{background:rgba(34,197,94,.08);border-color:rgba(34,197,94,.2);color:#4ade80}
.alert-red{background:rgba(239,68,68,.08);border-color:rgba(239,68,68,.2);color:#f87171}

/* Skeleton loader */
@keyframes shimmer{0%{background-position:-400px 0}100%{background-position:400px 0}}
.skeleton{background:linear-gradient(90deg,var(--surface2) 25%,var(--surface3) 50%,var(--surface2) 75%);background-size:400px 100%;animation:shimmer 1.4s infinite;border-radius:6px}

/* Confetti */
@keyframes confetti-fall{0%{transform:translateY(-20px) rotate(0deg);opacity:1}100%{transform:translateY(100vh) rotate(720deg);opacity:0}}
.confetti-piece{position:fixed;width:8px;height:8px;border-radius:2px;animation:confetti-fall linear forwards;pointer-events:none;z-index:9999}

/* Mobile nav */
.mobile-menu{display:none;flex-direction:column;gap:.5rem;position:absolute;top:100%;right:1rem;background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:1rem;min-width:180px;box-shadow:0 8px 32px rgba(0,0,0,.4);z-index:200}
.mobile-menu.open{display:flex}
.hamburger{display:none;background:none;border:none;cursor:pointer;padding:.4rem;color:var(--muted2)}

/* Responsive */
@media(max-width:768px){
  .container{padding:1rem}
  .header{padding:.75rem 1rem}
  .desktop-only{display:none}
  .hamburger{display:flex;flex-direction:column;gap:5px}
  .hamburger span{display:block;width:22px;height:2px;background:currentColor;border-radius:9999px;transition:all .2s}
  .header-nav{display:none}
  .header-nav.mobile-open{display:flex;flex-direction:column;gap:.5rem}
}

/* Print */
@media print{
  .header,.btn,.no-print{display:none!important}
  body{background:#fff;color:#000}
  .card{border:1px solid #ddd;break-inside:avoid}
  .print-title{display:block!important}
}
.print-title{display:none}
`;

// ─── Score Ring HTML ──────────────────────────────────────────────────────────
function scoreRing(score) {
  if (score === null) return `<div style="font-size:.75rem;color:var(--muted);">No data</div>`;
  const color = score >= 70 ? '#22c55e' : score >= 45 ? '#eab308' : '#ef4444';
  return `<div class="score-ring" style="border-color:${color};color:${color};">${score}</div>`;
}

// ─── LOGIN PAGE ───────────────────────────────────────────────────────────────
function renderLogin(error = null) {
  return `<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>School for Men — Login</title>
<style>${CSS}
body{display:flex;flex-direction:column;min-height:100vh;background:var(--bg)}
.login-bg{flex:1;display:flex;align-items:center;justify-content:center;padding:2rem;
  background:radial-gradient(ellipse 80% 50% at 50% -10%,rgba(201,168,76,.12) 0%,transparent 60%),
             radial-gradient(ellipse 50% 40% at 80% 80%,rgba(168,85,247,.06) 0%,transparent 50%)}
.box{width:100%;max-width:440px}
.brand{text-align:center;margin-bottom:2.5rem}
.brand-logo{margin-bottom:1.25rem;display:flex;justify-content:center}
.brand-logo img{height:52px;width:auto;object-fit:contain}
.brand-logo-fallback{width:64px;height:64px;background:linear-gradient(135deg,var(--gold),#8a5e1a);border-radius:16px;display:flex;align-items:center;justify-content:center;font-size:1.5rem;font-weight:900;color:#000;letter-spacing:-1px;margin:0 auto}
.brand .logo{font-size:1.6rem;display:block;margin-bottom:.3rem;letter-spacing:3px}
.brand .tag{font-size:.7rem;color:var(--muted);letter-spacing:3px;text-transform:uppercase}
.login-card{background:var(--surface);border:1px solid var(--border);border-radius:20px;padding:2.25rem;box-shadow:0 24px 64px rgba(0,0,0,.5),0 0 0 1px rgba(201,168,76,.06)}
.err{background:rgba(239,68,68,.1);border:1px solid rgba(239,68,68,.3);border-radius:10px;padding:.8rem 1rem;font-size:.85rem;color:#f87171;margin-bottom:1.25rem;display:flex;align-items:center;gap:.5rem}
.fg{margin-bottom:1.15rem}
.fg label{display:block;font-size:.68rem;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);margin-bottom:.4rem}
.sub-btn{width:100%;padding:.9rem;background:linear-gradient(135deg,var(--gold),#b8932a);color:#000;border:none;border-radius:10px;font-size:.95rem;font-weight:700;cursor:pointer;letter-spacing:.5px;transition:all .2s;margin-top:.5rem;box-shadow:0 4px 16px rgba(201,168,76,.3)}
.sub-btn:hover{transform:translateY(-1px);box-shadow:0 6px 24px rgba(201,168,76,.45)}
.sub-btn:active{transform:translateY(0)}
.foot{text-align:center;margin-top:1.5rem;font-size:.78rem;color:var(--muted)}
.divider{display:flex;align-items:center;gap:.75rem;margin:1.5rem 0;color:var(--muted);font-size:.7rem}
.divider::before,.divider::after{content:'';flex:1;height:1px;background:var(--border)}
</style></head><body>
<div class="login-bg"><div class="box">
  <div class="brand">
    <div class="brand-logo">
      <img src="/logo.png" alt="School for Men" onerror="this.style.display='none';this.nextElementSibling.style.display='flex'">
      <div class="brand-logo-fallback" style="display:none">SFM</div>
    </div>
    <span class="logo">SCHOOL FOR MEN</span>
    <span class="tag">Life Mastery Tracker</span>
  </div>
  <div class="login-card">
    <h2 style="font-size:1.4rem;font-weight:800;margin-bottom:.3rem">Welcome back.</h2>
    <p style="color:var(--muted);font-size:.875rem;margin-bottom:1.75rem;line-height:1.6">Sign in to access your personal Life Mastery dashboard.</p>
    ${error ? `<div class="err">⚠️ ${error}</div>` : ''}
    <form method="POST" action="/login">
      <div class="fg"><label>Email address</label><input type="email" name="email" placeholder="you@example.com" required autofocus></div>
      <div class="fg"><label>Password</label><input type="password" name="password" placeholder="••••••••" required></div>
      <button type="submit" class="sub-btn">Access My Dashboard →</button>
    </form>
    <div class="divider">need help?</div>
    <p style="text-align:center;font-size:.8rem;color:var(--muted)">Forgot your password? <span style="color:var(--gold)">Contact your coach.</span></p>
  </div>
</div></div>
<footer style="padding:1rem;text-align:center;font-size:.7rem;color:var(--muted);border-top:1px solid var(--border)">
  © ${new Date().getFullYear()} School for Men. All rights reserved.
</footer>
</body></html>`;
}

// ─── SET PASSWORD PAGE (Admin) ────────────────────────────────────────────────
function renderSetPassword(clients, message = null, error = null) {
  const options = clients
    .sort((a,b) => (fld(a,'Client name')||'').localeCompare(fld(b,'Client name')||''))
    .map(c => `<option value="${c.id}">${fld(c,'Client name')}</option>`)
    .join('');
  return `<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Set Client Password</title>
<style>${CSS}
body{display:flex;align-items:center;justify-content:center;min-height:100vh;padding:2rem}
.box{width:100%;max-width:460px}
select{width:100%;padding:.7rem 1rem;background:var(--surface2);border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:.9rem;outline:none}
select:focus{border-color:var(--gold)}
.fg{margin-bottom:1.1rem}
.fg label{display:block;font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);margin-bottom:.4rem}
</style></head><body>
<div class="box">
  <div style="margin-bottom:2rem;text-align:center">
    <div class="logo" style="display:block;margin-bottom:.25rem">SCHOOL FOR MEN</div>
    <div style="font-size:.75rem;color:var(--muted);letter-spacing:2px;text-transform:uppercase">Admin — Set Client Password</div>
  </div>
  <div class="card">
    ${message ? `<div style="background:rgba(34,197,94,.1);border:1px solid rgba(34,197,94,.3);border-radius:8px;padding:.75rem 1rem;color:#4ade80;font-size:.85rem;margin-bottom:1.25rem">${message}</div>` : ''}
    ${error   ? `<div style="background:rgba(239,68,68,.1);border:1px solid rgba(239,68,68,.3);border-radius:8px;padding:.75rem 1rem;color:#f87171;font-size:.85rem;margin-bottom:1.25rem">${error}</div>` : ''}
    <form method="POST" action="/admin/set-password">
      <div class="fg"><label>Admin PIN</label><input type="password" name="pin" placeholder="Enter admin PIN" required></div>
      <div class="fg"><label>Client</label><select name="clientId">${options}</select></div>
      <div class="fg"><label>New Password</label><input type="password" name="password" placeholder="Minimum 8 characters" required minlength="8"></div>
      <div class="fg"><label>Confirm Password</label><input type="password" name="confirm" placeholder="Repeat password" required></div>
      <button type="submit" class="btn btn-gold" style="width:100%;justify-content:center;padding:.85rem;font-size:.95rem">Set Password</button>
    </form>
    <div style="margin-top:1.5rem;text-align:center"><a href="/" class="btn btn-ghost" style="font-size:.75rem">← Back to Dashboard</a></div>
  </div>
</div></body></html>`;
}

// ─── GOAL CARDS ───────────────────────────────────────────────────────────────
function renderGoalCards(goals, isClientView = false) {
  return CAT_ORDER.map(({ label, aliases }) => {
    const cat = CATS[label];
    const catGoals = goals.filter(g => aliases.includes(fld(g,'Category')||''));
    if (!catGoals.length) return `
      <div style="background:var(--surface2);border:1px solid var(--border);border-radius:12px;padding:1.5rem;border-top:3px solid ${cat.color}20">
        <div style="display:flex;align-items:center;gap:.5rem;margin-bottom:.75rem">
          <span>${cat.emoji}</span>
          <span style="font-size:.65rem;text-transform:uppercase;letter-spacing:2px;color:${cat.color};font-weight:700">${label}</span>
        </div>
        <div style="color:var(--muted);font-size:.85rem;font-style:italic">No goal set yet</div>
      </div>`;

    const goalItems = catGoals.map(g => {
      const status   = fld(g,'Current status') || 'Active';
      const sc       = STATUS_COLORS[status] || STATUS_COLORS.Active;
      const progress = fld(g,'Progress') || 0;
      const achieved = status === 'Achieved';

      const progressBar = `
        <div style="margin-top:.6rem">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:.25rem">
            <span style="font-size:.65rem;color:var(--muted);text-transform:uppercase;letter-spacing:1px">Progress</span>
            <span style="font-size:.72rem;font-weight:700;color:${cat.color}">${progress}%</span>
          </div>
          <div style="height:6px;background:var(--border);border-radius:9999px;overflow:hidden">
            <div style="height:100%;width:${progress}%;background:${cat.color};border-radius:9999px;transition:width .3s"></div>
          </div>
          ${isClientView && !achieved ? `
          <div style="display:flex;gap:.5rem;margin-top:.6rem;flex-wrap:wrap">
            <input type="range" min="0" max="100" value="${progress}" style="flex:1;accent-color:${cat.color};cursor:pointer"
              oninput="this.nextElementSibling.textContent=this.value+'%'"
              onchange="updateProgress('${g.id}',this.value)">
            <span style="font-size:.72rem;color:${cat.color};font-weight:700;min-width:36px">${progress}%</span>
          </div>
          <button onclick="achieveGoal('${g.id}',this)" class="btn btn-green no-print" style="margin-top:.5rem;font-size:.72rem;padding:.3rem .75rem">
            🏆 I hit my goal!
          </button>` : ''}
        </div>`;

      return `
        <div style="margin-bottom:.85rem;padding-bottom:.85rem;border-bottom:1px solid var(--border)">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:.5rem">
            <div style="font-size:.95rem;font-weight:600">${fld(g,'Goal name')}</div>
            <span class="status-pill" style="background:${sc.bg};color:${sc.text};white-space:nowrap">${status}</span>
          </div>
          ${fld(g,'Target metric') ? `<div style="font-size:.78rem;color:var(--muted);margin-top:.2rem">${fld(g,'Target metric')}</div>` : ''}
          ${fld(g,'Pivot note')    ? `<div style="font-size:.78rem;color:#eab308;margin-top:.25rem;font-style:italic">↻ ${fld(g,'Pivot note')}</div>` : ''}
          ${progressBar}
        </div>`;
    }).join('');

    return `
      <div style="background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:1.5rem;border-top:3px solid ${cat.color}">
        <div style="display:flex;align-items:center;gap:.5rem;margin-bottom:1rem">
          <span style="font-size:1.2rem">${cat.emoji}</span>
          <span style="font-size:.65rem;text-transform:uppercase;letter-spacing:2px;color:${cat.color};font-weight:700">${label}</span>
        </div>
        ${goalItems}
      </div>`;
  }).join('');
}

// ─── CHARTS + BEFORE/NOW ─────────────────────────────────────────────────────
function renderCharts(submissions, monthlyUrl) {
  if (!submissions.length) return `
    <div class="card" style="text-align:center;padding:3.5rem 2rem">
      <div style="font-size:2.5rem;margin-bottom:1rem">📊</div>
      <div style="font-size:1.1rem;font-weight:700;margin-bottom:.5rem">No check-ins yet</div>
      <div style="color:var(--muted);font-size:.9rem;max-width:400px;margin:0 auto 1.5rem">Progress charts appear here after your first monthly check-in.</div>
      <a href="${monthlyUrl}" target="_blank" class="btn btn-gold no-print">Submit First Check-in →</a>
    </div>`;

  const sorted = [...submissions].sort((a,b) => new Date(fld(a,'Submission date')) - new Date(fld(b,'Submission date')));
  const labels   = sorted.map(s => fld(s,'Month period') || fld(s,'Submission date') || '?');
  const energy   = sorted.map(s => fld(s,'Energy level'));
  const revenue  = sorted.map(s => fld(s,'Revenue this month'));
  const vision   = sorted.map(s => fld(s,'Vision clarity'));
  const intimacy = sorted.map(s => fld(s,'Intimacy'));
  const comm     = sorted.map(s => fld(s,'Communication quality'));
  const presence = sorted.map(s => fld(s,'Presence'));

  // Before vs Now (if 2+ submissions)
  let beforeNow = '';
  if (sorted.length >= 2) {
    const first = sorted[0].fields;
    const last  = sorted[sorted.length-1].fields;
    const delta = (a, b) => {
      if (!a && !b) return '';
      const d = (b||0) - (a||0);
      const color = d > 0 ? '#22c55e' : d < 0 ? '#ef4444' : '#777';
      const sign  = d > 0 ? '+' : '';
      return `<span style="font-size:.7rem;color:${color};font-weight:700;margin-left:.4rem">${sign}${d}</span>`;
    };
    const row = (label, key) => `
      <tr style="border-bottom:1px solid var(--border)">
        <td style="padding:.5rem .75rem;font-size:.82rem;color:var(--muted)">${label}</td>
        <td style="padding:.5rem .75rem;font-size:.82rem;text-align:center">${first[key]||'—'}</td>
        <td style="padding:.5rem .75rem;font-size:.82rem;text-align:center">${last[key]||'—'}${delta(first[key],last[key])}</td>
      </tr>`;
    beforeNow = `
      <div class="card" style="margin-top:1.25rem">
        <div class="label" style="margin-bottom:1rem">Before vs. Now — ${fld(sorted[0],'Month period')||'Start'} → ${fld(sorted[sorted.length-1],'Month period')||'Latest'}</div>
        <table style="width:100%;border-collapse:collapse">
          <thead><tr style="border-bottom:1px solid var(--border)">
            <th style="padding:.4rem .75rem;font-size:.65rem;text-align:left;text-transform:uppercase;letter-spacing:1px;color:var(--muted);font-weight:600">Metric</th>
            <th style="padding:.4rem .75rem;font-size:.65rem;text-align:center;text-transform:uppercase;letter-spacing:1px;color:var(--muted);font-weight:600">Start</th>
            <th style="padding:.4rem .75rem;font-size:.65rem;text-align:center;text-transform:uppercase;letter-spacing:1px;color:var(--muted);font-weight:600">Now</th>
          </tr></thead>
          <tbody>
            ${row('Energy Level','Energy level')}
            ${row('Vision Clarity','Vision clarity')}
            ${row('Ease & Grace','Ease and grace')}
            ${row('Stress Level','Stress level')}
            ${row('Intimacy','Intimacy')}
            ${row('Communication','Communication quality')}
            ${row('Presence','Presence')}
          </tbody>
        </table>
      </div>`;
  }

  // Recent submissions table
  const rows = [...submissions]
    .sort((a,b) => new Date(fld(b,'Submission date')) - new Date(fld(a,'Submission date')))
    .slice(0,6).map(s => `
      <tr style="border-bottom:1px solid var(--border)">
        <td style="padding:.55rem .75rem;font-size:.82rem;color:var(--muted)">${fld(s,'Month period')||fld(s,'Submission date')||'—'}</td>
        <td style="padding:.55rem .75rem;font-size:.82rem;text-align:center">${fld(s,'Energy level')||'—'}/10</td>
        <td style="padding:.55rem .75rem;font-size:.82rem;text-align:center">${fld(s,'Revenue this month') ? '$'+Number(fld(s,'Revenue this month')).toLocaleString() : '—'}</td>
        <td style="padding:.55rem .75rem;font-size:.82rem">${fld(s,'Big win')||'—'}${fld(s,'Pivot flagged?') ? ' ⚠️':''}</td>
      </tr>`).join('');

  const opts = `{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'#888',font:{size:11}}}},scales:{x:{ticks:{color:'#666'},grid:{color:'#1e1e1e'},border:{color:'#252525'}},y:{ticks:{color:'#666'},grid:{color:'#1e1e1e'},border:{color:'#252525'}}}}`;

  return `
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem" class="charts-grid">
      <div class="card"><div class="label" style="margin-bottom:1rem">Energy Level</div><div style="height:180px"><canvas id="cE"></canvas></div></div>
      <div class="card"><div class="label" style="margin-bottom:1rem">Revenue</div><div style="height:180px"><canvas id="cR"></canvas></div></div>
      <div class="card"><div class="label" style="margin-bottom:1rem">Vision Clarity</div><div style="height:180px"><canvas id="cV"></canvas></div></div>
      <div class="card"><div class="label" style="margin-bottom:1rem">Relationship Scores</div><div style="height:180px"><canvas id="cRel"></canvas></div></div>
    </div>
    <div class="card" style="margin-top:1.25rem">
      <div class="label" style="margin-bottom:1rem">Recent Check-ins (${submissions.length} total)</div>
      <div style="overflow-x:auto">
        <table style="width:100%;border-collapse:collapse;min-width:400px">
          <thead><tr style="border-bottom:1px solid var(--border)">
            <th style="padding:.4rem .75rem;font-size:.65rem;text-align:left;text-transform:uppercase;letter-spacing:1px;color:var(--muted);font-weight:600">Month</th>
            <th style="padding:.4rem .75rem;font-size:.65rem;text-align:center;text-transform:uppercase;letter-spacing:1px;color:var(--muted);font-weight:600">Energy</th>
            <th style="padding:.4rem .75rem;font-size:.65rem;text-align:center;text-transform:uppercase;letter-spacing:1px;color:var(--muted);font-weight:600">Revenue</th>
            <th style="padding:.4rem .75rem;font-size:.65rem;text-align:left;text-transform:uppercase;letter-spacing:1px;color:var(--muted);font-weight:600">Big Win</th>
          </tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    </div>
    ${beforeNow}
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
    <script>
      const L=${JSON.stringify(labels)};
      const o=${opts};
      const line=(id,ds,yMax)=>new Chart(document.getElementById(id),{type:'line',data:{labels:L,datasets:ds},options:{...o,scales:{...o.scales,y:{...o.scales.y,min:0,max:yMax||10}}}});
      line('cE',[{label:'Energy',data:${JSON.stringify(energy)},borderColor:'#22c55e',backgroundColor:'rgba(34,197,94,.08)',tension:.3,fill:true,spanGaps:true}]);
      line('cV',[{label:'Vision',data:${JSON.stringify(vision)},borderColor:'#a855f7',backgroundColor:'rgba(168,85,247,.08)',tension:.3,fill:true,spanGaps:true}]);
      line('cRel',[
        {label:'Intimacy',data:${JSON.stringify(intimacy)},borderColor:'#ec4899',tension:.3,spanGaps:true},
        {label:'Communication',data:${JSON.stringify(comm)},borderColor:'#f97316',tension:.3,spanGaps:true},
        {label:'Presence',data:${JSON.stringify(presence)},borderColor:'#06b6d4',tension:.3,spanGaps:true}
      ]);
      new Chart(document.getElementById('cR'),{type:'bar',data:{labels:L,datasets:[{label:'Revenue ($)',data:${JSON.stringify(revenue)},backgroundColor:'rgba(234,179,8,.5)',borderColor:'#eab308',borderWidth:2}]},options:o});
    </script>`;
}

// ─── CLIENT DASHBOARD ─────────────────────────────────────────────────────────
function renderClientDashboard(client, goals, submissions, isClientView = false) {
  const name       = fld(client,'Client name') || 'Client';
  const occupation = lk(client,'Business / Occupation') || '';
  const commitment = lk(client,'Commitment declaration') || '';
  const vision6    = lk(client,'6-month vision') || '';
  const vision12   = lk(client,'12-month vision') || '';
  const breakthrough = lk(client,'Breakthrough result') || '';
  const coachStyle = lk(client,'Coaching style preference') || '';
  const sabotage   = lk(client,'Self-sabotage pattern') || '';
  const body       = lk(client,'Body relationship baseline') || '';
  const monthlyUrl = fld(client,'Monthly Form URL') || '#';
  const email      = lk(client,'Email') || '';
  const startDate  = fld(client,'Start date')
    ? new Date(fld(client,'Start date')).toLocaleDateString('en-AU',{day:'numeric',month:'long',year:'numeric'})
    : 'Not set';

  const latestSub  = submissions.length
    ? submissions.reduce((a,b) => new Date(fld(a,'Submission date')) > new Date(fld(b,'Submission date')) ? a : b)
    : null;
  const score = calcLifeScore(latestSub);

  const info = (label, val) => val ? `
    <div style="margin-bottom:1.1rem">
      <div class="label" style="margin-bottom:.3rem">${label}</div>
      <div style="font-size:.88rem">${val}</div>
    </div>` : '';

  const navRight = isClientView
    ? `<div style="display:flex;gap:.75rem;align-items:center">
        <button onclick="window.print()" class="btn btn-ghost no-print" style="font-size:.75rem">📄 Save PDF</button>
        <a href="/logout" class="btn btn-ghost no-print" style="font-size:.75rem">Log Out</a>
       </div>`
    : `<a href="/" class="btn btn-ghost">← All Clients</a>`;

  const changePasswordForm = isClientView ? `
    <div class="card no-print">
      <div class="label" style="margin-bottom:1rem">Change Password</div>
      <form id="changePwForm">
        <div style="margin-bottom:.75rem"><input type="password" name="current" placeholder="Current password" required></div>
        <div style="margin-bottom:.75rem"><input type="password" name="newpw" placeholder="New password (min 8 chars)" required minlength="8"></div>
        <div style="margin-bottom:.75rem"><input type="password" name="confirm" placeholder="Confirm new password" required></div>
        <button type="submit" class="btn btn-gold" style="width:100%;justify-content:center">Update Password</button>
        <div id="pwMsg" style="font-size:.8rem;margin-top:.5rem;text-align:center"></div>
      </form>
    </div>` : '';

  const goalScript = isClientView ? `
    <script>
      async function updateProgress(goalId, value) {
        await fetch('/api/goal/'+goalId+'/progress', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({progress:parseInt(value)})});
      }
      async function achieveGoal(goalId, btn) {
        if (!confirm('Mark this goal as achieved? This cannot be undone.')) return;
        btn.disabled = true;
        btn.textContent = '🏆 Goal Achieved!';
        btn.className = 'btn btn-ghost no-print';
        launchConfetti();
        await fetch('/api/goal/'+goalId+'/achieve', {method:'POST'});
        setTimeout(()=>location.reload(), 2000);
      }
      document.getElementById('changePwForm')?.addEventListener('submit', async e => {
        e.preventDefault();
        const fd = new FormData(e.target);
        const msg = document.getElementById('pwMsg');
        if (fd.get('newpw') !== fd.get('confirm')) { msg.style.color='#f87171'; msg.textContent='Passwords do not match'; return; }
        const r = await fetch('/dashboard/change-password', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({current:fd.get('current'),newpw:fd.get('newpw')})});
        const d = await r.json();
        msg.style.color = d.ok ? '#4ade80' : '#f87171';
        msg.textContent = d.message;
        if (d.ok) e.target.reset();
      });
    // Confetti on goal achieve
    function launchConfetti() {
      const colors = ['#C9A84C','#E8C96D','#22c55e','#a855f7','#ec4899','#fff'];
      for(let i=0;i<80;i++){
        const el=document.createElement('div');
        el.className='confetti-piece';
        el.style.cssText='left:'+Math.random()*100+'vw;top:-10px;background:'+colors[Math.floor(Math.random()*colors.length)]+';width:'+(6+Math.random()*6)+'px;height:'+(6+Math.random()*6)+'px;animation-duration:'+(2+Math.random()*2)+'s;animation-delay:'+Math.random()*0.5+'s;border-radius:'+(Math.random()>0.5?'50%':'2px');
        document.body.appendChild(el);
        el.addEventListener('animationend',()=>el.remove());
      }
    }
    // Session timeout warning (25 min)
    let sessionTimer;
    function resetTimer(){
      clearTimeout(sessionTimer);
      sessionTimer=setTimeout(()=>{
        const banner=document.createElement('div');
        banner.className='alert-banner alert-red no-print';
        banner.style.cssText='position:fixed;bottom:1rem;right:1rem;z-index:9999;border-radius:12px;border:1px solid rgba(239,68,68,.3);max-width:320px;box-shadow:0 8px 32px rgba(0,0,0,.4)';
        banner.innerHTML='⚠️ Your session will expire in 5 minutes. <a href="/dashboard" style="color:#f87171;text-decoration:underline;margin-left:.5rem">Stay logged in</a>';
        document.body.appendChild(banner);
      }, 25*60*1000);
    }
    ['click','keypress','mousemove','scroll'].forEach(e=>document.addEventListener(e,resetTimer,{passive:true}));
    resetTimer();
    </script>` : '';

  return `<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${name} — Life Mastery Dashboard</title>
<style>
${CSS}
.hero{background:linear-gradient(160deg,var(--surface) 0%,#0d0a14 50%,var(--surface) 100%);border-bottom:1px solid var(--border);padding:2.5rem 2rem;position:relative;overflow:hidden}
.hero::before{content:'';position:absolute;inset:0;background:radial-gradient(ellipse 60% 80% at 0% 50%,rgba(201,168,76,.06) 0%,transparent 60%);pointer-events:none}
.hero-inner{max-width:1400px;margin:0 auto}
.commitment{margin-top:1.25rem;padding:1rem 1.25rem;background:rgba(201,168,76,.07);border-left:3px solid var(--gold);border-radius:0 8px 8px 0;font-style:italic;color:var(--gold-light);font-size:1rem;max-width:720px}
.layout{display:grid;grid-template-columns:1fr 320px;gap:1.5rem;align-items:start}
.goals-grid{display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:1.5rem}
.charts-grid{grid-template-columns:1fr 1fr}
@media(max-width:960px){.layout{grid-template-columns:1fr}}
@media(max-width:640px){.goals-grid{grid-template-columns:1fr}.charts-grid{grid-template-columns:1fr}}
</style></head><body>
<header class="header">
  <div class="header-logo">
    <img src="/logo.png" alt="SFM" style="height:32px;width:auto;object-fit:contain" onerror="this.style.display='none'">
    <div><div class="logo">School for Men</div><div class="logo-sub">Life Mastery Tracker</div></div>
  </div>
  <div style="display:flex;align-items:center;gap:.75rem" class="desktop-only">${navRight}</div>
  <div style="position:relative">
    <button class="hamburger" id="ham" onclick="document.getElementById('mobileNav').classList.toggle('open')" aria-label="Menu" style="display:none">
      <span></span><span></span><span></span>
    </button>
    <div class="mobile-menu" id="mobileNav">
      ${isClientView ? `<a href="/dashboard/checkin" class="btn btn-gold" style="justify-content:center">Submit Check-in</a>
      <button onclick="window.print()" class="btn btn-ghost">📄 Save PDF</button>
      <a href="/logout" class="btn btn-ghost">Log Out</a>` : `<a href="/" class="btn btn-ghost">← All Clients</a>`}
    </div>
  </div>
</header>
<script>
// Show hamburger on mobile, hide desktop nav
if(window.innerWidth<=768){
  document.getElementById('ham').style.display='flex';
  document.querySelectorAll('.desktop-only').forEach(el=>el.style.display='none');
}
window.addEventListener('resize',()=>{
  const m=window.innerWidth<=768;
  document.getElementById('ham').style.display=m?'flex':'none';
  document.querySelectorAll('.desktop-only').forEach(el=>el.style.display=m?'none':'flex');
});
// Close menu on outside click
document.addEventListener('click',e=>{if(!e.target.closest('#ham')&&!e.target.closest('#mobileNav'))document.getElementById('mobileNav').classList.remove('open')});
</script>
<div class="print-title" style="padding:1.5rem 2rem;border-bottom:1px solid #ddd">
  <h1 style="font-size:1.5rem">${name} — Life Mastery Report</h1>
  <p style="color:#666;font-size:.85rem">Generated ${new Date().toLocaleDateString('en-AU',{day:'numeric',month:'long',year:'numeric'})}</p>
</div>
${(() => {
  const days = daysSinceCheckin(submissions);
  if (days === null || days > 28) {
    const msg = days === null ? "📋 You haven't submitted a check-in yet. Start tracking your progress!" : `⏰ It's been ${days} days since your last check-in. Time for a new one!`;
    return `<div class="alert-banner alert-gold no-print" style="background:rgba(201,168,76,.07);border-bottom:1px solid rgba(201,168,76,.2);color:var(--gold);padding:.7rem 2rem;font-size:.82rem;font-weight:600;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:.5rem">
      <span>${msg}</span>
      <a href="/dashboard/checkin" class="btn btn-gold" style="font-size:.72rem;padding:.35rem .8rem">Submit Check-in →</a>
    </div>`;
  }
  return '';
})()}
<div class="hero">
  <div class="hero-inner">
    <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:1rem;flex-wrap:wrap">
      <div>
        <div style="font-size:.65rem;text-transform:uppercase;letter-spacing:3px;color:var(--gold);margin-bottom:.5rem">Life Mastery Dashboard</div>
        <h1 style="font-size:2.25rem;font-weight:800;line-height:1.1">${name}</h1>
        ${occupation ? `<div style="color:var(--muted);margin-top:.35rem">${occupation}</div>` : ''}
      </div>
      <div style="text-align:center">
        <div class="label" style="margin-bottom:.5rem">Life Score</div>
        ${scoreRing(score)}
        ${score !== null ? `<div style="font-size:.7rem;color:var(--muted);margin-top:.3rem">out of 100</div>` : ''}
      </div>
    </div>
    ${commitment ? `<div class="commitment">"${commitment}"</div>` : ''}
  </div>
</div>
<main class="container">
  <div class="layout">
    <div>
      <div class="label" style="margin-bottom:1rem">90-Day Goals</div>
      <div class="goals-grid">${renderGoalCards(goals, isClientView)}</div>
      <div class="label" style="margin-bottom:1rem">Progress</div>
      ${renderCharts(submissions, monthlyUrl)}
      ${latestSub ? (() => {
        const ls = latestSub.fields;
        const month = ls['Month period'] || ls['Submission date'] || 'Latest';
        const reflection = (icon, label, color, text) => text ? `
          <div style="padding:1rem 1.25rem;background:${color}08;border-left:3px solid ${color};border-radius:0 10px 10px 0;margin-bottom:.85rem">
            <div style="font-size:.6rem;font-weight:700;text-transform:uppercase;letter-spacing:2px;color:${color};margin-bottom:.35rem">${icon} ${label}</div>
            <div style="font-size:.875rem;line-height:1.65;color:var(--text)">${text}</div>
          </div>` : '';
        const bigWin      = ls['Big win'] || '';
        const relMoment   = ls['Relationship win'] || '';
        const bizBrk      = ls['Business notes'] ? ls['Business notes'].split('\n')[0] : '';
        const mindshift   = ls['What I learned'] ? ls['What I learned'].split('\n').slice(0,3).join('\n') : '';
        const fatherhood  = ls['Relationship notes'] && ls['Relationship notes'].includes('FATHERHOOD')
          ? ls['Relationship notes'].split('---FATHERHOOD---')[1]?.split('\n').slice(0,4).join('\n').trim() || ''
          : '';
        if (!bigWin && !relMoment && !bizBrk && !mindshift) return '';
        return `
          <div class="card" style="margin-top:1.5rem">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:1.25rem;flex-wrap:wrap;gap:.5rem">
              <div class="label">Latest Month Reflections</div>
              <span style="font-size:.72rem;color:var(--gold);font-weight:600;background:rgba(201,168,76,.1);padding:.2rem .6rem;border-radius:9999px">${month}</span>
            </div>
            ${reflection('🏆','Biggest Win','#C9A84C', bigWin)}
            ${reflection('❤️','Relationship Moment','#ec4899', relMoment)}
            ${reflection('👨‍👧','Fatherhood Win','#f97316', fatherhood)}
            ${reflection('💼','Business Breakthrough','#eab308', bizBrk)}
            ${reflection('🧠','Mindset Shift','#06b6d4', mindshift)}
          </div>`;
      })() : ''}
    </div>
    <div style="display:flex;flex-direction:column;gap:1.25rem">
      <div class="card no-print">
        <div class="label" style="margin-bottom:1rem">Monthly Check-in</div>
        <p style="font-size:.82rem;color:var(--muted);margin-bottom:1rem;line-height:1.6">Sent every first Friday of the month.</p>
        ${isClientView
          ? `<a href="/dashboard/checkin" class="btn btn-gold" style="width:100%;justify-content:center">Submit Check-in →</a>`
          : `<a href="/client/${client.id}/checkin" class="btn btn-gold" style="width:100%;justify-content:center">Open Form →</a>`
        }
        ${submissions.length ? `<div style="font-size:.72rem;color:var(--muted);text-align:center;margin-top:.75rem">${submissions.length} check-in${submissions.length>1?'s':''} submitted</div>` : ''}
      </div>
      <div class="card">
        <div class="label" style="margin-bottom:1rem">Profile</div>
        ${info('Started', startDate)}
        ${info('Occupation', occupation)}
        ${!isClientView ? info('Email', email ? `<a href="mailto:${email}" style="color:var(--gold)">${email}</a>` : '') : ''}
        ${info('Coaching Style', coachStyle)}
        ${info('Self-Sabotage Pattern', sabotage)}
        ${info('Body Baseline', body)}
        ${info('Breakthrough Goal', breakthrough)}
      </div>
      ${vision6||vision12 ? `
      <div class="card">
        <div class="label" style="margin-bottom:1rem">Visions</div>
        ${info('6-Month Vision', vision6)}
        ${info('12-Month Vision', vision12)}
      </div>` : ''}
      ${changePasswordForm}
    </div>
  </div>
</main>
${goalScript}
</body></html>`;
}

// ─── ADMIN DASHBOARD ──────────────────────────────────────────────────────────
function renderAdmin(clients, goalsByClientId, allSubmissions, pendingTestimonials) {
  const totalGoals = Object.values(goalsByClientId).flat().length;
  const thisMonth  = new Date().toISOString().slice(0,7);
  const checkInsThisMonth = allSubmissions.filter(s => (fld(s,'Submission date')||'').startsWith(thisMonth)).length;

  // Build submission map per client (using resolved _clientId — linked field OR name fallback)
  const subsByClient = {};
  for (const s of allSubmissions) {
    const cid = s._clientId || (s.fields['Clients']||[])[0];
    if (cid) (subsByClient[cid] = subsByClient[cid]||[]).push(s);
  }

  const cards = clients
    .sort((a,b) => (fld(a,'Client name')||'').localeCompare(fld(b,'Client name')||''))
    .map(client => {
      const name       = fld(client,'Client name') || 'Unknown';
      const occupation = lk(client,'Business / Occupation') || '';
      const relStatus  = lk(client,'Relationship status') || '';
      const startDate  = fld(client,'Start date')
        ? new Date(fld(client,'Start date')).toLocaleDateString('en-AU',{month:'short',year:'numeric'})
        : '—';
      const goals = goalsByClientId[client.id] || [];
      const subs  = subsByClient[client.id] || [];
      const days  = daysSinceCheckin(subs);
      const flag  = flagColor(days);
      const score = calcLifeScore(subs.length ? subs.reduce((a,b) => new Date(fld(a,'Submission date')) > new Date(fld(b,'Submission date')) ? a : b) : null);
      const notes = fld(client,'Notes') || '';

      const goalRows = CAT_ORDER.map(({label,aliases}) => {
        const cat = CATS[label];
        const catGoals = goals.filter(g => aliases.includes(fld(g,'Category')||''));
        if (!catGoals.length) return '';
        return `<div style="display:flex;align-items:flex-start;gap:.5rem;padding:.5rem 0;border-bottom:1px solid var(--border)">
          <span>${cat.emoji}</span>
          <div>
            <div style="font-size:.6rem;text-transform:uppercase;letter-spacing:1.5px;color:${cat.color};font-weight:700;margin-bottom:.1rem">${label}</div>
            <div style="font-size:.8rem">${catGoals.map(g=>fld(g,'Goal name')).join(' · ')}</div>
          </div>
        </div>`;
      }).join('');

      return `
        <div class="card" style="display:flex;flex-direction:column;gap:1rem" data-client-card data-name="${name}" data-score="${score||''}" data-days="${days!==null?days:999}" data-goals="${goals.map(g=>fld(g,'Goal name')).join(', ')}">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:.5rem">
            <div>
              <div style="font-size:1.1rem;font-weight:700">${name}</div>
              <div style="font-size:.78rem;color:var(--muted);margin-top:.15rem">${[occupation,relStatus].filter(Boolean).join(' · ')}</div>
            </div>
            <div style="display:flex;flex-direction:column;align-items:flex-end;gap:.4rem">
              ${scoreRing(score)}
              <span style="font-size:.65rem;padding:.15rem .5rem;border-radius:9999px;background:${flag.bg};color:${flag.color};font-weight:600">${flag.label}</span>
            </div>
          </div>
          <div style="flex:1;min-height:60px">${goalRows||'<div style="color:var(--muted);font-size:.8rem;font-style:italic">No goals seeded</div>'}</div>
          <div>
            <div class="label" style="margin-bottom:.4rem">Coach Notes</div>
            <textarea rows="2" placeholder="Add private notes…" style="font-size:.82rem"
              onblur="saveNotes('${client.id}',this.value,this)">${notes}</textarea>
            <div id="note-${client.id}" style="font-size:.7rem;color:var(--muted);margin-top:.2rem;height:14px"></div>
          </div>
          <div style="display:flex;justify-content:space-between;align-items:center;padding-top:.75rem;border-top:1px solid var(--border)">
            <div style="font-size:.7rem;color:var(--muted)">Started ${startDate}</div>
            <a href="/client/${client.id}" class="btn btn-gold" style="padding:.4rem .9rem">View Dashboard →</a>
          </div>
        </div>`;
    }).join('');

  // Testimonials pending approval
  const testimonialRows = pendingTestimonials.map(s => {
    const clientLinks = s.fields['Clients'] || [];
    const clientName  = s._clientName || 'Unknown';
    const bigWin      = fld(s,'Big win') || '—';
    const date        = fld(s,'Submission date') || '—';
    return `
      <tr style="border-bottom:1px solid var(--border)">
        <td style="padding:.6rem .75rem;font-size:.82rem;font-weight:600">${clientName}</td>
        <td style="padding:.6rem .75rem;font-size:.82rem;color:var(--muted)">${date}</td>
        <td style="padding:.6rem .75rem;font-size:.82rem">${bigWin}</td>
        <td style="padding:.6rem .75rem;white-space:nowrap">
          <button onclick="approveTestimonial('${s.id}','${clientName}',this)" class="btn btn-green" style="font-size:.7rem;padding:.3rem .65rem">✓ Approve</button>
        </td>
      </tr>`;
  }).join('');

  return `<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>School for Men — Coach Dashboard</title>
<style>
${CSS}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:1rem;margin-bottom:2rem}
.stat{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:1.5rem;position:relative;overflow:hidden;transition:border-color .2s}
.stat:hover{border-color:var(--border2)}
.stat::before{content:'';position:absolute;top:-20px;right:-20px;width:80px;height:80px;border-radius:50%;background:var(--gold-dim);filter:blur(20px);pointer-events:none}
.stat-num{font-size:2.75rem;font-weight:800;line-height:1;margin-bottom:.25rem;color:var(--gold)}
.stat-lbl{font-size:.7rem;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted)}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:1.25rem}
@media(max-width:768px){.grid{grid-template-columns:1fr}}
</style></head><body>
<header class="header">
  <div class="header-logo">
    <img src="/logo.png" alt="SFM" style="height:32px;width:auto;object-fit:contain" onerror="this.style.display='none'">
    <div><div class="logo">School for Men</div><div class="logo-sub">Coach Dashboard</div></div>
  </div>
  <div style="display:flex;align-items:center;gap:.75rem">
    <button onclick="exportCSV()" class="btn btn-ghost desktop-only" style="font-size:.75rem">📥 Export CSV</button>
    <a href="/admin/set-password" class="btn btn-ghost desktop-only" style="font-size:.75rem">🔑 Set Passwords</a>
    <a href="/testimonials" target="_blank" class="btn btn-ghost desktop-only" style="font-size:.75rem">⭐ Testimonials</a>
    <a href="/login" class="btn btn-ghost" style="font-size:.75rem">Client Login →</a>
  </div>
</header>
<main class="container">
  <div style="margin-bottom:1.5rem;display:flex;align-items:flex-start;justify-content:space-between;gap:1rem;flex-wrap:wrap">
    <div>
      <h1 style="font-size:1.75rem;font-weight:800;margin-bottom:.25rem">Client Overview</h1>
      <p style="color:var(--muted);font-size:.9rem">All active clients, life scores, and check-in status.</p>
    </div>
    <div style="display:flex;gap:.5rem;align-items:center;flex-wrap:wrap">
      <select id="sortSelect" onchange="sortClients(this.value)" style="font-size:.78rem;padding:.4rem .75rem;width:auto">
        <option value="name">Sort: Name</option>
        <option value="score-desc">Sort: Score ↓</option>
        <option value="score-asc">Sort: Score ↑</option>
        <option value="checkin">Sort: Last Check-in</option>
      </select>
      <button onclick="exportCSV()" class="btn btn-ghost" style="font-size:.75rem">📥 Export CSV</button>
    </div>
  </div>
  <div class="stats">
    <div class="stat"><div class="stat-num">${clients.length}</div><div class="stat-lbl">Active Clients</div></div>
    <div class="stat"><div class="stat-num">${totalGoals}</div><div class="stat-lbl">Goals Seeded</div></div>
    <div class="stat"><div class="stat-num" style="color:#22c55e">${checkInsThisMonth}</div><div class="stat-lbl">Check-ins This Month</div></div>
    <div class="stat"><div class="stat-num" style="color:#a855f7">${pendingTestimonials.length}</div><div class="stat-lbl">Testimonials Pending</div></div>
  </div>

  ${pendingTestimonials.length ? `
  <div style="margin-bottom:2rem">
    <div class="label" style="margin-bottom:1rem">⭐ Testimonials Awaiting Approval (${pendingTestimonials.length})</div>
    <div class="card">
      <div style="overflow-x:auto">
        <table style="width:100%;border-collapse:collapse;min-width:500px">
          <thead><tr style="border-bottom:1px solid var(--border)">
            <th style="padding:.4rem .75rem;font-size:.65rem;text-align:left;text-transform:uppercase;letter-spacing:1px;color:var(--muted);font-weight:600">Client</th>
            <th style="padding:.4rem .75rem;font-size:.65rem;text-align:left;text-transform:uppercase;letter-spacing:1px;color:var(--muted);font-weight:600">Date</th>
            <th style="padding:.4rem .75rem;font-size:.65rem;text-align:left;text-transform:uppercase;letter-spacing:1px;color:var(--muted);font-weight:600">Big Win</th>
            <th style="padding:.4rem .75rem;font-size:.65rem;text-align:left;text-transform:uppercase;letter-spacing:1px;color:var(--muted);font-weight:600">Action</th>
          </tr></thead>
          <tbody>${testimonialRows}</tbody>
        </table>
      </div>
    </div>
  </div>` : ''}

  <div class="label" style="margin-bottom:1rem">Clients (${clients.length})</div>
  <div class="grid" id="clientGrid">${cards}</div>
</main>
<script>
  async function saveNotes(clientId, value, el) {
    const ind = document.getElementById('note-'+clientId);
    ind.style.color = 'var(--muted)'; ind.textContent = 'Saving…';
    await fetch('/api/client/'+clientId+'/notes', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({notes:value})});
    ind.style.color = '#4ade80'; ind.textContent = 'Saved ✓';
    setTimeout(()=>ind.textContent='', 2000);
  }
  // Sort clients
  function sortClients(mode) {
    const grid = document.getElementById('clientGrid');
    const cards = [...grid.querySelectorAll('[data-client-card]')];
    cards.sort((a,b) => {
      if (mode==='score-desc') return (Number(b.dataset.score)||0)-(Number(a.dataset.score)||0);
      if (mode==='score-asc') return (Number(a.dataset.score)||0)-(Number(b.dataset.score)||0);
      if (mode==='checkin') return (Number(a.dataset.days)||999)-(Number(b.dataset.days)||999);
      return a.dataset.name.localeCompare(b.dataset.name);
    });
    cards.forEach(c=>grid.appendChild(c));
  }
  // Export CSV
  function exportCSV() {
    const rows = [['Name','Score','Days Since Check-in','Goals']];
    document.querySelectorAll('[data-client-card]').forEach(c=>{
      rows.push([c.dataset.name, c.dataset.score||'', c.dataset.days||'Never', c.dataset.goals||'']);
    });
    const csv = rows.map(r=>r.map(v=>'"'+String(v).replace(/"/g,'""')+'"').join(',')).join('\n');
    const a = document.createElement('a');
    a.href = 'data:text/csv;charset=utf-8,'+encodeURIComponent(csv);
    a.download = 'sfm-clients-'+new Date().toISOString().slice(0,10)+'.csv';
    a.click();
  }
  async function approveTestimonial(submissionId, clientName, btn) {
    btn.disabled = true; btn.textContent = '✓ Approved';
    await fetch('/api/testimonial/'+submissionId+'/approve', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({clientName})});
    btn.closest('tr').style.opacity = '.4';
  }
</script>
</body></html>`;
}

// Fetch submissions for a single client
async function fetchSubmissionsForClient(clientId, clientName) {
  // Fetch all submissions and filter in JS — avoids formula errors if field names differ
  const all = await fetchAll(TABLES.submissions);

  // Match by linked Clients field (record ID)
  const byLink = all.filter(s => (s.fields['Clients'] || []).includes(clientId));
  if (byLink.length) return byLink;

  // Fallback: match by any text field equal to the client name
  if (!clientName) return [];
  const nameLower = clientName.toLowerCase().trim();
  return all.filter(s =>
    Object.values(s.fields).some(v =>
      typeof v === 'string' && v.toLowerCase().trim() === nameLower
    )
  );
}

// ─── CHECK-IN FORM ────────────────────────────────────────────────────────────
function renderCheckinForm(clientName = '', error = null, formAction = '/dashboard/checkin') {
  const today = new Date();
  const monthLabel = today.toLocaleDateString('en-AU', { month: 'long', year: 'numeric' });
  const dateISO   = today.toISOString().slice(0, 10);
  const firstName = clientName.split(' ')[0] || 'Man';

  // ── Helpers ──────────────────────────────────────────────────────────────────
  const q = (label, input, prompt = '') => `
    <div style="margin-bottom:1.75rem">
      <label style="display:block;font-size:.82rem;font-weight:700;color:var(--text);margin-bottom:.3rem;line-height:1.4">${label}</label>
      ${prompt ? `<div style="font-size:.75rem;color:var(--muted);margin-bottom:.6rem;line-height:1.5;font-style:italic">${prompt}</div>` : ''}
      ${input}
    </div>`;

  const slider = (name, label, leftLabel = 'Low', rightLabel = 'High', prompt = '') => q(label, `
    <div style="display:flex;align-items:center;gap:.85rem">
      <span style="font-size:.68rem;color:var(--muted);white-space:nowrap">${leftLabel}</span>
      <input type="range" name="${name}" min="1" max="10" value="5" style="flex:1;accent-color:var(--gold);cursor:pointer;height:6px" oninput="this.nextElementSibling.textContent=this.value">
      <span style="font-size:1.2rem;font-weight:900;color:var(--gold);min-width:28px;text-align:center">5</span>
      <span style="font-size:.68rem;color:var(--muted);white-space:nowrap">${rightLabel}</span>
    </div>`, prompt);

  const num = (name, label, placeholder = '', prompt = '') =>
    q(label, `<input type="number" name="${name}" placeholder="${placeholder}" style="width:100%;padding:.75rem 1rem;background:var(--surface2);border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:.95rem;outline:none" onfocus="this.style.borderColor='var(--gold)';this.style.boxShadow='0 0 0 3px rgba(201,168,76,.12)'" onblur="this.style.borderColor='var(--border)';this.style.boxShadow='none'">`, prompt);

  const ta = (name, label, placeholder = '', rows = 3, prompt = '') =>
    q(label, `<textarea name="${name}" rows="${rows}" placeholder="${placeholder}" style="width:100%;padding:.75rem 1rem;background:var(--surface2);border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:.92rem;outline:none;resize:vertical;font-family:inherit;line-height:1.6" onfocus="this.style.borderColor='var(--gold)';this.style.boxShadow='0 0 0 3px rgba(201,168,76,.12)'" onblur="this.style.borderColor='var(--border)';this.style.boxShadow='none'"></textarea>`, prompt);

  const pillar = (color, icon, title, subtitle, layerContent) => `
    <div style="margin-bottom:1.5rem">
      <div style="background:linear-gradient(135deg,var(--surface) 0%,var(--surface2) 100%);border:1px solid var(--border);border-radius:16px;overflow:hidden">
        <!-- Pillar Header -->
        <div style="padding:1.5rem 1.75rem 1.25rem;border-bottom:1px solid var(--border);background:${color}0d;border-top:3px solid ${color}">
          <div style="display:flex;align-items:center;gap:.75rem;margin-bottom:.35rem">
            <span style="font-size:1.5rem">${icon}</span>
            <div>
              <div style="font-size:1.05rem;font-weight:800;color:var(--text)">${title}</div>
              <div style="font-size:.72rem;color:${color};font-weight:600;text-transform:uppercase;letter-spacing:1.5px">${subtitle}</div>
            </div>
          </div>
        </div>
        <!-- Pillar Score -->
        <div style="padding:1.25rem 1.75rem;border-bottom:1px solid var(--border);background:${color}06">
          ${slider(`score_${title.toLowerCase().replace(/[^a-z]/g,'_')}`, `Rate your ${title} this month overall`, '1 — Rock bottom', '10 — Thriving')}
        </div>
        <!-- Layers -->
        <div style="padding:1.5rem 1.75rem">
          ${layerContent}
        </div>
      </div>
    </div>`;

  const layer = (num, label, color, content) => `
    <div style="margin-bottom:2rem;padding-bottom:2rem;border-bottom:1px solid var(--border)">
      <div style="display:flex;align-items:center;gap:.6rem;margin-bottom:1.25rem">
        <div style="background:${color}22;color:${color};border-radius:50%;width:24px;height:24px;display:flex;align-items:center;justify-content:center;font-size:.65rem;font-weight:800;flex-shrink:0">${num}</div>
        <div style="font-size:.65rem;font-weight:700;text-transform:uppercase;letter-spacing:2px;color:${color}">${label}</div>
      </div>
      ${content}
    </div>`;

  return `<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Monthly Check-in — School for Men</title>
<style>
${CSS}
.progress-bar{position:fixed;top:0;left:0;height:3px;background:linear-gradient(90deg,var(--gold),var(--gold-light));z-index:9999;transition:width .3s ease}
</style></head><body>
<div class="progress-bar" id="progressBar" style="width:0%"></div>
<header class="header">
  <div class="header-logo">
    <img src="/logo.png" alt="SFM" style="height:32px;width:auto;object-fit:contain" onerror="this.style.display='none'">
    <div><div class="logo">School for Men</div><div class="logo-sub">Monthly Check-in</div></div>
  </div>
  <a href="/dashboard" class="btn btn-ghost" style="font-size:.75rem">← Dashboard</a>
</header>

<!-- HERO -->
<div style="background:linear-gradient(160deg,var(--surface) 0%,#0a0710 100%);border-bottom:1px solid var(--border);padding:2.5rem 2rem;position:relative;overflow:hidden">
  <div style="position:absolute;inset:0;background:radial-gradient(ellipse 70% 60% at 0% 50%,rgba(201,168,76,.07) 0%,transparent 60%);pointer-events:none"></div>
  <div style="max-width:860px;margin:0 auto;position:relative">
    <div style="font-size:.6rem;text-transform:uppercase;letter-spacing:3px;color:var(--gold);margin-bottom:.6rem;font-weight:700">Monthly Truth Session</div>
    <h1 style="font-size:2rem;font-weight:900;margin-bottom:.5rem;line-height:1.1">${firstName}, this is your mirror.</h1>
    <p style="color:var(--muted);font-size:.95rem;max-width:560px;line-height:1.7">Answer every question with brutal honesty. Vague answers produce vague results. Give specifics. Give truth. This is where transformation is measured.</p>
    <div style="display:flex;gap:1.5rem;margin-top:1.5rem;flex-wrap:wrap">
      <div style="text-align:center"><div style="font-size:1.4rem;font-weight:900;color:var(--gold)">5</div><div style="font-size:.62rem;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted)">Pillars</div></div>
      <div style="text-align:center"><div style="font-size:1.4rem;font-weight:900;color:var(--gold)">25+</div><div style="font-size:.62rem;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted)">Questions</div></div>
      <div style="text-align:center"><div style="font-size:1.4rem;font-weight:900;color:var(--gold)">~15</div><div style="font-size:.62rem;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted)">Minutes</div></div>
    </div>
  </div>
</div>

<main style="max-width:860px;margin:0 auto;padding:2rem 1rem">
  ${error ? `<div style="background:rgba(239,68,68,.1);border:1px solid rgba(239,68,68,.3);border-radius:10px;padding:.9rem 1.1rem;color:#f87171;font-size:.875rem;margin-bottom:1.5rem">⚠️ ${error}</div>` : ''}

  <form method="POST" action="${formAction}" id="checkinForm">
    <input type="hidden" name="submissionDate" value="${dateISO}">

    <!-- Month -->
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:1.5rem;margin-bottom:1.5rem">
      ${q('Which month is this check-in for?',
        `<input type="text" name="monthPeriod" value="${monthLabel}" required style="width:100%;padding:.75rem 1rem;background:var(--surface2);border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:.95rem;outline:none" onfocus="this.style.borderColor='var(--gold)';this.style.boxShadow='0 0 0 3px rgba(201,168,76,.12)'" onblur="this.style.borderColor='var(--border)';this.style.boxShadow='none'">`,
        'Be specific about which 30-day cycle you are reflecting on.'
      )}
    </div>

    <!-- ═══════════════════════════════════════════════════════════ -->
    <!-- PILLAR 1: NERVOUS SYSTEM                                    -->
    <!-- ═══════════════════════════════════════════════════════════ -->
    ${pillar('#06b6d4','🧠','Nervous System','Emotional Regulation · Presence · Reactivity',`
      ${layer('1','Objective Metrics','#06b6d4',`
        ${slider('easeAndGrace','How calm have you been this month?','1 — Reactive / on edge','10 — Grounded / unshakeable','Rate your baseline state — not your best day. Your average.')}
        ${slider('stressLevel','How often did you react vs respond?','1 — Full reactivity','10 — Full ownership','1 = you snapped, escalated, or shut down. 10 = you chose your response every time.')}
        ${slider('presence','Presence in key moments','1 — Checked out','10 — Fully here','How present were you in the moments that actually mattered?')}
      `)}
      ${layer('2','Behavioural Shifts','#06b6d4',`
        ${ta('ns_behaviour','What are you doing now that you were not doing 30 days ago?','Be specific. Think: morning routines, breathing, pausing before responding, difficult conversations you\'re now having…',3,'Behaviour = action. Not mindset. Not intention. What are you actually doing differently?')}
      `)}
      ${layer('3','External Feedback','#06b6d4',`
        ${ta('ns_external','How are the people around you responding to your energy?','Your partner, children, team, clients — what are they doing or saying differently?',3,'Others are always responding to your nervous system. What signals are they giving you?')}
      `)}
      ${layer('4','Identity Shift','#06b6d4',`
        ${ta('ns_identity','Who are you becoming in the way you handle pressure?','Describe the version of you that\'s emerging under stress, conflict, or uncertainty.',3,'Compare the man you were 90 days ago to the man in the mirror today. Be specific about the gap.')}
      `)}
      ${layer('5','Emotional Truth','#06b6d4',`
        ${ta('ns_truth','Describe a moment this month where your response surprised you — in a good way.','What happened? How did you respond? How would the old you have handled it?',4,'This is where the real data lives. Be honest. Be specific. One real situation.')}
      `)}
    `)}

    <!-- ═══════════════════════════════════════════════════════════ -->
    <!-- PILLAR 2: RELATIONSHIP                                      -->
    <!-- ═══════════════════════════════════════════════════════════ -->
    ${pillar('#ec4899','❤️','Relationship','Intimacy · Polarity · Respect · Connection',`
      ${layer('1','Objective Metrics','#ec4899',`
        ${slider('intimacy','Intimacy and sexual connection this month','1 — Non-existent','10 — Deep and frequent','Rate frequency AND quality together.')}
        ${slider('communicationQuality','Quality of communication with your partner','1 — Disconnected','10 — Fully understood','How well are you hearing each other — not just talking?')}
        ${ta('rel_conflicts','How did you handle conflict this month?','Give one specific example — what happened, how it started, how it resolved.',2,'Conflict is data. Don\'t skip this.')}
      `)}
      ${layer('2','Behavioural Shifts','#ec4899',`
        ${ta('rel_behaviour','What are you doing differently in your relationship this month?','Initiating, listening, leading, being vulnerable, setting standards — what has changed?',3,'Actions only. What are you physically doing differently?')}
      `)}
      ${layer('3','External Feedback — CRITICAL','#ec4899',`
        ${ta('rel_external','What has your partner said or done differently toward you this month?','Exact words. Specific behaviours. Tone changes. Physical changes.',3,'Don\'t interpret. Don\'t assume. What has she actually said or done? Use her words if you can remember them.')}
      `)}
      ${layer('4','Identity Shift','#ec4899',`
        ${ta('rel_identity','Where are you leading in your relationship instead of reacting?','Give a real situation — a moment where you chose presence or leadership over avoidance or control.',3,'Leadership in relationship means safety, consistency, and direction. Where are you embodying that?')}
      `)}
      ${layer('5','Emotional Truth','#ec4899',`
        ${ta('relationshipWin','What would your partner say about you now that she would NOT have said 3 months ago?','Write it as if she is speaking. First person. Her voice.',4,'Push through the discomfort of this one. If you don\'t know — that\'s also an answer.')}
      `)}
    `)}

    <!-- ═══════════════════════════════════════════════════════════ -->
    <!-- PILLAR 3: FATHERHOOD                                        -->
    <!-- ═══════════════════════════════════════════════════════════ -->
    ${pillar('#f97316','👨‍👧','Fatherhood','Presence · Leadership · Emotional Availability',`
      ${layer('1','Objective Metrics','#f97316',`
        ${num('fatherhood_hours','Hours of intentional, undivided presence with your children this month','e.g. 12','Not hours in the same house. Hours truly present — phone down, eyes on them, fully engaged.')}
        ${slider('fatherhood_quality','Quality of fatherhood this month','1 — Absent or reactive','10 — Present and leading','How would your children rate the version of Dad they got this month?')}
      `)}
      ${layer('2','Behavioural Shifts','#f97316',`
        ${ta('fatherhood_behaviour','What are you doing now as a father that you were not doing 30 days ago?','Morning routines, conversations, discipline, emotional availability, physical presence.',3,'Be specific. Generalities don\'t build men — and they don\'t build children either.')}
      `)}
      ${layer('3','External Feedback','#f97316',`
        ${ta('fatherhood_external','What are your children doing differently around you?','Look for: trust, openness, wanting to be near you, improved behaviour, emotional expression.',3,'Children are always responding to who their father is — not just what he does. What are they telling you?')}
      `)}
      ${layer('4','Identity Shift','#f97316',`
        ${ta('fatherhood_identity','What kind of father are you becoming that your own father was not?','Or — what are you repeating that you know you need to stop?',3,'This is about the generational pattern. Be honest about what you are passing down.')}
      `)}
      ${layer('5','Emotional Truth','#f97316',`
        ${ta('fatherhood_truth','Describe a moment with your children this month that moved you.','What happened? What did you feel? What did it tell you about who you\'re becoming as a father?',4,'If you can\'t find a moment — that\'s the answer. Sit with that.')}
      `)}
    `)}

    <!-- ═══════════════════════════════════════════════════════════ -->
    <!-- PILLAR 4: LEADERSHIP & BUSINESS                             -->
    <!-- ═══════════════════════════════════════════════════════════ -->
    ${pillar('#eab308','💼','Leadership & Business','Clarity · Revenue · Decisions · Pressure',`
      ${layer('1','Objective Metrics','#eab308',`
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem">
          <div>${num('revenueThisMonth','Revenue This Month ($)','0')}</div>
          <div>${num('profitThisMonth','Profit This Month ($)','0')}</div>
          <div>${num('hoursWorked','Avg Hours Worked Per Week','40','Not total hours — average per week.')}</div>
          <div>${num('training_sessions','Training Sessions This Week','3')}</div>
        </div>
        ${slider('visionClarity','Business clarity and strategic focus','1 — Scattered / reactive','10 — Clear and executing','Do you know exactly what needs to happen next? Are you doing it?')}
      `)}
      ${layer('2','Behavioural Shifts','#eab308',`
        ${ta('leadership_behaviour','What decisions have you made this month that the old version of you would have avoided?','Firing someone, raising prices, ending a contract, delegating more, saying no.',3,'Leadership is decision-making under pressure. What have you decided?')}
      `)}
      ${layer('3','External Feedback','#eab308',`
        ${ta('leadership_external','How is your team, business, or market responding to your leadership right now?','Client feedback, team dynamics, revenue trends, referrals, repeat business.',3,'Numbers don\'t lie. People\'s behaviour doesn\'t lie. What are they telling you?')}
      `)}
      ${layer('4','Identity Shift','#eab308',`
        ${ta('businessNotes','Where are you leading from clarity instead of operating from fear?','Give a real example from this month — a decision, a conversation, a moment of commitment.',3,'Fear builds businesses that collapse. Clarity builds empires. Where are you operating from?')}
      `)}
      ${layer('5','Emotional Truth','#eab308',`
        ${ta('leadership_truth','What is the business or professional situation you have been avoiding — and why?','Be specific. Name it. Own it.',3,'Avoidance is the most expensive habit in business. What are you not looking at?')}
      `)}
    `)}

    <!-- ═══════════════════════════════════════════════════════════ -->
    <!-- PILLAR 5: HEALTH                                            -->
    <!-- ═══════════════════════════════════════════════════════════ -->
    ${pillar('#22c55e','🏋️','Health','Training · Energy · Discipline · Body',`
      ${layer('1','Objective Metrics','#22c55e',`
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem">
          <div>${num('sleepScore','Sleep Quality Score (1–100 or 1–10)','e.g. 78')}</div>
          <div>${num('deepSleepHrs','Deep Sleep (avg hrs/night)','e.g. 1.5')}</div>
          <div>${num('hrv','HRV (avg this month)','e.g. 52')}</div>
          <div>${num('avgHeartRate','Resting Heart Rate (bpm)','e.g. 58')}</div>
        </div>
        ${slider('energyLevel','Energy levels this month','1 — Exhausted','10 — Unstoppable','Your honest average — not your best day.')}
      `)}
      ${layer('2','Behavioural Shifts','#22c55e',`
        ${ta('health_behaviour','What health habits are you consistently doing now that you were not 30 days ago?','Training frequency, nutrition, sleep hygiene, supplements, recovery.',3,'Consistency is the metric. What have you actually stuck to?')}
      `)}
      ${layer('3','External Feedback','#22c55e',`
        ${ta('healthNotes','What are people noticing about your physical presence, energy, or appearance?','Comments received, looks you\'re getting, energy in rooms.',2,'Others notice before you do. What are they seeing?')}
      `)}
      ${layer('4','Identity Shift','#22c55e',`
        ${ta('health_identity','What is your relationship with your body becoming?','Move from "I should train more" to who you are becoming as a physical man.',3,'Discipline in the body creates discipline in everything. What identity are you building?')}
      `)}
      ${layer('5','Emotional Truth','#22c55e',`
        ${ta('health_truth','Where are you still self-sabotaging your health — and what is the honest reason?','Alcohol, food, sleep deprivation, skipping training. Name it. Own it.',3,'Self-sabotage always has a story underneath it. What is yours?')}
      `)}
    `)}

    <!-- ═══════════════════════════════════════════════════════════ -->
    <!-- GLOBAL: BIGGEST WINS                                        -->
    <!-- ═══════════════════════════════════════════════════════════ -->
    <div style="background:linear-gradient(135deg,rgba(201,168,76,.08),rgba(201,168,76,.03));border:1px solid rgba(201,168,76,.2);border-radius:16px;padding:1.75rem;margin-bottom:1.5rem">
      <div style="display:flex;align-items:center;gap:.75rem;margin-bottom:1.5rem">
        <span style="font-size:1.4rem">🏆</span>
        <div>
          <div style="font-size:1rem;font-weight:800;color:var(--gold)">Biggest Wins This Month</div>
          <div style="font-size:.72rem;color:var(--muted);margin-top:.1rem;text-transform:uppercase;letter-spacing:1px">Across all areas of your life</div>
        </div>
      </div>
      ${ta('bigWin','Win #1 — Your biggest shift or achievement this month','Describe it specifically. What happened, what did it mean, and why does it matter?',3,'This is the moment your coach can share. Make it real. Make it specific.')}
      ${ta('whatWentWell','Win #2 — A relationship, leadership, or health win','Another specific shift you are proud of this month.',3)}
      ${ta('what_i_learned_wins','Win #3 — A mindset or identity shift','Something you now believe or know about yourself that you didn\'t before.',2)}
    </div>

    <!-- ═══════════════════════════════════════════════════════════ -->
    <!-- GLOBAL: BRUTAL TRUTH                                        -->
    <!-- ═══════════════════════════════════════════════════════════ -->
    <div style="background:rgba(239,68,68,.04);border:1px solid rgba(239,68,68,.15);border-radius:16px;padding:1.75rem;margin-bottom:1.5rem">
      <div style="display:flex;align-items:center;gap:.75rem;margin-bottom:1.5rem">
        <span style="font-size:1.4rem">🔥</span>
        <div>
          <div style="font-size:1rem;font-weight:800;color:#f87171">Brutal Truth Section</div>
          <div style="font-size:.72rem;color:var(--muted);margin-top:.1rem;text-transform:uppercase;letter-spacing:1px">No comfortable answers allowed here</div>
        </div>
      </div>
      ${ta('whatDidntGoWell','Where are you still avoiding responsibility?','What are you not addressing that you know you should be? Name it specifically.',3,'The thing that came to mind first as you read this question — that\'s the answer.')}
      ${ta('purposeBlocker','Where are you out of alignment right now?','Think behaviour vs values. Where are you saying one thing and doing another?',3,'Integrity is doing what you said you would do, even when no one is watching. Where are you failing this?')}
    </div>

    <!-- ═══════════════════════════════════════════════════════════ -->
    <!-- GLOBAL: BEFORE vs AFTER                                     -->
    <!-- ═══════════════════════════════════════════════════════════ -->
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:16px;padding:1.75rem;margin-bottom:1.5rem">
      <div style="display:flex;align-items:center;gap:.75rem;margin-bottom:1.5rem">
        <span style="font-size:1.4rem">🔄</span>
        <div>
          <div style="font-size:1rem;font-weight:800">Before vs After Snapshot</div>
          <div style="font-size:.72rem;color:var(--muted);margin-top:.1rem;text-transform:uppercase;letter-spacing:1px">Who you were vs who you are</div>
        </div>
      </div>
      ${ta('whatILearned','Who were you 30–90 days ago vs who you are now?','Focus on identity, leadership, relationships, and emotional control. Be specific about the differences.',4,'Finish this sentence: "The man I was would have ______. The man I am now ______."')}
      ${ta('whatImChanging','What are you committing to changing in the next 30 days?','One specific, measurable behaviour shift. Not a wish. A commitment.',2,'If you can\'t write it clearly in one sentence — it\'s not clear enough.')}
    </div>

    <!-- ═══════════════════════════════════════════════════════════ -->
    <!-- GLOBAL: TESTIMONIAL EXTRACTION                              -->
    <!-- ═══════════════════════════════════════════════════════════ -->
    <div style="background:linear-gradient(135deg,rgba(168,85,247,.06),rgba(168,85,247,.02));border:1px solid rgba(168,85,247,.2);border-radius:16px;padding:1.75rem;margin-bottom:1.5rem">
      <div style="display:flex;align-items:center;gap:.75rem;margin-bottom:.75rem">
        <span style="font-size:1.4rem">⭐</span>
        <div>
          <div style="font-size:1rem;font-weight:800;color:#c084fc">Your Testimony</div>
          <div style="font-size:.72rem;color:var(--muted);margin-top:.1rem;text-transform:uppercase;letter-spacing:1px">What this work has actually done</div>
        </div>
      </div>
      <p style="font-size:.82rem;color:var(--muted);margin-bottom:1.5rem;line-height:1.7">These questions produce the most powerful, honest reflections. Answer them like you\'re speaking to another man who needs to hear the truth.</p>
      ${ta('testimonial_1','What has changed in your life since starting this work?','Think: relationships, business, self-respect, how you start your day, how you handle pressure.',4,'Be specific. Avoid clichés. Speak to the real change — the thing you\'d tell your best friend.')}
      ${ta('testimonial_2','If another man was on the fence about joining, what would you say to him?','Be direct. Speak to his current struggles. Tell him what you wish someone had told you.',3,'This is not a sales pitch. This is man-to-man truth.')}
      ${ta('testimonial_3','What is something you now experience that you didn\'t think was possible before starting?','Describe it in full. What is it like to be you in this area of life now?',3,'The impossible becoming normal is the greatest testimony. What is that for you?')}
    </div>

    <!-- FLAGS -->
    <div style="background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:1.5rem;margin-bottom:1.5rem">
      <div style="font-size:.65rem;font-weight:700;text-transform:uppercase;letter-spacing:2px;color:var(--muted);margin-bottom:1.25rem">Final Flags</div>
      <div style="display:flex;flex-direction:column;gap:1rem">
        <label style="display:flex;align-items:flex-start;gap:.75rem;cursor:pointer;padding:.85rem;background:var(--surface2);border-radius:10px;border:1px solid var(--border)">
          <input type="checkbox" name="pivotFlagged" value="true" style="width:18px;height:18px;accent-color:var(--gold);cursor:pointer;margin-top:2px;flex-shrink:0">
          <div>
            <div style="font-size:.9rem;font-weight:700">⚠️ I need to pivot my goals or approach</div>
            <div style="font-size:.78rem;color:var(--muted);margin-top:.2rem">Flag this for a deeper conversation with your coach in your next session.</div>
          </div>
        </label>
        <label style="display:flex;align-items:flex-start;gap:.75rem;cursor:pointer;padding:.85rem;background:var(--surface2);border-radius:10px;border:1px solid var(--border)">
          <input type="checkbox" name="testimonialReady" value="true" style="width:18px;height:18px;accent-color:#c084fc;cursor:pointer;margin-top:2px;flex-shrink:0">
          <div>
            <div style="font-size:.9rem;font-weight:700">⭐ I'm happy for my coach to use my words as a testimonial</div>
            <div style="font-size:.78rem;color:var(--muted);margin-top:.2rem">Your transformation story may inspire another man to take the first step.</div>
          </div>
        </label>
      </div>
    </div>

    <!-- SUBMIT -->
    <div style="text-align:center;padding:2rem 0">
      <p style="font-size:.85rem;color:var(--muted);margin-bottom:1.5rem;line-height:1.7;max-width:480px;margin-left:auto;margin-right:auto">You have done the hard work. Now record it. Every answer here becomes evidence of who you are becoming.</p>
      <button type="submit" class="btn btn-gold" style="padding:1rem 2.5rem;font-size:1.05rem;font-weight:800;letter-spacing:.5px">Submit This Month's Check-in →</button>
      <div style="margin-top:1rem"><a href="/dashboard" class="btn btn-ghost">Not now — back to dashboard</a></div>
    </div>
  </form>
</main>
<script>
  // Progress bar as you scroll
  window.addEventListener('scroll',()=>{
    const h=document.documentElement;
    const pct=h.scrollTop/(h.scrollHeight-h.clientHeight)*100;
    document.getElementById('progressBar').style.width=pct+'%';
  });
  // Disable submit on click
  document.getElementById('checkinForm').addEventListener('submit', function() {
    const btn = this.querySelector('button[type=submit]');
    btn.disabled = true;
    btn.textContent = 'Submitting your truth…';
  });
</script>
</body></html>`;
}

// GET check-in form
app.get('/dashboard/checkin', requireAuth, async (req, res) => {
  try {
    const client = await fetchRecord(TABLES.clients, req.session.clientId);
    const name   = fld(client, 'Client name') || 'Client';
    res.send(renderCheckinForm(name, req.query.error));
  } catch (err) {
    res.status(500).send(`<pre style="color:red;padding:2rem">${err.message}</pre>`);
  }
});

// POST check-in form
app.post('/dashboard/checkin', requireAuth, async (req, res) => {
  try {
    const clientId  = req.session.clientId;
    const clientRec = await fetchRecord(TABLES.clients, clientId);
    const clientName = fld(clientRec, 'Client name') || '';
    const b = req.body;

    // ── Consolidate new pillar narratives into existing Airtable text fields ──
    const nsNarrative = [
      b.ns_behaviour && `BEHAVIOURAL SHIFTS:\n${b.ns_behaviour}`,
      b.ns_external  && `EXTERNAL FEEDBACK:\n${b.ns_external}`,
      b.ns_identity  && `IDENTITY SHIFT:\n${b.ns_identity}`,
      b.ns_truth     && `EMOTIONAL TRUTH:\n${b.ns_truth}`,
    ].filter(Boolean).join('\n\n');

    const relNarrative = [
      b.rel_behaviour && `BEHAVIOURAL SHIFTS:\n${b.rel_behaviour}`,
      b.rel_external  && `EXTERNAL FEEDBACK:\n${b.rel_external}`,
      b.rel_identity  && `IDENTITY SHIFT:\n${b.rel_identity}`,
    ].filter(Boolean).join('\n\n');

    const fatherhoodNarrative = [
      b.fatherhood_behaviour && `FATHERHOOD SHIFTS:\n${b.fatherhood_behaviour}`,
      b.fatherhood_external  && `CHILDREN RESPONDING:\n${b.fatherhood_external}`,
      b.fatherhood_identity  && `IDENTITY:\n${b.fatherhood_identity}`,
      b.fatherhood_truth     && `EMOTIONAL TRUTH:\n${b.fatherhood_truth}`,
    ].filter(Boolean).join('\n\n');

    const leadershipNarrative = [
      b.leadership_behaviour && `LEADERSHIP SHIFTS:\n${b.leadership_behaviour}`,
      b.leadership_external  && `EXTERNAL FEEDBACK:\n${b.leadership_external}`,
      b.leadership_truth     && `EMOTIONAL TRUTH:\n${b.leadership_truth}`,
    ].filter(Boolean).join('\n\n');

    const healthNarrative = [
      b.health_behaviour && `BEHAVIOURAL SHIFTS:\n${b.health_behaviour}`,
      b.healthNotes      && `EXTERNAL FEEDBACK:\n${b.healthNotes}`,
      b.health_identity  && `IDENTITY SHIFT:\n${b.health_identity}`,
      b.health_truth     && `EMOTIONAL TRUTH:\n${b.health_truth}`,
    ].filter(Boolean).join('\n\n');

    const testimonials = [
      b.testimonial_1 && `WHAT HAS CHANGED:\n${b.testimonial_1}`,
      b.testimonial_2 && `TO A MAN ON THE FENCE:\n${b.testimonial_2}`,
      b.testimonial_3 && `WHAT SEEMED IMPOSSIBLE:\n${b.testimonial_3}`,
    ].filter(Boolean).join('\n\n');

    const fields = {
      // Core identifiers
      'Submission date':       b.submissionDate || new Date().toISOString().slice(0,10),
      'Month period':          b.monthPeriod || '',
      'Client':                clientName,

      // Nervous System → mapped to existing calm/stress/presence fields
      'Ease and grace':        parseInt(b.easeAndGrace)         || 5,
      'Stress level':          parseInt(b.stressLevel)          || 5,
      'Presence':              parseInt(b.presence)             || 5,

      // Relationship
      'Intimacy':              parseInt(b.intimacy)             || 5,
      'Communication quality': parseInt(b.communicationQuality) || 5,
      'Relationship win':      b.relationshipWin                || null,
      'Relationship notes':    [relNarrative, fatherhoodNarrative].filter(Boolean).join('\n\n---FATHERHOOD---\n\n') || null,

      // Leadership / Business
      'Vision clarity':        parseInt(b.visionClarity)        || 5,
      'Revenue this month':    parseFloat(b.revenueThisMonth)   || null,
      'Profit this month':     parseFloat(b.profitThisMonth)    || null,
      'Hours worked':          parseFloat(b.hoursWorked)        || null,
      'Business notes':        leadershipNarrative               || null,
      'Vision-aligned action': b.visionAlignedAction            || null,
      'Purpose blocker':       b.purposeBlocker                 || null,

      // Health
      'Energy level':          parseInt(b.energyLevel)          || 5,
      'Sleep score':           parseFloat(b.sleepScore)         || null,
      'Deep sleep hrs':        parseFloat(b.deepSleepHrs)       || null,
      'HRV':                   parseFloat(b.hrv)                || null,
      'Average heart rate':    parseFloat(b.avgHeartRate)       || null,
      'Health notes':          healthNarrative                   || null,

      // Global reflection sections
      'Big win':               b.bigWin                         || null,
      'What went well':        b.whatWentWell                   || null,
      "What didn't go well":   b.whatDidntGoWell                || null,
      'What I learned':        [b.whatILearned, b.what_i_learned_wins].filter(Boolean).join('\n\n') || null,
      "What I'm changing":     b.whatImChanging                 || null,

      // Testimonials → stored in existing narrative fields
      'Nervous System':        [nsNarrative, testimonials ? `\n\n---TESTIMONIALS---\n\n${testimonials}` : ''].join('') || null,

      // Flags
      'Pivot flagged?':        b.pivotFlagged === 'true',
      'Testimonial ready?':    b.testimonialReady === 'true',
    };

    // Remove null/empty fields so Airtable doesn't complain
    Object.keys(fields).forEach(k => { if (fields[k] === null || fields[k] === '') delete fields[k]; });

    // Try full save; if unknown fields, retry with safe core fields only
    try {
      await createRecord(TABLES.submissions, fields);
    } catch (airtableErr) {
      if (airtableErr.message.includes('UNKNOWN_FIELD_NAME')) {
        // Strip any field Airtable doesn't recognise and retry with core fields only
        const safeFields = {};
        const knownFields = ['Submission date','Month period','Client','Ease and grace','Stress level',
          'Presence','Intimacy','Communication quality','Relationship win','Relationship notes',
          'Vision clarity','Revenue this month','Profit this month','Hours worked','Business notes',
          'Energy level','Sleep score','Deep sleep hrs','HRV','Average heart rate','Health notes',
          'Big win','What went well',"What didn't go well",'What I learned',"What I'm changing",
          'Pivot flagged?','Testimonial ready?'];
        for (const k of knownFields) { if (fields[k] !== undefined) safeFields[k] = fields[k]; }
        await createRecord(TABLES.submissions, safeFields);
      } else {
        throw airtableErr;
      }
    }

    res.redirect('/dashboard?checkin=success');
  } catch (err) {
    console.error('[checkin error]', err.message);
    res.redirect('/dashboard/checkin?error=' + encodeURIComponent('Something went wrong. Please try again.'));
  }
});

// ─── GHL HELPER ───────────────────────────────────────────────────────────────
async function createGHLContact(data) {
  if (!GHL_TOKEN || !GHL_LOCATION) {
    console.warn('[GHL] Token or Location ID not set — skipping GHL sync');
    return null;
  }
  const nameParts = (data.fullName || '').trim().split(' ');
  const firstName = nameParts[0] || '';
  const lastName  = nameParts.slice(1).join(' ') || '';
  const body = {
    locationId: GHL_LOCATION,
    firstName, lastName,
    email:  data.email  || '',
    phone:  data.phone  || '',
    tags:   ['lmt'],
    customField: {},
  };
  try {
    // Try upsert first (search by email, then create)
    const searchRes = await fetch(
      `https://services.leadconnectorhq.com/contacts/?locationId=${GHL_LOCATION}&email=${encodeURIComponent(data.email)}`,
      { headers: { Authorization: `Bearer ${GHL_TOKEN}`, Version: '2021-07-28' } }
    );
    const searchData = await searchRes.json();
    const existing = searchData?.contacts?.[0];
    if (existing) {
      // Update existing contact — add lmt tag
      const existing_tags = existing.tags || [];
      if (!existing_tags.includes('lmt')) existing_tags.push('lmt');
      await fetch(`https://services.leadconnectorhq.com/contacts/${existing.id}`, {
        method: 'PUT',
        headers: { Authorization: `Bearer ${GHL_TOKEN}`, Version: '2021-07-28', 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...body, tags: existing_tags }),
      });
      console.log('[GHL] Updated contact:', data.email);
      return existing.id;
    }
    // Create new contact
    const res = await fetch('https://services.leadconnectorhq.com/contacts/', {
      method: 'POST',
      headers: { Authorization: `Bearer ${GHL_TOKEN}`, Version: '2021-07-28', 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const created = await res.json();
    console.log('[GHL] Created contact:', data.email, '— id:', created?.contact?.id);
    return created?.contact?.id;
  } catch (err) {
    console.error('[GHL] Error:', err.message);
    return null;
  }
}

// ─── PUBLIC ONBOARDING FORM ───────────────────────────────────────────────────
function renderOnboardingForm(error = null, success = false) {
  const q = (label, input, hint = '') => `
    <div style="margin-bottom:1.5rem">
      <label style="display:block;font-size:.82rem;font-weight:700;color:var(--text);margin-bottom:.3rem;line-height:1.4">${label}</label>
      ${hint ? `<div style="font-size:.74rem;color:var(--muted);margin-bottom:.5rem;line-height:1.5;font-style:italic">${hint}</div>` : ''}
      ${input}
    </div>`;

  const text  = (name, ph='') => `<input type="text" name="${name}" placeholder="${ph}" style="width:100%;padding:.75rem 1rem;background:var(--surface2);border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:.92rem;outline:none" onfocus="this.style.borderColor='var(--gold)';this.style.boxShadow='0 0 0 3px rgba(201,168,76,.12)'" onblur="this.style.borderColor='var(--border)';this.style.boxShadow='none'">`;
  const email = (name) => `<input type="email" name="${name}" placeholder="your@email.com" required style="width:100%;padding:.75rem 1rem;background:var(--surface2);border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:.92rem;outline:none" onfocus="this.style.borderColor='var(--gold)';this.style.boxShadow='0 0 0 3px rgba(201,168,76,.12)'" onblur="this.style.borderColor='var(--border)';this.style.boxShadow='none'">`;
  const tel   = (name) => `<input type="tel" name="${name}" placeholder="+1 555 000 0000" style="width:100%;padding:.75rem 1rem;background:var(--surface2);border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:.92rem;outline:none" onfocus="this.style.borderColor='var(--gold)';this.style.boxShadow='0 0 0 3px rgba(201,168,76,.12)'" onblur="this.style.borderColor='var(--border)';this.style.boxShadow='none'">`;
  const num   = (name, ph='') => `<input type="number" name="${name}" placeholder="${ph}" style="width:100%;padding:.75rem 1rem;background:var(--surface2);border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:.92rem;outline:none" onfocus="this.style.borderColor='var(--gold)';this.style.boxShadow='0 0 0 3px rgba(201,168,76,.12)'" onblur="this.style.borderColor='var(--border)';this.style.boxShadow='none'">`;
  const ta    = (name, ph='', rows=3) => `<textarea name="${name}" rows="${rows}" placeholder="${ph}" style="width:100%;padding:.75rem 1rem;background:var(--surface2);border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:.92rem;outline:none;resize:vertical;font-family:inherit;line-height:1.6" onfocus="this.style.borderColor='var(--gold)';this.style.boxShadow='0 0 0 3px rgba(201,168,76,.12)'" onblur="this.style.borderColor='var(--border)';this.style.boxShadow='none'"></textarea>`;
  const sel   = (name, opts) => `<select name="${name}" style="width:100%;padding:.75rem 1rem;background:var(--surface2);border:1px solid var(--border);border-radius:10px;color:var(--text);font-size:.92rem;outline:none" onfocus="this.style.borderColor='var(--gold)'" onblur="this.style.borderColor='var(--border)'">${opts.map(o=>`<option value="${o}">${o}</option>`).join('')}</select>`;
  const radio = (name, opts) => `<div style="display:flex;flex-direction:column;gap:.6rem">${opts.map(o=>`<label style="display:flex;align-items:center;gap:.65rem;cursor:pointer;padding:.6rem .85rem;background:var(--surface2);border:1px solid var(--border);border-radius:8px;font-size:.88rem"><input type="radio" name="${name}" value="${o}" style="accent-color:var(--gold)"> ${o}</label>`).join('')}</div>`;

  const section = (icon, title, color, content) => `
    <div style="margin-bottom:2rem">
      <div style="background:linear-gradient(135deg,var(--surface),var(--surface2));border:1px solid var(--border);border-radius:16px;overflow:hidden;border-top:3px solid ${color}">
        <div style="padding:1.25rem 1.75rem;border-bottom:1px solid var(--border);background:${color}0d;display:flex;align-items:center;gap:.75rem">
          <span style="font-size:1.3rem">${icon}</span>
          <div style="font-size:.65rem;font-weight:700;text-transform:uppercase;letter-spacing:2px;color:${color}">${title}</div>
        </div>
        <div style="padding:1.75rem">${content}</div>
      </div>
    </div>`;

  if (success) return `<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Welcome — School for Men</title>
<style>${CSS}</style></head><body>
<header class="header">
  <div class="header-logo">
    <img src="/logo.png" alt="SFM" style="height:32px;object-fit:contain" onerror="this.style.display='none'">
    <div><div class="logo">School for Men</div><div class="logo-sub">Life Mastery Tracker</div></div>
  </div>
</header>
<div style="min-height:80vh;display:flex;align-items:center;justify-content:center;padding:2rem;background:radial-gradient(ellipse 60% 50% at 50% 0%,rgba(201,168,76,.1) 0%,transparent 60%)">
  <div style="text-align:center;max-width:520px">
    <div style="font-size:3.5rem;margin-bottom:1.5rem">🏆</div>
    <div style="font-size:.65rem;text-transform:uppercase;letter-spacing:3px;color:var(--gold);margin-bottom:.75rem;font-weight:700">Application Received</div>
    <h1 style="font-size:2rem;font-weight:900;margin-bottom:1rem;line-height:1.2">You've taken the first step.</h1>
    <p style="color:var(--muted);line-height:1.8;font-size:.95rem">Your onboarding has been submitted. Your coach will review your responses and be in touch shortly with your next steps and login details.</p>
    <div style="margin-top:2rem;padding:1.25rem;background:var(--surface);border:1px solid var(--border);border-radius:14px;font-size:.85rem;color:var(--muted);line-height:1.7">
      In the meantime — <strong style="color:var(--text)">notice what you feel right now</strong>. That decision you just made matters. Don't minimise it.
    </div>
  </div>
</div></body></html>`;

  return `<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Onboarding — School for Men</title>
<style>
${CSS}
.progress-bar{position:fixed;top:0;left:0;height:3px;background:linear-gradient(90deg,var(--gold),var(--gold-light));z-index:9999;transition:width .2s}
</style></head><body>
<div class="progress-bar" id="progressBar" style="width:0%"></div>
<header class="header">
  <div class="header-logo">
    <img src="/logo.png" alt="SFM" style="height:32px;object-fit:contain" onerror="this.style.display='none'">
    <div><div class="logo">School for Men</div><div class="logo-sub">Client Onboarding</div></div>
  </div>
  <div style="font-size:.75rem;color:var(--muted)" class="desktop-only">Confidential · Secure</div>
</header>

<div style="background:linear-gradient(160deg,var(--surface) 0%,#080510 100%);border-bottom:1px solid var(--border);padding:3rem 2rem;text-align:center;position:relative;overflow:hidden">
  <div style="position:absolute;inset:0;background:radial-gradient(ellipse 60% 70% at 50% -10%,rgba(201,168,76,.09) 0%,transparent 60%);pointer-events:none"></div>
  <div style="position:relative;max-width:680px;margin:0 auto">
    <div style="font-size:.6rem;text-transform:uppercase;letter-spacing:3px;color:var(--gold);margin-bottom:.7rem;font-weight:700">School for Men — Life Mastery Tracker</div>
    <h1 style="font-size:2.25rem;font-weight:900;margin-bottom:.85rem;line-height:1.15">This is where it starts.</h1>
    <p style="color:var(--muted);font-size:.97rem;line-height:1.75;max-width:560px;margin:0 auto">Answer every question with full honesty. There are no right answers — only real ones. Your coach will use this to build your 90-day transformation plan.</p>
    <div style="display:flex;justify-content:center;gap:2rem;margin-top:1.75rem;flex-wrap:wrap">
      <div style="text-align:center"><div style="font-size:1.3rem;font-weight:900;color:var(--gold)">~20</div><div style="font-size:.6rem;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted)">Minutes</div></div>
      <div style="text-align:center"><div style="font-size:1.3rem;font-weight:900;color:var(--gold)">100%</div><div style="font-size:.6rem;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted)">Confidential</div></div>
      <div style="text-align:center"><div style="font-size:1.3rem;font-weight:900;color:var(--gold)">45</div><div style="font-size:.6rem;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted)">Questions</div></div>
    </div>
  </div>
</div>

<main style="max-width:820px;margin:0 auto;padding:2.5rem 1rem 4rem">
  ${error ? `<div style="background:rgba(239,68,68,.1);border:1px solid rgba(239,68,68,.3);border-radius:10px;padding:.9rem 1.1rem;color:#f87171;font-size:.875rem;margin-bottom:2rem">⚠️ ${error}</div>` : ''}
  <form method="POST" action="/onboarding" id="onboardForm">

    ${section('👤','Your Details','#C9A84C',`
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem">
        <div style="grid-column:1/-1">${q('Full Name *', text('fullName','John Smith'))}</div>
        <div>${q('Email Address *', email('email'))}</div>
        <div>${q('Phone Number', tel('phone'))}</div>
        <div>${q('Date of Birth', text('dob','DD/MM/YYYY'))}</div>
        <div>${q('Business / Occupation', text('occupation','e.g. Founder, CEO, Engineer'))}</div>
        <div style="grid-column:1/-1">${q('Relationship Status', sel('relationshipStatus',['Select…','Single','Dating','In a relationship','Married','Separated','Divorced']))}</div>
      </div>
    `)}

    ${section('💰','Money & Business','#eab308',`
      ${q('How much do you currently make? (personal income + business if applicable)',
        ta('currentIncome','e.g. I earn $150k personally and my business generates $500k…',2),
        'Be as specific as you\'re comfortable with. This helps calibrate your goals.')}
      ${q('How much would you realistically like to be making?',
        ta('targetIncome','e.g. I want to be earning $300k personally within 12 months…',2),
        'Push past what feels "safe" to say. What do you actually want?')}
    `)}

    ${section('🏋️','Health Baseline','#22c55e',`
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem">
        <div>${q('Average hours of sleep per night', num('sleepHours','e.g. 6.5'))}</div>
        <div>${q('Current stress level', sel('stressLevel',['Select…','1 — Barely stressed','2 — Low stress','3 — Moderate','4 — High stress','5 — Overwhelmed']))}</div>
      </div>
      ${q('Do you exercise consistently?', radio('exerciseConsistent',['Yes','No']))}
      ${q('If yes — what do you do, how often, and how intensely?',
        ta('exerciseDetail','e.g. Weights 4x/week at moderate intensity, plus 2x cardio…',2),
        'If no, skip this.')}
      ${q('Do you wear a biomarker tracking device?',
        radio('biomarker',['Yes — Whoop','Yes — Oura','Yes — Garmin','Yes — Other','No']),
        'e.g. Whoop, Oura Ring, Garmin')}
      ${q('How would you describe your relationship with your physical body right now?',
        ta('bodyRelationship','Energy levels, pain, vitality, how you feel in your own skin…',3),
        'Be honest. Not what you wish it was — what it actually is.')}
    `)}

    ${section('❤️','Relationships & Intimacy','#ec4899',`
      ${q('How would you describe your current intimate relationship(s)?',
        ta('currentRelationship','Depth of connection, satisfaction, tension, distance, what\'s working, what isn\'t…',3),
        'This is confidential. Be real.')}
      ${q('What are the biggest challenges you face in intimacy and connection right now?',
        ta('intimacyChallenges','Physical, emotional, trust, vulnerability, desire…',3))}
      ${q('Do you feel respected and desired in your relationship?',
        radio('feltRespected',['Yes, fully','Somewhat','Not really','No']),
        'Answer honestly.')}
      ${q('Why or why not?',
        ta('feltRespectedWhy','What specifically makes you feel that way?',2))}
    `)}

    ${section('🎯','Your 90-Day Goals','#a855f7',`
      <p style="font-size:.82rem;color:var(--muted);margin-bottom:1.5rem;line-height:1.7">These become your anchors for the next 90 days. Be specific. Avoid vague answers like "be healthier" — go for measurable, identity-level shifts.</p>
      ${q('HEALTH — One specific thing to achieve in the next 90 days',
        ta('goalHealth','e.g. Lose 12kg, train 5x per week consistently, bring resting HR below 60…',2),
        'Physical, measurable, and stretching but achievable.')}
      ${q('WEALTH — One business or financial result that would make the next 90 days a success',
        ta('goalWealth','e.g. Close $50k in new contracts, launch my course, hire my first VA…',2))}
      ${q('RELATIONSHIPS — One shift you want to make in your relationships',
        ta('goalRelationships','e.g. Go from roommates to genuinely connected with my partner, rebuild trust with my son…',2))}
      ${q('PURPOSE — What does living more aligned with your purpose look like?',
        ta('goalPurpose','e.g. Launch my podcast, stop taking clients who drain me, start writing the book…',2))}
    `)}

    ${section('🔭','Your Vision','#06b6d4',`
      ${q('Where do you see yourself in 6 months if things go right?',
        ta('vision6months','Describe your life — work, health, relationships, identity. Paint the full picture.',3))}
      ${q('Where do you see yourself in 12 months if things go right?',
        ta('vision12months','Push further. What does a year of this work unlock for you?',3))}
      ${q('What do you want most from this coaching container?',
        ta('wantFromCoaching','What do you need that you\'ve never been fully given?',2))}
      ${q('What would you consider a breakthrough result from this coaching?',
        ta('breakthroughResult','The one outcome that would make you say — "that changed my life."',2))}
    `)}

    ${section('🧱','Obstacles & Patterns','#f97316',`
      ${q('What are the biggest obstacles that have kept you from the results you want — until now?',
        ta('biggestObstacles','Be specific. Not "fear" — what exact fear? Not "time" — how do you actually spend it?',3))}
      ${q('What are you most afraid this coaching will ask of you?',
        ta('fearOfCoaching','The thing you hope doesn\'t come up — that\'s usually the most important thing.',2))}
      ${q('What patterns or habits do you want to break?',
        ta('patternsToBreak','Avoidance, reactivity, overworking, drinking, porn, isolation — name them.',3))}
      ${q('What tends to trigger you emotionally most often?',
        ta('emotionalTriggers','e.g. Criticism, being ignored, feeling out of control, financial pressure…',2))}
      ${q('What is your biggest self-sabotage pattern?',
        ta('selfSabotage','The thing you do that gets in your own way most reliably.',2))}
    `)}

    ${section('🪞','The Deep Work','#c084fc',`
      <p style="font-size:.82rem;color:var(--muted);margin-bottom:1.5rem;line-height:1.7">These questions go deeper. They are the most important ones on this form. Don't rush them.</p>
      ${q('What is the harshest thing you regularly say to yourself?',
        ta('harshSelfTalk','Write the exact words you use on yourself in your worst moments.',2))}
      ${q('What are you most ashamed of?',
        ta('biggestShame','You don\'t have to have an answer immediately. Sit with this one.',2))}
      ${q('Do you have a clear sense of your purpose beyond your roles?',
        radio('clearPurpose',['Yes, clearly','Somewhat','Not really','No']),
        'Beyond being a father, partner, or professional — what is your mission?')}
      ${q('When did you last feel genuinely alive — and why?',
        ta('lastFeltAlive','Describe the moment and what it tells you about what matters to you.',2))}
      ${q('Are there emotions you\'ve never allowed yourself to fully feel?',
        ta('unfeltEmotions','Grief, rage, love, fear, joy — what has been locked down and why?',2))}
      ${q('What is the real reason you\'re here right now?',
        ta('realReason','Not the surface answer. Not what sounds good. The true one.',3),
        'Take a breath before you answer this.')}
      ${q('What did your father model about what it means to be a man?',
        ta('fatherModel','What did he demonstrate — in his presence, his absence, his behaviour?',3))}
      ${q('Growing up, what kind of man were you told — directly or indirectly — you were supposed to be?',
        ta('manToldToBe','And how much of that are you still unconsciously living out?',2))}
    `)}

    ${section('🤝','Accountability','#22c55e',`
      ${q('Why is NOW the time you\'re committing to this change?',
        ta('whyNow','What has shifted, or what can no longer be tolerated?',2))}
      ${q('How committed are you to showing up fully for this coaching process?',
        sel('commitmentLevel',['Select…','10 — I\'m all in, no excuses','9 — Very committed','8 — Committed but nervous','7 — Fairly committed','6 or below — I have reservations']))}
      ${q('When being challenged or held accountable, how do you respond best?',
        ta('accountabilityStyle','Direct challenge? Questions? Space to reflect? Tell your coach.',2))}
      ${q('What would stop you from following through — and how should your coach hold you accountable if that happens?',
        ta('accountabilityPlan','Name your patterns. Give your coach permission to call them out.',2))}
      ${q('How much do you actually respect the man you currently are?',
        ta('selfRespect','On a scale of 1–10 — and why? Be honest, not humble.',2))}
    `)}

    <!-- Commitment Declaration -->
    <div style="background:linear-gradient(135deg,rgba(201,168,76,.08),rgba(201,168,76,.03));border:1px solid rgba(201,168,76,.25);border-radius:16px;padding:2rem;margin-bottom:2rem">
      <div style="display:flex;align-items:center;gap:.75rem;margin-bottom:1.25rem">
        <span style="font-size:1.3rem">✍️</span>
        <div style="font-size:.65rem;font-weight:700;text-transform:uppercase;letter-spacing:2px;color:var(--gold)">Your Commitment Declaration</div>
      </div>
      <p style="font-size:.85rem;color:var(--muted);margin-bottom:1.25rem;line-height:1.75">In your own words — write your declaration of commitment to this process. What are you committing to? What does the man you want to become demand of you right now?</p>
      ${ta('commitmentDeclaration','Write your declaration here. First person. Present tense. Speak it into existence.',4)}
    </div>

    <!-- Submit -->
    <div style="text-align:center;padding:1rem 0 2rem">
      <p style="font-size:.85rem;color:var(--muted);margin-bottom:1.5rem;max-width:480px;margin-left:auto;margin-right:auto;line-height:1.75">By submitting this form, you are saying yes to the man you are becoming. Your coach will review your answers and reach out personally.</p>
      <button type="submit" class="btn btn-gold" style="padding:1rem 3rem;font-size:1.05rem;font-weight:800;letter-spacing:.5px">Submit My Onboarding →</button>
    </div>
  </form>
</main>
<script>
  window.addEventListener('scroll',()=>{
    const h=document.documentElement;
    document.getElementById('progressBar').style.width=(h.scrollTop/(h.scrollHeight-h.clientHeight)*100)+'%';
  });
  document.getElementById('onboardForm').addEventListener('submit',function(){
    const btn=this.querySelector('button[type=submit]');
    btn.disabled=true;btn.textContent='Submitting…';
  });
</script>
</body></html>`;
}

app.get('/onboarding', (req, res) => {
  res.send(renderOnboardingForm(req.query.error, req.query.success === '1'));
});

app.post('/onboarding', async (req, res) => {
  try {
    const b = req.body;
    if (!b.fullName || !b.email) return res.redirect('/onboarding?error=Full name and email are required.');

    // ── Save to Airtable Onboarding table ────────────────────────────────────
    const atFields = {
      'Full Name':                     b.fullName,
      'Phone':                         b.phone                  || null,
      'Email':                         b.email,
      'Date of birth':                 b.dob                    || null,
      'Business (and title) / Occupation': b.occupation         || null,
      'Relationship status':           b.relationshipStatus !== 'Select…' ? b.relationshipStatus : null,
      'How much money do you make? (and if you own a business, how much does the business make?)': b.currentIncome || null,
      'How much would you realistically like to be making? (and the business)': b.targetIncome || null,
      'How many hours of sleep do you get on average?': parseFloat(b.sleepHours) || null,
      'Rate your stress levels (1 being chill, 5 being hyper stressed)': b.stressLevel !== 'Select…' ? parseInt(b.stressLevel) || null : null,
      'Do you exercise consistently?': b.exerciseConsistent      || null,
      'If yes — what do you do, how often, and how intensely? (mild, moderate, high)': b.exerciseDetail || null,
      'Do you wear a biomarker tracking device? (Whoop, Oura, Garmin, etc.)': b.biomarker || null,
      'How would you describe your current intimate relationship(s)?': b.currentRelationship || null,
      'How would you describe your relationship with your physical body right now? (energy, pain, vitality)': b.bodyRelationship || null,
      'HEALTH — What is the one thing you want to achieve in your health in the next 90 days? Be specific.': b.goalHealth || null,
      'WEALTH — What is the one business or financial result that would make the next 90 days a success?': b.goalWealth || null,
      'RELATIONSHIPS — What is the one shift you want to make in your relationships in the next 90 days?': b.goalRelationships || null,
      'PURPOSE — What does living more aligned with your purpose look like for you in the next 90 days?': b.goalPurpose || null,
      'Where do you see yourself in 6 months if things go right?': b.vision6months || null,
      'Where do you see yourself in 12 months if things go right?': b.vision12months || null,
      'What do you want most from this coaching container?': b.wantFromCoaching || null,
      'What would you consider a breakthrough result from this coaching?': b.breakthroughResult || null,
      'What are the biggest obstacles that have kept you from creating the results you want in the past?': b.biggestObstacles || null,
      'What are the biggest challenges you face in intimacy and connection right now?': b.intimacyChallenges || null,
      'Do you feel respected and desired in your relationship?': b.feltRespected || null,
      'Why or why not?': b.feltRespectedWhy || null,
      'What is the harshest thing you regularly say to yourself?': b.harshSelfTalk || null,
      'What are you most ashamed of?': b.biggestShame || null,
      'Do you have a clear sense of your purpose beyond your roles?': b.clearPurpose || null,
      'When did you last feel genuinely alive — and why?': b.lastFeltAlive || null,
      'Are there emotions you\'ve never allowed yourself to fully feel? What were they and why couldn\'t you express them?': b.unfeltEmotions || null,
      'What is the real reason you\'re here right now?': b.realReason || null,
      'What are you most afraid this coaching will ask of you?': b.fearOfCoaching || null,
      'What patterns or habits do you want to break?': b.patternsToBreak || null,
      'What tends to trigger you emotionally most often? (e.g. anger, frustration, anxiety)': b.emotionalTriggers || null,
      'Why is NOW the time you\'re committing to this change?': b.whyNow || null,
      'How committed are you to showing up fully for this coaching process?': b.commitmentLevel !== 'Select…' ? b.commitmentLevel : null,
      'What is your biggest self-sabotage pattern — the thing you do that gets in your own way most often?': b.selfSabotage || null,
      'When being challenged or held accountable, how do you respond best?': b.accountabilityStyle || null,
      'What would stop you from following through — and how should I hold you accountable if that happens?': b.accountabilityPlan || null,
      'How much do you actually respect the man you currently are?': b.selfRespect || null,
      'What did your father model about what it means to be a man?': b.fatherModel || null,
      'Growing up, what kind of man were you told — directly or indirectly — that you were supposed to be?': b.manToldToBe || null,
      'Commitment declaration': b.commitmentDeclaration || null,
    };
    // Remove nulls
    Object.keys(atFields).forEach(k => { if (atFields[k] === null) delete atFields[k]; });
    await createRecord(TABLES.onboarding, atFields);

    // ── Sync to GHL ──────────────────────────────────────────────────────────
    await createGHLContact({ fullName: b.fullName, email: b.email, phone: b.phone });

    res.redirect('/onboarding?success=1');
  } catch (err) {
    console.error('[onboarding error]', err.message);
    res.redirect('/onboarding?error=' + encodeURIComponent('Something went wrong — please try again or contact your coach.'));
  }
});

// ─── PUBLIC TESTIMONIALS PAGE ─────────────────────────────────────────────────
function renderTestimonialsPage(testimonials) {
  const cards = testimonials.length ? testimonials.map((t,i) => {
    const text   = t.fields['Testimonial text'] || '';
    const name   = t.fields['Client Name'] || 'SFM Client';
    const date   = t.fields['Date'] ? new Date(t.fields['Date']).toLocaleDateString('en-AU',{month:'long',year:'numeric'}) : '';
    // Show full name (as per user preference)
    const displayName = name || 'SFM Client';
    // Extract the most compelling quote — prefer "WHAT HAS CHANGED" section
    let quote = text;
    if (text.includes('WHAT HAS CHANGED:')) {
      quote = text.split('WHAT HAS CHANGED:')[1]?.split('\n\n')[0]?.trim() || text;
    } else if (text.includes('BIG WIN:')) {
      quote = text.split('BIG WIN:')[1]?.split('\n\n')[0]?.trim() || text;
    }
    // Trim to readable length
    if (quote.length > 400) quote = quote.slice(0,400).trim() + '…';
    return `
      <div style="background:var(--surface);border:1px solid var(--border);border-radius:16px;padding:2rem;display:flex;flex-direction:column;gap:1rem;position:relative;overflow:hidden">
        <div style="position:absolute;top:1.5rem;right:1.5rem;font-size:3rem;opacity:.06;line-height:1">"</div>
        <div style="font-size:2rem;color:var(--gold);line-height:1;opacity:.5">"</div>
        <p style="font-size:.97rem;line-height:1.8;color:var(--text);flex:1;font-style:italic">${quote.replace(/\n/g,'<br>')}</p>
        <div style="display:flex;align-items:center;gap:.75rem;padding-top:1rem;border-top:1px solid var(--border)">
          <div style="width:38px;height:38px;border-radius:50%;background:linear-gradient(135deg,var(--gold),#8a5e1a);display:flex;align-items:center;justify-content:center;font-weight:800;font-size:.95rem;color:#000;flex-shrink:0">${displayName[0]}</div>
          <div>
            <div style="font-weight:700;font-size:.88rem">${displayName}</div>
            ${date ? `<div style="font-size:.72rem;color:var(--muted)">${date}</div>` : ''}
          </div>
        </div>
      </div>`;
  }).join('') : `
    <div style="text-align:center;padding:4rem 2rem;color:var(--muted)">
      <div style="font-size:3rem;margin-bottom:1rem">⭐</div>
      <div style="font-size:1.1rem;font-weight:600">No testimonials yet</div>
      <div style="font-size:.85rem;margin-top:.5rem">Approved testimonials will appear here.</div>
    </div>`;

  return `<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Client Results — School for Men</title>
<style>
${CSS}
.tgrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:1.5rem}
@media(max-width:640px){.tgrid{grid-template-columns:1fr}}
</style></head><body>
<header class="header">
  <div class="header-logo">
    <img src="/logo.png" alt="SFM" style="height:32px;width:auto;object-fit:contain" onerror="this.style.display='none'">
    <div><div class="logo">School for Men</div><div class="logo-sub">Client Results</div></div>
  </div>
  <a href="/login" class="btn btn-gold" style="font-size:.78rem">Start Your Journey →</a>
</header>
<div style="background:linear-gradient(160deg,var(--surface) 0%,#080510 100%);border-bottom:1px solid var(--border);padding:4rem 2rem;text-align:center;position:relative;overflow:hidden">
  <div style="position:absolute;inset:0;background:radial-gradient(ellipse 60% 60% at 50% 0%,rgba(201,168,76,.1) 0%,transparent 60%);pointer-events:none"></div>
  <div style="position:relative;max-width:640px;margin:0 auto">
    <div style="font-size:.65rem;text-transform:uppercase;letter-spacing:3px;color:var(--gold);margin-bottom:.75rem;font-weight:700">Real Men. Real Results.</div>
    <h1 style="font-size:2.5rem;font-weight:900;margin-bottom:1rem;line-height:1.1">What Happens When a Man<br>Does The Work</h1>
    <p style="color:var(--muted);font-size:1rem;line-height:1.7;max-width:520px;margin:0 auto">These are unedited reflections from men inside the School for Men coaching program. This is what transformation looks like in real life.</p>
  </div>
</div>
<main class="container" style="max-width:1200px;padding-top:3rem;padding-bottom:4rem">
  <div class="tgrid">${cards}</div>
</main>
<footer style="border-top:1px solid var(--border);padding:2rem;text-align:center;font-size:.78rem;color:var(--muted)">
  © ${new Date().getFullYear()} School for Men. All rights reserved.
  <span style="margin:0 .75rem">·</span>
  <a href="/login" style="color:var(--gold)">Client Login</a>
</footer>
</body></html>`;
}

app.get('/testimonials', async (req, res) => {
  try {
    const all = await fetchAll(TABLES.testimonials);
    const approved = all.filter(t => t.fields['Approved'] === true);
    res.send(renderTestimonialsPage(approved));
  } catch (err) {
    console.error('[testimonials page]', err.message);
    res.status(500).send('Error loading testimonials.');
  }
});

// ─── Routes ───────────────────────────────────────────────────────────────────

// Admin dashboard
app.get('/', async (req, res) => {
  try {
    const [clients, goals, allSubmissions] = await Promise.all([
      fetchAll(TABLES.clients),
      fetchAll(TABLES.goals),
      fetchAll(TABLES.submissions),
    ]);
    const goalsByClientId = buildGoalsByClient(goals, clients);

    // Build bidirectional client maps
    const clientMap    = Object.fromEntries(clients.map(c => [c.id, fld(c,'Client name')]));
    const nameToClient = {};
    for (const c of clients) {
      const n = (fld(c,'Client name')||'').toLowerCase().trim();
      if (n) nameToClient[n] = c.id;
    }

    // Enrich submissions with resolved clientId (linked field OR name text field fallback)
    function resolveSubClientId(s) {
      const linked = (s.fields['Clients']||[])[0];
      if (linked) return linked;
      // Fallback: scan all string fields for a matching client name
      for (const v of Object.values(s.fields)) {
        if (typeof v === 'string') {
          const found = nameToClient[v.toLowerCase().trim()];
          if (found) return found;
        }
      }
      return null;
    }

    const enriched = allSubmissions.map(s => ({ ...s, _clientId: resolveSubClientId(s) }));

    const pendingTestimonials = enriched
      .filter(s => s.fields['Testimonial ready?'])
      .map(s => ({ ...s, _clientName: clientMap[s._clientId] || 'Unknown' }));

    res.send(renderAdmin(clients, goalsByClientId, enriched, pendingTestimonials));
  } catch (err) {
    console.error(err);
    res.status(500).send(`<pre style="color:red;padding:2rem">${err.message}</pre>`);
  }
});

// Admin individual client view
app.get('/client/:id', async (req, res) => {
  try {
    const clientId = req.params.id;
    const [client, goals] = await Promise.all([
      fetchRecord(TABLES.clients, clientId),
      fetchAll(TABLES.goals),
    ]);
    const clientName = fld(client,'Client name') || '';
    const submissions = await fetchSubmissionsForClient(clientId, clientName);
    const onbIds = new Set(client.fields['Onboarding response']||[]);
    res.send(renderClientDashboard(client, goalsForClient(goals, clientId, onbIds), submissions, false));
  } catch (err) {
    res.status(500).send(`<pre style="color:red;padding:2rem">${err.message}</pre>`);
  }
});

// Login page
app.get('/login', (req, res) => {
  if (req.session.clientId) return res.redirect('/dashboard');
  res.send(renderLogin(req.query.error));
});

// Login POST
app.post('/login', async (req, res) => {
  const email    = (req.body.email    || '').trim().toLowerCase();
  const password = (req.body.password || '').trim();
  if (!email || !password) return res.redirect('/login?error=Please enter your email and password.');
  try {
    const clients = await fetchAll(TABLES.clients);
    const match   = clients.find(c => (c.fields['Email']||[]).some(e => (e||'').toLowerCase() === email));
    if (!match) return res.redirect('/login?error=No account found with that email.');

    const hash = fld(match,'Password Hash');
    if (!hash) return res.redirect('/login?error=Password not set yet. Contact your coach.');

    const valid = await bcrypt.compare(password, hash);
    if (!valid) return res.redirect('/login?error=Incorrect password. Try again or contact your coach.');

    req.session.clientId = match.id;
    res.redirect('/dashboard');
  } catch (err) {
    console.error(err);
    res.redirect('/login?error=Something went wrong. Please try again.');
  }
});

// Client dashboard (protected)
app.get('/dashboard', requireAuth, async (req, res) => {
  try {
    const clientId = req.session.clientId;
    const [client, goals] = await Promise.all([
      fetchRecord(TABLES.clients, clientId),
      fetchAll(TABLES.goals),
    ]);
    const clientName  = fld(client,'Client name') || '';
    const submissions = await fetchSubmissionsForClient(clientId, clientName);
    const onbIds      = new Set(client.fields['Onboarding response']||[]);
    const html        = renderClientDashboard(client, goalsForClient(goals, clientId, onbIds), submissions, true);
    // Inject success banner if redirected after check-in
    if (req.query.checkin === 'success') {
      const banner = `<div style="background:rgba(34,197,94,.12);border-bottom:1px solid rgba(34,197,94,.25);padding:.85rem 2rem;text-align:center;font-size:.875rem;color:#4ade80;font-weight:600">✅ Check-in submitted! Your progress has been recorded.</div>`;
      res.send(html.replace('<div class="hero">', banner + '<div class="hero">'));
    } else {
      res.send(html);
    }
  } catch (err) {
    res.status(500).send(`<pre style="color:red;padding:2rem">${err.message}</pre>`);
  }
});

// Logout
app.get('/logout', (req, res) => req.session.destroy(() => res.redirect('/login')));

// ─── Admin: Set Password ──────────────────────────────────────────────────────
app.get('/admin/set-password', async (req, res) => {
  const clients = await fetchAll(TABLES.clients);
  res.send(renderSetPassword(clients, req.query.msg, req.query.err));
});

app.post('/admin/set-password', async (req, res) => {
  const { pin, clientId, password, confirm } = req.body;
  if (pin !== ADMIN_PIN) return res.redirect('/admin/set-password?err=Incorrect+admin+PIN.');
  if (!clientId)         return res.redirect('/admin/set-password?err=Select+a+client.');
  if (password !== confirm) return res.redirect('/admin/set-password?err=Passwords+do+not+match.');
  if (password.length < 8)  return res.redirect('/admin/set-password?err=Password+must+be+at+least+8+characters.');
  try {
    const hash = await bcrypt.hash(password, 10);
    await patchRecord(TABLES.clients, clientId, { 'Password Hash': hash });
    const clients = await fetchAll(TABLES.clients);
    const name = fld(clients.find(c => c.id === clientId), 'Client name') || 'Client';
    res.redirect(`/admin/set-password?msg=Password+set+successfully+for+${encodeURIComponent(name)}.`);
  } catch (err) {
    res.redirect('/admin/set-password?err=Error+saving+password.+Try+again.');
  }
});

// ─── Client: Change Password ──────────────────────────────────────────────────
app.post('/dashboard/change-password', requireAuth, async (req, res) => {
  const { current, newpw } = req.body;
  try {
    const client = await fetchRecord(TABLES.clients, req.session.clientId);
    const hash   = fld(client,'Password Hash');
    if (!hash || !(await bcrypt.compare(current, hash)))
      return res.json({ ok: false, message: 'Current password is incorrect.' });
    if (newpw.length < 8)
      return res.json({ ok: false, message: 'New password must be at least 8 characters.' });
    const newHash = await bcrypt.hash(newpw, 10);
    await patchRecord(TABLES.clients, req.session.clientId, { 'Password Hash': newHash });
    res.json({ ok: true, message: 'Password updated successfully.' });
  } catch (err) {
    res.json({ ok: false, message: 'Error updating password.' });
  }
});

// ─── API: Goal Progress ───────────────────────────────────────────────────────
app.post('/api/goal/:id/progress', requireAuth, async (req, res) => {
  try {
    await patchRecord(TABLES.goals, req.params.id, { Progress: req.body.progress });
    res.json({ ok: true });
  } catch (err) {
    res.json({ ok: false });
  }
});

// ─── API: Achieve Goal ────────────────────────────────────────────────────────
app.post('/api/goal/:id/achieve', requireAuth, async (req, res) => {
  try {
    await patchRecord(TABLES.goals, req.params.id, { 'Current status': 'Achieved' });
    res.json({ ok: true });
  } catch (err) {
    res.json({ ok: false });
  }
});

// ─── API: Coach Notes ─────────────────────────────────────────────────────────
app.post('/api/client/:id/notes', async (req, res) => {
  try {
    await patchRecord(TABLES.clients, req.params.id, { Notes: req.body.notes });
    res.json({ ok: true });
  } catch (err) {
    res.json({ ok: false });
  }
});

// ─── API: Approve Testimonial ─────────────────────────────────────────────────
app.post('/api/testimonial/:submissionId/approve', async (req, res) => {
  try {
    const sub = await fetchRecord(TABLES.submissions, req.params.submissionId);
    const clientName = req.body.clientName || '';
    // Pull richest available testimonial text from the submission
    const nsField = fld(sub,'Nervous System') || '';
    const testimonialBlock = nsField.includes('---TESTIMONIALS---')
      ? nsField.split('---TESTIMONIALS---')[1].trim()
      : '';
    const richText = [
      fld(sub,'Big win') && `BIG WIN:\n${fld(sub,'Big win')}`,
      testimonialBlock && `TESTIMONIALS:\n${testimonialBlock}`,
      fld(sub,'What went well') && `WIN #2:\n${fld(sub,'What went well')}`,
      fld(sub,'What I learned') && `IDENTITY SHIFT:\n${fld(sub,'What I learned')}`,
    ].filter(Boolean).join('\n\n');

    await createRecord(TABLES.testimonials, {
      'Testimonial text': richText || fld(sub,'Big win') || '',
      'Client Name':      clientName,
      'Date':             fld(sub,'Submission date') || new Date().toISOString().slice(0,10),
      'Approved':         true,
    });
    // Mark the submission as approved so it doesn't re-appear
    await patchRecord(TABLES.submissions, req.params.submissionId, { 'Testimonial approved?': true });
    res.json({ ok: true });
  } catch (err) {
    console.error('[approve testimonial]', err.message);
    res.json({ ok: false });
  }
});

// ─── Client Check-in (coach link / no login required) ─────────────────────────
app.get('/client/:id/checkin', async (req, res) => {
  try {
    const client = await fetchRecord(TABLES.clients, req.params.id);
    const name   = fld(client, 'Client name') || 'Client';
    res.send(renderCheckinForm(name, req.query.error, `/client/${req.params.id}/checkin`));
  } catch (err) {
    res.status(500).send(`<pre style="color:red;padding:2rem">${err.message}</pre>`);
  }
});

app.post('/client/:id/checkin', async (req, res) => {
  try {
    const clientId   = req.params.id;
    const clientRec  = await fetchRecord(TABLES.clients, clientId);
    const clientName = fld(clientRec, 'Client name') || '';
    const b = req.body;

    const nsNarrative = [
      b.ns_behaviour && `BEHAVIOURAL SHIFTS:\n${b.ns_behaviour}`,
      b.ns_external  && `EXTERNAL FEEDBACK:\n${b.ns_external}`,
      b.ns_identity  && `IDENTITY SHIFT:\n${b.ns_identity}`,
      b.ns_truth     && `EMOTIONAL TRUTH:\n${b.ns_truth}`,
    ].filter(Boolean).join('\n\n');

    const relNarrative = [
      b.rel_behaviour && `BEHAVIOURAL SHIFTS:\n${b.rel_behaviour}`,
      b.rel_external  && `EXTERNAL FEEDBACK:\n${b.rel_external}`,
      b.rel_identity  && `IDENTITY SHIFT:\n${b.rel_identity}`,
    ].filter(Boolean).join('\n\n');

    const fatherhoodNarrative = [
      b.fatherhood_behaviour && `FATHERHOOD SHIFTS:\n${b.fatherhood_behaviour}`,
      b.fatherhood_external  && `CHILDREN RESPONDING:\n${b.fatherhood_external}`,
      b.fatherhood_identity  && `IDENTITY:\n${b.fatherhood_identity}`,
      b.fatherhood_truth     && `EMOTIONAL TRUTH:\n${b.fatherhood_truth}`,
    ].filter(Boolean).join('\n\n');

    const leadershipNarrative = [
      b.leadership_behaviour && `LEADERSHIP SHIFTS:\n${b.leadership_behaviour}`,
      b.leadership_external  && `EXTERNAL FEEDBACK:\n${b.leadership_external}`,
      b.leadership_truth     && `EMOTIONAL TRUTH:\n${b.leadership_truth}`,
    ].filter(Boolean).join('\n\n');

    const healthNarrative = [
      b.health_behaviour && `BEHAVIOURAL SHIFTS:\n${b.health_behaviour}`,
      b.healthNotes      && `EXTERNAL FEEDBACK:\n${b.healthNotes}`,
      b.health_identity  && `IDENTITY SHIFT:\n${b.health_identity}`,
      b.health_truth     && `EMOTIONAL TRUTH:\n${b.health_truth}`,
    ].filter(Boolean).join('\n\n');

    const testimonials = [
      b.testimonial_1 && `WHAT HAS CHANGED:\n${b.testimonial_1}`,
      b.testimonial_2 && `TO A MAN ON THE FENCE:\n${b.testimonial_2}`,
      b.testimonial_3 && `WHAT SEEMED IMPOSSIBLE:\n${b.testimonial_3}`,
    ].filter(Boolean).join('\n\n');

    const fields = {
      'Submission date':       b.submissionDate || new Date().toISOString().slice(0,10),
      'Month period':          b.monthPeriod || '',
      'Client':                clientName,
      'Ease and grace':        parseInt(b.easeAndGrace)         || 5,
      'Stress level':          parseInt(b.stressLevel)          || 5,
      'Presence':              parseInt(b.presence)             || 5,
      'Intimacy':              parseInt(b.intimacy)             || 5,
      'Communication quality': parseInt(b.communicationQuality) || 5,
      'Relationship win':      b.relationshipWin                || null,
      'Relationship notes':    [relNarrative, fatherhoodNarrative].filter(Boolean).join('\n\n---FATHERHOOD---\n\n') || null,
      'Vision clarity':        parseInt(b.visionClarity)        || 5,
      'Revenue this month':    parseFloat(b.revenueThisMonth)   || null,
      'Profit this month':     parseFloat(b.profitThisMonth)    || null,
      'Hours worked':          parseFloat(b.hoursWorked)        || null,
      'Business notes':        leadershipNarrative               || null,
      'Vision-aligned action': b.visionAlignedAction            || null,
      'Purpose blocker':       b.purposeBlocker                 || null,
      'Energy level':          parseInt(b.energyLevel)          || 5,
      'Sleep score':           parseFloat(b.sleepScore)         || null,
      'Deep sleep hrs':        parseFloat(b.deepSleepHrs)       || null,
      'HRV':                   parseFloat(b.hrv)                || null,
      'Average heart rate':    parseFloat(b.avgHeartRate)       || null,
      'Health notes':          healthNarrative                   || null,
      'Big win':               b.bigWin                         || null,
      'What went well':        b.whatWentWell                   || null,
      "What didn't go well":   b.whatDidntGoWell                || null,
      'What I learned':        [b.whatILearned, b.what_i_learned_wins].filter(Boolean).join('\n\n') || null,
      "What I'm changing":     b.whatImChanging                 || null,
      'Nervous System':        [nsNarrative, testimonials ? `\n\n---TESTIMONIALS---\n\n${testimonials}` : ''].join('') || null,
      'Pivot flagged?':        b.pivotFlagged === 'true',
      'Testimonial ready?':    b.testimonialReady === 'true',
    };

    Object.keys(fields).forEach(k => { if (fields[k] === null || fields[k] === '') delete fields[k]; });

    try {
      await createRecord(TABLES.submissions, fields);
    } catch (airtableErr) {
      if (airtableErr.message.includes('UNKNOWN_FIELD_NAME')) {
        const knownFields = ['Submission date','Month period','Client','Ease and grace','Stress level',
          'Presence','Intimacy','Communication quality','Relationship win','Relationship notes',
          'Vision clarity','Revenue this month','Profit this month','Hours worked','Business notes',
          'Vision-aligned action','Purpose blocker','Energy level','Sleep score','Deep sleep hrs',
          'HRV','Average heart rate','Health notes','Big win','What went well',"What didn't go well",
          'What I learned',"What I'm changing",'Nervous System','Pivot flagged?','Testimonial ready?'];
        const safeFields = {};
        for (const k of knownFields) { if (fields[k] !== undefined) safeFields[k] = fields[k]; }
        await createRecord(TABLES.submissions, safeFields);
      } else {
        throw airtableErr;
      }
    }

    res.redirect(`/client/${clientId}?checkin=success`);
  } catch (err) {
    console.error('[checkin error]', err.message);
    res.redirect(`/client/${req.params.id}/checkin?error=` + encodeURIComponent('Something went wrong. Please try again.'));
  }
});

// ─── Start ────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`\n  School for Men — Life Mastery Tracker`);
  console.log(`  http://localhost:${PORT}`);
  console.log(`  Client login:    http://localhost:${PORT}/login`);
  console.log(`  Set passwords:   http://localhost:${PORT}/admin/set-password\n`);
});
