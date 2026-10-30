'use strict';

const $ = (id) => document.getElementById(id);
// 支持 ?token=xxx 直接进面板（首次配置方便）；读到后立刻从地址栏抹掉，避免留在历史里。
const urlToken = new URLSearchParams(location.search).get('token');
if (urlToken) {
  localStorage.setItem('trpool_token', urlToken);
  const u = new URL(location.href);
  u.searchParams.delete('token');
  history.replaceState(null, '', u.pathname + u.search + u.hash);
}

const state = {
  token: localStorage.getItem('trpool_token') || '',
  data: null,
  logs: [],
  logsFetchedAt: 0,
  view: 'overview',
  timer: null,
  formDirty: false,
  filters: { status: 'all', key: 'all', model: 'all', text: '' },
};

const VIEW_META = {
  overview: ['总览', '实时健康度'],
  accounts: ['账号池', '号 / 状态 / 余额'],
  usage: ['用量', '账本口径'],
  capacity: ['容量规划', '并发与限流窗口'],
  models: ['模型与档位', '上游价目表'],
  config: ['配置', '调度参数与运行时'],
  logs: ['运行日志', '逐请求排查'],
};

// ---------- 基础工具 ----------

function esc(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function money(v) {
  if (v == null || isNaN(v)) return '0.00';
  if (v > 0 && v < 0.01) return v.toFixed(4);
  return Number(v).toFixed(2);
}

// 单请求均价常在 1e-5 量级：两位/四位小数都会显示成 0.0000，等于没信息
function moneyFine(v) {
  if (v == null || isNaN(v)) return '0';
  if (v === 0) return '0';
  if (v < 0.0001) return v.toFixed(7).replace(/0+$/, '');
  if (v < 0.01) return v.toFixed(5);
  return v.toFixed(2);
}

function compact(n) {
  n = Number(n) || 0;
  if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
  if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return String(n);
}

function hhmmss(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  if (isNaN(d)) return '—';
  const p = (x) => String(x).padStart(2, '0');
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

function mmddhhmm(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  if (isNaN(d)) return '—';
  const p = (x) => String(x).padStart(2, '0');
  return `${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

// 兼容 Go 的 Duration.String()，它会输出 "30m0s" / "1h30m0s" 这种多段形式。
function parseDur(s) {
  if (typeof s === 'number') return s;
  if (!s) return 0;
  const str = String(s).trim();
  if (/^\d+(\.\d+)?$/.test(str)) return parseFloat(str);
  const unit = { ns: 1e-9, us: 1e-6, 'µs': 1e-6, ms: 1e-3, s: 1, m: 60, h: 3600 };
  const re = /(\d+(?:\.\d+)?)(ns|us|µs|ms|s|m|h)/g;
  let total = 0, m, found = false;
  while ((m = re.exec(str)) !== null) {
    total += parseFloat(m[1]) * (unit[m[2]] || 1);
    found = true;
  }
  return found ? Math.round(total * 1000) / 1000 : 0;
}

function fmtDur(sec) {
  sec = Math.round(sec);
  if (sec < 60) return sec + 's';
  if (sec < 3600) return Math.floor(sec / 60) + 'm' + (sec % 60 ? (sec % 60) + 's' : '');
  return Math.floor(sec / 3600) + 'h' + Math.floor((sec % 3600) / 60) + 'm';
}

function pct(a, b) {
  if (!b) return 0;
  return Math.max(0, Math.min(100, (a / b) * 100));
}

function toast(msg, bad) {
  const t = $('toast');
  t.textContent = msg;
  t.style.background = bad ? '#b42318' : '#101828';
  t.hidden = false;
  clearTimeout(t._h);
  t._h = setTimeout(() => { t.hidden = true; }, 2600);
}

function copyText(text, okMsg) {
  navigator.clipboard?.writeText(text).then(
    () => toast(okMsg || '已复制'),
    () => toast('复制失败，请手动选中', true));
}

// ---------- API ----------

async function api(path, opts = {}) {
  const res = await fetch(path, {
    ...opts,
    headers: {
      'Authorization': 'Bearer ' + state.token,
      'Content-Type': 'application/json',
      ...(opts.headers || {}),
    },
  });
  let body = null;
  try { body = await res.json(); } catch (_) { /* 非 JSON，忽略 */ }
  if (!res.ok) {
    throw new Error((body && body.error) || ('HTTP ' + res.status));
  }
  return body;
}

// ---------- 路由 ----------

function currentViewFromHash() {
  const v = (location.hash || '').replace(/^#/, '');
  return VIEW_META[v] ? v : 'overview';
}

function route() {
  const v = currentViewFromHash();
  state.view = v;
  document.querySelectorAll('.nav-list a').forEach((a) => {
    a.classList.toggle('on', a.dataset.view === v);
  });
  document.querySelectorAll('.view').forEach((el) => {
    el.hidden = el.dataset.view !== v;
  });
  const meta = VIEW_META[v] || ['', ''];
  $('viewTitle').textContent = meta[0];
  $('viewSub').textContent = meta[1];
  $('nav').classList.remove('open');
  if (v === 'logs') loadLogs();
  if (state.data) renderActive();
}

window.addEventListener('hashchange', route);

// ---------- 登录 ----------

function showLogin(err) {
  $('loginCard').hidden = false;
  $('app').hidden = true;
  $('loginErr').textContent = err || '';
  setNavState(false, err ? '未连接' : '待登录');
  if (state.timer) { clearInterval(state.timer); state.timer = null; }
}

function showApp() {
  $('loginCard').hidden = true;
  $('app').hidden = false;
  if (!state.timer) {
    state.timer = setInterval(() => {
      if (document.visibilityState === 'visible') refresh();
    }, 5000);
  }
}

function setNavState(ok, text) {
  const p = $('navPulse');
  p.classList.toggle('bad', !ok);
  $('navState').textContent = text;
}

$('loginForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const tok = $('tokenInput').value.trim();
  if (!tok) return;
  state.token = tok;
  try {
    await refresh();
    localStorage.setItem('trpool_token', tok);
    showApp();
    route();
  } catch (err) {
    localStorage.removeItem('trpool_token');
    state.token = '';
    showLogin('令牌无效：' + err.message);
  }
});

$('btnLogout').addEventListener('click', () => {
  localStorage.removeItem('trpool_token');
  state.token = '';
  location.reload();
});

$('navToggle').addEventListener('click', () => $('nav').classList.toggle('open'));

// ---------- 渲染：通用 ----------

const STATE_TEXT = {
  active: '可用', cooling: '冷却中', invalid: '密钥失效',
  exhausted: '额度耗尽', disabled: '已停用',
};

function stateBadge(k) {
  let text = STATE_TEXT[k.state] || k.state;
  if (k.state === 'cooling' && k.cooldown_left > 0) text += ' ' + fmtDur(k.cooldown_left);
  if (!k.enabled && k.state !== 'disabled') text = '已停用';
  const cls = !k.enabled ? 'disabled' : (k.state || 'active');
  return `<span class="badge ${esc(cls)}"><i></i>${esc(text)}</span>`;
}

function balanceCell(k) {
  if (k.budget <= 0) {
    return `<div class="balance unknown"><div class="balance-top"><span>未知</span><span>未设充值额</span></div>
      <div class="bar"><span style="width:0%"></span></div></div>`;
  }
  const p = pct(k.balance, k.budget);
  const cls = p < 10 ? 'crit' : (p < 30 ? 'low' : '');
  return `<div class="balance">
    <div class="balance-top"><span>¥${money(k.balance)} <small class="muted">/ ¥${money(k.budget)}</small></span><span>${p.toFixed(0)}%</span></div>
    <div class="bar"><span class="${cls}" style="width:${p.toFixed(1)}%"></span></div>
  </div>`;
}

function renderActive() {
  switch (state.view) {
    case 'overview': renderOverview(); break;
    case 'accounts': renderAccounts(); break;
    case 'usage': renderUsage(); break;
    case 'capacity': renderCapacity(); break;
    case 'models': renderModels(); break;
    case 'config': renderConfig(); break;
    case 'logs': renderLogsTable(); break;
  }
}

function renderChrome(d) {
  $('upstreamTag') && ($('upstreamTag').textContent = d.upstream || '');
  $('uptime').textContent = '运行 ' + fmtDur(d.uptime_seconds || 0);
  $('navVer').textContent = (d.version || 'dev') + ' · ' + (d.upstream || '').replace(/^https?:\/\//, '');
  setNavState(true, '已连接');
}

// ---------- 渲染：总览 ----------

function renderOverview() {
  const d = state.data;
  const t = d.totals || {};
  $('kHealthy').textContent = t.healthy ?? 0;
  $('kTotal').textContent = (d.keys || []).length;
  $('kRequests').textContent = compact(t.requests || 0);
  // 用账本口径（持久化）而不是进程内计数器：后者一重启就归零，看着像"没有流量"。
  const okN = t.requests || 0, errN = t.errors || 0, allN = okN + errN;
  $('kSuccess').textContent = allN
    ? `成功率 ${((okN / allN) * 100).toFixed(1)}% · 失败 ${errN}`
    : '成功率 —';
  $('kSpent').textContent = money(t.spent || 0);
  $('kBudget').textContent = t.budget > 0 ? `充值池 ¥${money(t.budget)}` : '充值池 未设置';

  const counts = {};
  (d.keys || []).forEach((k) => { counts[k.state] = (counts[k.state] || 0) + 1; });
  $('kStates').textContent = Object.keys(counts).length
    ? Object.entries(counts).map(([k, v]) => `${STATE_TEXT[k] || k} ${v}`).join(' · ')
    : '还没有号';

  const today = (d.days && d.days[0]) ? d.days[0] : null;
  $('kTodayCost').textContent = money(today ? today.cost : 0);
  $('kTodayReq').textContent = today ? `${today.requests} 次请求` : '暂无数据';

  renderAccess(d);
  renderBatchKv(d);
  renderErrFeed(d);
}

function renderAccess(d) {
  const el = $('accessBox');
  const keys = d.client_keys || [];
  el.innerHTML = `
    <div class="access-item"><span class="muted">Base URL</span><code>${esc(location.origin)}/v1</code></div>
    <div class="access-item"><span class="muted">客户端密钥</span><code>${esc(keys[0] || '未配置')}</code>
      <button class="icon-btn" id="btnCopyKey">复制</button></div>`;
  const btn = $('btnCopyKey');
  if (btn) btn.onclick = () => copyText(keys[0] || '');
}

function renderBatchKv(d) {
  const b = d.batch || {};
  const p = d.pool || {};
  const rows = [
    ['batch 模式', b.mode || (p.batch && p.batch.mode) || 'off'],
    ['batch 用量', `${b.size ?? 0} 个号 / 目标 ${b.target ?? 0}`],
    ['活跃会话', String(b.sessions ?? 0)],
    ['在途请求', String(b.total_inflight ?? 0)],
    ['粘性任务', String(b.sticky_tasks ?? 0)],
    ['策略', `${p.strategy || '—'} · topK ${p.top_k ?? '—'} · 单号上限 ${p.max_in_flight ?? '—'}`],
  ];
  $('batchKv').innerHTML = rows.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${esc(v)}</dd>`).join('');
  $('batchHint').textContent = b.mode === 'off' ? '全池平分' : '按并发放大';
}

function renderErrFeed(d) {
  const scanned = (d.logs || []).length;
  const bad = (d.logs || []).filter((l) => l.error || (l.status >= 400)).slice(0, 8);
  if (!bad.length) {
    // 说清窗口：累计失败数可能是历史账本里的，日志环只覆盖最近若干条
    $('errFeed').innerHTML = `<div class="empty">最近 ${scanned} 条请求里没有失败 👍</div>`;
    return;
  }
  $('errFeed').innerHTML = bad.map((l) => `
    <div class="feed-item">
      <div class="top">
        <span class="chip bad">${l.status || 'ERR'}</span>
        <strong>${esc(l.key_name || '—')}</strong>
        <code class="muted">${esc(l.model || '—')}</code>
        <span class="muted" style="margin-left:auto">${hhmmss(l.time)}</span>
      </div>
      <div class="msg" title="${esc(l.error || '')}">${esc(l.error || '（无备注）')}</div>
    </div>`).join('');
}

// ---------- 渲染：账号池 ----------

function renderAccounts() {
  const keys = state.data.keys || [];
  const tb = $('keyTable').querySelector('tbody');
  if (!keys.length) {
    tb.innerHTML = `<tr><td colspan="8"><div class="empty">池子还是空的 —— 点右上角「添加号」，把 sk_tr_ 密钥加进来。</div></td></tr>`;
    return;
  }
  tb.innerHTML = keys.map((k) => {
    const errLine = k.last_error
      ? `<div class="err-line" title="${esc(k.last_error)}">${esc(k.last_error.slice(0, 30))}${k.last_error.length > 30 ? '…' : ''}</div>`
      : '';
    const pills = [
      k.in_batch ? '<span class="pill on" title="当前 batch 成员">B</span>' : '',
      k.sessions > 0 ? `<span class="pill sess" title="${k.sessions} 个会话粘在此号">${k.sessions}</span>` : '',
    ].join('');
    return `<tr data-id="${esc(k.id)}">
      <td><div class="name-cell">${esc(k.name)}${pills}</div><div class="mask">${esc(k.masked)}</div>${errLine}</td>
      <td>${stateBadge(k)}</td>
      <td>${balanceCell(k)}</td>
      <td class="num">${compact(k.requests)}</td>
      <td class="num">${k.errors > 0 ? '<span style="color:var(--danger)">' + compact(k.errors) + '</span>' : '0'}</td>
      <td class="num" title="输入 ${compact(k.prompt_tokens)} / 输出 ${compact(k.completion_tokens)}">${compact(k.prompt_tokens + k.completion_tokens)}</td>
      <td class="num">${(k.weight_now || k.weight || 1).toFixed(2)}</td>
      <td class="right"><div class="ops">
        <button class="icon-btn" data-act="probe">探活</button>
        <button class="icon-btn" data-act="reset">重置</button>
        <button class="icon-btn" data-act="edit">编辑</button>
        <button class="icon-btn" data-act="toggle">${k.enabled ? '停用' : '启用'}</button>
        <button class="icon-btn danger" data-act="del">删除</button>
      </div></td>
    </tr>`;
  }).join('');
}

// ---------- 渲染：用量 ----------

function renderUsage() {
  const d = state.data;
  const days = d.days || [];
  const sum = days.reduce((a, x) => {
    a.cost += x.cost || 0; a.req += x.requests || 0;
    a.prompt += x.prompt_tokens || 0; a.completion += x.completion_tokens || 0;
    return a;
  }, { cost: 0, req: 0, prompt: 0, completion: 0 });
  const today = days[0] || null;

  $('uTodayCost').textContent = money(today ? today.cost : 0);
  $('uTodayReq').textContent = today ? `${today.requests} 次 · 失败 ${today.errors || 0}` : '暂无数据';
  $('u14Cost').textContent = money(sum.cost);
  $('u14Req').textContent = `${sum.req} 次请求`;
  $('uAvg').textContent = sum.req ? moneyFine(sum.cost / sum.req) : '0';
  $('uTokens').textContent = compact(sum.prompt + sum.completion);
  $('uTokenMix').textContent = `输入 ${compact(sum.prompt)} / 输出 ${compact(sum.completion)}`;

  const maxCost = Math.max(0.0001, ...days.map((x) => x.cost || 0));
  const tb = $('dayTable').querySelector('tbody');
  if (!days.length) {
    tb.innerHTML = `<tr><td colspan="7"><div class="empty">还没有账本记录。</div></td></tr>`;
  } else {
    tb.innerHTML = days.map((x) => {
      const w = pct(x.cost || 0, maxCost);
      return `<tr>
        <td class="mono">${esc(x.date)}</td>
        <td class="num">${x.requests || 0}</td>
        <td class="num">${(x.errors || 0) > 0 ? '<span style="color:var(--danger)">' + x.errors + '</span>' : '0'}</td>
        <td class="num">${compact(x.prompt_tokens || 0)}</td>
        <td class="num">${compact(x.completion_tokens || 0)}</td>
        <td class="num">¥${money(x.cost || 0)}</td>
        <td><div class="bar"><span class="blue" style="width:${w.toFixed(1)}%"></span></div></td>
      </tr>`;
    }).join('');
  }

  // 号 · 消耗构成（累计口径，来自号池账本）
  const keys = (d.keys || []).slice().sort((a, b) => (b.spent || 0) - (a.spent || 0));
  const maxSpent = Math.max(0.0001, ...keys.map((k) => k.spent || 0));
  const totalSpent = keys.reduce((a, k) => a + (k.spent || 0), 0);
  $('keyShare').innerHTML = keys.length ? keys.map((k) => {
    const share = totalSpent ? ((k.spent || 0) / totalSpent) * 100 : 0;
    return `<div class="feed-row">
      <span class="name" title="${esc(k.name)}">${esc(k.name)}</span>
      <div class="bar"><span class="blue" style="width:${pct(k.spent || 0, maxSpent).toFixed(1)}%"></span></div>
      <span class="val">¥${money(k.spent || 0)} · ${share.toFixed(1)}%</span>
    </div>`;
  }).join('') : '<div class="empty">暂无数据</div>';

  // 模型 · 近期成本（请求环聚合，明确标注窗口）
  const logs = d.logs || [];
  const byModel = {};
  logs.forEach((l) => {
    if (!l.model) return;
    const m = byModel[l.model] || (byModel[l.model] = { cost: 0, req: 0, tokens: 0 });
    m.cost += l.cost || 0;
    m.req += 1;
    m.tokens += (l.prompt_tokens || 0) + (l.completion_tokens || 0);
  });
  const list = Object.entries(byModel).sort((a, b) => b[1].cost - a[1].cost).slice(0, 8);
  const maxM = Math.max(0.0001, ...list.map(([, v]) => v.cost));
  $('modelShare').innerHTML = list.length ? list.map(([name, v]) => `
    <div class="feed-row">
      <span class="name" title="${esc(name)}"><code>${esc(name)}</code></span>
      <div class="bar"><span class="blue" style="width:${pct(v.cost, maxM).toFixed(1)}%"></span></div>
      <span class="val">¥${money(v.cost)} · ${v.req} 次</span>
    </div>`).join('') : '<div class="empty">暂无数据</div>';
  $('modelShareHint').textContent = `最近 ${logs.length} 条请求`;
}

// ---------- 渲染：容量规划 ----------

function latencyStats(entries) {
  const arr = entries.filter((l) => l.latency_ms > 0 && l.status >= 200 && l.status < 400)
    .map((l) => l.latency_ms).sort((a, b) => a - b);
  if (!arr.length) return { p50: 0, p95: 0, n: 0 };
  const at = (q) => arr[Math.min(arr.length - 1, Math.floor(q * arr.length))];
  return { p50: at(0.5), p95: at(0.95), n: arr.length };
}

function renderCapacity() {
  const d = state.data;
  const keys = d.keys || [];
  const pool = d.pool || {};
  const okKeys = keys.filter((k) => k.enabled && (k.state === 'active' || k.state === 'cooling'));
  const logs = d.logs || [];
  const st = latencyStats(logs);

  // 上游实测：每号 30 次 / 约 15 秒 ≈ 2 QPS
  const QPS_PER_KEY = 2;
  const safePerKey = st.p50 ? Math.max(1, Math.round(QPS_PER_KEY * (st.p50 / 1000))) : 0;

  $('cKeys').textContent = okKeys.length;
  $('cKeysSub').textContent = `共 ${keys.length} 个号 · 上限 ${pool.max_in_flight ?? '—'}/号`;
  $('cRpm').textContent = okKeys.length * QPS_PER_KEY * 60;
  $('cInflightSafe').textContent = safePerKey ? safePerKey * Math.max(1, okKeys.length) : '—';
  $('cInflightNow').textContent = `当前在途 ${(d.batch || {}).total_inflight ?? 0}`;
  $('cP95').textContent = st.p95 ? st.p95 + 'ms' : '—';
  $('cP50').textContent = st.p50 ? `p50 ${st.p50}ms · 样本 ${st.n}` : `p50 — · 样本 ${st.n}`;
  $('cSafePerKey').textContent = safePerKey ? String(safePerKey) : '（样本不足）';

  const tb = $('capTable').querySelector('tbody');
  if (!keys.length) {
    tb.innerHTML = `<tr><td colspan="9"><div class="empty">还没有号。</div></td></tr>`;
  } else {
    tb.innerHTML = keys.map((k) => {
      const mine = logs.filter((l) => l.key_id === k.id);
      const s = latencyStats(mine);
      const cap = pool.max_in_flight || 0;
      const w = cap ? pct(k.inflight || 0, cap) : 0;
      const cls = w >= 100 ? 'crit' : (w >= 66 ? 'low' : '');
      return `<tr>
        <td><div class="name-cell">${esc(k.name)}</div><div class="mask">${esc(k.masked)}</div></td>
        <td>${stateBadge(k)}</td>
        <td class="num">
          <div class="balance-top"><span>${k.inflight || 0}${cap ? ' / ' + cap : ''}</span></div>
          <div class="bar"><span class="${cls}" style="width:${w.toFixed(1)}%"></span></div>
        </td>
        <td class="num">${k.sessions || 0}</td>
        <td class="num">${compact(k.requests || 0)}</td>
        <td class="num">${k.errors > 0 ? '<span style="color:var(--danger)">' + k.errors + '</span>' : '0'}</td>
        <td class="num">${s.p50 ? s.p50 + 'ms' : '—'}</td>
        <td class="num">${k.cooldown_left > 0 ? fmtDur(k.cooldown_left) : '—'}</td>
        <td class="muted">${k.last_ok_at ? mmddhhmm(k.last_ok_at) : '—'}</td>
      </tr>`;
    }).join('');
  }
  $('capHint').textContent = `延迟样本来自最近 ${st.n} 次成功请求`;
}

// ---------- 渲染：模型与档位 ----------

function renderModels() {
  const d = state.data;
  const list = (d.prices && d.prices.list) || [];
  const q = ($('modelSearch').value || '').trim().toLowerCase();
  const rows = list.filter((p) => !q || p.id.toLowerCase().includes(q));
  const tb = $('priceTable').querySelector('tbody');
  if (!rows.length) {
    tb.innerHTML = `<tr><td colspan="6"><div class="empty">${list.length ? '没有匹配的模型' : '价目表还没拉到 —— 点「刷新价目表」。'}</div></td></tr>`;
  } else {
    tb.innerHTML = rows.map((p) => {
      const paid = (p.input > 0 || p.output > 0);
      const ratio = p.input > 0 ? (p.output / p.input).toFixed(2) + '×' : '—';
      return `<tr>
        <td><code>${esc(p.id)}</code></td>
        <td class="num">${p.input ? p.input.toFixed(3) : '0'}</td>
        <td class="num">${p.output ? p.output.toFixed(3) : '0'}</td>
        <td class="num">${p.cache_read ? p.cache_read.toFixed(3) : '—'}</td>
        <td class="num">${ratio}</td>
        <td class="muted">${paid ? '按单价' : '未定价 · 保守计费'}</td>
      </tr>`;
    }).join('');
  }
  $('priceHint').textContent = `价目表：${(d.prices && d.prices.count) || 0} 个模型 · 更新于 ${mmddhhmm(d.prices && d.prices.updated_at)}`;
}

// ---------- 渲染：配置 ----------

function renderConfig() {
  const d = state.data;
  const p = d.pool || {};
  if (!$('poolForm').dataset.dirty) fillPoolForm(p);

  const rows = [
    ['版本', `<code>${esc(d.version || 'dev')}</code>`],
    ['监听', `<code>${esc(d.listen || '—')}</code>`],
    ['上游', `<code>${esc(d.upstream || '—')}</code>`],
    ['数据目录', `<code>${esc(d.data_dir || '—')}</code>`],
    ['运行时长', fmtDur(d.uptime_seconds || 0)],
    ['自动探活', d.auto_probe ? `${d.auto_probe.enabled ? '开' : '关'} · 每 ${fmtDur(parseDur(d.auto_probe.interval))}` : '—'],
    ['日志环', d.logging ? `${d.logging.ring_size} 条${d.logging.log_bodies ? ' · 记 body' : ''}` : '—'],
    ['价目表', `${(d.prices && d.prices.count) || 0} 个 · ${mmddhhmm(d.prices && d.prices.updated_at)}`],
  ];
  $('runtimeKv').innerHTML = rows.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${v}</dd>`).join('');

  const keys = d.client_keys || [];
  $('clientKeys').innerHTML = keys.length ? keys.map((k, i) => `
    <div class="feed-row">
      <span class="name"><code>${esc(k)}</code></span>
      <span class="val"><button class="icon-btn" data-copykey="${i}">复制</button></span>
    </div>`).join('') : '<div class="empty">未配置客户端密钥（TRPOOL_CLIENT_KEYS）。</div>';
  $('clientKeys').querySelectorAll('[data-copykey]').forEach((b) => {
    b.onclick = () => copyText(keys[Number(b.dataset.copykey)] || '');
  });
}

function fillPoolForm(p) {
  $('pStrategy').value = p.strategy || 'weighted_random';
  $('pTopK').value = p.top_k ?? 5;
  $('pMaxInflight').value = p.max_in_flight ?? 0;
  $('pRetry').value = p.retry_max ?? 3;
  $('pFloor').value = p.credit_floor ?? 0;
  $('pIdle').value = p.idle_weight_per_hour ?? 0;
  $('pRate').value = Math.round(parseDur(p.rate_cooldown));
  $('pBreaker').value = Math.round(parseDur(p.breaker_cooldown));
  $('pInflight').value = p.inflight_penalty ?? 3;
  const ss = p.session_sticky || {};
  $('pSticky').value = ss.enabled ? '1' : '0';
  $('pStickyTtl').value = Math.max(1, Math.round(parseDur(ss.ttl) / 60) || 30);
  const bt = p.batch || {};
  $('pBatchMode').value = bt.mode || 'auto';
  $('pBatchSize').value = bt.size ?? 3;
  $('pBatchPer').value = bt.target_inflight_per_key ?? 4;
}

// 手动改过表单就别再被 5 秒轮询覆盖回去（否则输入到一半会跳值）
$('poolForm').addEventListener('input', () => {
  $('poolForm').dataset.dirty = '1';
  state.formDirty = true;
});

// ---------- 渲染：运行日志 ----------

async function loadLogs() {
  try {
    const r = await api('/panel/api/logs?n=400');
    state.logs = r.logs || [];
    state.logsFetchedAt = Date.now();
    if (state.view === 'logs') renderLogsTable();
  } catch (err) {
    toast(err.message, true);
  }
}

function logMatches(l) {
  const f = state.filters;
  const bad = l.error || l.status >= 400;
  if (f.status === 'bad' && !bad) return false;
  if (f.status === 'ok' && bad) return false;
  if (f.key !== 'all' && l.key_id !== f.key) return false;
  if (f.model !== 'all' && l.model !== f.model) return false;
  if (f.text) {
    const t = f.text.toLowerCase();
    const hay = `${l.trace_id || ''} ${l.error || ''} ${l.model || ''} ${l.key_name || ''}`.toLowerCase();
    if (!hay.includes(t)) return false;
  }
  return true;
}

function syncLogFilters() {
  const keys = state.data.keys || [];
  const models = [...new Set(state.logs.map((l) => l.model).filter(Boolean))].sort();

  const kSel = $('fKey');
  const kwant = ['all'].concat(keys.map((k) => k.id)).join(',');
  if (kSel.dataset.sig !== kwant) {
    kSel.dataset.sig = kwant;
    kSel.innerHTML = `<option value="all">全部号</option>` +
      keys.map((k) => `<option value="${esc(k.id)}">${esc(k.name)}</option>`).join('');
    kSel.value = state.filters.key;
  }

  const mSel = $('fModel');
  const msig = ['all'].concat(models).join(',');
  if (mSel.dataset.sig !== msig) {
    mSel.dataset.sig = msig;
    mSel.innerHTML = `<option value="all">全部模型</option>` +
      models.map((m) => `<option value="${esc(m)}">${esc(m)}</option>`).join('');
    mSel.value = state.filters.model;
  }
}

function renderLogsTable() {
  const logs = state.logs;
  syncLogFilters();
  const rows = logs.filter(logMatches);
  const tb = $('logTable').querySelector('tbody');
  if (!rows.length) {
    tb.innerHTML = `<tr><td colspan="10"><div class="empty">${logs.length ? '没有匹配的请求' : '还没有请求。把客户端的 base_url 指到本网关试试。'}</div></td></tr>`;
  } else {
    tb.innerHTML = rows.map((l) => {
      const ok = l.status > 0 && l.status < 400;
      const note = l.error ? l.error : (l.attempt > 1 ? `第 ${l.attempt} 次尝试成功` : '');
      return `<tr>
        <td class="muted mono">${mmddhhmm(l.time)}</td>
        <td>${esc(l.key_name || '—')}</td>
        <td><code>${esc(l.model || '—')}</code></td>
        <td class="${ok ? 'ok' : 'bad'}">${l.status || '—'}</td>
        <td class="num">${l.latency_ms}ms</td>
        <td class="num">${compact((l.prompt_tokens || 0) + (l.completion_tokens || 0))}</td>
        <td class="num">¥${money(l.cost)}</td>
        <td class="num">${l.attempt || 1}</td>
        <td class="note" title="${esc(note)}">${esc(note)}${l.trace_id ? ` <span class="chip">${esc(l.trace_id.slice(-8))}</span>` : ''}</td>
        <td>${l.trace_id ? `<button class="icon-btn" data-trace="${esc(l.trace_id)}">trace</button>` : ''}</td>
      </tr>`;
    }).join('');
  }
  $('logHint').textContent = `最近 ${logs.length} 条 · 匹配 ${rows.length} 条 · 环形缓冲上限 ${(state.data.logging && state.data.logging.ring_size) || '—'}`;
}

$('logTable').addEventListener('click', (e) => {
  const b = e.target.closest('[data-trace]');
  if (b) copyText(b.dataset.trace, '已复制 trace_id');
});

$('fStatus').addEventListener('change', () => { state.filters.status = $('fStatus').value; renderLogsTable(); });
$('fKey').addEventListener('change', () => { state.filters.key = $('fKey').value; renderLogsTable(); });
$('fModel').addEventListener('change', () => { state.filters.model = $('fModel').value; renderLogsTable(); });
$('fText').addEventListener('input', () => { state.filters.text = $('fText').value.trim(); renderLogsTable(); });

// ---------- 操作 ----------

async function refresh() {
  const d = await api('/panel/api/state');
  state.data = d;
  renderChrome(d);
  renderActive();
  if (state.view === 'logs' || !state.logs.length) loadLogs();
  return d;
}

$('btnRefresh').addEventListener('click', () => refresh().then(() => { if (state.view === 'logs') loadLogs(); toast('已刷新'); }).catch((e) => toast(e.message, true)));

$('btnAddToggle').addEventListener('click', () => {
  const f = $('addForm');
  f.hidden = !f.hidden;
  if (!f.hidden) $('addName').focus();
});

$('addForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  $('addErr').textContent = '';
  const payload = {
    name: $('addName').value.trim(),
    secret: $('addSecret').value.trim(),
    budget: parseFloat($('addBudget').value) || 0,
  };
  try {
    await api('/panel/api/keys', { method: 'POST', body: JSON.stringify(payload) });
    $('addForm').reset();
    $('addForm').hidden = true;
    await refresh();
    toast('已加入池');
  } catch (err) {
    $('addErr').textContent = err.message;
  }
});

$('keyTable').addEventListener('click', async (e) => {
  const btn = e.target.closest('button[data-act]');
  if (!btn) return;
  const tr = btn.closest('tr');
  const id = tr?.dataset.id;
  if (!id) return;
  const k = (state.data?.keys || []).find((x) => x.id === id);
  if (!k) return;
  const act = btn.dataset.act;

  try {
    if (act === 'probe') {
      btn.disabled = true; btn.textContent = '探测中';
      const r = await api(`/panel/api/keys/${id}/probe`, { method: 'POST' });
      toast(r.ok ? `探活成功 ${r.latency_ms}ms` : '探活失败：' + r.error, !r.ok);
    } else if (act === 'reset') {
      await api(`/panel/api/keys/${id}/reset`, { method: 'POST' });
      toast('已重置为可用');
    } else if (act === 'toggle') {
      await api(`/panel/api/keys/${id}`, { method: 'PATCH', body: JSON.stringify({ enabled: !k.enabled }) });
      toast(k.enabled ? '已停用' : '已启用');
    } else if (act === 'del') {
      if (!confirm(`确认从池中删除「${k.name}」？该操作只影响网关，不动上游密钥。`)) return;
      await api(`/panel/api/keys/${id}`, { method: 'DELETE' });
      toast('已删除');
    } else if (act === 'edit') {
      const name = prompt('名称', k.name);
      if (name === null) return;
      const budgetRaw = prompt('充值额（¥，填 0 表示不跟踪余额）', String(k.budget));
      if (budgetRaw === null) return;
      const weightRaw = prompt('手动权重（默认 1）', String(k.weight));
      if (weightRaw === null) return;
      await api(`/panel/api/keys/${id}`, {
        method: 'PATCH',
        body: JSON.stringify({
          name,
          budget: parseFloat(budgetRaw) || 0,
          weight: parseFloat(weightRaw) || 1,
        }),
      });
      toast('已保存');
    }
    await refresh();
  } catch (err) {
    toast(err.message, true);
  } finally {
    btn.disabled = false;
    if (act === 'probe') btn.textContent = '探活';
  }
});

$('btnSavePool').addEventListener('click', async () => {
  // 必须以服务端现有配置为底再覆盖：只发改动的字段，其余会被后端当成零值写回去。
  const base = (state.data && state.data.pool) || {};
  const payload = {
    ...base,
    strategy: $('pStrategy').value,
    top_k: parseInt($('pTopK').value, 10) || 5,
    max_in_flight: parseInt($('pMaxInflight').value, 10) || 0,
    retry_max: parseInt($('pRetry').value, 10) || 1,
    credit_floor: parseFloat($('pFloor').value) || 0,
    idle_weight_per_hour: parseFloat($('pIdle').value) || 0,
    rate_cooldown: Math.round(parseFloat($('pRate').value) || 20) + 's',
    breaker_cooldown: Math.round(parseFloat($('pBreaker').value) || 30) + 's',
    inflight_penalty: parseFloat($('pInflight').value) || 3,
    session_sticky: {
      ...(base.session_sticky || {}),
      enabled: $('pSticky').value === '1',
      ttl: Math.max(1, parseInt($('pStickyTtl').value, 10) || 30) + 'm',
    },
    batch: {
      ...(base.batch || {}),
      mode: $('pBatchMode').value,
      size: parseInt($('pBatchSize').value, 10) || 3,
      target_inflight_per_key: parseInt($('pBatchPer').value, 10) || 4,
    },
  };
  try {
    await api('/panel/api/pool', { method: 'PUT', body: JSON.stringify(payload) });
    $('poolForm').dataset.dirty = '';
    state.formDirty = false;
    await refresh();
    toast('策略已生效，并已写回 config.json');
  } catch (err) {
    toast(err.message, true);
  }
});

$('btnPrices').addEventListener('click', async () => {
  try {
    const r = await api('/panel/api/prices', { method: 'POST' });
    await refresh();
    toast(`价目表已刷新：${r.count} 个模型`);
  } catch (err) {
    toast(err.message, true);
  }
});

$('modelSearch').addEventListener('input', () => renderModels());

$('btnProbe').addEventListener('click', async () => {
  const btn = $('btnProbe');
  btn.disabled = true; btn.textContent = '探测中…';
  try {
    const r = await api('/panel/api/keys/probe', { method: 'POST' });
    const bad = (r.results || []).filter((x) => !x.ok);
    await refresh();
    toast(bad.length ? `${bad.length} 个号异常` : '全部探活通过', bad.length > 0);
  } catch (err) {
    toast(err.message, true);
  } finally {
    btn.disabled = false; btn.textContent = '全部探活';
  }
});

// ---------- 启动 ----------

(async function boot() {
  if (!state.token) { showLogin(); return; }
  try {
    await refresh();
    showApp();
    route();
  } catch (err) {
    localStorage.removeItem('trpool_token');
    state.token = '';
    showLogin('登录态失效：' + err.message);
  }
})();
