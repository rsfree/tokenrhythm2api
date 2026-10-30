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

const state = { token: localStorage.getItem('trpool_token') || '', data: null, timer: null };

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

function toast(msg, bad) {
  const t = $('toast');
  t.textContent = msg;
  t.style.background = bad ? '#b42318' : '#101828';
  t.hidden = false;
  clearTimeout(t._h);
  t._h = setTimeout(() => { t.hidden = true; }, 2600);
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

// ---------- 登录 ----------

function showLogin(err) {
  $('loginCard').hidden = false;
  $('app').hidden = true;
  $('loginErr').textContent = err || '';
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

$('loginForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const tok = $('tokenInput').value.trim();
  if (!tok) return;
  state.token = tok;
  try {
    await refresh();
    localStorage.setItem('trpool_token', tok);
    showApp();
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

// ---------- 渲染 ----------

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
  const pct = Math.max(0, Math.min(100, (k.balance / k.budget) * 100));
  const cls = pct < 10 ? 'crit' : (pct < 30 ? 'low' : '');
  return `<div class="balance">
    <div class="balance-top"><span>¥${money(k.balance)} <small class="muted">/ ¥${money(k.budget)}</small></span><span>${pct.toFixed(0)}%</span></div>
    <div class="bar"><span class="${cls}" style="width:${pct.toFixed(1)}%"></span></div>
  </div>`;
}

function renderKeys(keys) {
  const tb = $('keyTable').querySelector('tbody');
  if (!keys || !keys.length) {
    tb.innerHTML = `<tr><td colspan="8"><div class="empty">池子还是空的 —— 点右上角「添加号」，把 sk_tr_ 密钥加进来。</div></td></tr>`;
    return;
  }
  tb.innerHTML = keys.map((k) => {
    const errLine = k.last_error
      ? `<div class="err-line" title="${esc(k.last_error)}">${esc(k.last_error.slice(0, 30))}${k.last_error.length > 30 ? '…' : ''}</div>`
      : '';
    const toggleText = k.enabled ? '停用' : '启用';
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
        <button class="icon-btn" data-act="toggle">${toggleText}</button>
        <button class="icon-btn danger" data-act="del">删除</button>
      </div></td>
    </tr>`;
  }).join('');
}

function renderLogs(logs) {
  const tb = $('logTable').querySelector('tbody');
  if (!logs || !logs.length) {
    tb.innerHTML = `<tr><td colspan="8"><div class="empty">还没有请求。把客户端的 base_url 指到本网关试试。</div></td></tr>`;
    return;
  }
  tb.innerHTML = logs.map((l) => {
    const ok = l.status > 0 && l.status < 400;
    const note = l.error ? l.error : (l.attempt > 1 ? `第 ${l.attempt} 次换号后成功` : '');
    return `<tr>
      <td class="muted">${hhmmss(l.time)}</td>
      <td>${esc(l.key_name || '—')}</td>
      <td><code>${esc(l.model || '—')}</code></td>
      <td class="${ok ? 'ok' : 'bad'}">${l.status || '—'}</td>
      <td class="num">${l.latency_ms}ms</td>
      <td class="num">${compact((l.prompt_tokens || 0) + (l.completion_tokens || 0))}</td>
      <td class="num">¥${money(l.cost)}</td>
      <td class="note" title="${esc(note)}">${esc(note)}</td>
    </tr>`;
  }).join('');
}

function renderState(d) {
  state.data = d;

  // 概览
  const t = d.totals || {};
  $('kHealthy').textContent = t.healthy ?? 0;
  $('kTotal').textContent = (d.keys || []).length;
  $('kRequests').textContent = compact(t.requests || 0);
  // 用账本口径（持久化）而不是进程内计数器：后者一重启就归零，看着像"没有流量"。
  const okN = t.requests || 0;
  const errN = t.errors || 0;
  const allN = okN + errN;
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

  const b = d.batch || {};
  const sessN = b.sessions || 0;
  const infl = b.total_inflight || 0;
  $('kBatch').textContent = (b.mode && b.mode !== 'off')
    ? `batch ${b.size}/${b.target} · 会话 ${sessN} · 在途 ${infl}`
    : `全池平分 · 会话 ${sessN} · 在途 ${infl}`;

  const today = (d.days && d.days[0]) ? d.days[0] : null;
  $('kTodayCost').textContent = money(today ? today.cost : 0);
  $('kTodayReq').textContent = today ? `${today.requests} 次请求` : '暂无数据';

  $('upstreamTag').textContent = d.upstream || '';
  $('uptime').textContent = '运行 ' + fmtDur(d.uptime_seconds || 0);

  renderKeys(d.keys);
  renderLogs(d.logs);
  renderAccess(d);
  renderPoolForm(d.pool);
  $('priceHint').textContent = `价目表：${d.prices?.count || 0} 个模型 · 更新于 ${hhmmss(d.prices?.updated_at)}`;
  $('logHint').textContent = `最近 ${(d.logs || []).length} 条`;
}

function renderAccess(d) {
  const el = $('accessBox');
  if (!el) return;
  const keys = d.client_keys || [];
  el.innerHTML = `
    <div class="access-item"><span class="muted">Base URL</span><code>${esc(location.origin)}/v1</code></div>
    <div class="access-item"><span class="muted">客户端密钥</span><code>${esc(keys[0] || '未配置')}</code>
      <button class="icon-btn" id="btnCopyKey">复制</button></div>`;
  const btn = $('btnCopyKey');
  if (btn) {
    btn.onclick = () => {
      navigator.clipboard?.writeText(keys[0] || '').then(
        () => toast('已复制'), () => toast('复制失败，请手动选中', true));
    };
  }
}

function renderPoolForm(p) {
  if (!p) return;
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

async function refresh() {
  const d = await api('/panel/api/state');
  renderState(d);
  return d;
}

// ---------- 操作 ----------

$('btnRefresh').addEventListener('click', () => refresh().then(() => toast('已刷新')).catch((e) => toast(e.message, true)));

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
    await refresh();
    toast('策略已生效');
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
  } catch (err) {
    localStorage.removeItem('trpool_token');
    state.token = '';
    showLogin('登录态失效：' + err.message);
  }
})();
