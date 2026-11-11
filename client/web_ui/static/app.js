// Browser uses HTTP + SSE; all untrusted text is rendered with textContent.
const $ = (id) => document.getElementById(id);
let me = null, target = null, eventSource = null;
const transfers = new Map(), offers = new Set();
async function api(path, body) {
  const response = await fetch(path, {method:'POST', body:body instanceof Blob ? body : JSON.stringify(body)});
  const data = await response.json();
  if (!response.ok || data.ok === false) throw new Error(data.reason || 'Request failed');
  return data;
}
function addLine(text, cls='') {
  const div = document.createElement('div'); div.className = cls;
  div.textContent = `${new Date().toLocaleTimeString()} ${text}`;
  $('log').appendChild(div);
  while ($('log').children.length > 500) $('log').firstChild.remove();
  $('log').scrollTop = $('log').scrollHeight;
}
const guarded = (fn) => async () => { try { await fn(); } catch(e) { addLine(e.message, 'err'); $('loginErr').textContent = e.message; } };
$('loginBtn').onclick = guarded(async () => {
  await api('/api/login', {username:$('username').value, password:$('password').value});
  $('password').value = '';
  await refreshState();
});
$('logoutBtn').onclick = guarded(async () => { await api('/api/logout', {}); await refreshState(); });
function startEvents() {
  if (eventSource) return;
  eventSource = new EventSource('/api/events');
  eventSource.onmessage = (event) => handle(JSON.parse(event.data));
  eventSource.onerror = () => { $('uiNet').textContent = 'HTTP/SSE: mất kết nối (mỗi daemon dùng một tab)'; };
  eventSource.onopen = () => { $('uiNet').textContent = 'HTTP/SSE: kết nối'; };
}
async function refreshState() {
  const res = await fetch('/api/state');
  const state = await res.json();
  me = state.username;
  $('login').hidden = !!me; $('app').hidden = !me; $('me').textContent = me || '';
  $('net').textContent = `UDP server: ${state.connected ? (me ? 'đã đăng nhập' : 'chưa đăng nhập') : 'offline / đang thử lại'}`;
  const stats = state.transport_stats;
  $('stats').textContent = stats ? `RTT: ${stats.srtt_seconds == null ? 'chưa đo' : (stats.srtt_seconds * 1000).toFixed(1) + ' ms'}; retransmit: ${stats.retransmitted_packets}` : 'RTT/retransmit: chờ API transport của A';
  renderUsers(state.users);
}
function handle(ev) {
  switch(ev.event) {
    case 'status': $('net').textContent = `UDP server: ${ev.state} ${ev.reason || ''}`; break;
    case 'users': renderUsers(ev.users); break;
    case 'chat': addLine(`${ev.private ? '[riêng] ' : ''}${ev.from}: ${ev.text}`, ev.private ? 'private' : ''); break;
    case 'file_offer': offer(ev); break;
    case 'file_progress': progress(ev); break;
    case 'file_done': {
      const row = transfers.get(ev.file_id); if (row) { row.remove(); transfers.delete(ev.file_id); }
      addLine(`File ${ev.name || ev.file_id}: ${ev.ok ? 'hoàn tất, hash hợp lệ' : 'THẤT BẠI'}`, ev.ok ? 'ok' : 'err'); break;
    }
    case 'error': addLine('Lỗi: ' + ev.reason, 'err'); break;
  }
}
function renderUsers(list) {
  if (target && !list.includes(target)) target = null;
  $('target').textContent = target ? 'Riêng → ' + target : 'Chat chung';
  $('users').replaceChildren();
  list.filter((u) => u !== me).forEach((u) => {
    const li = document.createElement('li'); li.textContent = u;
    li.className = u === target ? 'sel' : '';
    li.onclick = () => { target = target === u ? null : u; renderUsers(list); };
    $('users').appendChild(li);
  });
}
const sendText = guarded(async () => {
  const text = $('text').value.trim(); if (!text) return;
  await api('/api/chat', {text, to:target});
  addLine(`Tôi${target ? ' → ' + target : ''}: ${text}`, 'mine'); $('text').value = '';
});
$('sendBtn').onclick = sendText;
$('text').onkeydown = (e) => { if(e.key === 'Enter') sendText(); };
$('fileBtn').onclick = () => { if(!target) addLine('Chọn người nhận trước.', 'err'); else $('file').click(); };
$('file').onchange = guarded(async () => {
  const file = $('file').files[0]; if(!file) return;
  try {
    await api(`/api/file?to=${encodeURIComponent(target)}&name=${encodeURIComponent(file.name)}`, file);
    addLine(`Đã gửi lời mời file "${file.name}" tới ${target}`);
  } finally { $('file').value = ''; }
});
function offer(ev) {
  if (offers.has(ev.file_id)) return;
  offers.add(ev.file_id);
  const div = document.createElement('div'); div.className = 'offer';
  div.textContent = `${ev.from} muốn gửi "${ev.name}" (${ev.size} byte) `;
  ['Nhận', 'Từ chối'].forEach((label, i) => {
    const button = document.createElement('button'); button.textContent = label;
    button.onclick = guarded(async () => {
      await api('/api/file/' + (i ? 'reject' : 'accept'), {file_id:ev.file_id});
      div.remove();
    });
    div.appendChild(button);
  });
  $('transfers').appendChild(div);
}
function progress(ev) {
  let row = transfers.get(ev.file_id);
  if(!row) {
    row = document.createElement('div'); row.append(document.createElement('span'), document.createElement('progress'));
    transfers.set(ev.file_id, row); $('transfers').appendChild(row);
  }
  row.lastChild.max = ev.total || 1; row.lastChild.value = ev.done;
  const stage = {queued:'đã xếp hàng; chờ xác nhận người nhận', acked_to_server:'đã ACK tới server; chờ người nhận', received:'đã nhận; đang kiểm hash'}[ev.stage] || ev.stage;
  row.firstChild.textContent = `${ev.file_id.slice(0,8)}: ${ev.done}/${ev.total} byte — ${stage} `;
}
startEvents();
refreshState().catch(e => { $('loginErr').textContent = e.message; });
setInterval(() => refreshState().catch(() => { $('uiNet').textContent = 'HTTP: daemon không phản hồi'; }), 3000);
