// [B – Bước 9] Logic giao diện. Chỉ dùng fetch + EventSource (HTTP thuần), không WebSocket.
// TODO(B): hiển thị RTT/số gói gửi lại (cần /api/state trả thêm), nút từ chối file, tự cuộn, định dạng giờ.
const $ = (id) => document.getElementById(id);
let me = null, target = null;          // target = null -> chat chung, ngược lại tên người nhận riêng
const transfers = {};                  // file_id -> phần tử <progress>

async function api(path, body, opts = {}) {
  const res = await fetch(path, { method: "POST", body: body instanceof Blob ? body : JSON.stringify(body), ...opts });
  return res.json();
}

function addLine(text, cls = "") {
  const div = document.createElement("div");
  div.className = cls; div.textContent = text;
  $("log").appendChild(div); $("log").scrollTop = $("log").scrollHeight;
}

$("loginBtn").onclick = async () => {
  const r = await api("/api/login", { username: $("username").value, password: $("password").value });
  if (!r.ok) { $("loginErr").textContent = r.reason; return; }
  me = $("username").value; $("me").textContent = me;
  $("login").hidden = true; $("app").hidden = false;
  startEvents();
};

function startEvents() {
  const es = new EventSource("/api/events");
  es.onmessage = (e) => handle(JSON.parse(e.data));
  es.onerror = () => { $("net").textContent = "● mất kết nối UI"; };
}

function handle(ev) {
  switch (ev.event) {
    case "status": $("net").textContent = ev.state === "connected" ? "● đã kết nối" : "● mất kết nối: " + (ev.reason || ""); break;
    case "users": renderUsers(ev.users); break;
    case "chat": addLine(`${ev.private ? "[riêng] " : ""}${ev.from}: ${ev.text}`, ev.private ? "private" : ""); break;
    case "file_offer": offer(ev); break;
    case "file_progress": progress(ev); break;
    case "file_done": addLine(`File ${ev.name || ev.file_id}: ${ev.ok ? "hoàn tất, hash hợp lệ" : "THẤT BẠI"}`, ev.ok ? "ok" : "err"); break;
    case "error": addLine("Lỗi: " + ev.reason, "err"); break;
  }
}

function renderUsers(list) {
  const ul = $("users"); ul.innerHTML = "";
  list.filter((u) => u !== me).forEach((u) => {
    const li = document.createElement("li"); li.textContent = u;
    if (u === target) li.className = "sel";
    li.onclick = () => { target = target === u ? null : u; $("target").textContent = target ? "Riêng → " + target : "Chat chung"; renderUsers(list); };
    ul.appendChild(li);
  });
}

$("sendBtn").onclick = sendText;
$("text").onkeydown = (e) => { if (e.key === "Enter") sendText(); };
async function sendText() {
  const text = $("text").value.trim(); if (!text) return;
  await api("/api/chat", { text, to: target });
  addLine(`Tôi${target ? " → " + target : ""}: ${text}`, "mine"); $("text").value = "";
}

$("fileBtn").onclick = () => { if (!target) { addLine("Chọn một người ở danh sách bên trái trước.", "err"); return; } $("file").click(); };
$("file").onchange = async () => {
  const f = $("file").files[0]; if (!f) return;
  addLine(`Đã gửi lời mời file "${f.name}" tới ${target}…`);
  await api(`/api/file?to=${encodeURIComponent(target)}&name=${encodeURIComponent(f.name)}`, f);
  $("file").value = "";
};

function offer(ev) {
  const div = document.createElement("div"); div.className = "offer";
  div.textContent = `${ev.from} muốn gửi "${ev.name}" (${ev.size} byte) `;
  const yes = document.createElement("button"); yes.textContent = "Nhận";
  const no = document.createElement("button"); no.textContent = "Từ chối";
  yes.onclick = () => { api("/api/file/accept", { file_id: ev.file_id }); div.remove(); };
  no.onclick = () => { api("/api/file/reject", { file_id: ev.file_id }); div.remove(); };
  div.append(yes, no); $("transfers").appendChild(div);
}

function progress(ev) {
  let bar = transfers[ev.file_id];
  if (!bar) { bar = transfers[ev.file_id] = document.createElement("progress"); $("transfers").appendChild(bar); }
  bar.max = ev.total; bar.value = ev.done; bar.title = `${ev.direction}: ${ev.done}/${ev.total}`;
  if (ev.done >= ev.total) setTimeout(() => { bar.remove(); delete transfers[ev.file_id]; }, 2000);
}
