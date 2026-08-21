import express from "express";
import { spawn } from "child_process";
import { WebSocketServer } from "ws";

const app = express();
app.use(express.json());

// ======================
// 🎯 STATE
// ======================
let ffmpegProcesses = {};
let viewerIntervals = {};
let viewers = {};

// إجمالي المشاهدات (لا يتم تصفيره)
let totalViews = {};

// عملاء الـ WebSocket المتصلين بالداشبورد
let clients = [];

// سجل الأحداث (آخر 200 حدث فقط)
let eventLog = [];
const MAX_EVENTS = 200;

function logEvent(id, type, message) {
  const entry = {
    id,
    type, // start | stop | exit | restart
    message,
    time: new Date().toISOString()
  };

  eventLog.unshift(entry);

  if (eventLog.length > MAX_EVENTS) {
    eventLog = eventLog.slice(0, MAX_EVENTS);
  }

  broadcastEvent(entry);
}

// ======================
// 🎯 CHANNELS
// ======================
const channels = {
  ch4k: {
    input: "http://195.182.16.45:8080/live/omar777/01103978590/460864.ts",
    output: "rtmp://live.twitch.tv/app/live_151597255_5HndsveAXExMraoT8RGtn23qCKcVx0"
  },
  ch1: {
    input: "https://163.ostv.info/krikar/krikar/652333?token=ShJcU0BbQQNHDgxcBwYDCVsBAwdTV1FYCVdTAQABBAtUCAAHCwZTCAAbGBpEF0dcWF1vWQIXXlIJWlcFVEgRR0JVRm1aV0EDRwgBCgRWDAkbHBJED1gBQwtTVQFQXQQKBwUHHhFDCl1HA1pNWw8ZG1xIRFUUWwUNbgYHQQwHVhALXkFeXx9BVgtmUF1aAltdGwoSAUQZRghCEkANCxFfXh0SVltHQQJNABsOVkIPWRUbU19FCEEWGBNYQH40Rh8QVEhAV11AClYLGw4aQxAXFRtZQ28UUBcVQwcDWgAWEQgTABYeEV4CQTpaW1ZZBlZNUF9eQ0QPRlATTkBaCgpaRl5Ca0JaV0EDC0xYVEo=",
    output: "rtmp://vsu.okcdn.ru/input/15037126680149_16572030782037_nwbfmzaoxm"
  },
  ch2: {
    input: "https://163.ostv.info/krikar/krikar/652334?token=ShJcU0BbQQNHDgxcBwYDCVsBAwdTV1FYCVdTAQABBAtUCAAHCwZTCAAbGBpEF0dcWF1vWQIXXlIJWlcFVEgRR0JVRm1aV0EDRwgBCgRWDAkbHBJED1gBQwtTVQFQXQQKBwUHHhFDCl1HA1pNWw8ZG1xIRFUUWwUNbgYHQQwHVhALXkFeXx9BVgtmUF1aAltdGwoSAUQZRghCEkANCxFfXh0SVltHQQJNABsOVkIPWRUbU19FCEEWGBNYQH40Rh8QVEhAV11AClYLGw4aQxAXFRtZQ28UUBcVQwcDWgAWEQgTABYeEV4CQTpaW1ZZBlZNUF9eQ0QPRlATTkBaCgpaRl5Ca0JaV0EDC0xYVEo=",
    output: "rtmp://vsu.okcdn.ru/input/15037158268501_16572084062805_f6sgg23zdy"
  },
  ch3: {
    input: "https://ranapkbd.site/RANAPK33g/TVD/play.php?id=1745020",
    output: "rtmp://msk.goodgame.ru:1940/live/221746?pwd=5dfed73aa7930d86"
  },
  ch4: {
    input: "http://185.160.192.14/live/171348492752/5S6HGsea3j/255226.m3u8",
    output: "rtmp://fr.pscp.tv:80/x/ivphyvtww7k3"
  },
  ch5: {
    input: "http://185.160.192.14/live/171348492752/5S6HGsea3j/255225.m3u8",
    output: "rtmp://vsu.okcdn.ru/input/14863707479574_16379956300310_uoslkp4xrm"
  },
  ch6: {
    input: "https://ranapkbd.site/RANAPK33g/TVD/play.php?id=1745020",
    output: "rtmp://vsu.okcdn.ru/input/14901168119318_16447213341206_ssfncxg2zu"
  },
  ch7: {
    input: "https://stream.camcloud.stream/stream/97e7e9e05d4e/playlist.m3u8",
    output: "rtmp://vsu.okcdn.ru/input/13415538433558_13690939181590_flxfen3y2u"
  },
  ch8: {
    input: "https://man1ted.com/watch/beemax1.m3u8",
    output: "rtmp://vsu.okcdn.ru/input/9978322492950_8842256321046_oxg7ed4dcm"
  },
  ch9: {
    input: "http://185.160.192.14/live/171348492752/5S6HGsea3j/255226.m3u8",
    output: "rtmp://vsu.okcdn.ru/input/13418102398486_13695919458838_h7ihlwq5ca"
  },
  ch10: {
    input: "http://185.160.192.14/live/171348492752/5S6HGsea3j/255225.m3u8",
    output: "rtmp://vsu.okcdn.ru/input/14994479390230_16613027809814_7sovqbfsba"
  },
  ch11: {
    input: "https://ranapkbd.site/RANAPK33g/TVD/play.php?id=1745020",
    output: "rtmp://vsu.okcdn.ru/input/14994482273814_16613032593942_cmf7uzoh2q"
  }
};

// ======================
// 🎬 LOGO
// ======================
function getLogo(id) {
  const logos = {
    ch4k: "logo4kh.png",
    ch1: "logo1.png",
    ch2: "logo22.png",
    ch3: "logo33.png",
    ch4: "logo44.png",
    ch5: "logo55.png",
    ch6: "logo66.png",
    ch7: "quran.png",
    ch8: "aflam.png",
    ch9: "mosalsalat.png",
    ch10: "animy.png",
    ch11: "kids.png",
  };

  return logos[id] || "logo.png";
}

// ======================
// 🛡️ SAFETY
// ======================
process.on("uncaughtException", (err) => {
  console.log("🔥 ERROR:", err.message);
});

process.on("unhandledRejection", (err) => {
  console.log("🔥 PROMISE ERROR:", err);
});

// ======================
// 🎬 START STREAM
// ======================
function spawnStream(id) {
  if (ffmpegProcesses[id]) return;

  const ch = channels[id];
  if (!ch) return;

  console.log("▶ START:", id);
  logEvent(id, "start", "تم تشغيل القناة");

  const ffmpeg = spawn("ffmpeg", [
    "-re",

    "-reconnect", "1",
    "-reconnect_streamed", "1",
    "-reconnect_delay_max", "5",

    "-i", ch.input,
    "-i", getLogo(id),

    "-filter_complex",
    "[0:v]scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2[base];[1:v]scale=-1:3000[logo];[base][logo]overlay=W-w-2:2",

    "-c:v", "libx264",
    "-preset", "veryfast",
    "-tune", "zerolatency",

    "-b:v", "2500k",
    "-maxrate", "2500k",
    "-bufsize", "4000k",

    "-r", "25",
    "-g", "50",

    "-c:a", "aac",
    "-b:a", "128k",

    "-f", "flv",
    ch.output
  ]);

  ffmpegProcesses[id] = ffmpeg;

  // متابعة عدد المشاهدين (تقريبي - غير مرتبط باتصالات فعلية)
  viewers[id] = Math.floor(Math.random() * 5) + 2;

  if (totalViews[id] == null) {
    totalViews[id] = 0;
  }

  if (viewerIntervals[id]) clearInterval(viewerIntervals[id]);

  viewerIntervals[id] = setInterval(() => {
    const r = Math.random();

    if (r > 0.7) {
      viewers[id]++;
      totalViews[id]++;
    } else if (r < 0.3) {
      viewers[id] = Math.max(1, viewers[id] - 1);
    }
  }, 5000);

  ffmpeg.stderr.on("data", (d) => {
    console.log(`[${id}] ${d.toString()}`);
  });

  ffmpeg.on("exit", () => {
    console.log("❌ EXIT:", id);
    logEvent(id, "exit", "توقفت القناة (خروج غير متوقع)");

    delete ffmpegProcesses[id];
    viewers[id] = 0;

    if (viewerIntervals[id]) {
      clearInterval(viewerIntervals[id]);
      delete viewerIntervals[id];
    }

    // إعادة تشغيل تلقائي
    setTimeout(() => {
      if (!ffmpegProcesses[id]) {
        logEvent(id, "restart", "إعادة تشغيل تلقائية بعد التوقف");
        spawnStream(id);
      }
    }, 8000);
  });
}

// ======================
// 🌐 ROUTES
// ======================
app.get("/", (req, res) => {
  res.send("🚀 IPTV PRO SERVER RUNNING");
});

app.get("/start", (req, res) => {
  const id = req.query.id;
  if (!channels[id]) return res.send("❌ invalid channel");

  spawnStream(id);
  res.send("started " + id);
});

app.get("/stop", (req, res) => {
  const id = req.query.id;

  if (ffmpegProcesses[id]) {
    ffmpegProcesses[id].kill("SIGKILL");
    delete ffmpegProcesses[id];
    logEvent(id, "stop", "تم إيقاف القناة يدويًا");
  }

  viewers[id] = 0;

  if (viewerIntervals[id]) {
    clearInterval(viewerIntervals[id]);
    delete viewerIntervals[id];
  }

  res.send("stopped " + id);
});

// ======================
// ▶⏹ تشغيل / إيقاف كل القنوات دفعة واحدة
// ======================
app.get("/start-all", (req, res) => {
  for (const id in channels) {
    spawnStream(id);
  }
  res.json({ ok: true });
});

app.get("/stop-all", (req, res) => {
  for (const id in channels) {
    if (ffmpegProcesses[id]) {
      ffmpegProcesses[id].kill("SIGKILL");
      delete ffmpegProcesses[id];
      logEvent(id, "stop", "تم إيقاف القناة يدويًا (إيقاف الكل)");
    }
    viewers[id] = 0;
    if (viewerIntervals[id]) {
      clearInterval(viewerIntervals[id]);
      delete viewerIntervals[id];
    }
  }
  res.json({ ok: true });
});

// ======================
// 📜 سجل الأحداث
// ======================
app.get("/events", (req, res) => {
  res.json(eventLog);
});

// ======================
// 📊 STATUS
// ======================
app.get("/status", (req, res) => {
  const result = {};

  for (const id in channels) {
    result[id] = {
      active: !!ffmpegProcesses[id],
      viewers: viewers[id] || 0,
      total: totalViews[id] || 0
    };
  }

  res.json(result);
});

// ======================
// 📺 CHANNELS API
// ======================
app.get("/channels", (req, res) => {
  res.json(channels);
});

app.post("/channel", (req, res) => {
  const { id, input, output } = req.body;

  if (!id || !input || !output)
    return res.status(400).json({ ok: false });

  channels[id] = { input, output };

  res.json({ ok: true });
});

app.put("/channel/:id", (req, res) => {
  const id = req.params.id;

  if (!channels[id])
    return res.status(404).json({ ok: false });

  channels[id] = {
    ...channels[id],
    input: req.body.input ?? channels[id].input,
    output: req.body.output ?? channels[id].output
  };

  res.json({ ok: true });
});

app.delete("/channel/:id", (req, res) => {
  const id = req.params.id;

  if (ffmpegProcesses[id]) {
    ffmpegProcesses[id].kill("SIGKILL");
    delete ffmpegProcesses[id];
  }

  delete channels[id];

  res.json({ ok: true });
});

// ===============================
// 📡 DASHBOARD PRO
// ===============================
app.get("/dashboard", (req, res) => {

  const html = `
<!DOCTYPE html>
<html dir="rtl">

<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,viewport-fit=cover">
<title>IPTV PRO PANEL</title>

<style>

*{
box-sizing:border-box;
}

html, body{
margin:0;
padding:0;
overflow-x:hidden;
}

body{
font-family:Arial;
background:linear-gradient(135deg,#070b1a,#0b1020);
color:white;
display:flex;
min-height:100vh;
min-height:100dvh;
}

.side{
width:260px;
flex-shrink:0;
background:rgba(16,25,56,0.9);
backdrop-filter:blur(10px);
padding:20px;
border-right:1px solid #1d2b56;
transition:transform 0.3s;
z-index:1000;
}

.side h2{
margin-bottom:20px;
color:#3da9fc;
}

.side button{
width:100%;
padding:12px;
margin-bottom:10px;
border:none;
border-radius:12px;
cursor:pointer;
background:#182347;
color:white;
font-weight:bold;
transition:0.3s;
font-size:15px;
}

.side button:hover{
background:#2a3a6a;
transform:scale(1.03);
}

.main{
flex:1;
padding:20px;
overflow:auto;
width:100%;
min-width:0;
}

.grid{
display:grid;
grid-template-columns:repeat(auto-fill,minmax(320px,1fr));
gap:18px;
}

.card{
background:linear-gradient(145deg,#141d38,#101938);
padding:18px;
border-radius:18px;
border:1px solid #26345f;
box-shadow:0 8px 20px rgba(0,0,0,0.4);
transition:0.3s;
position:relative;
overflow:hidden;
}

.card:hover{
transform:translateY(-5px);
box-shadow:0 12px 25px rgba(0,0,0,0.6);
}

.card::before{
content:"";
position:absolute;
top:0;
left:0;
width:100%;
height:3px;
background:linear-gradient(90deg,#3da9fc,#00ff99);
}

.live{color:#00ff99;font-weight:bold}
.off{color:#ff4d4d;font-weight:bold}

.info{
margin-top:10px;
font-size:13px;
color:#b8c1ec;
word-break:break-all;
}

.btns{
display:flex;
gap:8px;
margin-top:12px;
flex-wrap:wrap;
}

button{
padding:10px;
border:none;
border-radius:10px;
cursor:pointer;
font-weight:bold;
transition:0.2s;
}

button:hover{transform:scale(1.05)}

.start{background:#1db954;color:white}
.stop{background:#e74c3c;color:white}
.edit{background:#3498db;color:white}
.del{background:#555;color:white}

input{
width:100%;
padding:12px;
margin-bottom:10px;
border-radius:10px;
border:none;
background:#0f1733;
color:white;
outline:none;
}

input:focus{
border:1px solid #3da9fc;
}

h3{margin:0;color:#3da9fc}

hr{
border:0;
height:1px;
background:#26345f;
margin:10px 0;
}

#connStatus{
position:fixed;
top:10px;
left:10px;
padding:6px 12px;
border-radius:20px;
font-size:12px;
font-weight:bold;
z-index:999;
}

.conn-ok{background:#1db95433;color:#1db954;border:1px solid #1db954}
.conn-bad{background:#e74c3c33;color:#e74c3c;border:1px solid #e74c3c}

/* ======================
   📱 MOBILE / RESPONSIVE
   ====================== */

.menuBtn{
display:none;
position:fixed;
top:12px;
right:12px;
z-index:1100;
width:44px;
height:44px;
border-radius:12px;
border:1px solid #26345f;
background:#141d38;
color:#3da9fc;
font-size:20px;
cursor:pointer;
align-items:center;
justify-content:center;
}

.overlay{
display:none;
position:fixed;
inset:0;
background:rgba(0,0,0,0.5);
z-index:999;
}

.overlay.show{
display:block;
}

@media (max-width: 820px){

body{
display:block;
}

.menuBtn{
display:flex;
}

.side{
position:fixed;
top:0;
right:0;
height:100%;
height:100dvh;
transform:translateX(100%);
box-shadow:-10px 0 30px rgba(0,0,0,0.5);
overflow-y:auto;
}

.side.open{
transform:translateX(0);
}

.main{
padding:70px 14px 20px 14px;
}

.grid{
grid-template-columns:1fr;
gap:14px;
}

.card{
padding:14px;
}

.info{
font-size:12.5px;
}

.btns button{
flex:1 1 40%;
font-size:13px;
padding:10px 6px;
}

#connStatus{
top:auto;
bottom:10px;
left:10px;
right:auto;
}

}

@media (max-width: 380px){

.btns button{
flex:1 1 100%;
}

}

</style>

</head>

<body>

<div id="connStatus" class="conn-bad">⏳ اتصال...</div>

<button class="menuBtn" onclick="toggleMenu()">☰</button>
<div class="overlay" id="overlay" onclick="closeMenu()"></div>

<div class="side" id="sideMenu">

<h2>📡 IPTV PRO</h2>

<button onclick="show('channels');closeMenu()">📺 القنوات</button>
<button onclick="show('add');closeMenu()">➕ إضافة قناة</button>
<button onclick="show('events');closeMenu()">📜 سجل الأحداث</button>

<hr>

<button onclick="startAll()" style="background:#1db954">▶ تشغيل الكل</button>
<button onclick="stopAll()" style="background:#e74c3c">⏹ إيقاف الكل</button>

</div>

<div class="main">

<div id="channels">

<input id="searchBox" placeholder="🔍 بحث عن قناة..." oninput="onSearch(this.value)" style="max-width:400px">

<div id="list" class="grid"></div>
</div>

<div id="add" style="display:none">

<h2>➕ إضافة قناة</h2>

<input id="id" placeholder="Channel ID">
<input id="input" placeholder="Input URL">
<input id="output" placeholder="RTMP Output">

<button onclick="addChannel()">➕ إضافة القناة</button>

</div>

<div id="events" style="display:none">

<h2>📜 سجل الأحداث</h2>

<div id="eventsList"></div>

</div>

</div>

<script>

let channelsCache = {};
let statusCache = {};
let eventsCache = [];
let searchTerm = "";

function toggleMenu(){
document.getElementById("sideMenu").classList.toggle("open");
document.getElementById("overlay").classList.toggle("show");
}

function closeMenu(){
document.getElementById("sideMenu").classList.remove("open");
document.getElementById("overlay").classList.remove("show");
}

function show(id){
document.getElementById("channels").style.display="none";
document.getElementById("add").style.display="none";
document.getElementById("events").style.display="none";
document.getElementById(id).style.display="block";
}

function render(){

const box = document.getElementById("list");
box.innerHTML = "";

const term = searchTerm.trim().toLowerCase();

for(const id in channelsCache){

if(term && !id.toLowerCase().includes(term)) continue;

const current = statusCache[id]?.viewers || 0;

box.innerHTML += \`

<div class="card">

<h3>📺 \${id}</h3>

<div class="\${statusCache[id]?.active ? 'live' : 'off'}">
\${statusCache[id]?.active ? '🟢 LIVE' : '🔴 OFFLINE'}
</div>

<div class="info">
👁️ الحالي: <b>\${current}</b>
<br>
📊 الإجمالي: <b>\${statusCache[id]?.total || 0}</b>
</div>

<hr>

<div class="info">
<b>INPUT:</b><br>\${channelsCache[id].input}
</div>

<div class="info">
<b>OUTPUT:</b><br>\${channelsCache[id].output}
</div>

<div class="btns">
<button class="start" onclick="start('\${id}')">▶ تشغيل</button>
<button class="stop" onclick="stop('\${id}')">⏹ إيقاف</button>
<button class="edit" onclick="editChannel('\${id}')">✏ تعديل</button>
<button class="del" onclick="del('\${id}')">🗑 حذف</button>
</div>

</div>

\`;

}

}

// تحميل كامل (channels + status) - يُستخدم عند فتح الصفحة أو بعد أي تعديل
async function load(){
const ch = await fetch("/channels");
channelsCache = await ch.json();

const st = await fetch("/status");
statusCache = await st.json();

render();
}

// تحميل سجل الأحداث (مرة واحدة عند فتح الصفحة)
async function loadEvents(){
const r = await fetch("/events");
eventsCache = await r.json();
renderEvents();
}

function eventLabel(type){
if(type === "start") return { icon:"▶", cls:"live", text:"تشغيل" };
if(type === "stop") return { icon:"⏹", cls:"off", text:"إيقاف" };
if(type === "exit") return { icon:"❌", cls:"off", text:"خروج غير متوقع" };
if(type === "restart") return { icon:"🔄", cls:"live", text:"إعادة تشغيل" };
return { icon:"•", cls:"", text:type };
}

function renderEvents(){

const box = document.getElementById("eventsList");
if(!box) return;

if(eventsCache.length === 0){
box.innerHTML = '<div class="info">لا يوجد أحداث بعد</div>';
return;
}

box.innerHTML = "";

for(const ev of eventsCache){

const lbl = eventLabel(ev.type);
const t = new Date(ev.time);
const timeStr = t.toLocaleString("ar-EG");

box.innerHTML += \`
<div class="card" style="padding:12px 16px">
<div class="\${lbl.cls}">\${lbl.icon} \${lbl.text} — 📺 \${ev.id}</div>
<div class="info">\${ev.message}</div>
<div class="info" style="opacity:0.7">\${timeStr}</div>
</div>
\`;

}

}

function onSearch(val){
searchTerm = val;
render();
}

async function startAll(){
if(!confirm("تشغيل كل القنوات؟")) return;
await fetch("/start-all");
load();
}

async function stopAll(){
if(!confirm("إيقاف كل القنوات؟")) return;
await fetch("/stop-all");
load();
}

// ============================
// 🔌 WebSocket - تحديث لحظي لعدد المشاهدين وحالة البث
// ============================
let ws;
let wsReconnectTimer;

function connectWS(){
const proto = location.protocol === "https:" ? "wss:" : "ws:";
ws = new WebSocket(proto + "//" + location.host);

ws.onopen = () => {
document.getElementById("connStatus").className = "conn-ok";
document.getElementById("connStatus").innerText = "🟢 متصل مباشر";
};

ws.onclose = () => {
document.getElementById("connStatus").className = "conn-bad";
document.getElementById("connStatus").innerText = "🔴 منقطع - إعادة محاولة...";
clearTimeout(wsReconnectTimer);
wsReconnectTimer = setTimeout(connectWS, 3000);
};

ws.onerror = () => ws.close();

ws.onmessage = (msg) => {
try{
const parsed = JSON.parse(msg.data);

if(parsed.type === "status"){
statusCache = parsed.data;
render();
} else if(parsed.type === "event"){
eventsCache.unshift(parsed.data);
if(eventsCache.length > 200) eventsCache.pop();
renderEvents();
}

}catch(e){}
};
}

// ▶ actions
async function start(id){
await fetch("/start?id="+id);
load();
}

async function stop(id){
await fetch("/stop?id="+id);
load();
}

async function addChannel(){
await fetch("/channel",{
method:"POST",
headers:{ "Content-Type":"application/json" },
body:JSON.stringify({
id:id.value,
input:input.value,
output:output.value
})
});

load();
show("channels");
}

async function del(id){
await fetch("/channel/"+id,{ method:"DELETE" });
load();
}

async function editChannel(id){
const r = await fetch("/channels");
const data = await r.json();

const inputVal = prompt("Input", data[id].input);
if(!inputVal) return;

const outputVal = prompt("Output", data[id].output);
if(!outputVal) return;

await fetch("/channel/"+id,{
method:"PUT",
headers:{ "Content-Type":"application/json" },
body:JSON.stringify({
input:inputVal,
output:outputVal
})
});

load();
}

// 🚀 بداية التشغيل
load();
loadEvents();
connectWS();

// تحديث احتياطي لو الـ WebSocket انقطع لفترة (fallback فقط)
setInterval(()=>{
if(!ws || ws.readyState !== 1) load();
}, 5000);

</script>

</body>
</html>
`;

  res.send(html);

});

// ======================
// 🚀 SERVER START (مرة واحدة فقط)
// ======================
const port = process.env.PORT || 3000;

const server = app.listen(port, () => {
  console.log("🚀 IPTV PRO RUNNING ON PORT", port);
});

// ===============================
// 📡 WebSocket server (متصل بنفس الـ HTTP server)
// ===============================
const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
  clients.push(ws);

  ws.on("close", () => {
    clients = clients.filter(c => c !== ws);
  });

  ws.on("error", () => {
    clients = clients.filter(c => c !== ws);
  });
});

// بث دوري لحالة القنوات لكل المتصلين
function broadcast() {
  const data = {};

  for (const id in channels) {
    data[id] = {
      active: !!ffmpegProcesses[id],
      viewers: viewers[id] || 0,
      total: totalViews[id] || 0
    };
  }

  const payload = JSON.stringify({ type: "status", data });

  clients.forEach(ws => {
    if (ws.readyState === 1) {
      ws.send(payload);
    }
  });
}

// بث فوري لحدث جديد (تشغيل/إيقاف/خروج/إعادة تشغيل) لكل المتصلين
function broadcastEvent(entry) {
  const payload = JSON.stringify({ type: "event", data: entry });

  clients.forEach(ws => {
    if (ws.readyState === 1) {
      ws.send(payload);
    }
  });
}

setInterval(broadcast, 2000);
