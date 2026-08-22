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

// آخر سطور ffmpeg لكل قناة (لعرضها في اللوحة)
let ffmpegLogs = {};
const MAX_LOG_LINES = 300;

function pushLog(id, line) {
  if (!ffmpegLogs[id]) ffmpegLogs[id] = [];
  ffmpegLogs[id].push({ line, time: new Date().toISOString() });
  if (ffmpegLogs[id].length > MAX_LOG_LINES) {
    ffmpegLogs[id] = ffmpegLogs[id].slice(-MAX_LOG_LINES);
  }
}

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
    const text = d.toString();
    console.log(`[${id}] ${text}`);

    // نقسم الناتج لأسطر ونحفظها في لوج القناة
    text.split("\n").forEach(line => {
      const trimmed = line.trim();
      if (trimmed) pushLog(id, trimmed);
    });
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
// 📄 لوج ffmpeg لكل قناة
// ======================
app.get("/logs/:id", (req, res) => {
  const id = req.params.id;
  res.json(ffmpegLogs[id] || []);
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
<html dir="rtl" lang="ar">

<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,viewport-fit=cover">
<title>لوحة التحكم الرئيسية · Master Control</title>

<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Arabic:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap" rel="stylesheet">

<style>

:root{
--bg:#0b0d10;
--surface:#12151a;
--surface-2:#181c22;
--surface-3:#1e232a;
--line:#242931;
--line-soft:#1a1e24;
--text:#d7dce1;
--text-2:#8b929b;
--text-3:#565c65;
--ok-rgb:62,207,110;
--fault-rgb:229,72,77;
--warn-rgb:242,169,59;
--accent-rgb:91,141,238;
--off:#4a505a;
--shadow:0 10px 30px rgba(0,0,0,0.35);
}

html[data-theme="amber"]{
--bg:#0d0a06;
--surface:#161108;
--surface-2:#1d160c;
--surface-3:#241b0f;
--line:#332510;
--line-soft:#241b0f;
--text:#f2e4c8;
--text-2:#a68f66;
--text-3:#6b5a3c;
--ok-rgb:242,169,59;
--fault-rgb:224,90,45;
--warn-rgb:255,196,0;
--accent-rgb:242,169,59;
--off:#4a3f2c;
}

html[data-theme="arctic"]{
--bg:#f0f2f5;
--surface:#ffffff;
--surface-2:#f4f6f8;
--surface-3:#eaedf1;
--line:#dde1e6;
--line-soft:#e8eaed;
--text:#1c232b;
--text-2:#5b6572;
--text-3:#8791a0;
--ok-rgb:22,142,74;
--fault-rgb:196,40,48;
--warn-rgb:181,105,10;
--accent-rgb:34,92,201;
--off:#c3c9d1;
--shadow:0 8px 24px rgba(30,40,60,0.08);
}

html[data-theme="crimson"]{
--bg:#0c0a0b;
--surface:#161113;
--surface-2:#1d1517;
--surface-3:#24191c;
--line:#33191d;
--line-soft:#241518;
--text:#e9dcdc;
--text-2:#9c8386;
--text-3:#65484b;
--ok-rgb:62,207,110;
--fault-rgb:235,64,52;
--warn-rgb:242,169,59;
--accent-rgb:235,64,52;
--off:#4a3438;
}

*{ box-sizing:border-box; }

html, body{ margin:0; padding:0; overflow-x:hidden; }

@media (prefers-reduced-motion: reduce){
*{ animation-duration:0.01ms !important; animation-iteration-count:1 !important; transition-duration:0.01ms !important; }
}

body{
font-family:'IBM Plex Sans Arabic','IBM Plex Mono',Arial,sans-serif;
background:var(--bg);
color:var(--text);
min-height:100vh;
min-height:100dvh;
transition:background 0.2s, color 0.2s;
}

::selection{ background:rgb(var(--accent-rgb)); color:#0b0d10; }

:focus-visible{ outline:2px solid rgb(var(--accent-rgb)); outline-offset:2px; }

.mono{ font-family:'IBM Plex Mono',monospace; }

.app{ display:flex; min-height:100vh; min-height:100dvh; }

/* ---------- icon rail ---------- */

.side{
width:74px;
flex-shrink:0;
background:var(--surface);
border-left:1px solid var(--line);
display:flex;
flex-direction:column;
align-items:center;
padding:18px 0;
transition:transform 0.25s, background 0.2s;
z-index:1000;
}

.side .brandMark{
width:34px;
height:34px;
border-radius:8px;
background:var(--surface-3);
border:1px solid var(--line);
display:flex;
align-items:center;
justify-content:center;
font-size:15px;
margin-bottom:22px;
color:rgb(var(--accent-rgb));
}

.side nav{
display:flex;
flex-direction:column;
gap:6px;
width:100%;
align-items:center;
}

.side button{
width:52px;
height:52px;
border:1px solid transparent;
border-radius:9px;
background:transparent;
color:var(--text-3);
cursor:pointer;
font-size:18px;
display:flex;
flex-direction:column;
align-items:center;
justify-content:center;
gap:3px;
transition:0.15s;
}

.side button .navLbl{
font-size:9px;
font-family:'IBM Plex Mono',monospace;
letter-spacing:0.3px;
}

.side button:hover{ background:var(--surface-2); color:var(--text); }
.side button.navActive{ background:var(--surface-3); color:rgb(var(--accent-rgb)); border-color:var(--line); }

.side .railFoot{
margin-top:auto;
display:flex;
flex-direction:column;
gap:8px;
width:100%;
align-items:center;
}

.side .railFoot button.go{ color:rgb(var(--ok-rgb)); }
.side .railFoot button.stopAll{ color:rgb(var(--fault-rgb)); }

/* ---------- workspace ---------- */

.workspace{ flex:1; display:flex; flex-direction:column; min-width:0; }

.topbar{
height:58px;
flex-shrink:0;
border-bottom:1px solid var(--line);
background:var(--surface);
display:flex;
align-items:center;
justify-content:space-between;
padding:0 22px;
gap:16px;
transition:background 0.2s;
}

.topbar .titleBlock{ display:flex; flex-direction:column; line-height:1.25; }
.topbar .titleBlock .t1{ font-size:14px; font-weight:700; color:var(--text); }
.topbar .titleBlock .t2{ font-size:10px; color:var(--text-3); font-family:'IBM Plex Mono',monospace; letter-spacing:1px; }

.topbar .metaRight{ display:flex; align-items:center; gap:10px; }

.iconBtn{
width:36px;
height:36px;
display:flex;
align-items:center;
justify-content:center;
border-radius:7px;
border:1px solid var(--line);
background:var(--surface-2);
color:var(--text-2);
cursor:pointer;
font-size:15px;
padding:0;
transition:0.15s;
}

.iconBtn:hover{ color:var(--text); border-color:rgb(var(--accent-rgb)); }

.clock{
font-family:'IBM Plex Mono',monospace;
font-size:19px;
font-weight:500;
color:var(--text);
letter-spacing:1px;
padding:6px 12px;
background:var(--surface-2);
border:1px solid var(--line);
border-radius:7px;
}

.pill{
padding:6px 12px;
border-radius:20px;
font-size:11px;
font-family:'IBM Plex Mono',monospace;
letter-spacing:0.3px;
border:1px solid;
}

.conn-ok{ background:rgba(var(--ok-rgb),0.12); color:rgb(var(--ok-rgb)); border-color:rgba(var(--ok-rgb),0.35); }
.conn-bad{ background:rgba(var(--fault-rgb),0.12); color:rgb(var(--fault-rgb)); border-color:rgba(var(--fault-rgb),0.35); }

.content{ flex:1; padding:22px 26px 30px 26px; overflow:auto; }

/* ---------- status strip ---------- */

.statusStrip{
display:grid;
grid-template-columns:repeat(auto-fit,minmax(150px,1fr));
gap:1px;
background:var(--line);
border:1px solid var(--line);
border-radius:10px;
overflow:hidden;
margin-bottom:18px;
box-shadow:var(--shadow);
}

.stat{
background:var(--surface);
padding:14px 18px;
transition:background 0.2s;
}

.stat .num{ font-family:'IBM Plex Mono',monospace; font-size:26px; font-weight:600; line-height:1; display:block; margin-bottom:4px; }
.stat .lbl{ font-size:10.5px; color:var(--text-3); letter-spacing:0.4px; font-family:'IBM Plex Mono',monospace; }

.stat.ok .num{ color:rgb(var(--ok-rgb)); }
.stat.off .num{ color:var(--text-2); }
.stat.accent .num{ color:rgb(var(--accent-rgb)); }
.stat.warn .num{ color:rgb(var(--warn-rgb)); }

/* ---------- toolbar ---------- */

.toolbar{ display:flex; gap:10px; margin-bottom:16px; flex-wrap:wrap; }

.toolbar input, .toolbar select{
font-family:'IBM Plex Sans Arabic',Arial,sans-serif;
padding:10px 14px;
border-radius:7px;
border:1px solid var(--line);
background:var(--surface-2);
color:var(--text);
outline:none;
font-size:13px;
}

.toolbar input{ flex:1 1 260px; }
.toolbar select{ flex:0 1 220px; cursor:pointer; }
.toolbar input:focus, .toolbar select:focus{ border-color:rgb(var(--accent-rgb)); }

/* ---------- channel table ---------- */

.tableWrap{
border:1px solid var(--line);
border-radius:10px;
overflow:auto;
background:var(--surface);
box-shadow:var(--shadow);
}

table.chTable{ width:100%; border-collapse:collapse; min-width:760px; }

.chTable thead th{
text-align:right;
font-family:'IBM Plex Mono',monospace;
font-size:10px;
letter-spacing:0.8px;
color:var(--text-3);
font-weight:500;
padding:11px 16px;
border-bottom:1px solid var(--line);
background:var(--surface-2);
white-space:nowrap;
position:sticky;
top:0;
z-index:1;
}

.chTable tbody tr{ border-bottom:1px solid var(--line-soft); transition:background 0.12s; border-right:3px solid transparent; }
.chTable tbody tr:last-child{ border-bottom:none; }
.chTable tbody tr:nth-child(even){ background:rgba(255,255,255,0.012); }
.chTable tbody tr:hover{ background:var(--surface-2); }
.chTable tbody tr.rowOn{ border-right-color:rgba(var(--ok-rgb),0.55); }

.chTable td{ padding:12px 16px; vertical-align:middle; font-size:12.5px; }

.statusCell{ display:flex; align-items:center; gap:8px; white-space:nowrap; }

.dot{ width:8px; height:8px; border-radius:50%; background:var(--off); flex-shrink:0; }
.dot.on{ background:rgb(var(--ok-rgb)); box-shadow:0 0 8px 1px rgba(var(--ok-rgb),0.5); animation:dotPulse 2.2s ease-in-out infinite; }

@keyframes dotPulse{ 0%,100%{opacity:1;} 50%{opacity:0.5;} }

.statusTxt{ font-family:'IBM Plex Mono',monospace; font-size:10.5px; letter-spacing:0.4px; }
.statusTxt.on{ color:rgb(var(--ok-rgb)); }
.statusTxt.off{ color:var(--text-3); }

.chName{ font-family:'IBM Plex Mono',monospace; font-weight:600; font-size:13px; color:var(--text); }

.urlCell{
font-family:'IBM Plex Mono',monospace;
font-size:11px;
color:var(--text-2);
direction:ltr;
text-align:left;
max-width:220px;
overflow:hidden;
text-overflow:ellipsis;
white-space:nowrap;
}

.numCell{ font-family:'IBM Plex Mono',monospace; font-size:13px; color:var(--text); text-align:center; }
.numCell.dim{ color:var(--text-3); }

.rowBtns{ display:flex; gap:5px; flex-wrap:nowrap; justify-content:flex-end; }

button{
font-family:'IBM Plex Sans Arabic',Arial,sans-serif;
padding:7px 11px;
border:1px solid var(--line);
border-radius:6px;
cursor:pointer;
font-weight:600;
font-size:11.5px;
background:var(--surface-2);
color:var(--text-2);
transition:0.15s;
white-space:nowrap;
}

button:hover{ filter:brightness(1.2); }
button:active{ transform:translateY(1px); }

.rowBtns .start{ color:rgb(var(--ok-rgb)); border-color:rgba(var(--ok-rgb),0.35); }
.rowBtns .stop{ color:rgb(var(--fault-rgb)); border-color:rgba(var(--fault-rgb),0.35); }
.rowBtns .del{ color:var(--text-3); }

/* ---------- add channel form ---------- */

.formCard{
max-width:480px;
background:var(--surface);
border:1px solid var(--line);
border-radius:10px;
padding:22px;
box-shadow:var(--shadow);
}

.formCard label{
display:block;
font-family:'IBM Plex Mono',monospace;
font-size:10px;
letter-spacing:0.6px;
color:var(--text-3);
margin-bottom:6px;
margin-top:14px;
}

.formCard label:first-of-type{ margin-top:0; }

.formCard input{
width:100%;
padding:11px 13px;
border-radius:7px;
border:1px solid var(--line);
background:var(--surface-2);
color:var(--text);
outline:none;
font-family:'IBM Plex Mono',monospace;
font-size:13px;
direction:ltr;
text-align:left;
}

.formCard input:focus{ border-color:rgb(var(--accent-rgb)); }

.formCard .submit{
margin-top:20px;
width:100%;
background:rgb(var(--accent-rgb));
color:#0b0d10;
border:none;
padding:12px;
font-size:13px;
}

/* ---------- events ---------- */

.evRow{
display:flex;
align-items:center;
gap:14px;
padding:12px 16px;
border-bottom:1px solid var(--line-soft);
font-size:12.5px;
}

.evRow:last-child{ border-bottom:none; }

.evIcon{ width:22px; text-align:center; flex-shrink:0; }
.evIcon.on{ color:rgb(var(--ok-rgb)); }
.evIcon.off{ color:rgb(var(--fault-rgb)); }
.evBody{ flex:1; min-width:0; }
.evChan{ font-family:'IBM Plex Mono',monospace; font-weight:600; color:var(--text); }
.evMsg{ color:var(--text-2); }
.evTime{ font-family:'IBM Plex Mono',monospace; font-size:10.5px; color:var(--text-3); white-space:nowrap; }

/* ---------- log modal ---------- */

.logOverlay{
display:none;
position:fixed;
inset:0;
background:rgba(4,5,7,0.72);
z-index:2000;
align-items:center;
justify-content:center;
padding:20px;
}

.logOverlay.show{ display:flex; }

.logModal{
background:var(--surface);
border:1px solid var(--line);
border-radius:10px;
width:100%;
max-width:800px;
max-height:80vh;
display:flex;
flex-direction:column;
overflow:hidden;
box-shadow:var(--shadow);
}

.logHeader{
display:flex;
justify-content:space-between;
align-items:center;
padding:12px 16px;
border-bottom:1px solid var(--line);
background:var(--surface-2);
}

.logHeader h3{ margin:0; font-size:13px; font-family:'IBM Plex Mono',monospace; font-weight:500; }

.logBody{
padding:14px 16px;
overflow-y:auto;
font-family:'IBM Plex Mono',monospace;
font-size:11px;
color:var(--text-2);
white-space:pre-wrap;
word-break:break-all;
direction:ltr;
text-align:left;
background:var(--bg);
}

.logBody .logLine{ padding:3px 0; border-bottom:1px solid var(--line-soft); }
.logBody .logTime{ color:rgb(var(--accent-rgb)); margin-left:8px; }

/* ---------- theme menu ---------- */

.themeMenu{
position:absolute;
top:52px;
left:22px;
background:var(--surface-2);
border:1px solid var(--line);
border-radius:9px;
padding:6px;
display:none;
flex-direction:column;
gap:2px;
z-index:1500;
box-shadow:var(--shadow);
min-width:150px;
}

.themeMenu.show{ display:flex; }

.themeMenu button{
justify-content:flex-start;
display:flex;
align-items:center;
gap:8px;
background:transparent;
border:1px solid transparent;
color:var(--text-2);
padding:8px 10px;
}

.themeMenu button:hover{ background:var(--surface-3); color:var(--text); }
.themeMenu button.themeActive{ color:rgb(var(--accent-rgb)); border-color:var(--line); }

.swatch{ width:11px; height:11px; border-radius:50%; flex-shrink:0; }

/* ---------- mobile ---------- */

.menuBtn{
display:none;
position:fixed;
top:12px;
left:12px;
z-index:1100;
width:40px;
height:40px;
border-radius:8px;
border:1px solid var(--line);
background:var(--surface);
color:var(--text);
font-size:17px;
cursor:pointer;
align-items:center;
justify-content:center;
}

.overlay{ display:none; position:fixed; inset:0; background:rgba(0,0,0,0.5); z-index:999; }
.overlay.show{ display:block; }

@media (max-width: 820px){

.menuBtn{ display:flex; }

.side{
position:fixed;
top:0;
right:0;
height:100%;
height:100dvh;
width:110px;
transform:translateX(100%);
box-shadow:-10px 0 30px rgba(0,0,0,0.5);
}

.side.open{ transform:translateX(0); }

.topbar{ padding:0 14px 0 58px; }
.topbar .titleBlock .t2{ display:none; }
.clock{ font-size:15px; padding:5px 9px; }
.content{ padding:16px 14px 24px 14px; }

.statusStrip{ grid-template-columns:repeat(2,1fr); }

.themeMenu{ left:14px; top:56px; }

}

</style>
</head>

<body>

<button class="menuBtn" onclick="toggleMenu()">☰</button>
<div class="overlay" id="overlay" onclick="closeMenu()"></div>

<div class="app">

<div class="side" id="sideMenu">
<div class="brandMark">📡</div>
<nav>
<button class="navActive" id="navChannels" onclick="show('channels');closeMenu()">
<span>📺</span><span class="navLbl">قنوات</span>
</button>
<button id="navAdd" onclick="show('add');closeMenu()">
<span>➕</span><span class="navLbl">إضافة</span>
</button>
<button id="navEvents" onclick="show('events');closeMenu()">
<span>📜</span><span class="navLbl">أحداث</span>
</button>
</nav>
<div class="railFoot">
<button class="go" onclick="startAll()" title="تشغيل الكل">
<span>▶</span><span class="navLbl">الكل</span>
</button>
<button class="stopAll" onclick="stopAll()" title="إيقاف الكل">
<span>⏹</span><span class="navLbl">إيقاف</span>
</button>
</div>
</div>

<div class="workspace">

<div class="topbar">
<div class="titleBlock">
<span class="t1">غرفة التحكم الرئيسية</span>
<span class="t2">IPTV MASTER CONTROL</span>
</div>
<div class="metaRight">
<div class="clock mono" id="clock">--:--:--</div>
<button class="iconBtn" id="themeBtn" onclick="toggleThemeMenu(event)" title="تغيير المظهر">🎨</button>
<div id="connStatus" class="pill conn-bad">⏳ اتصال...</div>
</div>

<div class="themeMenu" id="themeMenu">
<button data-t="control" onclick="setTheme('control')"><span class="swatch" style="background:#3ecf6e"></span>تحكم — Control</button>
<button data-t="amber" onclick="setTheme('amber')"><span class="swatch" style="background:#f2a93b"></span>كهرماني — Amber</button>
<button data-t="crimson" onclick="setTheme('crimson')"><span class="swatch" style="background:#eb4034"></span>قرمزي — Crimson</button>
<button data-t="arctic" onclick="setTheme('arctic')"><span class="swatch" style="background:#225cc9"></span>فاتح — Arctic</button>
</div>
</div>

<div class="content">

<section id="channels">

<div id="statsBar" class="statusStrip"></div>

<div class="toolbar">
<input id="searchBox" placeholder="🔍 بحث عن قناة..." oninput="onSearch(this.value)">
<select id="sortSelect" onchange="onSort(this.value)">
<option value="status">ترتيب: الحالة (شغال أولاً)</option>
<option value="viewers">ترتيب: الأعلى مشاهدين</option>
<option value="name">ترتيب: الاسم</option>
</select>
</div>

<div class="tableWrap">
<table class="chTable">
<thead>
<tr>
<th>الحالة</th>
<th>القناة</th>
<th>المصدر (Input)</th>
<th>الخرج (Output)</th>
<th>المشاهدون</th>
<th>الإجمالي</th>
<th></th>
</tr>
</thead>
<tbody id="list"></tbody>
</table>
</div>

</section>

<section id="add" hidden>

<div class="formCard">
<label>Channel ID</label>
<input id="id" placeholder="ch12">
<label>Input URL</label>
<input id="input" placeholder="rtmp:// or http://...">
<label>RTMP Output</label>
<input id="output" placeholder="rtmp://...">
<button class="submit" onclick="addChannel()">➕ إضافة القناة</button>
</div>

</section>

<section id="events" hidden>

<div class="tableWrap" id="eventsList"></div>

</section>

</div>

</div>

</div>

<div class="logOverlay" id="logOverlay" onclick="closeLogs(event)">
<div class="logModal" onclick="event.stopPropagation()">

<div class="logHeader">
<h3 id="logTitle">لوج القناة</h3>
<button onclick="closeLogs()">✕ إغلاق</button>
</div>

<div class="logBody" id="logBody"></div>

<div class="logHeader">
<button onclick="refreshLogs()">🔄 تحديث</button>
</div>

</div>
</div>

<script>

let channelsCache = {};
let statusCache = {};
let eventsCache = [];
let searchTerm = "";
let sortMode = "status";

function toggleMenu(){
document.getElementById("sideMenu").classList.toggle("open");
document.getElementById("overlay").classList.toggle("show");
}

function closeMenu(){
document.getElementById("sideMenu").classList.remove("open");
document.getElementById("overlay").classList.remove("show");
}

function show(id){
document.getElementById("channels").hidden = (id !== "channels");
document.getElementById("add").hidden = (id !== "add");
document.getElementById("events").hidden = (id !== "events");

document.getElementById("navChannels").classList.toggle("navActive", id === "channels");
document.getElementById("navAdd").classList.toggle("navActive", id === "add");
document.getElementById("navEvents").classList.toggle("navActive", id === "events");
}

function setTheme(name){
document.documentElement.setAttribute("data-theme", name === "control" ? "" : name);
try{ localStorage.setItem("iptv_theme", name); }catch(e){}
document.querySelectorAll("#themeMenu button").forEach(b => {
b.classList.toggle("themeActive", b.dataset.t === name);
});
document.getElementById("themeMenu").classList.remove("show");
}

function toggleThemeMenu(e){
e.stopPropagation();
document.getElementById("themeMenu").classList.toggle("show");
}

document.addEventListener("click", () => {
document.getElementById("themeMenu").classList.remove("show");
});

(function initTheme(){
let saved = "control";
try{ saved = localStorage.getItem("iptv_theme") || "control"; }catch(e){}
setTheme(saved);
})();

function updateClock(){
const el = document.getElementById("clock");
if(!el) return;
const now = new Date();
const pad = n => String(n).padStart(2,"0");
el.textContent = pad(now.getHours()) + ":" + pad(now.getMinutes()) + ":" + pad(now.getSeconds());
}

function renderStats(){

const box = document.getElementById("statsBar");
if(!box) return;

let liveCount = 0, offCount = 0, totalViewers = 0;

for(const id in channelsCache){
if(statusCache[id]?.active){
liveCount++;
totalViewers += statusCache[id]?.viewers || 0;
} else {
offCount++;
}
}

const restarts = eventsCache.filter(e => e.type === "restart").length;

box.innerHTML = \`
<div class="stat ok">
<span class="num">\${liveCount}</span>
<span class="lbl">TRANSMITTING</span>
</div>
<div class="stat off">
<span class="num">\${offCount}</span>
<span class="lbl">OFFLINE</span>
</div>
<div class="stat accent">
<span class="num">\${totalViewers}</span>
<span class="lbl">LIVE VIEWERS</span>
</div>
<div class="stat warn">
<span class="num">\${restarts}</span>
<span class="lbl">AUTO-RESTARTS</span>
</div>
\`;

}

function sortedChannelIds(){

let ids = Object.keys(channelsCache);

const term = searchTerm.trim().toLowerCase();
if(term){
ids = ids.filter(id => id.toLowerCase().includes(term));
}

if(sortMode === "status"){
ids.sort((a,b) => {
const aActive = statusCache[a]?.active ? 1 : 0;
const bActive = statusCache[b]?.active ? 1 : 0;
return bActive - aActive;
});
} else if(sortMode === "viewers"){
ids.sort((a,b) => (statusCache[b]?.viewers||0) - (statusCache[a]?.viewers||0));
} else if(sortMode === "name"){
ids.sort((a,b) => a.localeCompare(b));
}

return ids;

}

function onSort(val){
sortMode = val;
render();
}

function render(){

renderStats();

const box = document.getElementById("list");
box.innerHTML = "";

const ids = sortedChannelIds();

for(const id of ids){

const isOn = !!statusCache[id]?.active;
const current = statusCache[id]?.viewers || 0;
const total = statusCache[id]?.total || 0;

box.innerHTML += \`
<tr class="\${isOn ? 'rowOn' : ''}">
<td>
<div class="statusCell">
<span class="dot \${isOn ? 'on' : ''}"></span>
<span class="statusTxt \${isOn ? 'on' : 'off'}">\${isOn ? 'يبث' : 'متوقف'}</span>
</div>
</td>
<td class="chName">\${id}</td>
<td class="urlCell" title="\${channelsCache[id].input || ''}">\${channelsCache[id].input || '—'}</td>
<td class="urlCell" title="\${channelsCache[id].output || ''}">\${channelsCache[id].output || '—'}</td>
<td class="numCell">\${current}</td>
<td class="numCell dim">\${total}</td>
<td>
<div class="rowBtns">
<button class="start" onclick="start('\${id}')">▶</button>
<button class="stop" onclick="stop('\${id}')">⏹</button>
<button onclick="editChannel('\${id}')">✏</button>
<button onclick="showLogs('\${id}')">📄</button>
<button class="del" onclick="del('\${id}')">🗑</button>
</div>
</td>
</tr>
\`;

}

if(ids.length === 0){
box.innerHTML = '<tr><td colspan="7" style="padding:24px;text-align:center;color:var(--text-3)">لا يوجد قنوات مطابقة</td></tr>';
}

}

async function load(){
const ch = await fetch("/channels");
channelsCache = await ch.json();

const st = await fetch("/status");
statusCache = await st.json();

render();
}

async function loadEvents(){
const r = await fetch("/events");
eventsCache = await r.json();
renderEvents();
renderStats();
}

function eventLabel(type){
if(type === "start") return { icon:"▶", cls:"on", text:"تشغيل" };
if(type === "stop") return { icon:"⏹", cls:"off", text:"إيقاف" };
if(type === "exit") return { icon:"❌", cls:"off", text:"خروج غير متوقع" };
if(type === "restart") return { icon:"🔄", cls:"on", text:"إعادة تشغيل" };
return { icon:"•", cls:"", text:type };
}

function renderEvents(){

const box = document.getElementById("eventsList");
if(!box) return;

if(eventsCache.length === 0){
box.innerHTML = '<div class="evRow" style="color:var(--text-3)">لا يوجد أحداث بعد</div>';
return;
}

box.innerHTML = "";

for(const ev of eventsCache){

const lbl = eventLabel(ev.type);
const t = new Date(ev.time);
const timeStr = t.toLocaleString("ar-EG");

box.innerHTML += \`
<div class="evRow">
<span class="evIcon \${lbl.cls}">\${lbl.icon}</span>
<div class="evBody">
<span class="evChan">\${ev.id}</span> — \${lbl.text}
<div class="evMsg">\${ev.message}</div>
</div>
<span class="evTime">\${timeStr}</span>
</div>
\`;

}

}

function onSearch(val){
searchTerm = val;
render();
}

let currentLogChannel = null;

async function showLogs(id){
currentLogChannel = id;
document.getElementById("logTitle").innerText = "لوج القناة: " + id;
document.getElementById("logOverlay").classList.add("show");
await refreshLogs();
}

function closeLogs(e){
if(e && e.target !== document.getElementById("logOverlay")) return;
document.getElementById("logOverlay").classList.remove("show");
currentLogChannel = null;
}

async function refreshLogs(){
if(!currentLogChannel) return;

const r = await fetch("/logs/" + currentLogChannel);
const lines = await r.json();

const body = document.getElementById("logBody");

if(lines.length === 0){
body.innerHTML = '<div class="logLine">لا يوجد سجل بعد لهذه القناة.</div>';
return;
}

body.innerHTML = lines.map(l => {
const t = new Date(l.time).toLocaleTimeString("en-GB");
return '<div class="logLine"><span class="logTime">[' + t + ']</span>' + l.line.replace(/</g,"&lt;") + '</div>';
}).join("");

body.scrollTop = body.scrollHeight;
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

let ws;
let wsReconnectTimer;

function connectWS(){
const proto = location.protocol === "https:" ? "wss:" : "ws:";
ws = new WebSocket(proto + "//" + location.host);

ws.onopen = () => {
document.getElementById("connStatus").className = "pill conn-ok";
document.getElementById("connStatus").innerText = "🟢 متصل مباشر";
};

ws.onclose = () => {
document.getElementById("connStatus").className = "pill conn-bad";
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
renderStats();
}

}catch(e){}
};
}

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

updateClock();
setInterval(updateClock, 1000);

load();
loadEvents();
connectWS();

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
