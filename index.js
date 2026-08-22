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
<html dir="rtl">

<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,viewport-fit=cover">
<title>IPTV PRO PANEL</title>

<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Oswald:wght@400;500;600;700&family=IBM+Plex+Sans+Arabic:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">

<style>
:root{
--bg:#0A0D12;
--panel:#12161E;
--panel-2:#161B24;
--line:#232A36;
--text:#E7ECF3;
--text-dim:#7C879B;
--live:#00D18F;
--live-glow:rgba(0,209,143,.55);
--off:#FF4B5C;
--amber:#FFB020;
--signal:#3FA9F5;
--radius:10px;
}

*{
box-sizing:border-box;
}

html, body{
margin:0;
padding:0;
overflow-x:hidden;
}

body{
font-family:'IBM Plex Sans Arabic', sans-serif;
background:
radial-gradient(1200px 600px at 100% -10%, rgba(63,169,245,0.06), transparent 60%),
radial-gradient(1000px 500px at -10% 110%, rgba(0,209,143,0.05), transparent 60%),
var(--bg);
color:var(--text);
display:flex;
min-height:100vh;
min-height:100dvh;
}

.mono{
font-family:'JetBrains Mono', monospace;
}

/* ======================
   🎛️ SIDE RACK (nav)
   ====================== */

.side{
width:264px;
flex-shrink:0;
background:var(--panel);
border-right:1px solid var(--line);
padding:0;
display:flex;
flex-direction:column;
transition:transform 0.3s;
z-index:1000;
}

.brand{
display:flex;
align-items:center;
gap:10px;
padding:22px 20px;
border-bottom:1px solid var(--line);
}

.brand .tally{
width:11px;
height:11px;
border-radius:50%;
background:var(--live);
box-shadow:0 0 10px var(--live-glow);
animation:tallyPulse 2.2s ease-in-out infinite;
flex-shrink:0;
}

.brand .word{
font-family:'Oswald', sans-serif;
font-weight:600;
letter-spacing:1.5px;
font-size:17px;
line-height:1.1;
}

.brand .word small{
display:block;
font-family:'JetBrains Mono', monospace;
font-weight:400;
letter-spacing:2px;
font-size:10px;
color:var(--text-dim);
margin-top:2px;
}

.navGroup{
padding:16px 14px;
}

.navLabel{
font-family:'JetBrains Mono', monospace;
font-size:10px;
letter-spacing:1.5px;
color:var(--text-dim);
padding:0 8px 8px 8px;
text-transform:uppercase;
}

.side button{
width:100%;
padding:12px 14px;
margin-bottom:6px;
border:none;
border-right:2px solid transparent;
border-radius:6px;
cursor:pointer;
background:transparent;
color:var(--text);
font-family:'IBM Plex Sans Arabic', sans-serif;
font-weight:500;
font-size:14px;
transition:0.15s;
text-align:right;
}

.side button:hover{
background:var(--panel-2);
border-right-color:var(--signal);
}

.rackDivider{
border:0;
height:1px;
background:var(--line);
margin:10px 14px;
}

.side .bulkBtn{
font-family:'Oswald', sans-serif;
letter-spacing:0.5px;
font-weight:600;
text-align:center;
border-radius:6px;
}

.side .bulkOn{
background:rgba(0,209,143,0.12);
color:var(--live);
border:1px solid rgba(0,209,143,0.35);
}

.side .bulkOn:hover{
background:rgba(0,209,143,0.2);
}

.side .bulkOff{
background:rgba(255,75,92,0.12);
color:var(--off);
border:1px solid rgba(255,75,92,0.35);
}

.side .bulkOff:hover{
background:rgba(255,75,92,0.2);
}

/* ======================
   🖥️ MAIN
   ====================== */

.main{
flex:1;
padding:24px 28px;
overflow:auto;
width:100%;
min-width:0;
}

.pageTitle{
font-family:'Oswald', sans-serif;
font-size:13px;
letter-spacing:2px;
color:var(--text-dim);
text-transform:uppercase;
margin:0 0 16px 0;
padding-bottom:12px;
border-bottom:1px solid var(--line);
}

.grid{
display:grid;
grid-template-columns:repeat(auto-fill,minmax(320px,1fr));
gap:16px;
}

/* ======================
   📺 MONITOR TILE (channel card)
   ====================== */

.card{
background:var(--panel);
padding:0;
border-radius:var(--radius);
border:1px solid var(--line);
transition:0.15s;
position:relative;
overflow:hidden;
}

.card:hover{
border-color:#2E3746;
transform:translateY(-2px);
}

.tileHead{
display:flex;
align-items:center;
justify-content:space-between;
padding:14px 16px;
border-bottom:1px solid var(--line);
background:var(--panel-2);
}

.tileHead .idWrap{
display:flex;
align-items:center;
gap:10px;
min-width:0;
}

.tallyDot{
width:10px;
height:10px;
border-radius:50%;
flex-shrink:0;
background:var(--off);
box-shadow:0 0 0 3px rgba(255,75,92,0.12);
}

.tallyDot.on{
background:var(--live);
box-shadow:0 0 8px var(--live-glow);
animation:tallyPulse 1.8s ease-in-out infinite;
}

.tileHead h3{
margin:0;
font-family:'JetBrains Mono', monospace;
font-size:14px;
font-weight:600;
letter-spacing:0.5px;
color:var(--text);
overflow:hidden;
text-overflow:ellipsis;
white-space:nowrap;
direction:ltr;
text-align:left;
}

.statusPill{
font-family:'Oswald', sans-serif;
font-size:11px;
font-weight:600;
letter-spacing:1px;
padding:4px 10px;
border-radius:4px;
flex-shrink:0;
}

.statusPill.on{
background:rgba(0,209,143,0.12);
color:var(--live);
border:1px solid rgba(0,209,143,0.35);
}

.statusPill.off{
background:rgba(255,75,92,0.12);
color:var(--off);
border:1px solid rgba(255,75,92,0.35);
}

.tileBody{
padding:14px 16px;
}

.readout{
display:flex;
gap:14px;
margin-bottom:12px;
}

.readoutBox{
flex:1;
background:var(--panel-2);
border:1px solid var(--line);
border-radius:8px;
padding:8px 12px;
}

.readoutBox .rLbl{
font-family:'JetBrains Mono', monospace;
font-size:9px;
letter-spacing:1.5px;
color:var(--text-dim);
text-transform:uppercase;
display:block;
margin-bottom:3px;
}

.readoutBox .rVal{
font-family:'JetBrains Mono', monospace;
font-size:17px;
font-weight:600;
color:var(--signal);
}

.techLine{
margin-bottom:8px;
}

.techLine .tLbl{
font-family:'JetBrains Mono', monospace;
font-size:9px;
letter-spacing:1.5px;
color:var(--text-dim);
text-transform:uppercase;
display:block;
margin-bottom:3px;
}

.techLine .tVal{
font-family:'JetBrains Mono', monospace;
font-size:11px;
color:#AEB8C9;
word-break:break-all;
direction:ltr;
text-align:left;
display:block;
line-height:1.5;
}

.btns{
display:flex;
gap:6px;
margin-top:14px;
flex-wrap:wrap;
}

button{
padding:9px 10px;
border:none;
border-radius:6px;
cursor:pointer;
font-family:'IBM Plex Sans Arabic', sans-serif;
font-weight:600;
font-size:12.5px;
transition:0.15s;
color:var(--text);
}

button:hover{
filter:brightness(1.15);
}

.start{background:rgba(0,209,143,0.14);color:var(--live);border:1px solid rgba(0,209,143,0.3)}
.stop{background:rgba(255,75,92,0.14);color:var(--off);border:1px solid rgba(255,75,92,0.3)}
.edit{background:rgba(63,169,245,0.14);color:var(--signal);border:1px solid rgba(63,169,245,0.3)}
.del{background:var(--panel-2);color:var(--text-dim);border:1px solid var(--line)}

input{
width:100%;
padding:12px;
margin-bottom:10px;
border-radius:8px;
border:1px solid var(--line);
background:var(--panel-2);
color:var(--text);
outline:none;
font-family:'IBM Plex Sans Arabic', sans-serif;
font-size:14px;
}

input:focus{
border-color:var(--signal);
}

h2{
font-family:'Oswald', sans-serif;
letter-spacing:0.5px;
font-weight:600;
}

hr{
border:0;
height:1px;
background:var(--line);
margin:10px 0;
}

#connStatus{
position:fixed;
top:14px;
left:14px;
padding:6px 12px;
border-radius:6px;
font-family:'JetBrains Mono', monospace;
font-size:11px;
letter-spacing:0.5px;
font-weight:600;
z-index:999;
}

.conn-ok{background:rgba(0,209,143,0.12);color:var(--live);border:1px solid rgba(0,209,143,0.35)}
.conn-bad{background:rgba(255,75,92,0.12);color:var(--off);border:1px solid rgba(255,75,92,0.35)}

/* ======================
   📊 CONSOLE STAT STRIP
   ====================== */

.statsBar{
display:grid;
grid-template-columns:repeat(auto-fit,minmax(150px,1fr));
gap:0;
margin-bottom:20px;
background:var(--panel);
border:1px solid var(--line);
border-radius:var(--radius);
overflow:hidden;
}

.statCard{
padding:16px 18px;
text-align:right;
border-left:1px solid var(--line);
position:relative;
}

.statCard:last-child{
border-left:none;
}

.statCard .num{
font-family:'JetBrains Mono', monospace;
font-size:26px;
font-weight:700;
display:block;
margin-bottom:4px;
line-height:1;
}

.statCard .lbl{
font-family:'Oswald', sans-serif;
font-size:10.5px;
letter-spacing:1px;
color:var(--text-dim);
text-transform:uppercase;
}

.statCard.live .num{color:var(--live)}
.statCard.off .num{color:var(--off)}
.statCard.info .num{color:var(--signal)}
.statCard.warn .num{color:var(--amber)}

.toolbar{
display:flex;
gap:10px;
margin-bottom:18px;
flex-wrap:wrap;
}

.toolbar input{
flex:1 1 260px;
margin-bottom:0;
}

.toolbar select{
flex:0 1 260px;
padding:12px;
border-radius:8px;
border:1px solid var(--line);
background:var(--panel-2);
color:var(--text);
outline:none;
cursor:pointer;
font-family:'IBM Plex Sans Arabic', sans-serif;
font-size:13.5px;
}

/* ======================
   📜 EVENT LOG TIMELINE
   ====================== */

.eventRow{
display:flex;
align-items:flex-start;
gap:12px;
padding:12px 16px;
background:var(--panel);
border:1px solid var(--line);
border-radius:8px;
margin-bottom:8px;
}

.eventRow .evDot{
width:8px;
height:8px;
border-radius:50%;
margin-top:5px;
flex-shrink:0;
}

.eventRow .evDot.live{background:var(--live);box-shadow:0 0 6px var(--live-glow)}
.eventRow .evDot.off{background:var(--off)}

.eventRow .evMain{
flex:1;
min-width:0;
}

.eventRow .evTitle{
font-size:13.5px;
font-weight:600;
margin-bottom:2px;
}

.eventRow .evMsg{
font-size:12.5px;
color:var(--text-dim);
}

.eventRow .evTime{
font-family:'JetBrains Mono', monospace;
font-size:10.5px;
color:var(--text-dim);
white-space:nowrap;
flex-shrink:0;
}

/* ======================
   📄 LOG MODAL (terminal)
   ====================== */

.logOverlay{
display:none;
position:fixed;
inset:0;
background:rgba(5,7,10,0.8);
backdrop-filter:blur(2px);
z-index:2000;
align-items:center;
justify-content:center;
padding:20px;
}

.logOverlay.show{
display:flex;
}

.logModal{
background:#0D1117;
border:1px solid var(--line);
border-radius:var(--radius);
width:100%;
max-width:800px;
max-height:80vh;
display:flex;
flex-direction:column;
overflow:hidden;
box-shadow:0 20px 60px rgba(0,0,0,0.5);
}

.logHeader{
display:flex;
justify-content:space-between;
align-items:center;
padding:12px 16px;
border-bottom:1px solid var(--line);
background:var(--panel);
}

.logHeader h3{
margin:0;
font-family:'JetBrains Mono', monospace;
font-size:13px;
font-weight:600;
color:var(--live);
}

.logHeader button{
padding:7px 12px;
font-size:11.5px;
}

.logBody{
padding:12px 16px;
overflow-y:auto;
font-family:'JetBrains Mono', monospace;
font-size:11.5px;
color:#8FE3C0;
background:#0D1117;
white-space:pre-wrap;
word-break:break-all;
direction:ltr;
text-align:left;
line-height:1.6;
}

.logBody .logLine{
padding:2px 0;
border-bottom:1px solid #161B22;
}

.logBody .logTime{
color:var(--signal);
margin-left:8px;
}

@keyframes tallyPulse{
0%, 100%{opacity:1}
50%{opacity:0.45}
}

/* ======================
   📱 MOBILE / RESPONSIVE
   ====================== */

.menuBtn{
display:none;
position:fixed;
top:12px;
right:12px;
z-index:1100;
width:42px;
height:42px;
border-radius:8px;
border:1px solid var(--line);
background:var(--panel);
color:var(--signal);
font-size:18px;
cursor:pointer;
align-items:center;
justify-content:center;
}

.overlay{
display:none;
position:fixed;
inset:0;
background:rgba(0,0,0,0.55);
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
gap:12px;
}

.btns button{
flex:1 1 40%;
font-size:12px;
padding:9px 6px;
}

.toolbar{
flex-direction:column;
}

.toolbar select{
flex:1 1 auto;
}

.statsBar{
grid-template-columns:repeat(2,1fr);
}

.statCard{
padding:12px 14px;
border-bottom:1px solid var(--line);
}

.statCard .num{
font-size:20px;
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

<div class="brand">
<div class="tally"></div>
<div class="word">IPTV CONTROL<small>BROADCAST CONSOLE</small></div>
</div>

<div class="navGroup">

<div class="navLabel">التنقل</div>

<button onclick="show('channels');closeMenu()">📺 القنوات</button>
<button onclick="show('add');closeMenu()">➕ إضافة قناة</button>
<button onclick="show('events');closeMenu()">📜 سجل الأحداث</button>

<div class="rackDivider"></div>

<div class="navLabel">تحكم جماعي</div>

<button class="bulkBtn bulkOn" onclick="startAll()">▶ تشغيل الكل</button>
<button class="bulkBtn bulkOff" onclick="stopAll()">⏹ إيقاف الكل</button>

</div>

</div>

<div class="main">

<div id="channels">

<div class="pageTitle">القنوات المباشرة</div>

<div id="statsBar" class="statsBar"></div>

<div class="toolbar">
<input id="searchBox" placeholder="🔍 بحث عن قناة..." oninput="onSearch(this.value)">
<select id="sortSelect" onchange="onSort(this.value)">
<option value="status">🔀 ترتيب: الحالة (شغال أولاً)</option>
<option value="viewers">👁️ ترتيب: الأعلى مشاهدين</option>
<option value="name">🔤 ترتيب: الاسم</option>
</select>
</div>

<div id="list" class="grid"></div>
</div>

<div id="add" style="display:none">

<div class="pageTitle">إضافة قناة جديدة</div>

<input id="id" placeholder="Channel ID">
<input id="input" placeholder="Input URL">
<input id="output" placeholder="RTMP Output">

<button class="start" onclick="addChannel()">➕ إضافة القناة</button>

</div>

<div id="events" style="display:none">

<div class="pageTitle">سجل الأحداث</div>

<div id="eventsList"></div>

</div>

</div>

<div class="logOverlay" id="logOverlay" onclick="closeLogs(event)">
<div class="logModal" onclick="event.stopPropagation()">

<div class="logHeader">
<h3 id="logTitle">📄 اللوج</h3>
<button class="del" onclick="closeLogs()">✕ إغلاق</button>
</div>

<div class="logBody" id="logBody"></div>

<div class="logHeader">
<button class="edit" onclick="refreshLogs()">🔄 تحديث</button>
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
document.getElementById("channels").style.display="none";
document.getElementById("add").style.display="none";
document.getElementById("events").style.display="none";
document.getElementById(id).style.display="block";
}

function renderStats(){

const box = document.getElementById("statsBar");
if(!box) return;

let live = 0, off = 0, totalViewers = 0;

for(const id in channelsCache){
if(statusCache[id]?.active){
live++;
totalViewers += statusCache[id]?.viewers || 0;
} else {
off++;
}
}

const restarts = eventsCache.filter(e => e.type === "restart").length;

box.innerHTML = \`
<div class="statCard live">
<span class="num mono">\${live}</span>
<span class="lbl">ON AIR</span>
</div>
<div class="statCard off">
<span class="num mono">\${off}</span>
<span class="lbl">OFFLINE</span>
</div>
<div class="statCard info">
<span class="num mono">\${totalViewers}</span>
<span class="lbl">إجمالي المشاهدين</span>
</div>
<div class="statCard warn">
<span class="num mono">\${restarts}</span>
<span class="lbl">إعادة تشغيل تلقائي</span>
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

const current = statusCache[id]?.viewers || 0;
const isOn = !!statusCache[id]?.active;

box.innerHTML += \`

<div class="card">

<div class="tileHead">
<div class="idWrap">
<div class="tallyDot \${isOn ? 'on' : ''}"></div>
<h3>\${id}</h3>
</div>
<div class="statusPill \${isOn ? 'on' : 'off'}">\${isOn ? 'ON AIR' : 'OFFLINE'}</div>
</div>

<div class="tileBody">

<div class="readout">
<div class="readoutBox">
<span class="rLbl">الحالي</span>
<span class="rVal">\${current}</span>
</div>
<div class="readoutBox">
<span class="rLbl">الإجمالي</span>
<span class="rVal">\${statusCache[id]?.total || 0}</span>
</div>
</div>

<div class="techLine">
<span class="tLbl">SRC · INPUT</span>
<span class="tVal">\${channelsCache[id].input || '—'}</span>
</div>

<div class="techLine">
<span class="tLbl">DST · OUTPUT</span>
<span class="tVal">\${channelsCache[id].output || '—'}</span>
</div>

<div class="btns">
<button class="start" onclick="start('\${id}')">▶ تشغيل</button>
<button class="stop" onclick="stop('\${id}')">⏹ إيقاف</button>
<button class="edit" onclick="editChannel('\${id}')">✏ تعديل</button>
<button class="edit" onclick="showLogs('\${id}')">📄 اللوج</button>
<button class="del" onclick="del('\${id}')">🗑 حذف</button>
</div>

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
renderStats();
}

function eventLabel(type){
if(type === "start") return { icon:"▶", dot:"live", text:"تشغيل" };
if(type === "stop") return { icon:"⏹", dot:"off", text:"إيقاف" };
if(type === "exit") return { icon:"❌", dot:"off", text:"خروج غير متوقع" };
if(type === "restart") return { icon:"🔄", dot:"live", text:"إعادة تشغيل" };
return { icon:"•", dot:"", text:type };
}

function renderEvents(){

const box = document.getElementById("eventsList");
if(!box) return;

if(eventsCache.length === 0){
box.innerHTML = '<div class="techLine"><span class="tVal">لا يوجد أحداث بعد</span></div>';
return;
}

box.innerHTML = "";

for(const ev of eventsCache){

const lbl = eventLabel(ev.type);
const t = new Date(ev.time);
const timeStr = t.toLocaleString("ar-EG");

box.innerHTML += \`
<div class="eventRow">
<div class="evDot \${lbl.dot}"></div>
<div class="evMain">
<div class="evTitle">\${lbl.icon} \${lbl.text} — <span class="mono">\${ev.id}</span></div>
<div class="evMsg">\${ev.message}</div>
</div>
<div class="evTime">\${timeStr}</div>
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
document.getElementById("logTitle").innerText = "📄 لوج القناة: " + id;
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
renderStats();
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
