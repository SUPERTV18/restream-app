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
<link href="https://fonts.googleapis.com/css2?family=Cairo:wght@400;600;700;900&family=IBM+Plex+Mono:wght@400;500;600&display=swap" rel="stylesheet">

<style>

:root{
--void:#121316;
--panel:#1b1d22;
--panel-raised:#23262d;
--panel-inset:#0e0f12;
--hairline:#2c2f37;
--hairline-soft:#20232a;
--text:#ece8e0;
--text-dim:#9a968d;
--text-faint:#605c54;
--tally:#e8402f;
--tally-dim:#3a1d16;
--tally-glow:rgba(232,64,47,0.45);
--amber:#ff9f1c;
--gold:#c9a227;
--off:#55524c;
}

*{
box-sizing:border-box;
}

html, body{
margin:0;
padding:0;
overflow-x:hidden;
}

@media (prefers-reduced-motion: reduce){
*{ animation-duration:0.01ms !important; animation-iteration-count:1 !important; transition-duration:0.01ms !important; }
}

body{
font-family:'Cairo',Arial,sans-serif;
background:
  radial-gradient(1200px 500px at 15% -10%, rgba(232,64,47,0.05), transparent),
  radial-gradient(900px 500px at 100% 0%, rgba(201,162,39,0.05), transparent),
  var(--void);
color:var(--text);
display:flex;
min-height:100vh;
min-height:100dvh;
}

::selection{ background:var(--tally); color:#fff; }

.side{
width:250px;
flex-shrink:0;
background:var(--panel);
padding:0;
border-right:1px solid var(--hairline);
transition:transform 0.3s;
z-index:1000;
display:flex;
flex-direction:column;
}

.side .rackHead{
padding:22px 20px 16px 20px;
border-bottom:1px solid var(--hairline);
}

.side .rackHead .eyebrow{
font-family:'IBM Plex Mono',monospace;
font-size:10px;
letter-spacing:2px;
color:var(--tally);
display:block;
margin-bottom:6px;
}

.side h2{
margin:0;
font-size:19px;
font-weight:900;
letter-spacing:0.3px;
color:var(--text);
}

.side .rackBody{
padding:18px 16px;
display:flex;
flex-direction:column;
gap:6px;
}

.side .rackLabel{
font-family:'IBM Plex Mono',monospace;
font-size:10px;
letter-spacing:1.5px;
color:var(--text-faint);
padding:10px 6px 4px 6px;
}

.side button{
width:100%;
padding:13px 14px;
margin-bottom:2px;
border:1px solid transparent;
border-radius:8px;
cursor:pointer;
background:transparent;
color:var(--text-dim);
font-weight:600;
font-family:'Cairo',Arial,sans-serif;
transition:0.15s;
font-size:14.5px;
text-align:right;
}

.side button:hover{
background:var(--panel-raised);
color:var(--text);
border-color:var(--hairline);
}

.side .rackFoot{
margin-top:auto;
padding:16px;
border-top:1px solid var(--hairline);
display:flex;
flex-direction:column;
gap:8px;
}

.side .rackFoot button.go{background:rgba(201,162,39,0.08);color:var(--gold);border:1px solid rgba(201,162,39,0.25);font-weight:700;}
.side .rackFoot button.go:hover{background:rgba(201,162,39,0.16);}
.side .rackFoot button.stop2{background:rgba(232,64,47,0.08);color:var(--tally);border:1px solid rgba(232,64,47,0.25);font-weight:700;}
.side .rackFoot button.stop2:hover{background:rgba(232,64,47,0.16);}

.main{
flex:1;
padding:26px 30px;
overflow:auto;
width:100%;
min-width:0;
}

.pageHead{
display:flex;
align-items:baseline;
justify-content:space-between;
margin-bottom:20px;
flex-wrap:wrap;
gap:10px;
}

.pageHead h1{
font-size:15px;
letter-spacing:2px;
font-family:'IBM Plex Mono',monospace;
color:var(--text-faint);
margin:0;
font-weight:500;
text-transform:uppercase;
}

.grid{
display:grid;
grid-template-columns:repeat(auto-fill,minmax(320px,1fr));
gap:16px;
}

/* ---------- monitor tile ---------- */

.card{
background:var(--panel);
border-radius:10px;
border:1px solid var(--hairline);
transition:0.2s;
position:relative;
overflow:hidden;
}

.card:hover{
border-color:#333c4d;
transform:translateY(-2px);
}

.card.isLive{
border-color:rgba(232,64,47,0.35);
}

.screen{
position:relative;
padding:16px 18px 14px 18px;
background:
  repeating-linear-gradient(180deg, rgba(255,255,255,0.014) 0px, rgba(255,255,255,0.014) 1px, transparent 1px, transparent 3px),
  var(--panel-inset);
border-bottom:1px solid var(--hairline);
}

.tally{
position:absolute;
top:14px;
left:16px;
width:9px;
height:9px;
border-radius:50%;
background:var(--off);
}

.card.isLive .tally{
background:var(--tally);
box-shadow:0 0 10px 2px var(--tally-glow);
animation:tallyPulse 2s ease-in-out infinite;
}

@keyframes tallyPulse{
0%,100%{ opacity:1; }
50%{ opacity:0.45; }
}

.screen h3{
margin:0;
padding-left:20px;
font-family:'IBM Plex Mono',monospace;
font-size:17px;
font-weight:600;
letter-spacing:0.5px;
color:var(--text);
}

.chState{
margin-top:8px;
padding-left:20px;
font-family:'IBM Plex Mono',monospace;
font-size:11px;
letter-spacing:1.5px;
font-weight:600;
}

.live{color:var(--tally)}
.off{color:var(--text-faint)}

.readout{
display:flex;
gap:22px;
margin-top:12px;
padding-left:20px;
}

.readout .rItem .rNum{
font-family:'IBM Plex Mono',monospace;
font-size:20px;
font-weight:600;
color:var(--gold);
display:block;
line-height:1.1;
}

.readout .rItem.dim .rNum{ color:var(--text-dim); }

.readout .rItem .rLbl{
font-size:10.5px;
color:var(--text-faint);
letter-spacing:0.5px;
}

.cardBody{
padding:14px 18px 16px 18px;
}

.info{
margin-top:0;
margin-bottom:9px;
font-size:12px;
color:var(--text-dim);
word-break:break-all;
line-height:1.5;
}

.info b{
display:block;
font-family:'IBM Plex Mono',monospace;
font-size:9.5px;
letter-spacing:1.5px;
color:var(--text-faint);
font-weight:500;
margin-bottom:2px;
}

.info span{
font-family:'IBM Plex Mono',monospace;
font-size:11.5px;
direction:ltr;
display:block;
text-align:left;
color:#aeb4c2;
}

.btns{
display:flex;
gap:6px;
margin-top:10px;
flex-wrap:wrap;
}

button{
padding:9px 10px;
border:1px solid transparent;
border-radius:7px;
cursor:pointer;
font-weight:700;
font-family:'Cairo',Arial,sans-serif;
font-size:12.5px;
transition:0.15s;
letter-spacing:0.2px;
}

button:hover{filter:brightness(1.12)}
button:active{transform:translateY(1px)}

.start{background:rgba(201,162,39,0.12);color:var(--gold);border-color:rgba(201,162,39,0.3)}
.stop{background:rgba(232,64,47,0.12);color:var(--tally);border-color:rgba(232,64,47,0.3)}
.edit{background:var(--panel-raised);color:var(--text-dim);border-color:var(--hairline)}
.del{background:transparent;color:var(--text-faint);border-color:var(--hairline)}

input{
width:100%;
padding:12px 14px;
margin-bottom:10px;
border-radius:8px;
border:1px solid var(--hairline);
background:var(--panel-inset);
color:var(--text);
outline:none;
font-family:'IBM Plex Mono',monospace;
font-size:13px;
direction:ltr;
text-align:left;
}

input::placeholder{ color:var(--text-faint); font-family:'Cairo',Arial,sans-serif; }

input:focus{
border-color:var(--gold);
}

h3{margin:0;color:var(--text)}

hr{
border:0;
height:1px;
background:var(--hairline);
margin:14px 0;
}

#connStatus{
position:fixed;
top:14px;
left:14px;
padding:7px 14px;
border-radius:20px;
font-size:11px;
font-family:'IBM Plex Mono',monospace;
letter-spacing:0.5px;
font-weight:500;
z-index:999;
border:1px solid;
}

.conn-ok{background:rgba(201,162,39,0.08);color:var(--gold);border-color:rgba(201,162,39,0.3)}
.conn-bad{background:rgba(232,64,47,0.08);color:var(--tally);border-color:rgba(232,64,47,0.3)}

/* ---------- master control strip ---------- */

.statsBar{
display:grid;
grid-template-columns:repeat(auto-fit,minmax(160px,1fr));
gap:1px;
margin-bottom:22px;
background:var(--hairline);
border:1px solid var(--hairline);
border-radius:10px;
overflow:hidden;
}

.statCard{
background:var(--panel);
padding:16px 18px;
text-align:right;
position:relative;
}

.statCard .num{
font-family:'IBM Plex Mono',monospace;
font-size:30px;
font-weight:600;
display:block;
margin-bottom:2px;
line-height:1;
}

.statCard .lbl{
font-size:10.5px;
letter-spacing:0.5px;
color:var(--text-faint);
}

.statCard.live .num{color:var(--tally)}
.statCard.off .num{color:var(--text-dim)}
.statCard.info .num{color:var(--gold)}
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
flex:0 1 240px;
padding:12px 14px;
border-radius:8px;
border:1px solid var(--hairline);
background:var(--panel-inset);
color:var(--text-dim);
outline:none;
cursor:pointer;
font-family:'Cairo',Arial,sans-serif;
font-size:13px;
}

/* ======================
   📄 LOG MODAL
   ====================== */

.logOverlay{
display:none;
position:fixed;
inset:0;
background:rgba(4,5,8,0.75);
z-index:2000;
align-items:center;
justify-content:center;
padding:20px;
backdrop-filter:blur(2px);
}

.logOverlay.show{
display:flex;
}

.logModal{
background:var(--panel);
border:1px solid var(--hairline);
border-radius:12px;
width:100%;
max-width:800px;
max-height:80vh;
display:flex;
flex-direction:column;
overflow:hidden;
}

.logHeader{
display:flex;
justify-content:space-between;
align-items:center;
padding:14px 18px;
border-bottom:1px solid var(--hairline);
background:var(--panel-raised);
}

.logHeader h3{
margin:0;
font-size:14px;
font-family:'IBM Plex Mono',monospace;
}

.logHeader button{
padding:8px 14px;
color:var(--text);
}

.logBody{
padding:14px 18px;
overflow-y:auto;
font-family:'IBM Plex Mono',monospace;
font-size:11.5px;
color:var(--text-dim);
white-space:pre-wrap;
word-break:break-all;
direction:ltr;
text-align:left;
background:var(--panel-inset);
}

.logBody .logLine{
padding:4px 0;
border-bottom:1px solid var(--hairline-soft);
}

.logBody .logTime{
color:var(--gold);
margin-left:8px;
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
width:44px;
height:44px;
border-radius:10px;
border:1px solid var(--hairline);
background:var(--panel);
color:var(--text);
font-size:19px;
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
gap:14px;
}

.btns button{
flex:1 1 40%;
font-size:12.5px;
padding:10px 6px;
}

.toolbar{
flex-direction:column;
}

.toolbar select{
flex:1 1 auto;
}

.statsBar{
grid-template-columns:repeat(2,1fr);
gap:1px;
}

.statCard{
padding:13px 14px;
}

.statCard .num{
font-size:22px;
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

<div class="rackHead">
<span class="eyebrow">MASTER CONTROL</span>
<h2>📡 IPTV PRO</h2>
</div>

<div class="rackBody">
<div class="rackLabel">التنقل</div>
<button onclick="show('channels');closeMenu()">📺 القنوات</button>
<button onclick="show('add');closeMenu()">➕ إضافة قناة</button>
<button onclick="show('events');closeMenu()">📜 سجل الأحداث</button>
</div>

<div class="rackFoot">
<button class="go" onclick="startAll()">▶ تشغيل الكل</button>
<button class="stop2" onclick="stopAll()">⏹ إيقاف الكل</button>
</div>

</div>

<div class="main">

<div id="channels">

<div class="pageHead">
<h1>حائط المراقبة — Monitor Wall</h1>
</div>

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

<div class="logOverlay" id="logOverlay" onclick="closeLogs(event)">
<div class="logModal" onclick="event.stopPropagation()">

<div class="logHeader">
<h3 id="logTitle">📄 اللوج</h3>
<button onclick="closeLogs()" style="background:#555">✕ إغلاق</button>
</div>

<div class="logBody" id="logBody"></div>

<div class="logHeader">
<button onclick="refreshLogs()" style="background:#3498db">🔄 تحديث</button>
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
<span class="num">\${live}</span>
<span class="lbl">ON AIR — قنوات شغالة</span>
</div>
<div class="statCard off">
<span class="num">\${off}</span>
<span class="lbl">OFFLINE — قنوات متوقفة</span>
</div>
<div class="statCard info">
<span class="num">\${totalViewers}</span>
<span class="lbl">LIVE VIEWERS — المشاهدون الآن</span>
</div>
<div class="statCard warn">
<span class="num">\${restarts}</span>
<span class="lbl">AUTO-RESTARTS — إعادة التشغيل</span>
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

const isLive = !!statusCache[id]?.active;
const total = statusCache[id]?.total || 0;

box.innerHTML += \`

<div class="card \${isLive ? 'isLive' : ''}">

<div class="screen">
<div class="tally"></div>
<h3>\${id}</h3>
<div class="chState \${isLive ? 'live' : 'off'}">\${isLive ? '● ON AIR' : '○ OFFLINE'}</div>

<div class="readout">
<div class="rItem \${isLive ? '' : 'dim'}">
<span class="rNum">\${current}</span>
<span class="rLbl">مشاهد الآن</span>
</div>
<div class="rItem dim">
<span class="rNum">\${total}</span>
<span class="rLbl">إجمالي</span>
</div>
</div>
</div>

<div class="cardBody">

<div class="info">
<b>INPUT</b>
<span>\${channelsCache[id].input || '—'}</span>
</div>

<div class="info">
<b>OUTPUT</b>
<span>\${channelsCache[id].output || '—'}</span>
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
