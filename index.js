import express from "express";
import { spawn } from "child_process";
import { WebSocketServer } from "ws";

const app = express();
app.use(express.json());

// ======================
// 🎯 STATE
// ======================
let ffmpegProcesses = {};

// تتبع هل القناة اتوقفت يدويًا (لمنع إعادة التشغيل التلقائي في الحالة دي)
let manuallyStopped = {};

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

// ======================
// 📊 مقاييس حقيقية (وقت بث فعلي + بت ريت فعلي من ffmpeg)
// ======================
let liveSince = {};       // id -> timestamp بداية الجلسة الحالية (لو شغالة)
let totalOnairMs = {};    // id -> إجمالي وقت البث التراكمي بالميلي ثانية
let lastBitrateKbps = {}; // id -> آخر بت ريت حقيقي اتقرأ من ffmpeg

// ======================
// 👁️ مشاهدين حقيقيين (مبنيين على طلبات فعلية وصلت لسيرفرنا عن طريق /watch/:id)
// ======================
let currentViewerLog = {}; // id -> Map(ip -> آخر وقت طلب)
let totalViewerSet = {};   // id -> Set(كل الـ IPs اللي طلبت أي وقت)
const VIEWER_WINDOW_MS = 20000; // أي IP ماطلبش خلال آخر 20 ثانية بيتشال من "الحالي"

// تنظيف دوري لقائمة المشاهدين الحاليين
setInterval(() => {
  const now = Date.now();
  for (const id in currentViewerLog) {
    for (const [ip, lastSeen] of currentViewerLog[id]) {
      if (now - lastSeen > VIEWER_WINDOW_MS) {
        currentViewerLog[id].delete(ip);
      }
    }
  }
}, 5000);

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
// كل قناة ممكن يكون ليها: input, output, logo (رابط صورة), category (تصنيف)
// ======================
const channels = {
  ch4k: {
    input: "http://195.182.16.45:8080/live/omar777/01103978590/460864.ts",
    output: "rtmp://live.twitch.tv/app/live_151597255_5HndsveAXExMraoT8RGtn23qCKcVx0",
    logo: "logo4kh.png",
    category: "",
    watchUrl: ""
  },
  ch1: {
    input: "https://163.ostv.info/krikar/krikar/652333?token=ShJcU0BbQQNHDgxcBwYDCVsBAwdTV1FYCVdTAQABBAtUCAAHCwZTCAAbGBpEF0dcWF1vWQIXXlIJWlcFVEgRR0JVRm1aV0EDRwgBCgRWDAkbHBJED1gBQwtTVQFQXQQKBwUHHhFDCl1HA1pNWw8ZG1xIRFUUWwUNbgYHQQwHVhALXkFeXx9BVgtmUF1aAltdGwoSAUQZRghCEkANCxFfXh0SVltHQQJNABsOVkIPWRUbU19FCEEWGBNYQH40Rh8QVEhAV11AClYLGw4aQxAXFRtZQ28UUBcVQwcDWgAWEQgTABYeEV4CQTpaW1ZZBlZNUF9eQ0QPRlATTkBaCgpaRl5Ca0JaV0EDC0xYVEo=",
    output: "rtmp://vsu.okcdn.ru/input/15037126680149_16572030782037_nwbfmzaoxm",
    logo: "logo1.png",
    category: "رياضة",
    watchUrl: "https://super-tvlive.vercel.app/SUPERTV_1.m3u8"
  },
  ch2: {
    input: "https://163.ostv.info/krikar/krikar/652334?token=ShJcU0BbQQNHDgxcBwYDCVsBAwdTV1FYCVdTAQABBAtUCAAHCwZTCAAbGBpEF0dcWF1vWQIXXlIJWlcFVEgRR0JVRm1aV0EDRwgBCgRWDAkbHBJED1gBQwtTVQFQXQQKBwUHHhFDCl1HA1pNWw8ZG1xIRFUUWwUNbgYHQQwHVhALXkFeXx9BVgtmUF1aAltdGwoSAUQZRghCEkANCxFfXh0SVltHQQJNABsOVkIPWRUbU19FCEEWGBNYQH40Rh8QVEhAV11AClYLGw4aQxAXFRtZQ28UUBcVQwcDWgAWEQgTABYeEV4CQTpaW1ZZBlZNUF9eQ0QPRlATTkBaCgpaRl5Ca0JaV0EDC0xYVEo=",
    output: "rtmp://vsu.okcdn.ru/input/15037158268501_16572084062805_f6sgg23zdy",
    logo: "logo22.png",
    category: "رياضة",
    watchUrl: "https://super-tvlive.vercel.app/SUPERTV_2.m3u8"
  },
  ch3: {
    input: "https://ranapkbd.site/RANAPK33g/TVD/play.php?id=1745020",
    output: "rtmp://msk.goodgame.ru:1940/live/221746?pwd=5dfed73aa7930d86",
    logo: "logo33.png",
    category: "رياضة",
    watchUrl: "https://super-tvlive.vercel.app/SUPERTV_3.m3u8"
  },
  ch4: {
    input: "http://185.160.192.14/live/171348492752/5S6HGsea3j/255226.m3u8",
    output: "rtmp://fr.pscp.tv:80/x/ivphyvtww7k3",
    logo: "logo44.png",
    category: "رياضة",
    watchUrl: "https://super-tvlive.vercel.app/SUPERTV_4.m3u8"
  },
  ch5: {
    input: "http://185.160.192.14/live/171348492752/5S6HGsea3j/255225.m3u8",
    output: "rtmp://vsu.okcdn.ru/input/14863707479574_16379956300310_uoslkp4xrm",
    logo: "logo55.png",
    category: "",
    watchUrl: ""
  },
  ch6: {
    input: "https://ranapkbd.site/RANAPK33g/TVD/play.php?id=1745020",
    output: "rtmp://vsu.okcdn.ru/input/14901168119318_16447213341206_ssfncxg2zu",
    logo: "logo66.png",
    category: "",
    watchUrl: ""
  },
  ch7: {
    input: "https://stream.camcloud.stream/stream/97e7e9e05d4e/playlist.m3u8",
    output: "rtmp://vsu.okcdn.ru/input/13415538433558_13690939181590_flxfen3y2u",
    logo: "quran.png",
    category: "دينية",
    watchUrl: ""
  },
  ch8: {
    input: "https://man1ted.com/watch/beemax1.m3u8",
    output: "rtmp://vsu.okcdn.ru/input/9978322492950_8842256321046_oxg7ed4dcm",
    logo: "aflam.png",
    category: "أفلام",
    watchUrl: ""
  },
  ch9: {
    input: "http://185.160.192.14/live/171348492752/5S6HGsea3j/255226.m3u8",
    output: "rtmp://vsu.okcdn.ru/input/13418102398486_13695919458838_h7ihlwq5ca",
    logo: "mosalsalat.png",
    category: "مسلسلات",
    watchUrl: ""
  },
  ch10: {
    input: "http://185.160.192.14/live/171348492752/5S6HGsea3j/255225.m3u8",
    output: "rtmp://vsu.okcdn.ru/input/14994479390230_16613027809814_7sovqbfsba",
    logo: "animy.png",
    category: "أنمي",
    watchUrl: ""
  },
  ch11: {
    input: "https://ranapkbd.site/RANAPK33g/TVD/play.php?id=1745020",
    output: "rtmp://vsu.okcdn.ru/input/14994482273814_16613032593942_cmf7uzoh2q",
    logo: "kids.png",
    category: "أطفال",
    watchUrl: ""
  }
};

// ======================
// 🎬 LOGO (fallback ثابت لو القناة مالهاش لوجو محدد)
// ======================
function getLogo(id) {
  const ch = channels[id];
  if (ch && ch.logo) return ch.logo;

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

  // أي تشغيل (يدوي أو تلقائي) يلغي حالة "متوقفة يدويًا"
  manuallyStopped[id] = false;

  // بداية جلسة بث حقيقية جديدة
  liveSince[id] = Date.now();
  if (totalOnairMs[id] == null) totalOnairMs[id] = 0;
  lastBitrateKbps[id] = null;

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

"-pix_fmt", "yuv420p",

"-profile:v", "high",
"-level", "4.1",

"-b:v", "5000k",
"-maxrate", "5500k",
"-bufsize", "10000k",

"-r", "25",
"-g", "50",

    "-c:a", "aac",
    "-b:a", "128k",

    "-f", "flv",
    ch.output
  ]);

  ffmpegProcesses[id] = ffmpeg;

  ffmpeg.stderr.on("data", (d) => {
    const text = d.toString();
    console.log(`[${id}] ${text}`);

    text.split("\n").forEach(line => {
      const trimmed = line.trim();
      if (trimmed) pushLog(id, trimmed);
    });

    // قراءة البت ريت الحقيقي اللي ffmpeg بيبعته فعليًا
    const match = text.match(/bitrate=\s*([\d.]+)\s*kbits\/s/i);
    if (match) {
      lastBitrateKbps[id] = parseFloat(match[1]);
    }
  });

  ffmpeg.on("exit", () => {
    delete ffmpegProcesses[id];

    // نضيف مدة الجلسة دي لإجمالي وقت البث الحقيقي قبل ما نصفرها
    if (liveSince[id]) {
      totalOnairMs[id] = (totalOnairMs[id] || 0) + (Date.now() - liveSince[id]);
      delete liveSince[id];
    }
    lastBitrateKbps[id] = null;

    // لو اتوقفت يدويًا، منعملش إعادة تشغيل تلقائي
    if (manuallyStopped[id]) {
      console.log("⏹ EXIT (manual stop):", id);
      return;
    }

    console.log("❌ EXIT (unexpected):", id);
    logEvent(id, "exit", "توقفت القناة (خروج غير متوقع)");

    setTimeout(() => {
      if (!ffmpegProcesses[id] && !manuallyStopped[id]) {
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

  manuallyStopped[id] = true;

  if (ffmpegProcesses[id]) {
    ffmpegProcesses[id].kill("SIGKILL");
    delete ffmpegProcesses[id];
    logEvent(id, "stop", "تم إيقاف القناة يدويًا");
  }

  res.send("stopped " + id);
});

app.get("/start-all", (req, res) => {
  for (const id in channels) {
    spawnStream(id);
  }
  res.json({ ok: true });
});

app.get("/stop-all", (req, res) => {
  for (const id in channels) {
    manuallyStopped[id] = true;

    if (ffmpegProcesses[id]) {
      ffmpegProcesses[id].kill("SIGKILL");
      delete ffmpegProcesses[id];
      logEvent(id, "stop", "تم إيقاف القناة يدويًا (إيقاف الكل)");
    }
  }
  res.json({ ok: true });
});

app.get("/events", (req, res) => {
  res.json(eventLog);
});

app.get("/logs/:id", (req, res) => {
  const id = req.params.id;
  res.json(ffmpegLogs[id] || []);
});

// ======================
// 👁️ رابط المشاهدة الحقيقي (بروكسي + عداد حقيقي)
// كل طلب بيعدي من هنا بيتسجل كمشاهد حقيقي
// ======================
app.get("/watch/:id", async (req, res) => {
  const id = req.params.id;
  const ch = channels[id];

  if (!ch || !ch.watchUrl) {
    return res.status(404).send("لا يوجد رابط مشاهدة لهذه القناة");
  }

  // تسجيل المشاهد (IP الطالب) كمشاهد حقيقي حالي وإجمالي
  const ip = req.ip || req.connection.remoteAddress || "unknown";

  if (!currentViewerLog[id]) currentViewerLog[id] = new Map();
  currentViewerLog[id].set(ip, Date.now());

  if (!totalViewerSet[id]) totalViewerSet[id] = new Set();
  totalViewerSet[id].add(ip);

  try {
    const upstream = await fetch(ch.watchUrl);

    if (!upstream.ok) {
      return res.status(502).send("تعذر الوصول لرابط المشاهدة الأصلي");
    }

    const text = await upstream.text();

    // إعادة كتابة أي أسطر نسبية (segments) عشان تفضل شغالة برضو بعد المرور من عندنا
    const rewritten = text.split("\n").map(line => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) return line;
      try {
        return new URL(trimmed, ch.watchUrl).href;
      } catch (e) {
        return line;
      }
    }).join("\n");

    res.set("Content-Type", "application/vnd.apple.mpegurl");
    res.send(rewritten);

  } catch (err) {
    console.log("🔥 /watch proxy error:", err.message);
    res.status(502).send("تعذر جلب رابط المشاهدة");
  }
});

app.get("/status", (req, res) => {
  const result = {};

  for (const id in channels) {
    const active = !!ffmpegProcesses[id];
    const currentSessionMs = active && liveSince[id] ? (Date.now() - liveSince[id]) : 0;

    result[id] = {
      active,
      uptimeSeconds: Math.floor(currentSessionMs / 1000),
      totalSeconds: Math.floor(((totalOnairMs[id] || 0) + currentSessionMs) / 1000),
      bitrateKbps: active ? (lastBitrateKbps[id] || null) : null,
      currentViewers: currentViewerLog[id] ? currentViewerLog[id].size : 0,
      totalViewers: totalViewerSet[id] ? totalViewerSet[id].size : 0
    };
  }

  res.json(result);
});

app.get("/channels", (req, res) => {
  res.json(channels);
});

app.post("/channel", (req, res) => {
  const { id, input, output, logo, category, watchUrl } = req.body;

  if (!id || !input || !output)
    return res.status(400).json({ ok: false });

  channels[id] = {
    input,
    output,
    logo: logo || "",
    category: category || "",
    watchUrl: watchUrl || ""
  };

  res.json({ ok: true });
});

app.put("/channel/:id", (req, res) => {
  const id = req.params.id;

  if (!channels[id])
    return res.status(404).json({ ok: false });

  channels[id] = {
    ...channels[id],
    input: req.body.input ?? channels[id].input,
    output: req.body.output ?? channels[id].output,
    logo: req.body.logo ?? channels[id].logo,
    category: req.body.category ?? channels[id].category,
    watchUrl: req.body.watchUrl ?? channels[id].watchUrl
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
<title>لوحة تحكم القنوات</title>

<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Arabic:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/tabler-icons/3.46.0/tabler-icons.min.css">

<style>

:root{
--page:#F5F6F8;
--surface:#FFFFFF;
--surface-2:#FAFBFC;
--border:#E4E7EC;
--border-strong:#D0D5DD;
--text:#101828;
--text-2:#475467;
--text-3:#98A2B3;
--accent:#2563EB;
--accent-bg:#EFF4FF;
--success:#16A34A;
--success-bg:#EDFAF1;
--danger:#DC2626;
--danger-bg:#FDEDEC;
--warning:#D97706;
--warning-bg:#FEF6E7;
--radius:10px;
--radius-lg:14px;
}

html[data-accent="teal"]{
--page:#F1F8F5;
--surface:#FFFFFF;
--surface-2:#EFFAF5;
--border:#D7EDE3;
--border-strong:#B9DDD0;
--accent:#0F6E56;
--accent-bg:#E1F5EE;
--success:#0F6E56;
--success-bg:#E1F5EE;
}

html[data-accent="amber"]{
--page:#FBF6ED;
--surface:#FFFFFF;
--surface-2:#FDF6E9;
--border:#F0DFB8;
--border-strong:#E3C98C;
--accent:#854F0B;
--accent-bg:#FAEEDA;
--success:#854F0B;
--success-bg:#FAEEDA;
}

*{ box-sizing:border-box; }

html, body{ margin:0; padding:0; overflow-x:hidden; }

body{
font-family:'IBM Plex Sans Arabic', Arial, sans-serif;
background:var(--page);
color:var(--text);
min-height:100vh;
min-height:100dvh;
}

.mono{ font-family:'IBM Plex Mono', monospace; }

.app{ display:flex; min-height:100vh; min-height:100dvh; }

/* ---------- sidebar ---------- */

.side{
width:240px;
flex-shrink:0;
background:var(--surface);
border-left:1px solid var(--border);
display:flex;
flex-direction:column;
padding:20px 16px;
transition:transform 0.25s;
z-index:1000;
}

.brand{
display:flex;
align-items:center;
gap:10px;
padding:0 6px 20px 6px;
margin-bottom:16px;
border-bottom:1px solid var(--border);
}

.brand .mark{
width:34px;
height:34px;
border-radius:9px;
background:var(--accent-bg);
color:var(--accent);
display:flex;
align-items:center;
justify-content:center;
font-size:17px;
flex-shrink:0;
}

.brand .word{ font-weight:600; font-size:15px; }
.brand .word small{ display:block; font-size:11px; color:var(--text-3); font-weight:400; margin-top:1px; }

.navLabel{
font-size:11px;
color:var(--text-3);
padding:0 8px 6px 8px;
margin-top:10px;
font-weight:500;
}

.side button{
width:100%;
display:flex;
align-items:center;
gap:10px;
padding:10px 12px;
margin-bottom:2px;
border:none;
border-radius:var(--radius);
background:transparent;
color:var(--text-2);
font-family:'IBM Plex Sans Arabic', Arial, sans-serif;
font-weight:500;
font-size:14px;
cursor:pointer;
text-align:right;
transition:0.12s;
}

.side button i{ font-size:17px; color:var(--text-3); }

.side button:hover{ background:var(--surface-2); }
.side button.navActive{ background:var(--accent-bg); color:var(--accent); }
.side button.navActive i{ color:var(--accent); }

.side .foot{ margin-top:auto; padding-top:14px; border-top:1px solid var(--border); }

.side .goBtn{ color:var(--success); }
.side .goBtn i{ color:var(--success); }
.side .stopBtn{ color:var(--danger); }
.side .stopBtn i{ color:var(--danger); }

/* ---------- workspace ---------- */

.workspace{ flex:1; display:flex; flex-direction:column; min-width:0; }

.topbar{
height:60px;
flex-shrink:0;
background:var(--surface);
border-bottom:1px solid var(--border);
display:flex;
align-items:center;
justify-content:space-between;
padding:0 24px;
}

.topbar h1{ font-size:16px; font-weight:600; margin:0; }

.pill{
display:inline-flex;
align-items:center;
gap:6px;
padding:5px 12px;
border-radius:20px;
font-size:12px;
font-weight:500;
}

.pill i{ font-size:14px; }

.conn-ok{ background:var(--success-bg); color:var(--success); }
.conn-bad{ background:var(--danger-bg); color:var(--danger); }

.iconBtn{
width:36px;
height:36px;
display:flex;
align-items:center;
justify-content:center;
border-radius:9px;
border:1px solid var(--border);
background:var(--surface);
color:var(--text-2);
cursor:pointer;
font-size:16px;
padding:0;
}

.iconBtn:hover{ border-color:var(--border-strong); background:var(--surface-2); }

.themeMenu{
position:absolute;
top:56px;
left:24px;
background:var(--surface);
border:1px solid var(--border);
border-radius:var(--radius);
padding:8px;
display:none;
flex-direction:column;
gap:2px;
z-index:1500;
box-shadow:0 8px 24px rgba(16,24,40,0.12);
min-width:160px;
}

.themeMenu.show{ display:flex; }

.themeMenu button{
display:flex;
align-items:center;
gap:8px;
background:transparent;
border:1px solid transparent;
color:var(--text-2);
padding:8px 10px;
border-radius:8px;
font-size:13px;
font-weight:500;
cursor:pointer;
text-align:right;
width:100%;
}

.themeMenu button:hover{ background:var(--surface-2); }
.themeMenu button.themeActive{ color:var(--text); border-color:var(--border); background:var(--surface-2); }

.swatch{ width:12px; height:12px; border-radius:50%; flex-shrink:0; }

.content{ flex:1; padding:24px; overflow:auto; }

/* ---------- stat cards ---------- */

.statGrid{
display:grid;
grid-template-columns:repeat(auto-fit,minmax(160px,1fr));
gap:12px;
margin-bottom:20px;
}

.statCard{
background:var(--surface);
border:1px solid var(--border);
border-radius:var(--radius-lg);
padding:16px 18px;
}

.statCard .lbl{ font-size:12.5px; color:var(--text-2); margin-bottom:6px; display:flex; align-items:center; gap:6px; }
.statCard .lbl i{ font-size:15px; }
.statCard .num{ font-size:26px; font-weight:600; font-family:'IBM Plex Mono', monospace; }

.statCard.ok .num{ color:var(--success); }
.statCard.off .num{ color:var(--text-2); }
.statCard.accent .num{ color:var(--accent); }
.statCard.warn .num{ color:var(--warning); }

/* ---------- toolbar ---------- */

.toolbar{ display:flex; gap:10px; margin-bottom:18px; flex-wrap:wrap; }

.toolbar input, .toolbar select{
font-family:'IBM Plex Sans Arabic', Arial, sans-serif;
padding:10px 14px;
border-radius:var(--radius);
border:1px solid var(--border);
background:var(--surface);
color:var(--text);
outline:none;
font-size:13.5px;
}

.toolbar input{ flex:1 1 240px; }
.toolbar select{ flex:0 1 190px; cursor:pointer; }
.toolbar input:focus, .toolbar select:focus{ border-color:var(--accent); }

/* ---------- channel cards ---------- */

.grid{
display:grid;
grid-template-columns:repeat(auto-fill,minmax(280px,1fr));
gap:14px;
}

.card{
background:var(--surface);
border:1px solid var(--border);
border-radius:var(--radius-lg);
padding:16px;
padding-top:19px;
transition:0.15s;
position:relative;
overflow:hidden;
}

.card::before{
content:"";
position:absolute;
top:0;
left:0;
right:0;
height:3px;
background:var(--accent);
}

.card:hover{ border-color:var(--border-strong); box-shadow:0 1px 3px rgba(16,24,40,0.06); }

.cardHead{ display:flex; align-items:center; gap:10px; margin-bottom:12px; }

.logoBox{
width:38px;
height:38px;
border-radius:9px;
background:var(--surface-2);
border:1px solid var(--border);
flex-shrink:0;
overflow:hidden;
display:flex;
align-items:center;
justify-content:center;
color:var(--text-3);
font-size:16px;
}

.logoBox img{ width:100%; height:100%; object-fit:cover; }

.cardHead .titleWrap{ flex:1; min-width:0; }

.cardHead h3{
margin:0;
font-family:'IBM Plex Mono', monospace;
font-size:15px;
font-weight:600;
direction:ltr;
text-align:left;
overflow:hidden;
text-overflow:ellipsis;
white-space:nowrap;
}

.catTag{
display:inline-block;
font-size:10.5px;
color:var(--text-3);
margin-top:2px;
}

.statusPill{
font-size:11px;
font-weight:600;
padding:4px 9px;
border-radius:6px;
display:inline-flex;
align-items:center;
gap:4px;
flex-shrink:0;
}

.statusPill.on{ background:var(--success-bg); color:var(--success); }
.statusPill.off{ background:var(--surface-2); color:var(--text-3); }
.statusPill i{ font-size:8px; }

.readoutRow{ display:flex; gap:10px; margin-bottom:12px; }

.readoutBox{
flex:1;
background:var(--surface-2);
border-radius:8px;
padding:8px 10px;
}

.readoutBox .rLbl{ font-size:10px; color:var(--text-3); margin-bottom:2px; }
.readoutBox .rVal{ font-family:'IBM Plex Mono', monospace; font-size:15px; font-weight:600; }
.readoutBox:first-child .rVal{ color:var(--accent); }

.techLine{ margin-bottom:7px; }
.techLine .tLbl{ font-size:10px; color:var(--text-3); margin-bottom:2px; }
.techLine .tVal{
font-family:'IBM Plex Mono', monospace;
font-size:11px;
color:var(--text-2);
direction:ltr;
text-align:left;
display:block;
overflow:hidden;
text-overflow:ellipsis;
white-space:nowrap;
}

.btnRow{ display:flex; gap:6px; margin-top:12px; flex-wrap:wrap; }

.editField{ margin-bottom:9px; }
.editField .tLbl{ font-size:10px; color:var(--text-3); margin-bottom:3px; }
.editField input{
width:100%;
padding:7px 9px;
border-radius:7px;
border:1px solid var(--border);
background:var(--surface-2);
color:var(--text);
outline:none;
font-family:'IBM Plex Mono', monospace;
font-size:11.5px;
direction:ltr;
text-align:left;
}
.editField input:focus{ border-color:var(--accent); }

.editActions{ display:flex; gap:6px; margin-top:12px; }
.editActions button{
flex:1;
padding:9px;
border-radius:8px;
border:1px solid var(--border);
cursor:pointer;
font-weight:600;
font-size:12.5px;
}
.editActions .saveBtn{ background:var(--accent); color:#fff; border-color:var(--accent); }
.editActions .cancelBtn{ background:var(--surface-2); color:var(--text-2); }

.iBtn{
width:32px;
height:32px;
display:flex;
align-items:center;
justify-content:center;
border-radius:8px;
border:1px solid var(--border);
background:var(--surface);
color:var(--text-2);
cursor:pointer;
font-size:15px;
transition:0.12s;
}

.iBtn:hover{ border-color:var(--border-strong); background:var(--surface-2); }
.iBtn.go{ color:var(--success); }
.iBtn.stop{ color:var(--danger); }

/* ---------- add channel form ---------- */

.formCard{
max-width:460px;
background:var(--surface);
border:1px solid var(--border);
border-radius:var(--radius-lg);
padding:22px;
}

.formCard label{
display:block;
font-size:12.5px;
color:var(--text-2);
margin-bottom:6px;
margin-top:14px;
font-weight:500;
}

.formCard label:first-of-type{ margin-top:0; }

.formCard input{
width:100%;
padding:10px 13px;
border-radius:var(--radius);
border:1px solid var(--border);
background:var(--surface);
color:var(--text);
outline:none;
font-family:'IBM Plex Mono', monospace;
font-size:13px;
direction:ltr;
text-align:left;
}

.formCard input:focus{ border-color:var(--accent); }

.formCard .hint{ font-size:11px; color:var(--text-3); margin-top:4px; }

.formCard .submit{
margin-top:20px;
width:100%;
background:var(--accent);
color:#fff;
border:none;
border-radius:var(--radius);
padding:11px;
font-size:14px;
font-weight:600;
cursor:pointer;
}

/* ---------- events panel ---------- */

.panel{
background:var(--surface);
border:1px solid var(--border);
border-radius:var(--radius-lg);
overflow:hidden;
}

.evRow{
display:flex;
align-items:center;
gap:12px;
padding:12px 16px;
border-bottom:1px solid var(--border);
}

.evRow:last-child{ border-bottom:none; }

.evIcon{
width:30px;
height:30px;
border-radius:8px;
display:flex;
align-items:center;
justify-content:center;
flex-shrink:0;
font-size:14px;
}

.evIcon.on{ background:var(--success-bg); color:var(--success); }
.evIcon.off{ background:var(--danger-bg); color:var(--danger); }
.evIcon.warn{ background:var(--warning-bg); color:var(--warning); }

.evBody{ flex:1; min-width:0; }
.evTitle{ font-size:13.5px; font-weight:500; }
.evTitle .mono{ font-weight:600; }
.evMsg{ font-size:12px; color:var(--text-2); margin-top:1px; }
.evTime{ font-size:11px; color:var(--text-3); white-space:nowrap; font-family:'IBM Plex Mono', monospace; }

/* ---------- log modal ---------- */

.logOverlay{
display:none;
position:fixed;
inset:0;
background:rgba(16,24,40,0.45);
z-index:2000;
align-items:center;
justify-content:center;
padding:20px;
}

.logOverlay.show{ display:flex; }

.logModal{
background:var(--surface);
border-radius:var(--radius-lg);
width:100%;
max-width:760px;
max-height:78vh;
display:flex;
flex-direction:column;
overflow:hidden;
box-shadow:0 20px 50px rgba(16,24,40,0.25);
}

.logHeader{
display:flex;
justify-content:space-between;
align-items:center;
padding:14px 18px;
border-bottom:1px solid var(--border);
}

.logHeader h3{ margin:0; font-size:14px; font-weight:600; }

.logHeader button{
padding:7px 12px;
border-radius:var(--radius);
border:1px solid var(--border);
background:var(--surface);
color:var(--text-2);
cursor:pointer;
font-size:12.5px;
font-weight:500;
}

.logBody{
padding:14px 18px;
overflow-y:auto;
background:#0D1117;
font-family:'IBM Plex Mono', monospace;
font-size:11.5px;
color:#8FE3C0;
white-space:pre-wrap;
word-break:break-all;
direction:ltr;
text-align:left;
line-height:1.6;
}

.logBody .logLine{ padding:2px 0; border-bottom:1px solid #1c2531; }
.logBody .logTime{ color:#5B9EF2; margin-left:8px; }

/* ---------- mobile ---------- */

.menuBtn{
display:none;
position:fixed;
top:12px;
left:12px;
z-index:1100;
width:40px;
height:40px;
border-radius:9px;
border:1px solid var(--border);
background:var(--surface);
color:var(--text);
font-size:17px;
cursor:pointer;
align-items:center;
justify-content:center;
}

.overlay{ display:none; position:fixed; inset:0; background:rgba(16,24,40,0.4); z-index:999; }
.overlay.show{ display:block; }

@media (max-width: 860px){

.app{ display:block; }

.menuBtn{ display:flex; }

.side{
position:fixed;
top:0;
right:0;
height:100%;
height:100dvh;
width:250px;
transform:translateX(100%);
box-shadow:-10px 0 30px rgba(16,24,40,0.25);
}

.side.open{ transform:translateX(0); }

.topbar{ padding:0 16px 0 60px; }

.content{ padding:16px; }

.statGrid{ grid-template-columns:repeat(2,1fr); }

.grid{ grid-template-columns:1fr; }

}

</style>
</head>

<body>

<button class="menuBtn" onclick="toggleMenu()"><i class="ti ti-menu-2"></i></button>
<div class="overlay" id="overlay" onclick="closeMenu()"></div>

<div class="app">

<div class="side" id="sideMenu">

<div class="brand">
<div class="mark"><i class="ti ti-broadcast"></i></div>
<div class="word">لوحة القنوات<small>IPTV CONTROL</small></div>
</div>

<div class="navLabel">التنقل</div>

<button class="navActive" id="navChannels" onclick="show('channels');closeMenu()"><i class="ti ti-device-tv"></i>القنوات</button>
<button id="navAdd" onclick="show('add');closeMenu()"><i class="ti ti-plus"></i>إضافة قناة</button>
<button id="navEvents" onclick="show('events');closeMenu()"><i class="ti ti-list-details"></i>سجل الأحداث</button>

<div class="foot">
<button class="goBtn" onclick="startAll()"><i class="ti ti-player-play"></i>تشغيل الكل</button>
<button class="stopBtn" onclick="stopAll()"><i class="ti ti-player-stop"></i>إيقاف الكل</button>
</div>

</div>

<div class="workspace">

<div class="topbar">
<h1>القنوات المباشرة</h1>
<div style="display:flex;align-items:center;gap:10px;position:relative">
<div id="clock" class="pill mono" style="background:var(--surface-2);color:var(--text-2);border:1px solid var(--border)">--:--:--</div>
<button class="iconBtn" onclick="toggleThemeMenu(event)" title="تغيير اللون"><i class="ti ti-palette"></i></button>
<div id="connStatus" class="pill conn-bad"><i class="ti ti-loader-2"></i>اتصال...</div>

<div class="themeMenu" id="themeMenu">
<button data-a="blue" onclick="setAccent('blue')"><span class="swatch" style="background:#2563EB"></span>أزرق</button>
<button data-a="teal" onclick="setAccent('teal')"><span class="swatch" style="background:#0F6E56"></span>أخضر مائي</button>
<button data-a="amber" onclick="setAccent('amber')"><span class="swatch" style="background:#854F0B"></span>كهرماني</button>
</div>

</div>
</div>

<div class="content">

<section id="channels">

<div id="statsBar" class="statGrid"></div>

<div class="toolbar">
<input id="searchBox" placeholder="بحث عن قناة..." oninput="onSearch(this.value)">
<select id="categorySelect" onchange="onCategory(this.value)">
<option value="">كل التصنيفات</option>
</select>
<select id="sortSelect" onchange="onSort(this.value)">
<option value="status">ترتيب: الحالة</option>
<option value="uptime">ترتيب: الأطول بثًا</option>
<option value="name">ترتيب: الاسم</option>
</select>
</div>

<div id="list" class="grid"></div>

</section>

<section id="add" style="display:none">

<div class="formCard">

<label>معرف القناة (Channel ID)</label>
<input id="f_id" placeholder="ch12">

<label>رابط البث (Input URL)</label>
<input id="f_input" placeholder="rtmp:// or http://...">

<label>رابط الإخراج (RTMP Output)</label>
<input id="f_output" placeholder="rtmp://...">

<label>رابط اللوجو (Logo URL)</label>
<input id="f_logo" placeholder="https://.../logo.png">
<div class="hint">اختياري — صورة صغيرة تظهر جنب اسم القناة</div>

<label>التصنيف (Category)</label>
<input id="f_category" placeholder="أفلام، رياضة، أطفال...">
<div class="hint">اختياري — بيستخدم في الفلترة أعلى القائمة</div>

<label>رابط المشاهدة الأصلي (m3u8)</label>
<input id="f_watchUrl" placeholder="https://.../stream.m3u8">
<div class="hint">اختياري — لو ضفته، السيرفر يعد المشاهدين الحقيقيين اللي بيدخلوا</div>

<button class="submit" onclick="addChannel()">إضافة القناة</button>

</div>

</section>

<section id="events" style="display:none">

<div class="panel" id="eventsList"></div>

</section>

</div>

</div>

</div>

<div class="logOverlay" id="logOverlay" onclick="closeLogs(event)">
<div class="logModal" onclick="event.stopPropagation()">

<div class="logHeader">
<h3 id="logTitle">لوج القناة</h3>
<button onclick="closeLogs()">إغلاق</button>
</div>

<div class="logBody" id="logBody"></div>

<div class="logHeader">
<button onclick="refreshLogs()">تحديث</button>
</div>

</div>
</div>

<script>

let channelsCache = {};
let statusCache = {};
let eventsCache = [];
let searchTerm = "";
let categoryFilter = "";
let sortMode = "status";

// حالة التعديل داخل الصفحة (بدون نافذة منبثقة)
let editingId = null;
let editDraft = {};

function updateDraft(id, field, val){
if(!editDraft[id]) editDraft[id] = {};
editDraft[id][field] = val;
}

function imgFallback(el){
el.parentElement.innerHTML = '<i class="ti ti-device-tv"></i>';
}

function copyWatchLink(id){
const link = window.location.origin + "/watch/" + id;
navigator.clipboard?.writeText(link).then(()=>{
alert("تم نسخ رابط المشاهدة:\\n" + link);
}).catch(()=>{
prompt("انسخ الرابط يدويًا:", link);
});
}

function updateClock(){
const el = document.getElementById("clock");
if(!el) return;
const now = new Date();
const pad = n => String(n).padStart(2,"0");
el.textContent = pad(now.getHours()) + ":" + pad(now.getMinutes()) + ":" + pad(now.getSeconds());
}
updateClock();
setInterval(updateClock, 1000);

function formatDuration(totalSeconds){
totalSeconds = totalSeconds || 0;
const h = Math.floor(totalSeconds / 3600);
const m = Math.floor((totalSeconds % 3600) / 60);
const s = Math.floor(totalSeconds % 60);
const pad = n => String(n).padStart(2,"0");
if(h > 0) return h + ":" + pad(m) + ":" + pad(s);
return m + ":" + pad(s);
}

function setAccent(name){
document.documentElement.setAttribute("data-accent", name === "blue" ? "" : name);
try{ localStorage.setItem("iptv_accent", name); }catch(e){}
document.querySelectorAll("#themeMenu button").forEach(b => {
b.classList.toggle("themeActive", b.dataset.a === name);
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

(function initAccent(){
let saved = "blue";
try{ saved = localStorage.getItem("iptv_accent") || "blue"; }catch(e){}
setAccent(saved);
})();

function toggleMenu(){
document.getElementById("sideMenu").classList.toggle("open");
document.getElementById("overlay").classList.toggle("show");
}

function closeMenu(){
document.getElementById("sideMenu").classList.remove("open");
document.getElementById("overlay").classList.remove("show");
}

function show(id){
document.getElementById("channels").style.display = (id === "channels") ? "block" : "none";
document.getElementById("add").style.display = (id === "add") ? "block" : "none";
document.getElementById("events").style.display = (id === "events") ? "block" : "none";

document.getElementById("navChannels").classList.toggle("navActive", id === "channels");
document.getElementById("navAdd").classList.toggle("navActive", id === "add");
document.getElementById("navEvents").classList.toggle("navActive", id === "events");
}

function renderStats(){

const box = document.getElementById("statsBar");
if(!box) return;

let live = 0, off = 0, bitrateSum = 0, bitrateCount = 0;

for(const id in channelsCache){
if(statusCache[id]?.active){
live++;
if(statusCache[id]?.bitrateKbps){
bitrateSum += statusCache[id].bitrateKbps;
bitrateCount++;
}
} else {
off++;
}
}

const restarts = eventsCache.filter(e => e.type === "restart").length;
const avgBitrate = bitrateCount > 0 ? Math.round(bitrateSum / bitrateCount) : 0;

box.innerHTML = \`
<div class="statCard ok">
<div class="lbl"><i class="ti ti-broadcast"></i>شغالة</div>
<div class="num mono">\${live}</div>
</div>
<div class="statCard off">
<div class="lbl"><i class="ti ti-player-stop"></i>متوقفة</div>
<div class="num mono">\${off}</div>
</div>
<div class="statCard accent">
<div class="lbl"><i class="ti ti-gauge"></i>متوسط البت ريت</div>
<div class="num mono">\${avgBitrate}<span style="font-size:13px">kbps</span></div>
</div>
<div class="statCard warn">
<div class="lbl"><i class="ti ti-refresh"></i>إعادة تشغيل</div>
<div class="num mono">\${restarts}</div>
</div>
\`;

}

function refreshCategoryOptions(){

const sel = document.getElementById("categorySelect");
const current = sel.value;

const cats = new Set();
for(const id in channelsCache){
const c = channelsCache[id].category;
if(c) cats.add(c);
}

sel.innerHTML = '<option value="">كل التصنيفات</option>' +
Array.from(cats).map(c => '<option value="' + c + '">' + c + '</option>').join("");

sel.value = current;

}

function sortedChannelIds(){

let ids = Object.keys(channelsCache);

const term = searchTerm.trim().toLowerCase();
if(term){
ids = ids.filter(id => id.toLowerCase().includes(term));
}

if(categoryFilter){
ids = ids.filter(id => channelsCache[id].category === categoryFilter);
}

if(sortMode === "status"){
ids.sort((a,b) => {
const aA = statusCache[a]?.active ? 1 : 0;
const bA = statusCache[b]?.active ? 1 : 0;
return bA - aA;
});
} else if(sortMode === "uptime"){
ids.sort((a,b) => (statusCache[b]?.uptimeSeconds||0) - (statusCache[a]?.uptimeSeconds||0));
} else if(sortMode === "name"){
ids.sort((a,b) => a.localeCompare(b));
}

return ids;

}

function onSearch(val){ searchTerm = val; render(); }
function onCategory(val){ categoryFilter = val; render(); }
function onSort(val){ sortMode = val; render(); }

function render(){

renderStats();
refreshCategoryOptions();

const box = document.getElementById("list");
box.innerHTML = "";

const ids = sortedChannelIds();

for(const id of ids){

const isOn = !!statusCache[id]?.active;
const uptimeSeconds = statusCache[id]?.uptimeSeconds || 0;
const totalSeconds = statusCache[id]?.totalSeconds || 0;
const bitrateKbps = statusCache[id]?.bitrateKbps;
const currentViewers = statusCache[id]?.currentViewers || 0;
const totalViewers = statusCache[id]?.totalViewers || 0;
const ch = channelsCache[id];
const logoUrl = ch.logo || "";
const category = ch.category || "";

box.innerHTML += \`
<div class="card">

<div class="cardHead">
<div class="logoBox">\${logoUrl ? '<img src="'+logoUrl+'" onerror="imgFallback(this)">' : '<i class="ti ti-device-tv"></i>'}</div>
<div class="titleWrap">
<h3>\${id}</h3>
\${category ? '<span class="catTag">'+category+'</span>' : ''}
</div>
<div class="statusPill \${isOn ? 'on' : 'off'}"><i class="ti ti-point-filled"></i>\${isOn ? 'شغالة' : 'متوقفة'}</div>
</div>

\${ editingId === id ? \`

<div class="editField">
<div class="tLbl">INPUT URL</div>
<input value="\${(editDraft[id]?.input ?? ch.input ?? '').replace(/"/g,'&quot;')}" oninput="updateDraft('\${id}','input',this.value)">
</div>

<div class="editField">
<div class="tLbl">RTMP OUTPUT</div>
<input value="\${(editDraft[id]?.output ?? ch.output ?? '').replace(/"/g,'&quot;')}" oninput="updateDraft('\${id}','output',this.value)">
</div>

<div class="editField">
<div class="tLbl">LOGO URL</div>
<input value="\${(editDraft[id]?.logo ?? ch.logo ?? '').replace(/"/g,'&quot;')}" oninput="updateDraft('\${id}','logo',this.value)">
</div>

<div class="editField">
<div class="tLbl">CATEGORY</div>
<input value="\${(editDraft[id]?.category ?? ch.category ?? '').replace(/"/g,'&quot;')}" oninput="updateDraft('\${id}','category',this.value)">
</div>

<div class="editField">
<div class="tLbl">رابط المشاهدة الأصلي (WATCH URL)</div>
<input value="\${(editDraft[id]?.watchUrl ?? ch.watchUrl ?? '').replace(/"/g,'&quot;')}" oninput="updateDraft('\${id}','watchUrl',this.value)">
</div>

<div class="editActions">
<button class="saveBtn" onclick="saveEdit('\${id}')">حفظ</button>
<button class="cancelBtn" onclick="cancelEdit()">إلغاء</button>
</div>

\` : \`

<div class="readoutRow">
<div class="readoutBox">
<div class="rLbl">مشاهدين الآن</div>
<div class="rVal">\${ch.watchUrl ? currentViewers : '—'}</div>
</div>
<div class="readoutBox">
<div class="rLbl">إجمالي المشاهدين</div>
<div class="rVal">\${ch.watchUrl ? totalViewers : '—'}</div>
</div>
</div>

\${ ch.watchUrl ? \`
<div class="techLine">
<div class="tLbl">رابط المشاهدة (شارك ده مع الجمهور)</div>
<span class="tVal" style="color:var(--accent);cursor:pointer" title="اضغط للنسخ" onclick="copyWatchLink('\${id}')">\${window.location.origin}/watch/\${id}</span>
</div>
\` : \`
<div class="techLine">
<span class="tVal" style="color:var(--text-3)">مفيش رابط مشاهدة مضاف — اضغط تعديل لإضافته</span>
</div>
\` }

<div class="techLine">
<div class="tLbl">مدة البث الحالية · إجمالي وقت البث</div>
<span class="tVal">\${isOn ? formatDuration(uptimeSeconds) : '—'} · \${formatDuration(totalSeconds)}</span>
</div>

<div class="techLine">
<div class="tLbl">BITRATE</div>
<span class="tVal">\${bitrateKbps ? bitrateKbps + ' kbps' : '—'}</span>
</div>

<div class="techLine">
<div class="tLbl">INPUT</div>
<span class="tVal" title="\${ch.input || ''}">\${ch.input || '—'}</span>
</div>

<div class="techLine">
<div class="tLbl">OUTPUT</div>
<span class="tVal" title="\${ch.output || ''}">\${ch.output || '—'}</span>
</div>

<div class="btnRow">
<button class="iBtn go" onclick="start('\${id}')" title="تشغيل"><i class="ti ti-player-play"></i></button>
<button class="iBtn stop" onclick="stop('\${id}')" title="إيقاف"><i class="ti ti-player-stop"></i></button>
<button class="iBtn" onclick="editChannel('\${id}')" title="تعديل"><i class="ti ti-edit"></i></button>
<button class="iBtn" onclick="showLogs('\${id}')" title="اللوج"><i class="ti ti-file-text"></i></button>
<button class="iBtn" onclick="del('\${id}')" title="حذف"><i class="ti ti-trash"></i></button>
</div>

\` }

</div>
\`;

}

if(ids.length === 0){
box.innerHTML = '<div style="grid-column:1/-1;padding:30px;text-align:center;color:var(--text-3)">لا يوجد قنوات مطابقة</div>';
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

function eventMeta(type){
if(type === "start") return { icon:"ti-player-play", cls:"on", text:"تشغيل" };
if(type === "stop") return { icon:"ti-player-stop", cls:"off", text:"إيقاف" };
if(type === "exit") return { icon:"ti-alert-triangle", cls:"off", text:"خروج غير متوقع" };
if(type === "restart") return { icon:"ti-refresh", cls:"warn", text:"إعادة تشغيل" };
return { icon:"ti-point", cls:"", text:type };
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

const m = eventMeta(ev.type);
const t = new Date(ev.time);
const timeStr = t.toLocaleString("ar-EG");

box.innerHTML += \`
<div class="evRow">
<div class="evIcon \${m.cls}"><i class="ti \${m.icon}"></i></div>
<div class="evBody">
<div class="evTitle"><span class="mono">\${ev.id}</span> — \${m.text}</div>
<div class="evMsg">\${ev.message}</div>
</div>
<div class="evTime">\${timeStr}</div>
</div>
\`;

}

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
document.getElementById("connStatus").innerHTML = '<i class="ti ti-plug-connected"></i>متصل مباشر';
};

ws.onclose = () => {
document.getElementById("connStatus").className = "pill conn-bad";
document.getElementById("connStatus").innerHTML = '<i class="ti ti-plug-connected-x"></i>منقطع - إعادة محاولة...';
clearTimeout(wsReconnectTimer);
wsReconnectTimer = setTimeout(connectWS, 3000);
};

ws.onerror = () => ws.close();

ws.onmessage = (msg) => {
try{
const parsed = JSON.parse(msg.data);

if(parsed.type === "status"){
statusCache = parsed.data;
if(!editingId) render();
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

const id = document.getElementById("f_id").value.trim();
const input = document.getElementById("f_input").value.trim();
const output = document.getElementById("f_output").value.trim();
const logo = document.getElementById("f_logo").value.trim();
const category = document.getElementById("f_category").value.trim();
const watchUrl = document.getElementById("f_watchUrl").value.trim();

if(!id || !input || !output){
alert("من فضلك املأ معرف القناة، رابط البث، ورابط الإخراج على الأقل");
return;
}

await fetch("/channel",{
method:"POST",
headers:{ "Content-Type":"application/json" },
body:JSON.stringify({ id, input, output, logo, category, watchUrl })
});

document.getElementById("f_id").value = "";
document.getElementById("f_input").value = "";
document.getElementById("f_output").value = "";
document.getElementById("f_logo").value = "";
document.getElementById("f_category").value = "";
document.getElementById("f_watchUrl").value = "";

load();
show("channels");
}

async function del(id){
if(!confirm("حذف القناة " + id + "؟")) return;
await fetch("/channel/"+id,{ method:"DELETE" });
load();
}

function editChannel(id){
editDraft[id] = {
input: channelsCache[id].input || "",
output: channelsCache[id].output || "",
logo: channelsCache[id].logo || "",
category: channelsCache[id].category || ""
};
editingId = id;
render();
}

async function saveEdit(id){
const draft = editDraft[id] || {};

await fetch("/channel/"+id,{
method:"PUT",
headers:{ "Content-Type":"application/json" },
body:JSON.stringify({
input: draft.input ?? channelsCache[id].input,
output: draft.output ?? channelsCache[id].output,
logo: draft.logo ?? "",
category: draft.category ?? "",
watchUrl: draft.watchUrl ?? ""
})
});

editingId = null;
delete editDraft[id];
await load();
}

function cancelEdit(){
if(editingId) delete editDraft[editingId];
editingId = null;
render();
}

load();
loadEvents();
connectWS();

setInterval(()=>{
if((!ws || ws.readyState !== 1) && !editingId) load();
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
    const active = !!ffmpegProcesses[id];
    const currentSessionMs = active && liveSince[id] ? (Date.now() - liveSince[id]) : 0;

    data[id] = {
      active,
      uptimeSeconds: Math.floor(currentSessionMs / 1000),
      totalSeconds: Math.floor(((totalOnairMs[id] || 0) + currentSessionMs) / 1000),
      bitrateKbps: active ? (lastBitrateKbps[id] || null) : null,
      currentViewers: currentViewerLog[id] ? currentViewerLog[id].size : 0,
      totalViewers: totalViewerSet[id] ? totalViewerSet[id].size : 0
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
