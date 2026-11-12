import Server from "arrpc/src/server.js";
import { spawn } from "child_process";
import "dotenv/config";
import fs from "fs/promises";
import readline from "readline";

let activitiesCache = null;
let activityNamesCache = [];
let getAppInfoQueue = [];
let refreshTimeout;

// ---------- Sable status state ----------
// Sable (sable-next) reads the status from this profile field: { text, emoji }.
const SABLE_STATUS_FIELD = "org.matrix.msc4426.status";
const MAX_STATUS_TEXT_BYTES = 256; // Sable's limit
const MAX_STATUS_EMOJI_BYTES = 32; // Sable's limit
const REASSERT_MS = Number(process.env.REASSERT_MS ?? 120000); // 0 disables
let sableCurrent = { text: "", emoji: "" }; // what we want shown
let sableSent = null; // what the server last accepted, as "emoji|text"
let fallbackMusic = null; // { text, emoji } from the optional playerctl fallback
let playerProc = null;
let shuttingDown = false;
let sableQueue = Promise.resolve();

// ---------- Sable bio state (opt-in: USE_SABLE_BIO=true) ----------
// Sable reads the bio from m.biography if it exists, otherwise gay.fomx.biography.
// Value shape: { "m.text": [ { body, mimetype? }, ... ] }
const BIO_FIELDS = ["m.biography", "gay.fomx.biography"];
const BIO_BACKUP_FILE = "./bioBackup.json";
const BIO_MODE = process.env.BIO_MODE === "replace" ? "replace" : "append";
// Heading level for the activity title in the bio: 1 = "#" ... 6 = "######"
const BIO_HEADING_LEVEL = Math.min(6, Math.max(1, Math.round(Number(process.env.BIO_HEADING_LEVEL)) || 3));
const MAX_BIO_LINE_CHARS = 200;
let bioEnabled = false;
let bioState = null; // { field, original, lastWritten }
let bioSent = null; // activity text last written, "" when the original is in place
let bioQueue = Promise.resolve();

const appCache = await loadOrCreateAppCache();
const activities = new Map();

if (process.env.USE_SABLE_BIO === "true") bioEnabled = await initBio();

const arrpc = await new Server();

arrpc.on("activity", async (data) => {
  const key = data.socketId ?? data.pid;

  if (data.activity) {
    activities.set(key, data.activity);
    const a = data.activity;
    console.log(`[activity] type=${a.type ?? 0} name=${a.name ?? "?"} details=${a.details ?? ""} state=${a.state ?? ""}`);
  } else {
    activities.delete(key);
  }

  console.log(`Ongoing activities: ${activities.size}`);

  // Sable status and bio go first because the blocks below can return early
  if (process.env.USE_SABLE_STATUS === "true" || bioEnabled) {
    try {
      await refreshSable();
    } catch (err) {
      console.warn("sable update failed:", err);
    }
  }

  if (process.env.USE_PROFILE_FIELD === "true") {
    const parsedActivities = await parseActivities(activities);
    if (!parsedActivities) return;
    if (!hasActivitiesChanged(parsedActivities)) return;
    await setMatrixRPC(parsedActivities);
  }
  if (process.env.USE_PRESENCE_STATUS === "true") {
    const parsedActivityNames = await parseActivityNames(activities);
    if (!parsedActivityNames) return;
    if (!hasActivityNamesChanged(parsedActivityNames)) return;
    await setMatrixStatus(parsedActivityNames);
  }
});

async function cleanup() {
  console.log("\nExiting cleanly");
  shuttingDown = true;
  if (playerProc) playerProc.kill();

  if (process.env.USE_SABLE_STATUS === "true") {
    sableCurrent = { text: "", emoji: "" };
    await pushSableStatus(true);
  }
  if (bioEnabled) {
    await bioQueue;
    await restoreBio();
  }
  if (process.env.USE_PROFILE_FIELD === "true") {
    await setMatrixRPC([]);
  }
  if (process.env.USE_PRESENCE_STATUS === "true") {
    await setMatrixStatus([]);
  }
  process.exit();
}

process.on("SIGINT", cleanup);
process.on("SIGTERM", cleanup);

// ---------- Sable status ----------

// Cuts text to a UTF-8 byte limit without splitting a character.
function truncateBytes(text, maxBytes) {
  const enc = new TextEncoder();
  if (enc.encode(text).length <= maxBytes) return text;
  let out = "";
  let bytes = enc.encode("…").length;
  for (const ch of Array.from(text)) {
    const n = enc.encode(ch).length;
    if (bytes + n > maxBytes) break;
    out += ch;
    bytes += n;
  }
  return out + "…";
}

// Turns one Discord activity into { emoji, text }, or null if it should not be shown.
async function describeActivity(a) {
  const type = a.type ?? 0;
  // Only look the app name up when it is actually needed
  const nameOf = async () => a.name ?? (a.application_id ? (await getAppInfo(a.application_id))?.name : undefined);

  switch (type) {
    case 0: {
      const name = await nameOf();
      return name ? { emoji: "🎮", text: `Playing ${name}` } : null;
    }
    case 1: {
      const what = a.details ?? (await nameOf());
      return what ? { emoji: "📡", text: `Streaming ${what}` } : null;
    }
    case 2: {
      if (a.details) return { emoji: "🎵", text: `Listening to ${a.details}${a.state ? " - " + a.state : ""}` };
      const name = await nameOf();
      return name ? { emoji: "🎵", text: `Listening to ${name}` } : null;
    }
    case 3: {
      const name = await nameOf();
      return name ? { emoji: "📺", text: `Watching ${name}` } : null;
    }
    case 5: {
      const name = await nameOf();
      return name ? { emoji: "🏆", text: `Competing in ${name}` } : null;
    }
    default:
      return null; // custom statuses (type 4) and unknown types
  }
}

async function buildSableStatus() {
  const parts = [];
  let hasMusic = false;
  for (const a of activities.values()) {
    const d = await describeActivity(a);
    if (!d) continue;
    parts.push(d);
    if ((a.type ?? 0) === 2) hasMusic = true;
  }
  // Optional playerctl fallback, only used when arRPC reports no music itself
  if (!hasMusic && fallbackMusic) parts.push(fallbackMusic);

  if (parts.length === 0) return { text: "", emoji: "" };
  return {
    text: truncateBytes(parts.map((p) => p.text).join(" | "), MAX_STATUS_TEXT_BYTES),
    emoji: truncateBytes(parts[0].emoji, MAX_STATUS_EMOJI_BYTES),
  };
}

async function updateSable() {
  sableCurrent = await buildSableStatus();
  await pushSableStatus(false);
}

// Sends the current status. Calls are queued so two updates can never overlap and arrive out of order.
function pushSableStatus(force) {
  sableQueue = sableQueue.then(() => doPushSableStatus(force)).catch((err) => console.warn("sable status:", err));
  return sableQueue;
}

async function doPushSableStatus(force) {
  const { text, emoji } = sableCurrent;
  const key = `${emoji}|${text}`;
  if (!force && key === sableSent) return;

  const url = `${process.env.SERVER_URL}/_matrix/client/v3/profile/${encodeURIComponent(process.env.MATRIX_USER_ID)}/${SABLE_STATUS_FIELD}`;
  try {
    const res = await fetch(url, {
      method: text ? "PUT" : "DELETE",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.TOKEN}`,
      },
      body: text ? JSON.stringify({ [SABLE_STATUS_FIELD]: { text, emoji } }) : undefined,
    });
    console.log("sable status:", text ? `${emoji} ${text}` : "(cleared)", res.status);
    if (res.ok) {
      sableSent = key;
    } else {
      console.warn(await res.text());
      sableSent = null; // retry on the next change or heartbeat
    }
  } catch (err) {
    console.warn("sable status request failed:", err);
    sableSent = null;
  }
}

// Re-sends the status now and then in case something else changed or cleared it.
if (process.env.USE_SABLE_STATUS === "true" && REASSERT_MS > 0) {
  setInterval(() => {
    // Only while something is shown, so a status you set by hand in Sable is left alone when idle
    if (shuttingDown || !sableCurrent.text) return;
    void pushSableStatus(true);
  }, REASSERT_MS);
}

// Optional fallback for a player that does not send Discord RPC, e.g. PLAYERCTL_PLAYER=spotify.
// Only that one player is watched, so browsers and other media never show up.
function watchPlayerctl(player) {
  playerProc = spawn("playerctl", [`--player=${player}`, "--follow", "metadata", "--format", "{{status}}\t{{artist}}\t{{title}}"]);
  playerProc.on("error", (err) => console.warn("playerctl:", err.message));
  playerProc.on("exit", () => {
    if (!shuttingDown) setTimeout(() => watchPlayerctl(player), 5000);
  });

  const rl = readline.createInterface({ input: playerProc.stdout });
  rl.on("line", async (line) => {
    const [status, artist, title] = line.split("\t");
    fallbackMusic =
      status === "Playing" && title
        ? { emoji: "🎵", text: `Listening to ${title}${artist ? " - " + artist : ""}` }
        : null;
    try {
      await refreshSable();
    } catch (err) {
      console.warn("sable update failed:", err);
    }
  });
}

if ((process.env.USE_SABLE_STATUS === "true" || bioEnabled) && process.env.PLAYERCTL_PLAYER) {
  watchPlayerctl(process.env.PLAYERCTL_PLAYER);
}

// ---------- Sable bio ----------

async function refreshSable() {
  if (process.env.USE_SABLE_STATUS === "true") await updateSable();
  if (bioEnabled) await updateBio();
}

function profileUrl(field) {
  return `${process.env.SERVER_URL}/_matrix/client/v3/profile/${encodeURIComponent(process.env.MATRIX_USER_ID)}${field ? "/" + field : ""}`;
}

async function fetchProfile() {
  try {
    const res = await fetch(profileUrl(), { headers: { Authorization: `Bearer ${process.env.TOKEN}` } });
    if (!res.ok) return null;
    return await res.json();
  } catch (err) {
    console.warn("bio: could not read profile:", err);
    return null;
  }
}

// value === null deletes the field
async function putProfileField(field, value) {
  try {
    const res = await fetch(profileUrl(field), {
      method: value === null ? "DELETE" : "PUT",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.TOKEN}`,
      },
      body: value === null ? undefined : JSON.stringify({ [field]: value }),
    });
    console.log("bio:", field, value === null ? "(removed)" : "written", res.status);
    if (!res.ok) console.warn(await res.text());
    return res.ok;
  } catch (err) {
    console.warn("bio request failed:", err);
    return false;
  }
}

async function readBioBackup() {
  try {
    return JSON.parse(await fs.readFile(BIO_BACKUP_FILE, "utf-8"));
  } catch {
    return null;
  }
}

async function saveBioBackup(data) {
  try {
    await fs.writeFile(BIO_BACKUP_FILE, JSON.stringify(data));
  } catch (err) {
    console.warn("bio: could not write backup file:", err);
  }
}

async function removeBioBackup() {
  try {
    await fs.rm(BIO_BACKUP_FILE, { force: true });
  } catch { }
}

// Reads the current bio so it can be put back later. Returns false (bio feature off) if anything is unsafe.
async function initBio() {
  const profile = await fetchProfile();
  if (!profile) {
    console.warn("bio: could not read your profile, bio updates are off");
    return false;
  }
  const field = BIO_FIELDS.find((f) => f in profile) ?? "gay.fomx.biography";
  let original = profile[field] ?? null;

  // Crash recovery: if the bio on the server is still the one we wrote, put the real one back first
  const saved = await readBioBackup();
  if (saved && saved.field === field && JSON.stringify(profile[field]) === JSON.stringify(saved.lastWritten)) {
    console.log("bio: restoring your original bio from the previous run");
    if (!(await putProfileField(field, saved.original ?? null))) {
      console.warn("bio: restore failed, bio updates are off (backup kept in bioBackup.json)");
      return false;
    }
    original = saved.original ?? null;
  }
  await removeBioBackup();

  bioState = { field, original, lastWritten: null };
  console.log(`bio: enabled (${BIO_MODE} mode, field ${field})`);
  return true;
}

function escapeHtml(s) {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function clip(s) {
  const chars = Array.from(s);
  return chars.length > MAX_BIO_LINE_CHARS ? chars.slice(0, MAX_BIO_LINE_CHARS).join("") + "…" : s;
}

// One block per activity: a headline plus whatever details the app sent.
async function describeBioBlock(a) {
  const d = await describeActivity(a);
  if (!d) return null;
  const type = a.type ?? 0;
  const lines = [];

  if (type === 2) {
    const album = a.assets?.large_text?.replace(/^album:\s*/i, "");
    if (album && album !== a.details) lines.push(`Album: ${album}`);
  } else {
    if (a.details) lines.push(a.details);
    if (a.state) lines.push(a.state);
    if (a.assets?.large_text) lines.push(a.assets.large_text);
  }
  if (a.assets?.small_text) lines.push(a.assets.small_text);
  if (Array.isArray(a.party?.size) && a.party.size.length === 2) lines.push(`Party: ${a.party.size[0]}/${a.party.size[1]}`);
  if (type !== 2 && a.timestamps?.start) {
    const ms = a.timestamps.start < 1e12 ? a.timestamps.start * 1000 : a.timestamps.start;
    lines.push(`Since ${new Date(ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`);
  }
  return { head: clip(`${d.emoji} ${d.text}`), lines: [...new Set(lines)].map(clip) };
}

async function buildBioBlocks() {
  const blocks = [];
  let hasMusic = false;
  for (const a of activities.values()) {
    const block = await describeBioBlock(a);
    if (!block) continue;
    blocks.push(block);
    if ((a.type ?? 0) === 2) hasMusic = true;
  }
  if (!hasMusic && fallbackMusic) blocks.push({ head: `${fallbackMusic.emoji} ${fallbackMusic.text}`, lines: [] });
  return blocks;
}

// The same blocks in the three forms a bio can hold. Sable's own editor stores a bio as markdown in the
// plain body (a line break is a bare newline, no backslash) plus the rendered HTML next to it, and it displays
// the HTML. A plain-only body is shown as-is, so markdown symbols in it would appear literally.
function escapeMd(s) {
  return s.replace(/[\\`*_~|\[\]<>]/g, "\\$&");
}
const blockMd = (b) => [`${"#".repeat(BIO_HEADING_LEVEL)} ${escapeMd(b.head)}`, ...b.lines.map(escapeMd)].join("\n");
const blockHtml = (b) =>
  `<h${BIO_HEADING_LEVEL}>${escapeHtml(b.head)}</h${BIO_HEADING_LEVEL}>` +
  (b.lines.length ? `<p>${b.lines.map(escapeHtml).join("<br>")}</p>` : "");
const blockPlain = (b) => [b.head, ...b.lines].join("\n");

// append: keeps the original bio and adds the activity under it. replace: only the activity.
function composeBio(original, blocks) {
  const md = blocks.map(blockMd).join("\n\n");
  const html = blocks.map(blockHtml).join("");
  const plain = blocks.map(blockPlain).join("\n\n");
  const texts = Array.isArray(original?.["m.text"]) ? original["m.text"] : [];

  // No bio to keep: markdown body plus its rendered HTML, exactly how Sable stores a formatted bio
  if (BIO_MODE === "replace" || texts.length === 0) {
    return { "m.text": [{ body: html, mimetype: "text/html" }, { body: md }] };
  }

  // A plain-only bio stays plain-only, so nothing in it changes; the activity is added as plain text
  if (!texts.some((t) => t?.mimetype === "text/html")) {
    return { ...original, "m.text": texts.map((t) => ({ ...t, body: t?.body ? `${t.body}\n\n${plain}` : plain })) };
  }

  // A formatted bio is displayed from its HTML, so the activity goes into both forms
  const out = texts.map((t) =>
    t?.mimetype === "text/html"
      ? { ...t, body: `${t.body}${html}` }
      : { ...t, body: t?.body ? `${t.body}\n\n${md}` : md },
  );
  if (!out.some((t) => t.mimetype !== "text/html")) out.push({ body: md });
  return { ...original, "m.text": out };
}

function updateBio() {
  bioQueue = bioQueue
    .then(async () => doUpdateBio(await buildBioBlocks()))
    .catch((err) => console.warn("bio update failed:", err));
  return bioQueue;
}

async function doUpdateBio(blocks) {
  const text = blocks.map(blockPlain).join("\n\n"); // also the change-detection key
  if (!bioState || text === bioSent) return;
  if (!text) {
    if (!bioState.lastWritten) {
      bioSent = "";
      return;
    }
    await restoreBio();
    return;
  }

  const content = composeBio(bioState.original, blocks);
  // Backup first, so a crash right after the write can still bring the real bio back
  await saveBioBackup({ field: bioState.field, original: bioState.original, lastWritten: content });
  if (await putProfileField(bioState.field, content)) {
    bioState.lastWritten = content;
    bioSent = text;
  } else {
    bioSent = null; // retry on the next change
  }
}

// Puts the original bio back, unless you changed the bio yourself in the meantime.
async function restoreBio() {
  if (!bioState?.lastWritten) return;
  const profile = await fetchProfile();
  if (profile && JSON.stringify(profile[bioState.field]) !== JSON.stringify(bioState.lastWritten)) {
    console.log("bio: it was changed outside this script, leaving it alone");
    bioState.lastWritten = null;
    bioSent = "";
    await removeBioBackup();
    return;
  }
  if (await putProfileField(bioState.field, bioState.original ?? null)) {
    bioState.lastWritten = null;
    bioSent = "";
    await removeBioBackup();
  } else {
    bioSent = null;
  }
}

// ---------- Original Matrix RPC / presence code ----------

async function parseActivities(activities) {
  const results = [];
  if (activities.size === 0) return results;

  for (const activity of activities.values()) {
    const appInfo = await getAppInfo(activity.application_id);
    if (!appInfo) return false;

    if (activity.type === 2 && process.env.PROCESS_MUSIC === "false") continue;
    if (activity.type === 2) {
      results.push({
        id: activity.application_id,
        type: "xyz.extera.msc4544.rpc.music",
        track: activity.details,
        artist: activity.state,
        album: activity.assets?.large_text.replace("album: ", ""),
        cover_url: await getAppImageMatrixUrl(
          activity.application_id,
          activity.assets?.large_image ?? `https://cdn.discordapp.com/app-icons/${activity.application_id}/${appInfo.icon.id}?size=128`,
        ),
        progress: {
          since: activity.timestamps?.start,
          until: activity.timestamps?.end,
        },
      });
    } else {
      results.push({
        id: activity.application_id,
        type: "xyz.extera.msc4544.rpc.activity",
        name: activity.name ?? appInfo.name,
        large_icon_url: await getAppImageMatrixUrl(
          activity.application_id,
          activity.assets?.large_image ?? `https://cdn.discordapp.com/app-icons/${activity.application_id}/${appInfo.icon.id}?size=128`,
        ),
        large_icon_tooltip: activity.assets?.large_text,
        small_icon_url: await getAppImageMatrixUrl(activity.application_id, activity.assets?.small_image),
        small_icon_tooltip: activity.assets?.small_text,
        details: activity.details,
        state: activity.state,
        since: activity.timestamps?.start,
        until: activity.timestamps?.end,
      });
    }
  }
  return results;
}

async function parseActivityNames(activities) {
  const results = [];
  for (const activity of activities.values()) {
    if (activity.type !== 0) continue;
    if (!activity.name) {
      const appInfo = await getAppInfo(activity.application_id);
      if (!appInfo) return false;
      results.push(appInfo.name);
    } else {
      results.push(activity.name);
    }
  }
  return results;
}

function hasActivitiesChanged(activities) {
  if (JSON.stringify(activities) === JSON.stringify(activitiesCache)) return false;
  activitiesCache = activities;
  return true;
}

function hasActivityNamesChanged(activityNames) {
  if (activityNamesCache.length === activityNames.length && activityNamesCache.every((str, i) => str === activityNames[i])) return false;
  activityNamesCache = activityNames;
  return true;
}

async function setMatrixRPC(activities) {
  const url = `${process.env.SERVER_URL}/_matrix/client/v3/profile/${encodeURIComponent(process.env.MATRIX_USER_ID)}/xyz.extera.msc4544.rpc`;
  clearTimeout(refreshTimeout);

  if (activities.length === 0) {
    const res = await fetch(url, {
      method: "DELETE",
      headers: {
        Authorization: `Bearer ${process.env.TOKEN}`,
      },
    });
    console.log(res.status);
  } else {
    refreshTimeout = setTimeout(() => setMatrixRPC(activitiesCache), Number(process.env.EXPIRY) - 5000);
    const body = JSON.stringify({
      "xyz.extera.msc4544.rpc": activities.map((activity) => ({
        ...activity,
        expiry: Date.now() + Number(process.env.EXPIRY),
      })),
    });

    const res = await fetch(url, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.TOKEN}`,
      },
      body,
    });
    console.log(res.status);
  }
}

async function setMatrixStatus(activities) {
  const url = `${process.env.SERVER_URL}/_matrix/client/v3/presence/${encodeURIComponent(process.env.MATRIX_USER_ID)}/status`;

  let statusMessage = "";
  if (activities.length > 0) statusMessage = "Playing";
  for (let i = 0; i < activities.length; i++) {
    if (i > 0 && i === activities.length - 1) {
      statusMessage = statusMessage.concat(" and");
    }
    statusMessage = statusMessage.concat(` ${activities[i]}`);
    if (i > 1 && i !== activities.length - 1) {
      statusMessage = statusMessage.concat(",");
    }
  }
  console.log(statusMessage);
  const res = await fetch(url, {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${process.env.TOKEN}`,
    },
    body: JSON.stringify({
      presence: "online",
      status_msg: statusMessage,
    }),
  });
  console.log(res.status);
}

async function getAppImageMatrixUrl(appId, url) {
  if (!url) return;

  if (url in appCache.get(appId).icon) {
    return appCache.get(appId).icon[url];
  }

  try {
    const imageRes = await fetch(url);
    if (!imageRes.ok) return "";

    const contentType = imageRes.headers.get("content-type") ?? "image/png";
    const body = Buffer.from(await imageRes.arrayBuffer());

    const matrixRes = await fetch(`${process.env.SERVER_URL}/_matrix/media/v3/upload?filename=${encodeURIComponent(crypto.randomUUID())}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${process.env.TOKEN}`,
        "Content-Type": contentType,
      },
      body,
    });
    if (!matrixRes.ok) return "";

    const { content_uri } = await matrixRes.json();
    appCache.get(appId).icon[url] = content_uri;
    await storeAppCache();
    return content_uri;
  } catch (err) {
    console.warn(err);
    return "";
  }
}

async function getAppInfo(id) {
  if (appCache.has(id)) {
    return appCache.get(id);
  } else {
    if (getAppInfoQueue.includes(id)) {
      console.log("nope");
      return false;
    }
    console.log("fetching new data for app:", id);
    getAppInfoQueue.push(id);
    try {
      const res = await fetch("https://discordgate.com/api/tools/lookup/applications/add", {
        method: "POST",
        headers: {
          "content-type": "application/json",
        },
        body: JSON.stringify({
          botId: id,
          fingerprint: "",
        }),
      });
      let json = await res.json();
      appCache.set(id, {
        name: json?.botData?.name,
        icon: { id: json?.botData?.icon },
      });
      getAppInfoQueue.splice(getAppInfoQueue.indexOf(id), 1);
      await storeAppCache();
      return appCache.get(id);
    } catch (err) {
      console.warn(err);
      getAppInfoQueue.splice(getAppInfoQueue.indexOf(id), 1);
      return { name: id };
    }
  }
}

async function loadOrCreateAppCache() {
  try {
    const map = new Map(Object.entries(JSON.parse(await fs.readFile("./appCache.json", "utf-8"))));
    console.log("AppCache loaded from disk");
    return map;
  } catch (err) {
    console.log("New appCache created");
    return new Map();
  }
}

async function storeAppCache() {
  try {
    await fs.writeFile("./appCache.json", JSON.stringify(Object.fromEntries(appCache)));
  } catch (err) {
    console.log("Error while writing appCache to disk", err);
  }
}
