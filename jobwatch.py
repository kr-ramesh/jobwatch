#!/usr/bin/env python3
"""jobwatch: a desktop app that watches jobs on your SSH servers and notifies you when they finish.

    jobwatch                     # start the background checker (if needed) and open the window
    jobwatch --hosts gpu1 gpu2   # also enable these servers
    jobwatch --service           # background checker only (what "start at login" runs)
    jobwatch --quit              # stop the background checker

Servers come from ~/.ssh/config (plus "local" = this machine). Each poll runs one
ssh command per server: squeue + sacct if Slurm is there, otherwise `ps` filtered
by the process patterns in Settings. ssh runs with BatchMode, so a server must
log in without prompting (keys/agent, or a ControlMaster connection you opened).

The checker is a small local web server on 127.0.0.1; the window shows its page
(GTK WebKit if available, else a Chrome/Chromium app window, else your browser).
Closing the window leaves the checker running so notifications keep coming.

Needs only Python 3.8+ and the ssh client. Enabled servers, watches and settings
are kept in ~/.config/jobwatch/state.json (override the folder with $JOBWATCH_HOME).
"""
from __future__ import annotations

__version__ = "0.1.0"

import argparse
import fcntl
import fnmatch
import glob
import html
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid
import webbrowser
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

STATE_DIR = Path(os.environ.get("JOBWATCH_HOME", str(Path.home() / ".config" / "jobwatch")))
STATE_FILE = STATE_DIR / "state.json"
RUN_FILE = STATE_DIR / "service.json"   # port + token of the running checker
LOCK_FILE = STATE_DIR / "service.lock"
LOG_FILE = STATE_DIR / "service.log"
AUTOSTART_FILE = Path(os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))) / "autostart" / "jobwatch.desktop"
CTL_DIR = Path("/tmp") / ("jobwatch-%d" % os.getuid())  # short path: macOS caps socket paths at 104 chars
LOCAL = "local"
HOST_RE = re.compile(r"^[A-Za-z0-9_.@:%\[\]-]+$")

DEFAULT_SETTINGS = {
    "poll_seconds": 60,
    "history_hours": 24,
    "notify_every_job": True,
    "desktop_notifications": True,
    "browser_notifications": False,
    "ntfy_topic": "",
    "theme": "auto",
    "process_patterns": ["python", "torchrun", "accelerate", "deepspeed"],
    "process_exclude": [".vscode-server", ".cursor-server"],
}

# Slurm states that mean "not finished yet" (first word of the State column).
ACTIVE_STATES = {"RUNNING", "PENDING", "REQUEUED", "SUSPENDED", "COMPLETING", "CONFIGURING",
                 "RESIZING", "SIGNALING", "STAGE_OUT", "STOPPED", "REQUEUE_FED", "REQUEUE_HOLD",
                 "RESV_DEL_HOLD"}
# Worst first; used to summarise an array job or a watch with one state.
SEVERITY = ["NODE_FAIL", "OUT_OF_MEMORY", "FAILED", "BOOT_FAIL", "TIMEOUT", "DEADLINE",
            "PREEMPTED", "CANCELLED", "ENDED", "COMPLETED"]
GOOD_STATES = {"COMPLETED", "ENDED"}

REMOTE_SCRIPT = r"""
U="${USER:-$(id -un)}"
echo __JW_BEGIN__
if command -v squeue >/dev/null 2>&1; then
  echo __JW_SLURM__
  squeue -u "$U" -h -o '%i|%j|%T|%M|%l|%D|%R|%P' || echo __JW_FAIL__ squeue
  echo __JW_SACCT__
  sacct -u "$U" -X -n -P -S now-@HOURS@hours --format=JobID,JobName,State,ExitCode,Elapsed,End,Partition 2>/dev/null
else
  echo __JW_PS__
  ps -u "$U" -o pid=,etime=,args= || echo __JW_FAIL__ ps
fi
echo __JW_END__
"""


class PollError(Exception):
    pass


def base_id(job_id):
    """1234_5, 1234_[3-9] and 1234+0 all belong to job 1234."""
    return re.split(r"[_+]", job_id, maxsplit=1)[0]


def worst(states):
    rank = {s: i for i, s in enumerate(SEVERITY)}
    return min(states, key=lambda s: rank.get(s, -1)) if states else "ENDED"


def summarize(states):
    c = Counter(states)
    return ", ".join("%d %s" % (c[s], s) for s in sorted(c, key=lambda s: SEVERITY.index(s) if s in SEVERITY else -1))


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S")


# ---------------------------------------------------------------- ssh config

_hosts_cache = [0.0, []]


def ssh_config_hosts(path=None, seen=None):
    """Concrete Host names from ~/.ssh/config (following Include), wildcards skipped."""
    path = Path(os.path.expanduser(str(path or "~/.ssh/config")))
    seen = set() if seen is None else seen
    if path in seen or not path.is_file():
        return []
    seen.add(path)
    hosts = []
    try:
        lines = path.read_text(errors="replace").splitlines()
    except OSError:
        return []
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = re.split(r"\s*=\s*|\s+", line, maxsplit=1)
        if len(parts) < 2:
            continue
        key, val = parts[0].lower(), parts[1]
        if key == "include":
            for pat in val.split():
                pat = os.path.expanduser(pat.strip('"'))
                if not os.path.isabs(pat):
                    pat = str(Path.home() / ".ssh" / pat)
                for f in sorted(glob.glob(pat)):
                    hosts += ssh_config_hosts(f, seen)
        elif key == "host":
            for h in val.split():
                h = h.strip('"')
                if h and not any(c in h for c in "*?!"):
                    hosts.append(h)
    return list(dict.fromkeys(hosts))


def cached_config_hosts():
    if time.time() - _hosts_cache[0] > 30:
        _hosts_cache[:] = [time.time(), ssh_config_hosts()]
    return _hosts_cache[1]


def ssh_command(host):
    opts = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=15",
            "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=2"]
    # Reuse one connection between polls, unless the user's config already sets up multiplexing.
    try:
        out = subprocess.run(["ssh", "-G", host], capture_output=True, text=True, timeout=10).stdout
        cfg = dict(l.split(" ", 1) for l in out.lower().splitlines() if " " in l)
    except Exception:
        cfg = {}
    if cfg.get("controlmaster", "false") in ("false", "no") and cfg.get("controlpath", "none") == "none":
        CTL_DIR.mkdir(mode=0o700, exist_ok=True)
        opts += ["-o", "ControlMaster=auto", "-o", "ControlPath=%s/%%C" % CTL_DIR, "-o", "ControlPersist=10m"]
    return ["ssh"] + opts + [host, "bash -l -s"]


# ---------------------------------------------------------------- parsing

def parse_output(out, err, settings):
    """Returns (backend, active_jobs, history_rows)."""
    if "__JW_BEGIN__" not in out or "__JW_END__" not in out:
        msg = (err or "").strip().splitlines()
        msg = msg[-1] if msg else "no output"
        if re.search(r"permission denied|password|keyboard-interactive|verification", msg, re.I):
            msg += " (jobwatch can't answer login prompts: use key auth, or open a ControlMaster connection first)"
        raise PollError(msg)
    body = out.split("__JW_BEGIN__", 1)[1].split("__JW_END__", 1)[0]
    sections, cur = {}, None
    for line in body.splitlines():
        if line.startswith("__JW_FAIL__"):
            tail = " ".join((err or "").strip().splitlines()[-2:])
            raise PollError("%s failed on the server%s" % (line.split()[-1], ": " + tail if tail else ""))
        m = re.match(r"^__JW_(\w+?)__$", line.strip())
        if m:
            cur = m.group(1)
            sections[cur] = []
        elif cur and line.strip():
            sections[cur].append(line)
    if "SLURM" in sections:
        return "slurm", parse_squeue(sections["SLURM"]), parse_sacct(sections.get("SACCT", []))
    if "PS" in sections:
        return "ps", parse_ps(sections["PS"], settings["process_patterns"], settings["process_exclude"]), []
    raise PollError("unexpected output from server")


def parse_squeue(lines):
    jobs = []
    for line in lines:
        f = line.split("|")
        if len(f) < 8:
            continue
        jobs.append({"id": f[0], "name": "|".join(f[1:-6]), "state": f[-6], "elapsed": f[-5],
                     "limit": f[-4], "nodes": f[-3], "where": f[-2], "partition": f[-1]})
    return jobs


def parse_sacct(lines):
    rows = []
    for line in lines:
        f = line.split("|")
        if len(f) < 7:
            continue
        full = f[-5].strip()
        rows.append({"id": f[0], "name": "|".join(f[1:-5]), "state": (full.split() or ["UNKNOWN"])[0],
                     "state_full": full, "exit": f[-4], "elapsed": f[-3], "end": f[-2], "partition": f[-1]})
    return rows


def parse_ps(lines, patterns, excludes):
    jobs = []
    for line in lines:
        parts = line.split(None, 2)
        if len(parts) < 3:
            continue
        pid, etime, args = parts
        if not any(p in args for p in patterns) or any(x in args for x in excludes):
            continue
        exe, _, rest = args.partition(" ")
        name = (os.path.basename(exe) + " " + rest).strip()
        jobs.append({"id": pid, "name": name[:200], "state": "RUNNING", "elapsed": etime})
    return jobs


# ---------------------------------------------------------------- notifications

def desktop_notify(title, body):
    try:
        if sys.platform == "darwin":
            subprocess.run(["osascript", "-e", "on run argv",
                            "-e", 'display notification (item 2 of argv) with title (item 1 of argv) sound name "Glass"',
                            "-e", "end run", title, body], capture_output=True, timeout=10)
        elif shutil.which("notify-send"):
            subprocess.run(["notify-send", "-a", "jobwatch", "-i", "jobwatch", title, html.escape(body)],
                           capture_output=True, timeout=10)
    except Exception:
        pass


def ntfy_notify(topic, title, body, failed):
    url = topic if topic.startswith(("http://", "https://")) else "https://ntfy.sh/" + urllib.parse.quote(topic)
    headers = {"Title": title.encode("latin-1", "ignore").decode("latin-1").strip(),
               "Tags": "warning" if failed else "white_check_mark"}
    try:
        urllib.request.urlopen(urllib.request.Request(url, data=body.encode(), headers=headers), timeout=15)
    except Exception as e:
        print("ntfy failed:", e, file=sys.stderr)


# ---------------------------------------------------------------- app state

class HostState:
    def __init__(self, name):
        self.name = name
        self.status = "waiting"   # waiting | ok | error
        self.error = ""
        self.backend = ""
        self.last_poll = None
        self.last_ok = None
        self.polling = False
        self.active = []
        self.finished = []
        self.history = []          # sacct rows (slurm) or ends we observed (ps)
        self.prev = None           # {id: job} at the last successful poll

    def view(self):
        return {k: getattr(self, k) for k in
                ("name", "status", "error", "backend", "last_poll", "last_ok", "polling", "active", "finished")}


class App:
    def __init__(self):
        self.lock = threading.RLock()
        self.wake = threading.Event()
        self.token = secrets.token_urlsafe(24)
        self.settings = dict(DEFAULT_SETTINGS)
        self.enabled = []
        self.custom_hosts = []
        self.watches = []
        self.events = []
        self.seq = 0
        self.hosts = {}
        self.ssh_cmds = {}
        self.next_poll = 0.0
        self.server = None
        self.pool = ThreadPoolExecutor(max_workers=16)
        self.load()

    # -- persistence
    def load(self):
        try:
            data = json.loads(STATE_FILE.read_text())
        except FileNotFoundError:
            return
        except Exception as e:
            bad = STATE_FILE.with_suffix(".corrupt.json")
            print("warning: could not read %s (%s); moved it to %s" % (STATE_FILE, e, bad), file=sys.stderr)
            os.replace(STATE_FILE, bad)
            return
        self.settings.update({k: v for k, v in data.get("settings", {}).items() if k in DEFAULT_SETTINGS})
        self.enabled = data.get("enabled", [])
        self.custom_hosts = data.get("custom_hosts", [])
        self.watches = data.get("watches", [])
        self.events = data.get("events", [])
        self.seq = max([e["seq"] for e in self.events] + [0])
        # Remembering what was running lets us report jobs that ended while jobwatch was closed.
        for name, prev in data.get("last_active", {}).items():
            self.host(name).prev = prev

    def save(self):
        with self.lock:
            data = {"settings": self.settings, "enabled": self.enabled, "custom_hosts": self.custom_hosts,
                    "watches": self.watches, "events": self.events[-200:],
                    "last_active": {n: h.prev for n, h in self.hosts.items() if h.prev is not None and n in self.enabled}}
            text = json.dumps(data, indent=1)
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(text)
        os.replace(tmp, STATE_FILE)

    def host(self, name):
        if name not in self.hosts:
            self.hosts[name] = HostState(name)
        return self.hosts[name]

    def available_hosts(self):
        return list(dict.fromkeys([LOCAL] + cached_config_hosts() + self.custom_hosts + self.enabled))

    # -- polling
    def command(self, name):
        if name == LOCAL:
            return ["bash", "-l", "-s"]
        if name not in self.ssh_cmds:
            self.ssh_cmds[name] = ssh_command(name)
        return self.ssh_cmds[name]

    def loop(self):
        while True:
            with self.lock:
                names, interval = list(self.enabled), self.settings["poll_seconds"]
            list(self.pool.map(self.poll_host, names))
            try:
                self.save()
            except OSError as e:
                print("warning: could not save state:", e, file=sys.stderr)
            self.next_poll = time.time() + interval
            self.wake.wait(interval)
            self.wake.clear()

    def poll_host(self, name):
        with self.lock:
            settings = dict(self.settings)
            self.host(name).polling = True
        script = REMOTE_SCRIPT.replace("@HOURS@", str(max(1, int(settings["history_hours"]))))
        result, error = None, ""
        try:
            p = subprocess.run(self.command(name), input=script, capture_output=True, text=True,
                               errors="replace", timeout=90)
            result = parse_output(p.stdout, p.stderr, settings)
        except subprocess.TimeoutExpired:
            error = "timed out after 90s"
        except PollError as e:
            error = str(e)
        except Exception as e:
            error = "%s: %s" % (type(e).__name__, e)
        outbox = []
        with self.lock:
            h = self.host(name)
            h.polling, h.last_poll = False, time.time()
            if result is None:
                # Never infer completions from a failed poll; keep showing the last good data.
                h.status, h.error = "error", error
                if name in self.ssh_cmds and "ControlPath" in " ".join(self.ssh_cmds[name]):
                    del self.ssh_cmds[name]  # re-read ssh config next time
            else:
                h.status, h.error, h.last_ok = "ok", "", h.last_poll
                outbox = self.apply(h, *result)
        for title, body, failed in outbox:
            self.send_notification(title, body, failed)

    def apply(self, h, backend, active, history):
        """Diff against the previous poll, update watches; returns notifications to send."""
        now = time.time()
        hours = self.settings["history_hours"]
        curr = {j["id"]: j for j in active}
        now_state = {}
        for j in active:
            b = base_id(j["id"])
            if now_state.get(b) != "RUNNING":
                now_state[b] = j["state"]

        if backend == "ps":
            history = h.history if h.backend == "ps" else []
        rows_by_base = {}
        for r in history:
            if r["state"] not in ACTIVE_STATES:
                rows_by_base.setdefault(base_id(r["id"]), []).append(r)

        ended = []
        if h.prev is not None:
            for jid, job in h.prev.items():
                if jid in curr or ("[" in jid and base_id(jid) in now_state):
                    continue  # still there, or a pending array range that shrank as tasks started
                rows = [r for r in rows_by_base.get(base_id(jid), []) if r["id"] == jid or "[" in jid]
                row = rows[0] if len(rows) == 1 else None
                ended.append({"id": jid, "name": job.get("name", ""),
                              "state": worst([r["state"] for r in rows]) if rows else "ENDED",
                              "exit": row["exit"] if row else "", "elapsed": row["elapsed"] if row else job.get("elapsed", ""),
                              "end": row["end"] if row else now_iso(), "seen_at": now})
        h.prev = curr

        if backend == "ps":
            h.history = [r for r in ended + h.history if now - r["seen_at"] < hours * 3600][:500]
            for r in ended:
                rows_by_base.setdefault(base_id(r["id"]), []).append(r)
        else:
            h.history = history
        h.backend, h.active = backend, active
        h.finished = sorted([r for r in h.history if r["state"] not in ACTIVE_STATES],
                            key=lambda r: r.get("end", ""), reverse=True)

        def final_state(b):
            return worst([r["state"] for r in rows_by_base.get(b, [])])

        outbox, grouped = [], set()
        for w in self.watches:
            if w["host"] != h.name or w["done"]:
                continue
            members = w["members"]
            if w["kind"] == "pattern":
                for j in active:
                    if fnmatch.fnmatchcase(j["name"], w["pattern"]):
                        members.setdefault(base_id(j["id"]), {"name": j["name"]})
            for b, m in members.items():
                if b in now_state:
                    m.update(state=now_state[b], active=True)
                elif m.get("active", True):
                    m.update(state=final_state(b), active=False)
            grouped |= set(members)
            if members and not any(m["active"] for m in members.values()):
                w["done"], w["done_at"] = True, now
                states = [m["state"] for m in members.values()]
                failed = any(s not in GOOD_STATES for s in states)
                title = "%s %s finished on %s" % ("⚠️" if failed else "✅", w["name"], h.name)
                body = "%d job%s: %s" % (len(states), "" if len(states) == 1 else "s", summarize(states))
                outbox.append(self.log_event(h.name, "watch", title, body, failed, True))

        solo = [r for r in ended if base_id(r["id"]) not in grouped]
        if solo:
            failed = any(r["state"] not in GOOD_STATES for r in solo)
            if len(solo) == 1:
                r = solo[0]
                title = "%s %s %s on %s" % ("⚠️" if failed else "✅", r["name"] or r["id"], r["state"], h.name)
                body = "Job %s%s%s" % (r["id"], " · elapsed " + r["elapsed"] if r["elapsed"] else "",
                                       " · exit " + r["exit"] if r["exit"] else "")
            else:
                title = "%s %d jobs finished on %s" % ("⚠️" if failed else "✅", len(solo), h.name)
                body = summarize([r["state"] for r in solo]) + ": " + ", ".join(r["name"] or r["id"] for r in solo[:8])
                if len(solo) > 8:
                    body += ", …"
            outbox.append(self.log_event(h.name, "job", title, body, failed, self.settings["notify_every_job"]))
        return [o for o in outbox if o]

    def log_event(self, host, kind, title, body, failed, notify):
        self.seq += 1
        self.events.append({"seq": self.seq, "at": time.time(), "host": host, "kind": kind, "title": title,
                            "body": body, "failed": failed, "notified": notify})
        self.events = self.events[-200:]
        return (title, body, failed) if notify else None

    def send_notification(self, title, body, failed):
        s = self.settings
        if s["desktop_notifications"]:
            desktop_notify(title, body)
        if s["ntfy_topic"]:
            threading.Thread(target=ntfy_notify, args=(s["ntfy_topic"], title, body, failed), daemon=True).start()

    # -- API
    def snapshot(self):
        with self.lock:
            return {"now": time.time(), "next_poll": self.next_poll, "settings": self.settings,
                    "enabled": self.enabled, "hosts": [self.host(n).view() for n in self.enabled],
                    "watches": self.watches, "events": self.events[-100:],
                    "app": {"linux": sys.platform.startswith("linux"), "autostart": AUTOSTART_FILE.exists()}}

    def api_hosts_list(self, _=None):
        with self.lock:
            return {"available": self.available_hosts(), "enabled": self.enabled, "custom": self.custom_hosts}

    def api_set_hosts(self, body):
        names = [str(n) for n in body.get("enabled", [])]
        custom = [str(n) for n in body.get("custom", [])]
        for n in names + custom:
            if not HOST_RE.match(n) or n.startswith("-"):
                raise ValueError("invalid host name: %r" % n)
        with self.lock:
            self.custom_hosts = list(dict.fromkeys(custom))
            self.enabled = list(dict.fromkeys(names))
        self.save()
        self.wake.set()

    def api_settings(self, body):
        with self.lock:
            s = self.settings
            for k in ("notify_every_job", "desktop_notifications", "browser_notifications"):
                if k in body:
                    s[k] = bool(body[k])
            if "poll_seconds" in body:
                s["poll_seconds"] = min(3600, max(15, int(body["poll_seconds"])))
            if "history_hours" in body:
                s["history_hours"] = min(24 * 30, max(1, int(body["history_hours"])))
            if body.get("theme") in ("auto", "light", "dark"):
                s["theme"] = body["theme"]
            if "ntfy_topic" in body:
                s["ntfy_topic"] = str(body["ntfy_topic"]).strip()
            for k in ("process_patterns", "process_exclude"):
                if k in body:
                    v = body[k]
                    v = v.split(",") if isinstance(v, str) else v
                    s[k] = [str(x).strip() for x in v if str(x).strip()]
        self.save()
        self.wake.set()

    def api_watch(self, body):
        host, label = str(body.get("host", "")), str(body.get("name", "")).strip()
        with self.lock:
            if host not in self.enabled:
                raise ValueError("unknown host")
            h = self.host(host)
            w = {"id": uuid.uuid4().hex[:8], "host": host, "created": time.time(), "done": False, "members": {}}
            if body.get("pattern"):
                w.update(kind="pattern", pattern=str(body["pattern"]))
                for j in h.active:
                    if fnmatch.fnmatchcase(j["name"], w["pattern"]):
                        w["members"].setdefault(base_id(j["id"]), {"name": j["name"], "state": j["state"], "active": True})
                default = "all jobs" if w["pattern"] == "*" else w["pattern"]
            else:
                ids = [str(i) for i in body.get("ids", [])]
                if not ids:
                    raise ValueError("no jobs selected")
                byid = {j["id"]: j for j in h.active}
                for i in ids:
                    j = byid.get(i, {})
                    w["members"].setdefault(base_id(i), {"name": j.get("name", ""), "state": j.get("state", "?"), "active": True})
                w.update(kind="ids", pattern="")
                names = sorted({m["name"] for m in w["members"].values() if m["name"]})
                prefix = os.path.commonprefix(names) if len(names) > 1 else ""
                default = names[0] if len(names) == 1 else (prefix + "*" if len(prefix) >= 3 else "%d jobs" % len(w["members"]))
            w["name"] = label or default
            self.watches.append(w)
        self.save()
        return w

    def api_unwatch(self, body):
        with self.lock:
            self.watches = [w for w in self.watches if w["id"] != body.get("id")]
        self.save()

    def api_clear_done(self, _):
        with self.lock:
            self.watches = [w for w in self.watches if not w["done"]]
        self.save()

    def api_clear_events(self, _):
        with self.lock:
            self.events = []
        self.save()

    def api_refresh(self, _):
        self.wake.set()

    def api_test_notify(self, _):
        self.send_notification("✅ jobwatch test", "Notifications are working.", False)

    def api_autostart(self, body):
        if not body.get("enabled"):
            if AUTOSTART_FILE.exists():
                AUTOSTART_FILE.unlink()
            return
        AUTOSTART_FILE.parent.mkdir(parents=True, exist_ok=True)
        AUTOSTART_FILE.write_text(
            "[Desktop Entry]\nType=Application\nName=jobwatch (background)\n"
            "Comment=Checks jobs on your servers and notifies you when they finish\n"
            "Exec=%s --service\nIcon=jobwatch\nTerminal=false\nX-GNOME-Autostart-enabled=true\n" % launcher())

    def api_quit(self, _):
        threading.Thread(target=self.server.shutdown, daemon=True).start()


def launcher():
    """How to start this program again (for the autostart entry and for spawning the checker)."""
    me = os.path.realpath(__file__)
    parts = [me] if os.access(me, os.X_OK) else [sys.executable, me]
    return " ".join('"%s"' % p if " " in p else p for p in parts)


# ---------------------------------------------------------------- web server

class Handler(BaseHTTPRequestHandler):
    app: App = None

    def log_message(self, *args):
        pass

    def _send(self, code, body, ctype="application/json"):
        data = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype + "; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _local_host_header(self):
        # Blocks DNS-rebinding pages from talking to this server.
        host = (self.headers.get("Host") or "").rsplit(":", 1)[0]
        return host in ("127.0.0.1", "localhost", "[::1]")

    def do_GET(self):
        if not self._local_host_header():
            return self._send(403, '"forbidden"')
        path = urllib.parse.urlparse(self.path).path
        if path == "/":
            page = PAGE.replace("@TOKEN@", self.app.token).replace("@THEME@", self.app.settings["theme"])
            return self._send(200, page, "text/html")
        if path == "/api/state":
            return self._send(200, json.dumps(self.app.snapshot()))
        if path == "/api/hosts":
            return self._send(200, json.dumps(self.app.api_hosts_list()))
        if path == "/api/ping":
            return self._send(200, '"jobwatch"')
        self._send(404, '"not found"')

    def do_POST(self):
        if not self._local_host_header() or self.headers.get("X-JW-Token") != self.app.token:
            return self._send(403, '"forbidden"')
        routes = {"/api/hosts": self.app.api_set_hosts, "/api/settings": self.app.api_settings,
                  "/api/watch": self.app.api_watch, "/api/unwatch": self.app.api_unwatch,
                  "/api/clear-done": self.app.api_clear_done, "/api/clear-events": self.app.api_clear_events,
                  "/api/refresh": self.app.api_refresh, "/api/test-notify": self.app.api_test_notify,
                  "/api/autostart": self.app.api_autostart, "/api/quit": self.app.api_quit}
        fn = routes.get(urllib.parse.urlparse(self.path).path)
        if not fn:
            return self._send(404, '"not found"')
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
            self._send(200, json.dumps(fn(body) or {"ok": True}))
        except (ValueError, TypeError) as e:
            self._send(400, json.dumps({"error": str(e)}))


# ---------------------------------------------------------------- service + window

def run_service(port, hosts, interval):
    """The background checker: polls servers, sends notifications, serves the UI. One per user."""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    lock = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("jobwatch is already running")
        return
    if not shutil.which("ssh"):
        print("warning: no ssh client on PATH; only 'local' will work", file=sys.stderr)
    app = App()
    if hosts:
        app.api_set_hosts({"enabled": app.enabled + hosts,
                           "custom": app.custom_hosts + [h for h in hosts if h not in app.available_hosts()]})
    if interval:
        app.api_settings({"poll_seconds": interval})
    Handler.app = app
    for p in range(port, port + 20):
        try:
            app.server = ThreadingHTTPServer(("127.0.0.1", p), Handler)
            break
        except OSError:
            continue
    if app.server is None:
        sys.exit("could not bind a port in %d-%d" % (port, port + 19))
    app.server.daemon_threads = True
    port = app.server.server_address[1]
    fd = os.open(str(RUN_FILE), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump({"pid": os.getpid(), "port": port, "token": app.token}, f)
    print("jobwatch checker running at http://127.0.0.1:%d/  (state in %s)" % (port, STATE_FILE), flush=True)
    threading.Thread(target=app.loop, daemon=True).start()
    try:
        app.server.serve_forever()
    except KeyboardInterrupt:
        print()
    finally:
        app.save()
        try:
            RUN_FILE.unlink()
        except OSError:
            pass


def running_service():
    """{port, token, url} of the running checker, or None."""
    try:
        info = json.loads(RUN_FILE.read_text())
        info["url"] = "http://127.0.0.1:%d/" % info["port"]
        urllib.request.urlopen(info["url"] + "api/ping", timeout=2).read()
        return info
    except Exception:
        return None


def call_service(info, path, body):
    req = urllib.request.Request(info["url"] + path.lstrip("/"), data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json", "X-JW-Token": info["token"]})
    return urllib.request.urlopen(req, timeout=10).read()


def start_service(port):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    log = open(LOG_FILE, "a")
    subprocess.Popen([sys.executable, os.path.realpath(__file__), "--service", "--port", str(port)],
                     stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    for _ in range(100):
        time.sleep(0.1)
        info = running_service()
        if info:
            return info
    sys.exit("the jobwatch checker did not start; see %s" % LOG_FILE)


def open_window(url, use_browser):
    if not use_browser:
        try:
            import gi
            gi.require_version("Gtk", "3.0")
            try:
                gi.require_version("WebKit2", "4.1")
            except ValueError:
                gi.require_version("WebKit2", "4.0")
            from gi.repository import GLib, Gtk, WebKit2
        except Exception:
            pass
        else:
            GLib.set_prgname("jobwatch")   # matches StartupWMClass so the panel groups it with the menu entry
            GLib.set_application_name("jobwatch")
            win = Gtk.Window(title="jobwatch")
            win.set_default_size(1280, 820)
            win.set_icon_name("jobwatch")
            view = WebKit2.WebView()
            view.load_uri(url)
            win.add(view)
            win.connect("destroy", Gtk.main_quit)
            win.show_all()
            Gtk.main()
            return
        for b in ("google-chrome", "chromium", "chromium-browser", "brave-browser", "microsoft-edge"):
            if sys.platform.startswith("linux") and shutil.which(b):
                subprocess.Popen([b, "--app=" + url], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                 start_new_session=True)
                return
    webbrowser.open(url)


def main():
    ap = argparse.ArgumentParser(description="Watch jobs on your SSH servers and get notified when they finish.")
    ap.add_argument("--hosts", nargs="*", default=[], help="servers to enable (names from ~/.ssh/config, or 'local')")
    ap.add_argument("--interval", type=int, help="seconds between checks (saved)")
    ap.add_argument("--port", type=int, default=8765, help="local port for the checker (default 8765)")
    ap.add_argument("--browser", action="store_true", help="open in your web browser instead of an app window")
    ap.add_argument("--no-window", action="store_true", help="just make sure the checker is running")
    ap.add_argument("--service", action="store_true", help="run the checker in this process (used at login)")
    ap.add_argument("--quit", action="store_true", help="stop the background checker")
    ap.add_argument("--version", action="version", version="jobwatch " + __version__)
    a = ap.parse_args()

    if a.service:
        return run_service(a.port, a.hosts, a.interval)
    info = running_service()
    if a.quit:
        if info:
            call_service(info, "/api/quit", {})
            print("jobwatch stopped")
        else:
            print("jobwatch is not running")
        return
    info = info or start_service(a.port)
    if a.hosts:
        current = json.loads(urllib.request.urlopen(info["url"] + "api/hosts", timeout=10).read())
        call_service(info, "/api/hosts", {"enabled": current["enabled"] + a.hosts,
                                          "custom": current["custom"] + [h for h in a.hosts if h not in current["available"]]})
    if a.interval:
        call_service(info, "/api/settings", {"poll_seconds": a.interval})
    if a.no_window:
        print("jobwatch checker running at " + info["url"])
    else:
        open_window(info["url"], a.browser)


# ---------------------------------------------------------------- UI

PAGE = r"""<!doctype html>
<html lang="en" data-theme="@THEME@">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>jobwatch</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><rect width='32' height='32' rx='8' fill='%235b7cfa'/><path d='M9 17l5 5 9-11' stroke='white' stroke-width='3.5' fill='none' stroke-linecap='round' stroke-linejoin='round'/></svg>">
<style>
:root{
  --bg:#0e1014; --panel:#15181e; --panel2:#1b1f27; --hover:#20252e; --border:#262b35; --text:#e7e9ee; --muted:#8a92a2; --faint:#5b6372;
  --accent:#6b8cff; --accent-ink:#fff; --run:#5aa9e6; --pend:#e0a43a; --ok:#3fb27f; --bad:#ef6461; --idle:#7c8494;
  --shadow:0 1px 2px rgba(0,0,0,.3),0 8px 24px rgba(0,0,0,.18); color-scheme:dark;
}
@media (prefers-color-scheme: light){:root:not([data-theme="dark"]){
  --bg:#f4f5f8; --panel:#ffffff; --panel2:#f2f4f7; --hover:#eef1f6; --border:#e1e4ea; --text:#1a1e26; --muted:#677083; --faint:#9aa1ae;
  --accent:#3b63e6; --run:#2f80c9; --pend:#b97c10; --ok:#1f8f5f; --bad:#d23f3c; --idle:#7c8494;
  --shadow:0 1px 2px rgba(20,25,40,.06),0 8px 24px rgba(20,25,40,.06); color-scheme:light;
}}
:root[data-theme="light"]{
  --bg:#f4f5f8; --panel:#ffffff; --panel2:#f2f4f7; --hover:#eef1f6; --border:#e1e4ea; --text:#1a1e26; --muted:#677083; --faint:#9aa1ae;
  --accent:#3b63e6; --run:#2f80c9; --pend:#b97c10; --ok:#1f8f5f; --bad:#d23f3c; --idle:#7c8494;
  --shadow:0 1px 2px rgba(20,25,40,.06),0 8px 24px rgba(20,25,40,.06); color-scheme:light;
}
*{box-sizing:border-box}
html,body{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
button,input{font:inherit;color:inherit}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:12.5px}
.muted{color:var(--muted)}
header{position:sticky;top:0;z-index:5;display:flex;align-items:center;gap:16px;padding:12px 24px;background:color-mix(in srgb,var(--bg) 85%,transparent);backdrop-filter:blur(10px);border-bottom:1px solid var(--border)}
.brand{display:flex;align-items:center;gap:10px;font-weight:650;font-size:16px;letter-spacing:-.01em}
.brand i{width:26px;height:26px;border-radius:7px;background:var(--accent);display:grid;place-items:center}
#meta{color:var(--muted);font-size:13px;flex:1;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.btn{border:1px solid var(--border);background:var(--panel);border-radius:8px;padding:6px 12px;cursor:pointer;display:inline-flex;align-items:center;gap:6px;white-space:nowrap}
.btn:hover{background:var(--hover)}
.btn.primary{background:var(--accent);border-color:var(--accent);color:var(--accent-ink)}
.btn.primary:hover{filter:brightness(1.08)}
.btn:disabled{opacity:.45;cursor:default;filter:none}
.btn.ghost{border-color:transparent;background:transparent}
.btn.ghost:hover{background:var(--hover)}
.btn.small{padding:3px 9px;font-size:12.5px}
input[type=text],input[type=number]{background:var(--panel2);border:1px solid var(--border);border-radius:8px;padding:6px 10px;min-width:0}
input:focus{outline:2px solid color-mix(in srgb,var(--accent) 55%,transparent);outline-offset:-1px}
input[type=checkbox]{accent-color:var(--accent);width:15px;height:15px;margin:0;cursor:pointer}
main{display:grid;grid-template-columns:minmax(0,1fr) 320px;gap:20px;padding:20px 24px 60px;max-width:1500px;margin:0 auto}
@media (max-width:1050px){main{grid-template-columns:minmax(0,1fr)}}
@media (max-width:600px){main{padding:16px}header{padding:10px 16px;gap:10px}#meta{display:none}}
.section-title{font-size:12px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);font-weight:600;margin:4px 2px 10px;display:flex;align-items:center;gap:8px}
.section-title .btn{margin-left:auto;text-transform:none;letter-spacing:0;font-weight:500}
.card{background:var(--panel);border:1px solid var(--border);border-radius:12px;box-shadow:var(--shadow)}
#watches{margin-bottom:22px}
.watch-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:12px}
.watch{padding:12px 14px;position:relative}
.watch.done{border-color:color-mix(in srgb,var(--ok) 60%,var(--border))}
.watch.done.failed{border-color:color-mix(in srgb,var(--bad) 60%,var(--border))}
.watch .wtop{display:flex;align-items:baseline;gap:8px;padding-right:22px}
.watch .wname{font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.watch .whost{color:var(--muted);font-size:12px;white-space:nowrap}
.watch .x{position:absolute;top:7px;right:7px;width:24px;height:24px;padding:0;justify-content:center;color:var(--muted)}
.bar{height:6px;background:var(--panel2);border-radius:99px;margin:10px 0 7px;overflow:hidden;display:flex}
.bar span{display:block;height:100%}
.wmeta{font-size:12.5px;color:var(--muted)}
.chips{display:flex;flex-wrap:wrap;gap:3px;margin-top:8px}
.chips i{width:9px;height:9px;border-radius:2.5px;background:var(--idle)}
.hosts{display:flex;flex-direction:column;gap:16px}
.host-head{display:flex;align-items:center;gap:10px;padding:12px 14px 12px 16px;cursor:pointer;user-select:none}
.host-head h2{font-size:15px;margin:0;font-weight:620}
.dot{width:9px;height:9px;border-radius:50%;background:var(--idle);flex:none;box-shadow:0 0 0 3px color-mix(in srgb,currentColor 0%,transparent)}
.dot.ok{background:var(--ok)} .dot.error{background:var(--bad)} .dot.polling{animation:pulse 1s ease-in-out infinite}
@keyframes pulse{50%{opacity:.35}}
.tag{font-size:11px;padding:1px 7px;border-radius:99px;background:var(--panel2);color:var(--muted);border:1px solid var(--border)}
.stats{color:var(--muted);font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.stats b{color:var(--text);font-weight:600}
.spacer{flex:1}
.age{color:var(--faint);font-size:12px;white-space:nowrap}
.chev{transition:transform .15s;color:var(--muted)}
.host.collapsed .chev{transform:rotate(-90deg)}
.host.collapsed .host-body{display:none}
.host-err{margin:0 14px 10px;padding:8px 12px;border-radius:8px;background:color-mix(in srgb,var(--bad) 12%,transparent);color:var(--bad);font-size:13px;word-break:break-word}
.host-err:empty{display:none}
.tabs{display:flex;gap:2px;padding:0 12px;border-bottom:1px solid var(--border)}
.tab{border:0;background:none;padding:8px 10px;cursor:pointer;color:var(--muted);border-bottom:2px solid transparent;margin-bottom:-1px}
.tab.on{color:var(--text);border-bottom-color:var(--accent)}
.tab .n{font-size:11.5px;background:var(--panel2);padding:0 6px;border-radius:99px;margin-left:4px}
.toolbar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;padding:10px 14px;border-bottom:1px solid var(--border);background:var(--panel2)}
.toolbar .grp{display:flex;gap:6px;align-items:center;flex-wrap:wrap}
.toolbar .grp + .grp{padding-left:10px;border-left:1px solid var(--border)}
.toolbar input{width:170px}
.selinfo{font-size:12.5px;color:var(--muted);min-width:70px}
.tablewrap{overflow-x:auto;max-height:520px;overflow-y:auto}
table{width:100%;border-collapse:collapse}
th{position:sticky;top:0;background:var(--panel);text-align:left;font-weight:550;font-size:12px;color:var(--muted);padding:8px 10px;border-bottom:1px solid var(--border);white-space:nowrap;z-index:1}
td{padding:7px 10px;border-bottom:1px solid var(--border);white-space:nowrap;vertical-align:middle}
tr:last-child td{border-bottom:0}
tbody tr:hover td{background:var(--hover)}
tbody tr.sel td{background:color-mix(in srgb,var(--accent) 10%,transparent)}
td.cb,th.cb{width:30px;padding-right:0}
td.name{max-width:380px;overflow:hidden;text-overflow:ellipsis}
td.dim{color:var(--muted)}
.bell{color:var(--accent);margin-left:6px;font-size:12px}
.empty{padding:26px;text-align:center;color:var(--muted)}
.pill{display:inline-flex;align-items:center;gap:6px;font-size:12px;font-weight:550;padding:2px 9px 2px 8px;border-radius:99px;background:color-mix(in srgb,var(--c) 14%,transparent);color:var(--c)}
.pill::before{content:"";width:6px;height:6px;border-radius:50%;background:var(--c)}
.c-run{--c:var(--run)} .c-pend{--c:var(--pend)} .c-ok{--c:var(--ok)} .c-bad{--c:var(--bad)} .c-idle{--c:var(--idle)}
.chips i.c-run,.chips i.c-pend,.chips i.c-ok,.chips i.c-bad,.chips i.c-idle{background:var(--c)}
aside .card{padding:4px 0}
.ev{display:flex;gap:10px;padding:10px 14px;border-bottom:1px solid var(--border)}
.ev:last-child{border-bottom:0}
.ev .ic{width:22px;height:22px;border-radius:50%;flex:none;display:grid;place-items:center;font-size:12px;font-weight:700;color:#fff;background:var(--ok);margin-top:1px}
.ev .ic.bad{background:var(--bad)}
.ev .t{font-weight:560;word-break:break-word}
.ev .b{color:var(--muted);font-size:12.5px;word-break:break-word}
.ev .when{color:var(--faint);font-size:11.5px;margin-top:2px}
.ev.quiet .t{font-weight:450}
.blank{padding:60px 20px;text-align:center}
.blank h3{margin:0 0 6px;font-size:18px}
.blank p{color:var(--muted);margin:0 0 18px}
#offline{display:none;background:var(--bad);color:#fff;text-align:center;padding:6px;font-size:13px}
/* drawer */
.scrim{position:fixed;inset:0;background:rgba(0,0,0,.35);z-index:20;display:none}
.drawer{position:fixed;top:0;right:0;bottom:0;width:min(440px,100%);background:var(--panel);border-left:1px solid var(--border);z-index:21;transform:translateX(100%);transition:transform .2s ease;display:flex;flex-direction:column;box-shadow:var(--shadow)}
.open .scrim{display:block} .open .drawer{transform:none}
.drawer .dh{display:flex;align-items:center;padding:14px 18px;border-bottom:1px solid var(--border)}
.drawer .dh h3{margin:0;font-size:16px;flex:1}
.drawer .db{overflow:auto;padding:6px 18px 18px;flex:1}
.drawer .df{padding:12px 18px;border-top:1px solid var(--border);display:flex;gap:8px;justify-content:flex-end}
.fs{margin-top:16px}
.fs h4{margin:0 0 8px;font-size:12px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted)}
.hostlist{border:1px solid var(--border);border-radius:10px;max-height:260px;overflow:auto}
.hostlist label{display:flex;align-items:center;gap:10px;padding:8px 12px;border-bottom:1px solid var(--border);cursor:pointer}
.hostlist label:last-child{border-bottom:0}
.hostlist label:hover{background:var(--hover)}
.row{display:flex;gap:8px;align-items:center;margin:8px 0}
.row input[type=text]{flex:1}
.row input[type=number]{width:90px}
.check{display:flex;gap:10px;align-items:flex-start;margin:10px 0;cursor:pointer}
.check input{margin-top:3px}
.hint{font-size:12.5px;color:var(--muted)}
#toast{position:fixed;bottom:20px;left:50%;transform:translateX(-50%) translateY(20px);background:var(--text);color:var(--bg);padding:9px 16px;border-radius:9px;opacity:0;transition:.2s;z-index:30;pointer-events:none;font-size:13.5px}
#toast.show{opacity:1;transform:translateX(-50%)}
</style>
</head>
<body>
<div id="offline">Lost connection to jobwatch — is it still running?</div>
<header>
  <div class="brand"><i><svg width="16" height="16" viewBox="0 0 32 32"><path d="M8 17l5 5 11-12" stroke="#fff" stroke-width="4" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg></i>jobwatch</div>
  <div id="meta"></div>
  <button class="btn" id="themeBtn"></button>
  <button class="btn" id="refreshBtn" title="Poll all servers now">↻ Refresh</button>
  <button class="btn" id="settingsBtn">⚙ Settings</button>
</header>
<main>
  <div>
    <section id="watches" hidden></section>
    <div class="section-title" id="serversTitle" hidden>Servers</div>
    <section class="hosts" id="hosts"></section>
  </div>
  <aside>
    <div class="section-title">Activity <button class="btn ghost small" id="clearEvents">Clear</button></div>
    <div class="card" id="events"></div>
  </aside>
</main>

<div id="drawerWrap">
  <div class="scrim" id="scrim"></div>
  <div class="drawer" role="dialog" aria-label="Settings">
    <div class="dh"><h3>Settings</h3><button class="btn ghost" id="closeDrawer">✕</button></div>
    <div class="db">
      <div class="fs">
        <h4>Servers</h4>
        <div class="hint" style="margin-bottom:8px">From <span class="mono">~/.ssh/config</span>. Each must log in without a password prompt.</div>
        <div class="hostlist" id="hostlist"></div>
        <div class="row"><input type="text" id="customHost" placeholder="add another, e.g. user@host"><button class="btn" id="addHost">Add</button></div>
      </div>
      <div class="fs">
        <h4>Polling</h4>
        <div class="row">Check every <input type="number" id="pollSeconds" min="15" max="3600"> seconds</div>
        <div class="row">Show jobs finished in the last <input type="number" id="historyHours" min="1" max="720"> hours</div>
      </div>
      <div class="fs">
        <h4>Notifications</h4>
        <label class="check"><input type="checkbox" id="notifyEvery"><span>Notify for every job that finishes<div class="hint">Jobs in a watch are reported once, when the whole watch is done.</div></span></label>
        <label class="check"><input type="checkbox" id="desktopNotif"><span>Desktop notifications<div class="hint">macOS Notification Center, or notify-send on Linux.</div></span></label>
        <label class="check"><input type="checkbox" id="browserNotif"><span>Browser notifications<div class="hint">Only when jobwatch is open in a web browser tab (<span class="mono">jobwatch --browser</span>).</div></span></label>
        <div class="row"><input type="text" id="ntfyTopic" placeholder="ntfy topic for phone push (optional)"></div>
        <div class="hint">Sends job names to <span class="mono">ntfy.sh</span> (or a full URL to your own ntfy server). Use a hard-to-guess topic.</div>
        <div class="row"><button class="btn" id="testNotif">Send test notification</button></div>
      </div>
      <div class="fs">
        <h4>Servers without Slurm</h4>
        <div class="hint">Your processes whose command line contains any of these are tracked as jobs.</div>
        <div class="row"><input type="text" id="patterns" placeholder="python, torchrun"></div>
        <div class="hint">…unless it contains any of these:</div>
        <div class="row"><input type="text" id="excludes" placeholder=".vscode-server"></div>
      </div>
      <div class="fs">
        <h4>App</h4>
        <label class="check" id="autostartRow"><input type="checkbox" id="autostart"><span>Check in the background from login<div class="hint">Notifications arrive even if you never open this window.</div></span></label>
        <div class="hint">Closing this window keeps checking in the background.</div>
        <div class="row"><button class="btn" id="quitBtn">Stop background checking</button></div>
      </div>
    </div>
    <div class="df"><button class="btn" id="cancelDrawer">Cancel</button><button class="btn primary" id="saveDrawer">Save</button></div>
  </div>
</div>
<div id="toast"></div>

<script>
const TOKEN = "@TOKEN@";
const $ = (s, el=document) => el.querySelector(s);
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const ACTIVE = new Set(['RUNNING','PENDING','REQUEUED','SUSPENDED','COMPLETING','CONFIGURING','RESIZING','SIGNALING','STAGE_OUT','STOPPED','REQUEUE_FED','REQUEUE_HOLD','RESV_DEL_HOLD']);
const GOOD = new Set(['COMPLETED','ENDED']);
let S = null, lastSeq = null, stopped = false;
const sel = new Map(), cards = new Map();
let ui = {tabs:{}, collapsed:{}};
try { ui = Object.assign(ui, JSON.parse(localStorage.getItem('jobwatch-ui') || '{}')); } catch (e) {}
const saveUi = () => { try { localStorage.setItem('jobwatch-ui', JSON.stringify(ui)); } catch (e) {} };

function stateClass(s){
  if (['RUNNING','COMPLETING','CONFIGURING','RESIZING','SIGNALING','STAGE_OUT'].includes(s)) return 'c-run';
  if (ACTIVE.has(s)) return 'c-pend';
  if (s === 'COMPLETED' || s === 'ENDED') return 'c-ok';
  if (s === 'CANCELLED' || s === 'PREEMPTED' || s === '?') return 'c-idle';
  return 'c-bad';
}
const pill = s => `<span class="pill ${stateClass(s)}">${esc(s.replace(/_/g,' ').toLowerCase())}</span>`;
function ago(t){
  if (!t) return 'never';
  const s = Math.max(0, Math.round(Date.now()/1000 - t));
  if (s < 60) return s + 's ago'; if (s < 3600) return Math.floor(s/60) + 'm ago';
  if (s < 86400) return Math.floor(s/3600) + 'h ago'; return Math.floor(s/86400) + 'd ago';
}
function fmtEnd(s){
  const m = /^(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d)/.exec(s || '');
  if (!m) return s || '';
  return `${'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split(' ')[+m[2]-1]} ${+m[3]} ${m[4]}:${m[5]}`;
}
function toast(msg){ const t = $('#toast'); t.textContent = msg; t.classList.add('show'); clearTimeout(t._h); t._h = setTimeout(() => t.classList.remove('show'), 2200); }
async function api(path, body){
  const r = await fetch(path, {method:'POST', headers:{'Content-Type':'application/json','X-JW-Token':TOKEN}, body:JSON.stringify(body || {})});
  const j = await r.json().catch(() => ({}));
  if (!r.ok) { toast(j.error || 'Request failed'); throw new Error(j.error); }
  return j;
}
const selOf = h => { if (!sel.has(h)) sel.set(h, new Set()); return sel.get(h); };
const baseId = id => id.split(/[_+]/)[0];

async function tick(){
  if (stopped) return;
  try {
    const r = await fetch('/api/state', {cache:'no-store'});
    S = await r.json();
    $('#offline').style.display = 'none';
    render();
  } catch (e) { $('#offline').style.display = 'block'; }
}

function render(){
  if (S.settings.theme !== document.documentElement.dataset.theme) showTheme(S.settings.theme);
  renderMeta(); renderWatches(); renderHosts(); renderEvents(); browserNotify();
}

function renderMeta(){
  if (!S) return;
  const n = S.enabled.length, p = S.settings.poll_seconds;
  const busy = S.hosts.some(h => h.polling);
  const next = Math.max(0, Math.round(S.next_poll - Date.now()/1000));
  $('#meta').textContent = !n ? 'No servers selected' :
    `${n} server${n>1?'s':''} · every ${p}s · ` + (busy ? 'checking now…' : `next check in ${next}s`);
}

function renderWatches(){
  const box = $('#watches');
  if (!S.watches.length) { box.hidden = true; box.innerHTML = ''; return; }
  box.hidden = false;
  const anyDone = S.watches.some(w => w.done);
  box.innerHTML = `<div class="section-title">Watching${anyDone ? '<button class="btn ghost small" data-cleardone>Clear finished</button>' : ''}</div><div class="watch-grid">` +
    S.watches.map(w => {
      const ms = Object.entries(w.members), total = ms.length;
      const fin = ms.filter(([,m]) => m.active === false);
      const bad = fin.filter(([,m]) => !GOOD.has(m.state)).length, good = fin.length - bad;
      let meta;
      if (!total) meta = `Waiting for jobs matching <span class="mono">${esc(w.pattern)}</span>`;
      else if (w.done) meta = `Finished ${ago(w.done_at)} · ${good} ok` + (bad ? ` · <span style="color:var(--bad)">${bad} not ok</span>` : '');
      else meta = `${fin.length} / ${total} finished` + (bad ? ` · <span style="color:var(--bad)">${bad} not ok</span>` : '') +
                  (w.kind === 'pattern' ? ` · <span class="mono">${esc(w.pattern)}</span>` : '');
      const chips = ms.slice(0, 80).map(([id, m]) => `<i class="${stateClass(m.state || '?')}" title="${esc(id)} ${esc(m.name)} — ${esc(m.state || '')}"></i>`).join('') +
                    (total > 80 ? `<span class="muted" style="font-size:11px">+${total-80}</span>` : '');
      return `<div class="card watch ${w.done ? 'done' : ''} ${bad ? 'failed' : ''}">
        <div class="wtop"><span class="wname" title="${esc(w.name)}">${w.done ? (bad ? '⚠️ ' : '✅ ') : ''}${esc(w.name)}</span><span class="whost">${esc(w.host)}</span></div>
        <button class="btn ghost x" data-unwatch="${esc(w.id)}" title="Stop watching">✕</button>
        <div class="bar"><span style="width:${total ? good/total*100 : 0}%;background:var(--ok)"></span><span style="width:${total ? bad/total*100 : 0}%;background:var(--bad)"></span></div>
        <div class="wmeta">${meta}</div>
        ${total > 1 ? `<div class="chips">${chips}</div>` : ''}
      </div>`;
    }).join('') + '</div>';
}
$('#watches').addEventListener('click', async e => {
  const u = e.target.closest('[data-unwatch]');
  if (u) { await api('/api/unwatch', {id: u.dataset.unwatch}); tick(); }
  if (e.target.closest('[data-cleardone]')) { await api('/api/clear-done'); tick(); }
});

function renderHosts(){
  const wrap = $('#hosts');
  $('#serversTitle').hidden = !S.enabled.length;
  if (!S.enabled.length) {
    cards.clear();
    wrap.innerHTML = `<div class="card blank"><h3>Pick the servers to watch</h3><p>jobwatch lists the hosts in your ~/.ssh/config. Choose which ones to check.</p><button class="btn primary" onclick="openDrawer()">Choose servers</button></div>`;
    return;
  }
  if (!cards.size) wrap.innerHTML = '';
  for (const [name, el] of cards) if (!S.enabled.includes(name)) { el.remove(); cards.delete(name); }
  S.hosts.forEach((h, i) => {
    let el = cards.get(h.name);
    if (!el) { el = buildCard(h.name); cards.set(h.name, el); }
    if (wrap.children[i] !== el) wrap.insertBefore(el, wrap.children[i] || null);
    updateCard(el, h);
  });
}

function buildCard(name){
  const el = document.createElement('div');
  el.className = 'card host' + (ui.collapsed[name] ? ' collapsed' : '');
  el.innerHTML = `
    <div class="host-head"><span class="dot"></span><h2>${esc(name)}</h2><span class="tag backend"></span>
      <span class="stats"></span><span class="spacer"></span><span class="age"></span><span class="chev">▾</span></div>
    <div class="host-err"></div>
    <div class="host-body">
      <div class="tabs"><button class="tab" data-tab="active">Active<span class="n na"></span></button><button class="tab" data-tab="finished">Finished<span class="n nf"></span></button></div>
      <div class="toolbar">
        <div class="grp"><span class="selinfo"></span><input type="text" class="wname" placeholder="name (optional)"><button class="btn primary small wsel" disabled>🔔 Notify when selected finish</button></div>
        <div class="grp"><input type="text" class="wpat" placeholder="name pattern, e.g. sweep-*"><button class="btn small wpatb">Watch pattern</button><button class="btn small wall" title="Notify once nothing is left running or pending here">Notify when all finish</button></div>
      </div>
      <div class="tablewrap"><table><thead></thead><tbody></tbody></table></div>
    </div>`;
  $('.host-head', el).onclick = () => { el.classList.toggle('collapsed'); ui.collapsed[name] = el.classList.contains('collapsed'); saveUi(); };
  el.querySelectorAll('.tab').forEach(b => b.onclick = () => { ui.tabs[name] = b.dataset.tab; saveUi(); updateCard(el, S.hosts.find(h => h.name === name)); });
  const watch = async body => {
    const w = await api('/api/watch', Object.assign({host: name}, body));
    toast(`Watching “${w.name}” on ${name}`); tick();
  };
  $('.wsel', el).onclick = async () => {
    const ids = [...selOf(name)]; if (!ids.length) return;
    await watch({ids, name: $('.wname', el).value.trim()});
    selOf(name).clear(); $('.wname', el).value = '';
  };
  const pat = $('.wpat', el);
  $('.wpatb', el).onclick = async () => { if (!pat.value.trim()) { pat.focus(); return; } await watch({pattern: pat.value.trim()}); pat.value = ''; };
  pat.onkeydown = e => { if (e.key === 'Enter') $('.wpatb', el).click(); };
  $('.wname', el).onkeydown = e => { if (e.key === 'Enter') $('.wsel', el).click(); };
  $('.wall', el).onclick = () => watch({pattern: '*', name: 'all jobs'});
  $('tbody', el).addEventListener('click', e => {
    const tr = e.target.closest('tr[data-id]'); if (!tr) return;
    const s = selOf(name), id = tr.dataset.id;
    s.has(id) ? s.delete(id) : s.add(id);
    updateCard(el, S.hosts.find(h => h.name === name));
  });
  $('thead', el).addEventListener('change', e => {
    if (!e.target.matches('input')) return;
    const h = S.hosts.find(h => h.name === name), s = selOf(name);
    if (e.target.checked) h.active.forEach(j => s.add(j.id)); else s.clear();
    updateCard(el, h);
  });
  return el;
}

function updateCard(el, h){
  if (!h) return;
  const name = h.name, slurm = h.backend !== 'ps';
  const dot = $('.dot', el);
  dot.className = 'dot ' + (h.status === 'ok' ? 'ok' : h.status === 'error' ? 'error' : '') + (h.polling ? ' polling' : '');
  dot.title = h.status;
  $('.backend', el).textContent = h.backend === 'ps' ? 'processes' : h.backend || '…';
  $('.backend', el).style.display = h.backend ? '' : 'none';
  const run = h.active.filter(j => stateClass(j.state) === 'c-run').length, pend = h.active.length - run;
  const bad = h.finished.filter(j => !GOOD.has(j.state)).length;
  $('.stats', el).innerHTML = h.last_ok == null ? (h.status === 'error' ? '' : 'connecting…') :
    `<b>${run}</b> running` + (slurm ? ` · <b>${pend}</b> pending` : '') + ` · <b>${h.finished.length}</b> finished` + (bad ? ` (<span style="color:var(--bad)">${bad} not ok</span>)` : '');
  $('.age', el).textContent = h.polling ? 'checking…' : h.last_ok ? 'updated ' + ago(h.last_ok) : '';
  $('.host-err', el).textContent = h.status === 'error' ? h.error + (h.last_ok ? ` — showing data from ${ago(h.last_ok)}` : '') : '';
  $('.na', el).textContent = h.active.length; $('.nf', el).textContent = h.finished.length;

  const tab = ui.tabs[name] || 'active';
  el.querySelectorAll('.tab').forEach(b => b.classList.toggle('on', b.dataset.tab === tab));
  $('.toolbar', el).style.display = tab === 'active' ? '' : 'none';

  const s = selOf(name), ids = new Set(h.active.map(j => j.id));
  for (const id of [...s]) if (!ids.has(id)) s.delete(id);
  $('.selinfo', el).textContent = s.size ? `${s.size} selected` : 'Select jobs ↓';
  $('.wsel', el).disabled = !s.size;
  const watched = new Set(S.watches.filter(w => w.host === name && !w.done).flatMap(w => Object.keys(w.members)));

  const thead = $('thead', el), tbody = $('tbody', el);
  if (tab === 'active') {
    const allOn = h.active.length && s.size === h.active.length;
    thead.innerHTML = `<tr><th class="cb"><input type="checkbox" ${allOn ? 'checked' : ''} ${h.active.length ? '' : 'disabled'}></th>` +
      (slurm ? '<th>Job ID</th><th>Name</th><th>State</th><th>Elapsed</th><th>Limit</th><th>Nodes / reason</th><th>Partition</th>'
             : '<th>PID</th><th>Command</th><th>State</th><th>Elapsed</th>') + '</tr>';
    const rows = [...h.active].sort((a, b) => (stateClass(a.state) === 'c-run' ? 0 : 1) - (stateClass(b.state) === 'c-run' ? 0 : 1));
    tbody.innerHTML = rows.length ? rows.map(j => `<tr data-id="${esc(j.id)}" class="${s.has(j.id) ? 'sel' : ''}">
        <td class="cb"><input type="checkbox" ${s.has(j.id) ? 'checked' : ''}></td>
        <td class="mono">${esc(j.id)}</td>
        <td class="name" title="${esc(j.name)}">${esc(j.name)}${watched.has(baseId(j.id)) ? '<span class="bell" title="In a watch">🔔</span>' : ''}</td>
        <td>${pill(j.state)}</td><td class="mono">${esc(j.elapsed)}</td>` +
        (slurm ? `<td class="mono dim">${esc(j.limit)}</td><td class="dim">${esc(j.where)}</td><td class="dim">${esc(j.partition)}</td>` : '') + '</tr>').join('')
      : `<tr><td colspan="8" class="empty">${h.last_ok ? 'Nothing running or pending.' : '…'}</td></tr>`;
  } else {
    thead.innerHTML = '<tr>' + (slurm ? '<th>Job ID</th><th>Name</th><th>State</th><th>Exit</th><th>Elapsed</th><th>Ended</th><th>Partition</th>'
                                      : '<th>PID</th><th>Command</th><th>State</th><th>Ran for</th><th>Ended</th>') + '</tr>';
    tbody.innerHTML = h.finished.length ? h.finished.map(j => `<tr>
        <td class="mono">${esc(j.id)}</td><td class="name" title="${esc(j.name)}">${esc(j.name)}</td>
        <td title="${esc(j.state_full || j.state)}">${pill(j.state)}</td>` +
        (slurm ? `<td class="mono dim">${esc(j.exit)}</td>` : '') +
        `<td class="mono">${esc(j.elapsed)}</td><td class="dim">${esc(fmtEnd(j.end))}</td>` +
        (slurm ? `<td class="dim">${esc(j.partition)}</td>` : '') + '</tr>').join('')
      : `<tr><td colspan="7" class="empty">Nothing finished in the last ${S.settings.history_hours}h.</td></tr>`;
  }
}

function renderEvents(){
  const ev = [...S.events].reverse();
  $('#events').innerHTML = ev.length ? ev.map(e => `<div class="ev ${e.notified ? '' : 'quiet'}">
      <div class="ic ${e.failed ? 'bad' : ''}">${e.failed ? '!' : '✓'}</div>
      <div style="min-width:0"><div class="t">${esc(e.title.replace(/^(✅|⚠️)\s*/u, ''))}</div><div class="b">${esc(e.body)}</div>
      <div class="when">${ago(e.at)}${e.notified ? '' : ' · not notified'}</div></div></div>`).join('')
    : '<div class="empty">Finished jobs and watches will show up here.</div>';
}

function browserNotify(){
  const maxSeq = Math.max(0, ...S.events.map(e => e.seq));
  if (lastSeq === null) { lastSeq = maxSeq; return; }
  if (S.settings.browser_notifications && 'Notification' in window && Notification.permission === 'granted')
    S.events.filter(e => e.seq > lastSeq && e.notified).forEach(e => new Notification(e.title, {body: e.body, tag: 'jw' + e.seq}));
  lastSeq = Math.max(lastSeq, maxSeq);
}

/* settings drawer */
let draft = null;
async function openDrawer(){
  const r = await fetch('/api/hosts', {cache:'no-store'}).then(r => r.json());
  draft = {enabled: new Set(r.enabled), custom: [...r.custom], available: r.available};
  renderHostList();
  const st = S.settings;
  $('#pollSeconds').value = st.poll_seconds; $('#historyHours').value = st.history_hours;
  $('#notifyEvery').checked = st.notify_every_job; $('#desktopNotif').checked = st.desktop_notifications;
  $('#browserNotif').checked = st.browser_notifications; $('#ntfyTopic').value = st.ntfy_topic;
  $('#patterns').value = st.process_patterns.join(', '); $('#excludes').value = st.process_exclude.join(', ');
  $('#autostartRow').style.display = S.app.linux ? '' : 'none'; $('#autostart').checked = S.app.autostart;
  $('#drawerWrap').classList.add('open');
}
function renderHostList(){
  const list = draft.available.concat(draft.custom.filter(c => !draft.available.includes(c)));
  $('#hostlist').innerHTML = list.map(h => `<label><input type="checkbox" value="${esc(h)}" ${draft.enabled.has(h) ? 'checked' : ''}>
      <span>${esc(h)}${h === 'local' ? ' <span class="muted">(this machine)</span>' : ''}</span></label>`).join('');
  $('#hostlist').querySelectorAll('input').forEach(i => i.onchange = () => i.checked ? draft.enabled.add(i.value) : draft.enabled.delete(i.value));
}
function closeDrawer(){ $('#drawerWrap').classList.remove('open'); }
$('#settingsBtn').onclick = openDrawer;
$('#scrim').onclick = $('#closeDrawer').onclick = $('#cancelDrawer').onclick = closeDrawer;
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeDrawer(); });
$('#addHost').onclick = () => {
  const v = $('#customHost').value.trim(); if (!v) return;
  if (!draft.custom.includes(v) && !draft.available.includes(v)) draft.custom.push(v);
  draft.enabled.add(v); $('#customHost').value = ''; renderHostList();
};
$('#customHost').onkeydown = e => { if (e.key === 'Enter') $('#addHost').click(); };
$('#browserNotif').onchange = async e => {
  if (e.target.checked && 'Notification' in window && Notification.permission !== 'granted') {
    const p = await Notification.requestPermission();
    if (p !== 'granted') { e.target.checked = false; toast('Browser notifications were blocked'); }
  }
};
$('#saveDrawer').onclick = async () => {
  const ordered = draft.available.concat(draft.custom).filter((h, i, a) => draft.enabled.has(h) && a.indexOf(h) === i);
  await api('/api/hosts', {enabled: ordered, custom: draft.custom});
  await api('/api/settings', {
    poll_seconds: +$('#pollSeconds').value || 60, history_hours: +$('#historyHours').value || 24,
    notify_every_job: $('#notifyEvery').checked, desktop_notifications: $('#desktopNotif').checked,
    browser_notifications: $('#browserNotif').checked, ntfy_topic: $('#ntfyTopic').value,
    process_patterns: $('#patterns').value, process_exclude: $('#excludes').value});
  if (S.app.linux && $('#autostart').checked !== S.app.autostart) await api('/api/autostart', {enabled: $('#autostart').checked});
  closeDrawer(); toast('Saved'); tick();
};
$('#quitBtn').onclick = async () => {
  if (!confirm('Stop checking your servers? No notifications until you open jobwatch again.')) return;
  await api('/api/quit'); closeDrawer(); stopped = true;
  document.body.innerHTML = '<div class="blank"><h3>jobwatch stopped</h3><p>Open jobwatch from your app menu to start it again.</p></div>';
};
$('#testNotif').onclick = async () => {
  await api('/api/test-notify');
  if ($('#browserNotif').checked && 'Notification' in window && Notification.permission === 'granted') new Notification('✅ jobwatch test', {body: 'Notifications are working.'});
  toast('Test notification sent');
};
const THEMES = {auto: ['◐ Auto', 'Theme follows your system. Click for dark.'], dark: ['☾ Dark', 'Click for light.'], light: ['☀ Light', 'Click to follow your system.']};
function showTheme(t){
  document.documentElement.dataset.theme = t;
  $('#themeBtn').textContent = THEMES[t][0]; $('#themeBtn').title = THEMES[t][1];
}
showTheme(document.documentElement.dataset.theme in THEMES ? document.documentElement.dataset.theme : 'auto');
$('#themeBtn').onclick = async () => {
  const next = {auto: 'dark', dark: 'light', light: 'auto'}[document.documentElement.dataset.theme] || 'auto';
  showTheme(next);
  await api('/api/settings', {theme: next});
  if (S) S.settings.theme = next;
};
$('#refreshBtn').onclick = async () => { await api('/api/refresh'); toast('Checking all servers…'); setTimeout(tick, 400); };
$('#clearEvents').onclick = async () => { await api('/api/clear-events'); tick(); };

tick();
setInterval(tick, 3000);
setInterval(() => { if (S) { renderMeta(); document.querySelectorAll('.host').forEach(el => {
  const h = S.hosts.find(x => cards.get(x.name) === el);
  if (h && !h.polling && h.last_ok) $('.age', el).textContent = 'updated ' + ago(h.last_ok); }); } }, 1000);
</script>
</body>
</html>
"""

if __name__ == "__main__":
    main()
