import os
import shlex
import shutil
import socket
import subprocess
import time
from pathlib import Path

from flask import Flask, jsonify, request, send_file


APP = Flask(__name__)
APP_START = time.time()
ROOT_DIR = Path.cwd().resolve()
CURRENT_DIR = ROOT_DIR

TEXT_EXTENSIONS = {
    ".txt",
    ".md",
    ".py",
    ".json",
    ".yaml",
    ".yml",
    ".ini",
    ".cfg",
    ".csv",
    ".log",
    ".xml",
    ".html",
    ".css",
    ".js",
    ".ts",
    ".sh",
    ".bash",
}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".svg"}


def safe_path(raw_path: str | None) -> Path:
    if not raw_path:
        return CURRENT_DIR
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = (CURRENT_DIR / candidate).resolve()
    else:
        candidate = candidate.resolve()
    if ROOT_DIR not in [candidate, *candidate.parents]:
        raise ValueError("Path is outside workspace root")
    return candidate


def get_ip_address() -> str:
    try:
        host = socket.gethostname()
        return socket.gethostbyname(host)
    except Exception:
        return "N/A"


def get_process_count() -> int:
    try:
        output = subprocess.run(["ps", "-e"], capture_output=True, text=True, check=True).stdout
        return max(0, len(output.splitlines()) - 1)
    except Exception:
        return 0


def uptime_text() -> str:
    total = int(time.time() - APP_START)
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def file_kind(path: Path) -> str:
    if path.is_dir():
        return "dir"
    ext = path.suffix.lower()
    if ext in IMAGE_EXTENSIONS:
        return "image"
    if ext in TEXT_EXTENSIONS or ext == "":
        return "text"
    if ext == ".zip":
        return "zip"
    return "file"


def list_dir(path: Path) -> list[dict]:
    rows = []
    for item in sorted(path.iterdir(), key=lambda p: (p.is_file(), p.name.lower())):
        rows.append(
            {
                "name": item.name,
                "path": str(item),
                "is_dir": item.is_dir(),
                "kind": file_kind(item),
                "size": item.stat().st_size if item.is_file() else 0,
                "ext": item.suffix.lower(),
            }
        )
    return rows


def run_linux_like(command: str) -> tuple[str, bool]:
    global CURRENT_DIR
    try:
        parts = shlex.split(command)
    except ValueError as ex:
        return str(ex), False

    if not parts:
        return "", True

    cmd = parts[0]
    args = parts[1:]

    try:
        if cmd in {"help", "?"}:
            return (
                "Builtins: pwd, ls, cd, cat, clear, mkdir, touch, rm, cp, mv, echo, whoami, uname\n"
                "Other commands execute in system shell.",
                True,
            )
        if cmd == "pwd":
            return str(CURRENT_DIR), True
        if cmd == "ls":
            target = safe_path(args[0]) if args else CURRENT_DIR
            if not target.exists():
                return f"ls: cannot access '{target}'", False
            if target.is_file():
                return target.name, True
            lines = [("[D] " if p.is_dir() else "[F] ") + p.name for p in target.iterdir()]
            return "\n".join(sorted(lines)) if lines else "(empty)", True
        if cmd == "cd":
            target = safe_path(args[0]) if args else ROOT_DIR
            if not target.exists() or not target.is_dir():
                return f"cd: no such directory: {target}", False
            CURRENT_DIR = target
            return str(CURRENT_DIR), True
        if cmd == "cat":
            if not args:
                return "cat: missing operand", False
            target = safe_path(args[0])
            if not target.exists() or not target.is_file():
                return "cat: file not found", False
            if file_kind(target) == "image":
                return "image file: open in Files page", True
            return target.read_text(encoding="utf-8", errors="replace"), True
        if cmd == "mkdir":
            if not args:
                return "mkdir: missing operand", False
            safe_path(args[0]).mkdir(parents=True, exist_ok=True)
            return "directory created", True
        if cmd == "touch":
            if not args:
                return "touch: missing operand", False
            safe_path(args[0]).touch(exist_ok=True)
            return "file touched", True
        if cmd == "rm":
            if not args:
                return "rm: missing operand", False
            recursive = any(flag in args for flag in ("-r", "-rf", "-fr"))
            target_arg = [a for a in args if not a.startswith("-")]
            if not target_arg:
                return "rm: missing target", False
            target = safe_path(target_arg[0])
            if not target.exists():
                return "rm: target not found", False
            if target.is_dir():
                if not recursive:
                    return "rm: is a directory (use -r)", False
                shutil.rmtree(target)
            else:
                target.unlink()
            return "removed", True
        if cmd == "cp":
            if len(args) < 2:
                return "cp: missing operands", False
            src = safe_path(args[0])
            dst = safe_path(args[1])
            if src.is_dir():
                shutil.copytree(src, dst, dirs_exist_ok=True)
            else:
                shutil.copy2(src, dst)
            return "copied", True
        if cmd == "mv":
            if len(args) < 2:
                return "mv: missing operands", False
            shutil.move(str(safe_path(args[0])), str(safe_path(args[1])))
            return "moved", True
        if cmd == "echo":
            return " ".join(args), True
        if cmd == "clear":
            return "__CLEAR__", True
        if cmd == "whoami":
            return os.getenv("USER") or os.getenv("USERNAME") or "unknown", True
        if cmd == "uname":
            return os.uname().sysname if hasattr(os, "uname") else os.name, True
    except Exception as ex:
        return str(ex), False

    try:
        res = subprocess.run(
            command,
            shell=True,
            cwd=str(CURRENT_DIR),
            capture_output=True,
            text=True,
            timeout=25,
        )
        output = (res.stdout or "") + (res.stderr or "")
        output = output.strip() or "(done)"
        return output, res.returncode == 0
    except Exception as ex:
        return str(ex), False


HTML = """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Neon Server Studio</title>
  <style>
    :root{
      --bg:#070b17; --panel:#101a2f; --panel2:#172644; --line:#2a3a5f;
      --text:#ecf4ff; --muted:#96abd1; --neon:#00d9ff; --ok:#2ed47a; --bad:#ff7b8d;
    }
    *{box-sizing:border-box;font-family:Inter,Segoe UI,Arial,sans-serif}
    body{margin:0;background:radial-gradient(circle at top right,#10264b 0%,var(--bg) 45%);color:var(--text)}
    .wrap{max-width:1400px;margin:20px auto;padding:0 14px}
    .top{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:12px}
    .card{background:linear-gradient(135deg,var(--panel),var(--panel2));border:1px solid #2b416d;border-radius:16px;padding:14px;box-shadow:0 8px 25px #00000055}
    .label{font-size:12px;color:var(--muted);margin-bottom:8px}
    .value{font-size:24px;font-weight:700}
    .tabs{display:flex;gap:8px;margin:12px 0}
    .tab{background:#1a2b4a;border:1px solid var(--line);color:var(--text);padding:10px 14px;border-radius:12px;cursor:pointer;transition:.18s}
    .tab:hover{transform:translateY(-1px) scale(1.02);background:#26416d}
    .tab.active{outline:2px solid #00d9ff44}
    .page{display:none}
    .page.active{display:block}
    .grid{display:grid;grid-template-columns:1.1fr 1fr;gap:12px}
    .panel{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:12px;min-height:520px}
    .panel h3{margin:0 0 10px 0;font-size:17px}
    .toolbar{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px}
    .btn{background:#1a2b4a;color:var(--text);border:1px solid var(--line);padding:8px 11px;border-radius:10px;cursor:pointer;transition:.18s}
    .btn:hover{background:#26416d;transform:scale(1.03)}
    .path{font-size:12px;color:var(--neon);word-break:break-all;margin-bottom:8px}
    .files{height:430px;overflow:auto;border:1px solid var(--line);border-radius:12px;padding:8px;background:#0a1326}
    .file{display:flex;gap:8px;align-items:center;padding:8px;border-radius:9px;cursor:pointer;transition:.14s}
    .file:hover{background:#142341}
    .dot{width:10px;height:10px;border-radius:50%}
    .dot.dir{background:#8fb0ff}.dot.text{background:#9fb7ff}.dot.image{background:#68f0ce}.dot.zip{background:#ffc857}.dot.file{background:#aab7d1}
    .term{height:420px;overflow:auto;border:1px solid var(--line);border-radius:12px;padding:10px;background:#091224;font-family:Consolas,monospace;font-size:13px;white-space:pre-wrap}
    .line-ok{color:var(--text)} .line-err{color:var(--bad)} .line-cmd{color:var(--neon)}
    .cmdrow{display:flex;gap:8px;margin-top:10px}
    input[type=text], textarea{width:100%;background:#0b1325;color:var(--text);border:1px solid var(--line);border-radius:10px;padding:10px}
    textarea{height:380px;resize:vertical;font-family:Consolas,monospace}
    .imgbox{height:380px;display:flex;align-items:center;justify-content:center;background:#0a1326;border:1px solid var(--line);border-radius:12px}
    .imgbox img{max-width:100%;max-height:360px;border-radius:8px}
    .muted{color:var(--muted);font-size:13px}
    @media(max-width:1100px){.grid,.top{grid-template-columns:1fr}}
  </style>
</head>
<body>
<div class="wrap">
  <h2 style="margin:0 0 6px 0;">Neon Server Studio</h2>
  <div class="muted" style="margin-bottom:10px;">Linux-first web panel: console + file manager</div>

  <div class="top">
    <div class="card"><div class="label">Uptime</div><div id="uptime" class="value">--:--:--</div></div>
    <div class="card"><div class="label">IP Address</div><div id="ip" class="value" style="font-size:20px;">-</div></div>
    <div class="card"><div class="label">Active Processes</div><div id="proc" class="value">-</div></div>
  </div>

  <div class="tabs">
    <button class="tab active" data-page="console">Console</button>
    <button class="tab" data-page="files">Files</button>
  </div>

  <div id="page-console" class="page active">
    <div class="panel">
      <h3>Удобная Linux-консоль</h3>
      <div class="path" id="console-path"></div>
      <div id="terminal" class="term"></div>
      <div class="cmdrow">
        <input id="command" type="text" placeholder="Введите команду (ls, cd, cat, rm, mkdir, ...)" />
        <button class="btn" onclick="runCommand()">Run</button>
        <button class="btn" onclick="clearTerminal()">Clear</button>
      </div>
    </div>
  </div>

  <div id="page-files" class="page">
    <div class="grid">
      <div class="panel">
        <h3>Файлы</h3>
        <div class="path" id="files-path"></div>
        <div class="toolbar">
          <button class="btn" onclick="goUp()">Вверх</button>
          <button class="btn" onclick="refreshFiles()">Обновить</button>
          <button class="btn" onclick="createFile()">Новый файл</button>
          <button class="btn" onclick="pickUpload()">Загрузить</button>
          <input id="upload" type="file" multiple style="display:none" />
        </div>
        <div id="file-list" class="files"></div>
      </div>
      <div class="panel">
        <h3>Просмотр / Редактирование</h3>
        <div id="selected-name" style="font-weight:600;margin-bottom:6px;">Ничего не выбрано</div>
        <div id="selected-info" class="muted" style="margin-bottom:8px;">Выберите файл или папку слева.</div>
        <div class="toolbar">
          <button class="btn" onclick="openFolder()">Открыть папку</button>
          <button class="btn" onclick="saveFile()">Сохранить</button>
          <button class="btn" onclick="deleteSelected()">Удалить</button>
        </div>
        <div id="preview"></div>
      </div>
    </div>
  </div>
</div>

<script>
let selected = null;
let selectedType = null;
let selectedName = null;

function addTermLine(text, cls="line-ok"){
  const terminal = document.getElementById("terminal");
  const div = document.createElement("div");
  div.className = cls;
  div.textContent = text;
  terminal.appendChild(div);
  terminal.scrollTop = terminal.scrollHeight;
}

function clearTerminal(){
  document.getElementById("terminal").innerHTML = "";
}

async function fetchStats(){
  const r = await fetch("/api/stats");
  const d = await r.json();
  document.getElementById("uptime").textContent = d.uptime;
  document.getElementById("ip").textContent = d.ip;
  document.getElementById("proc").textContent = d.processes;
  document.getElementById("console-path").textContent = d.current_dir;
  document.getElementById("files-path").textContent = d.current_dir;
}

async function runCommand(){
  const input = document.getElementById("command");
  const cmd = input.value.trim();
  if(!cmd) return;
  addTermLine("$ " + cmd, "line-cmd");
  input.value = "";
  const r = await fetch("/api/run", {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify({command:cmd})});
  const d = await r.json();
  if(d.clear){ clearTerminal(); }
  if(d.output){
    d.output.split("\\n").forEach(line => addTermLine(line, d.ok ? "line-ok" : "line-err"));
  }
  await fetchStats();
  await refreshFiles();
}

async function refreshFiles(){
  const r = await fetch("/api/files");
  const d = await r.json();
  const box = document.getElementById("file-list");
  box.innerHTML = "";
  d.items.forEach(item => {
    const row = document.createElement("div");
    row.className = "file";
    row.innerHTML = `<span class="dot ${item.kind}"></span><div><div>${item.name}</div><div class="muted">${item.is_dir ? "folder" : (item.ext || "file")} ${item.is_dir ? "" : "• " + item.size + " B"}</div></div>`;
    row.onclick = () => selectItem(item.path);
    box.appendChild(row);
  });
}

async function selectItem(path){
  const r = await fetch("/api/read?path=" + encodeURIComponent(path));
  const d = await r.json();
  selected = path;
  selectedType = d.kind;
  selectedName = d.name;
  document.getElementById("selected-name").textContent = d.name;
  document.getElementById("selected-info").textContent = d.info;
  const preview = document.getElementById("preview");
  preview.innerHTML = "";
  if(d.kind === "image"){
    preview.innerHTML = `<div class="imgbox"><img src="/api/image?path=${encodeURIComponent(path)}" alt="${d.name}" /></div>`;
  } else if(d.kind === "text"){
    preview.innerHTML = `<textarea id="editor">${(d.content || "").replaceAll("<","&lt;")}</textarea>`;
  } else {
    preview.innerHTML = `<div class="muted">Невозможно редактировать этот тип файла.</div>`;
  }
}

async function saveFile(){
  if(!selected || selectedType !== "text"){ alert("Выберите текстовый файл."); return; }
  const editor = document.getElementById("editor");
  if(!editor){ alert("Нет редактора."); return; }
  const r = await fetch("/api/save", {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify({path:selected, content:editor.value})});
  const d = await r.json();
  alert(d.message);
}

async function deleteSelected(){
  if(!selected){ alert("Выберите объект."); return; }
  if(!confirm("Удалить " + selectedName + "?")) return;
  const r = await fetch("/api/delete", {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify({path:selected})});
  const d = await r.json();
  alert(d.message);
  selected = null;
  document.getElementById("selected-name").textContent = "Ничего не выбрано";
  document.getElementById("selected-info").textContent = "Выберите файл или папку слева.";
  document.getElementById("preview").innerHTML = "";
  await refreshFiles();
}

async function openFolder(){
  if(!selected){ alert("Выберите папку."); return; }
  const r = await fetch("/api/open", {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify({path:selected})});
  const d = await r.json();
  if(!d.ok){ alert(d.message); return; }
  await fetchStats();
  await refreshFiles();
}

async function goUp(){
  await fetch("/api/up", {method:"POST"});
  await fetchStats();
  await refreshFiles();
}

async function createFile(){
  const name = prompt("Имя файла (например: notes.txt)");
  if(!name) return;
  const r = await fetch("/api/create", {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify({name:name})});
  const d = await r.json();
  alert(d.message);
  await refreshFiles();
}

function pickUpload(){
  document.getElementById("upload").click();
}

document.getElementById("upload").addEventListener("change", async (ev) => {
  const files = ev.target.files;
  if(!files.length) return;
  const fd = new FormData();
  for(const f of files) fd.append("files", f);
  const r = await fetch("/api/upload", {method:"POST", body:fd});
  const d = await r.json();
  alert(d.message);
  await refreshFiles();
});

document.getElementById("command").addEventListener("keydown", (e) => {
  if(e.key === "Enter"){ runCommand(); }
});

document.querySelectorAll(".tab").forEach(tab => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
    tab.classList.add("active");
    document.querySelectorAll(".page").forEach(p => p.classList.remove("active"));
    document.getElementById("page-" + tab.dataset.page).classList.add("active");
  });
});

setInterval(fetchStats, 1000);
fetchStats();
refreshFiles();
addTermLine("Neon web console ready.");
addTermLine("Type `help` for commands.");
</script>
</body>
</html>
"""


@APP.get("/")
def index():
    return HTML


@APP.get("/api/stats")
def api_stats():
    return jsonify(
        {
            "uptime": uptime_text(),
            "ip": get_ip_address(),
            "processes": get_process_count(),
            "current_dir": str(CURRENT_DIR),
        }
    )


@APP.post("/api/run")
def api_run():
    payload = request.get_json(silent=True) or {}
    cmd = (payload.get("command") or "").strip()
    if not cmd:
        return jsonify({"ok": False, "output": "Empty command"})
    out, ok = run_linux_like(cmd)
    return jsonify({"ok": ok, "output": "" if out == "__CLEAR__" else out, "clear": out == "__CLEAR__"})


@APP.get("/api/files")
def api_files():
    try:
        return jsonify({"ok": True, "items": list_dir(CURRENT_DIR)})
    except Exception as ex:
        return jsonify({"ok": False, "items": [], "message": str(ex)})


@APP.get("/api/read")
def api_read():
    raw = request.args.get("path")
    try:
        path = safe_path(raw)
        if path.is_dir():
            return jsonify({"ok": True, "name": path.name, "kind": "dir", "info": "Folder selected"})
        kind = file_kind(path)
        info = f"File | {path.suffix or 'no ext'} | {path.stat().st_size} B"
        if kind == "image":
            return jsonify({"ok": True, "name": path.name, "kind": "image", "info": info})
        if kind == "text":
            content = path.read_text(encoding="utf-8", errors="replace")
            return jsonify({"ok": True, "name": path.name, "kind": "text", "info": info, "content": content})
        return jsonify({"ok": True, "name": path.name, "kind": "file", "info": info})
    except Exception as ex:
        return jsonify({"ok": False, "message": str(ex)})


@APP.get("/api/image")
def api_image():
    raw = request.args.get("path")
    path = safe_path(raw)
    return send_file(path)


@APP.post("/api/save")
def api_save():
    payload = request.get_json(silent=True) or {}
    try:
        path = safe_path(payload.get("path"))
        content = payload.get("content", "")
        if file_kind(path) not in {"text"}:
            return jsonify({"ok": False, "message": "Only text files can be edited"})
        path.write_text(content, encoding="utf-8")
        return jsonify({"ok": True, "message": f"Saved: {path.name}"})
    except Exception as ex:
        return jsonify({"ok": False, "message": str(ex)})


@APP.post("/api/delete")
def api_delete():
    payload = request.get_json(silent=True) or {}
    try:
        path = safe_path(payload.get("path"))
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink(missing_ok=True)
        return jsonify({"ok": True, "message": f"Deleted: {path.name}"})
    except Exception as ex:
        return jsonify({"ok": False, "message": str(ex)})


@APP.post("/api/open")
def api_open():
    global CURRENT_DIR
    payload = request.get_json(silent=True) or {}
    try:
        path = safe_path(payload.get("path"))
        if not path.is_dir():
            return jsonify({"ok": False, "message": "Selected object is not a folder"})
        CURRENT_DIR = path
        return jsonify({"ok": True, "message": str(path)})
    except Exception as ex:
        return jsonify({"ok": False, "message": str(ex)})


@APP.post("/api/up")
def api_up():
    global CURRENT_DIR
    parent = CURRENT_DIR.parent
    if ROOT_DIR in [parent, *parent.parents]:
        CURRENT_DIR = parent
    return jsonify({"ok": True, "current_dir": str(CURRENT_DIR)})


@APP.post("/api/create")
def api_create():
    payload = request.get_json(silent=True) or {}
    name = (payload.get("name") or "").strip()
    if not name:
        return jsonify({"ok": False, "message": "Filename is empty"})
    try:
        path = safe_path(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=True)
        return jsonify({"ok": True, "message": f"Created: {path.name}"})
    except Exception as ex:
        return jsonify({"ok": False, "message": str(ex)})


@APP.post("/api/upload")
def api_upload():
    files = request.files.getlist("files")
    count = 0
    for f in files:
        if not f.filename:
            continue
        target = (CURRENT_DIR / Path(f.filename).name).resolve()
        if ROOT_DIR not in [target, *target.parents]:
            continue
        f.save(str(target))
        count += 1
    return jsonify({"ok": True, "message": f"Uploaded: {count}"})


if __name__ == "__main__":
    APP.run(host="0.0.0.0", port=8000, debug=False)
import asyncio
import datetime
import os
import shutil
import socket
import subprocess
from pathlib import Path

import flet as ft


APP_START = datetime.datetime.now()
TEXT_EXTENSIONS = {
    ".txt",
    ".md",
    ".py",
    ".json",
    ".yaml",
    ".yml",
    ".ini",
    ".cfg",
    ".csv",
    ".log",
    ".xml",
    ".html",
    ".css",
    ".js",
    ".ts",
    ".sh",
    ".bat",
}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".svg"}

PALETTE = {
    "bg": "#070B17",
    "surface": "#101A2F",
    "surface_alt": "#172644",
    "card": "#111D36",
    "stroke": "#2A3A5F",
    "graphite": "#3A4664",
    "neon": "#00D9FF",
    "neon_soft": "#7CEBFF",
    "text_main": "#ECF4FF",
    "text_muted": "#93A8CC",
    "danger": "#FF7B8D",
    "success": "#2ED47A",
}


def get_ip_address() -> str:
    try:
        host = socket.gethostname()
        return socket.gethostbyname(host)
    except Exception:
        return "N/A"


def get_active_processes_count() -> int:
    try:
        if os.name == "nt":
            out = subprocess.run(
                ["tasklist"], capture_output=True, text=True, check=True
            ).stdout
            return max(0, len(out.strip().splitlines()) - 3)
        out = subprocess.run(["ps", "-e"], capture_output=True, text=True, check=True).stdout
        return max(0, len(out.strip().splitlines()) - 1)
    except Exception:
        return 0


def format_uptime() -> str:
    total = int((datetime.datetime.now() - APP_START).total_seconds())
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def file_icon_for(path: Path) -> ft.Icon:
    ext = path.suffix.lower()
    if path.is_dir():
        return ft.Icon(ft.Icons.FOLDER_ROUNDED, color="#8FB0FF")
    if ext in IMAGE_EXTENSIONS:
        return ft.Icon(ft.Icons.IMAGE_ROUNDED, color="#68F0CE")
    if ext == ".zip":
        return ft.Icon(ft.Icons.FOLDER_ZIP_ROUNDED, color="#FFC857")
    if ext in {".py", ".js", ".ts", ".json", ".html", ".css"}:
        return ft.Icon(ft.Icons.CODE_ROUNDED, color="#FF9FD7")
    if ext in TEXT_EXTENSIONS:
        return ft.Icon(ft.Icons.DESCRIPTION_ROUNDED, color="#9FB7FF")
    return ft.Icon(ft.Icons.INSERT_DRIVE_FILE_ROUNDED, color="#AAB7D1")


class HoverButton(ft.Container):
    def __init__(self, text: str, icon: str, on_click, accent: str | None = None):
        super().__init__()
        self._action = on_click
        self._base = "#1A2B4A"
        self._hover = "#26416D"
        self._accent = accent or PALETTE["neon"]
        self.border_radius = 12
        self.padding = ft.padding.symmetric(horizontal=14, vertical=9)
        self.bgcolor = self._base
        self.animate = ft.Animation(180, ft.AnimationCurve.EASE_OUT)
        self.animate_scale = ft.Animation(180, ft.AnimationCurve.EASE_OUT)
        self.scale = 1
        self.ink = True
        self.content = ft.Row(
            [
                ft.Icon(icon, size=16, color=self._accent),
                ft.Text(text, color=PALETTE["text_main"], weight=ft.FontWeight.W_600, size=13),
            ],
            spacing=8,
            tight=True,
        )
        self.on_hover = self._on_hover
        self.on_click = self._on_click

    def _on_hover(self, e: ft.HoverEvent):
        self.bgcolor = self._hover if e.data == "true" else self._base
        self.scale = 1.03 if e.data == "true" else 1
        self.update()

    def _on_click(self, _):
        if self._action:
            self._action()


def main(page: ft.Page):
    page.title = "Neon Server Studio"
    page.bgcolor = PALETTE["bg"]
    page.theme_mode = ft.ThemeMode.DARK
    page.padding = 16
    page.window_min_width = 1150
    page.window_min_height = 760

    working_dir = Path.cwd()
    selected_path: Path | None = None
    command_history: list[str] = []
    history_index = 0

    # ----------------------------- Shared UI bits -----------------------------
    uptime_value = ft.Text("00:00:00", size=22, weight=ft.FontWeight.BOLD, color=PALETTE["text_main"])
    ip_value = ft.Text(get_ip_address(), size=18, weight=ft.FontWeight.W_600, color=PALETTE["text_main"])
    proc_value = ft.Text("0", size=22, weight=ft.FontWeight.BOLD, color=PALETTE["text_main"])
    current_path_label = ft.Text(str(working_dir), color=PALETTE["neon_soft"], size=13)

    snack = ft.SnackBar(ft.Text(""))

    def notify(msg: str, ok: bool = True):
        snack.content = ft.Text(msg, color=PALETTE["text_main"])
        snack.bgcolor = "#143C2C" if ok else "#532332"
        page.open(snack)
        page.update()

    def glass_panel(title: str, body: ft.Control, actions: list[ft.Control] | None = None):
        return ft.Container(
            border_radius=18,
            padding=14,
            bgcolor=PALETTE["surface"],
            border=ft.border.all(1, PALETTE["stroke"]),
            shadow=ft.BoxShadow(blur_radius=20, spread_radius=1, color="#2600D9FF", offset=ft.Offset(0, 6)),
            content=ft.Column(
                [
                    ft.Row(
                        [
                            ft.Text(title, size=17, color=PALETTE["text_main"], weight=ft.FontWeight.W_600, expand=True),
                            *(actions or []),
                        ]
                    ),
                    ft.Divider(height=10, color="#243655"),
                    body,
                ],
                spacing=10,
                expand=True,
            ),
        )

    def stat_card(title: str, icon: str, val: ft.Text):
        return ft.Container(
            expand=1,
            height=130,
            padding=16,
            border_radius=18,
            gradient=ft.LinearGradient(
                begin=ft.Alignment(-1, -1),
                end=ft.Alignment(1, 1),
                colors=[PALETTE["card"], PALETTE["surface_alt"]],
            ),
            border=ft.border.all(1, "#27406A"),
            content=ft.Column(
                [
                    ft.Row(
                        [
                            ft.Icon(icon, size=17, color=PALETTE["neon"]),
                            ft.Text(title, color=PALETTE["text_muted"], size=13),
                        ],
                        spacing=6,
                    ),
                    ft.Container(height=8),
                    val,
                ],
                spacing=2,
            ),
        )

    # ----------------------------- Console page ------------------------------
    terminal_output = ft.ListView(expand=True, auto_scroll=True, spacing=3)
    command_input = ft.TextField(
        hint_text="Введите Linux/PowerShell команду (поддерживаются cd, ls, cat, pwd, rm, mkdir, cp, mv...)",
        bgcolor="#0B1325",
        border_color=PALETTE["stroke"],
        color=PALETTE["text_main"],
        cursor_color=PALETTE["neon"],
        border_radius=12,
        text_size=13,
    )

    def append_terminal_line(text: str, color: str = PALETTE["text_main"]):
        terminal_output.controls.append(ft.Text(text, color=color, size=13, font_family="Consolas"))

    def resolve_path(raw: str) -> Path:
        nonlocal working_dir
        p = Path(raw.strip('"').strip("'"))
        if p.is_absolute():
            return p.resolve()
        return (working_dir / p).resolve()

    def cmd_ls(args: list[str]) -> str:
        target = resolve_path(args[0]) if args else working_dir
        if not target.exists():
            return f"ls: cannot access '{target}': No such file or directory"
        if target.is_file():
            return target.name
        items = sorted(target.iterdir(), key=lambda i: (i.is_file(), i.name.lower()))
        return "\n".join([f"[D] {i.name}" if i.is_dir() else f"[F] {i.name}" for i in items]) or "(empty)"

    def cmd_cat(args: list[str]) -> str:
        if not args:
            return "cat: missing file operand"
        target = resolve_path(args[0])
        if not target.exists() or not target.is_file():
            return f"cat: {args[0]}: No such file"
        if target.suffix.lower() in IMAGE_EXTENSIONS:
            return f"{target.name}: binary image file (open from Files page)"
        return target.read_text(encoding="utf-8", errors="replace")

    def cmd_rm(args: list[str]) -> str:
        if not args:
            return "rm: missing operand"
        recursive = "-r" in args or "-rf" in args or "-fr" in args
        filtered = [a for a in args if not a.startswith("-")]
        if not filtered:
            return "rm: missing target"
        target = resolve_path(filtered[0])
        if not target.exists():
            return f"rm: cannot remove '{filtered[0]}': No such file"
        if target.is_dir() and not recursive:
            return "rm: cannot remove directory without -r"
        if target.is_dir():
            shutil.rmtree(target)
        else:
            target.unlink()
        return "removed"

    def execute_portable(command: str) -> tuple[str, bool]:
        nonlocal working_dir
        tokens = command.strip().split()
        if not tokens:
            return "", True
        name = tokens[0]
        args = tokens[1:]

        try:
            if name in {"help", "?"}:
                return (
                    "Builtins: pwd, ls, cd, cat, clear, mkdir, touch, rm, cp, mv, echo, help\n"
                    "Other commands execute in PowerShell.",
                    True,
                )
            if name == "pwd":
                return str(working_dir), True
            if name == "ls":
                return cmd_ls(args), True
            if name == "cd":
                target = resolve_path(args[0] if args else str(Path.home()))
                if not target.exists() or not target.is_dir():
                    return f"cd: {target}: No such directory", False
                working_dir = target
                current_path_label.value = str(working_dir)
                return str(working_dir), True
            if name == "cat":
                return cmd_cat(args), True
            if name == "clear":
                terminal_output.controls.clear()
                return "terminal cleared", True
            if name == "mkdir":
                if not args:
                    return "mkdir: missing operand", False
                resolve_path(args[0]).mkdir(parents=True, exist_ok=True)
                return "directory created", True
            if name == "touch":
                if not args:
                    return "touch: missing file operand", False
                resolve_path(args[0]).touch(exist_ok=True)
                return "file touched", True
            if name == "rm":
                return cmd_rm(args), True
            if name == "cp":
                if len(args) < 2:
                    return "cp: missing operands", False
                src, dst = resolve_path(args[0]), resolve_path(args[1])
                if src.is_dir():
                    shutil.copytree(src, dst, dirs_exist_ok=True)
                else:
                    shutil.copy2(src, dst)
                return "copied", True
            if name == "mv":
                if len(args) < 2:
                    return "mv: missing operands", False
                shutil.move(str(resolve_path(args[0])), str(resolve_path(args[1])))
                return "moved", True
            if name == "echo":
                return " ".join(args), True
        except Exception as ex:
            return f"{name}: {ex}", False

        # Fallback for native shell commands
        try:
            result = subprocess.run(
                ["powershell", "-NoProfile", "-Command", command],
                cwd=str(working_dir),
                capture_output=True,
                text=True,
                timeout=25,
            )
            out = result.stdout.strip()
            err = result.stderr.strip()
            if err and not out:
                return err, False
            return out or err or "(done)", result.returncode == 0
        except Exception as ex:
            return str(ex), False

    def run_command(_=None):
        nonlocal history_index
        command = command_input.value.strip()
        if not command:
            return
        append_terminal_line(f"{working_dir} $ {command}", PALETTE["neon"])
        command_history.append(command)
        history_index = len(command_history)
        command_input.value = ""

        output, ok = execute_portable(command)
        for line in output.splitlines() or [""]:
            append_terminal_line(line, PALETTE["text_main"] if ok else PALETTE["danger"])

        refresh_files()
        page.update()

    def history_up(_):
        nonlocal history_index
        if not command_history:
            return
        history_index = max(0, history_index - 1)
        command_input.value = command_history[history_index]
        page.update()

    def history_down(_):
        nonlocal history_index
        if not command_history:
            return
        history_index = min(len(command_history), history_index + 1)
        command_input.value = command_history[history_index] if history_index < len(command_history) else ""
        page.update()

    command_input.on_submit = run_command

    # ----------------------------- Files page --------------------------------
    files_column = ft.Column(expand=True, spacing=6, scroll=ft.ScrollMode.AUTO)
    file_preview_title = ft.Text("Ничего не выбрано", color=PALETTE["text_main"], size=16, weight=ft.FontWeight.W_600)
    file_preview_sub = ft.Text("Выберите файл или папку в списке слева.", color=PALETTE["text_muted"], size=13)
    file_editor = ft.TextField(
        multiline=True,
        min_lines=15,
        max_lines=22,
        text_style=ft.TextStyle(font_family="Consolas", size=13),
        bgcolor="#0B1325",
        border_color=PALETTE["stroke"],
        color=PALETTE["text_main"],
        border_radius=12,
        visible=False,
    )
    image_preview = ft.Image(fit=ft.ImageFit.CONTAIN, border_radius=12, visible=False, height=350)
    preview_info = ft.Text("", color=PALETTE["text_muted"], size=12)

    picker = ft.FilePicker()
    page.overlay.append(picker)

    def human_size(path: Path) -> str:
        if not path.exists() or path.is_dir():
            return "-"
        size = path.stat().st_size
        units = ["B", "KB", "MB", "GB"]
        idx = 0
        while size > 1024 and idx < len(units) - 1:
            size /= 1024
            idx += 1
        return f"{size:.1f} {units[idx]}"

    def select_item(path: Path):
        nonlocal selected_path
        selected_path = path
        file_preview_title.value = path.name
        preview_info.value = f"{'Folder' if path.is_dir() else 'File'} | {path.suffix or 'no ext'} | {human_size(path)}"
        file_editor.visible = False
        image_preview.visible = False
        file_preview_sub.value = ""

        if path.is_dir():
            file_preview_sub.value = "Папка выбрана. Используйте 'Открыть папку' для перехода."
        else:
            ext = path.suffix.lower()
            if ext in IMAGE_EXTENSIONS:
                image_preview.src = str(path)
                image_preview.visible = True
                file_preview_sub.value = "Предпросмотр изображения."
            elif ext in TEXT_EXTENSIONS or not ext:
                file_editor.value = path.read_text(encoding="utf-8", errors="replace")
                file_editor.visible = True
                file_preview_sub.value = "Текстовый файл можно редактировать и сохранить."
            else:
                file_preview_sub.value = "Двоичный файл. Открытие в редакторе отключено."
        page.update()

    def open_folder():
        nonlocal working_dir, selected_path
        if not selected_path:
            notify("Сначала выберите папку.", False)
            return
        if not selected_path.is_dir():
            notify("Выбран не каталог.", False)
            return
        working_dir = selected_path.resolve()
        current_path_label.value = str(working_dir)
        selected_path = None
        refresh_files()
        notify("Переход выполнен.")

    def go_up():
        nonlocal working_dir, selected_path
        parent = working_dir.parent
        if parent == working_dir:
            return
        working_dir = parent
        selected_path = None
        current_path_label.value = str(working_dir)
        refresh_files()

    def save_file():
        if not selected_path or not selected_path.is_file():
            notify("Выберите текстовый файл для сохранения.", False)
            return
        if not file_editor.visible:
            notify("Этот файл не редактируется в текстовом режиме.", False)
            return
        selected_path.write_text(file_editor.value or "", encoding="utf-8")
        notify(f"Сохранено: {selected_path.name}")
        refresh_files()

    def delete_selected():
        nonlocal selected_path
        if not selected_path:
            notify("Выберите файл или папку.", False)
            return
        try:
            if selected_path.is_dir():
                shutil.rmtree(selected_path)
            else:
                selected_path.unlink(missing_ok=True)
            notify(f"Удалено: {selected_path.name}")
            selected_path = None
            file_preview_title.value = "Ничего не выбрано"
            file_preview_sub.value = "Выберите файл или папку в списке слева."
            file_editor.visible = False
            image_preview.visible = False
            preview_info.value = ""
            refresh_files()
        except Exception as ex:
            notify(f"Ошибка удаления: {ex}", False)

    def create_new_file():
        dialog_name = ft.TextField(label="Имя файла (например notes.txt)", autofocus=True)

        def do_create(_):
            name = (dialog_name.value or "").strip()
            if not name:
                notify("Введите имя файла.", False)
                page.close(dlg)
                return
            p = (working_dir / name).resolve()
            p.parent.mkdir(parents=True, exist_ok=True)
            p.touch(exist_ok=True)
            page.close(dlg)
            refresh_files()
            notify(f"Создано: {name}")

        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Новый файл"),
            content=dialog_name,
            actions=[ft.TextButton("Отмена", on_click=lambda e: page.close(dlg)), ft.TextButton("Создать", on_click=do_create)],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        page.open(dlg)

    def upload_files():
        picker.pick_files(allow_multiple=True, dialog_title="Выберите файлы для копирования")

    def on_picked(e: ft.FilePickerResultEvent):
        if not e.files:
            return
        copied = 0
        for f in e.files:
            try:
                src = Path(f.path)
                if src.exists():
                    shutil.copy2(src, working_dir / src.name)
                    copied += 1
            except Exception:
                continue
        refresh_files()
        notify(f"Загружено файлов: {copied}")

    picker.on_result = on_picked

    def refresh_files():
        files_column.controls.clear()
        current_path_label.value = str(working_dir)
        try:
            entries = sorted(working_dir.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
        except Exception as ex:
            files_column.controls.append(ft.Text(f"Ошибка чтения директории: {ex}", color=PALETTE["danger"]))
            page.update()
            return

        if not entries:
            files_column.controls.append(ft.Text("(Папка пуста)", color=PALETTE["text_muted"]))
        for entry in entries:
            subtitle = "folder" if entry.is_dir() else f"{entry.suffix or 'file'} • {human_size(entry)}"
            row = ft.Container(
                border_radius=12,
                bgcolor="#0E1830",
                padding=10,
                ink=True,
                on_click=lambda e, p=entry: select_item(p),
                content=ft.Row(
                    [
                        file_icon_for(entry),
                        ft.Column(
                            [
                                ft.Text(entry.name, color=PALETTE["text_main"], size=14, no_wrap=True),
                                ft.Text(subtitle, color=PALETTE["text_muted"], size=11),
                            ],
                            spacing=2,
                            expand=True,
                        ),
                    ],
                    spacing=10,
                ),
            )
            files_column.controls.append(row)
        page.update()

    # ----------------------------- Layout pages -------------------------------
    dashboard_page = ft.Column(
        [
            ft.Text("Server Control Center", size=30, weight=ft.FontWeight.BOLD, color=PALETTE["text_main"]),
            ft.Text("Apple-like minimal dark + Material 3 usability", color=PALETTE["text_muted"]),
            ft.Row(
                [
                    stat_card("Uptime", ft.Icons.SCHEDULE_ROUNDED, uptime_value),
                    stat_card("IP Address", ft.Icons.LAN_ROUNDED, ip_value),
                    stat_card("Active Processes", ft.Icons.MEMORY_ROUNDED, proc_value),
                ],
                spacing=12,
            ),
            glass_panel(
                "Быстрые действия",
                ft.Row(
                    [
                        HoverButton("Обновить файлы", ft.Icons.REFRESH_ROUNDED, lambda: refresh_files()),
                        HoverButton("Открыть консоль", ft.Icons.TERMINAL_ROUNDED, lambda: switch_tab(1)),
                    ],
                    spacing=10,
                ),
            ),
        ],
        spacing=14,
    )

    console_page = glass_panel(
        "Удобная консоль",
        ft.Column(
            [
                ft.Text("Текущая директория", color=PALETTE["text_muted"], size=12),
                current_path_label,
                ft.Container(
                    expand=True,
                    border_radius=14,
                    bgcolor="#091224",
                    border=ft.border.all(1, PALETTE["stroke"]),
                    padding=10,
                    content=terminal_output,
                ),
                ft.Row(
                    [
                        HoverButton("Prev", ft.Icons.KEYBOARD_ARROW_UP_ROUNDED, history_up),
                        HoverButton("Next", ft.Icons.KEYBOARD_ARROW_DOWN_ROUNDED, history_down),
                        HoverButton("Clear", ft.Icons.CLEANING_SERVICES_ROUNDED, lambda: terminal_output.controls.clear()),
                    ],
                    spacing=8,
                ),
                command_input,
            ],
            spacing=8,
            expand=True,
        ),
    )

    files_page = ft.Row(
        [
            glass_panel(
                "Файлы",
                ft.Column([current_path_label, ft.Divider(color="#253859"), ft.Container(content=files_column, expand=True)], expand=True),
                actions=[
                    HoverButton("Вверх", ft.Icons.ARROW_UPWARD_ROUNDED, go_up),
                    HoverButton("Новый файл", ft.Icons.NOTE_ADD_ROUNDED, create_new_file),
                    HoverButton("Загрузить", ft.Icons.UPLOAD_FILE_ROUNDED, upload_files),
                    HoverButton("Обновить", ft.Icons.REFRESH_ROUNDED, refresh_files),
                ],
            ),
            glass_panel(
                "Просмотр / Редактирование",
                ft.Column(
                    [
                        file_preview_title,
                        file_preview_sub,
                        preview_info,
                        image_preview,
                        file_editor,
                    ],
                    spacing=8,
                    expand=True,
                    scroll=ft.ScrollMode.AUTO,
                ),
                actions=[
                    HoverButton("Открыть папку", ft.Icons.FOLDER_OPEN_ROUNDED, open_folder),
                    HoverButton("Сохранить", ft.Icons.SAVE_ROUNDED, save_file, accent=PALETTE["success"]),
                    HoverButton("Удалить", ft.Icons.DELETE_OUTLINE_ROUNDED, delete_selected, accent=PALETTE["danger"]),
                ],
            ),
        ],
        spacing=12,
        expand=True,
    )

    def on_tab_change(_):
        page.update()

    def switch_tab(index: int):
        tabs.selected_index = index
        page.update()

    tabs = ft.Tabs(
        selected_index=0,
        animation_duration=250,
        on_change=on_tab_change,
        tabs=[
            ft.Tab(text="Dashboard", icon=ft.Icons.DASHBOARD_ROUNDED, content=dashboard_page),
            ft.Tab(text="Console", icon=ft.Icons.TERMINAL_ROUNDED, content=console_page),
            ft.Tab(text="Files", icon=ft.Icons.FOLDER_ROUNDED, content=files_page),
        ],
        expand=True,
    )

    page.add(tabs)

    append_terminal_line("Neon console ready.", PALETTE["text_muted"])
    append_terminal_line("Type `help` to see portable commands.", PALETTE["text_muted"])
    refresh_files()

    async def ticker():
        while True:
            uptime_value.value = format_uptime()
            proc_value.value = str(get_active_processes_count())
            page.update()
            await asyncio.sleep(1)

    page.run_task(ticker)


if __name__ == "__main__":
    ft.run(main)
