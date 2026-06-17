#!/usr/bin/env python3
"""Tiny local web dashboard for pipeline worker progress."""

from __future__ import annotations

import json
import threading
import time
from dataclasses import asdict, dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


@dataclass
class WorkerState:
    worker: str
    status: str = "idle"
    doi: str | None = None
    source: str | None = None
    title: str | None = None
    detail: str | None = None
    started_at: float | None = None
    updated_at: float | None = None


class ProgressWebServer:
    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 8765,
        worker_count: int = 1,
        query: str = "",
        log_limit: int = 250,
    ) -> None:
        self.host = host
        self.port = port
        self.worker_count = worker_count
        self.query = query
        self.log_limit = log_limit
        self.started_at = time.time()
        self._lock = threading.Lock()
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._total = 0
        self._completed = 0
        self._workers: dict[str, WorkerState] = {}
        self._logs: list[dict[str, Any]] = []

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}/"

    def start(self) -> str:
        handler = self._make_handler()
        last_error: OSError | None = None
        for candidate_port in range(self.port, self.port + 20):
            try:
                self._server = ThreadingHTTPServer((self.host, candidate_port), handler)
                self.port = candidate_port
                break
            except OSError as exc:
                last_error = exc
        if not self._server:
            raise RuntimeError(f"Cannot start progress web server: {last_error}")

        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self.url

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread:
            self._thread.join(timeout=2)
            self._thread = None

    def set_total(self, total: int) -> None:
        with self._lock:
            self._total = max(0, total)
            self._completed = min(self._completed, self._total)

    def start_worker(
        self,
        worker: str,
        *,
        doi: str | None,
        source: str | None,
        title: str | None,
        detail: str | None = None,
    ) -> None:
        now = time.time()
        with self._lock:
            self._workers[worker] = WorkerState(
                worker=worker,
                status="working",
                doi=doi,
                source=source,
                title=title,
                detail=detail,
                started_at=now,
                updated_at=now,
            )

    def update_worker(
        self,
        worker: str,
        *,
        doi: str | None = None,
        source: str | None = None,
        title: str | None = None,
        detail: str | None = None,
        status: str = "working",
    ) -> None:
        now = time.time()
        with self._lock:
            state = self._workers.get(worker) or WorkerState(worker=worker, started_at=now)
            state.status = status
            if doi is not None:
                state.doi = doi
            if source is not None:
                state.source = source
            if title is not None:
                state.title = title
            if detail is not None:
                state.detail = detail
            state.updated_at = now
            self._workers[worker] = state

    def finish_worker(self, worker: str, *, status: str = "done", detail: str | None = None) -> None:
        now = time.time()
        with self._lock:
            state = self._workers.get(worker) or WorkerState(worker=worker)
            state.status = status
            state.detail = detail
            state.updated_at = now
            self._workers[worker] = state

    def increment_completed(self, amount: int = 1) -> None:
        with self._lock:
            self._completed = min(self._total, self._completed + amount)

    def add_log(self, message: str) -> None:
        if not message:
            return
        with self._lock:
            self._logs.append({"ts": time.time(), "message": message})
            if len(self._logs) > self.log_limit:
                self._logs = self._logs[-self.log_limit :]

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            total = self._total
            completed = self._completed
            workers = sorted(self._workers.values(), key=lambda item: item.worker)
            return {
                "query": self.query,
                "started_at": self.started_at,
                "now": time.time(),
                "worker_count": self.worker_count,
                "total": total,
                "completed": completed,
                "remaining": max(0, total - completed),
                "workers": [asdict(worker) for worker in workers],
                "logs": list(self._logs),
            }

    def _make_handler(self) -> type[BaseHTTPRequestHandler]:
        owner = self

        class ProgressHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path in {"/", "/index.html"}:
                    self._send(200, "text/html; charset=utf-8", DASHBOARD_HTML)
                    return
                if self.path.startswith("/state"):
                    payload = json.dumps(owner.snapshot(), ensure_ascii=False).encode("utf-8")
                    self._send(200, "application/json; charset=utf-8", payload)
                    return
                self._send(404, "text/plain; charset=utf-8", b"not found")

            def log_message(self, format: str, *args: Any) -> None:
                return

            def _send(self, status: int, content_type: str, body: str | bytes) -> None:
                payload = body.encode("utf-8") if isinstance(body, str) else body
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        return ProgressHandler


DASHBOARD_HTML = r"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Pipeline progress</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --text: #1f2328;
      --muted: #68707d;
      --line: #d8dee8;
      --accent: #1b7f79;
      --accent-soft: #dff4f0;
      --warn: #946200;
      --error: #b42318;
      --ok: #1f7a3f;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.45 ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    main {
      width: min(1180px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 24px 0;
    }
    header {
      display: grid;
      grid-template-columns: 1fr auto;
      align-items: end;
      gap: 16px;
      margin-bottom: 18px;
    }
    h1 {
      margin: 0;
      font-size: 22px;
      font-weight: 750;
      letter-spacing: 0;
    }
    .query {
      margin-top: 5px;
      color: var(--muted);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      max-width: 760px;
    }
    .counter {
      min-width: 230px;
      padding: 14px 16px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      text-align: right;
    }
    .remaining {
      display: block;
      font-size: 34px;
      font-weight: 800;
      line-height: 1;
      color: var(--accent);
      letter-spacing: 0;
    }
    .counter small { color: var(--muted); }
    .bar {
      height: 10px;
      border-radius: 999px;
      background: #e7ebf0;
      overflow: hidden;
      margin-bottom: 18px;
    }
    .bar span {
      display: block;
      height: 100%;
      width: 0;
      background: var(--accent);
      transition: width .25s ease;
    }
    .workers {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
      gap: 12px;
    }
    .worker {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 14px;
      min-height: 148px;
      display: grid;
      grid-template-rows: auto auto 1fr auto;
      gap: 10px;
    }
    .worker-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
    }
    .worker-id { font-weight: 760; font-size: 16px; }
    .status {
      border-radius: 999px;
      padding: 3px 9px;
      background: #eef1f5;
      color: var(--muted);
      font-size: 12px;
      white-space: nowrap;
    }
    .status.working { color: var(--accent); background: var(--accent-soft); }
    .status.done { color: var(--ok); background: #e4f6ea; }
    .status.failed { color: var(--error); background: #fde8e5; }
    .source {
      color: var(--muted);
      font-size: 13px;
      min-height: 19px;
    }
    .doi {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      overflow-wrap: anywhere;
      font-size: 13px;
    }
    .title {
      color: var(--muted);
      overflow: hidden;
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
    }
    .detail {
      color: var(--muted);
      font-size: 12px;
      border-top: 1px solid var(--line);
      padding-top: 8px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .log-panel {
      margin-top: 16px;
      background: #101418;
      color: #d7dde5;
      border-radius: 8px;
      border: 1px solid #2a3038;
      height: 210px;
      overflow: auto;
      padding: 10px 12px;
      font: 12px/1.45 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }
    .log-line {
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      padding: 1px 0;
    }
    @media (max-width: 680px) {
      main { width: min(100vw - 20px, 1180px); padding-top: 14px; }
      header { grid-template-columns: 1fr; align-items: stretch; }
      .counter { text-align: left; }
      .query { max-width: 100%; white-space: normal; }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>Загрузка статей</h1>
        <div class="query" id="query"></div>
      </div>
      <div class="counter">
        <span class="remaining" id="remaining">0</span>
        <small id="summary">ожидает обработки</small>
      </div>
    </header>
    <div class="bar"><span id="progress"></span></div>
    <section class="workers" id="workers"></section>
    <section class="log-panel" id="logs"></section>
  </main>
  <script>
    const workersEl = document.getElementById('workers');
    const logsEl = document.getElementById('logs');
    const remainingEl = document.getElementById('remaining');
    const summaryEl = document.getElementById('summary');
    const progressEl = document.getElementById('progress');
    const queryEl = document.getElementById('query');

    function text(value, fallback = '') {
      return value === null || value === undefined || value === '' ? fallback : String(value);
    }

    function esc(value) {
      return text(value)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }

    function elapsed(state, now) {
      if (!state.started_at) return '';
      const seconds = Math.max(0, Math.floor(now - state.started_at));
      const mins = Math.floor(seconds / 60);
      const rest = seconds % 60;
      return mins ? `${mins}m ${rest}s` : `${rest}s`;
    }

    function render(data) {
      queryEl.textContent = data.query || '';
      remainingEl.textContent = data.remaining;
      summaryEl.textContent = `${data.completed} / ${data.total} обработано`;
      progressEl.style.width = data.total ? `${Math.round(data.completed / data.total * 100)}%` : '0%';

      const workers = data.workers.length ? data.workers : Array.from(
        {length: data.worker_count},
        (_, i) => ({worker: `w${i + 1}`, status: 'idle'})
      );
      workersEl.innerHTML = workers.map((worker) => {
        const status = text(worker.status, 'idle');
        const source = text(worker.source, 'ожидает источник');
        const doi = text(worker.doi, 'DOI пока нет');
        const title = text(worker.title, 'ожидает задачу');
        const detail = [text(worker.detail), elapsed(worker, data.now)].filter(Boolean).join(' · ');
        return `
          <article class="worker">
            <div class="worker-head">
              <div class="worker-id">${esc(worker.worker)}</div>
              <div class="status ${esc(status)}">${esc(status)}</div>
            </div>
            <div class="source">${esc(source)}</div>
            <div>
              <div class="doi">${esc(doi)}</div>
              <div class="title">${esc(title)}</div>
            </div>
            <div class="detail">${esc(detail || ' ')}</div>
          </article>
        `;
      }).join('');

      logsEl.innerHTML = data.logs.map((item) => (
        `<div class="log-line">${esc(new Date(item.ts * 1000).toLocaleTimeString())} ${esc(item.message)}</div>`
      )).join('');
      logsEl.scrollTop = logsEl.scrollHeight;
    }

    async function refresh() {
      try {
        const response = await fetch('/state', {cache: 'no-store'});
        render(await response.json());
      } catch (error) {
        logsEl.innerHTML = `<div class="log-line">Нет соединения с progress server: ${error}</div>`;
      }
    }

    refresh();
    setInterval(refresh, 500);
  </script>
</body>
</html>
"""
