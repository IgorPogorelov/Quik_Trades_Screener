# ui_server.py
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from threading import Thread
from typing import Any, Callable, Dict, List, Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from starlette.responses import FileResponse, HTMLResponse, JSONResponse
from starlette.staticfiles import StaticFiles


# ===================== РЕСУРСЫ (index.html, static/) =====================

def _base_dir() -> Path:
    # PyInstaller onefile: все данные распакованы во временную папку
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    # PyInstaller onedir: ресурсы лежат рядом с exe
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    # Обычный запуск из исходников
    return Path(__file__).resolve().parent


BASE_DIR: Path = _base_dir()
STATIC_DIR: Path = BASE_DIR / "static"
INDEX_HTML: Path = BASE_DIR / "index.html"


# ===================== EVENT BUS =====================

class UIEventBus:
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        self.loop: Optional[asyncio.AbstractEventLoop] = loop
        self.subscribers: Set[asyncio.Queue] = set()
        self.quik_status: Dict[str, Any] = {"connected": False, "mode": "unknown"}

    async def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self.subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self.subscribers.discard(q)

    def publish(self, payload: Dict[str, Any]) -> None:
        """
        Потокобезопасная публикация события всем подписчикам.
        Можно вызывать из любых потоков.
        """
        if self.loop is None:
            return

        def _put_all() -> None:
            for q in list(self.subscribers):
                try:
                    q.put_nowait(payload)
                except Exception:
                    # переполненный/закрытый — молча пропустим
                    pass

        # прокинем задачу в цикл, где живут очереди
        try:
            self.loop.call_soon_threadsafe(_put_all)
        except Exception:
            pass


# Глобальные ссылки, которые подменяет main()
event_bus: UIEventBus = UIEventBus()
get_active_snapshot: Callable[[], List[Dict[str, Any]]] = lambda: []


def push_quik_status(connected: bool, mode: str) -> None:
    """
    Обновляет статус и пушит событие в шину.
    Безопасно вызывать из любых потоков.
    """
    mode_up = str(mode or "unknown").upper()
    event_bus.quik_status = {"connected": bool(connected), "mode": mode_up}
    event_bus.publish({"type": "quik_status", "connected": bool(connected), "mode": mode_up})


# ===================== FASTAPI =====================

app = FastAPI()

# static/
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
else:
    print(f"[ui] static dir not found: {STATIC_DIR}")

# index.html
@app.get("/")
async def root():
    if INDEX_HTML.exists():
        return FileResponse(str(INDEX_HTML))
    return HTMLResponse("<h3>index.html не найден</h3><p>Проверь упаковку/путь.</p>", status_code=404)


# API для снапшота активных роботов (используется фронтом на старте)
@app.get("/robots/active")
async def robots_active():
    try:
        data = get_active_snapshot() or []
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# API статуса QUIK (используется фронтом)
@app.get("/quik/status")
async def quik_status():
    return JSONResponse(event_bus.quik_status or {"connected": False, "mode": "unknown"})


# WebSocket для событий
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()

    # подписка на события
    q = await event_bus.subscribe()

    # отправим текущий статус при подключении
    try:
        await ws.send_text(json.dumps({"type": "quik_status", **(event_bus.quik_status or {})}))
    except Exception:
        pass

    try:
        while True:
            # ждём событие от event_bus и отправляем клиенту
            payload = await q.get()
            try:
                await ws.send_text(json.dumps(payload, ensure_ascii=False))
            except Exception:
                # если сокет помер, выходим
                break
    except WebSocketDisconnect:
        pass
    finally:
        event_bus.unsubscribe(q)
        try:
            await ws.close()
        except Exception:
            pass


@app.on_event("startup")
async def _on_startup():
    # привязываем шину к реальному event loop uvicorn
    event_bus.loop = asyncio.get_running_loop()


# ===================== ЗАПУСК В ОТДЕЛЬНОМ ПОТОКЕ =====================

def start_ui_server_in_thread(host: str = "127.0.0.1", port: int = 8100):
    """
    Запускает uvicorn с этим приложением в фоне.
    Возвращает (loop_stub, bus) для совместимости с существующим кодом.
    """
    import uvicorn

    bus = UIEventBus()
    # делаем bus видимым для роутов до старта uvicorn
    global event_bus
    event_bus = bus

    # loop_stub вернём для совместимости, но реальный цикл создаст uvicorn в своём потоке
    loop_stub = asyncio.new_event_loop()

    def _run():
        config = uvicorn.Config(app, host=host, port=port, loop="asyncio", log_level="info")
        server = uvicorn.Server(config)
        server.run()  # блокирует поток до остановки

    t = Thread(target=_run, daemon=True)
    t.start()
    return loop_stub, bus
