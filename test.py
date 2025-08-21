# robot_app.py
# Python 3.10+
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Tuple, List, Optional, Iterable, Set, Callable
from threading import Lock, Thread, Event
from queue import Queue, Empty
from collections import deque
from datetime import datetime, timezone
import time, datetime as dt, json, http.server, socketserver, statistics, fnmatch, os

# ===== YAML =====
try:
    import yaml  # pip install pyyaml
except Exception:
    yaml = None

# ===== QUIK =====
try:
    from QuikPy import QuikPy
    HAS_QUIKPY = True
except Exception:
    HAS_QUIKPY = False

# ===== UI =====
import ui_server  # ui_server.py рядом с этим файлом

# ---------- ЛОГИ ----------
def log_json(level: str, event: str, **fields):
    rec = {"ts": datetime.now(timezone.utc).isoformat(), "level": level, "event": event}
    rec.update(fields)
    print(json.dumps(rec, ensure_ascii=False))

# ---------- ПРОФИЛИ ----------
@dataclass(frozen=True)
class VolumeProfile:
    min_volume: int
    max_volume: int
    volume_tolerance_lots: int = 1

@dataclass(frozen=True)
class Rule:
    class_mask: str
    symbol_mask: str
    side: Optional[str]  # "buy" | "sell" | None
    when_start: Optional[dt.time] = None
    when_end: Optional[dt.time] = None
    profile: VolumeProfile = None
    # интервальные переопределения (опционально)
    min_interval: Optional[float] = None
    max_interval: Optional[float] = None
    interval_tolerance_pct: Optional[float] = None
    ui_show_from_candidate: Optional[bool] = None

    def matches(self, class_code: str, symbol: str, side: str, now: dt.time) -> bool:
        if not fnmatch.fnmatch(class_code, self.class_mask): return False
        if not fnmatch.fnmatch(symbol, self.symbol_mask):   return False
        if self.side and self.side.lower() != side.lower(): return False
        if self.when_start and self.when_end:
            if not (self.when_start <= now <= self.when_end): return False
        return True

@dataclass(frozen=True)
class ResolvedProfile:
    min_volume: int
    max_volume: int
    volume_tolerance_lots: int
    min_interval: float
    max_interval: float
    interval_tolerance_pct: float
    ui_show_from_candidate: bool

class ProfileManager:
    def __init__(self, rules, default, excluded_classes, excluded_symbols, default_ui_show: bool = False):
        self.rules = rules
        self.default = default
        self.excluded_classes = excluded_classes
        self.excluded_symbols = excluded_symbols
        self.default_ui_show = default_ui_show   # <<< сохраняем сюда


    def resolve(self, class_code: str, symbol: str, side: str,
                *, now: Optional[dt.time] = None,
                global_min_interval: float, global_max_interval: float, global_interval_tol_pct: float) -> ResolvedProfile:
        t = now or dt.datetime.now().time()
        vol = self.default
        min_int, max_int, tol = global_min_interval, global_max_interval, global_interval_tol_pct
        for r in self.rules:
            if r.matches(class_code, symbol, side, t):
                vol = r.profile
                if r.min_interval is not None: min_int = r.min_interval
                if r.max_interval is not None: max_int = r.max_interval
                if r.interval_tolerance_pct is not None: tol = r.interval_tolerance_pct
                break
        return ResolvedProfile(
            vol.min_volume, vol.max_volume, vol.volume_tolerance_lots,
            min_int, max_int, tol,
            self.default_ui_show  # ← чтобы сигнатура совпала
        )

    def resolve_many(self, class_code: str, symbol: str, side: str,
                     *, now: Optional[dt.time] = None,
                     global_min_interval: float, global_max_interval: float, global_interval_tol_pct: float
                     ) -> List[ResolvedProfile]:
        t = now or dt.datetime.now().time()
        # default_ui_show возьми из замыкания/храни как поле; чтобы не переизобретать,
        # можно сохранить его в self в __init__, но проще — вычислить в load_profiles и передать сюда.
        results: List[ResolvedProfile] = []
        for r in self.rules:
            if r.matches(class_code, symbol, side, t):
                vol = r.profile
                min_int = r.min_interval if r.min_interval is not None else global_min_interval
                max_int = r.max_interval if r.max_interval is not None else global_max_interval
                tol = r.interval_tolerance_pct if r.interval_tolerance_pct is not None else global_interval_tol_pct
                ui_sfc = r.ui_show_from_candidate
                results.append(ResolvedProfile(
                    min_volume=vol.min_volume,
                    max_volume=vol.max_volume,
                    volume_tolerance_lots=vol.volume_tolerance_lots,
                    min_interval=min_int,
                    max_interval=max_int,
                    interval_tolerance_pct=tol,
                    ui_show_from_candidate=bool(ui_sfc) if ui_sfc is not None else self.default_ui_show
                ))
        if not results:
            vol = self.default
            results.append(ResolvedProfile(
                min_volume=vol.min_volume,
                max_volume=vol.max_volume,
                volume_tolerance_lots=vol.volume_tolerance_lots,
                min_interval=global_min_interval,
                max_interval=global_max_interval,
                interval_tolerance_pct=global_interval_tol_pct,
                ui_show_from_candidate=self.default_ui_show
            ))
        return results

def _parse_time(s: Optional[str]) -> Optional[dt.time]:
    if not s: return None
    return dt.datetime.strptime(s, "%H:%M:%S").time()

def load_profiles_from_yaml(path: str) -> tuple[ProfileManager, set[str], set[str]]:
    if yaml is None: raise RuntimeError("PyYAML не установлен. pip install pyyaml")
    if not os.path.exists(path): raise FileNotFoundError(f"profiles.yaml не найден: {path}")
    cfg = yaml.safe_load(open(path, "r", encoding="utf-8"))
    if "default" not in cfg: raise ValueError("В YAML должен быть раздел 'default'.")

    def mk_profile(node: dict) -> VolumeProfile:
        return VolumeProfile(
            min_volume=int(node["min_volume"]),
            max_volume=int(node["max_volume"]),
            volume_tolerance_lots=int(node.get("volume_tolerance_lots", 1)),
        )

    default_p = mk_profile(cfg["default"])
    default_ui = (cfg.get("default", {}).get("ui", {}) or {})
    default_ui_show = bool(default_ui.get("show_from_candidate", False))

    # excluded
    excluded_cfg = cfg.get("excluded", {}) or {}
    excluded_classes = set(excluded_cfg.get("classes", []) or [])
    excluded_symbols = set(excluded_cfg.get("symbols", []) or [])

    rules: List[Rule] = []
    for r in cfg.get("profiles", []) or []:
        match = str(r["match"]).strip().split()
        inst = match[0]; side = match[1] if len(match) > 1 else None
        if ":" not in inst: raise ValueError(f"match '{inst}' должен быть 'CLASS:SYMBOL'.")
        class_mask, symbol_mask = inst.split(":", 1)
        when = r.get("when", {}) or {}
        interval = r.get("interval", {}) or {}

        ui_node = r.get("ui", {}) or {}

        rules.append(Rule(
            class_mask=class_mask, symbol_mask=symbol_mask, side=side,
            when_start=_parse_time(when.get("start")), when_end=_parse_time(when.get("end")),
            profile=mk_profile(r),
            min_interval=float(interval["min"]) if "min" in interval else None,
            max_interval=float(interval["max"]) if "max" in interval else None,
            interval_tolerance_pct=float(interval["tolerance_pct"]) if "tolerance_pct" in interval else None,
            ui_show_from_candidate=ui_node.get("show_from_candidate", None)
        ))

    pm = ProfileManager(
        rules=rules,
        default=default_p,
        excluded_classes=excluded_classes,
        excluded_symbols=excluded_symbols,
        default_ui_show=default_ui_show
    )
    log_json("info", "profiles_loaded", path=path, rules=len(rules),
             excluded_classes=len(excluded_classes), excluded_symbols=len(excluded_symbols),
             default={"min": default_p.min_volume, "max": default_p.max_volume, "tol": default_p.volume_tolerance_lots})
    return pm, excluded_classes, excluded_symbols

# ---------- КОНФИГ ----------
@dataclass
class DetectorConfig:
    allowed_classes: Optional[Set[str]] = None
    allowed_symbols: Optional[Set[str]] = None
    excluded_classes: Optional[Set[str]] = None
    excluded_symbols: Optional[Set[str]] = None
    min_interval: float = 10.0
    max_interval: float = 15.0
    interval_tolerance_pct: float = 2.0
    trade_merge_delay: float = 0.2
    merge_epsilon_ms: int = 5
    profiles: ProfileManager | None = None
    profiles_path: Optional[str] = None
    verbose: bool = True
    health_port: int = 8099

# ---------- МОДЕЛИ / ТАЙМЕР ----------
@dataclass
class Trade:
    class_code: str
    symbol: str
    ts_ms: int
    volume: int
    side: str  # "buy" | "sell"
    @property
    def ts_s(self) -> float: return self.ts_ms / 1000.0

@dataclass
class Candidate:
    class_code: str
    symbol: str
    side: str
    interval: float
    last_trade_ts_s: float
    trades_count: int = 2
    volumes: deque = field(default_factory=lambda: deque(maxlen=7))

    # таймер и колбэк на таймаут
    _timer: Optional[MonotonicTimer] = field(default=None, repr=False, compare=False)
    _on_timeout: Optional[Callable[['Candidate'], None]] = field(default=None, repr=False, compare=False)

    def vol_median(self) -> int:
        return int(round(statistics.median(self.volumes))) if self.volumes else 0

    def volume_match(self, vol: int, tol: int) -> bool:
        return True if not self.volumes else abs(vol - self.vol_median()) <= tol

    def match_interval(self, new_interval: float, tol_pct: float) -> bool:
        if self.interval <= 0: return False
        return abs(new_interval - self.interval) / self.interval * 100.0 <= tol_pct

    def observe_volume(self, vol: int):
        self.volumes.append(vol)

    # Candidate.start_or_restart_timer
    def start_or_restart_timer(self, extra_seconds: float = 2.0):
        if self._timer: self._timer.cancel()
        # вместо плясок с time.time и last_trade_ts_s — просто ждём интервал
        self._timer = MonotonicTimer(self.interval + extra_seconds, self._fire_timeout)

    def stop_timer(self):
        if self._timer:
            self._timer.cancel()
            self._timer = None

    def _fire_timeout(self):
        if self._on_timeout:
            self._on_timeout(self)


class MonotonicTimer:
    def __init__(self, delay_sec: float, callback: Callable[[], None]):
        self._delay = max(0.0, delay_sec); self._callback = callback
        self._stop = Event(); self._thread = Thread(target=self._run, daemon=True); self._thread.start()
    def _run(self):
        if not self._stop.wait(timeout=self._delay):
            try: self._callback()
            except Exception as e: log_json("error", "timer_callback_error", error=str(e))
    def cancel(self): self._stop.set()

@dataclass
class Robot:
    class_code: str
    symbol: str
    side: str
    interval: float
    count: int = 0
    volumes: deque = field(default_factory=lambda: deque(maxlen=15))
    _timer: Optional[MonotonicTimer] = field(default=None, repr=False, compare=False)
    last_seen_ms: int = 0
    expected_next_ms: int = 0
    _on_timeout: Optional[Callable[['Robot'], None]] = field(default=None, repr=False, compare=False)
    _on_tick: Optional[Callable[['Robot'], None]] = field(default=None, repr=False, compare=False)

    def vol_median(self) -> int: return int(round(statistics.median(self.volumes))) if self.volumes else 0
    def volume_match(self, vol: int, tol: int) -> bool: return True if not self.volumes else abs(vol - self.vol_median()) <= tol
    def observe_volume(self, vol: int): self.volumes.append(vol)

    def start_or_restart_timer(self, extra_seconds: float = 2.0):
        if self._timer: self._timer.cancel()
        self._timer = MonotonicTimer(max(0.0, self.interval + extra_seconds), self._fire_timeout)

    def stop_timer(self):
        if self._timer: self._timer.cancel(); self._timer = None

    def _fire_timeout(self):
        if self._on_timeout: self._on_timeout(self)

    def on_matched_trade(self, observed_volume: int):
        self.count += 1; self.observe_volume(observed_volume)
        now_ms = int(time.time() * 1000)
        self.last_seen_ms = now_ms
        self.expected_next_ms = now_ms + int(self.interval * 1000)
        if self._on_tick: self._on_tick(self)

# ---------- ХРАНИЛИЩА ----------
Key = Tuple[str, str, int, str]
ISKey = Tuple[str, str, str]

class TradeStorage:
    def __init__(self, keep_history: bool = True, history_limit: int = 100_000):
        self._keep_history = keep_history; self._history_limit = history_limit
        self._all: List[Trade] = []; self._last_by_key: Dict[Key, Trade] = {}
        self._last_by_is_vol: Dict[ISKey, Dict[int, Trade]] = {}; self._lock = Lock()
    def add(self, trade: Trade) -> None:
        with self._lock:
            if self._keep_history:
                self._all.append(trade)
                if len(self._all) > self._history_limit:
                    drop = len(self._all) - self._history_limit
                    if drop > 0: self._all = self._all[drop:]
            self._last_by_key[(trade.class_code, trade.symbol, trade.volume, trade.side)] = trade
            iskey = (trade.class_code, trade.symbol, trade.side)
            d = self._last_by_is_vol.get(iskey) or {}
            d[trade.volume] = trade; self._last_by_is_vol[iskey] = d
    def last_for_within_volume_tol(self, class_code: str, symbol: str, side: str, volume: int, tol: int) -> Optional[Trade]:
        with self._lock:
            d = self._last_by_is_vol.get((class_code, symbol, side))
            if not d: return None
            lo, hi = max(0, volume - tol), volume + tol
            candidates = [t for v, t in d.items() if lo <= v <= hi]
            return max(candidates, key=lambda t: t.ts_ms) if candidates else None
    def reversed_iter(self) -> Iterable[Trade]:
        with self._lock: return list(reversed(self._all))

class CandidateStorage:
    def __init__(self): self._by_iss: Dict[ISKey, List[Candidate]] = {}; self._lock = Lock()
    def list_for(self, class_code: str, symbol: str, side: str) -> List[Candidate]:
        with self._lock: return list(self._by_iss.get((class_code, symbol, side), []))
    def upsert(self, cand: Candidate) -> None:
        with self._lock:
            key = (cand.class_code, cand.symbol, cand.side)
            lst = self._by_iss.get(key)
            if lst is None: self._by_iss[key] = [cand]
            elif cand not in lst: lst.append(cand)
    def remove(self, cand: Candidate) -> None:
        with self._lock:
            key = (cand.class_code, cand.symbol, cand.side)
            lst = self._by_iss.get(key);
            if not lst: return
            try: lst.remove(cand)
            except ValueError: pass
            if not lst: self._by_iss.pop(key, None)

class RobotRegistry:
    def __init__(self, verbose: bool):
        self.active: Dict[Tuple[str, str, str, float, int], Robot] = {}
        self.inactive: Dict[Tuple[str, str, str, float, int], Robot] = {}
        self._lock = Lock(); self.verbose = verbose
    def _key(self, r: Robot) -> Tuple[str, str, str, float, int]:
        return (r.class_code, r.symbol, r.side, r.interval, r.vol_median())
    def add_active(self, robot: Robot):
        with self._lock:
            self.active[self._key(robot)] = robot
            log_json("info", "robot_activated", class_code=robot.class_code, symbol=robot.symbol,
                     side=robot.side, interval=round(robot.interval,6), count=robot.count, volume_median=robot.vol_median())
    def move_to_inactive(self, robot: Robot):
        with self._lock:
            self.active.pop(self._key(robot), None)
            self.inactive[self._key(robot)] = robot
            log_json("info", "robot_deactivated", class_code=robot.class_code, symbol=robot.symbol,
                     side=robot.side, interval=round(robot.interval,6), final_count=robot.count, volume_median=robot.vol_median())
    def find_active(self, class_code: str, symbol: str, side: str,
                    interval: float, tol_pct: float, volume: int, vol_tol: int) -> Optional[Robot]:
        with self._lock:
            for (cc, sym, s, intv, _vm), r in self.active.items():
                if cc==class_code and sym==symbol and s==side:
                    if intv>0 and abs(interval-intv)/intv*100.0 <= tol_pct:
                        if r.volume_match(volume, vol_tol): return r
            return None

# ---------- ДЕТЕКТОР ----------
class RobotDetector:
    def __init__(self, cfg: DetectorConfig):
        if cfg.profiles is None: raise ValueError("DetectorConfig.profiles обязателен")
        self.cfg = cfg
        self.trades = TradeStorage()
        self.candidates = CandidateStorage()
        self.robots = RobotRegistry(verbose=cfg.verbose)

    def on_trade(self, trade: Trade):
        profiles = self.cfg.profiles.resolve_many(
            trade.class_code, trade.symbol, trade.side,
            global_min_interval=self.cfg.min_interval,
            global_max_interval=self.cfg.max_interval,
            global_interval_tol_pct=self.cfg.interval_tolerance_pct,
        )

        # локальная защита от дублирования обработки одного и того же интервала в рамках этой сделки
        seen_intervals: Set[int] = set()  # секунды*1e6 -> int, чтобы стабильно сравнивать

        for prof in profiles:
            # 1) фильтр по объёму под конкретный профиль
            if not (prof.min_volume <= trade.volume <= prof.max_volume):
                continue

            # 2) ищем предыдущую "похожую" сделку с толерансом объёма для этого профиля
            prev = self.trades.last_for_within_volume_tol(
                trade.class_code, trade.symbol, trade.side,
                trade.volume, prof.volume_tolerance_lots
            )
            if not prev:
                continue

            interval = trade.ts_s - prev.ts_s
            if interval <= 0:
                continue
            if not (prof.min_interval <= interval <= prof.max_interval):
                continue

            # не обрабатываем один и тот же интервал дважды в этой итерации, если он одинаковый до микросекунд
            key_i = int(round(interval * 1_000_000))
            if key_i in seen_intervals:
                continue
            seen_intervals.add(key_i)

            # 3) есть ли активный робот, подходящий по интервалу/объёму в рамках профиля
            existing = self.robots.find_active(
                trade.class_code, trade.symbol, trade.side,
                interval, prof.interval_tolerance_pct,
                trade.volume, prof.volume_tolerance_lots
            )
            if existing:
                existing.on_matched_trade(trade.volume)
                log_json("debug", "robot_tick",
                         class_code=existing.class_code, symbol=existing.symbol,
                         side=existing.side, interval=round(existing.interval, 6),
                         count=existing.count, volume_median=existing.vol_median())
                existing.start_or_restart_timer()
                ui_server.event_bus.publish({
                    "type": "robot_tick",
                    "class_code": existing.class_code, "symbol": existing.symbol, "side": existing.side,
                    "interval_sec": float(existing.interval), "count": int(existing.count),
                    "volume_median": int(existing.vol_median()),
                    "last_seen_ms": int(existing.last_seen_ms),
                    "expected_next_ms": int(existing.expected_next_ms),
                })
                continue

            # 4) кандидаты (тот же алгоритм, только под текущий профайл)
            cands = self.candidates.list_for(trade.class_code, trade.symbol, trade.side)
            match = None
            for c in cands:
                if c.match_interval(interval, prof.interval_tolerance_pct) and c.volume_match(trade.volume,
                                                                                              prof.volume_tolerance_lots):
                    match = c
                    break

            # создание кандидата
            if match is None:
                c = Candidate(trade.class_code, trade.symbol, trade.side, interval, trade.ts_s, 2)
                c.observe_volume(prev.volume)
                c.observe_volume(trade.volume)
                c._on_timeout = self._on_candidate_timeout  # <<< колбэк
                c.start_or_restart_timer(extra_seconds=2.0)  # <<< таймер
                self.candidates.upsert(c)
                if prof.ui_show_from_candidate:
                    now_ms = int(time.time() * 1000)
                    ui_server.event_bus.publish({
                        "type": "candidate_updated",
                        "class_code": c.class_code, "symbol": c.symbol, "side": c.side,
                        "interval_sec": float(c.interval), "count": int(c.trades_count),
                        "volume_median": int(c.vol_median()),
                        "last_seen_ms": now_ms,
                        "expected_next_ms": now_ms + int(c.interval * 1000),
                    })

                continue

            # обновление кандидата (пока он не робот)
            match.trades_count += 1
            match.last_trade_ts_s = trade.ts_s
            match.observe_volume(trade.volume)
            match.start_or_restart_timer(extra_seconds=2.0)  # <<< перезапуск таймера
            if match.trades_count >= 3:
                self._promote_candidate_to_robot(match)
                self.candidates.remove(match)
            else:
                self.candidates.upsert(match)
                if prof.ui_show_from_candidate:
                    now_ms = int(time.time() * 1000)
                    ui_server.event_bus.publish({
                        "type": "candidate_updated",
                        "class_code": match.class_code, "symbol": match.symbol, "side": match.side,
                        "interval_sec": float(match.interval), "count": int(match.trades_count),
                        "volume_median": int(match.vol_median()),
                        "last_seen_ms": now_ms,
                        "expected_next_ms": now_ms + int(match.interval * 1000),
                    })

        # добавляем сделку в историю ОДИН раз после обработки всех профилей
        self.trades.add(trade)

    def _promote_candidate_to_robot(self, cand: Candidate):
        cand.stop_timer()
        robot = Robot(cand.class_code, cand.symbol, cand.side, cand.interval, cand.trades_count)
        for v in cand.volumes: robot.observe_volume(v)
        now_ms = int(time.time()*1000)
        robot.last_seen_ms = now_ms; robot.expected_next_ms = now_ms + int(robot.interval*1000)
        robot._on_timeout = self._on_robot_timeout; robot._on_tick = self._on_robot_tick
        self.robots.add_active(robot); robot.start_or_restart_timer()
        log_json("info","robot_detected", class_code=robot.class_code, symbol=robot.symbol,
                 side=robot.side, interval=round(robot.interval,6), count=robot.count, volume_median=robot.vol_median())
        # push to UI
        ui_server.event_bus.publish({
            "type":"robot_detected","class_code":robot.class_code,"symbol":robot.symbol,"side":robot.side,
            "interval_sec": float(robot.interval),"count": int(robot.count),
            "volume_median": int(robot.vol_median()),"last_seen_ms": int(robot.last_seen_ms),
            "expected_next_ms": int(robot.expected_next_ms),
        })

    def _on_robot_timeout(self, robot: Robot):
        log_json("info","robot_timeout", class_code=robot.class_code, symbol=robot.symbol,
                 side=robot.side, interval=round(robot.interval,6), count=robot.count, volume_median=robot.vol_median())
        ui_server.event_bus.publish({
            "type":"robot_timeout","class_code":robot.class_code,"symbol":robot.symbol,"side":robot.side,
            "interval_sec": float(robot.interval),"count": int(robot.count),
            "volume_median": int(robot.vol_median()),
            "last_seen_ms": int(getattr(robot,"last_seen_ms",0)),
            "expected_next_ms": int(getattr(robot,"expected_next_ms",0)),
        })
        robot.stop_timer(); self.robots.move_to_inactive(robot)

    def _on_robot_tick(self, robot: Robot):
        # уже отправлено в on_trade(); оставим хук на будущее
        pass

    def _on_candidate_timeout(self, cand: Candidate):
        # Лог и событие для фронта
        log_json("info", "candidate_timeout",
                 class_code=cand.class_code, symbol=cand.symbol, side=cand.side,
                 interval=round(cand.interval, 6), count=cand.trades_count, volume_median=cand.vol_median())
        try:
            ui_server.event_bus.publish({
                "type": "candidate_timeout",
                "class_code": cand.class_code, "symbol": cand.symbol, "side": cand.side,
                "interval_sec": float(cand.interval), "count": int(cand.trades_count),
                "volume_median": int(cand.vol_median()),
                "last_seen_ms": int(cand.last_trade_ts_s * 1000),
                "expected_next_ms": int(cand.last_trade_ts_s * 1000 + int(cand.interval * 1000)),
            })
        except Exception as e:
            log_json("error", "candidate_timeout_publish_failed", error=str(e))
        # убрать из стора и остановить таймер
        cand.stop_timer()
        self.candidates.remove(cand)


# ---------- Источник тиков (QUIK/симулятор) ----------
def _convert_quik_datetime_to_ms(d: dict) -> int:
    y=d.get("year",1970); m=d.get("month",1); day=d.get("day",1)
    hh=d.get("hour",0); mm=d.get("min",0); ss=d.get("sec",0); ms=d.get("ms",0)
    t = dt.datetime(y,m,day,hh,mm,ss,ms*1000); return int(t.timestamp()*1000)

class QuikStream:
    def __init__(self, cfg: DetectorConfig, out_queue: Queue):
        self.cfg = cfg; self.out = out_queue; self.qp: Optional[QuikPy] = None

    # внутри QuikStream.start
    def start(self):
        if not HAS_QUIKPY:
            ui_server.push_quik_status(True, "SIMULATOR")
            Thread(target=self._simulate_stream, daemon=True).start()
            return
        self.qp = QuikPy()
        self.qp.on_all_trade = self._on_all_trade
        ui_server.push_quik_status(True, "QUIK")
        log_json("info", "quik_subscribed")

    def close(self):
        try:
            if self.qp:
                self.qp.on_all_trade = None
                # у некоторых версий нет close(); не споткнёмся
                if hasattr(self.qp, "Close"):
                    self.qp.Close()
            ui_server.push_quik_status(False, "QUIK")
        except Exception:
            pass

    def _pass_filters(self, class_code: str, symbol: str) -> bool:
        ac=self.cfg.allowed_classes; asy=self.cfg.allowed_symbols
        ec=self.cfg.excluded_classes; esy=self.cfg.excluded_symbols
        if ec is not None and class_code in ec: return False
        if esy is not None and symbol in esy: return False
        if ac is not None and class_code not in ac: return False
        if asy is not None and symbol not in asy: return False
        return True
    def _on_all_trade(self, msg: dict):
        data = msg.get("data", msg)
        class_code = str(data.get("class_code","")).strip()
        symbol = str(data.get("sec_code","")).strip()
        if not class_code or not symbol: return
        if not self._pass_filters(class_code, symbol): return
        volume = int(data.get("qty",0))
        if volume <= 0: return
        flags = int(data.get("flags",0))
        side = "buy" if (flags & 0x1) == 0 else "sell"
        ts_ms = _convert_quik_datetime_to_ms(data.get("datetime", {}))
        self.out.put(Trade(class_code, symbol, ts_ms, volume, side))
    def _simulate_stream(self):
        # два робота: SIBN buy 114±1 лот с интервалом 12с; GAZP sell 75±1 лот с интервалом 9с
        log_json("info","sim_start", patterns=[{"class":"TQBR","symbol":"SIBN","side":"buy","vol~":114,"intv":12.0},
                                               {"class":"TQBR","symbol":"GAZP","side":"sell","vol~":75,"intv":9.0}])
        try:
            ui_server.event_bus.publish({"type": "quik_status", "connected": True, "mode": "simulator"})
        except Exception:
            pass

        base_m = time.monotonic(); last_m = {}
        patterns = [("TQBR","SIBN","buy",114,12.0), ("TQBR","GAZP","sell",75,9.0)]
        for p in patterns: last_m[p]=base_m
        import random
        while True:
            now_m = time.monotonic(); now_w = time.time()
            for p in patterns:
                cc,sym,side,vol,intv = p
                if not self._pass_filters(cc,sym): continue
                if now_m - last_m[p] >= intv:
                    last_m[p] = now_m
                    v = vol + random.choice([-1,0,1])
                    self.out.put(Trade(cc, sym, int(now_w*1000), v, side))
            time.sleep(0.01)

# ---------- Процессор склейки ----------
class TradeProcessor(Thread):
    def __init__(self, cfg: DetectorConfig, detector: RobotDetector, in_queue: Queue):
        super().__init__(daemon=True)
        self.cfg=cfg; self.detector=detector; self.in_q=in_queue
        self._running=True; self._current: Optional[Trade]=None; self._flush_at_monotonic=0.0
    def run(self):
        while self._running:
            try:
                trade = self.in_q.get(timeout=0.05); self._ingest(trade)
            except Empty: pass
            if self._current and time.monotonic() >= self._flush_at_monotonic:
                self._flush_current()
            else:
                time.sleep(0.002)
        if self._current: self._flush_current()
    def stop(self): self._running=False
    def _ingest(self, t: Trade):
        if self._current and self._is_same_bucket(self._current, t):
            self._current.volume += t.volume
            self._flush_at_monotonic = time.monotonic() + self.cfg.trade_merge_delay
        else:
            if self._current: self._flush_current()
            self._current = Trade(t.class_code, t.symbol, t.ts_ms, t.volume, t.side)
            self._flush_at_monotonic = time.monotonic() + self.cfg.trade_merge_delay
    def _flush_current(self):
        if self._current: self.detector.on_trade(self._current); self._current=None; self._flush_at_monotonic=0.0
    def _is_same_bucket(self, a: Trade, b: Trade) -> bool:
        if a.side != b.side or a.class_code != b.class_code or a.symbol != b.symbol: return False
        return abs(a.ts_ms - b.ts_ms) <= self.cfg.merge_epsilon_ms

# ---------- Health / Reload ----------
class HealthHandler(http.server.BaseHTTPRequestHandler):
    detector_ref: Optional[RobotDetector] = None
    config_ref: Optional[DetectorConfig] = None
    def log_message(self, format, *args): return
    def do_GET(self):
        if self.path.startswith("/healthz"): return self._ok({"status":"ok"})
        if self.path.startswith("/readyz"):  return self._ok({"status":"ready"})
        return self._fail({"error":"not_found"},404)
    def do_POST(self):
        if self.path != "/config/reload": return self._fail({"error":"not_found"},404)
        ln = int(self.headers.get("Content-Length","0")); raw = self.rfile.read(ln) if ln>0 else b"{}"
        try: body = json.loads(raw or b"{}")
        except Exception: body={}
        path = body.get("path") or (self.config_ref.profiles_path if self.config_ref else None)
        if not path: return self._fail({"error":"no_path_provided"},400)
        try:
            pm, excl_classes, excl_symbols = load_profiles_from_yaml(path)
            self.config_ref.profiles = pm; self.config_ref.profiles_path = path
            self.config_ref.excluded_classes = excl_classes; self.config_ref.excluded_symbols = excl_symbols
            self._ok({"status":"reloaded","path":path})
        except Exception as e:
            log_json("error","profiles_reload_failed",error=str(e)); self._fail({"error":str(e)},400)
    def _ok(self, obj):
        body=json.dumps(obj).encode("utf-8"); self.send_response(200)
        self.send_header("Content-Type","application/json; charset=utf-8")
        self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
    def _fail(self, obj, code=500):
        body=json.dumps(obj).encode("utf-8"); self.send_response(code)
        self.send_header("Content-Type","application/json; charset=utf-8")
        self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)

def start_http_server(cfg: DetectorConfig, detector: RobotDetector):
    def _serve():
        with socketserver.TCPServer(("0.0.0.0", cfg.health_port), HealthHandler) as httpd:
            HealthHandler.detector_ref = detector; HealthHandler.config_ref = cfg
            log_json("info","control_server_started", port=cfg.health_port); httpd.serve_forever()
    Thread(target=_serve, daemon=True).start()

# ---------- MAIN ----------
def main():
    profiles_path = os.getenv("PROFILES_PATH", "profiles.yaml")
    try:
        profiles_pm, excl_classes, excl_symbols = load_profiles_from_yaml(profiles_path)
    except Exception as e:
        log_json("warn","profiles_load_failed_fallback", error=str(e), path=profiles_path)

        # 1) задаём дефолтный volume-профиль
        default_profile = VolumeProfile(min_volume=50, max_volume=200, volume_tolerance_lots=1)

        # 2) пустые множества исключений
        excl_classes, excl_symbols = set(), set()

        # 3) создаём ProfileManager по НОВОЙ сигнатуре (5 аргументов)
        profiles_pm = ProfileManager(
            rules=[],                      # правил нет, работаем на дефолте
            default=default_profile,       # дефолтный объёмный профиль
            excluded_classes=excl_classes, # исключённые классы (пусто)
            excluded_symbols=excl_symbols, # исключённые тикеры (пусто)
            default_ui_show=False          # по умолчанию кандидатов НЕ показываем
        )


    cfg = DetectorConfig(
        allowed_classes=None, allowed_symbols=None,
        excluded_classes=excl_classes, excluded_symbols=excl_symbols,
        min_interval=10.0, max_interval=60.0, interval_tolerance_pct=5.0,
        trade_merge_delay=0.2, merge_epsilon_ms=5,
        profiles=profiles_pm, profiles_path=profiles_path,
        verbose=True, health_port=8099,
    )

    detector = RobotDetector(cfg)
    start_http_server(cfg, detector)

    # --- UI server ---
    loop, bus = ui_server.start_ui_server_in_thread(host="127.0.0.1", port=8100)
    ui_server.event_bus = bus
    def _active_snapshot():
        snap=[]
        for (_cc,_sym,_side,_intv,_vm), r in detector.robots.active.items():
            snap.append({
                "type":"robot_detected","class_code":r.class_code,"symbol":r.symbol,"side":r.side,
                "interval_sec": float(r.interval),"count": int(r.count),"volume_median": int(r.vol_median()),
                "last_seen_ms": int(getattr(r,"last_seen_ms", int(time.time()*1000))),
                "expected_next_ms": int(getattr(r,"expected_next_ms", int(time.time()*1000)+int(r.interval*1000))),
            })
        return snap
    ui_server.get_active_snapshot = _active_snapshot

    q: Queue[Trade] = Queue(maxsize=10000)
    stream = QuikStream(cfg, q); stream.start()
    proc = TradeProcessor(cfg, detector, q); proc.start()

    log_json("info","run_started", hint="Ctrl+C to stop", profiles_path=profiles_path,
             excluded_classes=list(excl_classes), excluded_symbols=list(excl_symbols))
    try:
        while True: time.sleep(1.0)
    except KeyboardInterrupt:
        log_json("info","stopping")
    finally:
        proc.stop(); proc.join(timeout=1.0); stream.close()
        for r in list(detector.robots.active.values()): r.stop_timer()
        log_json("info","stopped")

if __name__ == "__main__":
    main()
