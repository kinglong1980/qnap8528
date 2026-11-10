#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
QNAP 8528 (TS-453Dmini) 风扇控制器
版本: v3.0-unified  (pid + linear + step 三模式合一 + WebUI + 配置持久化)

特性:
  1. 三模式: pid(PID闭环) | step(阶梯查表) | linear(曲线线性插值)
  2. UI 分离: 页面为脚本同目录的 UI 文件(fanui.html / fanwebui.html / ui.html / webui.html / fanweb.html, 按序取首个存在的), 
  由内置 HTTP 服务(默认 18080)提供
  3. 配置持久化: /data/curve-config.json (curve / fan_control_enabled / manual_pwm)
  4. 文件日志可选: 默认 /data/qnap-fan-control.log, 打开失败自动降级为仅 journal
  5. 监控模式: fan_control_enabled=False 时只读不写 PWM
  6. 手动模式: manual_mode=True 时按 manual_pwm 直接输出(不持久化, 重启回自动)
  7. 安全链: boot-boost → 风扇堵转 → 温度丢失 → 硬限温 → 紧急超温 → 高温保持 → 下降限速
  8. PWM 防抖 + 平滑逼近 + 硬件回读校验

HTTP API:
  GET  /                → UI 页面 (fanui/fanwebui/ui/webui/fanweb.html 之一)
  GET  /api/status      → 实时状态(温度/PWM/RPM/曲线/模式)
  GET  /api/log         → 最近 3 分钟采样
  POST /api/curve       → {curve, fan_control_enabled, manual_mode, manual_pwm}

依赖: python3 标准库 + drivetemp 内核模块 + systemd
systemd:
  Type=notify, NotifyAccess=main, WatchdogSec=30
  ExecStart=/usr/bin/python3 /usr/local/bin/qnap-fan-control.py pid --kp 11.5 --ki 0.032 --kd 2.6 --target 52
  ExecStopPost=-/bin/sh -c 'for f in /sys/class/hwmon/*/name; do if grep -qi qnap "$f"; then echo 65 > "$(dirname "$f")/pwm1"; fi; done'
  # 需要 ReadWritePaths=/data (配置+日志) ; ProtectKernelTunables=no (写 pwm1)

日志格式: CPU:x|DISK_MAX:x|SSD:x|CTRL:x|RAW:x|TGT:x|HW:x|RPM:x
"""

from __future__ import annotations

import argparse
import glob
import json
import logging
import logging.handlers
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


# ====================================================================== 常量与路径配置

VERSION = "v3.0-unified"


# 日志文件: 设为 none/off/空 可关闭文件日志(只走 journal)
LOG_PATH = os.environ.get("QNAP_FAN_LOG", "/data/qnap-fan-control.log")
CONFIG_PATH = os.environ.get("QNAP_FAN_CONFIG", "/data/curve-config.json")


# 历史采样落盘(JSONL): 默认与配置同目录, QNAP_FAN_HISTORY 可覆盖; 置 none/off 关闭
HISTORY_PATH = os.environ.get("QNAP_FAN_HISTORY") or os.path.join(
    os.path.dirname(CONFIG_PATH), "fan-history.jsonl")


# ---- UI 页面 ----
# UI 页面候选名(脚本同目录): 按顺序取第一个存在的文件
UI_CANDIDATES = ("fanui.html", "fanwebui.html", "ui.html", "webui.html", "fanweb.html")
_UI_DIR = os.path.dirname(os.path.abspath(__file__))
# 可用 QNAP_FAN_UI 直接指定页面; 否则按候选顺序查找, 全不存在时回退 webui.html
UI_PATH = os.environ.get("QNAP_FAN_UI", "").strip() or next(
    (p for p in (os.path.join(_UI_DIR, n) for n in UI_CANDIDATES) if os.path.exists(p)),
    os.path.join(_UI_DIR, "webui.html"),
)


HTTP_PORT = int(os.environ.get("QNAP_FAN_PORT", "18080"))
HISTORY_SIZE = 1800         # /api/log 采样点数 (2s x 1800 ≈ 1 小时, UI 可切 3分/30分/1时)
SAMPLE_INTERVAL = 2.0


# WebUI 默认曲线 (温度℃ → PWM), 持久化到 CONFIG_PATH
# 40℃ 以下 70; 40-60℃ 线性斜坡(70→190, 每℃ +6); 60℃ 以上继续爬升
DEFAULT_CURVE = [
    {"temp": 40, "pwm": 65},
    {"temp": 45, "pwm": 65},
    {"temp": 50, "pwm": 120},
    {"temp": 55, "pwm": 160},
    {"temp": 60, "pwm": 210},
    {"temp": 65, "pwm": 255},
    {"temp": 70, "pwm": 255},
    {"temp": 75, "pwm": 255},
    {"temp": 80, "pwm": 255},
]


# 低频批量写(60s 一次)避免磨损 NAS 存储; 超过 8MB 轮转, 只保留最近 HISTORY_SIZE 条
HIST_FLUSH_INTERVAL = 60.0
HIST_MAX_BYTES = 8 * 1024 * 1024


# ====================================================================== 全局变量与运行状态

# UI 可见事件环形缓冲(最新在后, /api/status 的 events 字段输出最新在前)
EVENTS: deque = deque(maxlen=100)


CURVE: list[dict[str, Any]] = [dict(p) for p in DEFAULT_CURVE]
fan_control_enabled: bool = True     # False → 监控只读, 不写 PWM
manual_mode: bool = False           # True → 手动强制 manual_pwm (不持久化)
manual_pwm: int = 100
HISTORY: deque[dict[str, Any]] = deque(maxlen=HISTORY_SIZE)
state_lock = threading.RLock()


# ---- 历史落盘缓冲 ----
_hist_pending: list[str] = []
_hist_last_flush = 0.0


# ---- UI 取数活跃度 (无人看页面时暂停历史采样与落盘, 给 NAS 减负) ----
UI_LAST_SEEN: float = 0.0        # 最近一次 UI 拉数据的时间(time.time()); 0 = 从未访问


def mark_ui_seen() -> None:
    """UI 在拉数据(状态/日志/下发指令)时调用, 用于判断是否还有人在看"""
    global UI_LAST_SEEN
    UI_LAST_SEEN = time.time()


def ui_streaming() -> bool:
    """是否还有页面在取数; 无访问超过 CFG['UI_IDLE_SECS'] 秒则返回 False
    (该值 <=0 表示不启用该优化, 始终采样)"""
    idle_secs = float(CFG["UI_IDLE_SECS"])
    if idle_secs <= 0:
        return True
    return (time.time() - UI_LAST_SEEN) <= idle_secs


# ---- 服务层运行期句柄 (HTTP 线程与主循环共用) ----
g_fc: FanController | None = None
g_pid: PID | None = None
g_run_mode = "pid"


# ====================================================================== 主配置 CFG

CFG: dict[str, Any] = {
    # ---- 主循环 ----
    "LOOP_INTERVAL": 0.5,
    "PID_INTERVAL": 0.5,            # PID 重算间隔(仅 pid 模式生效)
    "DT_MIN": 0.1,
    "DT_MAX": 5.0,
    "STATUS_LOG_INTERVAL": 600.0,   # INFO 状态日志间隔(10 分钟, 给 NAS 日志减负)
    "DEBUG_LOG_INTERVAL": 1.0,      # DEBUG 日志间隔(独立于 INFO, 不再被其门控)
    "PWM_VERIFY_INTERVAL": 60.0,    # 硬件 PWM 回读校验/日志间隔
    "UI_IDLE_SECS": 300.0,          # 无页面取数超过该秒数后暂停历史采样与落盘(0=不启用)

    # ---- PWM 硬件边界 ----
    "PWM_HW_MIN": 16,
    "PWM_HW_MAX": 255,
    "MIN_PWM": 60,                  # 自动模式(PID/曲线)运行下限; 51→60 抬高常驻气流, 削掉突发尖峰
    "MAX_PWM": 255,
    "DEFAULT_PWM": 68,              # 退出/故障恢复值, 必须与 systemd ExecStopPost 一致
    "PWM_FAIL_MAX": 5,
    "PWM_DEBOUNCE_COUNT": 3,        # 目标连续 N 次一致才写硬件(仅 |Δ|<5 的微抖)
    "PWM_JUMP_BYPASS": 5,           # 目标与当前差值≥该值时跳过防抖立即写(单调逼近不受防抖拖累)
    "MAX_PWM_DROP_PER_SEC": 15,     # PWM 每秒最大下降幅度, 抑制转速骤降噪音
    "MAX_PWM_RISE_PER_SEC": 60,     # PWM 每秒最大上升幅度, 抑制急升过冲(紧急/手动/boost 走 force 不受限)
    "RESCALE_TABLE": True,          # 阶梯表低于 MIN_PWM 的档位自动按比例重映射

    # ---- 平滑逼近步进 - 上升 (目标PWM上限, 单步最大增量) ----
    # 升降均衡: 68→255 约 6 拍(≈3s), 上升速度比原来减半, 抑制"急升过冲→过冷"的锯齿
    "SMOOTH_UP_STEPS": [
        (100, 20),
        (160, 30),
        (255, 45),
    ],
    # ---- 平滑逼近步进 - 下降 (目标PWM上限, 单步最大增量) ----
    # 静音优先: 慢降 + 限速, 抑制风噪跳变
    "SMOOTH_STEPS": [
        (100, 12),                  # 低转速区: 响应快, 起步尽快脱离失速区
        (160, 18),
        (255, 22),                  # 高转速区: 慢逼近, 抑制风噪跳变
    ],

    # ---- 启动升速 boot-boost ----
    "BOOT_BOOST_PWM": 140,
    "BOOT_BOOST_SECS": 10.0,
    "BOOT_MIN_PWM": 60,             # 快转结束后的运行下限, 与 MIN_PWM 保持一致(低于它启动时会被自动抬到 MIN_PWM)

    # ---- PID 参数(命令行可覆盖) ----
    # 2026-11-09 调参: Kp 15→6, Ki 0.032→0.02. 原参数在实测曲线上表现为 45↔59℃
    # 极限环(周期 8~15s, PWM 51↔170 满幅摆), 根因是增益相对 14s 风扇滞后过大.
    "PID_KP": 6,
    "PID_KI": 0.02,
    "PID_KD": 2.6,
    "PID_TARGET": 45.0,
    "PID_INT_LIMIT": 800.0,         # 积分限幅
    "PID_INT_DECAY_FACTOR": 0.75,   # 积分泄漏系数, 抑制积分饱和
    "PID_D_TERM_LIMIT": 40.0,       # 微分项限幅
    "PID_D_FILTER_ALPHA": 0.25,     # 微分低通, 抑制温度抖动放大
    "PID_DEADBAND": 1.0,            # 温度死区(℃), 死区内跳过运算降 CPU
    "PID_OUTPUT_DEADBAND": 6,       # PWM 输出死区, 微小变化不写硬件

    # ---- 阶梯 / 线性映射表 (触发上限, 回落阈值, PWM) ----
    # 由 /data/curve-config.json 的 curve 在启动时生成, 运行期随 API 更新
    "STEP_TABLE": [],

    # ---- 温度采样 ----
    "TEMP_VALID_MIN": 0.0,
    "TEMP_VALID_MAX": 95.0,
    "FILTER_ALPHA": 0.14,           # 主控温度一阶低通系数, 按 0.5s 周期校准(平滑时间常数≈4s, 与原 1s/0.25 一致)
    "TEMP_HYSTERESIS": 1.5,         # 非 PID 模式: 温度变化超过该值才重算
    "FORCE_RECALC_INTERVAL": 10.0,  # 最长重算间隔, 防滞死后输出僵住
    "TEMP_LOST_GRACE": 3,           # 温度丢失宽限周期, 超限强制满速
    "SSD_IGNORE_TEMP": 60.0,        # SSD 参与控温门槛, 低于该值仅日志展示
    "TEMP_SOURCE": "all",           # 控温依据: all=全部最大 / cpu=仅CPU / disk=仅硬盘

    # ---- 安全阈值 ----
    "EMERGENCY_TEMP": 70.0,         # 紧急超温 → 满速
    "HARD_LIMIT_TEMP": 74.0,        # 硬限温 → 满速
    "EMERGENCY_HYST": 2.0,          # 退出高温保持的迟滞带宽
    "EMERGENCY_HOLD_CNT": 10,       # 超温后保持满速的周期数
    "FAN_STALL_THRESHOLD": 5,       # 连续 N 次读不到转速判定堵转

    # ---- 硬件扫描 ----
    "HDD_REFRESH_INTERVAL": 600.0,  # 硬盘 hwmon 节点完整重扫间隔
    "HDD_READ_INTERVAL": 5.0,       # 硬盘温度读取间隔(远小于重扫间隔)
    "SCAN_RETRY_MAX": 15,           # 启动扫描最大重试次数
    "SCAN_RETRY_DELAY": 2.0,        # 启动扫描重试间隔(秒)
}


# PID 默认参数快照(脚本内置): 配置缺失/非法/三个增益全为 0 时回退到这里
DEFAULT_PID_PARAMS: dict[str, float] = {
    "kp": float(CFG["PID_KP"]),
    "ki": float(CFG["PID_KI"]),
    "kd": float(CFG["PID_KD"]),
    "target": float(CFG["PID_TARGET"]),
}


# 控温依据可选值 (与 CFG["TEMP_SOURCE"] 对应)
TEMP_SOURCES = ("all", "cpu", "disk")


# ====================================================================== 工具函数

def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def hwmon_index(path: str) -> int:
    """hwmon12 → 12; glob 字典序会把 hwmon10 排在 hwmon2 前面"""
    try:
        return int(os.path.basename(path).replace("hwmon", ""))
    except ValueError:
        return 1 << 30


# ====================================================================== 日志与事件

def setup_logger() -> logging.Logger:
    """控制台(journal) + 可选文件; 文件不可写时降级, 绝不因日志崩进程"""
    logger = logging.getLogger("qnap-fanctrl")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    target = (LOG_PATH or "").strip()
    if target and target.lower() not in ("none", "off", "-"):
        try:
            parent = os.path.dirname(target)
            if parent:
                os.makedirs(parent, exist_ok=True)
            fh = logging.handlers.RotatingFileHandler(
                target, maxBytes=2 * 1024 * 1024, backupCount=2, encoding="utf-8"
            )
            fh.setFormatter(fmt)
            logger.addHandler(fh)
        except Exception as e:
            logger.warning(f"文件日志不可用({target}): {e}, 仅输出到 journal")
    return logger


log = setup_logger()


# ---- 事件记录 ----
def note_event(level: str, text: str) -> None:
    """记录事件; 与上一条相同且 60s 内的重复(如安全动作逐拍触发)不记录"""
    if EVENTS:
        last = EVENTS[-1]
        if last["text"] == text and time.time() - last["ts"] < 60:
            return
    EVENTS.append({"ts": round(time.time(), 1), "level": level, "text": text})


# ====================================================================== 历史采样落盘 (JSONL)

def load_history() -> int:
    """启动时回读历史采样(最多 HISTORY_SIZE 条), 重启后曲线不归零"""
    n = 0
    try:
        if not os.path.exists(HISTORY_PATH):
            return 0
        with open(HISTORY_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()[-HISTORY_SIZE:]
        for line in lines:
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            if isinstance(rec, dict) and isinstance(rec.get("ts"), (int, float)):
                HISTORY.append(rec)
                n += 1
        if n:
            log.info(f"回读历史采样 {n} 条: {HISTORY_PATH}")
    except Exception as e:
        log.warning(f"回读历史失败 {HISTORY_PATH}: {e}")
    return n


def flush_history(force: bool = False) -> None:
    """把待写采样追加写入 JSONL; 周期限流 + 超限轮转"""
    global _hist_last_flush
    now = time.time()
    if not _hist_pending:
        return
    if not force and now - _hist_last_flush < HIST_FLUSH_INTERVAL:
        return
    _hist_last_flush = now
    try:
        parent = os.path.dirname(HISTORY_PATH)
        if parent:
            os.makedirs(parent, exist_ok=True)
        try:
            rotate = os.path.getsize(HISTORY_PATH) > HIST_MAX_BYTES
        except OSError:
            rotate = False
        if rotate:
            try:
                with open(HISTORY_PATH, "r", encoding="utf-8") as f:
                    keep = f.readlines()[-HISTORY_SIZE:]
                tmp = HISTORY_PATH + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    f.writelines(keep)
                os.replace(tmp, HISTORY_PATH)
                log.info(f"历史文件轮转: 保留最近 {len(keep)} 条")
            except OSError as e:
                log.warning(f"历史轮转失败: {e}")
        with open(HISTORY_PATH, "a", encoding="utf-8") as f:
            f.writelines(_hist_pending)
        _hist_pending.clear()
    except Exception as e:
        log.warning(f"历史落盘失败 {HISTORY_PATH}: {e}")
        _hist_pending.clear()   # 丢弃避免无限增长


# ====================================================================== 配置读写

def sanitize_curve(raw: Any) -> list[dict[str, Any]]:
    """校验前端提交的曲线, 返回按温度升序的安全副本"""
    out: list[dict[str, Any]] = []
    if not isinstance(raw, list):
        return [dict(p) for p in DEFAULT_CURVE]
    for p in raw:
        if not isinstance(p, dict):
            continue
        try:
            t = float(p["temp"])
            v = int(float(p["pwm"]))
        except (KeyError, TypeError, ValueError):
            continue
        if not (0.0 < t < 120.0):
            continue
        out.append({"temp": int(t) if t == int(t) else round(t, 1),
                    "pwm": int(clamp(v, 0, 255))})
    out.sort(key=lambda x: x["temp"])
    # 温度点去重(保留后者)
    dedup: list[dict[str, Any]] = []
    for p in out:
        if dedup and p["temp"] == dedup[-1]["temp"]:
            dedup[-1] = p
        else:
            dedup.append(p)
    return dedup if len(dedup) >= 2 else [dict(p) for p in DEFAULT_CURVE]


def sanitize_pid_params(data: Any) -> dict[str, float] | None:
    """校验 PID 参数; 缺失/非法/三个增益全为 0/目标温度越界 → None(使用默认参数)"""
    if not isinstance(data, dict):
        return None
    try:
        kp = float(data.get("kp") or 0)
        ki = float(data.get("ki") or 0)
        kd = float(data.get("kd") or 0)
        tgt = float(data.get("target") or 0)
    except (TypeError, ValueError):
        return None
    if kp <= 0 and ki <= 0 and kd <= 0:
        return None
    if not (CFG["TEMP_VALID_MIN"] < tgt < CFG["EMERGENCY_TEMP"]):
        return None
    return {
        "kp": max(0.0, kp),
        "ki": max(0.0, ki),
        "kd": max(0.0, kd),
        "target": tgt,
    }


def sanitize_temp_source(data: Any) -> str | None:
    """校验控温依据选择; 缺失/非法 → None(保持现值)"""
    if not isinstance(data, str):
        return None
    return data if data in TEMP_SOURCES else None


def sanitize_boot_params(data: Any) -> dict[str, int] | None:
    """校验 boot boost 参数; 缺失/非法/为 0/越界(不在 MIN~MAX PWM 区间) → None(使用脚本默认)"""
    if not isinstance(data, dict):
        return None
    try:
        bmin = int(float(data.get("boot_min_pwm") or 0))
        boost = int(float(data.get("boot_boost") or 0))
    except (TypeError, ValueError):
        return None
    if bmin <= 0 and boost <= 0:
        return None
    lo, hi = int(CFG["MIN_PWM"]), int(CFG["MAX_PWM"])
    if not (lo <= bmin <= hi) or not (lo <= boost <= hi):
        return None
    return {"boot_min_pwm": bmin, "boot_boost": boost}


def load_config() -> None:
    """读取持久化配置; manual_mode 不持久化, 重启自动回到自动温控"""
    global CURVE, fan_control_enabled, manual_mode, manual_pwm
    manual_mode = False
    pid_note = " pid=默认参数"
    boot_note = " boot=默认参数"
    try:
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                if "curve" in data:
                    CURVE = sanitize_curve(data["curve"])
                if "fan_control_enabled" in data:
                    fan_control_enabled = bool(data["fan_control_enabled"])
                if "manual_pwm" in data:
                    # 下限 PWM_HW_MIN(16): 更小的值写进硬件会被取整到 12 而停转
                    manual_pwm = int(clamp(float(data["manual_pwm"]),
                                           int(CFG["PWM_HW_MIN"]), 255))
                pid_in = sanitize_pid_params(data.get("pid"))
                if pid_in:
                    CFG["PID_KP"] = pid_in["kp"]
                    CFG["PID_KI"] = pid_in["ki"]
                    CFG["PID_KD"] = pid_in["kd"]
                    CFG["PID_TARGET"] = pid_in["target"]
                    pid_note = (f" pid=kp:{pid_in['kp']} ki:{pid_in['ki']} "
                                f"kd:{pid_in['kd']} target:{pid_in['target']}")
                else:
                    pid_note = " pid=默认参数"
                boot_in = sanitize_boot_params(data.get("boot"))
                if boot_in:
                    CFG["BOOT_MIN_PWM"] = boot_in["boot_min_pwm"]
                    CFG["BOOT_BOOST_PWM"] = boot_in["boot_boost"]
                    boot_note = (f" boot=min:{boot_in['boot_min_pwm']} "
                                 f"boost:{boot_in['boot_boost']}")
                else:
                    boot_note = " boot=默认参数"
                ts_in = sanitize_temp_source(data.get("temp_source"))
                if ts_in:
                    CFG["TEMP_SOURCE"] = ts_in
                    boot_note += f" temp_source={ts_in}"
            log.info(
                f"加载配置 {CONFIG_PATH}: curve={len(CURVE)}点 "
                f"enable={fan_control_enabled} manual_pwm={manual_pwm}"
                f"{pid_note}{boot_note}"
            )
            return
    except Exception as e:
        log.warning(f"读取配置失败 {CONFIG_PATH}: {e}")
    CURVE = [dict(p) for p in DEFAULT_CURVE]
    save_config()


def save_config() -> None:
    """原子写入, 断电不会留下半截 JSON"""
    try:
        parent = os.path.dirname(CONFIG_PATH)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with state_lock:
            payload = {
                "curve": [dict(p) for p in CURVE],
                "fan_control_enabled": bool(fan_control_enabled),
                "manual_pwm": int(manual_pwm),
                "pid": {
                    "kp": float(CFG["PID_KP"]),
                    "ki": float(CFG["PID_KI"]),
                    "kd": float(CFG["PID_KD"]),
                    "target": float(CFG["PID_TARGET"]),
                },
                "boot": {
                    "boot_min_pwm": int(CFG["BOOT_MIN_PWM"]),
                    "boot_boost": int(CFG["BOOT_BOOST_PWM"]),
                },
                "temp_source": str(CFG["TEMP_SOURCE"]),
            }
        tmp = CONFIG_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        os.replace(tmp, CONFIG_PATH)
    except Exception as e:
        log.warning(f"保存配置失败 {CONFIG_PATH}: {e}")


def apply_pid_params(params: dict[str, float]) -> None:
    """写入 CFG 并即时生效到运行中的 PID 实例"""
    CFG["PID_KP"] = params["kp"]
    CFG["PID_KI"] = params["ki"]
    CFG["PID_KD"] = params["kd"]
    CFG["PID_TARGET"] = params["target"]
    if g_pid is not None:
        g_pid.kp = params["kp"]
        g_pid.ki = params["ki"]
        g_pid.kd = params["kd"]
        g_pid.setpoint = params["target"]


def curve_to_step_table(curve: list[dict[str, Any]]) -> list[tuple[float, float, int]]:
    """UI 曲线 → 引擎阶梯表 (触发温度, 回落阈值, PWM)。回落列暂未参与查表"""
    table: list[tuple[float, float, int]] = []
    for pt in sorted(curve, key=lambda x: x["temp"]):
        t = float(pt["temp"])
        down = t - 2.0
        if table:
            down = max(down, table[-1][0] + 0.1)
        table.append((t, down, int(pt["pwm"])))
    if len(table) < 2:
        table = [(35.0, 33.0, 55), (70.0, 68.0, 255)]
    return table


def rescale_step_table(cfg: dict[str, Any], quiet: bool = False) -> None:
    """阶梯表中低于 MIN_PWM 的档位按比例抬进 [MIN_PWM, MAX_PWM],
    避免整档被 clamp 成同一个值而丢失低温区分度"""
    if not cfg["RESCALE_TABLE"] or not cfg["STEP_TABLE"]:
        return
    lo, hi = int(cfg["MIN_PWM"]), int(cfg["MAX_PWM"])
    table = cfg["STEP_TABLE"]
    raw_lo = min(row[2] for row in table)
    raw_hi = max(row[2] for row in table)
    if raw_hi <= raw_lo or hi <= lo:
        return
    new_table = []
    changed = 0
    for up, down, pwm in table:
        scaled = lo + (pwm - raw_lo) * (hi - lo) / (raw_hi - raw_lo)
        new_pwm = int(round(clamp(scaled, lo, hi)))
        if new_pwm != pwm:
            changed += 1
        new_table.append((up, down, new_pwm))
    cfg["STEP_TABLE"] = new_table
    if changed and not quiet:
        log.info(
            f"阶梯表按 [{lo},{hi}] 重映射 {changed}/{len(table)} 档 "
            f"(原始 {raw_lo}..{raw_hi}); RESCALE_TABLE=False 可关闭"
        )


def rebuild_step_table(cfg: dict[str, Any], quiet: bool = False) -> None:
    """从当前 CURVE 重新生成引擎表, 再按 MIN/MAX PWM 重映射"""
    with state_lock:
        curve = [dict(p) for p in CURVE]
    cfg["STEP_TABLE"] = curve_to_step_table(curve)
    rescale_step_table(cfg, quiet=quiet)


# ====================================================================== 命令行参数

def parse_args() -> argparse.Namespace:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--min-pwm", type=int, default=CFG["MIN_PWM"],
                        help=f"运行 PWM 下限 (默认 {CFG['MIN_PWM']})")
    common.add_argument("--max-pwm", type=int, default=CFG["MAX_PWM"],
                        help=f"PWM 上限 (默认 {CFG['MAX_PWM']})")
    common.add_argument("--boot-min-pwm", type=int, default=CFG["BOOT_MIN_PWM"],
                        help="boot-boost 交回控制后的最低 PWM")
    common.add_argument("--boot-boost", type=int, default=CFG["BOOT_BOOST_PWM"],
                        help="启动升速 PWM 值")
    common.add_argument("--port", type=int, default=HTTP_PORT,
                        help=f"WebUI HTTP 端口 (默认 {HTTP_PORT})")
    common.add_argument("--no-web", action="store_true", help="不启动 HTTP 服务")
    common.add_argument("--dry-run", action="store_true", help="试运行, 不写硬件")
    common.add_argument("--debug", action="store_true", help="开启调试日志")

    parser = argparse.ArgumentParser(
        description=f"QNAP 8528 (TS-453Dmini) 风扇控制器 {VERSION} (pid/linear/step 三模式合一)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
模式:
  pid      PID 闭环控温(推荐, 温度稳定在 target 附近)
  linear   曲线线性插值(随温度连续变化)
  step     阶梯映射(分段定速, 最省 CPU、行为最可预测)

示例:
  %(prog)s pid --kp 11.5 --ki 0.032 --kd 2.6 --target 52
  %(prog)s linear --min-pwm 68 --boot-boost 140
  %(prog)s step --dry-run --debug

WebUI:
  浏览器打开 http://<nas-ip>:18080 (页面与脚本分离, 读取同目录 UI 页面:
  fanui.html / fanwebui.html / ui.html / webui.html / fanweb.html, 按序取首个存在的)

systemd:
  ExecStart=/usr/bin/python3 /usr/local/bin/qnap-fan-control.py pid --kp 11.5 --ki 0.032 --kd 2.6 --target 52
        """,
    )
    sub = parser.add_subparsers(dest="mode", required=True, help="控制模式")

    p_pid = sub.add_parser("pid", parents=[common], help="PID 闭环控制(推荐)")
    p_pid.add_argument("--kp", type=float, default=CFG["PID_KP"], help="比例系数")
    p_pid.add_argument("--ki", type=float, default=CFG["PID_KI"], help="积分系数")
    p_pid.add_argument("--kd", type=float, default=CFG["PID_KD"], help="微分系数")
    p_pid.add_argument("--target", type=float, default=CFG["PID_TARGET"],
                       help="目标控温温度(℃)")

    sub.add_parser("linear", parents=[common], help="曲线线性插值")
    sub.add_parser("step", parents=[common], help="阶梯映射")
    return parser.parse_args()


def apply_args(cfg: dict[str, Any], args: argparse.Namespace) -> None:
    hw_lo, hw_hi = int(cfg["PWM_HW_MIN"]), int(cfg["PWM_HW_MAX"])

    cfg["MIN_PWM"] = int(clamp(args.min_pwm, hw_lo, hw_hi))
    cfg["MAX_PWM"] = int(clamp(args.max_pwm, hw_lo, hw_hi))
    if cfg["MAX_PWM"] < cfg["MIN_PWM"]:
        log.warning(
            f"参数警告: MAX_PWM({cfg['MAX_PWM']}) < MIN_PWM({cfg['MIN_PWM']}), "
            f"自动修正 MAX_PWM = MIN_PWM"
        )
        cfg["MAX_PWM"] = cfg["MIN_PWM"]

    cfg["BOOT_MIN_PWM"] = int(clamp(args.boot_min_pwm, hw_lo, hw_hi))
    if cfg["BOOT_MIN_PWM"] < cfg["MIN_PWM"]:
        log.warning(
            f"参数警告: BOOT_MIN_PWM({cfg['BOOT_MIN_PWM']}) < MIN_PWM({cfg['MIN_PWM']}), "
            f"自动修正为 MIN_PWM"
        )
        cfg["BOOT_MIN_PWM"] = cfg["MIN_PWM"]
    if cfg["BOOT_MIN_PWM"] > cfg["MAX_PWM"]:
        cfg["BOOT_MIN_PWM"] = cfg["MAX_PWM"]

    cfg["BOOT_BOOST_PWM"] = int(clamp(args.boot_boost, hw_lo, hw_hi))
    if cfg["BOOT_BOOST_PWM"] < cfg["BOOT_MIN_PWM"]:
        log.warning(
            f"参数警告: BOOT_BOOST_PWM({cfg['BOOT_BOOST_PWM']}) < BOOT_MIN_PWM"
            f"({cfg['BOOT_MIN_PWM']}), 启动阶段实际按硬件下限输出"
        )

    if args.mode == "pid":
        cfg["PID_KP"] = max(0.0, float(args.kp))
        cfg["PID_KI"] = max(0.0, float(args.ki))
        cfg["PID_KD"] = max(0.0, float(args.kd))
        cfg["PID_TARGET"] = float(args.target)
        if not (cfg["TEMP_VALID_MIN"] < cfg["PID_TARGET"] < cfg["EMERGENCY_TEMP"]):
            log.warning(
                f"目标温度 {cfg['PID_TARGET']}℃ 不在 "
                f"({cfg['TEMP_VALID_MIN']}, {cfg['EMERGENCY_TEMP']}) 有效区间, 请确认参数"
            )
        if cfg["PID_KP"] <= 0.0 and cfg["PID_KI"] <= 0.0 and cfg["PID_KD"] <= 0.0:
            log.warning("Kp/Ki/Kd 全为 0, PID 输出将恒为下限值, 请确认参数")

    rebuild_step_table(cfg)


# ====================================================================== 状态快照与 HTTP 服务

def build_status() -> dict[str, Any]:
    """webui.html 轮询用的状态快照 (只读缓存, 不额外访问磁盘)"""
    fc = g_fc
    if fc is None:
        return {"ok": False, "error": "hwmon 未就绪"}
    cpu_t = fc.read_cpu_temp()
    ctrl = fc.filtered_ctrl_temp
    if ctrl is None:
        ctrl = fc.get_control_temp(cpu_t)
    pwm_path = os.path.join(fc.qnap_hwmon, "pwm1") if fc.qnap_hwmon else ""
    rpm = fc.read_fan_rpm()
    with state_lock:
        curve = [dict(p) for p in CURVE]
    return {
        "version": VERSION,
        "cpu_temp": None if cpu_t is None else round(cpu_t),
        "cpu_core_temps": [round(v) for v in fc.read_cpu_core_temps()],
        "nvme0_temp": None,
        "nvme1_temp": None,
        "board_temps": fc.read_board_temps(),
        "disk_temps": fc.disk_slot_temps(),
        "avg_temp": None if ctrl is None else round(ctrl, 1),
        "ctrl_temp_raw": None if ctrl is None else round(ctrl, 1),
        "pwm": fc.read_current_pwm(),
        "target_pwm": int(fc.last_target_pwm),
        "fan_raw": rpm,
        "fan_rpm": rpm,
        "manual_mode": manual_mode,
        "manual_pwm": manual_pwm,
        "fan_control_enabled": fan_control_enabled,
        "run_mode": g_run_mode,
        "dry_run": fc.dry_run,
        "curve": curve,
        "pid": {
            "kp": CFG["PID_KP"], "ki": CFG["PID_KI"],
            "kd": CFG["PID_KD"], "target": CFG["PID_TARGET"],
        },
        "boot": {
            "boot_min_pwm": int(fc.cfg["BOOT_MIN_PWM"]),
            "boot_boost": int(fc.cfg["BOOT_BOOST_PWM"]),
            "boot_secs": float(fc.cfg["BOOT_BOOST_SECS"]),
        },
        "temp_source": str(CFG["TEMP_SOURCE"]),
        "step_table": [{"temp": up, "pwm": pwm} for up, _down, pwm in fc.cfg["STEP_TABLE"]],
        "min_pwm": fc.cfg["MIN_PWM"],
        "max_pwm": fc.cfg["MAX_PWM"],
        "system": {
            "module_loaded": bool(fc.qnap_hwmon),
            "pwm_accessible": bool(pwm_path) and os.access(pwm_path, os.W_OK),
            "hwmon_path": fc.qnap_hwmon or "--",
            "config_path": CONFIG_PATH,
            "ui_path": UI_PATH,
            "log_path": LOG_PATH or "(disabled)",
        },
        "defaults": {
            "pid": dict(DEFAULT_PID_PARAMS),
            "boot": {
                "boot_min_pwm": int(CFG["BOOT_MIN_PWM"]),
                "boot_boost": int(CFG["BOOT_BOOST_PWM"]),
            },
            "temp_source": "all",
        },
        "events": list(EVENTS)[-50:][::-1],
    }


def _apply_manual_output(pwm: int) -> None:
    """手动模式立即写硬件, 不等主循环"""
    fc = g_fc
    if fc is None or not fan_control_enabled:
        return

    # 手动模式：只受硬件边界约束，不受 MIN_PWM 自动下限限制
    # 但下限必须是 PWM_HW_MIN(16)：该风扇无停机功能, PWM<15 会停转并触发固件保护
    # (极短停转→最大转速→回 BIOS 默认), 而硬件按百分比取整, 写 15 实际只到 12
    target = int(clamp(float(pwm), int(fc.cfg["PWM_HW_MIN"]), int(fc.cfg["MAX_PWM"])))
    fc.last_target_pwm = target
    fc.set_pwm_raw(target, rate_limit=False, force=True)
    if g_pid is not None:
        g_pid.sync_output(target, pv=fc.filtered_ctrl_temp)


def start_http_server(fc: FanController, pid_obj: PID | None, run_mode: str,
                      port: int = HTTP_PORT) -> None:
    global g_fc, g_pid, g_run_mode
    g_fc = fc
    g_pid = pid_obj
    g_run_mode = run_mode
    try:
        server = ThreadingHTTPServer(("0.0.0.0", port), ApiHandler)
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        log.info(f"HTTP 服务启动 0.0.0.0:{port} 页面文件: {UI_PATH}")
        if not os.path.exists(UI_PATH):
            log.warning(
                f"页面文件不存在: {UI_PATH} "
                f"(把 {'/'.join(UI_CANDIDATES)} 之一放到脚本同目录即可)"
            )
    except Exception as e:
        log.warning(f"HTTP 服务启动失败 {e} (风扇控制不受影响)")


# ====================================================================== PID 控制器类

class PID:
    """测量值微分式 PID: 温度死区 + 积分泄漏 + 微分低通 + 微分/输出限幅 + 抗饱和回算"""

    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg: dict[str, Any] = cfg
        self.kp = float(cfg["PID_KP"])
        self.ki = float(cfg["PID_KI"])
        self.kd = float(cfg["PID_KD"])
        self.setpoint = float(cfg["PID_TARGET"])
        self.int_limit = float(cfg["PID_INT_LIMIT"])
        self.deadband = float(cfg["PID_DEADBAND"])
        self.output_deadband = int(cfg["PID_OUTPUT_DEADBAND"])
        self.min_pwm = int(cfg["MIN_PWM"])
        self.max_pwm = int(cfg["MAX_PWM"])
        self.integral = 0.0
        self.deriv = 0.0
        self.last_pv: float | None = None
        self.last_output = int(clamp(float(cfg["DEFAULT_PWM"]), self.min_pwm, self.max_pwm))
        self.last_dt = float(cfg["LOOP_INTERVAL"])

    def compute(self, pv: float, dt: float) -> int:
        cfg = self.cfg
        if dt <= 0:
            dt = self.last_dt
        dt = clamp(dt, float(cfg["DT_MIN"]), float(cfg["DT_MAX"]))
        self.last_dt = dt

        err = pv - self.setpoint
        # 死区: 误差够小直接沿用上次输出, 跳过全部浮点运算
        if abs(err) <= self.deadband:
            self.integral *= float(cfg["PID_INT_DECAY_FACTOR"])
            self.last_pv = pv
            return self.last_output

        p_term = self.kp * err
        d_term = self._compute_d_term(pv, dt)

        # 抗积分饱和: 仅在未饱和方向累积, 饱和时反向误差仍可退出
        tentative = p_term + self.ki * self.integral + d_term
        saturated = (
            (tentative >= self.max_pwm and err > 0) or
            (tentative <= self.min_pwm and err < 0)
        )
        if not saturated:
            self.integral = self.integral * float(cfg["PID_INT_DECAY_FACTOR"]) + err * dt
            self.integral = clamp(self.integral, -self.int_limit, self.int_limit)

        out = p_term + self.ki * self.integral + d_term
        out_pwm = int(round(clamp(out, self.min_pwm, self.max_pwm)))
        if abs(out_pwm - self.last_output) <= self.output_deadband:
            return self.last_output
        self.last_output = out_pwm
        return out_pwm

    def _compute_d_term(self, pv: float, dt: float) -> float:
        if self.last_pv is None:
            self.last_pv = pv
            return self.kd * self.deriv
        raw = (pv - self.last_pv) / dt
        alpha = float(self.cfg["PID_D_FILTER_ALPHA"])
        self.deriv = alpha * raw + (1.0 - alpha) * self.deriv
        self.last_pv = pv
        limit = float(self.cfg["PID_D_TERM_LIMIT"])
        return clamp(self.kd * self.deriv, -limit, limit)

    def reset(self, output: int | None = None, pv: float | None = None) -> None:
        """故障/移交重置: 不清零积分(保留残余减小切换冲击), 只重置输出与 pv"""
        self.deriv = 0.0
        self.last_pv = pv
        target = self.cfg["DEFAULT_PWM"] if output is None else output
        self.last_output = int(clamp(float(target), self.min_pwm, self.max_pwm))

    def sync_output(self, output: int, pv: float | None = None) -> None:
        """外部(安全保护/手动)改写 PWM 后同步内部状态, 避免下一拍被自身历史值拉回"""
        self.last_output = int(clamp(float(output), self.min_pwm, self.max_pwm))
        if pv is not None:
            self.last_pv = pv


# ====================================================================== 风扇控制器类

class FanController:
    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg: dict[str, Any] = cfg
        self.hdd_temp_paths: list[str] = []
        self.disk_ssd_flag: list[bool] = []
        self.qnap_hwmon: str = ""
        self.cpu_temp_node: str = ""
        self.cpu_core_nodes: list[str] = []   # coretemp 各核心温度节点(不含 Package)
        self.fan_rpm_node: str = ""

        self.last_pwm: int = int(cfg["DEFAULT_PWM"])
        self.last_target_pwm: int = int(cfg["DEFAULT_PWM"])
        self.pwm_fail_cnt: int = 0

        # v1.9 PWM 防抖
        self.pwm_debounce_counter: int = 0
        self.pwm_debounce_last_target: int = int(cfg["DEFAULT_PWM"])

        self.last_hdd_scan: float = 0.0
        self.last_hdd_read: float = 0.0
        self.last_pwm_verify: float = 0.0
        self.cached_hdd_temps: list[float | None] = []
        self._pwm1_enable_saved: str | None = None   # 进入手动模式前的 pwm1_enable 原值

        self.emergency_hold_cnt: int = 0
        self.last_status_log: float = 0.0
        self.last_debug_log: float = 0.0
        self.last_force_recalc: float = 0.0
        self.fan_stall_counter: int = 0
        self.temp_lost_cnt: int = 0
        self.last_sample_ts: float = 0.0
        self.ui_idle_note: bool = False      # 无人看页面时的"已暂停/已恢复"日志去重标记

        self.filtered_ctrl_temp: float | None = None
        self.last_ctrl_temp: float | None = None
        self.boot_boost_active: bool = False
        self.boot_start_time: float = 0.0

        self._last_pwm_drop_time: float = 0.0
        self._last_pwm_for_drop: int = int(cfg["DEFAULT_PWM"])

        self._cpu_err_logged: bool = False
        self._lsblk_done: bool = False
        self.debug_mode: bool = False
        self.dry_run: bool = False

        # systemd sd-notify
        self.notify_socket: str | None = os.environ.get("NOTIFY_SOCKET")
        self.watchdog_usec: int | None = None
        wd = os.environ.get("WATCHDOG_USEC")
        if wd:
            try:
                self.watchdog_usec = int(wd)
            except ValueError:
                pass

    # ---- systemd ----
    def _sd_notify(self, state: str) -> None:
        if not self.notify_socket:
            return
        sock_path = self.notify_socket
        if sock_path[0] == "@":
            sock_path = "\0" + sock_path[1:]
        af_unix = getattr(socket, "AF_UNIX", None)   # Windows 版 socket 模块没有 AF_UNIX, 直接跳过
        if af_unix is None:
            return
        try:
            sock = socket.socket(af_unix, socket.SOCK_DGRAM)
            sock.connect(sock_path)
            sock.sendall(state.encode("utf-8"))
            sock.close()
        except Exception:
            pass

    def notify_ready(self) -> None:
        self._sd_notify("READY=1")

    def notify_stopping(self) -> None:
        self._sd_notify("STOPPING=1")

    def watchdog_ping(self) -> None:
        if self.watchdog_usec is not None:
            self._sd_notify("WATCHDOG=1")

    # ---- 硬件 IO ----
    def restore_pwm(self) -> None:
        log.info("恢复默认风扇 PWM")
        if not self.qnap_hwmon or self.dry_run:
            return
        try:
            with open(os.path.join(self.qnap_hwmon, "pwm1"), "w") as f:
                f.write(str(self.cfg["DEFAULT_PWM"]))
            self.last_pwm = int(self.cfg["DEFAULT_PWM"])
        except Exception as e:
            log.warning(f"恢复 PWM 失败: {e}")
        # 若曾把 pwm1_enable 改为手动模式, 退出时恢复原值(通常为自动模式)
        if self._pwm1_enable_saved is not None and self._pwm1_enable_saved != "1":
            try:
                with open(os.path.join(self.qnap_hwmon, "pwm1_enable"), "w") as f:
                    f.write(self._pwm1_enable_saved)
                log.info(f"pwm1_enable 恢复为 {self._pwm1_enable_saved}")
            except Exception as e:
                log.warning(f"恢复 pwm1_enable 失败: {e}")

    def enable_pwm_manual(self) -> None:
        """部分 hwmon 驱动默认自动模式, 写 pwm1 会被忽略; 需要 pwm1_enable=1。
        记录原值供退出时恢复, 文件不存在则跳过, 失败仅告警。"""
        if not self.qnap_hwmon or self.dry_run:
            return
        path = os.path.join(self.qnap_hwmon, "pwm1_enable")
        try:
            with open(path, "r") as f:
                cur = f.read().strip()
        except FileNotFoundError:
            if self.debug_mode:
                log.debug("pwm1_enable 不存在, 跳过手动模式切换")
            return
        except Exception as e:
            log.warning(f"读取 pwm1_enable 失败: {e}")
            return
        if cur == "1":
            return
        try:
            with open(path, "w") as f:
                f.write("1")
            self._pwm1_enable_saved = cur
            log.info(f"pwm1_enable: {cur} → 1 (切换为手动 PWM 模式)")
        except Exception as e:
            log.warning(f"设置 pwm1_enable=1 失败: {e} (pwm1 写入可能无效)")

    def load_drivetemp(self) -> None:
        try:
            subprocess.run(
                ["modprobe", "drivetemp"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        except Exception:
            log.warning("加载 drivetemp 模块失败, 硬盘温度可能无法读取")

    def read_fan_rpm(self) -> int:
        if not self.fan_rpm_node:
            return 0
        try:
            with open(self.fan_rpm_node, "r") as f:
                return int(f.read().strip())
        except (OSError, ValueError):
            return 0

    def read_current_pwm(self) -> int:
        if self.dry_run:
            return int(self.last_pwm)
        if not self.qnap_hwmon:
            return int(self.cfg["DEFAULT_PWM"])
        try:
            with open(os.path.join(self.qnap_hwmon, "pwm1"), "r") as f:
                return int(f.read().strip())
        except (OSError, ValueError):
            return int(self.last_pwm)

    def read_cpu_temp(self) -> float | None:
        """Package/Tctl 温度作为 CPU 温度; 主节点读取失败时回退第一个核心节点"""
        nodes = [self.cpu_temp_node] if self.cpu_temp_node else []
        if self.cpu_core_nodes:
            nodes.append(self.cpu_core_nodes[0])
        val: float | None = None
        for node in nodes:
            try:
                with open(node, "r") as f:
                    val = int(f.read().strip()) / 1000.0
                break
            except Exception as e:
                if not self._cpu_err_logged:
                    log.error(f"读取 CPU 温度失败({node}): {e}")
                    self._cpu_err_logged = True
        if val is None:
            return None
        self._cpu_err_logged = False
        if self.cfg["TEMP_VALID_MIN"] <= val <= self.cfg["TEMP_VALID_MAX"]:
            return val
        log.warning(f"CPU 温度读数异常 {val:.1f}℃")
        return None

    def read_cpu_core_temps(self) -> list[float]:
        """Package + 各核心温度(℃), 用于 UI CPU 卡片展示; 单个节点失败即跳过"""
        vals: list[float] = []
        for node in ([self.cpu_temp_node] if self.cpu_temp_node else []) + self.cpu_core_nodes:
            try:
                with open(node, "r") as f:
                    vals.append(int(f.read().strip()) / 1000.0)
            except (OSError, ValueError):
                continue
        return vals

    def _build_hctl_cache(self) -> dict[str, bool]:
        """lsblk HCTL→是否SSD 映射, 用于 drivetemp 属性缺失时识别盘型"""
        cache: dict[str, bool] = {}
        if self._lsblk_done:
            return cache
        try:
            result = subprocess.run(
                ["lsblk", "-o", "HCTL,ROTA", "-n", "-l"],
                capture_output=True, text=True, timeout=5, check=False,
            )
            for line in result.stdout.strip().split("\n"):
                parts = line.split()
                if len(parts) >= 2:
                    cache[parts[0]] = (parts[1] == "0")   # ROTA=0 → 非旋转介质
            self._lsblk_done = True
        except Exception:
            pass
        return cache

    def _detect_ssd(self, hwmon: str, hctl_cache: dict[str, bool]) -> bool:
        """盘型识别三级兜底: model 字符串 → device/block/rotational → lsblk HCTL"""
        if not os.path.islink(hwmon):
            return False
        try:
            with open(os.path.join(hwmon, "model"), "r") as f:
                mdl = f.read().strip().lower()
            if "ssd" in mdl or "nvme" in mdl:
                return True
        except OSError:
            pass
        try:
            dev_link = os.path.join(hwmon, "device")
            if os.path.islink(dev_link):
                real = os.path.realpath(dev_link)
                block_dir = os.path.join(real, "block")
                if os.path.isdir(block_dir):
                    for blk_name in os.listdir(block_dir):
                        rot_path = f"/sys/block/{blk_name}/queue/rotational"
                        if os.path.exists(rot_path):
                            with open(rot_path, "r") as f:
                                return int(f.read().strip()) == 0
        except Exception:
            pass
        try:
            dev_link = os.path.join(hwmon, "device")
            if os.path.islink(dev_link):
                m = re.search(r'(\d+:\d+:\d+:\d+)', os.path.realpath(dev_link))
                if m and m.group(1) in hctl_cache:
                    return hctl_cache[m.group(1)]
        except Exception:
            pass
        return False

    def _hctl_target(self, hwmon: str) -> int | None:
        """从 device 链接解析 SCSI HCTL 的 target 字段, SATA 上通常对应物理盘位序号"""
        try:
            real = os.path.realpath(os.path.join(hwmon, "device"))
            m = re.search(r'(\d+):(\d+):(\d+):(\d+)$', real)
            if m:
                return int(m.group(3))
        except Exception:
            pass
        return None

    @staticmethod
    def _temp_label(temp_input: str) -> str:
        """读取温度通道标签(无标签返回空串)"""
        try:
            with open(temp_input.replace("_input", "_label"), "r") as f:
                return f.read().strip()
        except OSError:
            return ""

    @staticmethod
    def _temp_num(path: str) -> int:
        m = re.search(r"temp(\d+)_input", path)
        return int(m.group(1)) if m else 0

    def scan_hwmon_all(self) -> bool:
        """动态扫描 hwmon: 风扇/PWM 控制器、CPU(Package/核心)、硬盘(drivetemp)。
        不写死设备序号/通道号, 全部按名称、标签与节点存在性自动发现"""
        hdd_paths: list[str] = []
        ssd_flags: list[bool] = []
        hdd_entries: list[tuple[int, int, str, bool]] = []   # (槽位序, 枚举序, 温度路径, 是否SSD)
        fan_candidates: list[tuple[int, str, str]] = []      # (优先级, hwmon, fan节点)
        cpu_pkg: list[tuple[int, str]] = []                  # (优先级, Package/Tctl 节点)
        cpu_core_nodes: list[tuple[int, str]] = []           # (核心编号, 温度路径)
        thermal_fallback = ""

        hctl_cache = self._build_hctl_cache()

        for hwmon in sorted(glob.glob("/sys/class/hwmon/hwmon*"), key=hwmon_index):
            try:
                with open(os.path.join(hwmon, "name"), "r") as f:
                    dev_name = f.read().strip()
            except OSError:
                continue
            is_disk = dev_name == "drivetemp"

            # ---- 风扇/PWM 控制器: qnap 名称优先, 其它带 pwm1+fan1_input 的设备兜底 ----
            if "qnap" in dev_name.lower():
                fan_candidates.append((0, hwmon, os.path.join(hwmon, "fan1_input")))
            elif (os.path.exists(os.path.join(hwmon, "pwm1"))
                  and os.path.exists(os.path.join(hwmon, "fan1_input"))):
                fan_candidates.append((1, hwmon, os.path.join(hwmon, "fan1_input")))

            if is_disk:
                # ---- 硬盘(drivetemp): 取该设备的第一个温度通道 ----
                temps = sorted(glob.glob(os.path.join(hwmon, "temp*_input")), key=self._temp_num)
                if not temps:
                    continue
                # 按 SCSI target(≈物理盘位)排序; hwmon 枚举顺序与盘位无关
                tgt = self._hctl_target(hwmon)
                hdd_entries.append(
                    (tgt if tgt is not None else (1 << 20), len(hdd_entries),
                     temps[0], self._detect_ssd(hwmon, hctl_cache))
                )
                continue

            # ---- CPU 温度: 全部 hwmon 的温度通道按标签识别, 不限定驱动名 ----
            for temp in sorted(glob.glob(os.path.join(hwmon, "temp*_input")), key=self._temp_num):
                lbl = self._temp_label(temp)
                m = re.match(r"Core (\d+)", lbl)
                if m:
                    cpu_core_nodes.append((int(m.group(1)), temp))
                elif lbl.startswith("Package") or lbl in ("Tctl", "Tdie"):
                    prio = 0 if dev_name in ("coretemp", "k10temp", "x86_pkg_temp", "cpu_thermal") else 1
                    cpu_pkg.append((prio, temp))

        # ---- 硬盘按槽位排序 ----
        if hdd_entries:
            hdd_entries.sort(key=lambda e: (e[0], e[1]))
            hdd_paths = [e[2] for e in hdd_entries]
            ssd_flags = [e[3] for e in hdd_entries]
            mapping = ", ".join(
                f"槽{ i + 1 }←{'tgt' + str(e[0]) if e[0] < (1 << 20) else '?'}"
                for i, e in enumerate(hdd_entries)
            )
            log.info(f"盘位映射(按SCSI槽位排序): {mapping}")

        # ---- 风扇/PWM 控制器择优 ----
        qnap_node, fan_node = "", ""
        if fan_candidates:
            fan_candidates.sort(key=lambda e: e[0])
            _, qnap_node, fan_node = fan_candidates[0]

        # ---- CPU 节点择优: Package/Tctl 优先; 都没有时回退 thermal_zone(type 含 cpu 优先) ----
        cpu_node = ""
        if cpu_pkg:
            cpu_pkg.sort(key=lambda e: e[0])
            cpu_node = cpu_pkg[0][1]
        if not cpu_node:
            for zone in sorted(glob.glob("/sys/class/thermal/thermal_zone*"), key=hwmon_index):
                ztype = ""
                try:
                    with open(os.path.join(zone, "type"), "r") as f:
                        ztype = f.read().strip().lower()
                except OSError:
                    continue
                t = os.path.join(zone, "temp")
                if not os.path.exists(t):
                    continue
                if "cpu" in ztype or "pkg" in ztype:
                    cpu_node = t
                    break
                if not thermal_fallback:
                    thermal_fallback = t
            if not cpu_node and thermal_fallback:
                cpu_node = thermal_fallback

        if qnap_node:
            self.qnap_hwmon = qnap_node
            self.fan_rpm_node = fan_node
        if cpu_node:
            self.cpu_temp_node = cpu_node
        if cpu_core_nodes:
            cpu_core_nodes.sort(key=lambda e: e[0])
            self.cpu_core_nodes = [p for _, p in cpu_core_nodes]

        # 盘位变化(换盘/掉盘)才清缓存, 避免无故丢失滤波历史
        if hdd_paths != self.hdd_temp_paths:
            self.cached_hdd_temps = []
        self.hdd_temp_paths = hdd_paths
        self.disk_ssd_flag = ssd_flags

        return bool(qnap_node and cpu_node and fan_node)

    def refresh_hdd_temps(self) -> tuple[list[float | None], bool]:
        temps: list[float | None] = []
        has_error = False
        for path in self.hdd_temp_paths:
            try:
                with open(path, "r") as f:
                    val = int(f.read().strip()) / 1000.0
                if self.cfg["TEMP_VALID_MIN"] < val < self.cfg["TEMP_VALID_MAX"]:
                    temps.append(val)
                else:
                    temps.append(None)
                    has_error = True
            except OSError:
                temps.append(None)
                has_error = True
        self.cached_hdd_temps = temps
        return temps, has_error

    def get_control_temp(self, cpu_t: float | None) -> float | None:
        """控温候选集按 TEMP_SOURCE 选择: all=CPU+硬盘最大 / cpu=仅CPU / disk=仅硬盘;
        SSD 达阈值才参与(高温保护优先于来源选择)"""
        source = str(self.cfg.get("TEMP_SOURCE", "all"))
        valid: list[float] = []
        if cpu_t is not None and source != "disk":
            valid.append(cpu_t)
        if source != "cpu":
            for idx, t in enumerate(self.cached_hdd_temps):
                if t is None or t <= 0:
                    continue
                is_ssd = idx < len(self.disk_ssd_flag) and self.disk_ssd_flag[idx]
                if is_ssd and t < self.cfg["SSD_IGNORE_TEMP"]:
                    continue          # SSD 常态温度高 8-10℃, 达阈值前不参与, 达到后保留高温保护
                valid.append(t)
        if not valid:
            return None
        return max(valid)

    def get_max_disk_temp(self) -> tuple[float | None, float | None]:
        max_hdd: float | None = None
        max_ssd: float | None = None
        for idx, t in enumerate(self.cached_hdd_temps):
            if t is None or t <= 0:
                continue
            is_ssd = idx < len(self.disk_ssd_flag) and self.disk_ssd_flag[idx]
            if is_ssd:
                if max_ssd is None or t > max_ssd:
                    max_ssd = t
            else:
                if max_hdd is None or t > max_hdd:
                    max_hdd = t
        return max_hdd, max_ssd

    def read_board_temps(self) -> dict[str, float]:
        """主板温度: 风扇控制器 hwmon 上的全部温度通道, 带标签用标签, 否则 tempN; 不写死通道号"""
        out: dict[str, float] = {}
        if not self.qnap_hwmon:
            return out
        temps = sorted(glob.glob(os.path.join(self.qnap_hwmon, "temp*_input")), key=self._temp_num)
        for path in temps:
            try:
                with open(path, "r") as f:
                    v = int(f.read().strip()) / 1000.0
            except (OSError, ValueError):
                continue
            if self.cfg["TEMP_VALID_MIN"] < v < self.cfg["TEMP_VALID_MAX"]:
                lbl = self._temp_label(path)
                name = re.search(r"(temp\d+)_input", path)
                out[lbl or (name.group(1) if name else path)] = round(v, 1)
        return out

    def disk_slot_temps(self) -> dict[str, float]:
        """硬盘温度: 按扫描顺序编号 1..n, 附带 max_temp (与 webui.html 约定一致)"""
        out: dict[str, float] = {}
        for i, t in enumerate(self.cached_hdd_temps, start=1):
            if t is not None and t > 0:
                out[str(i)] = round(t, 1)
        if out:
            out["max_temp"] = max(out.values())
        return out

    # ---- PWM 输出 ----
    def _clamp_pwm(self, val: float) -> int:
        return int(clamp(float(val), self.cfg["MIN_PWM"], self.cfg["MAX_PWM"]))

    def set_pwm_raw(self, val: int, rate_limit: bool = True, force: bool = False) -> int:
        """直接写硬件:
        force=True：手动模式/安全恢复，绕过 MIN_PWM 自动下限，但仍守 PWM_HW_MIN 硬下限；
        force=False：自动模式，使用 _clamp_pwm 受 MIN_PWM 约束。
        返回实际写入值, 调用方需以此同步内部状态。"""
        hw_lo = int(self.cfg["PWM_HW_MIN"])
        if force:
            # force=True 手动模式、紧急保护：只受硬件边界约束
            # 该风扇无停机功能, PWM<15 会停转并触发固件保护(极短停转→最大转速→回 BIOS 默认),
            # 硬件又按百分比取整(写 15 实际落到 12), 因此下限取 PWM_HW_MIN=16(实际生效 15)
            val_clamp = int(clamp(float(val), hw_lo, int(self.cfg["MAX_PWM"])))
            if float(val) < hw_lo:
                if not getattr(self, "_pwm_min_warned", False):
                    self._pwm_min_warned = True
                    log.warning(
                        f"PWM {int(val)} 低于硬件下限 {hw_lo}"
                        f"(会被百分比取整降到 12 导致停转), 已抬到 {hw_lo}"
                    )
            else:
                self._pwm_min_warned = False
        else:
            # 自动模式：受MIN_PWM下限保护
            val_clamp = self._clamp_pwm(val)

        # 双向速率限幅: 下降慢(静音), 上升也设上限(抑制急升过冲)
        # 紧急/手动/boot-boost 一律走 force=True & rate_limit=False, 不受此处限制
        if rate_limit and not force:
            now = time.monotonic()
            drop_cap = int(float(self.cfg["MAX_PWM_DROP_PER_SEC"])
                           * max(0.0, now - self._last_pwm_drop_time))
            rise_cap = int(float(self.cfg["MAX_PWM_RISE_PER_SEC"])
                           * max(0.0, now - self._last_pwm_drop_time))
            if now - self._last_pwm_drop_time > 0.001:
                if val_clamp < self.last_pwm - drop_cap:
                    val_clamp = self._clamp_pwm(max(val_clamp, self.last_pwm - drop_cap))
                    if self.debug_mode:
                        log.debug(f"PWM 下降限速: 请求{val} → 限为{val_clamp}")
                elif rise_cap > 0 and val_clamp > self.last_pwm + rise_cap:
                    val_clamp = self._clamp_pwm(self.last_pwm + rise_cap)
                    if self.debug_mode:
                        log.debug(f"PWM 上升限速: 请求{val} → 限为{val_clamp}")
            self._last_pwm_drop_time = now
            self._last_pwm_for_drop = val_clamp

        if self.dry_run:
            self.last_pwm = val_clamp
            self.pwm_fail_cnt = 0
            if self.debug_mode:
                log.debug(f"[DryRun] 设置 PWM={val_clamp}")
            return val_clamp
        if not self.qnap_hwmon:
            log.warning("无 hwmon 节点, 无法设置 PWM")
            return val_clamp
        try:
            with open(os.path.join(self.qnap_hwmon, "pwm1"), "w") as f:
                f.write(str(val_clamp))
            self.last_pwm = val_clamp
            self.pwm_fail_cnt = 0
            self._last_pwm_drop_time = time.monotonic()   # 写入成功后重置限速基准
        except OSError as e:
            self.pwm_fail_cnt += 1
            log.warning(f"PWM 写入失败({self.pwm_fail_cnt}): {e}")
            if self.pwm_fail_cnt > int(self.cfg["PWM_FAIL_MAX"]):
                log.critical("PWM 持续写入失败, 恢复默认值后继续运行")
                self.restore_pwm()
                self.pwm_fail_cnt = 0
        return val_clamp

    def set_pwm_debounced(self, target_pwm: int) -> int | None:
        """防抖: 小幅目标变化需连续 PWM_DEBOUNCE_COUNT 次一致才写硬件;
        大变化(|Δ|≥PWM_JUMP_BYPASS)豁免立即写, 避免升降温过程中被防抖拖慢。"""
        target_pwm = self._clamp_pwm(target_pwm)
        if abs(target_pwm - self.last_pwm) >= int(self.cfg["PWM_JUMP_BYPASS"]):
            self.pwm_debounce_counter = 0
            self.pwm_debounce_last_target = target_pwm
            return self.set_pwm_raw(target_pwm)
        if target_pwm != self.pwm_debounce_last_target:
            self.pwm_debounce_counter = 0
            self.pwm_debounce_last_target = target_pwm
        self.pwm_debounce_counter += 1
        if self.pwm_debounce_counter >= int(self.cfg["PWM_DEBOUNCE_COUNT"]):
            if target_pwm != self.last_pwm:
                written = self.set_pwm_raw(target_pwm)
                self.pwm_debounce_counter = 0
                return written
        return None

    def get_smooth_step(self, target_pwm: int, rising: bool = False) -> int:
        table = self.cfg["SMOOTH_UP_STEPS"] if rising else self.cfg["SMOOTH_STEPS"]
        for upper, step in table:
            if target_pwm <= upper:
                return int(step)
        return int(table[-1][1])

    def smooth_adjust(self, target_pwm: int) -> int | None:
        """平滑逼近: 上升走大步进(快速压制温度), 下降走小步进(静音);
        叠加防抖后写硬件。返回实际写入值或 None"""
        target_pwm = self._clamp_pwm(target_pwm)
        rising = target_pwm > self.last_pwm
        step = self.get_smooth_step(target_pwm, rising)
        if self.last_pwm < target_pwm:
            new_pwm = min(self.last_pwm + step, target_pwm)
        elif self.last_pwm > target_pwm:
            new_pwm = max(self.last_pwm - step, target_pwm)
        else:
            return None
        if new_pwm != self.last_pwm:
            return self.set_pwm_debounced(new_pwm)
        return None

    def boot_boost_start(self) -> None:
        log.info(
            f"启动 boot boost: PWM={self.cfg['BOOT_BOOST_PWM']} "
            f"持续 {self.cfg['BOOT_BOOST_SECS']}s"
        )
        note_event("info",
                   f"开机高速运行启动 PWM={self.cfg['BOOT_BOOST_PWM']}"
                   f" 持续 {self.cfg['BOOT_BOOST_SECS']}s")
        self.boot_start_time = time.monotonic()
        self.boot_boost_active = True
        self.set_pwm_raw(self.cfg["BOOT_BOOST_PWM"], rate_limit=False, force=True)

    # ---- 映射 ----
    def calc_step_target(self, temp: float) -> int:
        """阶梯: 按 up 阈值命中档位(down 列预留回差扩展)"""
        for up, _down, pwm in reversed(self.cfg["STEP_TABLE"]):
            if temp >= up:
                return self._clamp_pwm(pwm)
        return self._clamp_pwm(self.cfg["STEP_TABLE"][0][2])

    def calc_linear_target(self, temp: float) -> int:
        """线性: 在阶梯表相邻档位之间插值(整条曲线连续)"""
        table = self.cfg["STEP_TABLE"]
        if not table:
            return self._clamp_pwm(self.cfg["DEFAULT_PWM"])
        if temp <= table[0][0]:
            return self._clamp_pwm(table[0][2])
        if temp >= table[-1][0]:
            return self._clamp_pwm(table[-1][2])
        for i in range(len(table) - 1):
            t1, _d1, p1 = table[i]
            t2, _d2, p2 = table[i + 1]
            if t1 <= temp <= t2:
                if t2 <= t1:
                    return self._clamp_pwm(p2)
                ratio = (temp - t1) / (t2 - t1)
                return self._clamp_pwm(p1 + ratio * (p2 - p1))
        return self._clamp_pwm(table[-1][2])

    def calc_target(self, temp: float, mode: str) -> int:
        if mode == "linear":
            return self.calc_linear_target(temp)
        if mode == "step":
            return self.calc_step_target(temp)
        return int(self.cfg["DEFAULT_PWM"])

    # ---- 日志 ----
    def _temp_line(self, cpu_t, ctrl_temp, _ctrl_raw, tag: str = "") -> str:
        max_hdd, max_ssd = self.get_max_disk_temp()
        cpu_s = f"{cpu_t:.0f}" if cpu_t is not None else "NaN"
        hdd_s = f"{max_hdd:.0f}" if max_hdd is not None else "--"
        ssd_s = f"{max_ssd:.0f}" if max_ssd is not None else "--"
        ctrl_s = f"{ctrl_temp:.1f}" if ctrl_temp is not None else "NaN"
        line = (f"CPU:{cpu_s}|DSK:{hdd_s}|SSD:{ssd_s}|CT:{ctrl_s}"
                f"|TGT:{self.last_target_pwm}|PWM:{self.read_current_pwm()}"
                f"|RPM:{self.read_fan_rpm()}")
        return f"{tag} {line}" if tag else line


# ====================================================================== HTTP 请求处理类

class ApiHandler(BaseHTTPRequestHandler):
    server_version = "qnap-fan/3.0"
    sys_version = ""

    def _send(self, code: int, body: bytes, ctype: str) -> None:
        try:
            self.send_response(code)
            # 全局增加CORS头，兼容file://本地打开页面调试
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            if body:
                self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _json(self, obj, code: int = 200) -> None:
        self._send(code, json.dumps(obj).encode("utf-8"), "application/json; charset=utf-8")

    def _html(self, path: str) -> None:
        try:
            with open(path, "rb") as f:
                body = f.read()
        except OSError as e:
            self._json({"ok": False, "error": f"页面不存在: {path} ({e})"}, 404)
            return
        self._send(200, body, "text/html; charset=utf-8")

    def do_OPTIONS(self):
        """处理浏览器跨域预检请求"""
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        # ---- 原有路径别名全部保留, 根路径返回 UI 页面 ----
        if (path in ("/", "/ui", "/webui", "/index", "/index.html")
                or path.lstrip("/") in UI_CANDIDATES):
            self._html(UI_PATH)
        elif path == "/api/status":
            mark_ui_seen()                 # 有人在看 → 恢复采样
            self._json(build_status())
        elif path == "/api/log":
            mark_ui_seen()
            self._json({"log": list(HISTORY)})
        elif path == "/favicon.ico":
            self._send(204, b"", "image/x-icon")
        else:
            self._json({"ok": False, "error": "not found"}, 404)

    def do_POST(self):
        global CURVE, fan_control_enabled, manual_mode, manual_pwm
        path = self.path.split("?", 1)[0]
        if path != "/api/curve":
            self._json({"ok": False, "error": "not found"}, 404)
            return
        mark_ui_seen()                     # 有人在操作页面 → 恢复采样
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            data = json.loads(raw.decode("utf-8") or "{}")
            if not isinstance(data, dict):
                raise ValueError("body must be a JSON object")
        except Exception as e:
            self._json({"ok": False, "error": f"bad json: {e}"}, 400)
            return

        fc = g_fc
        was_manual = manual_mode
        was_enabled = fan_control_enabled
        curve_changed = False

        # 加锁保护全局配置，防止多客户端并发写损坏json
        with state_lock:
            if isinstance(data.get("curve"), list) and len(data["curve"]) >= 2:
                CURVE = sanitize_curve(data["curve"])
                curve_changed = True

            if "fan_control_enabled" in data:
                fan_control_enabled = bool(data["fan_control_enabled"])
            if "manual_mode" in data:
                manual_mode = bool(data["manual_mode"])
            if "manual_pwm" in data:
                try:
                    # 手动模式：不受自动模式 MIN_PWM 限制，但下限必须是 PWM_HW_MIN(16)
                    # 该风扇无停机功能, 写 15 会被百分比取整落到 12 而停转
                    lo = int(fc.cfg["PWM_HW_MIN"]) if fc else 16
                    hi = int(fc.cfg["MAX_PWM"]) if fc else 255
                    manual_pwm = int(clamp(float(data["manual_pwm"]), lo, hi))
                except (TypeError, ValueError):
                    pass

            # PID参数更新
            pid_out: dict[str, float] | None = None
            if isinstance(data.get("pid"), dict):
                pid_in = sanitize_pid_params(data["pid"])
                apply_pid_params(pid_in or dict(DEFAULT_PID_PARAMS))
                pid_out = {
                    "kp": CFG["PID_KP"], "ki": CFG["PID_KI"],
                    "kd": CFG["PID_KD"], "target": CFG["PID_TARGET"],
                }
                pid_msg = ("默认参数" if pid_in is None
                           else f"kp={pid_in['kp']} ki={pid_in['ki']} kd={pid_in['kd']} target={pid_in['target']}")
                log.info("PID 参数更新: " + pid_msg)
                note_event("info", "PID 参数更新: " + pid_msg)

            # boot参数更新
            boot_out: dict[str, int] | None = None
            if isinstance(data.get("boot"), dict):
                boot_in = sanitize_boot_params(data["boot"])
                if boot_in:
                    CFG["BOOT_MIN_PWM"] = boot_in["boot_min_pwm"]
                    CFG["BOOT_BOOST_PWM"] = boot_in["boot_boost"]
                boot_out = {
                    "boot_min_pwm": int(CFG["BOOT_MIN_PWM"]),
                    "boot_boost": int(CFG["BOOT_BOOST_PWM"]),
                }
                boot_msg = ("默认参数" if boot_in is None
                            else f"min={boot_in['boot_min_pwm']} boost={boot_in['boot_boost']}")
                log.info("Boot boost 参数更新: " + boot_msg)
                note_event("info", "Boot boost 参数更新: " + boot_msg)

            if curve_changed and fc is not None:
                rebuild_step_table(fc.cfg, quiet=True)
                note_event("info", f"控制曲线更新({len(CURVE)}点)")

            if fan_control_enabled != was_enabled:
                note_event("info", f"自动温控{'启用' if fan_control_enabled else '禁用'}")
            if manual_mode != was_manual:
                note_event("warning" if manual_mode else "info",
                           f"手动锁定{'开启' if manual_mode else '解除'} PWM={manual_pwm}")

            save_config()

        # 锁释放之后执行硬件输出，不要把硬件IO放到锁内部
        if manual_mode and fan_control_enabled:
            _apply_manual_output(manual_pwm)
        if was_manual and not manual_mode:
            log.info("退出手动模式, 返回自动温控")
            if fc is not None:
                fc.last_target_pwm = fc.read_current_pwm()
                fc.pwm_debounce_counter = 0
                fc.pwm_debounce_last_target = fc.last_pwm
            if g_pid is not None and fc is not None:
                g_pid.reset(output=fc.read_current_pwm(), pv=fc.filtered_ctrl_temp)

        ts_out = str(CFG["TEMP_SOURCE"])
        if "temp_source" in data:
            ts_in = sanitize_temp_source(data.get("temp_source"))
            if ts_in and ts_in != CFG["TEMP_SOURCE"]:
                CFG["TEMP_SOURCE"] = ts_in
                log.info(f"控温依据更新: {ts_in}")
                note_event("info", f"控温依据更新: {ts_in}")
            ts_out = str(CFG["TEMP_SOURCE"])

        log.info(
            f"API 更新 curve={len(CURVE)}点 enable={fan_control_enabled} "
            f"manual={manual_mode} pwm={manual_pwm}"
        )
        with state_lock:
            curve_out = [dict(p) for p in CURVE]
        self._json({
            "ok": True, "curve": curve_out,
            "fan_control_enabled": fan_control_enabled,
            "manual_mode": manual_mode, "manual_pwm": manual_pwm,
            "pid": {
                "kp": CFG["PID_KP"], "ki": CFG["PID_KI"],
                "kd": CFG["PID_KD"], "target": CFG["PID_TARGET"],
            },
            "boot": {
                "boot_min_pwm": int(CFG["BOOT_MIN_PWM"]),
                "boot_boost": int(CFG["BOOT_BOOST_PWM"]),
            },
            "temp_source": ts_out,
        })

    def log_message(self, format: str, *args: Any) -> None:
        pass


# ====================================================================== 主流程

def main() -> None:
    load_config()
    load_history()
    args = parse_args()
    cfg = CFG
    apply_args(cfg, args)

    fc = FanController(cfg)
    fc.dry_run = args.dry_run
    fc.debug_mode = args.debug
    if fc.debug_mode:
        log.setLevel(logging.DEBUG)
        for h in log.handlers:
            h.setLevel(logging.DEBUG)

    def sig_handler(signum: int, _frame) -> None:
        log.info(f"收到信号 {signum}, 准备退出")
        fc.notify_stopping()
        # 监控模式不写 PWM, 避免退出时改动风扇
        if fan_control_enabled:
            fc.restore_pwm()
        sys.exit(0)

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    fc.load_drivetemp()
    log.info("开始扫描 hwmon 硬件")
    scan_ok = False
    for i in range(int(cfg["SCAN_RETRY_MAX"])):
        if fc.scan_hwmon_all():
            scan_ok = True
            break
        log.warning(
            f"hwmon 扫描失败, 等待 {cfg['SCAN_RETRY_DELAY']}s 重试 "
            f"({i + 1}/{int(cfg['SCAN_RETRY_MAX'])})"
        )
        time.sleep(float(cfg["SCAN_RETRY_DELAY"]))
    if not scan_ok:
        log.critical("hwmon 硬件扫描全部失败, 退出")
        sys.exit(1)
    log.info(
        f"hwmon 扫描成功: fan={os.path.basename(fc.fan_rpm_node)}, "
        f"cpu={os.path.basename(fc.cpu_temp_node)}, 硬盘温度节点 {len(fc.hdd_temp_paths)} 个"
    )
    fc.enable_pwm_manual()

    pid: PID | None = PID(cfg) if args.mode == "pid" else None
    log.info(
        f"PWM 边界: MIN={cfg['MIN_PWM']}, MAX={cfg['MAX_PWM']}, "
        f"BOOT_MIN={cfg['BOOT_MIN_PWM']}, BOOST={cfg['BOOT_BOOST_PWM']}"
    )

    if not args.no_web:
        start_http_server(fc, pid, args.mode, getattr(args, "port", HTTP_PORT))
    else:
        log.info("已指定 --no-web, 不启动 HTTP 服务")

    # 启动后先给 UI_IDLE_SECS 的采样宽限期, 刚开机就打开页面仍有连续数据
    mark_ui_seen()
    if float(cfg["UI_IDLE_SECS"]) > 0:
        log.info(f"无人看页面超过 {int(float(cfg['UI_IDLE_SECS']))}s 将暂停历史采样(温控不受影响)")

    # ---- 初始化温度与首拍目标 ----
    init_cpu = fc.read_cpu_temp()
    fc.last_pwm = fc.read_current_pwm()   # 同步硬件真实 PWM, 避免基准偏差导致低目标时显示不一致
    fc.refresh_hdd_temps()
    now0 = time.monotonic()
    fc.last_hdd_read = now0
    fc.last_hdd_scan = now0
    fc.last_force_recalc = now0
    fc.last_pwm_verify = now0
    init_temp = fc.get_control_temp(init_cpu)
    fc.last_ctrl_temp = init_temp
    fc.filtered_ctrl_temp = init_temp

    if init_temp is not None:
        if pid is not None:
            fc.last_target_pwm = pid.compute(init_temp, float(cfg["LOOP_INTERVAL"]))
        else:
            fc.last_target_pwm = fc.calc_target(init_temp, args.mode)
    fc.last_target_pwm = int(max(fc.last_target_pwm, cfg["BOOT_MIN_PWM"], cfg["MIN_PWM"]))
    if pid is not None:
        pid.sync_output(fc.last_target_pwm, pv=init_temp)

    if manual_mode and fan_control_enabled:
        # 手动下限取 PWM_HW_MIN(16): 写更小值会被硬件取整到 12 并停转
        fc.last_target_pwm = int(clamp(float(manual_pwm), cfg["PWM_HW_MIN"], cfg["MAX_PWM"]))
        log.info(f"手动模式启动, 直接输出 PWM={fc.last_target_pwm}")
        fc.set_pwm_raw(fc.last_target_pwm, rate_limit=False, force=True)
        if pid is not None:
            pid.sync_output(fc.last_target_pwm, pv=init_temp)
    elif not fan_control_enabled:
        log.info("监控模式(fan_control_enabled=False), 不写 PWM, 跳过 boot boost")
    elif not fc.dry_run:
        fc.boot_boost_start()
    else:
        log.info("[DryRun] 试运行模式, 不会写入 PWM")

    def sleep_rest(t0: float) -> None:
        rest = float(cfg["LOOP_INTERVAL"]) - (time.monotonic() - t0)
        if rest > 0:
            time.sleep(rest)

    fc.notify_ready()
    log.info(f"==== QNAP 8528 Fan Controller {VERSION} START ====")
    log.info(
        f"MODE={args.mode}, DRY_RUN={fc.dry_run}, DEBUG={fc.debug_mode}, "
        f"ENABLE={int(fan_control_enabled)}, MANUAL={int(manual_mode)}"
    )
    if pid is not None:
        log.info(
            f"PID 模式: Kp={cfg['PID_KP']}, Ki={cfg['PID_KI']}, Kd={cfg['PID_KD']}, "
            f"target={cfg['PID_TARGET']}℃"
        )
    else:
        log.info(f"{args.mode} 模式: 阶梯表 {[f'{r[0]}℃→{r[2]}' for r in cfg['STEP_TABLE']]}")

    last_pid_time = time.monotonic()
    while True:
        now = time.monotonic()
        fc.watchdog_ping()
        cpu_t = fc.read_cpu_temp()

        # ---- 硬盘节点周期性重扫 + 温度高频读取 ----
        if now - fc.last_hdd_scan >= float(cfg["HDD_REFRESH_INTERVAL"]):
            fc.scan_hwmon_all()
            fc.refresh_hdd_temps()
            fc.last_hdd_scan = now
            fc.last_hdd_read = now
        elif now - fc.last_hdd_read >= float(cfg["HDD_READ_INTERVAL"]):
            fc.refresh_hdd_temps()
            fc.last_hdd_read = now

        # ---- 温度丢失: 立即重扫, 超过宽限周期强制满速 ----
        ctrl_temp_raw = fc.get_control_temp(cpu_t)
        if ctrl_temp_raw is None:
            fc.temp_lost_cnt += 1
            log.warning(
                f"控温温度读取失败, 重新扫描 hwmon "
                f"({fc.temp_lost_cnt}/{cfg['TEMP_LOST_GRACE']})"
            )
            fc.scan_hwmon_all()
            fc.refresh_hdd_temps()
            fc.last_hdd_scan = now
            fc.last_hdd_read = now
            ctrl_temp_raw = fc.get_control_temp(cpu_t)
            if ctrl_temp_raw is None and fc.temp_lost_cnt >= int(cfg["TEMP_LOST_GRACE"]):
                log.critical("控温温度完全丢失, 强制满速 PWM")
                note_event("critical", "控温温度完全丢失, 强制满速 PWM")
                if fan_control_enabled:
                    fc.set_pwm_raw(cfg["MAX_PWM"], rate_limit=False, force=True)
                fc.emergency_hold_cnt = int(cfg["EMERGENCY_HOLD_CNT"])
                if pid is not None:
                    pid.reset(output=cfg["MAX_PWM"], pv=None)
                sleep_rest(now)
                continue
        else:
            fc.temp_lost_cnt = 0

        # ---- 一阶低通滤波 ----
        if ctrl_temp_raw is not None:
            if fc.filtered_ctrl_temp is None:
                fc.filtered_ctrl_temp = ctrl_temp_raw
            else:
                alpha = float(cfg["FILTER_ALPHA"])
                fc.filtered_ctrl_temp = (
                    fc.filtered_ctrl_temp * (1 - alpha) + ctrl_temp_raw * alpha
                )
        ctrl_temp = fc.filtered_ctrl_temp

        # ---- 风扇堵转检测(连续 N 拍无转速) ----
        fan_rpm = 0
        if fc.fan_stall_counter < int(cfg["FAN_STALL_THRESHOLD"]):
            fan_rpm = fc.read_fan_rpm()
        if fan_rpm <= 0:
            fc.fan_stall_counter += 1
            if fc.fan_stall_counter >= int(cfg["FAN_STALL_THRESHOLD"]):
                log.critical("风扇堵转! 强制满速 PWM")
                note_event("critical", "风扇堵转! 强制满速 PWM")
                if fan_control_enabled:
                    fc.set_pwm_raw(cfg["MAX_PWM"], rate_limit=False, force=True)
                fc.emergency_hold_cnt = int(cfg["EMERGENCY_HOLD_CNT"])
                if pid is not None:
                    pid.reset(output=cfg["MAX_PWM"], pv=ctrl_temp)
                fc.fan_stall_counter = 0
                sleep_rest(now)
                continue
        else:
            fc.fan_stall_counter = 0

        # ---- /api/log 采样(2s 一点): 无页面取数时整体跳过(省掉硬盘扫描+落盘) ----
        now_ts = time.time()
        if now_ts - fc.last_sample_ts >= SAMPLE_INTERVAL:
            fc.last_sample_ts = now_ts
            if ui_streaming():
                if fc.ui_idle_note:
                    fc.ui_idle_note = False
                    log.info("UI 已恢复访问, 历史采样继续")
                max_hdd_s, max_ssd_s = fc.get_max_disk_temp()
                sample = {
                    "ts": int(now_ts),
                    "cpu": None if cpu_t is None else int(round(cpu_t)),
                    "avg": None if ctrl_temp is None else round(ctrl_temp, 1),
                    "pwm": fc.read_current_pwm(),
                    "tgt": int(fc.last_target_pwm),
                    "rpm": fan_rpm,
                    "disk": {"max_temp": None if max_hdd_s is None else round(max_hdd_s, 1)},
                    "ssd": None if max_ssd_s is None else round(max_ssd_s, 1),
                }
                HISTORY.append(sample)
                if HISTORY_PATH.lower() not in ("none", "off", ""):
                    _hist_pending.append(json.dumps(sample, separators=(",", ":")) + "\n")
                    flush_history()
            elif not fc.ui_idle_note:
                fc.ui_idle_note = True
                log.info(f"UI 无访问超过 {int(float(cfg['UI_IDLE_SECS']))}s, 暂停历史采样与落盘"
                         f"(温控与安全链路不受影响)")

        if ctrl_temp is None:
            log.critical("控温温度完全丢失, 强制满速 PWM")
            if fan_control_enabled:
                fc.set_pwm_raw(cfg["MAX_PWM"], rate_limit=False, force=True)
            fc.emergency_hold_cnt = int(cfg["EMERGENCY_HOLD_CNT"])
            if pid is not None:
                pid.reset(output=cfg["MAX_PWM"], pv=None)
            sleep_rest(now)
            continue

        # 宽限期内原始读数仍为空: 保持当前目标, 不参与安全阈值判断
        if ctrl_temp_raw is None:
            if fc.debug_mode:
                log.debug("温度读取宽限期内, 保持当前输出")
            if fan_control_enabled:
                fc.smooth_adjust(fc.last_target_pwm)
            sleep_rest(now)
            continue

        # ---- boot-boost 移交: 不清积分, 只对齐输出与 pv, 减少切换冲击 ----
        if fc.boot_boost_active and manual_mode:
            fc.boot_boost_active = False
            log.info("手动模式接管, 跳过 boot boost")
        if fc.boot_boost_active:
            elapsed = now - fc.boot_start_time
            if elapsed < float(cfg["BOOT_BOOST_SECS"]):
                if fan_control_enabled:
                    fc.set_pwm_raw(cfg["BOOT_BOOST_PWM"], rate_limit=False, force=True)
                if fc.debug_mode:
                    log.debug(f"boot boost ({elapsed:.0f}s)")
                sleep_rest(now)
                continue
            fc.boot_boost_active = False
            log.info(f"boot boost 结束 ({cfg['BOOT_BOOST_SECS']}s), 移交 {args.mode} 控制")
            note_event("info", f"开机高速运行结束, 移交 {args.mode} 控制")
            hw_pwm = fc.read_current_pwm()
            fc.last_pwm = hw_pwm
            fc.pwm_debounce_counter = 0
            fc.pwm_debounce_last_target = hw_pwm
            if pid is not None:
                pid.reset(output=hw_pwm, pv=ctrl_temp)
            fc.last_ctrl_temp = ctrl_temp      # 强制下一拍重算
            fc.last_force_recalc = 0.0
            last_pid_time = time.monotonic()

        # ---- 安全判断: 滤波值与原始值取 max, 兼顾防毛刺与响应速度 ----
        safety_temp = max(ctrl_temp, ctrl_temp_raw)
        hardlimit = safety_temp >= float(cfg["HARD_LIMIT_TEMP"])
        emergency = safety_temp >= float(cfg["EMERGENCY_TEMP"])

        if hardlimit or emergency:
            tag = "硬限温" if hardlimit else "紧急超温"
            log_func = log.critical if hardlimit else log.warning
            fc.last_target_pwm = int(cfg["MAX_PWM"])   # 日志/状态立即反映满速目标
            log_func(f"{tag}触发 " + fc._temp_line(cpu_t, ctrl_temp, ctrl_temp_raw, ""))
            note_event("critical" if hardlimit else "warning", f"{tag}触发, 强制满速 PWM")
            if fan_control_enabled:
                fc.set_pwm_raw(cfg["MAX_PWM"], rate_limit=False, force=True)
            fc.emergency_hold_cnt = int(cfg["EMERGENCY_HOLD_CNT"])
            if pid is not None:
                pid.reset(output=cfg["MAX_PWM"], pv=ctrl_temp)
            sleep_rest(now)
            continue

        # ---- 高温保持: 回落后再维持若干周期, 带迟滞避免临界反复进出 ----
        if fc.emergency_hold_cnt > 0:
            exit_thresh = float(cfg["EMERGENCY_TEMP"]) - float(cfg["EMERGENCY_HYST"])
            if safety_temp >= float(cfg["EMERGENCY_TEMP"]):
                fc.emergency_hold_cnt = int(cfg["EMERGENCY_HOLD_CNT"])
                log.warning("温度持续超标, 刷新高温保持周期")
            elif ctrl_temp < exit_thresh:
                fc.emergency_hold_cnt = 0
                log.warning(f"滤波温度低于迟滞阈值 {exit_thresh:.1f}℃, 退出高温保持")
            else:
                fc.emergency_hold_cnt -= 1
                log.warning(f"高温保持, 剩余 {fc.emergency_hold_cnt} 周期")
            if fan_control_enabled:
                fc.set_pwm_raw(cfg["MAX_PWM"], rate_limit=False, force=True)
            if pid is not None:
                pid.sync_output(cfg["MAX_PWM"], pv=ctrl_temp)
            sleep_rest(now)
            continue

        # ---- 目标计算: 手动 > PID > 映射 ----
        if manual_mode:
            # 手动下限 PWM_HW_MIN(16), 低值会被硬件取整到 12 而停转
            target_pwm = int(clamp(float(manual_pwm), cfg["PWM_HW_MIN"], cfg["MAX_PWM"]))
            fc.last_target_pwm = target_pwm
            if fan_control_enabled and fc.read_current_pwm() != target_pwm:
                fc.set_pwm_raw(target_pwm, rate_limit=False, force=True)
                if pid is not None:
                    pid.sync_output(target_pwm, pv=ctrl_temp)
        else:
            if pid is not None:
                # PID 每 PID_INTERVAL 重算一次; dt 用真实间隔, 休眠漂移不影响积分
                if now - last_pid_time >= float(cfg["PID_INTERVAL"]):
                    target_pwm = pid.compute(ctrl_temp, now - last_pid_time)
                    last_pid_time = now
                else:
                    target_pwm = fc.last_target_pwm
            else:
                recalc = False
                if (fc.last_ctrl_temp is None or
                        abs(ctrl_temp - fc.last_ctrl_temp) >= float(cfg["TEMP_HYSTERESIS"])):
                    recalc = True
                    fc.last_ctrl_temp = ctrl_temp
                if now - fc.last_force_recalc >= float(cfg["FORCE_RECALC_INTERVAL"]):
                    recalc = True
                    fc.last_force_recalc = now
                target_pwm = (fc.calc_target(ctrl_temp, args.mode)
                              if recalc else fc.last_target_pwm)
                
            # round做四舍五入，再钳位上下限
            target_pwm = round(target_pwm)
            target_pwm = int(max(target_pwm, cfg["BOOT_MIN_PWM"], cfg["MIN_PWM"]))
            target_pwm = int(min(target_pwm, cfg["MAX_PWM"]))
            fc.last_target_pwm = target_pwm

            # 平滑逼近 + 防抖写硬件; 写入值可能低于目标(限速/上下限), 回灌 PID 防积分漂移
            if fan_control_enabled:
                written = fc.smooth_adjust(target_pwm)
                if written is not None and pid is not None:
                    pid.sync_output(written, pv=ctrl_temp)

        # ---- 硬件回读校验: 目标与实际长期不符说明 hwmon 被外部改写 ----
        if fan_control_enabled and now - fc.last_pwm_verify >= float(cfg["PWM_VERIFY_INTERVAL"]):
            hw_pwm = fc.read_current_pwm()
            if abs(hw_pwm - fc.last_target_pwm) > 2:
                log.warning(
                    f"PWM 校验偏差: 目标 {fc.last_target_pwm}, 硬件 {hw_pwm}, "
                    f"差 {abs(hw_pwm - fc.last_target_pwm)}"
                )
            fc.last_pwm_verify = now

        # ---- INFO 状态日志 ----
        if now - fc.last_status_log >= float(cfg["STATUS_LOG_INTERVAL"]):
            suffix = "MAN" if manual_mode else ("OFF" if not fan_control_enabled else args.mode)
            log.info("STATE " + fc._temp_line(cpu_t, ctrl_temp, ctrl_temp_raw, "") + f"|{suffix}")
            fc.last_status_log = now

        # ---- DEBUG 日志(独立节奏, 不再被 INFO 间隔门控) ----
        if fc.debug_mode and now - fc.last_debug_log >= float(cfg["DEBUG_LOG_INTERVAL"]):
            log.debug("DEBUG " + fc._temp_line(cpu_t, ctrl_temp, ctrl_temp_raw, ""))
            fc.last_debug_log = now

        sleep_rest(now)


if __name__ == "__main__":
    main()
