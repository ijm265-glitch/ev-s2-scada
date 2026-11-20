"""
EV-S2 승강기 통합 감시제어 시스템 (Web SCADA)
  브라우저 <-(WebSocket/HTTP)-> Python(FastAPI) <-(Modbus TCP)-> LS XGB + XBL-EMTA
  (XG5000 프로젝트 EV_s2.xgwx 의 프로그램/변수 기준으로 작성)

EV_s2 = EV_s1 + 스캔 프로그램 P00_P_MIRROR (맨 앞에서 실행)
  F00099(상시 ON) -> MOV P0000 M0010 ... MOV P0005 M0015
  => P 디바이스(센서/버튼/밸브)를 Modbus 비트 읽기 영역(M)으로 복사해서 SCADA 에서 읽음
     P 비트 n  ->  M 비트 (M00100 + n)    예) P0004B 좌문닫힘 -> M0014B

PLC 프로그램 구성 (스캔 순서)
  INIT           : D10/D4/D9/D12, 호출 래치(M0002, M0003 워드) 초기화
  VFD_PARAM      : D1000~D1006 인버터 파라미터 (고속 40Hz / 저속 20Hz / 점검 5Hz)
  P00_P_MIRROR   : P0000~P0005 -> M0010~M0015 (Modbus 모니터용 P 복사)
  P01_POSITION   : 층 센서(P0001~P000C) -> 위치_현재층 D0 (10~60, 층사이 x3/x7), 판단층 D5
  P02_REQUEST    : 바깥 버튼(P0040~49), 화면 카 버튼(M0011~16) -> FF 래치 -> 요청_n층(M0040~45)
  P03_JUDGE      : 현재층/위/아래 요청, 정위치, 문닫힘, 상승정지/하강정지, 갈곳없음 판단
  P04_TRANSITION : 운행상태 D10 전이 (0대기 1상승 2하강 3도착 8점검 9에러)
  P05_DRIVE      : 인버터 상승/하강/정지(M0000/M0001/M0007), 지령주파수 D1005
  P06_ARRIVE     : 도착 -> 문 시퀀스 D9 (0대기 -4초- 1열림 -4초- 2닫힘 -4초- 완료 M0004)
  P07_DISPATCH   : 도착층 호출 삭제, 다음 방향 결정
  P08_OUPUT      : 문 열림/닫힘 밸브(P0025/P0024)

XG-PD Modbus 서버 설정
  비트 읽기 영역  M00000 -> Discrete Input (FC02)  : M 비트 전부 모니터
  비트 쓰기 영역  M00000 -> Coil (FC05)            : 화면_* 버튼 쓰기 (코일 n = M 비트 n)
  워드 읽기 영역  D00000 -> Input Register (FC04)  : D0~D12 상태 워드
  워드 쓰기 영역  D00000 -> Holding Register        : (이 프로그램에서는 사용 안 함)

화면 언어: 한국어 / English (화면 오른쪽 위 KO | EN 전환, 이벤트 메시지도 두 언어로 저장)

이벤트 이력 DB (SQLite, 파이썬 표준 라이브러리 - 추가 설치 없음)
  파일   : ev_scada_events.db (이 파일과 같은 폴더, 환경변수 EV_SCADA_DB 로 경로 변경 가능)
  테이블 : events(id, ts, level, category, message, message_en, run_state, floor, source, mode)
  보관   : LOG_RETENTION_DAYS 일 경과분 자동 삭제
  API    : GET /api/events      (기간/등급/분류/검색 + 페이지)
           GET /api/events.csv  (같은 조건으로 엑셀용 CSV 내보내기, lang=ko|en)
           GET /api/stats       (금일 집계)

설치:  pip install fastapi "uvicorn[standard]" pymodbus
실행:  python ev_scada.py          (실제 PLC 연결)
       python ev_scada.py --sim    (PLC 없이 내장 시뮬레이터로 동작 - 래더 로직을 그대로 이식)
접속:  http://localhost:8000   (다른 기기: http://<PC IP>:8000, 방화벽 8000 허용)
"""
import asyncio
import csv
import io
import json
import os
import sqlite3
import sys
import threading
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel

# ===== 설정: 현장에 맞게 수정 =====
SIM_MODE = "--sim" in sys.argv  # True 로 고정하면 항상 시뮬레이터

PLC_IP = "192.168.10.233"  # PLC(XBL-EMTA) IP  (xgwx FEnet 설정값)
PLC_PORT = 502
UNIT_ID = 1

READ_BIT_BASE = "M00000"   # XG-PD '비트 읽기 영역' 시작
READ_BIT_COUNT = 256       # M00000 ~ M0015F  (M0000~M0005 프로그램 비트 + M0010~M0015 P 미러)
P_MIRROR_BASE = "M00100"   # EV_s2 P00_P_MIRROR: P0000 이 복사되는 M 주소 (P 비트 n -> 이 주소 + n)
P_MIRROR_WORDS = 6         # P0000 ~ P0005
READ_WORD_COUNT = 13       # D0 ~ D12   (XG-PD '워드 읽기 영역' D0 기준)
VFD_BLOCK_ADDR = 1000      # 인버터 블록: D1000~D1006 (FC04 주소 1000, 실제 PLC 에서 읽기 확인함)
VFD_BLOCK_COUNT = 7        #   D1000 고속 / D1001 저속 / D1002~4 명령값 / D1005 지령주파수 / D1006 점검

# XG-PD '비트 쓰기 영역' 시작. 코일 0번 = 이 주소.
#  M00000 이면 코일 n = M 비트 n  (예: 화면_내부1층 M00011 -> 코일 17, 화면_점검 M00054 -> 코일 84)
WRITE_BIT_BASE = "M00000"

POLL_SEC = 0.2             # PLC 폴링 주기 (버튼을 누르면 즉시 한 번 더 읽음)
TIMEOUT = 3
PULSE_SEC = 0.2            # 카 버튼 등 순간 버튼 ON 유지 시간 (FF 상승엣지용, PLC 스캔보다 충분히 김)
HOLD_TIMEOUT = 1.0         # 누르고 있는 버튼(수동운전/열림) 갱신이 끊기면 자동 OFF (안전)

DB_PATH = Path(os.environ.get("EV_SCADA_DB", Path(__file__).with_name("ev_scada_events.db")))
LOG_RETENTION_DAYS = 180   # 이 기간이 지난 이벤트는 자동 삭제
LIVE_LOG_SIZE = 80         # 화면 '실시간 이벤트' 에 보내는 최근 건수
# =================================

# ----- 변수 테이블 (EV_s2.xgwx 글로벌 변수, 95개) : (이름, 디바이스, 비트/워드 인덱스, 타입, 설명)
SYMBOLS = [
    ("문_닫힘요청", "M", 0x3, "BIT", "도착단계 2"),
    ("문_동작완료", "M", 0x4, "BIT", "닫힘 4초 + 문닫힘"),
    ("문_열림요청", "M", 0x2, "BIT", "도착단계 1"),
    ("문_열림유지", "M", 0xD, "BIT", "열림버튼 또는 문센서"),
    ("버튼_1층상행", "P", 0x40, "BIT", "바깥 호출 버튼"),
    ("버튼_2층상행", "P", 0x42, "BIT", "바깥 호출 버튼"),
    ("버튼_2층하행", "P", 0x41, "BIT", "바깥 호출 버튼"),
    ("버튼_3층상행", "P", 0x44, "BIT", "바깥 호출 버튼"),
    ("버튼_3층하행", "P", 0x43, "BIT", "바깥 호출 버튼"),
    ("버튼_4층상행", "P", 0x46, "BIT", "바깥 호출 버튼"),
    ("버튼_4층하행", "P", 0x45, "BIT", "바깥 호출 버튼"),
    ("버튼_5층상행", "P", 0x48, "BIT", "바깥 호출 버튼"),
    ("버튼_5층하행", "P", 0x47, "BIT", "바깥 호출 버튼"),
    ("버튼_6층하행", "P", 0x49, "BIT", "바깥 호출 버튼"),
    ("상태_도착단계", "D", 0x9, "WORD", "0대기 1열림 2닫힘"),
    ("상태_방향", "D", 0x4, "WORD", "0없음 1상승 2하강"),
    ("상태_에러코드", "D", 0xC, "WORD", "1비상정지 2운전중문열림"),
    ("상태_운행", "D", 0xA, "WORD", "0대기 1상승 2하강 3도착 8점검 9에러"),
    ("설정_고속주파수", "D", 0x3E8, "WORD", "4000 = 40Hz"),
    ("설정_상승명령값", "D", 0x3EA, "WORD", "1"),
    ("설정_저속주파수", "D", 0x3E9, "WORD", "2000 = 20Hz"),
    ("설정_점검주파수", "D", 0x3EE, "WORD", "500 = 5Hz"),
    ("설정_정지명령값", "D", 0x3EC, "WORD", "0"),
    ("설정_하강명령값", "D", 0x3EB, "WORD", "3"),
    ("센서_1층상부", "P", 0x2, "BIT", "층 위치 센서"),
    ("센서_1층하부", "P", 0x1, "BIT", "층 위치 센서"),
    ("센서_2층상부", "P", 0x4, "BIT", "층 위치 센서"),
    ("센서_2층하부", "P", 0x3, "BIT", "층 위치 센서"),
    ("센서_3층상부", "P", 0x6, "BIT", "층 위치 센서"),
    ("센서_3층하부", "P", 0x5, "BIT", "층 위치 센서"),
    ("센서_4층상부", "P", 0x8, "BIT", "층 위치 센서"),
    ("센서_4층하부", "P", 0x7, "BIT", "층 위치 센서"),
    ("센서_5층상부", "P", 0xA, "BIT", "층 위치 센서"),
    ("센서_5층하부", "P", 0x9, "BIT", "층 위치 센서"),
    ("센서_6층상부", "P", 0xC, "BIT", "층 위치 센서"),
    ("센서_6층하부", "P", 0xB, "BIT", "층 위치 센서"),
    ("센서_문", "P", 0x4E, "BIT", "문 사이 사람, 물체 감지"),
    ("센서_우문닫힘", "P", 0x4D, "BIT", "문 실린더 센서"),
    ("센서_우문열림", "P", 0x4C, "BIT", "문 실린더 센서"),
    ("센서_좌문닫힘", "P", 0x4B, "BIT", "문 실린더 센서"),
    ("센서_좌문열림", "P", 0x4A, "BIT", "문 실린더 센서"),
    ("스위치_비상정지", "P", 0x4F, "BIT", "A접점"),
    ("요청_1층", "M", 0x40, "BIT", "1층 호출 묶음"),
    ("요청_2층", "M", 0x41, "BIT", "2층 호출 묶음"),
    ("요청_3층", "M", 0x42, "BIT", "3층 호출 묶음"),
    ("요청_4층", "M", 0x43, "BIT", "4층 호출 묶음"),
    ("요청_5층", "M", 0x44, "BIT", "5층 호출 묶음"),
    ("요청_6층", "M", 0x45, "BIT", "6층 호출 묶음"),
    ("위치_판단층", "D", 0x5, "WORD", "정지 판단 기준 층"),
    ("위치_현재층", "D", 0x0, "WORD", "10~60, 층사이 x3 x7"),
    ("인버터_상승", "M", 0x0, "BIT", "p2p블럭으로 VFD에 상승요청"),
    ("인버터_정지", "M", 0x7, "BIT", "p2p블럭으로 VFD에 정지요청"),
    ("인버터_지령주파수", "D", 0x3ED, "WORD", "P2P 0번으로 전송"),
    ("인버터_하강", "M", 0x1, "BIT", "p2p블럭으로 VFD에 하강요청"),
    ("출력_문닫힘밸브", "P", 0x24, "BIT", "문 솔밸브"),
    ("출력_문열림밸브", "P", 0x25, "BIT", "문 솔밸브"),
    ("판단_갈곳없음", "M", 0xF, "BIT", "운전 중 갈 요청 없음"),
    ("판단_문닫힘", "M", 0xE, "BIT", "좌우 실린더 모두 닫힘"),
    ("판단_상승정지", "M", 0x9, "BIT", "올라가는 중 판단층에서 정지"),
    ("판단_아래요청", "M", 0x6, "BIT", "판단층보다 아래에 요청 있음"),
    ("판단_위요청", "M", 0x5, "BIT", "판단층보다 위에 요청 있음"),
    ("판단_저속", "M", 0xB, "BIT", "저속 운전"),
    ("판단_정위치", "M", 0xC, "BIT", "현재층이 10 20 ... 60"),
    ("판단_하강정지", "M", 0xA, "BIT", "내려가는 중 판단층에서 정지"),
    ("판단_현재층요청", "M", 0x8, "BIT", "판단층에 요청 있음"),
    ("호출_1층상행", "M", 0x20, "BIT", "바깥 버튼 래치"),
    ("호출_2층상행", "M", 0x22, "BIT", "바깥 버튼 래치"),
    ("호출_2층하행", "M", 0x21, "BIT", "바깥 버튼 래치"),
    ("호출_3층상행", "M", 0x24, "BIT", "바깥 버튼 래치"),
    ("호출_3층하행", "M", 0x23, "BIT", "바깥 버튼 래치"),
    ("호출_4층상행", "M", 0x26, "BIT", "바깥 버튼 래치"),
    ("호출_4층하행", "M", 0x25, "BIT", "바깥 버튼 래치"),
    ("호출_5층상행", "M", 0x28, "BIT", "바깥 버튼 래치"),
    ("호출_5층하행", "M", 0x27, "BIT", "바깥 버튼 래치"),
    ("호출_6층하행", "M", 0x29, "BIT", "바깥 버튼 래치"),
    ("호출_내부1층", "M", 0x30, "BIT", "카 안 버튼 래치"),
    ("호출_내부2층", "M", 0x31, "BIT", "카 안 버튼 래치"),
    ("호출_내부3층", "M", 0x32, "BIT", "카 안 버튼 래치"),
    ("호출_내부4층", "M", 0x33, "BIT", "카 안 버튼 래치"),
    ("호출_내부5층", "M", 0x34, "BIT", "카 안 버튼 래치"),
    ("호출_내부6층", "M", 0x35, "BIT", "카 안 버튼 래치"),
    ("화면_내부1층", "M", 0x11, "BIT", "HMI 카 안 버튼"),
    ("화면_내부2층", "M", 0x12, "BIT", "HMI 카 안 버튼"),
    ("화면_내부3층", "M", 0x13, "BIT", "HMI 카 안 버튼"),
    ("화면_내부4층", "M", 0x14, "BIT", "HMI 카 안 버튼"),
    ("화면_내부5층", "M", 0x15, "BIT", "HMI 카 안 버튼"),
    ("화면_내부6층", "M", 0x16, "BIT", "HMI 카 안 버튼"),
    ("화면_닫힘", "M", 0x53, "BIT", "HMI 문 닫힘버튼"),
    ("화면_수동닫힘", "M", 0x58, "BIT", "HMI 점검 중 수동 문"),
    ("화면_수동상승", "M", 0x55, "BIT", "HMI 점검 중 수동 운전"),
    ("화면_수동열림", "M", 0x57, "BIT", "HMI 점검 중 수동 문"),
    ("화면_수동하강", "M", 0x56, "BIT", "HMI 점검 중 수동 운전"),
    ("화면_에러리셋", "M", 0x50, "BIT", "HMI"),
    ("화면_열림", "M", 0x52, "BIT", "HMI 문 열림버튼"),
    ("화면_점검", "M", 0x54, "BIT", "HMI 점검 스위치"),
]
SYM = {s[0]: s for s in SYMBOLS}


def addr_str(dev, idx, typ):
    """XG5000 표기: 비트 M0004B (워드4자리+비트hex), 워드 D01000"""
    if typ == "BIT":
        return f"{dev}{idx // 16:04d}{idx % 16:X}"
    return f"{dev}{idx:05d}"


def parse_bit_addr(s):
    """XG5000 표기 -> 비트 인덱스. 'M00100' = 워드 10 의 0번 비트 = 160, 'M0002A' = 워드 2 의 A번 비트.
    4자리 이하('M0010')는 워드 주소로 보고 0번 비트."""
    body = s[1:]
    if len(body) >= 5:
        return int(body[:-1]) * 16 + int(body[-1], 16)
    return int(body) * 16


# ----- 다국어 표기 : (한국어, English)
RUN_NAME = {0: ("대기", "Standby"), 1: ("상승 운행", "Moving up"), 2: ("하강 운행", "Moving down"),
            3: ("도착", "Arrived"), 8: ("점검 운전", "Inspection"), 9: ("이상 정지", "Fault stop")}
DOOR_NAME = {0: ("대기", "idle"), 1: ("열림 동작", "opening"), 2: ("닫힘 동작", "closing")}
ERR_NAME = {0: ("정상", "Normal"), 1: ("비상정지", "Emergency stop"), 2: ("운행 중 도어 열림", "Door opened while running")}


def lang_idx(lang):
    return 1 if lang == "en" else 0


def nm(table, key, i=0):
    """다국어 표에서 이름 꺼내기 (없으면 값 그대로)"""
    v = table.get(key)
    return v[i] if v else ("" if key is None else str(key))


def pick(msg, lang="ko"):
    """(한국어, English) 튜플 또는 문자열 -> 해당 언어 문자열"""
    if isinstance(msg, (tuple, list)):
        return msg[lang_idx(lang)]
    return msg


COMMENT_EN = {
    "도착단계 2": "Arrival stage 2", "닫힘 4초 + 문닫힘": "Close 4 s + door closed", "도착단계 1": "Arrival stage 1",
    "열림버튼 또는 문센서": "Open button or door sensor", "바깥 호출 버튼": "Hall call button",
    "0대기 1열림 2닫힘": "0 idle · 1 open · 2 close", "0없음 1상승 2하강": "0 none · 1 up · 2 down",
    "1비상정지 2운전중문열림": "1 E-stop · 2 door open while running",
    "0대기 1상승 2하강 3도착 8점검 9에러": "0 idle · 1 up · 2 down · 3 arrived · 8 inspection · 9 fault",
    "층 위치 센서": "Floor position sensor", "문 사이 사람, 물체 감지": "Person/object between doors",
    "문 실린더 센서": "Door cylinder sensor", "A접점": "Normally-open contact",
    "정지 판단 기준 층": "Stop decision floor", "10~60, 층사이 x3 x7": "10–60, between floors x3 / x7",
    "p2p블럭으로 VFD에 상승요청": "Up request to VFD via P2P", "p2p블럭으로 VFD에 정지요청": "Stop request to VFD via P2P",
    "p2p블럭으로 VFD에 하강요청": "Down request to VFD via P2P", "P2P 0번으로 전송": "Sent via P2P block 0",
    "문 솔밸브": "Door solenoid valve", "운전 중 갈 요청 없음": "No pending request while running",
    "좌우 실린더 모두 닫힘": "Both door cylinders closed", "올라가는 중 판단층에서 정지": "Stop at decision floor (up)",
    "판단층보다 아래에 요청 있음": "Request below decision floor", "판단층보다 위에 요청 있음": "Request above decision floor",
    "저속 운전": "Low-speed run", "현재층이 10 20 ... 60": "Current floor is 10, 20 … 60",
    "내려가는 중 판단층에서 정지": "Stop at decision floor (down)", "판단층에 요청 있음": "Request at decision floor",
    "바깥 버튼 래치": "Hall button latch", "카 안 버튼 래치": "Car button latch", "HMI 카 안 버튼": "HMI car button",
    "HMI 문 닫힘버튼": "HMI door close button", "HMI 점검 중 수동 문": "HMI manual door (inspection)",
    "HMI 점검 중 수동 운전": "HMI manual run (inspection)", "HMI 문 열림버튼": "HMI door open button",
    "HMI 점검 스위치": "HMI inspection switch",
}


def comment_en(c):
    if c in COMMENT_EN:
        return COMMENT_EN[c]
    if c.endswith("층 호출 묶음"):
        return f"Floor {c[0]} combined request"
    return c


# =====================================================================
#  이벤트 이력 DB (SQLite)
#   level    : info(정보) / warn(주의) / alarm(경보)
#   category : run 운행, arrive 도착, pos 위치, door 도어, call 호출,
#              alarm 알람, comm 통신, op 조작, sys 시스템
# =====================================================================
LEVELS = ("info", "warn", "alarm")
CATEGORIES = ("run", "arrive", "pos", "door", "call", "alarm", "comm", "op", "sys")
LEVEL_NAME = {"info": ("정보", "Info"), "warn": ("주의", "Warning"), "alarm": ("경보", "Alarm")}
CAT_NAME = {"run": ("운행", "Run"), "arrive": ("도착", "Arrival"), "pos": ("위치", "Position"), "door": ("도어", "Door"),
            "call": ("호출", "Call"), "alarm": ("알람", "Fault"), "comm": ("통신", "Comms"), "op": ("조작", "Operator"),
            "sys": ("시스템", "System")}


class EventDB:
    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.Lock()
        self.con = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self.con.row_factory = sqlite3.Row
        self.con.execute("PRAGMA journal_mode=WAL")
        self.con.execute("PRAGMA synchronous=NORMAL")
        self.con.executescript("""
            CREATE TABLE IF NOT EXISTS events(
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                ts        REAL    NOT NULL,          -- epoch 초
                level     TEXT    NOT NULL,
                category  TEXT    NOT NULL,
                message   TEXT    NOT NULL,          -- 한국어
                message_en TEXT,                     -- English
                run_state INTEGER,                   -- 기록 시점 D10
                floor     INTEGER,                   -- 기록 시점 D0
                source    TEXT,                      -- 조작한 클라이언트 IP 등
                mode      TEXT                       -- plc / sim
            );
            CREATE INDEX IF NOT EXISTS ix_events_ts  ON events(ts);
            CREATE INDEX IF NOT EXISTS ix_events_lc  ON events(level, category);
        """)
        cols = {r[1] for r in self.con.execute("PRAGMA table_info(events)")}
        if "message_en" not in cols:            # 이전 버전 DB 자동 업그레이드
            self.con.execute("ALTER TABLE events ADD COLUMN message_en TEXT")

    def insert(self, ts, level, category, message, message_en, run_state, floor, source, mode):
        with self.lock:
            cur = self.con.execute(
                "INSERT INTO events(ts,level,category,message,message_en,run_state,floor,source,mode) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (ts, level, category, message, message_en, run_state, floor, source, mode))
            return cur.lastrowid

    def recent(self, n):
        with self.lock:
            return [dict(r) for r in self.con.execute("SELECT * FROM events ORDER BY id DESC LIMIT ?", (n,))]

    @staticmethod
    def _where(level, category, q, start, end):
        where, args = [], []
        if level in LEVELS:
            where.append("level=?")
            args.append(level)
        if category in CATEGORIES:
            where.append("category=?")
            args.append(category)
        if q:
            where.append("(message LIKE ? OR message_en LIKE ? OR source LIKE ?)")
            args += [f"%{q}%"] * 3
        for val, op, extra in ((start, ">=", 0), (end, "<", 86400)):
            try:
                if val:
                    args.append(datetime.strptime(val, "%Y-%m-%d").timestamp() + extra)
                    where.append(f"ts{op}?")
            except ValueError:
                pass
        return (" WHERE " + " AND ".join(where)) if where else "", args

    def query(self, level="", category="", q="", start="", end="", limit=100, offset=0):
        w, args = self._where(level, category, q, start, end)
        with self.lock:
            total = self.con.execute(f"SELECT COUNT(*) FROM events{w}", args).fetchone()[0]
            rows = [dict(r) for r in self.con.execute(
                f"SELECT * FROM events{w} ORDER BY id DESC LIMIT ? OFFSET ?", args + [limit, offset])]
        return total, rows

    def summary(self, level="", category="", q="", start="", end=""):
        w, args = self._where(level, category, q, start, end)
        with self.lock:
            rows = self.con.execute(f"SELECT level, category, COUNT(*) n FROM events{w} GROUP BY level, category",
                                    args).fetchall()
        out = {"total": 0, "level": defaultdict(int), "category": defaultdict(int)}
        for r in rows:
            out["total"] += r["n"]
            out["level"][r["level"]] += r["n"]
            out["category"][r["category"]] += r["n"]
        return out

    def purge(self, days):
        with self.lock:
            n = self.con.execute("DELETE FROM events WHERE ts<?", (time.time() - days * 86400,)).rowcount
        return n

    def size_bytes(self):
        try:
            return sum(p.stat().st_size for p in (self.path, Path(str(self.path) + "-wal")) if p.exists())
        except OSError:
            return 0

    def close(self):
        with self.lock:
            self.con.close()


db: EventDB | None = None
log = deque(maxlen=LIVE_LOG_SIZE)   # 화면 실시간 이벤트 (최신이 앞)
today = {"date": "", "arrive": 0, "call": 0, "alarm": 0, "op": 0, "total": 0}


def _bump_today(level, category, date):
    if today["date"] != date:
        today.update(date=date, arrive=0, call=0, alarm=0, op=0, total=0)
    today["total"] += 1
    if category in ("arrive", "call", "op"):
        today[category] += 1
    if level == "alarm":
        today["alarm"] += 1


def _row_to_live(r):
    return {"id": r["id"], "t": time.strftime("%H:%M:%S", time.localtime(r["ts"])),
            "d": time.strftime("%Y-%m-%d", time.localtime(r["ts"])),
            "lv": r["level"], "cat": r["category"], "src": r.get("source"),
            "msg": [r["message"], r.get("message_en") or r["message"]]}


def add_log(msg, level="info", category="sys", source=None):
    """msg: (한국어, English) 튜플 또는 문자열(두 언어 공통)"""
    ko, en = (msg[0], msg[1]) if isinstance(msg, (tuple, list)) else (msg, msg)
    ts = time.time()
    tags = state.get("tags") or {}
    run_state, floor = tags.get("상태_운행"), tags.get("위치_현재층")
    rid = None
    if db:
        try:
            rid = db.insert(ts, level, category, ko, en, run_state, floor, source, state["mode"])
        except sqlite3.Error as e:
            print(f"[DB] 이벤트 저장 실패: {e}", file=sys.stderr)
    log.appendleft(_row_to_live({"id": rid, "ts": ts, "level": level, "category": category,
                                 "message": ko, "message_en": en, "source": source}))
    _bump_today(level, category, time.strftime("%Y-%m-%d", time.localtime(ts)))


def init_db():
    global db
    try:
        db = EventDB(DB_PATH)
        purged = db.purge(LOG_RETENTION_DAYS)
        for r in db.recent(LIVE_LOG_SIZE):
            log.append(_row_to_live(r))
        s = db.summary(start=time.strftime("%Y-%m-%d"))
        today.update(date=time.strftime("%Y-%m-%d"), total=s["total"], alarm=s["level"]["alarm"],
                     arrive=s["category"]["arrive"], call=s["category"]["call"], op=s["category"]["op"])
        if purged:
            add_log((f"보관 기간({LOG_RETENTION_DAYS}일)이 지난 이벤트 {purged:,}건을 정리했습니다",
                     f"Purged {purged:,} events older than {LOG_RETENTION_DAYS} days"))
    except sqlite3.Error as e:
        db = None
        print(f"[DB] 이벤트 DB 를 열 수 없습니다 ({DB_PATH}): {e} - 이력 저장 없이 계속합니다", file=sys.stderr)


state = {"mode": "sim" if SIM_MODE else "plc", "connected": False, "updated": None, "error": None,
         "pos": 1.0, "door": 0.0, "vfd_error": None, "tags": {}, "log": [], "today": today,
         "plc": f"{PLC_IP}:{PLC_PORT}", "db": None}
ws_clients: set[WebSocket] = set()
_prev_tags: dict = {}


def call_label(name):
    """'호출_3층상행' -> ('승강장 3층 ▲', 'Hall 3F ▲'),  '호출_내부3층' -> ('카 내부 3층', 'Car 3F')"""
    body = name.split("_", 1)[1]
    if body.startswith("내부"):
        return f"카 내부 {body[2:]}", f"Car {body[2]}F"
    arrow = " ▲" if body.endswith("상행") else " ▼"
    return f"승강장 {body[:-2]}{arrow}", f"Hall {body[0]}F{arrow}"


def track_events(tags):
    """상태 변화 -> 이벤트 로그"""
    p = _prev_tags
    if not p:
        p.update(tags)
        return
    g = tags.get
    run, prun = g("상태_운행"), p.get("상태_운행")
    cur = g("위치_현재층")
    if run is not None and run != prun:
        if run == 3 and cur and cur % 10 == 0:
            add_log((f"{cur // 10}층 도착", f"Arrived at floor {cur // 10}"), "info", "arrive")
        elif prun == 9:
            add_log((f"이상 해제 · 정상 상태로 복귀 (현재 {nm(RUN_NAME, run)})",
                     f"Fault cleared · back to normal ({nm(RUN_NAME, run, 1)})"), "info", "alarm")
        else:
            add_log((f"운행 상태 전환 · {nm(RUN_NAME, prun)} → {nm(RUN_NAME, run)}",
                     f"Status changed · {nm(RUN_NAME, prun, 1)} → {nm(RUN_NAME, run, 1)}"),
                    "warn" if run in (8, 9) else "info", "run")
    er = g("상태_에러코드")
    if er and er != p.get("상태_에러코드"):
        add_log((f"이상 발생 · {nm(ERR_NAME, er) if er in ERR_NAME else '미정의 코드'} (에러코드 {er})",
                 f"Fault · {nm(ERR_NAME, er, 1) if er in ERR_NAME else 'Undefined code'} (code {er})"),
                "alarm", "alarm")
    ds = g("상태_도착단계")
    if ds != p.get("상태_도착단계") and run == 3 and ds in (1, 2):
        add_log((f"도어 {nm(DOOR_NAME, ds)}", f"Door {nm(DOOR_NAME, ds, 1)}"), "info", "door")
    if cur != p.get("위치_현재층") and cur and cur % 10 == 0 and run in (1, 2, 8):
        add_log((f"{cur // 10}층 정위치 감지", f"Level position detected at floor {cur // 10}"), "info", "pos")
    for name, val in tags.items():
        if name.startswith("호출_") and val is not None and p.get(name) is not None and val != p.get(name):
            ko, en = call_label(name)
            add_log((f"호출 {'등록' if val else '해제'} · {ko}", f"Call {'registered' if val else 'cleared'} · {en}"),
                    "info", "call")
    p.clear()
    p.update(tags)


async def broadcast():
    state["log"] = list(log)
    msg = json.dumps(state, ensure_ascii=False)
    for ws in list(ws_clients):
        try:
            await ws.send_text(msg)
        except Exception:
            ws_clients.discard(ws)


# =====================================================================
#  시뮬레이터 : EV_s1 래더 로직을 그대로 파이썬으로 이식 + 물리(카 위치/문) 모델
# =====================================================================
class EVSim:
    V_RATED = 0.5      # 40Hz(4000) 일 때 속도 [층/초]
    ACCEL = 1.0        # [층/초²]
    DOOR_SPEED = 0.5   # 문 개폐 속도 [전체/초]  -> 2초에 완전 개폐
    SENS_EXACT = 0.03  # 정위치(상/하부 센서 동시 ON) 폭
    SENS_RANGE = 0.25  # 상부/하부 센서 감지 길이

    def __init__(self):
        self.M = [0] * 256
        self.P = [0] * 256
        self.D = defaultdict(int)
        self.T = defaultdict(float)
        self.ff_prev = defaultdict(int)
        self.pos, self.vel, self.door = 1.0, 0.0, 0.0
        self.pulses = {}
        self.scan(0, first=True)

    def ton(self, key, inp, preset, dt):
        self.T[key] = self.T[key] + dt if inp else 0.0
        return self.T[key] >= preset

    def ff(self, src_key, inp, m_idx):
        if inp and not self.ff_prev[src_key]:
            self.M[m_idx] ^= 1
        self.ff_prev[src_key] = inp

    def clear_word(self, w):
        for b in range(16):
            self.M[w * 16 + b] = 0

    # --- 센서 (물리 -> P 입력)
    def sensors(self):
        p = self.P
        for n in range(1, 7):
            lo, hi = 2 * n - 1, 2 * n          # 하부 P(2n-1), 상부 P(2n)
            p[lo] = int(n - self.SENS_RANGE <= self.pos <= n + self.SENS_EXACT)
            p[hi] = int(n - self.SENS_EXACT <= self.pos <= n + self.SENS_RANGE)
        opened, closed = int(self.door >= 0.99), int(self.door <= 0.01)
        p[0x4A] = p[0x4C] = opened
        p[0x4B] = p[0x4D] = closed

    def scan(self, dt, first=False):
        m, p, d = self.M, self.P, self.D
        # ---- INIT
        if first:
            d[10] = d[4] = d[9] = d[12] = 0
            self.clear_word(2)
            self.clear_word(3)
        # ---- VFD_PARAM
        d[1000], d[1001], d[1006], d[1004], d[1002], d[1003] = 4000, 2000, 500, 0, 1, 3
        # ---- P01_POSITION
        if p[1] and p[2]:
            d[0] = 10
        if not p[1] and p[2]:
            d[0] = 13
        for n in range(2, 7):
            lo, hi = p[2 * n - 1], p[2 * n]
            if lo and not hi:
                d[0] = (n - 1) * 10 + 7
            if lo and hi:
                d[0] = n * 10
            if n < 6 and not lo and hi:
                d[0] = n * 10 + 3
        d[5] = d[0]
        if d[10] == 1 and d[0] in (17, 27, 37, 47, 57):
            d[5] = d[0] + 3
        if d[10] == 2 and d[0] in (13, 23, 33, 43, 53):
            d[5] = d[0] - 3
        # ---- P02_REQUEST
        for i in range(10):
            self.ff(("P", 0x40 + i), p[0x40 + i], 0x20 + i)
        for i in range(6):
            self.ff(("M", 0x11 + i), m[0x11 + i], 0x30 + i)
        if m[0x54]:
            self.clear_word(2)
            self.clear_word(3)
        m[0x40] = m[0x20] | m[0x30]
        m[0x41] = m[0x21] | m[0x22] | m[0x31]
        m[0x42] = m[0x23] | m[0x24] | m[0x32]
        m[0x43] = m[0x25] | m[0x26] | m[0x33]
        m[0x44] = m[0x27] | m[0x28] | m[0x34]
        m[0x45] = m[0x29] | m[0x35]
        # ---- P03_JUDGE
        req = [m[0x40 + k] for k in range(6)]                     # 요청_1층..6층
        m[0x08] = int(any(req[k] and d[5] == 10 * (k + 1) for k in range(6)))
        m[0x05] = int(any(req[k] and d[5] < 10 * (k + 1) for k in range(1, 6)))
        m[0x06] = int(any(req[k] and d[5] > 10 * (k + 1) for k in range(0, 5)))
        m[0x0C] = int(d[0] in (10, 20, 30, 40, 50, 60))
        m[0x0E] = int(p[0x4B] and p[0x4D])

        def dn(f): return m[0x21 + 2 * (f - 2)] if f >= 2 else 0    # 호출_f층하행
        def up(f): return m[0x22 + 2 * (f - 2)] if f >= 2 else m[0x20]  # 호출_f층상행
        def cab(f): return m[0x30 + f - 1]                            # 호출_내부f층
        up_stop = any(d[5] == 10 * f and ((dn(f) and not m[0x05]) or up(f) or cab(f)) for f in range(2, 6))
        up_stop |= d[5] == 60 and ((dn(6) and not m[0x05]) or cab(6))
        m[0x09] = int(up_stop)
        dn_stop = any(d[5] == 10 * f and (dn(f) or (up(f) and not m[0x06]) or cab(f)) for f in range(2, 6))
        dn_stop |= d[5] == 10 and (m[0x20] or m[0x30])
        m[0x0A] = int(dn_stop)
        m[0x0F] = int((d[10] == 1 and not m[0x05] and not m[0x09]) or
                      (d[10] == 2 and not m[0x06] and not m[0x0A]))
        # ---- P04_TRANSITION
        if d[10] == 0:
            if m[0x08]:
                d[10] = 3
            if m[0x05] and not m[0x08] and m[0x0E]:
                d[10], d[4] = 1, 1
            if m[0x06] and not m[0x08] and m[0x0E]:
                d[10], d[4] = 2, 2
            if m[0x54]:
                d[10], d[4] = 8, 0
        if d[10] == 8 and not m[0x54]:
            d[10] = 0
        if p[0x4F]:
            d[10], d[4], d[12] = 9, 0, 1
        if d[10] in (1, 2) and not m[0x0E]:
            d[10], d[4], d[12] = 9, 0, 2
        if d[10] == 9 and not p[0x4F] and m[0x50]:
            d[10] = d[12] = d[4] = d[9] = 0
            self.clear_word(2)
            self.clear_word(3)
        # ---- P05_DRIVE
        m[0x00] = int((d[10] == 1 or (d[10] == 8 and d[0] != 60 and m[0x55])) and m[0x0E])
        m[0x01] = int((d[10] == 2 or (d[10] == 8 and d[0] != 10 and m[0x56])) and m[0x0E])
        m[0x07] = int(not m[0x00] and not m[0x01])
        m[0x0B] = int((d[0] != d[5] and ((d[10] == 1 and m[0x09]) or (d[10] == 2 and m[0x0A]))) or m[0x0F])
        if m[0x0B] and not m[0x54]:
            d[1005] = d[1001]
        if not m[0x0B] and not m[0x54]:
            d[1005] = d[1000]
        if m[0x54]:
            d[1005] = d[1006]
        # ---- P06_ARRIVE
        if d[10] == 1 and m[0x09] and m[0x0C]:
            d[10] = 3
        if d[10] == 2 and m[0x0A] and m[0x0C]:
            d[10] = 3
        if d[10] in (1, 2) and m[0x0F] and m[0x0C]:
            d[10], d[4] = 0, 0
        if d[10] != 3:
            d[9] = 0
        if d[10] == 0 and m[0x52] and m[0x0C]:
            d[10] = 3
        m[0x0D] = int(m[0x52] or p[0x4E])
        if d[10] == 3 and d[9] == 1 and m[0x53] and not m[0x0D]:
            d[9] = 2
        if d[10] == 3 and d[9] == 2 and m[0x0D]:
            d[9] = 1
        s3 = d[10] == 3
        c0 = s3 and d[9] == 0
        if self.ton("T0000", c0, 4.0, dt) and c0:
            d[9] = 1
        c1 = s3 and d[9] == 1
        m[0x02] = int(c1)
        if self.ton("T0001", c1 and not m[0x0D], 4.0, dt) and c1:
            d[9] = 2
        c2 = s3 and d[9] == 2
        m[0x03] = int(c2)
        m[0x04] = int(self.ton("T0002", c2, 4.0, dt) and c2 and m[0x0E])
        # ---- P07_DISPATCH
        if d[10] == 3:
            if d[0] == 10:
                m[0x20] = m[0x30] = 0
            for f in range(2, 6):
                if d[0] == 10 * f:
                    if d[4] != 1:
                        m[0x21 + 2 * (f - 2)] = 0
                    if d[4] != 2:
                        m[0x22 + 2 * (f - 2)] = 0
                    m[0x30 + f - 1] = 0
            if d[0] == 60:
                m[0x29] = m[0x35] = 0
            if not m[0x05] and not m[0x06]:
                d[4] = 0
            if m[0x05] and (d[4] != 2 or not m[0x06]):
                d[4] = 1
            if m[0x06] and (d[4] != 1 or not m[0x05]):
                d[4] = 2
            if m[0x04]:
                d[10] = {0: 0, 1: 1, 2: 2}.get(d[4], d[10])
        # ---- P08_OUPUT
        p[0x25] = int(m[0x02] or (d[10] == 8 and m[0x57] and not m[0x58]))
        p[0x24] = int(m[0x03] or (d[10] == 0 and not m[0x0E] and not p[0x4E]) or
                      (d[10] == 8 and m[0x58] and not m[0x57]))

    def physics(self, dt):
        m, p, d = self.M, self.P, self.D
        cmd = 1 if (m[0x00] and not m[0x01]) else -1 if (m[0x01] and not m[0x00]) else 0
        if cmd == 0:
            self.vel = 0.0                       # 정지 = 브레이크 (즉시 정지)
        else:
            target = cmd * d[1005] / 4000 * self.V_RATED
            step = self.ACCEL * dt
            self.vel += max(-step, min(step, target - self.vel))
        self.pos = max(0.75, min(6.25, self.pos + self.vel * dt))
        if p[0x25] and not p[0x24]:
            self.door = min(1.0, self.door + self.DOOR_SPEED * dt)
        elif p[0x24] and not p[0x25]:
            self.door = max(0.0, self.door - self.DOOR_SPEED * dt)

    def step(self, dt):
        now = time.monotonic()
        for (dev, idx), until in list(self.pulses.items()):
            if now >= until:
                (self.M if dev == "M" else self.P)[idx] = 0
                del self.pulses[(dev, idx)]
        self.sensors()
        self.scan(dt)
        self.physics(dt)

    def set_bit(self, dev, idx, val):
        (self.M if dev == "M" else self.P)[idx] = int(val)

    def tags(self):
        out = {}
        for name, dev, idx, typ, _ in SYMBOLS:
            out[name] = self.D[idx] if typ == "WORD" else (self.M if dev == "M" else self.P)[idx]
        return out


sim: EVSim | None = None


async def sim_loop():
    dt = 0.05
    n = 0
    while True:
        sim.step(dt)
        n += 1
        if n % 2 == 0:      # 10Hz 로 화면 갱신
            tags = sim.tags()
            track_events(tags)
            state.update(connected=True, updated=time.strftime("%H:%M:%S"), error=None,
                         pos=round(sim.pos, 4), door=round(sim.door, 3), tags=tags, vfd_error=None)
            await broadcast()
        await asyncio.sleep(dt)


# =====================================================================
#  실제 PLC (Modbus TCP)
# =====================================================================
client = None
plc_lock = asyncio.Lock()
READ_BIT_BASE_IDX = parse_bit_addr(READ_BIT_BASE)
P_MIRROR_IDX = parse_bit_addr(P_MIRROR_BASE)
WRITE_BIT_BASE_IDX = parse_bit_addr(WRITE_BIT_BASE)


async def _call(func, *args, **kwargs):
    """pymodbus 버전 차이 흡수 (3.10+: device_id, 이전: slave)"""
    try:
        return await func(*args, device_id=UNIT_ID, **kwargs)
    except TypeError:
        return await func(*args, slave=UNIT_ID, **kwargs)


poll_now = asyncio.Event()   # 쓰기 직후 폴링 대기 없이 바로 읽기

MODBUS_EXC = {1: ("지원하지 않는 기능 코드", "Illegal Function"),
              2: ("XG-PD 워드 읽기 영역을 벗어난 주소", "Illegal Data Address (outside XG-PD word read area)"),
              3: ("개수 또는 값 오류", "Illegal Data Value"),
              4: ("PLC 내부 오류", "Slave Device Failure"),
              6: ("PLC 처리 중", "Slave Device Busy")}


async def read_vfd_block():
    """인버터 블록 D1000~D1006 (FC04). 성공: (값 리스트, None) / 실패: (None, (이유 한국어, English))"""
    try:
        r = await _call(client.read_input_registers, VFD_BLOCK_ADDR, count=VFD_BLOCK_COUNT)
    except Exception as e:
        return None, (f"응답 없음 또는 통신 오류 ({type(e).__name__}: {e})",
                      f"No response or communication error ({type(e).__name__}: {e})")
    if r.isError():
        code = getattr(r, "exception_code", None)
        ko, en = MODBUS_EXC.get(code, (str(r), str(r)))
        return None, (f"예외 코드 {code} · {ko}", f"Exception code {code} · {en}")
    return list(r.registers), None


async def plc_loop():
    door_est, last = 0.0, time.monotonic()
    vfd_err_prev = None
    mirror_ok, mirror_bad = None, 0    # P 미러 프로그램 동작 확인 (판단_문닫힘 M000E == P004B & P004D)
    while True:
        try:
            t0 = time.perf_counter()
            async with plc_lock:
                if not client.connected:
                    await client.connect()
                if not client.connected:
                    raise ConnectionError(f"connect failed {PLC_IP}:{PLC_PORT}")
                di = await _call(client.read_discrete_inputs, 0, count=READ_BIT_COUNT)
                ir = await _call(client.read_input_registers, 0, count=READ_WORD_COUNT)
                vfd_regs, vfd_err = await read_vfd_block()     # 별도 블록 (실패해도 본 폴링은 계속)
            if vfd_err != vfd_err_prev:
                rng = f"D{VFD_BLOCK_ADDR}~D{VFD_BLOCK_ADDR + VFD_BLOCK_COUNT - 1}"
                if vfd_err:
                    add_log((f"VFD 파라미터 블록({rng}) 읽기 실패 · {vfd_err[0]}",
                             f"VFD parameter block ({rng}) read failed · {vfd_err[1]}"), "warn", "comm")
                elif vfd_err_prev:
                    add_log(("VFD 파라미터 블록 읽기 정상 복구", "VFD parameter block read recovered"),
                            "info", "comm")
                vfd_err_prev = vfd_err
            rtt = round((time.perf_counter() - t0) * 1000)
            if di.isError():
                raise IOError(f"FC02 bit read error: {di}")
            if ir.isError():
                raise IOError(f"FC04 word read error: {ir}")
            bits = list(di.bits)[:READ_BIT_COUNT]
            words = list(ir.registers)
            tags = {}
            for name, dev, idx, typ, _ in SYMBOLS:
                if typ == "WORD":
                    if idx < len(words):
                        tags[name] = words[idx]
                    elif vfd_regs is not None and 0 <= idx - VFD_BLOCK_ADDR < len(vfd_regs):
                        tags[name] = vfd_regs[idx - VFD_BLOCK_ADDR]
                    else:
                        tags[name] = None
                else:
                    # M 은 그대로, P 는 미러 영역(M0010~)에서
                    if dev == "M":
                        b = idx - READ_BIT_BASE_IDX
                    else:
                        b = P_MIRROR_IDX + idx - READ_BIT_BASE_IDX if idx < P_MIRROR_WORDS * 16 else -1
                    tags[name] = int(bits[b]) if 0 <= b < len(bits) else None
            # P 미러 검증: PLC 가 같은 스캔에서 M000E = P004B AND P004D 로 계산하므로 서로 맞아야 함
            #   (EV_s1 이 돌고 있으면 미러 영역이 0 이라, 문이 닫혀 있을 때 불일치로 드러남)
            if tags.get("판단_문닫힘") is not None:
                closed_m = tags["판단_문닫힘"]
                closed_p = int(bool(tags.get("센서_좌문닫힘") and tags.get("센서_우문닫힘")))
                ok = mirror_ok
                if closed_m == closed_p:
                    mirror_bad = 0
                    if closed_m:
                        ok = True                 # 닫힘 상태에서 일치 = 미러 동작 확인
                else:
                    mirror_bad += 1
                    if mirror_bad >= 2:           # 스캔 경계 오차 1 회는 무시
                        ok = False
                if ok is not None and ok != mirror_ok:
                    if ok:
                        add_log(("P 디바이스 미러(P00_P_MIRROR) 동작 확인 · 현장 입출력 표시를 시작합니다",
                                 "P-device mirror (P00_P_MIRROR) verified · field I/O display enabled"), "info", "comm")
                    else:
                        add_log(("P 디바이스 미러 값이 PLC 로직과 일치하지 않습니다 · "
                                 "PLC 에 EV_s2(P00_P_MIRROR) 프로그램이 다운로드되었는지 확인하십시오",
                                 "P-device mirror does not match PLC logic · "
                                 "check that EV_s2 (P00_P_MIRROR) is downloaded to the PLC"), "warn", "comm")
                    mirror_ok = ok
            if mirror_ok is False:
                for name, dev, *_ in SYMBOLS:
                    if dev == "P":
                        tags[name] = None
            # 문 위치: 문 센서/밸브(P 미러)가 있으면 사용, 없으면 문_열림/닫힘요청으로 추정
            now = time.monotonic()
            dt, last = now - last, now
            g = tags.get
            opened = g("센서_좌문열림") and g("센서_우문열림")
            closed = g("센서_좌문닫힘") and g("센서_우문닫힘") if g("센서_좌문닫힘") is not None else g("판단_문닫힘")
            v_open = g("출력_문열림밸브") if g("출력_문열림밸브") is not None else g("문_열림요청")
            v_close = g("출력_문닫힘밸브") if g("출력_문닫힘밸브") is not None else g("문_닫힘요청")
            if closed:
                door_est = 0.0
            elif opened:
                door_est = 1.0
            elif v_open and not v_close:
                door_est = min(0.95, door_est + 0.5 * dt)
            elif v_close and not v_open:
                door_est = max(0.05, door_est - 0.5 * dt)
            elif door_est == 0.0:
                door_est = 0.5           # 닫힘 센서 OFF 인데 명령 없음 -> 반쯤 열린 것으로 표시
            cur = tags.get("위치_현재층") or 10
            if not state["connected"]:
                add_log((f"PLC 통신 연결 ({PLC_IP}:{PLC_PORT}, 응답 {rtt} ms)",
                         f"PLC connected ({PLC_IP}:{PLC_PORT}, {rtt} ms)"), "info", "comm")
            state.update(connected=True, tags=tags)
            track_events(tags)
            state.update(pos=cur / 10, door=round(door_est, 3),
                         updated=time.strftime("%H:%M:%S"), error=None, vfd_error=vfd_err, rtt=rtt,
                         p_mirror=mirror_ok)
        except Exception as e:
            if state["connected"] or state["error"] is None:
                add_log((f"PLC 통신 두절 · {e}", f"PLC communication lost · {e}"), "alarm", "comm")
            state.update(connected=False, error=str(e) or type(e).__name__)
            client.close()
        await broadcast()
        try:
            await asyncio.wait_for(poll_now.wait(), POLL_SEC)
        except asyncio.TimeoutError:
            pass
        poll_now.clear()


# =====================================================================
#  쓰기 (공통)
# =====================================================================
holds: dict[str, float] = {}

OP_LABEL = {"화면_점검": ("점검 모드", "Inspection mode"), "화면_에러리셋": ("에러 리셋", "Fault reset"),
            "화면_열림": ("도어 열림 버튼", "Door open button"), "화면_닫힘": ("도어 닫힘 버튼", "Door close button"),
            "화면_수동상승": ("수동 상승", "Manual up"), "화면_수동하강": ("수동 하강", "Manual down"),
            "화면_수동열림": ("수동 도어 열림", "Manual door open"), "화면_수동닫힘": ("수동 도어 닫힘", "Manual door close"),
            "스위치_비상정지": ("비상정지 스위치(시뮬레이션)", "Emergency stop switch (simulation)"),
            "센서_문": ("도어 물체 감지(시뮬레이션)", "Door obstruction sensor (simulation)")}


def op_label(name):
    """(한국어, English)"""
    if name in OP_LABEL:
        return OP_LABEL[name]
    if name.startswith("화면_내부"):
        return f"카 내부 {name[5:]} 버튼", f"Car {name[5]}F button"
    if name.startswith("버튼_"):
        arrow = " ▲" if name.endswith("상행") else " ▼"
        return f"승강장 {name[3:-2]}{arrow} 버튼(시뮬레이션)", f"Hall {name[3]}F{arrow} button (simulation)"
    return name, name


def writable(name):
    s = SYM.get(name)
    if not s or s[3] != "BIT":
        return False
    if name.startswith("화면_"):
        return True
    return SIM_MODE and (name.startswith("버튼_") or name in ("센서_문", "스위치_비상정지"))


async def write_bit(name, val):
    _, dev, idx, _, _ = SYM[name]
    if SIM_MODE:
        sim.set_bit(dev, idx, val)
        return None
    offset = idx - WRITE_BIT_BASE_IDX
    if dev != "M" or offset < 0:
        a = addr_str(dev, idx, 'BIT')
        return (f"{name}({a})은(는) 비트 쓰기 영역({WRITE_BIT_BASE}~)을 벗어납니다",
                f"{name} ({a}) is outside the bit write area ({WRITE_BIT_BASE}~)")
    async with plc_lock:
        if not client.connected:
            return "PLC 가 연결되어 있지 않습니다", "PLC is not connected"
        rr = await _call(client.write_coil, offset, bool(val))
    poll_now.set()
    return str(rr) if rr.isError() else None


async def pulse(name):
    _, dev, idx, _, _ = SYM[name]
    if SIM_MODE:
        sim.set_bit(dev, idx, 1)
        sim.pulses[(dev, idx)] = time.monotonic() + PULSE_SEC
        return None
    err = await write_bit(name, 1)
    if err:
        return err

    async def off():                      # OFF 는 백그라운드로 -> 응답은 즉시
        await asyncio.sleep(PULSE_SEC)
        e = await write_bit(name, 0)
        if e:
            ko, en = op_label(name)
            add_log((f"쓰기 실패 · {ko} OFF ({pick(e)})", f"Write failed · {en} OFF ({pick(e, 'en')})"), "warn", "op")
    asyncio.create_task(off())
    return None


async def hold_watchdog():
    """누르는 동안만 ON 해야 하는 버튼: 브라우저 갱신이 끊기면 자동 OFF"""
    while True:
        now = time.monotonic()
        for name, until in list(holds.items()):
            if now > until:
                holds.pop(name, None)
                await write_bit(name, 0)
                ko, en = op_label(name)
                add_log((f"{ko} 자동 해제 · 조작 신호가 끊겨 안전을 위해 OFF 했습니다",
                         f"{en} auto-released · operator signal lost, switched OFF for safety"), "warn", "op")
        await asyncio.sleep(0.2)


async def maintenance():
    """하루 한 번 보관 기간 지난 이벤트 정리"""
    while True:
        await asyncio.sleep(24 * 3600)
        if db:
            n = db.purge(LOG_RETENTION_DAYS)
            if n:
                add_log((f"보관 기간({LOG_RETENTION_DAYS}일)이 지난 이벤트 {n:,}건을 정리했습니다",
                         f"Purged {n:,} events older than {LOG_RETENTION_DAYS} days"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, sim
    init_db()
    state["db"] = str(DB_PATH) if db else None
    tasks = [asyncio.create_task(hold_watchdog()), asyncio.create_task(maintenance())]
    if SIM_MODE:
        sim = EVSim()
        add_log(("SCADA 서버 시작 · 시뮬레이션 모드 (EV_s1 래더 로직 이식)",
                 "SCADA server started · simulation mode (EV_s1 ladder logic)"))
        tasks.append(asyncio.create_task(sim_loop()))
    else:
        from pymodbus.client import AsyncModbusTcpClient
        client = AsyncModbusTcpClient(PLC_IP, port=PLC_PORT, timeout=TIMEOUT, retries=2)
        add_log((f"SCADA 서버 시작 · PLC 모드 ({PLC_IP}:{PLC_PORT})",
                 f"SCADA server started · PLC mode ({PLC_IP}:{PLC_PORT})"))
        tasks.append(asyncio.create_task(plc_loop()))
    yield
    for t in tasks:
        t.cancel()
    add_log(("SCADA 서버 종료", "SCADA server stopped"))
    if client:
        client.close()
    if db:
        db.close()


app = FastAPI(lifespan=lifespan, title="EV-S2 SCADA")


class CmdReq(BaseModel):
    tag: str
    op: str = "pulse"     # pulse | set | toggle | hold | release
    value: int = 1
    lang: str = "ko"      # 오류 메시지 언어


@app.post("/cmd")
async def cmd(req: CmdReq, request: Request):
    if not writable(req.tag):
        return {"ok": False, "error": pick((f"쓰기가 허용되지 않은 변수입니다: {req.tag}",
                                             f"Tag is not writable: {req.tag}"), req.lang)}
    src = request.client.host if request.client else None
    ko, en = op_label(req.tag)
    note = None                         # 조작 이력에 남길 문구 (hold 갱신 등 반복 신호는 남기지 않음)
    if req.op == "pulse":
        err = await pulse(req.tag)
        note = (f"{ko} 누름", f"{en} pressed")
    elif req.op == "set":
        err = await write_bit(req.tag, 1 if req.value else 0)
        note = (f"{ko} {'ON' if req.value else 'OFF'}", f"{en} {'ON' if req.value else 'OFF'}")
    elif req.op == "toggle":
        new = 0 if state["tags"].get(req.tag) else 1
        err = await write_bit(req.tag, new)
        note = (f"{ko} {'ON' if new else 'OFF'}", f"{en} {'ON' if new else 'OFF'}")
    elif req.op == "hold":
        first = req.tag not in holds
        holds[req.tag] = time.monotonic() + HOLD_TIMEOUT
        err = await write_bit(req.tag, 1) if first else None
        note = (f"{ko} 시작", f"{en} started") if first else None
    elif req.op == "release":
        was = holds.pop(req.tag, None) is not None
        err = await write_bit(req.tag, 0)
        note = (f"{ko} 종료", f"{en} ended") if was else None
    else:
        return {"ok": False, "error": pick((f"알 수 없는 명령 방식입니다: {req.op}", f"Unknown op: {req.op}"), req.lang)}
    if err:
        add_log((f"쓰기 실패 · {ko} ({pick(err)})", f"Write failed · {en} ({pick(err, 'en')})"), "warn", "op", src)
    elif note:
        add_log((f"운전원 조작 · {note[0]}", f"Operator · {note[1]}"), "info", "op", src)
    return {"ok": err is None, "error": pick(err, req.lang) if err else None}


@app.get("/symbols")
async def symbols():
    return [{"name": n, "addr": addr_str(d, i, t), "type": t, "comment": c, "comment_en": comment_en(c),
             "writable": writable(n)} for n, d, i, t, c in SYMBOLS]


def _event_out(r, lang="ko"):
    msg = (r.get("message_en") or r["message"]) if lang == "en" else r["message"]
    return {"id": r["id"], "time": datetime.fromtimestamp(r["ts"]).strftime("%Y-%m-%d %H:%M:%S"),
            "level": r["level"], "category": r["category"], "message": msg,
            "run_state": r["run_state"], "floor": r["floor"], "source": r["source"], "mode": r["mode"]}


@app.get("/api/events")
async def api_events(level: str = "", category: str = "", q: str = "", start: str = "", end: str = "",
                     limit: int = Query(50, ge=1, le=1000), offset: int = Query(0, ge=0), lang: str = "ko"):
    if not db:
        return {"total": 0, "rows": [], "summary": None,
                "error": pick(("이벤트 DB 를 사용할 수 없습니다", "Event database is unavailable"), lang)}
    total, rows = db.query(level, category, q.strip(), start, end, limit, offset)
    s = db.summary(level, category, q.strip(), start, end)
    return {"total": total, "rows": [_event_out(r, lang) for r in rows],
            "summary": {"level": s["level"], "category": s["category"]}}


@app.get("/api/events.csv")
async def api_events_csv(level: str = "", category: str = "", q: str = "", start: str = "", end: str = "",
                         lang: str = "ko"):
    i = lang_idx(lang)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow((["번호", "일시", "등급", "분류", "내용", "운행상태", "위치(D0)", "조작 위치", "모드"],
                ["No.", "Time", "Level", "Category", "Message", "Status", "Position (D0)", "Operator IP", "Mode"])[i])
    modes = {"sim": ("시뮬레이션", "Simulation"), "plc": ("PLC", "PLC")}
    if db:
        _, rows = db.query(level, category, q.strip(), start, end, 100000, 0)
        for r in (_event_out(x, lang) for x in rows):
            w.writerow([r["id"], r["time"], nm(LEVEL_NAME, r["level"], i), nm(CAT_NAME, r["category"], i),
                        r["message"], nm(RUN_NAME, r["run_state"], i),
                        "" if r["floor"] is None else r["floor"], r["source"] or "", nm(modes, r["mode"], i)])
    fname = f"EV-S2_event_log_{datetime.now():%Y%m%d_%H%M}.csv"
    return Response(("﻿" + buf.getvalue()).encode("utf-8"), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f"attachment; filename=\"{fname}\""})


@app.get("/api/stats")
async def api_stats():
    return {"today": today, "db_path": str(DB_PATH) if db else None,
            "db_bytes": db.size_bytes() if db else 0, "retention_days": LOG_RETENTION_DAYS}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    ws_clients.add(ws)
    try:
        state["log"] = list(log)
        await ws.send_text(json.dumps(state, ensure_ascii=False))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        ws_clients.discard(ws)


PAGE = r"""<!DOCTYPE html>
<html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light dark">
<script>/* 테마: 저장된 선택 > 운영체제 설정 (깜빡임 방지로 먼저 적용). 기본은 주간(라이트) */
(function(){var t;try{t=localStorage.getItem('evscada.theme')}catch(e){}
 if(t!=='light'&&t!=='dark') t=matchMedia('(prefers-color-scheme: dark)').matches?'dark':'light';
 document.documentElement.dataset.theme=t})();</script>
<title>EV-S2 SCADA</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@500&family=IBM+Plex+Sans+KR:wght@400;500;600;700&display=swap">
<style>
/* =====================================================================
   디자인 원칙 (ISA-101 고성능 HMI)
   - 바탕은 전기 분전반 도장색(RAL 7035 계열)의 무채색. 정상 상태는 색을 쓰지 않음
   - 색은 의미가 있을 때만: 운행/점등 = 신호 청색, 주의 = 황토, 경보 = 적색
   - 단 하나의 강조: 승강기 홀 층 표시기(오렌지 LED) + 승강로
   - 고정폭 서체는 PLC 주소/시각에만
   ===================================================================== */
:root{
 --bg:#c9cec9;        /* RAL 7035 분전반 */
 --panel:#dde1dd;     /* 패널 면 */
 --raise:#e9ece9;     /* 버튼/입력 */
 --inset:#b9bfba;     /* 움푹한 영역 (승강로) */
 --line:#a7aea8;--line2:#8f9690;
 --ink:#1b201d;--ink2:#46504a;--mut:#626b65;--dim:#8d958f;
 --act:#1d5f93;--actbg:rgba(29,95,147,.12);--actln:rgba(29,95,147,.45);
 --warn:#9a6a00;--warnbg:rgba(201,138,0,.16);
 --alarm:#c0151c;--alarmbg:rgba(192,21,28,.10);
 --led:#ff9a1f;--ledoff:#3a2d1b;--lantern:#141715;
 --steel1:#d3d7d6;--steel2:#b9bebd;
 --shadow:rgba(20,28,24,.22);
 --fh:76px;--head:30px;   /* 승강로 최상층 위 오버헤드 (카 표시 라벨 공간) */
 --sans:"IBM Plex Sans KR","Malgun Gothic","Apple SD Gothic Neo",system-ui,sans-serif;
 --mono:"IBM Plex Mono",Consolas,"Malgun Gothic",monospace;
 color-scheme:light;
}
:root[data-theme="dark"]{   /* 야간 제어실: 검정이 아닌 어두운 회색 */
 --bg:#262b29;--panel:#303633;--raise:#3a413d;--inset:#1f2321;
 --line:#47504b;--line2:#5c6660;
 --ink:#e3e7e4;--ink2:#b3bbb6;--mut:#8f9893;--dim:#66706a;
 --act:#6aa6d8;--actbg:rgba(106,166,216,.14);--actln:rgba(106,166,216,.5);
 --warn:#e0ad3a;--warnbg:rgba(224,173,58,.14);
 --alarm:#f0545a;--alarmbg:rgba(240,84,90,.14);
 --lantern:#0e100f;
 --steel1:#9aa1a0;--steel2:#7e8584;
 --shadow:rgba(0,0,0,.5);
 color-scheme:dark;
}
*{box-sizing:border-box}
html,body{margin:0}
body{min-height:100vh;background:var(--bg);color:var(--ink);font:14px/1.5 var(--sans);-webkit-font-smoothing:antialiased}
button,input,select{font-family:inherit}
.num{font-variant-numeric:tabular-nums}
:focus-visible{outline:2px solid var(--act);outline-offset:2px}
::-webkit-scrollbar{width:10px;height:10px}::-webkit-scrollbar-thumb{background:var(--line);border-radius:5px;border:2px solid var(--panel)}::-webkit-scrollbar-track{background:transparent}

/* ---------- 상단 ---------- */
header{position:sticky;top:0;z-index:20;background:var(--panel);border-bottom:1px solid var(--line)}
.hbar{max-width:1600px;margin:0 auto;padding:0 24px;height:56px;display:flex;align-items:center;gap:28px}
.brand{display:flex;align-items:center;gap:11px;min-width:0}
.mark{width:30px;height:30px;border-radius:4px;display:grid;place-items:center;flex:none;background:var(--lantern)}
.mark svg{width:16px;height:16px}
.brand b{display:block;font-size:15px;font-weight:700;white-space:nowrap}
.brand small{display:block;font-size:12px;color:var(--mut);white-space:nowrap;margin-top:-2px}
.tabs{display:flex;height:100%}
.tabs button{position:relative;border:0;background:none;color:var(--mut);font-size:14px;font-weight:600;padding:0 16px;cursor:pointer}
.tabs button:hover{color:var(--ink)}
.tabs button.on{color:var(--ink)}
.tabs button.on::after{content:"";position:absolute;left:16px;right:16px;bottom:-1px;height:3px;background:var(--ink)}
.sp{flex:1}
.pills{display:flex;gap:14px;align-items:center}
.pill{display:inline-flex;align-items:center;gap:7px;font-size:13px;font-weight:500;color:var(--ink2);white-space:nowrap}
.pill i{width:8px;height:8px;border-radius:50%;border:1.5px solid var(--mut);flex:none}
.pill.ok i{background:var(--ink2);border-color:var(--ink2)}
.pill.ac i{background:var(--act);border-color:var(--act)}
.pill.warn{color:var(--warn)}.pill.warn i{background:var(--warn);border-color:var(--warn)}
.pill.err{color:var(--alarm);font-weight:700}.pill.err i{background:var(--alarm);border-color:var(--alarm)}
.seg-ctl{display:flex;border:1px solid var(--line);border-radius:4px;overflow:hidden}
.seg-ctl button{border:0;background:var(--raise);color:var(--mut);font-size:12px;font-weight:600;padding:4px 10px;cursor:pointer}
.seg-ctl button+button{border-left:1px solid var(--line)}
.seg-ctl button.on{background:var(--ink);color:var(--panel)}
.icon-btn{width:30px;height:30px;border-radius:4px;border:1px solid var(--line);background:var(--raise);color:var(--ink2);cursor:pointer;display:grid;place-items:center;padding:0}
.icon-btn:hover{color:var(--ink);border-color:var(--line2)}
.icon-btn svg{width:16px;height:16px}
:root[data-theme="dark"] .i-moon,:root:not([data-theme="dark"]) .i-sun{display:none}
.clock{text-align:right;line-height:1.25;min-width:100px}
.clock b{display:block;font:500 15px var(--mono)}
.clock span{font-size:12px;color:var(--mut)}
@media(max-width:1100px){.hbar{height:auto;flex-wrap:wrap;padding:10px 16px;gap:10px 16px}.tabs{order:9;width:100%;height:40px;border-top:1px solid var(--line)}.clock{display:none}}

/* ---------- 경보 배너: 이상일 때만 강하게 ---------- */
.banner{display:none;align-items:center;gap:14px;padding:12px 24px;background:var(--alarm);color:#fff}
.banner.show{display:flex}
.banner .ico{flex:none;display:grid;place-items:center}
.banner b{font-size:15px;font-weight:700;margin-right:12px}.banner span{font-size:13.5px;opacity:.92}
.banner .live{margin-left:auto;width:10px;height:10px;border-radius:50%;background:#fff;animation:blink 1.2s infinite}
@keyframes blink{50%{opacity:.2}}

/* ---------- 상태 띠 (KPI) ---------- */
.wrap{max-width:1600px;margin:0 auto;padding:20px 24px 36px}
@media(max-width:640px){.wrap{padding:12px}}
.kpis{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));background:var(--panel);border:1px solid var(--line);border-radius:4px;margin-bottom:16px}
.kpi{padding:12px 18px 13px;min-width:0;border-left:1px solid var(--line)}
.kpi:first-child{border-left:0}
@media(max-width:1250px){.kpis{grid-template-columns:repeat(3,minmax(0,1fr))}.kpi:nth-child(4){border-left:0}.kpi:nth-child(n+4){border-top:1px solid var(--line)}}
@media(max-width:640px){.kpis{grid-template-columns:repeat(2,minmax(0,1fr))}.kpi{border-left:0!important;border-top:1px solid var(--line)}.kpi:nth-child(-n+2){border-top:0}.kpi:nth-child(even){border-left:1px solid var(--line)!important}}
.kpi label{display:flex;align-items:center;gap:7px;font-size:12.5px;color:var(--mut)}
.kpi label i{width:7px;height:7px;border-radius:50%;background:transparent;border:1.5px solid var(--dim);flex:none}
.kpi .v{display:block;font-size:20px;font-weight:600;margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.kpi .s{display:block;font-size:12.5px;color:var(--ink2);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.t-ok{color:var(--ink)!important}.t-warn{color:var(--warn)!important}.t-err{color:var(--alarm)!important}.t-ac{color:var(--act)!important}.t-mut{color:var(--mut)!important}
.d-ok{background:var(--ink2)!important;border-color:var(--ink2)!important}.d-warn{background:var(--warn)!important;border-color:var(--warn)!important}
.d-err{background:var(--alarm)!important;border-color:var(--alarm)!important}.d-ac{background:var(--act)!important;border-color:var(--act)!important}

/* ---------- 레이아웃 ---------- */
.view{display:none}.view.on{display:block}
.ops{display:grid;grid-template-columns:360px minmax(0,1fr);gap:16px;align-items:start}
@media(max-width:1050px){.ops{grid-template-columns:1fr}}
.main{display:grid;gap:16px;min-width:0}
.r3{display:grid;grid-template-columns:minmax(0,1.1fr) auto minmax(0,1fr);gap:16px;align-items:stretch}
.r2{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:16px;align-items:stretch}
@media(max-width:1350px){.r3{grid-template-columns:minmax(0,1fr) auto}.r3>.card:last-child{grid-column:1/-1}}
@media(max-width:760px){.r3,.r2{grid-template-columns:1fr}}
.card{background:var(--panel);border:1px solid var(--line);border-radius:4px;min-width:0;display:flex;flex-direction:column}
.ch{display:flex;align-items:center;gap:12px;padding:10px 16px;border-bottom:1px solid var(--line);min-height:42px}
.ch h3{margin:0;font-size:14px;font-weight:600}
.ch .aside{margin-left:auto;font-size:12.5px;color:var(--mut)}
.cb-body{padding:16px;flex:1}
.note{font-size:12.5px;color:var(--mut);margin-top:12px;line-height:1.6}
.divider{height:1px;background:var(--line);margin:14px 0}

/* ---------- 홀 층 표시기 (이 화면의 단 하나의 강조) ---------- */
.hoist .cb-body{padding:14px}
.indicator{display:grid;grid-template-columns:auto 1fr auto;align-items:center;gap:16px;padding:14px 18px;margin-bottom:14px;border-radius:3px;background:var(--lantern);box-shadow:inset 0 2px 6px rgba(0,0,0,.6);color:#9aa39d}
.arrows{display:flex;flex-direction:column;gap:6px}
.arrow{width:26px;height:26px;color:var(--ledoff);transition:color .2s}
.arrow.on{color:var(--led);filter:drop-shadow(0 0 6px rgba(255,154,31,.55))}
.arrow.blink{animation:blink 1s infinite}
.seg{font:600 64px/1 var(--mono);color:var(--led);text-shadow:0 0 14px rgba(255,154,31,.45);text-align:center;font-variant-numeric:tabular-nums;letter-spacing:-.02em}
.indicator .lbl{font-size:12px;text-align:right;color:#7f8882}
.indicator .lbl b{display:block;font-size:14px;font-weight:600;color:#dfe3e0;margin-top:2px}
.indicator .lbl b.t-err{color:#ff6b6f!important}.indicator .lbl b.t-warn{color:#f0c050!important}
.indicator .lbl b.t-ac,.indicator .lbl b.t-ok{color:#dfe3e0!important}

/* ---------- 승강로 ---------- */
.shaft-wrap{display:flex;gap:10px}
.labels{position:relative;width:84px;height:calc(var(--fh)*6 + var(--head));flex:none}
.floor-row{position:absolute;left:0;right:0;height:var(--fh);display:flex;align-items:center;gap:8px}
.fl{font:600 17px/1 var(--sans);width:30px;color:var(--dim);font-variant-numeric:tabular-nums;transition:color .2s}
.fl.cur{color:var(--ink)}
.hall{display:flex;flex-direction:column;gap:5px}
.hb{width:30px;height:26px;border-radius:13px;border:1px solid var(--line2);background:linear-gradient(var(--steel1),var(--steel2));color:#4a524d;font-size:9px;cursor:pointer;padding:0;display:grid;place-items:center}
.hb:not(:disabled):hover{border-color:var(--act)}
.hb.on{color:#fff;background:var(--act);border-color:var(--act);box-shadow:0 0 0 3px var(--actbg)}
.hb:disabled{cursor:default}.hb.ghost{visibility:hidden}
.shaft{position:relative;flex:1;height:calc(var(--fh)*6 + var(--head));border-radius:3px;overflow:hidden;background:var(--inset);box-shadow:inset 0 2px 8px var(--shadow)}
.lvl{position:absolute;left:0;right:0;height:var(--fh);border-bottom:1px dashed var(--line2)}
.overhead{position:absolute;left:0;right:0;top:0;height:var(--head);border-bottom:1px solid var(--line2);background:repeating-linear-gradient(135deg,transparent 0 6px,rgba(0,0,0,.05) 6px 8px)}
.lvl:last-child{border-bottom:0}
.lvl span{position:absolute;left:8px;top:5px;font:500 10.5px var(--mono);color:var(--mut)}
.sens{position:absolute;right:8px;width:8px;height:8px;border-radius:1px;border:1.5px solid var(--dim);transition:.15s}
.sens.on{background:var(--act);border-color:var(--act)}
.rail{position:absolute;top:0;bottom:0;width:3px;background:var(--line2)}
.cable{position:absolute;left:50%;top:0;width:2px;margin-left:-1px;background:repeating-linear-gradient(var(--dim) 0 3px,transparent 3px 5px)}
.car{position:absolute;left:22%;right:22%;height:calc(var(--fh) - 12px);bottom:6px}
.car-box{position:absolute;inset:0;border-radius:2px;overflow:hidden;background:#f3f4f2;border:2px solid #6f7772}
.person{position:absolute;bottom:3px;left:50%;margin-left:-8px;width:16px;height:30px;opacity:.15}
.door{position:absolute;top:0;bottom:0;width:50%;background:repeating-linear-gradient(90deg,#aab0af 0 2px,#bcc2c1 2px 5px)}
.door.l{left:0;box-shadow:inset -1px 0 0 #6f7772}.door.r{right:0;box-shadow:inset 1px 0 0 #6f7772}
.car-led{position:absolute;top:-21px;left:50%;transform:translateX(-50%);white-space:nowrap;font-size:11px;font-weight:600;color:var(--ink2);background:var(--panel);border:1px solid var(--line2);padding:0 7px;border-radius:2px}
.car.run .car-box{border-color:var(--act)}
.car.run .car-led{color:#fff;background:var(--act);border-color:var(--act)}
.car.err .car-box{border-color:var(--alarm)}
.car.err .car-led{color:#fff;background:var(--alarm);border-color:var(--alarm)}
.legend{display:flex;gap:16px;flex-wrap:wrap;margin-top:12px;font-size:12px;color:var(--mut)}
.legend span{display:inline-flex;align-items:center;gap:6px}
.legend i{width:9px;height:9px;display:inline-block}

/* ---------- 값 / 램프 ---------- */
.kv{display:grid;grid-template-columns:auto 1fr;gap:9px 14px;align-items:baseline;margin:0}
.kv dt{font-size:13px;color:var(--ink2)}
.kv dt code{font:500 11px var(--mono);color:var(--mut);margin-left:6px}
.kv dd{margin:0;text-align:right;font-weight:600;font-variant-numeric:tabular-nums}
.kv dd.big{font-size:18px}
.lamps{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:4px}
.lp{display:flex;align-items:center;gap:9px;font-size:13px;color:var(--mut);padding:6px 9px;border-radius:2px;background:transparent;border:1px solid transparent}
.lp i{width:9px;height:9px;border-radius:1px;border:1.5px solid var(--dim);flex:none}
.lp.on{color:var(--ink);font-weight:600}.lp.on i{background:var(--ink2);border-color:var(--ink2)}
.lp.on.y i{background:var(--act);border-color:var(--act)}
.lp.on.r{background:var(--alarmbg);border-color:var(--alarm);color:var(--alarm)}.lp.on.r i{background:var(--alarm);border-color:var(--alarm)}
.lp.na{opacity:.4}
.lp small{margin-left:auto;font:500 10.5px var(--mono);color:var(--dim);font-weight:500}
.sub{font-size:12.5px;color:var(--ink2);font-weight:600;margin:16px 0 8px}
.meter{height:8px;border-radius:1px;background:var(--inset);overflow:hidden;position:relative}
.meter>div{height:100%;width:0;background:var(--act);transition:width .4s}
.hz{display:flex;align-items:baseline;gap:6px;margin:0 0 10px}
.hz b{font:600 30px var(--sans);font-variant-numeric:tabular-nums}
.hz span{color:var(--mut);font-size:13px}
.err-text{color:var(--alarm);font-size:12.5px;margin-top:10px}
.err-text:empty,.note:empty{display:none}

/* ---------- 카 조작반: 스테인리스 패널 ---------- */
.cop-card .cb-body{display:flex;justify-content:center;align-items:center;padding:18px 22px}
.cop{padding:20px 24px;border-radius:3px;background:repeating-linear-gradient(90deg,#c9cecd 0 1px,#d6dad9 1px 3px,#cdd2d1 3px 4px);border:1px solid #9aa1a0;box-shadow:inset 0 1px 0 rgba(255,255,255,.6),0 1px 2px var(--shadow)}
.cop-grid,.cop-door{display:grid;grid-template-columns:repeat(2,52px);gap:14px}
.cop-door{margin-top:16px;padding-top:16px;border-top:1px solid #a3aaa9}
.cb{width:52px;height:52px;border-radius:50%;cursor:pointer;color:#3b423e;font:600 18px var(--sans);border:1px solid #8b9291;background:radial-gradient(circle at 50% 40%,#eef0ef,#c3c8c7);box-shadow:inset 0 0 0 4px #d7dbda,0 1px 2px rgba(0,0,0,.25);display:grid;place-items:center;transition:box-shadow .15s,color .15s}
.cb:hover{color:#111}
.cb:active{transform:translateY(1px)}
.cb.on{color:var(--act);box-shadow:inset 0 0 0 3px #1d5f93,0 0 0 3px rgba(29,95,147,.28),0 0 10px rgba(29,95,147,.35)}
.cb svg{width:20px;height:20px}
.cop-card .note{margin:0 16px 16px}

/* ---------- 버튼 ---------- */
.btns{display:flex;flex-wrap:wrap;gap:8px}
.btn{display:inline-flex;align-items:center;gap:8px;height:36px;padding:0 14px;border-radius:3px;cursor:pointer;user-select:none;touch-action:none;border:1px solid var(--line2);background:var(--raise);color:var(--ink);font-size:13.5px;font-weight:600;white-space:nowrap}
.btn:hover:not(:disabled){border-color:var(--ink2)}
.btn.on{background:var(--act);border-color:var(--act);color:#fff}
.btn.danger{color:var(--alarm)}
.btn.danger.on,.btn.danger:active:not(:disabled){background:var(--alarm);border-color:var(--alarm);color:#fff}
.btn.primary{background:var(--ink);border-color:var(--ink);color:var(--panel)}
.btn:disabled{opacity:.4;cursor:not-allowed}
.btn svg{width:15px;height:15px;flex:none}
.btn.sm{height:30px;padding:0 11px;font-size:12.5px}
.mbtns{display:grid;grid-template-columns:repeat(2,1fr);gap:8px}
.mbtns .btn{justify-content:center}
.simbox{margin-top:16px;padding:12px 14px;border-radius:3px;border:1px dashed var(--warn);display:none}
.simbox .sub{margin-top:0;color:var(--warn)}

/* ---------- 이벤트 ---------- */
.feed{max-height:340px;overflow:auto}
.ev{display:grid;grid-template-columns:64px 58px 1fr;gap:10px;align-items:start;padding:7px 16px;border-bottom:1px solid var(--line);font-size:13px}
.ev:last-child{border-bottom:0}
.ev time{font:500 11.5px var(--mono);color:var(--mut);padding-top:2px}
.ev p{margin:0;color:var(--ink2);word-break:keep-all}
.ev.alarm{background:var(--alarmbg)}.ev.alarm p{color:var(--alarm);font-weight:600}.ev.warn p{color:var(--warn)}
.ev em{font-style:normal;color:var(--mut);font-size:11px;margin-left:6px;font-family:var(--mono)}
.tag{display:inline-flex;align-items:center;height:20px;padding:0 6px;border-radius:2px;font-size:11.5px;font-weight:600;white-space:nowrap;color:var(--ink2);border:1px solid var(--line)}
.tag.warn{color:var(--warn);border-color:var(--warn);background:var(--warnbg)}
.tag.alarm,.tag.c-alarm{color:#fff;background:var(--alarm);border-color:var(--alarm)}
.empty{padding:28px 0;text-align:center;color:var(--mut);font-size:13px}

/* ---------- 표 / 필터 ---------- */
.filters{display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;margin-bottom:14px}
.field{display:flex;flex-direction:column;gap:5px}
.field label{font-size:12.5px;color:var(--ink2)}
.inp{height:36px;padding:0 10px;border-radius:3px;border:1px solid var(--line2);background:var(--raise);color:var(--ink);font-size:13.5px;outline:none}
.inp:focus{border-color:var(--act);box-shadow:0 0 0 2px var(--actbg)}
.inp.q{width:240px}
.chips{display:flex;gap:18px;flex-wrap:wrap;margin-bottom:12px;font-size:13px;color:var(--ink2)}
.chip{display:inline-flex;align-items:center;gap:7px}
.chip b{color:var(--ink);font-variant-numeric:tabular-nums}
.tblw{overflow:auto;border:1px solid var(--line);border-radius:3px;max-height:620px;background:var(--raise)}
table{border-collapse:separate;border-spacing:0;width:100%;font-size:13px}
th,td{padding:9px 14px;text-align:left;white-space:nowrap;border-bottom:1px solid var(--line)}
th{position:sticky;top:0;z-index:1;background:var(--panel);color:var(--ink2);font-size:12.5px;font-weight:600}
tbody tr:hover td{background:var(--actbg)}
tbody tr:last-child td{border-bottom:0}
td.msg{white-space:normal;min-width:280px;color:var(--ink)}
td.m{font:500 12px var(--mono);color:var(--ink2)}
td.v{font:600 13px var(--mono)}
.v1{color:var(--act)}.vna{color:var(--dim)}
.pager{display:flex;align-items:center;gap:8px;justify-content:flex-end;margin-top:12px;font-size:13px;color:var(--ink2)}
.wtag{font-size:11.5px;font-weight:600;color:var(--act);padding:1px 6px;border-radius:2px;border:1px solid var(--actln)}

footer{max-width:1600px;margin:0 auto;padding:0 24px 24px;display:flex;gap:22px;flex-wrap:wrap;font-size:12.5px;color:var(--mut)}
footer b{color:var(--ink2);font-weight:500}
#toast{position:fixed;right:24px;bottom:24px;z-index:50;max-width:440px;padding:12px 16px;border-radius:3px;font-size:13.5px;background:var(--ink);color:var(--panel);border-left:4px solid var(--alarm);box-shadow:0 12px 30px -8px var(--shadow);transform:translateY(16px);opacity:0;pointer-events:none;transition:.2s}
#toast.show{transform:none;opacity:1}
@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
</style></head><body>
<header><div class="hbar">
 <div class="brand">
  <div class="mark" aria-hidden="true"><svg viewBox="0 0 24 24" fill="#ff9a1f"><path d="M12 3l6 8H6zM12 21l-6-8h12z"/></svg></div>
  <div><b>EV-S2 SCADA</b><small data-i18n="subtitle"></small></div>
 </div>
 <nav class="tabs" id="tabs">
  <button data-v="ops" class="on" data-i18n="tab_ops"></button>
  <button data-v="hist" data-i18n="tab_hist"></button>
  <button data-v="vars" data-i18n="tab_vars"></button>
 </nav>
 <span class="sp"></span>
 <div class="pills">
  <span class="pill" id="pMode"><i></i><span>-</span></span>
  <span class="pill" id="pConn"><i></i><span>-</span></span>
  <span class="pill" id="pRtt" style="display:none"><i></i><span>-</span></span>
  <div class="seg-ctl" id="langSw" role="group" aria-label="Language"><button data-l="ko">KO</button><button data-l="en">EN</button></div>
  <button class="icon-btn" id="themeBtn">
   <svg class="i-sun" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/></svg>
   <svg class="i-moon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/></svg>
  </button>
 </div>
 <div class="clock"><b id="clkT">--:--:--</b><span id="clkD">-</span></div>
</div></header>

<div class="banner" id="banner" role="alert"><div class="ico"><svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round"><path d="M12 3l10 18H2z"/><path d="M12 10v5M12 18v.01"/></svg></div>
 <div><b id="bnT">-</b><span id="bnS"></span></div><span class="live"></span></div>

<div class="wrap">
 <section class="kpis" aria-live="polite">
  <div class="kpi"><label><i id="kRunD"></i><span data-i18n="k_run"></span></label><span class="v" id="kRun">-</span><span class="s" id="kRunS">-</span></div>
  <div class="kpi"><label><i></i><span data-i18n="k_pos"></span></label><span class="v num" id="kPos">-</span><span class="s" id="kPosS">-</span></div>
  <div class="kpi"><label><i id="kDirD"></i><span data-i18n="k_dir"></span></label><span class="v" id="kDir">-</span><span class="s" id="kDirS">-</span></div>
  <div class="kpi"><label><i id="kDoorD"></i><span data-i18n="k_door"></span></label><span class="v" id="kDoor">-</span><span class="s" id="kDoorS">-</span></div>
  <div class="kpi"><label><i id="kHzD"></i><span data-i18n="k_hz"></span></label><span class="v num" id="kHz">-</span><span class="s" id="kHzS">-</span></div>
  <div class="kpi"><label><i id="kDayD"></i><span data-i18n="k_day"></span></label><span class="v num" id="kDay">-</span><span class="s" id="kDayS">-</span></div>
 </section>

 <!-- ================= 운전 현황 ================= -->
 <section class="view on" id="v-ops">
  <div class="ops">
   <div class="card hoist">
    <div class="ch"><h3 data-i18n="c_shaft"></h3><span class="aside" id="shaftAside"></span></div>
    <div class="cb-body">
     <div class="indicator">
      <div class="arrows">
       <svg class="arrow" id="aUp" viewBox="0 0 24 24" fill="currentColor"><path d="M12 4l9 12H3z"/></svg>
       <svg class="arrow" id="aDn" viewBox="0 0 24 24" fill="currentColor"><path d="M12 20L3 8h18z"/></svg>
      </div>
      <span id="seg" class="seg">-</span>
      <div class="lbl"><span data-i18n="ind_floor"></span><b id="indSt">-</b></div>
     </div>
     <div class="shaft-wrap">
      <div class="labels" id="labels"></div>
      <div class="shaft" id="shaft">
       <div id="lvls"></div>
       <div class="rail" style="left:14%"></div><div class="rail" style="right:14%"></div>
       <div class="cable" id="cable"></div>
       <div class="car" id="car"><div class="car-led" id="carLed">-</div>
        <div class="car-box">
         <svg class="person" viewBox="0 0 18 34"><circle cx="9" cy="5" r="4.5" fill="#111"/><rect x="3" y="11" width="12" height="23" rx="5" fill="#111"/></svg>
         <div class="door l" id="dl"></div><div class="door r" id="dr"></div>
        </div></div>
      </div>
     </div>
     <div class="legend">
      <span><i style="background:var(--act);border-radius:50%"></i><span data-i18n="lg_call"></span></span>
      <span><i style="background:var(--act);border-radius:1px"></i><span data-i18n="lg_sens"></span></span>
     </div>
    </div>
   </div>

   <div class="main">
    <div class="r3">
     <div class="card">
      <div class="ch"><h3 data-i18n="c_op"></h3></div>
      <div class="cb-body"><dl class="kv">
       <dt><span data-i18n="r_run"></span><code>D10</code></dt><dd class="big" id="vRun">-</dd>
       <dt><span data-i18n="r_dir"></span><code>D4</code></dt><dd id="vDir">-</dd>
       <dt><span data-i18n="r_cur"></span><code>D0</code></dt><dd id="vCur">-</dd>
       <dt><span data-i18n="r_jdg"></span><code>D5</code></dt><dd id="vJdg">-</dd>
       <dt><span data-i18n="r_door"></span><code>D9</code></dt><dd id="vDoor">-</dd>
       <dt><span data-i18n="r_err"></span><code>D12</code></dt><dd id="vErr">-</dd>
       <dt><span data-i18n="r_dpos"></span></dt><dd id="vDoorPos">-</dd>
      </dl></div>
     </div>
     <div class="card cop-card">
      <div class="ch"><h3 data-i18n="c_cop"></h3></div>
      <div class="cb-body"><div class="cop">
       <div class="cop-grid" id="cop"></div>
       <div class="cop-door">
        <button class="cb" id="bOpen" data-tag="화면_열림" data-i18n-title="t_open"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M4 12l6-5v10zM20 12l-6-5v10z"/></svg></button>
        <button class="cb" id="bClose" data-tag="화면_닫힘" data-i18n-title="t_close"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M10 12L4 7v10zM14 12l6-5v10z"/></svg></button>
       </div>
      </div></div>
     </div>
     <div class="card">
      <div class="ch"><h3 data-i18n="c_vfd"></h3><span class="aside" id="vfdAside"></span></div>
      <div class="cb-body">
       <div class="hz"><b id="vHz">-</b><span>Hz</span><span id="vHzS" style="margin-left:auto"></span></div>
       <div class="meter"><div id="hzBar"></div></div>
       <div class="lamps" id="vfdLamps" style="margin-top:12px;grid-template-columns:repeat(2,minmax(0,1fr))"></div>
       <div class="divider"></div>
       <dl class="kv">
        <dt><span data-i18n="p_hi"></span><code>D1000</code></dt><dd id="pHi">-</dd>
        <dt><span data-i18n="p_lo"></span><code>D1001</code></dt><dd id="pLo">-</dd>
        <dt><span data-i18n="p_in"></span><code>D1006</code></dt><dd id="pIn">-</dd>
       </dl>
       <div class="err-text" id="vfdErr"></div>
      </div>
     </div>
    </div>

    <div class="r2">
     <div class="card">
      <div class="ch"><h3 data-i18n="c_maint"></h3></div>
      <div class="cb-body">
       <div class="btns">
        <button class="btn" id="bInsp" data-tag="화면_점검"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M14.7 6.3a4 4 0 0 0-5.4 5.4L3 18l3 3 6.3-6.3a4 4 0 0 0 5.4-5.4l-2.6 2.6-2.4-.6-.6-2.4z"/></svg><span data-i18n="b_insp"></span></button>
        <button class="btn danger" id="bReset" data-tag="화면_에러리셋"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M3 12a9 9 0 1 0 3-6.7L3 8"/><path d="M3 3v5h5"/></svg><span data-i18n="b_reset"></span></button>
       </div>
       <div class="sub" data-i18n="s_manual"></div>
       <div class="mbtns">
        <button class="btn hold" data-tag="화면_수동상승">▲ <span data-i18n="b_up"></span></button>
        <button class="btn hold" data-tag="화면_수동하강">▼ <span data-i18n="b_dn"></span></button>
        <button class="btn hold" data-tag="화면_수동열림">◀▶ <span data-i18n="b_open"></span></button>
        <button class="btn hold" data-tag="화면_수동닫힘">▶◀ <span data-i18n="b_close"></span></button>
       </div>
       <div class="simbox" id="simBox">
        <div class="sub" data-i18n="s_sim"></div>
        <div class="btns">
         <button class="btn danger sm" data-tag="스위치_비상정지" data-toggle data-i18n="b_estop"></button>
         <button class="btn sm" data-tag="센서_문" data-toggle data-i18n="b_obst"></button>
        </div>
       </div>
      </div>
     </div>
     <div class="card">
      <div class="ch"><h3 data-i18n="c_logic"></h3><span class="aside" data-i18n="a_logic"></span></div>
      <div class="cb-body"><div class="lamps" id="flagLamps"></div></div>
     </div>
    </div>

    <div class="r2">
     <div class="card">
      <div class="ch"><h3 data-i18n="c_io"></h3></div>
      <div class="cb-body"><div class="lamps" id="ioLamps"></div><div class="note t-err" id="ioNote"></div></div>
     </div>
     <div class="card">
      <div class="ch"><h3 data-i18n="c_live"></h3><button class="btn sm" style="margin-left:auto" onclick="go('hist')" data-i18n="b_all"></button></div>
      <div class="feed" id="feed"></div>
     </div>
    </div>
   </div>
  </div>
 </section>

 <!-- ================= 이벤트 이력 ================= -->
 <section class="view" id="v-hist">
  <div class="card">
   <div class="ch"><h3 data-i18n="c_hist"></h3><span class="aside" id="dbInfo"></span></div>
   <div class="cb-body">
   <form class="filters" id="hForm" onsubmit="event.preventDefault();H.page=0;loadHist()">
    <div class="field"><label for="hS" data-i18n="f_start"></label><input class="inp" type="date" id="hS"></div>
    <div class="field"><label for="hE" data-i18n="f_end"></label><input class="inp" type="date" id="hE"></div>
    <div class="field"><label for="hLv" data-i18n="f_lv"></label><select class="inp" id="hLv"></select></div>
    <div class="field"><label for="hCat" data-i18n="f_cat"></label><select class="inp" id="hCat"></select></div>
    <div class="field"><label for="hQ" data-i18n="f_q"></label><input class="inp q" type="search" id="hQ" data-i18n-ph="ph_q"></div>
    <div class="field"><label>&nbsp;</label><div class="btns">
     <button class="btn primary" type="submit"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><circle cx="11" cy="11" r="7"/><path d="M20 20l-3.5-3.5"/></svg><span data-i18n="b_search"></span></button>
     <button class="btn" type="button" onclick="quick(0)" data-i18n="b_today"></button>
     <button class="btn" type="button" onclick="quick(6)" data-i18n="b_7d"></button>
     <button class="btn" type="button" onclick="quick(29)" data-i18n="b_30d"></button>
     <button class="btn" type="button" id="hCsv"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M12 3v12M7 10l5 5 5-5M4 21h16"/></svg><span data-i18n="b_csv"></span></button>
    </div></div>
   </form>
   <div class="chips" id="hSum"></div>
   <div class="tblw"><table>
    <thead><tr><th data-i18n="h_no"></th><th data-i18n="h_time"></th><th data-i18n="h_lv"></th><th data-i18n="h_cat"></th><th data-i18n="h_msg"></th><th data-i18n="h_run"></th><th data-i18n="h_pos"></th><th data-i18n="h_src"></th></tr></thead>
    <tbody id="hBody"></tbody></table></div>
   <div class="pager"><span id="hInfo">-</span>
    <select class="inp" id="hSize" style="height:30px" aria-label="Page size"><option>50</option><option selected>100</option><option>200</option><option>500</option></select>
    <button class="btn sm" id="hPrev" data-i18n="b_prev"></button><button class="btn sm" id="hNext" data-i18n="b_next"></button></div>
   </div>
  </div>
 </section>

 <!-- ================= 변수 모니터 ================= -->
 <section class="view" id="v-vars">
  <div class="card">
   <div class="ch"><h3 data-i18n="c_vars"></h3><span class="aside" id="varCount"></span></div>
   <div class="cb-body">
   <div class="filters">
    <div class="field"><label for="flt" data-i18n="f_q"></label><input class="inp q" type="search" id="flt" data-i18n-ph="ph_var"></div>
    <div class="field"><label for="fDev" data-i18n="f_dev"></label><select class="inp" id="fDev"></select></div>
    <div class="field"><label for="fVal" data-i18n="f_val"></label><select class="inp" id="fVal"></select></div>
   </div>
   <div class="tblw"><table><thead><tr><th data-i18n="v_name"></th><th data-i18n="v_addr"></th><th data-i18n="v_type"></th><th data-i18n="v_val"></th><th data-i18n="v_desc"></th><th></th></tr></thead><tbody id="vt"></tbody></table></div>
   </div>
  </div>
 </section>
</div>

<footer>
 <span><span data-i18n="ft_comm"></span> <b id="fPlc">-</b> (Modbus TCP)</span>
 <span><span data-i18n="ft_upd"></span> <b id="fUpd">-</b></span>
 <span><span data-i18n="ft_db"></span> <b id="fDb">-</b></span>
 <span class="sp"></span>
 <span>LS XGB, XBL-EMTA, EV_s2</span>
</footer>
<div id="toast" role="status"></div>
<script>
const $=id=>document.getElementById(id);
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

/* ================= 다국어 ================= */
const T={
ko:{subtitle:'승강기 통합 감시제어',tab_ops:'운전 현황',tab_hist:'이벤트 이력',tab_vars:'변수 모니터',
 sim:'시뮬레이션',plc:'PLC 실기',online:'통신 정상',offline:'통신 두절',rtt:'응답 {n} ms',
 k_run:'운행 상태',k_pos:'현재 위치',k_dir:'운행 방향',k_door:'도어',k_hz:'VFD 지령',k_day:'금일 운행',
 c_shaft:'승강로',ind_floor:'현재 층',lg_call:'호출 등록',lg_sens:'층 위치 센서 감지',
 c_op:'운행 상세',r_run:'운행 상태',r_dir:'운행 방향',r_cur:'현재 위치',r_jdg:'판단 기준층',r_door:'도착 단계',r_err:'에러 코드',r_dpos:'도어 개방률',
 c_cop:'카 조작반',t_open:'도어 열림 (누르고 있는 동안)',t_close:'도어 닫힘 (누르고 있는 동안)',
 n_cop:'층 버튼을 누르면 행선 호출이 등록되고, 한 번 더 누르면 취소됩니다. 도어 버튼은 누르고 있는 동안만 동작합니다.',
 c_vfd:'VFD',p_hi:'고속 운전',p_lo:'저속 운전',p_in:'점검 운전',
 c_maint:'점검과 복구',b_insp:'점검 모드',b_reset:'에러 리셋',s_manual:'수동 운전 (점검 모드 전용)',
 b_up:'수동 상승',b_dn:'수동 하강',b_open:'도어 열림',b_close:'도어 닫힘',
 n_manual:'수동 운전은 점검 모드에서 버튼을 누르고 있는 동안에만 동작하며, 손을 떼거나 통신이 끊기면 즉시 정지합니다.',
 s_sim:'시뮬레이션 현장 입력',b_estop:'비상정지 스위치',b_obst:'도어 물체 감지',
 c_logic:'제어 판단과 호출 요청',a_logic:'PLC 판단 비트',c_io:'도어와 안전 입출력',c_live:'실시간 이벤트',b_all:'전체 이력',
 c_hist:'이벤트 이력',f_start:'시작일',f_end:'종료일',f_lv:'등급',f_cat:'분류',f_q:'검색',ph_q:'내용 또는 조작 IP',
 b_search:'조회',b_today:'오늘',b_7d:'7일',b_30d:'30일',b_csv:'CSV 내보내기',all:'전체',
 h_no:'번호',h_time:'일시',h_lv:'등급',h_cat:'분류',h_msg:'내용',h_run:'운행 상태',h_pos:'위치',h_src:'조작 위치',
 b_prev:'이전',b_next:'다음',pg:'전체 {t}건 중 {a}–{b}',res:'조회 결과',cnt:'{n}건',no_ev:'조건에 맞는 이벤트가 없습니다',no_live:'기록된 이벤트가 없습니다',
 db_info:'DB {mb} MB, {d}일 보관',db_off:'이력 DB 사용 안 함',
 c_vars:'PLC 변수 모니터',ph_var:'변수명, 주소, 설명',f_dev:'디바이스',f_val:'필터',dev_m:'M (내부 릴레이)',dev_p:'P (입출력)',dev_d:'D (데이터)',
 val_on:'ON / 0 아님',val_w:'쓰기 가능',v_name:'변수명',v_addr:'주소',v_type:'타입',v_val:'현재값',v_desc:'설명',wr:'쓰기',var_cnt:'변수 {n}개',
 ft_comm:'통신',ft_upd:'최종 수신',ft_db:'이력 DB',sim_engine:'내장 시뮬레이터',unused:'사용 안 함',
 bn_off:'PLC 통신 두절',bn_off_s:'자동으로 재연결을 시도합니다',bn_fault:'이상 정지',bn_fault_s:'현장 원인을 제거한 뒤 에러 리셋을 눌러 주십시오',code:'코드 {n}',
 exact:'착상 완료',between:'층간 이동 중',dir_up:'▲ 상승',dir_dn:'▼ 하강',dir_none:'정지',
 cruise:'정속 주행',slowing:'감속 구간',vfd_stop:'VFD 정지',
 d_closed:'닫힘',d_open:'열림',d_moving:'동작 중',d_sub:'{p}% 열림{e}, {s}',est:' (추정)',seq_idle:'시퀀스 대기',seq:'{x} 단계',
 hz_hi:'고속 지령',hz_lo:'저속 지령',hz_in:'점검 지령',hz_idle:'대기 지령 {v} Hz',
 day:'{n}회',day_s:'호출 {c}, 조작 {o}, 경보 {a}',
 running:'운전 중',stopped:'정지',judge:'판단층 {n}',vfd_err:'VFD 파라미터 블록을 읽을 수 없습니다: ',
 io_sim:'시뮬레이션 모델이 계산한 현장 입출력 값입니다.',
 io_bad:'P 디바이스 미러 값이 PLC 로직과 일치하지 않아 표시를 중단했습니다. PLC 에 EV_s2(P00_P_MIRROR) 프로그램이 다운로드되었는지 확인하십시오.',
 io_ok:'P00_P_MIRROR 가 M0010~M0015 로 복사한 현장 입출력 값입니다 (검증 완료).',
 io_wait:'P00_P_MIRROR 미러 값 · 도어가 닫히면 자동으로 일치 여부를 검증합니다.',
 note_sim:'시뮬레이션 모드에서는 승강장 호출 버튼(▲▼)을 직접 누를 수 있습니다.',
 note_plc:'카 위치는 PLC 현재층 값(D0) 기준이며, 승강장 호출 버튼은 현장 입력이라 표시만 합니다.',
 hall_up:'{f}층 승강장 상행 호출',hall_dn:'{f}층 승강장 하행 호출',field_only:' (현장 입력, 표시 전용)',cab:'{f}층 행선 버튼',
 sens_u:'{f}층 상부 위치 센서',sens_l:'{f}층 하부 위치 센서',
 fail:'명령 실패: ',srv_err:'서버와 통신할 수 없습니다: ',ws_lost:'SCADA 서버와의 연결이 끊어졌습니다',hist_err:'이력을 불러올 수 없습니다: ',
 to_light:'라이트 테마로 전환',to_dark:'다크 테마로 전환',
 wd:['일','월','화','수','목','금','토']},
en:{subtitle:'Elevator Supervisory Control',tab_ops:'Operations',tab_hist:'Event History',tab_vars:'Tag Monitor',
 sim:'Simulation',plc:'Live PLC',online:'Online',offline:'Offline',rtt:'RTT {n} ms',
 k_run:'Status',k_pos:'Position',k_dir:'Direction',k_door:'Door',k_hz:'VFD Command',k_day:'Trips Today',
 c_shaft:'Hoistway',ind_floor:'Current floor',lg_call:'Call registered',lg_sens:'Floor sensor active',
 c_op:'Operation Detail',r_run:'Status',r_dir:'Direction',r_cur:'Position',r_jdg:'Decision floor',r_door:'Arrival stage',r_err:'Error code',r_dpos:'Door opening',
 c_cop:'Car Operating Panel',t_open:'Door open (hold)',t_close:'Door close (hold)',
 n_cop:'Press a floor button to register a car call; press again to cancel. Door buttons act only while held.',
 c_vfd:'VFD',p_hi:'High speed',p_lo:'Low speed',p_in:'Inspection',
 c_maint:'Maintenance & Recovery',b_insp:'Inspection mode',b_reset:'Fault reset',s_manual:'Manual run (inspection mode only)',
 b_up:'Manual up',b_dn:'Manual down',b_open:'Door open',b_close:'Door close',
 n_manual:'Manual run works only in inspection mode while the button is held, and stops immediately on release or communication loss.',
 s_sim:'Simulated field inputs',b_estop:'Emergency stop switch',b_obst:'Door obstruction',
 c_logic:'Control Logic & Calls',a_logic:'PLC decision bits',c_io:'Door & Safety I/O',c_live:'Live Events',b_all:'View all',
 c_hist:'Event History',f_start:'From',f_end:'To',f_lv:'Level',f_cat:'Category',f_q:'Search',ph_q:'Message or operator IP',
 b_search:'Search',b_today:'Today',b_7d:'7 days',b_30d:'30 days',b_csv:'Export CSV',all:'All',
 h_no:'No.',h_time:'Time',h_lv:'Level',h_cat:'Category',h_msg:'Message',h_run:'Status',h_pos:'Position',h_src:'Operator',
 b_prev:'Previous',b_next:'Next',pg:'{a}–{b} of {t}',res:'Results',cnt:'{n}',no_ev:'No events match the filter',no_live:'No events recorded yet',
 db_info:'DB {mb} MB, kept {d} days',db_off:'Event DB disabled',
 c_vars:'PLC Tag Monitor',ph_var:'Name, address or description',f_dev:'Device',f_val:'Filter',dev_m:'M (internal relay)',dev_p:'P (I/O)',dev_d:'D (data)',
 val_on:'ON / non-zero',val_w:'Writable',v_name:'Tag',v_addr:'Address',v_type:'Type',v_val:'Value',v_desc:'Description',wr:'Write',var_cnt:'{n} tags',
 ft_comm:'Link',ft_upd:'Last update',ft_db:'Event DB',sim_engine:'Built-in simulator',unused:'disabled',
 bn_off:'PLC communication lost',bn_off_s:'Reconnecting automatically',bn_fault:'Fault stop',bn_fault_s:'Clear the cause on site, then press Fault reset',code:'code {n}',
 exact:'Landed',between:'Travelling between floors',dir_up:'▲ Up',dir_dn:'▼ Down',dir_none:'Stopped',
 cruise:'Cruising',slowing:'Decelerating',vfd_stop:'VFD stopped',
 d_closed:'Closed',d_open:'Open',d_moving:'Moving',d_sub:'{p}% open{e}, {s}',est:' (est.)',seq_idle:'Sequence idle',seq:'{x} stage',
 hz_hi:'High-speed command',hz_lo:'Low-speed command',hz_in:'Inspection command',hz_idle:'Standby setpoint {v} Hz',
 day:'{n}',day_s:'Calls {c}, ops {o}, alarms {a}',
 running:'Running',stopped:'Stopped',judge:'Decision floor {n}',vfd_err:'Cannot read VFD parameter block: ',
 io_sim:'Field I/O values computed by the simulation model.',
 io_bad:'P-device mirror does not match PLC logic, so field I/O is hidden. Check that EV_s2 (P00_P_MIRROR) is downloaded to the PLC.',
 io_ok:'Field I/O copied to M0010–M0015 by P00_P_MIRROR (verified).',
 io_wait:'P00_P_MIRROR values · verified automatically when the door closes.',
 note_sim:'In simulation mode you can press the hall call buttons (▲▼) directly.',
 note_plc:'Car position follows the PLC floor value (D0). Hall call buttons are field inputs and are display-only.',
 hall_up:'Floor {f} hall call up',hall_dn:'Floor {f} hall call down',field_only:' (field input, display only)',cab:'Floor {f} car call',
 sens_u:'Floor {f} upper position sensor',sens_l:'Floor {f} lower position sensor',
 fail:'Command failed: ',srv_err:'Cannot reach server: ',ws_lost:'Connection to SCADA server lost',hist_err:'Cannot load history: ',
 to_light:'Switch to light theme',to_dark:'Switch to dark theme',
 wd:['Sun','Mon','Tue','Wed','Thu','Fri','Sat']}};
const E={
 ko:{RUN:{0:'대기',1:'상승 운행',2:'하강 운행',3:'도착',8:'점검 운전',9:'이상 정지'},
  RUN_D:{0:'호출 대기 중',1:'목적층으로 상승 중',2:'목적층으로 하강 중',3:'도어 시퀀스 진행',8:'유지보수 수동 운전',9:'원인 제거 후 리셋 필요'},
  DIR:{0:'정지',1:'상승',2:'하강'},DOOR:{0:'대기',1:'열림',2:'닫힘'},ERR:{0:'정상',1:'비상정지',2:'운행 중 도어 열림'},
  LV:{info:'정보',warn:'주의',alarm:'경보'},
  CAT:{run:'운행',arrive:'도착',pos:'위치',door:'도어',call:'호출',alarm:'알람',comm:'통신',op:'조작',sys:'시스템'}},
 en:{RUN:{0:'Standby',1:'Moving up',2:'Moving down',3:'Arrived',8:'Inspection',9:'Fault stop'},
  RUN_D:{0:'Waiting for calls',1:'Travelling up to destination',2:'Travelling down to destination',3:'Door sequence in progress',8:'Manual maintenance run',9:'Reset required after clearing cause'},
  DIR:{0:'Stopped',1:'Up',2:'Down'},DOOR:{0:'Idle',1:'Open',2:'Close'},ERR:{0:'Normal',1:'Emergency stop',2:'Door opened while running'},
  LV:{info:'Info',warn:'Warning',alarm:'Alarm'},
  CAT:{run:'Run',arrive:'Arrival',pos:'Position',door:'Door',call:'Call',alarm:'Fault',comm:'Comms',op:'Operator',sys:'System'}}};
let lang='ko';
try{const q=new URLSearchParams(location.search).get('lang'); lang=q||localStorage.getItem('evscada.lang')||(navigator.language||'ko').slice(0,2)}catch(e){}
if(!T[lang]) lang='ko';
const t=(k,p)=>{let s=T[lang][k]??T.ko[k]??k; if(p) for(const x in p) s=s.split('{'+x+'}').join(p[x]); return s};
const e=k=>E[lang][k];
const flr=n=>lang==='ko'?`${n}층`:`${n}F`;
const lm=m=>Array.isArray(m)?(lang==='en'?m[1]:m[0]):(m??'');
const nf=n=>Number(n||0).toLocaleString(lang==='ko'?'ko-KR':'en-US');

let S={tags:{},today:{}}, SYMS=[], mode='', lastPoll=0, pollDt=500, feedKey='';
const fh=()=>parseFloat(getComputedStyle(document.documentElement).getPropertyValue('--fh'));
const hd=()=>parseFloat(getComputedStyle(document.documentElement).getPropertyValue('--head'));
const pad=n=>String(n).padStart(2,'0');
const ymd=d=>`${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;

function applyLang(){
 document.documentElement.lang=lang;
 document.querySelectorAll('[data-i18n]').forEach(el=>el.textContent=t(el.dataset.i18n));
 document.querySelectorAll('[data-i18n-ph]').forEach(el=>el.placeholder=t(el.dataset.i18nPh));
 document.querySelectorAll('[data-i18n-title]').forEach(el=>el.title=t(el.dataset.i18nTitle));
 document.querySelectorAll('#langSw button').forEach(b=>b.classList.toggle('on',b.dataset.l===lang));
 themeTitle();
 const opt=(id,list)=>{const el=$(id),v=el.value; el.innerHTML=list.map(([k,l])=>`<option value="${k}">${esc(l)}</option>`).join(''); el.value=v||''};
 opt('hLv',[['',t('all')],...Object.entries(e('LV'))]);
 opt('hCat',[['',t('all')],...Object.entries(e('CAT'))]);
 opt('fDev',[['',t('all')],['M',t('dev_m')],['P',t('dev_p')],['D',t('dev_d')]]);
 opt('fVal',[['',t('all')],['on',t('val_on')],['w',t('val_w')]]);
 buildShaft(); buildLamps(); buildTable(); bindDynamic(); applyMode(); tick();
 feedKey=''; if(S.tags) render();
 if($('v-hist').classList.contains('on')) loadHist();
}
document.querySelectorAll('#langSw button').forEach(b=>b.onclick=()=>{lang=b.dataset.l; try{localStorage.setItem('evscada.lang',lang)}catch(e){} applyLang()});

/* ---------- 테마 ---------- */
const curTheme=()=>document.documentElement.dataset.theme==='light'?'light':'dark';
function themeTitle(){const b=$('themeBtn'); b.title=t(curTheme()==='light'?'to_dark':'to_light'); b.setAttribute('aria-label',b.title)}
$('themeBtn').onclick=()=>{const n=curTheme()==='light'?'dark':'light'; document.documentElement.dataset.theme=n;
 try{localStorage.setItem('evscada.theme',n)}catch(e){} themeTitle()};
try{matchMedia('(prefers-color-scheme: light)').addEventListener('change',ev=>{
 let saved=null; try{saved=localStorage.getItem('evscada.theme')}catch(e){}
 if(!saved){document.documentElement.dataset.theme=ev.matches?'light':'dark'; themeTitle()}})}catch(e){}

/* ---------- 탭 ---------- */
function go(v){
 document.querySelectorAll('#tabs button').forEach(b=>b.classList.toggle('on',b.dataset.v===v));
 document.querySelectorAll('.view').forEach(s=>s.classList.toggle('on',s.id==='v-'+v));
 try{history.replaceState(null,'','#'+v)}catch(e){}
 if(v==='hist') loadHist();
 if(v==='vars') render();
}
document.querySelectorAll('#tabs button').forEach(b=>b.onclick=()=>go(b.dataset.v));

/* ---------- 시계 ---------- */
function tick(){const d=new Date(); $('clkT').textContent=`${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
 const w=t('wd')[d.getDay()];
 $('clkD').textContent=lang==='ko'?`${d.getFullYear()}. ${d.getMonth()+1}. ${d.getDate()} (${w})`:`${w}, ${d.toLocaleDateString('en-US',{month:'short',day:'numeric',year:'numeric'})}`}
setInterval(tick,1000);

/* ---------- 정적 UI ---------- */
function buildShaft(){
 const L=$('labels'), V=$('lvls'), H=fh(), HD=hd(); L.innerHTML=''; V.innerHTML='<div class="overhead"></div>';
 for(let f=6;f>=1;f--){
  const top=(6-f)*H+HD;
  const up=f<6?`<button class="hb" id="hu${f}" data-tag="버튼_${f}층상행" title="${esc(t('hall_up',{f}))}">▲</button>`:'<button class="hb ghost" tabindex="-1"></button>';
  const dn=f>1?`<button class="hb" id="hd${f}" data-tag="버튼_${f}층하행" title="${esc(t('hall_dn',{f}))}">▼</button>`:'<button class="hb ghost" tabindex="-1"></button>';
  L.insertAdjacentHTML('beforeend',`<div class="floor-row" style="top:${top}px"><span class="fl" id="fl${f}">${f}F</span><div class="hall">${up}${dn}</div></div>`);
  V.insertAdjacentHTML('beforeend',`<div class="lvl" style="top:${top}px"><span>L${f}</span>
   <div class="sens" id="su${f}" style="top:${H*0.25-3}px" title="${esc(t('sens_u',{f}))}"></div>
   <div class="sens" id="sl${f}" style="top:${H*0.75-3}px" title="${esc(t('sens_l',{f}))}"></div></div>`);
 }
 $('cop').innerHTML=[5,6,3,4,1,2].map(f=>`<button class="cb" id="cab${f}" data-tag="화면_내부${f}층" title="${esc(t('cab',{f}))}">${f}</button>`).join('');
}
const lamp=(id,name,label,cls='',addr='')=>`<div class="lp ${cls}" id="${id}" title="${esc(name)}"><i></i>${esc(label)}${addr?`<small>${addr}</small>`:''}</div>`;
// [변수명, 한국어, English, 색상, 주소]
const VFD=[['인버터_상승','상승 지령','Up command','','M0000'],['인버터_하강','하강 지령','Down command','','M0001'],['인버터_정지','정지 지령','Stop command','y','M0007'],['판단_저속','저속 운전','Low speed','y','M000B']];
const F=[['판단_현재층요청','현재층 요청','Call at floor'],['판단_위요청','상방 요청','Call above'],['판단_아래요청','하방 요청','Call below'],['판단_상승정지','상승 중 정지','Stop (up)'],['판단_하강정지','하강 중 정지','Stop (down)'],
 ['판단_정위치','정위치','Level'],['판단_문닫힘','도어 닫힘 확인','Door closed'],['판단_갈곳없음','잔여 요청 없음','No pending call','y'],['문_열림유지','도어 열림 유지','Door hold open','y'],['문_동작완료','도어 시퀀스 완료','Door cycle done']];
const IO=[['출력_문열림밸브','열림 밸브','Open valve','y','P0025'],['출력_문닫힘밸브','닫힘 밸브','Close valve','y','P0024'],['센서_좌문열림','좌측 열림','Left open','','P004A'],['센서_우문열림','우측 열림','Right open','','P004C'],
 ['센서_좌문닫힘','좌측 닫힘','Left closed','','P004B'],['센서_우문닫힘','우측 닫힘','Right closed','','P004D'],['센서_문','물체 감지','Obstruction','r','P004E'],['스위치_비상정지','비상정지','E-stop','r','P004F']];
const L2=x=>lang==='en'?x[2]:x[1];
function buildLamps(){
 $('vfdLamps').innerHTML=VFD.map((x,i)=>lamp('vf'+i,x[0],L2(x),x[3]||'',x[4])).join('');
 let h=F.map((x,i)=>lamp('f'+i,x[0],L2(x),x[3]||'')).join('');
 for(let f=1;f<=6;f++) h+=lamp('rq'+f,'요청_'+f+'층',lang==='en'?`Floor ${f} request`:`${f}층 요청`,'y');
 $('flagLamps').innerHTML=h;
 $('ioLamps').innerHTML=IO.map((x,i)=>lamp('io'+i,x[0],L2(x),x[3]||'',x[4])).join('');
}
function buildTable(){
 $('vt').innerHTML=SYMS.map((s,i)=>{const c=lang==='en'?(s.comment_en||s.comment):s.comment;
  return `<tr data-k="${esc((s.name+' '+s.addr+' '+s.comment+' '+(s.comment_en||'')).toLowerCase())}" data-d="${s.addr[0]}" data-w="${s.writable?1:0}" data-i="${i}">
  <td>${esc(s.name)}</td><td class="m">${s.addr}</td><td class="m">${s.type}</td><td class="v" id="vv${i}">-</td><td style="color:var(--tx2)">${esc(c)}</td>
  <td>${s.writable?`<span class="wtag">${t('wr')}</span>`:''}</td></tr>`}).join('');
 $('varCount').textContent=t('var_cnt',{n:SYMS.length});
 filterVars();
}
function filterVars(){
 const q=$('flt').value.toLowerCase(), dv=$('fDev').value, fv=$('fVal').value;
 document.querySelectorAll('#vt tr').forEach(r=>{
  const v=(S.tags||{})[SYMS[r.dataset.i].name];
  const ok=r.dataset.k.includes(q)&&(!dv||r.dataset.d===dv)&&(!fv||(fv==='on'?!!v:r.dataset.w==='1'));
  r.style.display=ok?'':'none'});
}
['flt','fDev','fVal'].forEach(id=>$(id).addEventListener('input',filterVars));

/* ---------- 명령 ---------- */
let toastT;
function toast(m){const el=$('toast');el.textContent=m;el.classList.add('show');clearTimeout(toastT);toastT=setTimeout(()=>el.classList.remove('show'),4500)}
async function cmd(tag,op='pulse',value=1){
 try{const r=await fetch('/cmd',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({tag,op,value,lang})});
  const j=await r.json(); if(!j.ok) toast(t('fail')+j.error); return j.ok;}catch(err){toast(t('srv_err')+err)}
}
function bindHold(el){
 const tag=el.dataset.tag; let iv=null;
 const start=ev=>{ev.preventDefault(); if(iv||el.disabled) return; el.classList.add('on'); cmd(tag,'hold'); iv=setInterval(()=>cmd(tag,'hold'),300)};
 const stop=()=>{if(!iv) return; clearInterval(iv); iv=null; el.classList.remove('on'); cmd(tag,'release')};
 el.addEventListener('pointerdown',start); ['pointerup','pointerleave','pointercancel'].forEach(x=>el.addEventListener(x,stop));
 el.addEventListener('contextmenu',ev=>ev.preventDefault());
}
function bindDynamic(){
 document.querySelectorAll('.hb[data-tag],.cb[id^=cab]').forEach(b=>b.onclick=()=>{b.classList.add('on');cmd(b.dataset.tag,'pulse')});
}
function bindStatic(){
 document.querySelectorAll('.btn.hold').forEach(bindHold);
 bindHold($('bOpen')); bindHold($('bClose'));
 $('bInsp').onclick=()=>cmd('화면_점검','toggle');
 $('bReset').onclick=()=>cmd('화면_에러리셋','pulse');
 document.querySelectorAll('[data-toggle]').forEach(b=>b.onclick=()=>cmd(b.dataset.tag,'toggle'));
}

/* ---------- 카/도어 애니메이션 (수신값 사이 선형 보간) ---------- */
const anim={pos:1,door:0,fp:1,tp:1,fd:0,td:0,t0:0,dur:100,
 set(p,d){this.fp=this.pos; this.fd=this.door; this.tp=p; this.td=d; this.t0=performance.now();
  this.dur=mode==='sim'?110:Math.min(pollDt,400);
  if(Math.abs(p-this.pos)>1.5){this.pos=this.fp=p} this.draw()},
 frame(now){const k=Math.min(1,(now-this.t0)/this.dur);
  this.pos=this.fp+(this.tp-this.fp)*k; this.door=this.fd+(this.td-this.fd)*k;
  this.draw(); requestAnimationFrame(x=>this.frame(x))},
 draw(){const H=fh();
  $('car').style.transform=`translateY(${-(this.pos-1)*H}px)`;
  $('cable').style.height=`${(6-this.pos)*H+hd()+6}px`;
  $('dl').style.transform=`translateX(${-this.door*96}%)`; $('dr').style.transform=`translateX(${this.door*96}%)`}};
requestAnimationFrame(x=>anim.frame(x));

/* ---------- 화면 갱신 ---------- */
const on=(id,v)=>{const el=$(id); if(!el) return; el.classList.toggle('on',!!v); el.classList.toggle('na',v===null||v===undefined)};
const setPill=(id,cls,txt)=>{const el=$(id); el.className='pill '+cls; el.lastElementChild.textContent=txt};
const hzTxt=v=>v==null?'-':(v/100).toFixed(2);
const TONE={0:'',1:'ac',2:'ac',3:'ok',8:'warn',9:'err'};
function kpi(id,v,s,tone=''){const el=$(id); el.textContent=v; el.className='v'+(tone?' t-'+tone:'')+(['kPos','kHz','kDay'].includes(id)?' num':'');
 if(s!==undefined) $(id+'S').textContent=s; const d=$(id+'D'); if(d) d.className=tone?'d-'+tone:''}
function render(){
 const tg=S.tags||{}, g=k=>tg[k];
 setPill('pMode',mode==='sim'?'warn':'ac',mode==='sim'?t('sim'):t('plc'));
 setPill('pConn',S.connected?'ok':'err',S.connected?t('online'):t('offline'));
 if(S.rtt!=null&&S.connected&&mode!=='sim'){$('pRtt').style.display='';setPill('pRtt','',t('rtt',{n:S.rtt}))} else $('pRtt').style.display='none';
 $('fPlc').textContent=mode==='sim'?t('sim_engine'):(S.plc||'-'); $('fUpd').textContent=S.updated||'-'; $('fDb').textContent=S.db||t('unused');
 const run=g('상태_운행'), er=g('상태_에러코드'), dir=g('상태_방향'), cur=g('위치_현재층'), ds=g('상태_도착단계');
 const moving=!!(g('인버터_상승')||g('인버터_하강')), slow=!!g('판단_저속');
 // 배너
 const bn=$('banner');
 if(!S.connected){bn.classList.add('show');$('bnT').textContent=t('bn_off');$('bnS').textContent=(S.error?S.error+'. ':'')+t('bn_off_s')}
 else if(run===9){bn.classList.add('show');$('bnT').textContent=t('bn_fault')+': '+(e('ERR')[er]??t('code',{n:er}));$('bnS').textContent=t('bn_fault_s')}
 else bn.classList.remove('show');
 // KPI
 const pos=S.pos||1, d=Math.max(0,Math.min(1,S.door||0));
 kpi('kRun',e('RUN')[run]??'-',e('RUN_D')[run]??'-',TONE[run]||'');
 const exact=cur!=null&&cur%10===0, lo=Math.floor((cur||0)/10);
 kpi('kPos',cur==null?'-':(exact?flr(cur/10):`${flr(lo)} – ${flr(lo+1)}`),cur==null?'-':`${exact?t('exact'):t('between')} (D0=${cur})`);
 kpi('kDir',dir==null?'-':(dir===1?t('dir_up'):dir===2?t('dir_dn'):t('dir_none')),moving?(slow?t('slowing'):t('cruise')):t('vfd_stop'),dir?'ac':'');
 kpi('kDoor',d<=0.01?t('d_closed'):d>=0.99?t('d_open'):t('d_moving'),
  t('d_sub',{p:Math.round(d*100),e:mode==='plc'?t('est'):'',s:ds?t('seq',{x:e('DOOR')[ds]}):t('seq_idle')}),d<=0.01?'':'ac');
 const hz=g('인버터_지령주파수'), hi=g('설정_고속주파수');
 kpi('kHz',moving&&hz!=null?`${hzTxt(hz)} Hz`:'0.00 Hz',hz==null?'-':(moving?(run===8?t('hz_in'):slow?t('hz_lo'):t('hz_hi')):t('hz_idle',{v:hzTxt(hz)})),moving?'ac':'');
 const td=S.today||{};
 kpi('kDay',t('day',{n:nf(td.arrive)}),t('day_s',{c:nf(td.call),o:nf(td.op),a:nf(td.alarm)}),td.alarm?'err':'');
 $('kDay').classList.remove('t-err');
 // 승강로
 anim.set(pos,d); $('car').classList.toggle('err',run===9); $('car').classList.toggle('run',moving&&run!==9);
 $('seg').textContent=cur==null?'-':Math.round(pos);
 $('indSt').textContent=e('RUN')[run]??'-'; $('indSt').className=TONE[run]?'t-'+TONE[run]:'';
 $('aUp').classList.toggle('on',dir===1); $('aUp').classList.toggle('blink',dir===1&&moving);
 $('aDn').classList.toggle('on',dir===2); $('aDn').classList.toggle('blink',dir===2&&moving);
 $('carLed').textContent=(e('RUN')[run]??'-')+(slow&&moving?(lang==='ko'?', 저속':', slow'):'');
 for(let f=1;f<=6;f++){
  on('hu'+f,g('호출_'+f+'층상행')); on('hd'+f,g('호출_'+f+'층하행')); on('cab'+f,g('호출_내부'+f+'층'));
  on('su'+f,g('센서_'+f+'층상부')); on('sl'+f,g('센서_'+f+'층하부'));
  $('fl'+f)?.classList.toggle('cur',Math.round(pos)===f);
 }
 $('shaftAside').textContent=cur==null?'':t('judge',{n:g('위치_판단층')??'-'});
 // 운행 상세
 $('vRun').textContent=run==null?'-':e('RUN')[run]??run; $('vRun').className='big'+(TONE[run]?' t-'+TONE[run]:'');
 $('vDir').textContent=dir==null?'-':`${e('DIR')[dir]??dir} (${dir})`;
 $('vCur').textContent=cur==null?'-':`${flr((cur/10).toFixed(1))} (${cur})`;
 $('vJdg').textContent=g('위치_판단층')??'-';
 $('vDoor').textContent=ds==null?'-':`${e('DOOR')[ds]??ds} (${ds})`;
 $('vErr').textContent=er==null?'-':`${e('ERR')[er]??'?'} (${er})`; $('vErr').className=er?'t-err':'';
 $('vDoorPos').textContent=`${Math.round(d*100)}%`+(mode==='plc'?t('est'):'');
 // VFD
 VFD.forEach((x,i)=>on('vf'+i,g(x[0])));
 $('vHz').textContent=hz==null?'-':(moving?hzTxt(hz):'0.00');
 $('vHzS').textContent=hz==null?'':moving?`D1005 = ${hz}`:t('hz_idle',{v:hzTxt(hz)});
 $('vHzS').className='t-mut';
 $('hzBar').style.width=hz==null||!moving?'0':`${Math.min(100,hz/(hi||4000)*100)}%`;
 $('vfdAside').textContent=moving?t('running'):t('stopped');
 const fx=v=>v==null?'-':`${hzTxt(v)} Hz`;
 $('pHi').textContent=fx(hi); $('pLo').textContent=fx(g('설정_저속주파수')); $('pIn').textContent=fx(g('설정_점검주파수'));
 $('vfdErr').textContent=S.vfd_error?t('vfd_err')+lm(S.vfd_error):'';
 // 점검
 $('bInsp').classList.toggle('on',!!g('화면_점검'));
 const insp=run===8; document.querySelectorAll('.btn.hold').forEach(b=>b.disabled=!insp);
 document.querySelectorAll('[data-toggle]').forEach(b=>b.classList.toggle('on',!!g(b.dataset.tag)));
 // 플래그 / IO
 F.forEach((x,i)=>on('f'+i,g(x[0]))); for(let f=1;f<=6;f++) on('rq'+f,g('요청_'+f+'층'));
 IO.forEach((x,i)=>on('io'+i,g(x[0])));
 $('ioNote').textContent=mode!=='sim'&&S.p_mirror===false?t('io_bad'):'';   // 미러 불일치 경고만 표시
 // 변수표
 if($('v-vars').classList.contains('on')){
  SYMS.forEach((s,i)=>{const el=$('vv'+i); if(!el) return; const v=tg[s.name];
   el.textContent=v==null?'—':v; el.className='v '+(v==null?'vna':(s.type==='BIT'&&v)?'v1':'')});
  if($('fVal').value==='on') filterVars();
 }
 // 실시간 이벤트
 const L=S.log||[], key=lang+L.length+':'+(L[0]?.id??L[0]?.t);
 if(key!==feedKey){feedKey=key;
  $('feed').innerHTML=L.length?L.map(l=>`<div class="ev ${l.lv}"><time>${l.t}</time><span><span class="tag c-${l.cat} ${l.lv==='info'?'':l.lv}">${esc(e('CAT')[l.cat]??l.cat)}</span></span>
   <p>${esc(lm(l.msg))}${l.src?`<em>${esc(l.src)}</em>`:''}</p></div>`).join(''):`<div class="empty">${t('no_live')}</div>`;
 }
}
function applyMode(){
 const sim=mode==='sim';
 $('simBox').style.display=sim?'block':'none';
 document.querySelectorAll('.hb[data-tag]').forEach(b=>{b.disabled=!sim; if(!sim&&!b.title.endsWith(t('field_only'))) b.title+=t('field_only')});
}

/* ---------- 이벤트 이력 ---------- */
const H={page:0};
function quick(days){const en=new Date(), s=new Date(); s.setDate(s.getDate()-days); $('hS').value=ymd(s); $('hE').value=ymd(en); H.page=0; loadHist()}
function hParams(){return new URLSearchParams({start:$('hS').value,end:$('hE').value,level:$('hLv').value,category:$('hCat').value,q:$('hQ').value,lang})}
async function loadHist(){
 const size=+$('hSize').value, p=hParams(); p.set('limit',size); p.set('offset',H.page*size);
 let j; try{j=await (await fetch('/api/events?'+p)).json()}catch(err){toast(t('hist_err')+err);return}
 if(j.error){$('hBody').innerHTML=`<tr><td colspan="8" class="empty">${esc(j.error)}</td></tr>`;return}
 $('hBody').innerHTML=j.rows.length?j.rows.map(r=>`<tr>
  <td class="m">${r.id}</td><td class="m">${r.time}</td>
  <td><span class="tag ${r.level}">${esc(e('LV')[r.level]??r.level)}</span></td>
  <td><span class="tag c-${r.category}">${esc(e('CAT')[r.category]??r.category)}</span></td>
  <td class="msg">${esc(r.message)}</td>
  <td>${r.run_state==null?'<span class="t-mut">-</span>':esc(e('RUN')[r.run_state]??r.run_state)}</td>
  <td class="m">${r.floor==null?'-':r.floor%10===0?flr(r.floor/10):r.floor}</td>
  <td class="m">${esc(r.source??'-')}</td></tr>`).join(''):`<tr><td colspan="8" class="empty">${t('no_ev')}</td></tr>`;
 const from=j.total?H.page*size+1:0, to=Math.min(j.total,(H.page+1)*size);
 $('hInfo').textContent=t('pg',{t:nf(j.total),a:nf(from),b:nf(to)});
 $('hPrev').disabled=H.page===0; $('hNext').disabled=to>=j.total;
 const sm=j.summary||{level:{},category:{}};
 $('hSum').innerHTML=`<span class="chip">${t('res')} <b>${t('cnt',{n:nf(j.total)})}</b></span>`
  +['alarm','warn','info'].map(k=>`<span class="chip"><span class="tag ${k}">${e('LV')[k]}</span><b>${nf(sm.level[k])}</b></span>`).join('')
  +['arrive','call','op','comm'].map(k=>`<span class="chip">${e('CAT')[k]} <b>${nf(sm.category[k])}</b></span>`).join('');
 try{const st=await (await fetch('/api/stats')).json();
  $('dbInfo').textContent=st.db_path?t('db_info',{mb:(st.db_bytes/1048576).toFixed(2),d:st.retention_days}):t('db_off')}catch(err){}
}
$('hPrev').onclick=()=>{if(H.page>0){H.page--;loadHist()}};
$('hNext').onclick=()=>{H.page++;loadHist()};
$('hSize').onchange=()=>{H.page=0;loadHist()};
['hLv','hCat'].forEach(id=>$(id).onchange=()=>{H.page=0;loadHist()});
$('hCsv').onclick=()=>{location.href='/api/events.csv?'+hParams()};

/* ---------- 연결 ---------- */
function conn(){
 const ws=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws');
 ws.onmessage=ev=>{const s=JSON.parse(ev.data); const now=performance.now();
  if(lastPoll) pollDt=Math.min(1500,Math.max(100,now-lastPoll)); lastPoll=now;
  S=s; if(s.mode!==mode){mode=s.mode;applyMode()} render()};
 ws.onclose=()=>{S.connected=false;S.error=t('ws_lost');render();setTimeout(conn,2000)};
}
(async()=>{
 const n=new Date(), s=new Date(); s.setDate(s.getDate()-6); $('hS').value=ymd(s); $('hE').value=ymd(n);
 try{SYMS=await (await fetch('/symbols')).json()}catch(err){SYMS=[]}
 applyLang(); bindStatic(); conn();
 const v=location.hash.slice(1); if(['ops','hist','vars'].includes(v)) go(v);
})();
</script></body></html>"""


@app.get("/")
async def index():
    return HTMLResponse(PAGE)


if __name__ == "__main__":
    # host="0.0.0.0" 이어야 다른 기기(폰, 태블릿)에서 접속 가능
    uvicorn.run(app, host="0.0.0.0", port=8000)
