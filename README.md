# EV-S2 SCADA

LS산전 **XGB PLC + XBL-EMTA** 로 제어하는 6층 승강기 모형(EV-S2)을 위한 **웹 기반 감시제어(SCADA) 시스템**입니다.
Python(FastAPI) 서버가 Modbus TCP 로 PLC 를 폴링하고, 브라우저 화면에 WebSocket 으로 실시간 상태를 보여줍니다.
PLC 가 없어도 래더 로직을 그대로 이식한 **내장 시뮬레이터**로 바로 실행해 볼 수 있습니다.

*Web-based SCADA for a 6-floor elevator trainer controlled by an LS XGB PLC. Includes a built-in simulator that ports the ladder logic to Python, so it runs without hardware. UI in Korean / English.*

```
브라우저  <── WebSocket / HTTP ──>  Python (FastAPI)  <── Modbus TCP ──>  LS XGB + XBL-EMTA
                                          │
                                          └── SQLite (이벤트 이력)
```

## 주요 기능

- **운전 현황** — 승강로 애니메이션(카 위치·도어 개폐), 층 표시기, 호출 등록 상태, 층 위치 센서 표시
- **카 조작반 / 승강장 호출** — 화면에서 행선 버튼, 도어 열림·닫힘 조작
- **VFD 모니터** — 상승/하강/정지 지령, 지령 주파수(D1005), 고속·저속·점검 파라미터
- **점검 · 복구** — 점검 모드, 에러 리셋, 수동 상승/하강/도어 (누르는 동안만 동작, 통신이 끊기면 자동 OFF)
- **PLC 판단 비트 / 도어·안전 입출력** 램프 표시
- **이벤트 이력** — 운행·도착·호출·알람·통신·조작 이벤트를 SQLite 에 저장, 기간/등급/분류/검색 조회, CSV(엑셀용) 내보내기
- **변수 모니터** — PLC 글로벌 변수 95개의 주소·현재값 실시간 표시
- **한국어 / English** 화면 전환, **다크 / 라이트 테마**
- **시뮬레이션 모드** — PLC 없이 래더 로직 + 물리 모델(카 속도·가감속, 도어)로 동작

## 요구 사항

- Python **3.10 이상**
- 실기 연결 시: LS XGB PLC + XBL-EMTA(FEnet), 저장소의 XG5000 프로젝트 `EV_s2.xgwx` 다운로드, XG-PD Modbus 서버 설정

## 설치

```bash
git clone https://github.com/ijm265-glitch/ev-s2-scada.git
cd ev-s2-scada
python -m venv .venv
# Windows: .venv\Scripts\activate   /   macOS·Linux: source .venv/bin/activate
pip install -r requirements.txt
```

## 실행

```bash
python ev_scada.py --sim    # PLC 없이 내장 시뮬레이터로 실행
python ev_scada.py          # 실제 PLC 연결
```

브라우저에서 **http://localhost:8000** 접속.
같은 네트워크의 다른 기기(폰·태블릿)에서는 `http://<PC IP>:8000` 으로 접속합니다 (방화벽에서 TCP 8000 허용 필요).

- 언어 지정: `http://localhost:8000/?lang=en`
- 탭 바로가기: `#ops`(운전 현황), `#hist`(이벤트 이력), `#vars`(변수 모니터)

## 설정

`ev_scada.py` 상단의 설정 블록을 현장에 맞게 수정합니다.

| 항목 | 기본값 | 설명 |
|---|---|---|
| `PLC_IP` | `192.168.10.233` | XBL-EMTA IP 주소 |
| `PLC_PORT` / `UNIT_ID` | `502` / `1` | Modbus TCP 포트 / 국번 |
| `POLL_SEC` | `0.2` | PLC 폴링 주기(초) |
| `PULSE_SEC` | `0.2` | 순간 버튼 ON 유지 시간 |
| `HOLD_TIMEOUT` | `1.0` | 누르고 있는 버튼 신호가 끊기면 자동 OFF 까지 시간 |
| `LOG_RETENTION_DAYS` | `180` | 이벤트 보관 기간(일), 지난 이벤트는 자동 삭제 |

환경 변수 `EV_SCADA_DB` 로 이벤트 DB 파일 경로를 바꿀 수 있습니다 (기본: 스크립트와 같은 폴더의 `ev_scada_events.db`).

## PLC 측 설정

### XG-PD Modbus 서버

| 영역 | 시작 주소 | Modbus 기능 | 용도 |
|---|---|---|---|
| 비트 읽기 | `M00000` | Discrete Input (FC02) | M 비트 전체 모니터 |
| 비트 쓰기 | `M00000` | Coil (FC05) | 화면 버튼 쓰기 (코일 n = M 비트 n) |
| 워드 읽기 | `D00000` | Input Register (FC04) | D0~D12 상태, D1000~D1006 인버터 파라미터 |

### P 디바이스 미러 (`P00_P_MIRROR`)

Modbus 로 P(입출력) 영역을 직접 읽을 수 없기 때문에, EV_s2 는 스캔 맨 앞에서 `P0000~P0005` 를 `M0010~M0015` 로 복사합니다.
SCADA 는 도어가 닫혀 있을 때 `판단_문닫힘(M000E)` 과 미러된 도어 센서 값을 비교해 미러 동작을 **자동 검증**하고,
일치하지 않으면 현장 입출력 표시를 중단하고 경고를 남깁니다 (EV_s1 이 다운로드되어 있는 경우 등).

### PLC 프로그램 구성 (스캔 순서)

| 프로그램 | 내용 |
|---|---|
| `INIT` | 운행 상태·방향·도착 단계·에러 코드, 호출 래치 초기화 |
| `VFD_PARAM` | 인버터 파라미터 (고속 40Hz / 저속 20Hz / 점검 5Hz) |
| `P00_P_MIRROR` | P0000~P0005 → M0010~M0015 |
| `P01_POSITION` | 층 센서 → 현재층 D0 (10~60, 층 사이 x3/x7), 판단층 D5 |
| `P02_REQUEST` | 승강장/카 버튼 → 호출 래치 → 층별 요청 |
| `P03_JUDGE` | 위/아래 요청, 정위치, 도어 닫힘, 정지 판단 |
| `P04_TRANSITION` | 운행 상태 D10 전이 (0 대기 · 1 상승 · 2 하강 · 3 도착 · 8 점검 · 9 에러) |
| `P05_DRIVE` | 인버터 상승/하강/정지, 지령 주파수 D1005 |
| `P06_ARRIVE` | 도착 후 도어 시퀀스 (대기 → 열림 → 닫힘, 각 4초) |
| `P07_DISPATCH` | 도착층 호출 삭제, 다음 방향 결정 |
| `P08_OUPUT` | 도어 열림/닫힘 솔밸브 출력 |

## HTTP API

| 메서드 | 경로 | 설명 |
|---|---|---|
| `GET` | `/` | SCADA 화면 |
| `WS` | `/ws` | 실시간 상태 스트림 (JSON) |
| `GET` | `/symbols` | 변수 목록 (이름·주소·타입·설명·쓰기 가능 여부) |
| `POST` | `/cmd` | 비트 쓰기 `{"tag": "화면_점검", "op": "toggle"}` — `op`: `pulse` · `set` · `toggle` · `hold` · `release` |
| `GET` | `/api/events` | 이벤트 조회 (`start`, `end`, `level`, `category`, `q`, `limit`, `offset`, `lang`) |
| `GET` | `/api/events.csv` | 같은 조건으로 CSV 내보내기 |
| `GET` | `/api/stats` | 금일 집계, DB 크기, 보관 기간 |

쓰기는 `화면_*` 변수(HMI 비트)만 허용됩니다. 시뮬레이션 모드에서는 승강장 버튼·비상정지·도어 물체 감지도 화면에서 조작할 수 있습니다.

## 이벤트 이력 DB

SQLite(파이썬 표준 라이브러리)를 사용하므로 별도 설치가 필요 없습니다.

```
events(id, ts, level, category, message, message_en, run_state, floor, source, mode)
```

- `level`: `info` 정보 · `warn` 주의 · `alarm` 경보
- `category`: `run` 운행 · `arrive` 도착 · `pos` 위치 · `door` 도어 · `call` 호출 · `alarm` 알람 · `comm` 통신 · `op` 조작 · `sys` 시스템
- 메시지는 한국어/영어 두 언어로 저장되며, 조작 이벤트에는 조작한 클라이언트 IP 가 기록됩니다.

## 프로젝트 구조

```
ev-s2-scada/
├── ev_scada.py        # 서버 + 시뮬레이터 + 웹 화면 (단일 파일)
├── EV_s2.xgwx         # XG5000 PLC 프로젝트 (래더 프로그램 · 변수 · FEnet 설정)
├── requirements.txt
├── README.md
├── LICENSE
└── .gitignore
```

## 주의 사항

- 이 프로젝트는 **교육용 승강기 모형**을 대상으로 작성되었습니다. 실제 승객용 승강기의 안전 기능을 대신하지 않습니다.
- 서버에는 인증 기능이 없습니다. 같은 네트워크에서 접속한 누구나 조작할 수 있으므로 신뢰할 수 있는 내부망에서만 사용하십시오.
- 화면은 Pretendard 웹폰트를 CDN 에서 불러옵니다. 인터넷이 없는 환경에서는 시스템 글꼴로 표시됩니다.

## 라이선스

[MIT](LICENSE)
