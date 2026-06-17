# 量化交易系统 — 设计文档

> **版本**: v0.2.0  
> **阶段**: 纸面交易（Paper Trading）原型  
> **目标**: 架构保证未来可"零修改策略"切换到迅投 QMT 实盘 API  
> **修订**: 新增 Decimal 精度约定、零股卖出规则、交易时间校验、涨跌停差异化字段

---

## 目录

1. [项目背景与设计原则](#1-项目背景与设计原则)
2. [系统架构总览](#2-系统架构总览)
3. [目录结构](#3-目录结构)
4. [核心模块设计](#4-核心模块设计)
   - [4.1 数据模型 (`core/models.py`)](#41-数据模型-coremodelspy)
   - [4.2 事件引擎 (`core/event_engine.py`)](#42-事件引擎-coreevent_enginepy)
   - [4.3 抽象网关 (`gateways/base.py`)](#43-抽象网关-gatewaysbasepy)
   - [4.4 纸面交易网关 (`gateways/paper_gateway.py`)](#44-纸面交易网关-gatewayspaper_gatewaypy)
   - [4.5 策略基类 (`strategies/base.py`)](#45-策略基类-strategiesbasepy)
5. [A 股交易规则建模](#5-a-股交易规则建模)
6. [事件流与数据流](#6-事件流与数据流)
7. [状态机设计](#7-状态机设计)
8. [测试场景](#8-测试场景)
9. [未来扩展规划](#9-未来扩展规划)

---

## 1. 项目背景与设计原则

### 1.1 核心约束

| 约束 | 说明 |
|------|------|
| **纸面交易优先** | 当前阶段使用本地模拟撮合，不接任何券商 API |
| **零修改切换** | 策略代码只依赖 `BaseGateway` 抽象接口，未来换 QMT 网关时策略文件不动一行 |
| **异步事件驱动** | 基于 `asyncio` + 事件总线，天然适配实盘的高并发行情推送 |
| **A 股规则完备** | T+1、涨跌停、手续费、手数限制必须在网关层实现，策略层无需关心 |
| **Decimal 精度** | 所有涉及价格、金额、费用的字段统一使用 `Decimal`（保留 4 位小数），杜绝浮点精度丢失 |

### 1.2 设计原则

```
┌──────────────────────────────────────────────────┐
│                  策略层 (Strategies)               │
│         只依赖抽象接口，不感知底层是纸面还是实盘      │
├──────────────────────────────────────────────────┤
│                  网关抽象层 (BaseGateway)           │
│         定义 connect / send_order / cancel_order   │
│         query_positions / on_tick 等标准接口        │
├──────────────────────────────────────────────────┤
│              PaperGateway  │  未来: QmtGateway      │
│         本地模拟撮合       │  迅投 QMT API 对接      │
├──────────────────────────────────────────────────┤
│                  核心层 (Core)                     │
│         EventEngine + Pydantic 数据模型            │
└──────────────────────────────────────────────────┘
```

**依赖倒置**：上层（策略）只依赖抽象（BaseGateway），底层实现可以替换。

---

## 2. 系统架构总览

```
                         ┌────────────────┐
                         │    main.py     │  ← 编排调度 & 测试入口
                         └───────┬────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              │                  │                  │
              ▼                  ▼                  ▼
     ┌────────────┐    ┌──────────────┐    ┌──────────────┐
     │ EventEngine│    │ PaperGateway │    │   Strategy   │
     │ (pub/sub)  │◄──▶│ (模拟撮合)    │◄──▶│  (策略基类)   │
     └────────────┘    └──────────────┘    └──────────────┘
              ▲                  │
              │                  │ 产生/消费
              ▼                  ▼
     ┌────────────────────────────────────┐
     │          Pydantic 数据模型          │
     │  Tick / Order / Trade / Position   │
     └────────────────────────────────────┘
```

**核心思想**：所有模块通过 `EventEngine` 解耦。Gateway 产生事件，Strategy 消费事件并生成信号，信号再通过 Gateway 执行。

---

## 3. 目录结构

```
quant_trading/                     # 项目根目录
│
├── core/                          # ★ 核心层（无外部依赖）
│   ├── __init__.py                #    导出所有模型 + EventEngine
│   ├── models.py                  #    Pydantic 数据模型（Tick/Order/Trade/Position/Account/Event）
│   └── event_engine.py            #    asyncio.Queue 发布-订阅事件引擎
│
├── gateways/                      # ★ 网关层（交易执行通道）
│   ├── __init__.py                #    导出 BaseGateway + PaperGateway
│   ├── base.py                    #    抽象基类：定义标准交易接口
│   └── paper_gateway.py           #    纸面交易网关（A股规则撮合 + T+1 + 风控）
│
├── strategies/                    # ★ 策略层（用户编写策略）
│   ├── __init__.py
│   └── base.py                    #    策略基类（接收事件，产生信号）
│
├── app/                           # ★ 应用层（未来 UI / Web 接口预留）
│   └── __init__.py
│
├── config/                        # 配置（可选，后续添加）
│   └── settings.py
│
├── main.py                        # 主入口 & 集成测试
├── requirements.txt               # 依赖清单
├── DESIGN.md                      # 本设计文档
└── README.md
```

---

## 4. 核心模块设计

### 4.1 数据模型 (`core/models.py`)

所有模型使用 **Pydantic v2** 定义，自带类型校验。

#### 4.1.1 枚举定义

> ⚠️ **致命陷阱**: 枚举值字符串末尾**严禁出现空格**。`"BUY"` ≠ `"BUY "`，一个空格字符就会导致所有状态判断 `== OrderStatus.FILLED` 返回 `False`。以下代码已经过 hex 级审查，确保每个字符串值的引号内零空格。

```python
# ⚠️ 重要: 所有枚举值字符串末尾严禁出现空格。
class OrderSide(str, Enum):
    BUY = "BUY"      # 买入
    SELL = "SELL"    # 卖出

class OrderStatus(str, Enum):
    SUBMITTED = "SUBMITTED"    # 已提交，等待成交
    PARTIAL = "PARTIAL"        # 部分成交
    FILLED = "FILLED"          # 全部成交
    CANCELLED = "CANCELLED"    # 已撤销（含超时自动撤单）
    REJECTED = "REJECTED"      # 已拒绝（资金不足/涨跌停/手数不对/T+1/非交易时间）

class OrderType(str, Enum):
    LIMIT = "LIMIT"  # 限价单（A股主流）

class EventType(str, Enum):
    TICK = "EVENT_TICK"          # 行情 Tick
    ORDER = "EVENT_ORDER"        # 订单状态变更
    TRADE = "EVENT_TRADE"        # 成交回报
    POSITION = "EVENT_POSITION"  # 持仓变动
    ACCOUNT = "EVENT_ACCOUNT"    # 账户变动
    LOG = "EVENT_LOG"            # 日志事件
```

#### 4.1.2 核心模型

> ⚠️ **精度约定**: 所有价格、金额、费用字段统一使用 `Decimal` 类型，通过 Pydantic 的 `condecimal(max_digits=18, decimal_places=4)` 约束精度。严禁使用 `float` 进行金融计算——`0.1 + 0.2 = 0.30000000000000004` 会导致资金对账失败。

```
StandardTick
├── symbol: str                     # 标的代码，如 "000001.SZ"
├── timestamp: datetime             # 时间戳
├── last_price: Decimal             # 最新价（精度 4 位）
├── high_limit: Decimal             # ★ 涨停价（由行情源提供，避免网关硬编码涨跌停比例）
├── low_limit: Decimal              # ★ 跌停价
├── volume: int                     # 成交量
├── bid_prices: List[Decimal]       # 买五价 (bid_1 ~ bid_5)
├── bid_volumes: List[int]          # 买五量
├── ask_prices: List[Decimal]       # 卖五价
└── ask_volumes: List[int]          # 卖五量

StandardOrder
├── order_id: str                   # 订单ID，格式 "ORD-000001"
├── symbol: str                     # 标的代码
├── side: OrderSide                 # BUY / SELL
├── order_type: OrderType           # 默认 LIMIT
├── price: Decimal                  # 委托价格 (>0, 精度4位)
├── quantity: int                   # 委托数量 (>0, 买入须为100的整数倍)
├── filled_quantity: int = 0        # 已成交数量
├── status: OrderStatus             # 订单状态
├── submit_time: datetime           # 提交时间
├── update_time: datetime           # 最后更新时间
└── reject_reason: Optional[str]    # 拒绝原因（REJECTED 时填充）

StandardTrade
├── trade_id: str                   # 成交ID，格式 "TRD-000001"
├── order_id: str                   # 关联订单ID
├── symbol: str
├── side: OrderSide
├── price: Decimal                  # 成交价（精度4位）
├── quantity: int                   # 成交量
├── commission: Decimal = 0         # 佣金（Decimal，精度4位）
├── stamp_tax: Decimal = 0          # 印花税（仅卖出，精度4位）
└── trade_time: datetime            # 成交时间

StandardPosition
├── symbol: str
├── total_quantity: int = 0         # 总持仓（股）
├── available_quantity: int = 0     # 可用持仓（总持仓 - T+1锁定 - 挂单冻结）
├── frozen_quantity: int = 0        # 挂单冻结数量
├── t1_locked_quantity: int = 0     # T+1 锁定数量（今日买入）
├── avg_price: Decimal = 0          # 持仓均价（Decimal，精度4位）
├── market_value: Decimal = 0       # 持仓市值（Decimal）
└── unrealized_pnl: Decimal = 0     # 浮动盈亏（Decimal）

StandardAccount
├── total_cash: Decimal = 0         # 总现金（仅在实际成交时变动）
├── buy_frozen: Decimal = 0         # 买入冻结资金（挂单预留，未真正扣除）
├── available_cash: Decimal         # ★ 计算属性 = total_cash - buy_frozen
├── market_value: Decimal = 0       # 持仓总市值
├── total_asset: Decimal            # ★ 计算属性 = total_cash + market_value
├── initial_capital: Decimal = 0    # 初始资金（计算收益率用）
└── positions: Dict[str, StandardPosition]

Event
├── type: EventType                 # 事件类型
├── data: Any = None                # 事件载荷（可以是 Tick/Order/Trade/Position/dict）
└── timestamp: datetime             # 事件产生时间
```

---

### 4.2 事件引擎 (`core/event_engine.py`)

#### 设计要点

- 基于 `asyncio.Queue` 实现异步发布-订阅
- 支持按事件类型注册多个异步回调
- 后台协程循环消费队列，逐个调用注册的 handler
- 单个 handler 异常不影响其他 handler

#### 接口

```python
class EventEngine:
    def register(self, event_type: EventType, handler: Callable) -> None:
        """注册事件处理器（async callable）"""

    def unregister(self, event_type: EventType, handler: Callable) -> None:
        """注销事件处理器"""

    def put(self, event: Event) -> None:
        """向队列投放事件（非阻塞）"""

    async def start(self) -> None:
        """启动事件处理循环（后台 asyncio.Task）"""

    async def stop(self) -> None:
        """停止事件处理循环"""

    async def _process(self) -> None:
        """内部：从 Queue 取事件 → 分发到注册的 handlers"""
```

#### 内部流程

```
put(event)                     _process() 循环
    │                              │
    ▼                              ▼
┌─────────┐                ┌──────────────┐
│ asyncio │  ──── get ───▶ │ 按 event.type │
│ .Queue  │                │ 查找 handlers │
└─────────┘                └──────┬───────┘
                                  │
                    ┌─────────────┼─────────────┐
                    ▼             ▼             ▼
                handler1     handler2      handler3
                (async)      (async)       (async)
```

**关键实现细节**：
- `_process` 中使用 `asyncio.wait_for(self._queue.get(), timeout=1.0)` 实现可中断的阻塞等待
- 每个 handler 用 `try/except` 包裹，确保单个异常不中断分发循环
- `put` 使用 `put_nowait` 避免生产者阻塞

---

### 4.3 抽象网关 (`gateways/base.py`)

```python
class BaseGateway(ABC):
    """
    交易网关抽象基类。
    所有方法均为 async，策略层只依赖此接口。
    未来 QmtGateway 只需实现同样的接口即可无缝替换。
    """

    @abstractmethod
    async def connect(self) -> None:
        """连接交易通道"""

    @abstractmethod
    async def disconnect(self) -> None:
        """断开连接"""

    @abstractmethod
    async def send_order(
        self,
        symbol: str,
        side: OrderSide,
        price: float,
        quantity: int,
        order_type: OrderType = OrderType.LIMIT,
    ) -> str:
        """提交订单，返回 order_id"""

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        """撤销订单，返回是否成功"""

    @abstractmethod
    async def query_positions(self) -> Dict[str, StandardPosition]:
        """查询当前持仓"""

    @abstractmethod
    async def query_account(self) -> StandardAccount:
        """查询账户信息"""

    @abstractmethod
    async def on_tick(self, tick: StandardTick) -> None:
        """接收行情 Tick（由外部行情源驱动）"""
```

---

### 4.4 纸面交易网关 (`gateways/paper_gateway.py`)

这是本阶段**最核心、最复杂的模块**。需要完整模拟 A 股交易规则。

#### 4.4.1 内部状态

```
PaperGateway 内部状态:

_account:
    total_cash: Decimal         # 总现金（仅成交时变动，源账户）
    buy_frozen: Decimal         # 买入冻结资金（订单 SUBMITTED 时预留）
    # available_cash 是计算属性: total_cash - buy_frozen
    initial_capital: Decimal    # 初始资金

_positions: Dict[str, {
    "total": int,               # 总持仓股数
    "t1_locked": int,           # T+1 锁定股数
    "sell_frozen": int,         # 卖出挂单冻结股数
    "avg_price": Decimal,       # 加权均价（Decimal）
}]

_orders: Dict[str, StandardOrder]       # 所有订单
_submit_times: Dict[str, float]         # 订单提交时间（用于超时撤单）
_prev_close: Dict[str, Decimal]         # 前收盘价（用于涨跌停校验）
_last_prices: Dict[str, Decimal]        # 最新价缓存

_counters:
    _order_counter: int                 # 订单号自增
    _trade_counter: int                 # 成交号自增
```

#### 4.4.2 A 股规则常量

```python
COMMISSION_RATE  = Decimal("0.00025")    # 佣金万2.5
MIN_COMMISSION   = Decimal("5.0")        # 最低佣金 5 元
STAMP_TAX_RATE   = Decimal("0.001")      # 印花税千1（仅卖出）
DEFAULT_LOT_SIZE = 100                   # 1手 = 100股
DEFAULT_CANCEL_TIMEOUT = 180             # 超时撤单 180 秒

# ★ 交易时间（A股连续竞价时段，不含集合竞价）
TRADING_SESSIONS = [
    (time(9, 30), time(11, 30)),   # 上午连续竞价
    (time(13, 0),  time(15, 0)),   # 下午连续竞价
]
```

> **注意**: 
> - 常量全部使用 `Decimal` 字符串构造器 `Decimal("0.00025")`，避免 `Decimal(0.00025)` 引入 float 中间态。
> - 涨跌停比例**不再出现于此**：网关从 `StandardTick.high_limit / low_limit` 取值校验，不对板块（主板±10%、创业板/科创板±20%、ST±5%）做硬编码假设。

#### 4.4.3 订单生命周期（状态机）

```
                        ┌──────────┐
                        │ 用户调用  │
                        │ send_order│
                        └─────┬────┘
                              │
                     ┌────────▼────────┐
                     │   订单验证       │
                     │ (validate_order) │
                     └───┬─────────┬───┘
                         │         │
                    验证通过    验证失败
                         │         │
                    ┌────▼───┐ ┌──▼──────┐
                    │SUBMITTED│ │REJECTED │
                    └───┬────┘ └─────────┘
                        │
            ┌───────────┼───────────┐
            │           │           │
       tick触发撮合  超时3分钟   手动撤单
            │           │           │
       ┌────▼───┐  ┌───▼────┐  ┌──▼──────┐
       │ FILLED │  │CANCELLED│  │CANCELLED│
       └────────┘  └────────┘  └────────┘
```

#### 4.4.4 买入撮合流程

```
send_order(symbol, BUY, price, qty)
│
├─ 1. ★ 交易时间校验:
│     current_time = now().time()
│     is_trading = any(start <= current_time <= end for start, end in TRADING_SESSIONS)
│     └─ 否 → REJECTED "非交易时间（A股连续竞价：9:30-11:30, 13:00-15:00）"
│
├─ 2. 手数检查: qty % 100 == 0 ?
│     └─ 否 → REJECTED "买入委托数量必须是100股(1手)的整数倍，当前: {qty}股"
│
├─ 3. 涨跌停检查: 从 StandardTick 取 high_limit / low_limit
│     price > tick.high_limit ?
│     └─ 是 → REJECTED "买入价{price}超过涨停价{tick.high_limit}"
│
├─ 4. 资金校验（Decimal 精确计算）:
│     trade_value = price * qty                           # Decimal
│     commission = max(trade_value * COMMISSION_RATE, MIN_COMMISSION)
│     required_cash = trade_value + commission
│     available = total_cash - buy_frozen                 # ★ 计算可用资金
│     available >= required_cash ?
│     └─ 否 → REJECTED "可用资金不足：需要{required_cash}，可用{available}"
│
├─ 5. ★ 冻结资金（不扣 total_cash，只增加预留）:
│     buy_frozen += required_cash                         # ★ 资金被冻结，但 total_cash 不变
│
├─ 6. 订单 SUBMITTED，记录提交时间
│
└─ 等待 tick 触发撮合 ──▶ on_tick(tick)
      │
      ├─ tick.last_price <= order.price ?
      │     └─ 是 → 撮合成交:
      │           total_cash -= actual_cost               # ★ 真正扣钱
      │           buy_frozen -= required_cash             # ★ 释放冻结
      │           total_position += qty
      │           t1_locked += qty                        # ★ T+1 核心
      │           avg_price 加权更新 (Decimal)
      │           order.status = FILLED
      │           生成 StandardTrade (全部 Decimal)
      │           推送 EVENT_ORDER + EVENT_TRADE + EVENT_POSITION
      │
      └─ 否 → 继续等待 (或超时 → CANCELLED，buy_frozen -= required_cash 释放冻结)
```

#### 4.4.5 卖出撮合流程

> **★ A股零股规则**: 买入必须是 100 股的整数倍，但卖出时——
> - 若持仓 ≥ 100 股：可按整手卖出（100 的整数倍），也可一次性清仓（含零股）
> - 若持仓 < 100 股（零股）：必须一次性全部卖出，不可拆分

```
send_order(symbol, SELL, price, qty)
│
├─ 1. ★ 交易时间校验:
│     current_time = now().time()
│     is_trading = any(start <= current_time <= end for start, end in TRADING_SESSIONS)
│     └─ 否 → REJECTED "非交易时间"
│
├─ 2. ★ 卖出数量校验（含零股逻辑）:
│     total_holding = position.total_quantity
│     available = total_holding - t1_locked - sell_frozen
│     │
│     ├─ available == 0 → REJECTED "可卖数量为0"
│     │
│     ├─ 零股场景: total_holding < 100
│     │     ├─ qty != total_holding → REJECTED "零股必须一次性全部卖出({total_holding}股)"
│     │     └─ qty == total_holding → 通过（清仓）
│     │
│     └─ 正常场景: total_holding >= 100
│           ├─ qty == total_holding → 通过（整仓清仓，含可能的零头）
│           ├─ qty % 100 == 0 且 qty <= available → 通过（整手卖出）
│           └─ 其他 → REJECTED "卖出数量必须是100的整数倍或全部清仓"
│
├─ 3. 涨跌停检查（双向校验）:
│     price < tick.low_limit ?
│     └─ 是 → REJECTED "卖出价{price}低于跌停价{tick.low_limit}"
│     price > tick.high_limit ?
│     └─ 是 → REJECTED "卖出价{price}超过涨停价{tick.high_limit}，疑似策略计算错误"
│
├─ 4. 可用持仓检查:
│     available = total - t1_locked - sell_frozen
│     available >= qty ?
│     └─ 否 → REJECTED "可卖数量不足：需要{qty}股，可用{available}股"
│           （附明细：T+1锁定{t1_locked}股，挂单冻结{sell_frozen}股）
│
├─ 5. 冻结持仓:
│     sell_frozen += qty
│
├─ 6. 订单 SUBMITTED，记录提交时间
│
└─ 等待 tick 触发撮合 ──▶ on_tick(tick)
      │
      ├─ tick.last_price >= order.price ?
      │     └─ 是 → 撮合成交:
      │           sell_frozen -= qty
      │           total_position -= qty
      │           trade_value = price * qty              # Decimal
      │           commission = max(trade_value * COMMISSION_RATE, MIN_COMMISSION)
      │           stamp_tax = trade_value * STAMP_TAX_RATE
      │           net_proceeds = trade_value - commission - stamp_tax  # ★ 印花税仅卖出
      │           total_cash += net_proceeds                            # ★ 资金回到总现金
      │           order.status = FILLED
      │           生成 StandardTrade (含印花税，全部 Decimal)
      │           推送 EVENT_ORDER + EVENT_TRADE + EVENT_POSITION
      │
      └─ 否 → 继续等待 (或超时 → CANCELLED，恢复 sell_frozen)
```

#### 4.4.6 超时撤单

```
异步后台任务 _auto_cancel_loop():
    每 1 秒检查一次 _submit_times
    for order_id, submit_time in _submit_times:
        if 订单状态 == SUBMITTED 且 (now - submit_time) > cancel_timeout:
            → 撤单:
              - 买入单: buy_frozen -= required_cash （释放冻结资金，total_cash 不变）
              - 卖出单: sell_frozen 归还可用持仓
              - order.status = CANCELLED
              - 推送 EVENT_ORDER
```

#### 4.4.7 T+1 结算

```python
def settle_t1(self) -> None:
    """
    模拟次日开盘：将所有 T+1 锁定股数释放为可用。
    实际系统中应由日期变更自动触发。
    """
    for pos in self._positions.values():
        pos["t1_locked"] = 0
```

---

### 4.5 策略基类 (`strategies/base.py`)

```python
class BaseStrategy(ABC):
    """
    策略基类。
    策略不直接操作 Gateway，而是通过 EventEngine 监听事件，
    通过 Gateway 发送订单。
    """

    def __init__(self, event_engine: EventEngine, gateway: BaseGateway):
        ...

    @abstractmethod
    async def on_tick(self, event: Event) -> None:
        """处理行情 Tick，生成交易信号"""

    @abstractmethod
    async def on_order(self, event: Event) -> None:
        """处理订单状态变更"""

    @abstractmethod
    async def on_trade(self, event: Event) -> None:
        """处理成交回报"""

    def start(self) -> None:
        """注册事件监听，启动策略"""

    def stop(self) -> None:
        """注销事件监听，停止策略"""
```

---

## 5. A 股交易规则建模

| # | 规则 | 建模位置 | 实现方式 |
|---|------|---------|---------|
| 1 | **Decimal 精度** | models.py + 全网关 | 所有价格/金额字段使用 `Decimal`（18,4），禁止 float 金融运算 |
| 2 | **T+1 卖出限制** | PaperGateway | 买入成交后 `t1_locked += qty`，可用持仓 = total - t1_locked - sell_frozen |
| 3 | **买入手数限制** | PaperGateway._validate_order | `qty % 100 != 0 → REJECTED`（仅买入方向强制） |
| 4 | **零股卖出规则** | PaperGateway._validate_order | 持仓 < 100 股必须一次性清仓；持仓 ≥ 100 股可整手卖或清仓 |
| 5 | **涨跌停** | PaperGateway._validate_order | 取值 `tick.high_limit / tick.low_limit`，不对板块比例做硬编码 |
| 6 | **交易时间** | PaperGateway._validate_order | 9:30-11:30, 13:00-15:00 之外 → REJECTED "非交易时间" |
| 7 | **佣金万2.5 最低5元** | PaperGateway._fill_order | `max(trade_value * COMMISSION_RATE, MIN_COMMISSION)`（Decimal） |
| 8 | **印花税千1 (仅卖出)** | PaperGateway._fill_order | `trade_value * STAMP_TAX_RATE`（Decimal, SELL only） |
| 9 | **资金冻结（买入）** | PaperGateway.send_order | SUBMITTED → `buy_frozen += required_cash`（total_cash 不变）；FILLED → `total_cash -= actual_cost`, `buy_frozen -= required_cash`；CANCEL → `buy_frozen -= required_cash` |
| 10 | **持仓冻结（卖出）** | PaperGateway.send_order | SUBMITTED → `sell_frozen += qty`；FILLED → `total_position -= qty`, `sell_frozen -= qty`；CANCEL → `sell_frozen -= qty` |
| 11 | **超时撤单** | PaperGateway._auto_cancel_loop | 后台 asyncio 协程，每 1 秒巡检，超时 → CANCELLED + 释放 buy_frozen / sell_frozen |

---

## 6. 事件流与数据流

### 6.1 行情驱动流程

```
外部行情源 (未来: akshare/QMT)
    │
    │ StandardTick
    ▼
PaperGateway.on_tick(tick)
    │
    ├──▶ EventEngine.put(Event(TICK, tick))
    │         │
    │         ▼
    │    Strategy.on_tick(event)    ← 策略计算信号
    │         │
    │         ▼
    │    Gateway.send_order(...)    ← 策略下单
    │         │
    │         ▼
    │    EventEngine.put(Event(ORDER, order))
    │
    ├──▶ 内部撮合检查
    │         │
    │         ▼
    │    _fill_order(order)
    │         │
    │         ├──▶ Event(ORDER, order)      ← 状态变为 FILLED
    │         ├──▶ Event(TRADE, trade)      ← 成交回报
    │         └──▶ Event(POSITION, position) ← 持仓更新
    │
    └──▶ EventEngine.put(Event(ACCOUNT, account))
```

### 6.2 关键时序（一次完整买入）

```
时间轴 ──────────────────────────────────────────────────────▶

[策略]                    [网关]                    [事件引擎]
  │                         │                         │
  │── send_order(BUY) ────▶│                         │
  │                         │── validate ──────────▶  │  (REJECTED 或)
  │                         │── freeze cash           │
  │                         │── Event(ORDER,          │
  │                         │     SUBMITTED) ──────▶  │
  │                         │                         │──▶ Strategy.on_order()
  │                         │                         │
  │                         │◀── on_tick(tick) ──────│  (价格触发)
  │                         │                         │
  │                         │── _fill_order()         │
  │                         │── Event(ORDER, FILLED)─▶│──▶ Strategy.on_order()
  │                         │── Event(TRADE, ...) ──▶ │──▶ Strategy.on_trade()
  │                         │── Event(POSITION, ...)─▶│──▶ Strategy 更新状态
  │                         │                         │
```

---

## 7. 状态机设计

### 7.1 订单状态机

```
                    ┌──────────┐
                    │  初始     │
                    └────┬─────┘
                         │ send_order()
                    ┌────▼─────┐
              ┌─────│  验证    │─────┐
              │     └────┬─────┘     │
         验证失败      验证通过    系统错误
              │          │          │
         ┌────▼──┐  ┌───▼────┐ ┌──▼──────┐
         │REJECTED│  │SUBMITTED│ │REJECTED │
         └────────┘  └───┬───┘ └────────┘
                         │
            ┌────────────┼────────────┐
            │            │            │
        tick 撮合    超时 3min    cancel_order()
            │            │            │
       ┌────▼──┐   ┌────▼───┐  ┌────▼───┐
       │ FILLED │   │CANCELLED│  │CANCELLED│
       └────────┘   └────────┘  └────────┘
```

### 7.2 持仓状态模型

```
持仓 = total_quantity 股
       │
       ├── t1_locked:  今日买入，明日才可卖
       ├── sell_frozen: 已挂卖单，被冻结
       └── available:   实际可卖 = total - t1_locked - sell_frozen

买入成交后: total += qty, t1_locked += qty
卖出成交后: total -= qty, sell_frozen -= qty
次日结算后: t1_locked = 0 (全部释放)
```

---

## 8. 测试场景

`main.py` 将覆盖以下测试用例：

| # | 场景 | 预期结果 |
|---|------|---------|
| 1 | 正常买入 1000 股 @ 10.00 | SUBMITTED → (tick=10.00, high_limit=11.00) → FILLED, total=1000, available=0 (T+1) |
| 2 | 资金不足买入 | REJECTED，reason 含 "资金不足" |
| 3 | 买入手数非 100 整数倍 (如 150 股) | REJECTED，reason 含 "100股" |
| 4 | 价格超涨停 (price=11.00, high_limit=10.90) | REJECTED，reason 含 "涨停价" |
| 5 | 卖出 T+1 锁定股 | REJECTED，reason 含 "T+1锁定" |
| 6 | 零股清仓 (持有 50 股，卖 50 股) | 通过 → SUBMITTED → FILLED（允许清仓） |
| 7 | 零股拆卖 (持有 50 股，试图卖 30 股) | REJECTED，reason 含 "零股必须一次性全部卖出" |
| 8 | 整仓清仓 (持有 350 股，卖 350 股) | 通过 → FILLED（允许含零头清仓） |
| 9 | 非交易时间发单 (如 12:00) | REJECTED，reason 含 "非交易时间" |
| 10 | settle_t1 后卖出 | 正常 SUBMITTED → FILLED，扣除印花税 |
| 11 | 超时撤单 | SUBMITTED → (3分钟) → CANCELLED，资金/持仓解冻 |
| 12 | 多个并发订单 | 各自独立冻结，互不干扰 |
| 13 | Decimal 精度验证 | 佣金计算 0.1+0.2 不会出现浮点误差 |

---

## 9. 未来扩展规划

### 9.1 QMT 网关对接

```
PaperGateway                       QmtGateway
─────────────                      ──────────
本地撮合                           调用 xtquant SDK
mock 成交价                        xt_trader.order_stock_async()
内存状态                           券商柜台真实回报
```

只需实现 `BaseGateway` 的 6 个抽象方法，策略代码无需任何修改。

### 9.2 后续模块（不在本期范围）

| 模块 | 职责 |
|------|------|
| `data_fetcher/` | akshare → SQLite 数据管道 |
| `strategy_engine/` | 向量化回测引擎 |
| `risk_manager/` | 仓位限制 + 回撤熔断 |
| `notifier/` | 钉钉/企微推送 |
| `app/` | FastAPI Web 界面 |

---

> **下一步**: 按此设计文档逐模块实现代码。先 `core/models.py` → `core/event_engine.py` → `gateways/base.py` → `gateways/paper_gateway.py` → `strategies/base.py` → `main.py`。
