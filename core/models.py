"""
core/models.py — A 股量化交易系统标准数据模型

设计原则:
  - 所有价格 / 金额字段统一使用 Decimal（Pydantic 自动将 float/str 转为 Decimal）
  - 枚举值字符串已通过 hex 审查，零尾部空格
  - 可用数量 / 可用资金为计算属性（@computed_field），杜绝数据不一致

参考: DESIGN.md v0.2.1 — 4.1 数据模型
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, computed_field

# ============================================================================
# 枚举定义 — ⚠️ 引号内零尾部空格，已 hex 审查
# ============================================================================


class OrderSide(str, Enum):
    """买卖方向"""
    BUY = "BUY"        # 买入
    SELL = "SELL"      # 卖出


class OrderStatus(str, Enum):
    """订单状态"""
    SUBMITTED = "SUBMITTED"    # 已提交，等待成交
    PARTIAL = "PARTIAL"        # 部分成交
    FILLED = "FILLED"          # 全部成交
    CANCELLED = "CANCELLED"    # 已撤销（含超时自动撤单）
    REJECTED = "REJECTED"      # 已拒绝


class OrderType(str, Enum):
    """订单类型"""
    LIMIT = "LIMIT"  # 限价单（A 股主流）


class EventType(str, Enum):
    """事件类型 — EventEngine 分发依据"""
    TICK = "EVENT_TICK"          # 行情 Tick
    ORDER = "EVENT_ORDER"        # 订单状态变更
    TRADE = "EVENT_TRADE"        # 成交回报
    POSITION = "EVENT_POSITION"  # 持仓变动
    ACCOUNT = "EVENT_ACCOUNT"    # 账户变动
    LOG = "EVENT_LOG"            # 日志事件


# ============================================================================
# 核心数据模型
# ============================================================================


class StandardTick(BaseModel):
    """
    标准行情 Tick。

    涨跌停价 (high_limit / low_limit) 由行情源提供，
    网关不硬编码板块涨跌停比例，天然适配:
      - 主板   ±10%
      - 创业板 ±20%
      - 科创板 ±20%
      - ST 股  ±5%
    """
    symbol: str
    timestamp: datetime = Field(default_factory=datetime.now)
    last_price: Decimal = Field(gt=0)
    high_limit: Decimal = Field(gt=0)
    low_limit: Decimal = Field(gt=0)
    volume: int = Field(ge=0)
    bid_prices: List[Decimal] = Field(default_factory=lambda: [Decimal("0")] * 5)
    bid_volumes: List[int] = Field(default_factory=lambda: [0] * 5)
    ask_prices: List[Decimal] = Field(default_factory=lambda: [Decimal("0")] * 5)
    ask_volumes: List[int] = Field(default_factory=lambda: [0] * 5)


class StandardOrder(BaseModel):
    """
    标准订单。

    A 股约束（由 PaperGateway 校验，不在模型中硬编码）:
      - 买入数量必须为 100 股（1 手）的整数倍
      - 价格必须在 [low_limit, high_limit] 区间
    """
    order_id: str
    symbol: str
    side: OrderSide
    order_type: OrderType = OrderType.LIMIT
    price: Decimal = Field(gt=0)
    quantity: int = Field(gt=0, description="委托数量（股）")
    filled_quantity: int = Field(default=0, ge=0, description="已成交数量")
    status: OrderStatus = OrderStatus.SUBMITTED
    submit_time: datetime = Field(default_factory=datetime.now)
    update_time: datetime = Field(default_factory=datetime.now)
    reject_reason: Optional[str] = Field(default=None, description="拒绝原因（REJECTED 时填充）")


class StandardTrade(BaseModel):
    """
    成交回报。

    费用字段均为 Decimal，杜绝浮点精度丢失。
    stamp_tax 仅在卖出时 > 0。
    """
    trade_id: str
    order_id: str
    symbol: str
    side: OrderSide
    price: Decimal = Field(gt=0, description="成交价")
    quantity: int = Field(gt=0, description="成交数量（股）")
    commission: Decimal = Field(default=Decimal("0"), ge=0, description="佣金")
    stamp_tax: Decimal = Field(default=Decimal("0"), ge=0, description="印花税（仅卖出）")
    trade_time: datetime = Field(default_factory=datetime.now)


class StandardPosition(BaseModel):
    """
    持仓信息。

    关键约束（A 股 T+1）:
      - total_quantity = available_quantity + frozen_quantity + t1_locked_quantity
      - available_quantity: 今日实际可卖数量
      - t1_locked_quantity: 今日买入，T+1 日才可卖
      - frozen_quantity: 已挂卖单，被冻结
    """
    symbol: str
    total_quantity: int = Field(default=0, ge=0, description="总持仓（股）")
    frozen_quantity: int = Field(default=0, ge=0, description="挂单冻结数量")
    t1_locked_quantity: int = Field(default=0, ge=0, description="T+1 锁定数量（今日买入）")
    avg_price: Decimal = Field(default=Decimal("0"), ge=0, description="持仓均价")
    market_value: Decimal = Field(default=Decimal("0"), ge=0, description="持仓市值")
    unrealized_pnl: Decimal = Field(default=Decimal("0"), description="浮动盈亏")

    @computed_field
    @property
    def available_quantity(self) -> int:
        """
        实际可卖数量 = 总持仓 - T+1 锁定 - 挂单冻结。
        计算属性，杜绝数据不一致。
        """
        return self.total_quantity - self.t1_locked_quantity - self.frozen_quantity


class StandardAccount(BaseModel):
    """
    账户信息。

    资金模型（防破产设计）:
      - total_cash: 源账户，仅在实际成交时变动
      - buy_frozen: 买入订单冻结预留
      - available_cash: 计算属性 = total_cash - buy_frozen

    这种设计确保:
      - 挂单撤单只需操作 buy_frozen，total_cash 不受影响
      - 资金对账路径清晰，不会出现 "钱去了哪里" 的困惑
    """
    total_cash: Decimal = Field(default=Decimal("0"), ge=0, description="总现金（仅成交时变动）")
    buy_frozen: Decimal = Field(default=Decimal("0"), ge=0, description="买入冻结资金（挂单预留）")
    market_value: Decimal = Field(default=Decimal("0"), ge=0, description="持仓总市值")
    initial_capital: Decimal = Field(default=Decimal("0"), ge=0, description="初始入金")
    positions: Dict[str, StandardPosition] = Field(default_factory=dict)

    @computed_field
    @property
    def available_cash(self) -> Decimal:
        """可用资金 = 总现金 - 买入冻结"""
        return self.total_cash - self.buy_frozen

    @computed_field
    @property
    def total_asset(self) -> Decimal:
        """总资产 = 总现金 + 持仓市值"""
        return self.total_cash + self.market_value


class Event(BaseModel):
    """
    事件 — EventEngine 的消息载体。

    data 字段为 Any 类型，实际传入可以是:
      - StandardTick / StandardOrder / StandardTrade / StandardPosition
      - dict（日志文本等）
    """
    type: EventType
    data: Any = None
    timestamp: datetime = Field(default_factory=datetime.now)
