"""
gateways/paper_gateway.py — A 股纸面交易网关

完整模拟 A 股交易规则:
  - T+1 卖出限制（今日买入，明日才可卖）
  - 买入 100 股整数倍，卖出零股清仓特例
  - 涨跌停校验（取值 tick.high_limit / low_limit，不硬编码比例）
  - 交易时间校验（9:30-11:30, 13:00-15:00）
  - 佣金万 2.5（最低 5 元）、印花税千 1（仅卖出）
  - 资金冻结模型（total_cash / buy_frozen 分离）
  - 超时自动撤单（后台 asyncio 协程）

参考: DESIGN.md v0.2.1 — 4.4 纸面交易网关
"""

import asyncio
import logging
import time
from datetime import datetime, time as dtime
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Tuple

from .base import BaseGateway
from core.models import (
    Event,
    EventType,
    OrderSide,
    OrderStatus,
    OrderType,
    StandardAccount,
    StandardOrder,
    StandardPosition,
    StandardTick,
    StandardTrade,
)

logger = logging.getLogger(__name__)


# ============================================================================
# A 股规则常量
# ============================================================================

# 佣金：万 2.5，最低 5 元
COMMISSION_RATE = Decimal("0.00025")
MIN_COMMISSION = Decimal("5")

# 印花税：千 1，仅卖出时收取
STAMP_TAX_RATE = Decimal("0.001")

# 1 手 = 100 股
DEFAULT_LOT_SIZE = 100

# 超时撤单（秒）
DEFAULT_CANCEL_TIMEOUT = 180

# 交易时间（连续竞价时段）
TRADING_SESSIONS: List[Tuple[dtime, dtime]] = [
    (dtime(9, 30), dtime(11, 30)),
    (dtime(13, 0), dtime(15, 0)),
]

# Decimal 量化精度
QUANT = Decimal("0.0001")


def _round(v: Decimal) -> Decimal:
    """四舍五入到 4 位小数"""
    return v.quantize(QUANT, rounding=ROUND_HALF_UP)


# ============================================================================
# PaperGateway
# ============================================================================


class PaperGateway(BaseGateway):
    """
    A 股纸面交易网关。

    在内存中模拟完整的交易撮合，所有 A 股规则（T+1、涨跌停、
    手数限制、零股清仓、交易时间、佣金印花税）在此层实现。
    策略层通过 BaseGateway 接口操作，不感知纸面/实盘差异。
    """

    def __init__(
        self,
        event_engine,
        initial_cash: Decimal = Decimal("1000000"),
        cancel_timeout: int = DEFAULT_CANCEL_TIMEOUT,
        bypass_trading_hours: bool = False,
    ) -> None:
        """
        Args:
            event_engine:  EventEngine 实例，用于推送事件。
            initial_cash:  初始资金（Decimal 字符串或 Decimal 对象）。
            cancel_timeout: 超时撤单秒数，默认 180。
            bypass_trading_hours: True 则跳过交易时间校验（测试用）。
        """
        self._event_engine = event_engine
        self._cancel_timeout = cancel_timeout
        self._bypass_trading_hours = bypass_trading_hours

        # ── 账户 ──────────────────────────────────────────
        self._total_cash = _round(Decimal(str(initial_cash)))
        self._buy_frozen = Decimal("0")
        self._initial_capital = self._total_cash

        # ── 持仓 {symbol: {total, t1_locked, sell_frozen, avg_price}} ─
        self._positions: Dict[str, dict] = {}

        # ── 订单 & 成交 ───────────────────────────────────
        self._orders: Dict[str, StandardOrder] = {}
        self._submit_times: Dict[str, float] = {}
        self._order_counter = 0
        self._trade_counter = 0

        # ── 行情缓存 ─────────────────────────────────────
        self._last_prices: Dict[str, Decimal] = {}
        self._high_limits: Dict[str, Decimal] = {}
        self._low_limits: Dict[str, Decimal] = {}

        # ── 运行状态 ─────────────────────────────────────
        self._connected = False
        self._cancel_task: Optional[asyncio.Task] = None

    # ==================================================================
    # 生命周期
    # ==================================================================

    async def connect(self) -> None:
        """连接网关，启动自动撤单后台任务。"""
        self._connected = True
        self._cancel_task = asyncio.create_task(self._auto_cancel_loop())
        self._push_event(EventType.LOG, {"msg": "纸面交易网关已连接"})
        logger.info(
            "PaperGateway 已连接 | 初始资金=%s | 撤单超时=%ds",
            self._initial_capital,
            self._cancel_timeout,
        )

    async def disconnect(self) -> None:
        """断开网关，停止后台任务。"""
        self._connected = False
        if self._cancel_task is not None:
            self._cancel_task.cancel()
            try:
                await self._cancel_task
            except asyncio.CancelledError:
                pass
            self._cancel_task = None
        self._push_event(EventType.LOG, {"msg": "纸面交易网关已断开"})
        logger.info("PaperGateway 已断开")

    # ==================================================================
    # 行情驱动
    # ==================================================================

    async def on_tick(self, tick: StandardTick) -> None:
        """
        接收行情 Tick，驱动:

        1. 更新行情缓存（last_price / high_limit / low_limit）
        2. 推送 TICK 事件
        3. 遍历待成交订单 → 条件满足则撮合成交
        """
        self._last_prices[tick.symbol] = tick.last_price
        self._high_limits[tick.symbol] = tick.high_limit
        self._low_limits[tick.symbol] = tick.low_limit

        self._push_event(EventType.TICK, tick)

        # 撮合待成交订单
        for order_id, order in list(self._orders.items()):
            if order.status != OrderStatus.SUBMITTED:
                continue
            if order.symbol != tick.symbol:
                continue

            if order.side == OrderSide.BUY and tick.last_price <= order.price:
                await self._fill_order(order)
            elif order.side == OrderSide.SELL and tick.last_price >= order.price:
                await self._fill_order(order)

    # ==================================================================
    # 订单操作
    # ==================================================================

    async def send_order(
        self,
        symbol: str,
        side: OrderSide,
        price: float,
        quantity: int,
        order_type: OrderType = OrderType.LIMIT,
    ) -> str:
        """
        提交订单。完整的 A 股校验链:

        买入: 交易时间 → 手数 → 涨跌停 → 资金 → 冻结 → SUBMITTED
        卖出: 交易时间 → 零股规则 → 涨跌停 → 持仓 → 冻结 → SUBMITTED
        """
        price_d = _round(Decimal(str(price)))
        order_id = f"ORD-{self._order_counter:06d}"
        self._order_counter += 1

        order = StandardOrder(
            order_id=order_id,
            symbol=symbol.strip().upper(),
            side=side,
            order_type=order_type,
            price=price_d,
            quantity=quantity,
        )

        # ── 校验 ──────────────────────────────────────────
        reject_reason = self._validate_order(order)
        if reject_reason is not None:
            order.status = OrderStatus.REJECTED
            order.reject_reason = reject_reason
            order.update_time = datetime.now()
            self._orders[order_id] = order
            self._push_event(EventType.ORDER, order)
            logger.warning("订单被拒: %s | %s", order_id, reject_reason)
            return order_id

        # ── 冻结 ──────────────────────────────────────────
        if side == OrderSide.BUY:
            required = self._calc_buy_required(price_d, quantity)
            self._buy_frozen += required
        else:
            self._positions[symbol]["sell_frozen"] += quantity

        # ── 提交 ──────────────────────────────────────────
        order.status = OrderStatus.SUBMITTED
        self._orders[order_id] = order
        self._submit_times[order_id] = time.time()
        self._push_event(EventType.ORDER, order)
        logger.info(
            "订单已提交: %s %s %s %s股@%s",
            order_id, side.value, symbol, quantity, price_d,
        )
        return order_id

    async def cancel_order(self, order_id: str) -> bool:
        """撤销订单，解冻资金/持仓。"""
        order = self._orders.get(order_id)
        if order is None:
            logger.warning("撤单失败: %s 不存在", order_id)
            return False
        if order.status != OrderStatus.SUBMITTED:
            logger.warning("撤单失败: %s 状态=%s 不可撤销", order_id, order.status.value)
            return False

        order.status = OrderStatus.CANCELLED
        order.update_time = datetime.now()

        # 解冻
        if order.side == OrderSide.BUY:
            required = self._calc_buy_required(order.price, order.quantity)
            self._buy_frozen -= required
        else:
            self._positions[order.symbol]["sell_frozen"] -= order.quantity

        self._submit_times.pop(order_id, None)
        self._push_event(EventType.ORDER, order)
        logger.info("订单已撤销: %s", order_id)
        return True

    # ==================================================================
    # 查询接口
    # ==================================================================

    async def query_positions(self) -> Dict[str, StandardPosition]:
        """返回当前所有持仓。"""
        result: Dict[str, StandardPosition] = {}
        for sym, pos in self._positions.items():
            if pos["total"] == 0:
                continue
            last_price = self._last_prices.get(sym, pos["avg_price"])
            mv = _round(last_price * pos["total"])
            pnl = _round((last_price - pos["avg_price"]) * pos["total"])
            result[sym] = StandardPosition(
                symbol=sym,
                total_quantity=pos["total"],
                frozen_quantity=pos["sell_frozen"],
                t1_locked_quantity=pos["t1_locked"],
                avg_price=pos["avg_price"],
                market_value=mv,
                unrealized_pnl=pnl,
            )
        return result

    async def query_account(self) -> StandardAccount:
        """返回当前账户信息。"""
        positions = await self.query_positions()
        mv = _round(sum(p.market_value for p in positions.values()))
        return StandardAccount(
            total_cash=self._total_cash,
            buy_frozen=self._buy_frozen,
            market_value=mv,
            initial_capital=self._initial_capital,
            positions=positions,
        )

    # ==================================================================
    # 辅助方法（测试/调试用）
    # ==================================================================

    def set_price_limits(
        self, symbol: str, high_limit: Decimal, low_limit: Decimal
    ) -> None:
        """手动设置涨跌停价（测试用，无需 tick 即可校验价格）。"""
        symbol = symbol.strip().upper()
        self._high_limits[symbol] = _round(Decimal(str(high_limit)))
        self._low_limits[symbol] = _round(Decimal(str(low_limit)))

    def settle_t1(self) -> None:
        """模拟次日开盘：释放所有 T+1 锁定股数。"""
        for pos in self._positions.values():
            pos["t1_locked"] = 0
        logger.info("T+1 已结算，所有锁定股数已释放")

    def print_status(self) -> None:
        """控制台打印当前账户状态。"""
        import sys
        acct = asyncio.get_event_loop().run_until_complete(
            self.query_account()
        )
        lines = [
            f"\n{'='*55}",
            f"  总现金:     {acct.total_cash:>14.4f}",
            f"  买入冻结:   {acct.buy_frozen:>14.4f}",
            f"  可用资金:   {acct.available_cash:>14.4f}",
            f"  持仓市值:   {acct.market_value:>14.4f}",
            f"  总资产:     {acct.total_asset:>14.4f}",
            f"  初始资金:   {acct.initial_capital:>14.4f}",
        ]
        if acct.positions:
            lines.append(f"  持仓明细:")
            for sym, p in acct.positions.items():
                lines.append(
                    f"    {sym:<10s} 总{p.total_quantity}股 "
                    f"可用{p.available_quantity}股 "
                    f"T+1锁{p.t1_locked_quantity}股 "
                    f"均价{p.avg_price}"
                )
        lines.append(f"{'='*55}\n")
        sys.stdout.write("\n".join(lines) + "\n")

    # ==================================================================
    # 内部：校验
    # ==================================================================

    def _validate_order(self, order: StandardOrder) -> Optional[str]:
        """
        验证订单是否符合 A 股规则。

        Returns:
            None 表示通过；非 None 字符串表示拒绝原因。
        """
        # ── 0. 交易时间校验 ──────────────────────────────
        if not self._bypass_trading_hours:
            now = datetime.now().time()
            in_session = any(
                start <= now <= end for start, end in TRADING_SESSIONS
            )
            if not in_session:
                return (
                    "非交易时间：A股连续竞价时段为 "
                    "9:30-11:30 和 13:00-15:00"
                )

        # ── 1. 买入手数校验 ──────────────────────────────
        if order.side == OrderSide.BUY:
            if order.quantity % DEFAULT_LOT_SIZE != 0:
                return (
                    f"买入数量必须是{DEFAULT_LOT_SIZE}股(1手)的整数倍，"
                    f"当前: {order.quantity}股"
                )

        # ── 2. 涨跌停校验 ──────────────────────────────
        high = self._high_limits.get(order.symbol)
        low = self._low_limits.get(order.symbol)
        if high is not None and low is not None:
            if order.price > high:
                return f"买入价{order.price}超过涨停价{high}"
            if order.price < low:
                return f"卖出价{order.price}低于跌停价{low}"

        # ── 3. 卖出量 / 资金校验 ──────────────────────────
        if order.side == OrderSide.SELL:
            return self._validate_sell(order)
        else:
            return self._validate_buy(order)

    def _validate_buy(self, order: StandardOrder) -> Optional[str]:
        """买入资金校验。"""
        required = self._calc_buy_required(order.price, order.quantity)
        available = self._total_cash - self._buy_frozen
        if available < required:
            return (
                f"可用资金不足：需要{required}元（含佣金"
                f"{_round(required - order.price * order.quantity)}元），"
                f"可用{available}元"
            )
        return None

    def _validate_sell(self, order: StandardOrder) -> Optional[str]:
        """卖出校验（含零股规则）。"""
        pos = self._positions.get(order.symbol)

        # 无持仓
        if pos is None or pos["total"] == 0:
            return f"无{order.symbol}持仓，无法卖出"

        total_holding = pos["total"]
        t1_locked = pos["t1_locked"]
        sell_frozen = pos["sell_frozen"]
        available = total_holding - t1_locked - sell_frozen

        if available <= 0:
            parts = []
            if t1_locked > 0:
                parts.append(f"{t1_locked}股T+1锁定（今日买入不可卖）")
            if sell_frozen > 0:
                parts.append(f"{sell_frozen}股已挂单冻结")
            return f"可卖数量为0：{'；'.join(parts)}"

        # ── 零股规则 ──────────────────────────────────
        if total_holding < DEFAULT_LOT_SIZE:
            # 零股场景：必须一次性全部卖出
            if order.quantity != total_holding:
                return (
                    f"零股必须一次性全部卖出：持有{total_holding}股"
                    f"（不足{DEFAULT_LOT_SIZE}股），必须全部卖出"
                )
        else:
            # 正常场景：整手卖出 或 全仓清仓
            if order.quantity == total_holding:
                pass  # 整仓清仓允许（含可能的零头）
            elif order.quantity % DEFAULT_LOT_SIZE != 0:
                return (
                    f"卖出数量必须是{DEFAULT_LOT_SIZE}股的整数倍"
                    f"或全部清仓（{total_holding}股），当前: {order.quantity}股"
                )

        # ── 可用数量校验 ──────────────────────────────────
        if order.quantity > available:
            return (
                f"可卖数量不足：需要{order.quantity}股，"
                f"可用{available}股"
                f"（T+1锁定{t1_locked}股，挂单冻结{sell_frozen}股）"
            )

        return None

    # ==================================================================
    # 内部：撮合成交
    # ==================================================================

    async def _fill_order(self, order: StandardOrder) -> None:
        """
        订单成交。

        - 买入: total_cash -= actual_cost, buy_frozen -= required,
                total+=qty, t1_locked+=qty
        - 卖出: total_cash += net_proceeds,
                total-=qty, sell_frozen-=qty
        """
        fill_price = order.price  # 纸面交易以委托价成交
        qty = order.quantity

        if order.side == OrderSide.BUY:
            await self._fill_buy(order, fill_price, qty)
        else:
            await self._fill_sell(order, fill_price, qty)

        order.filled_quantity = qty
        order.status = OrderStatus.FILLED
        order.update_time = datetime.now()
        self._submit_times.pop(order.order_id, None)

        self._push_event(EventType.ORDER, order)

    async def _fill_buy(
        self, order: StandardOrder, fill_price: Decimal, qty: int
    ) -> None:
        """执行买入成交。"""
        trade_value = _round(fill_price * qty)
        commission = max(_round(trade_value * COMMISSION_RATE), MIN_COMMISSION)
        actual_cost = trade_value + commission
        required = self._calc_buy_required(order.price, qty)

        # 扣款
        self._total_cash -= actual_cost
        self._buy_frozen -= required

        # 更新持仓
        self._ensure_position(order.symbol)
        pos = self._positions[order.symbol]
        old_total = pos["total"]
        old_avg = pos["avg_price"]
        new_total = old_total + qty
        if new_total > 0:
            pos["avg_price"] = _round(
                (old_avg * old_total + fill_price * qty) / new_total
            )
        pos["total"] = new_total
        pos["t1_locked"] += qty  # ★ T+1

        # 成交记录
        trade = StandardTrade(
            trade_id=f"TRD-{self._trade_counter:06d}",
            order_id=order.order_id,
            symbol=order.symbol,
            side=OrderSide.BUY,
            price=fill_price,
            quantity=qty,
            commission=commission,
            stamp_tax=Decimal("0"),
        )
        self._trade_counter += 1
        self._push_event(EventType.TRADE, trade)

        logger.info(
            "成交: %s 买入 %s %d股@%s 佣金=%s 剩余现金=%s",
            trade.trade_id, order.symbol, qty, fill_price,
            commission, self._total_cash,
        )

    async def _fill_sell(
        self, order: StandardOrder, fill_price: Decimal, qty: int
    ) -> None:
        """执行卖出成交。"""
        trade_value = _round(fill_price * qty)
        commission = max(_round(trade_value * COMMISSION_RATE), MIN_COMMISSION)
        stamp_tax = _round(trade_value * STAMP_TAX_RATE)
        net_proceeds = trade_value - commission - stamp_tax

        # 入金
        self._total_cash += net_proceeds

        # 更新持仓
        pos = self._positions[order.symbol]
        pos["sell_frozen"] -= qty
        pos["total"] -= qty
        if pos["total"] == 0:
            del self._positions[order.symbol]

        # 成交记录
        trade = StandardTrade(
            trade_id=f"TRD-{self._trade_counter:06d}",
            order_id=order.order_id,
            symbol=order.symbol,
            side=OrderSide.SELL,
            price=fill_price,
            quantity=qty,
            commission=commission,
            stamp_tax=stamp_tax,
        )
        self._trade_counter += 1
        self._push_event(EventType.TRADE, trade)

        logger.info(
            "成交: %s 卖出 %s %d股@%s 佣金=%s 印花税=%s 现金=%s",
            trade.trade_id, order.symbol, qty, fill_price,
            commission, stamp_tax, self._total_cash,
        )

    # ==================================================================
    # 内部：自动撤单
    # ==================================================================

    async def _auto_cancel_loop(self) -> None:
        """
        后台协程：每 1 秒检查超时 SUBMITTED 订单并自动撤单。
        """
        while self._connected:
            await asyncio.sleep(1)
            now = time.time()
            for order_id, submit_time in list(self._submit_times.items()):
                if now - submit_time > self._cancel_timeout:
                    order = self._orders.get(order_id)
                    if order is None or order.status != OrderStatus.SUBMITTED:
                        self._submit_times.pop(order_id, None)
                        continue

                    # 超时撤单
                    order.status = OrderStatus.CANCELLED
                    order.update_time = datetime.now()
                    order.reject_reason = (
                        f"超时未成交（>{self._cancel_timeout}秒）自动撤单"
                    )

                    # 解冻
                    if order.side == OrderSide.BUY:
                        required = self._calc_buy_required(
                            order.price, order.quantity
                        )
                        self._buy_frozen -= required
                    else:
                        pos = self._positions.get(order.symbol)
                        if pos is not None:
                            pos["sell_frozen"] -= order.quantity

                    self._submit_times.pop(order_id, None)
                    self._push_event(EventType.ORDER, order)
                    logger.info("超时撤单: %s", order_id)

    # ==================================================================
    # 内部：工具方法
    # ==================================================================

    def _calc_buy_required(self, price: Decimal, quantity: int) -> Decimal:
        """计算买入所需资金（含佣金）。"""
        trade_value = _round(price * quantity)
        commission = max(_round(trade_value * COMMISSION_RATE), MIN_COMMISSION)
        return trade_value + commission

    def _ensure_position(self, symbol: str) -> None:
        """确保持仓记录存在。"""
        if symbol not in self._positions:
            self._positions[symbol] = {
                "total": 0,
                "t1_locked": 0,
                "sell_frozen": 0,
                "avg_price": Decimal("0"),
            }

    def _push_event(self, event_type: EventType, data) -> None:
        """向事件引擎推送事件。"""
        if isinstance(data, Event):
            event = data
        else:
            event = Event(type=event_type, data=data)
        self._event_engine.put(event)
