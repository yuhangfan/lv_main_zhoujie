"""
gateways/base.py — 交易网关抽象基类

所有交易网关（纸面模拟 / 迅投 QMT / 其他券商）必须实现此接口。
策略层只依赖 BaseGateway，不感知底层是模拟还是实盘，
从而达成"零修改切换"。

参考: DESIGN.md v0.2.1 — 4.3 抽象网关
"""

from abc import ABC, abstractmethod
from typing import Dict

from core.models import (
    OrderSide,
    OrderType,
    StandardAccount,
    StandardPosition,
    StandardTick,
)


class BaseGateway(ABC):
    """
    交易网关抽象基类。

    每个抽象方法的签名已固定。子类需实现全部 7 个方法。
    未来对接 QMT / XTP / 华泰等券商，只需新建一个子类并实现这些方法。
    """

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    @abstractmethod
    async def connect(self) -> None:
        """
        连接交易通道。

        PaperGateway:  初始化内部状态、启动自动撤单定时器。
        QmtGateway:    调用 xtquant 的 connect()。
        """
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """
        断开交易通道。

        PaperGateway:  停止自动撤单定时器、清理资源。
        QmtGateway:    调用 xtquant 的 disconnect()。
        """
        ...

    # ------------------------------------------------------------------
    # 订单操作
    # ------------------------------------------------------------------

    @abstractmethod
    async def send_order(
        self,
        symbol: str,
        side: OrderSide,
        price: float,
        quantity: int,
        order_type: OrderType = OrderType.LIMIT,
    ) -> str:
        """
        提交订单。

        Args:
            symbol:     标的代码，如 "000001.SZ"。
            side:       买卖方向。
            price:      委托价格。
            quantity:   委托数量（股）。
            order_type: 订单类型，默认限价单。

        Returns:
            order_id: 订单编号（无论 REJECTED 还是 SUBMITTED 都有 ID）。
        """
        ...

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        """
        撤销订单。

        Args:
            order_id: 要撤销的订单 ID。

        Returns:
            True 表示撤单请求已受理，False 表示订单不存在或不可撤销。
        """
        ...

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    @abstractmethod
    async def query_positions(self) -> Dict[str, StandardPosition]:
        """
        查询当前所有持仓。

        Returns:
            {symbol: StandardPosition} 字典。
        """
        ...

    @abstractmethod
    async def query_account(self) -> StandardAccount:
        """
        查询当前账户信息。

        Returns:
            StandardAccount，含 total_cash / buy_frozen / available_cash /
            market_value / total_asset / positions。
        """
        ...

    # ------------------------------------------------------------------
    # 行情驱动
    # ------------------------------------------------------------------

    @abstractmethod
    async def on_tick(self, tick: StandardTick) -> None:
        """
        接收行情 Tick，驱动内部撮合（PaperGateway）或仅做缓存（QmtGateway）。

        Args:
            tick: StandardTick 实例。
        """
        ...
