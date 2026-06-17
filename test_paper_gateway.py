"""
PaperGateway 全覆盖集成测试
"""
import asyncio
from decimal import Decimal

from core.event_engine import EventEngine
from core.models import EventType, OrderSide, OrderStatus, StandardTick
from gateways.paper_gateway import PaperGateway, _round, MIN_COMMISSION, COMMISSION_RATE


async def main():
    engine = EventEngine()
    gw = PaperGateway(
        engine,
        initial_cash=Decimal("100000"),
        cancel_timeout=999,
        bypass_trading_hours=True,
    )
    await engine.start()
    await gw.connect()

    gw.set_price_limits("000001.SZ", Decimal("11.00"), Decimal("9.00"))

    # 事件收集器
    events = {et: [] for et in EventType}

    async def collector(event):
        events[event.type].append(event)

    for et in EventType:
        engine.register(et, collector)

    async def wait():
        await asyncio.sleep(0.1)

    def last_ev(et):
        lst = events[et]
        return lst[-1].data if lst else None

    # ===== Test 1: 正常买入 =====
    print("=== Test 1: 正常买入 1000 股 @ 10.00 ===")
    await gw.send_order("000001.SZ", OrderSide.BUY, 10.0, 1000)
    await wait()
    order = last_ev(EventType.ORDER)
    assert order.status == OrderStatus.SUBMITTED
    assert gw._buy_frozen > 0
    print(f"  SUBMITTED: buy_frozen={gw._buy_frozen}")

    await gw.on_tick(StandardTick(
        symbol="000001.SZ", last_price=Decimal("10.00"),
        high_limit=Decimal("11.00"), low_limit=Decimal("9.00"), volume=100000,
    ))
    await wait()
    order2 = last_ev(EventType.ORDER)
    trade = last_ev(EventType.TRADE)
    assert order2.status == OrderStatus.FILLED
    assert trade.side == OrderSide.BUY
    pos = gw._positions["000001.SZ"]
    assert pos["total"] == 1000
    assert pos["t1_locked"] == 1000
    print(f"  FILLED: total=1000 t1_locked=1000 buy_frozen={gw._buy_frozen}")
    print("  PASS\n")

    # ===== Test 2: 资金不足 =====
    print("=== Test 2: 资金不足买入 ===")
    await gw.send_order("000001.SZ", OrderSide.BUY, 10.0, 100000)
    await wait()
    order = last_ev(EventType.ORDER)
    assert order.status == OrderStatus.REJECTED
    assert "资金不足" in order.reject_reason
    print(f"  REJECTED: {order.reject_reason[:60]}")
    print("  PASS\n")

    # ===== Test 3: 手数非整百 =====
    print("=== Test 3: 买入手数 150 股 === ")
    await gw.send_order("000001.SZ", OrderSide.BUY, 10.0, 150)
    await wait()
    order = last_ev(EventType.ORDER)
    assert order.status == OrderStatus.REJECTED
    assert "100股" in order.reject_reason
    print(f"  REJECTED: {order.reject_reason}")
    print("  PASS\n")

    # ===== Test 4: 超涨停 =====
    print("=== Test 4: 买入价 12.00 > 涨停 11.00 ===")
    await gw.send_order("000001.SZ", OrderSide.BUY, 12.0, 1000)
    await wait()
    order = last_ev(EventType.ORDER)
    assert order.status == OrderStatus.REJECTED
    assert "涨停" in order.reject_reason
    print(f"  REJECTED: {order.reject_reason}")
    print("  PASS\n")

    # ===== Test 5: 卖出 T+1 锁定 =====
    print("=== Test 5: 卖出 T+1 锁定股 ===")
    await gw.send_order("000001.SZ", OrderSide.SELL, 10.5, 500)
    await wait()
    order = last_ev(EventType.ORDER)
    assert order.status == OrderStatus.REJECTED
    assert "T+1" in order.reject_reason
    print(f"  REJECTED: {order.reject_reason}")
    print("  PASS\n")

    # ===== Test 6: settle_t1 后卖出 =====
    print("=== Test 6: settle_t1 后卖出 500 股 ===")
    gw.settle_t1()
    assert gw._positions["000001.SZ"]["t1_locked"] == 0
    cash_before = gw._total_cash
    await gw.send_order("000001.SZ", OrderSide.SELL, 10.5, 500)
    await wait()
    order = last_ev(EventType.ORDER)
    assert order.status == OrderStatus.SUBMITTED
    assert gw._positions["000001.SZ"]["sell_frozen"] == 500

    await gw.on_tick(StandardTick(
        symbol="000001.SZ", last_price=Decimal("10.50"),
        high_limit=Decimal("11.00"), low_limit=Decimal("9.00"), volume=100000,
    ))
    await wait()
    order2 = last_ev(EventType.ORDER)
    trade = last_ev(EventType.TRADE)
    assert order2.status == OrderStatus.FILLED
    assert trade.side == OrderSide.SELL
    assert trade.stamp_tax > 0
    print(f"  FILLED: cash {cash_before} -> {gw._total_cash} 印花税={trade.stamp_tax}")
    print("  PASS\n")

    # ===== Test 7: 零股清仓 =====
    print("=== Test 7: 零股清仓 (持有 50 卖 50) ===")
    gw.set_price_limits("000002.SZ", Decimal("5.50"), Decimal("4.50"))
    gw._ensure_position("000002.SZ")
    gw._positions["000002.SZ"] = {
        "total": 50, "t1_locked": 0, "sell_frozen": 0, "avg_price": Decimal("5.0"),
    }
    await gw.send_order("000002.SZ", OrderSide.SELL, 5.0, 50)
    await wait()
    order = last_ev(EventType.ORDER)
    assert order.status == OrderStatus.SUBMITTED, f"零股清仓应通过: {order.reject_reason}"
    await gw.on_tick(StandardTick(
        symbol="000002.SZ", last_price=Decimal("5.00"),
        high_limit=Decimal("5.50"), low_limit=Decimal("4.50"), volume=50000,
    ))
    await wait()
    assert "000002.SZ" not in gw._positions or gw._positions["000002.SZ"]["total"] == 0
    print("  零股 50 股清仓通过")
    print("  PASS\n")

    # ===== Test 8: 零股拆卖拒绝 =====
    print("=== Test 8: 持有 50 股，拆卖 30 股 → REJECTED ===")
    gw.set_price_limits("000003.SZ", Decimal("8.80"), Decimal("7.20"))
    gw._ensure_position("000003.SZ")
    gw._positions["000003.SZ"] = {
        "total": 50, "t1_locked": 0, "sell_frozen": 0, "avg_price": Decimal("8.0"),
    }
    await gw.send_order("000003.SZ", OrderSide.SELL, 8.0, 30)
    await wait()
    order = last_ev(EventType.ORDER)
    assert order.status == OrderStatus.REJECTED
    assert "全部卖出" in order.reject_reason or "一次性" in order.reject_reason
    print(f"  REJECTED: {order.reject_reason}")
    print("  PASS\n")

    # ===== Test 9: 撤单解冻 =====
    print("=== Test 9: 撤单解冻 ===")
    bf_before = gw._buy_frozen
    oid = await gw.send_order("000001.SZ", OrderSide.BUY, 9.5, 1000)
    await wait()
    assert gw._buy_frozen > bf_before
    ok = await gw.cancel_order(oid)
    await wait()
    assert ok
    assert gw._buy_frozen == bf_before
    order = last_ev(EventType.ORDER)
    assert order.status == OrderStatus.CANCELLED
    print(f"  撤单成功: buy_frozen 恢复为 {gw._buy_frozen}")
    print("  PASS\n")

    # ===== Test 10: Decimal 精度 =====
    print("=== Test 10: Decimal 精度验证 ===")
    # 1000 股 × 10 元 = 10000 元成交额
    tv = Decimal("1000") * Decimal("10")
    c = _round(tv * Decimal("0.00025"))
    assert c == Decimal("2.5000"), f"佣金计算应为 2.5，实际 {c}"
    # 万2.5 = 2.5 元 < 最低 5 元，实际收取 5 元
    actual = max(_round(tv * COMMISSION_RATE), MIN_COMMISSION)
    assert actual == Decimal("5"), f"最低佣金应为 5，实际 {actual}"
    print("  10000 成交额 → 佣金 2.5 → 实收 min=5 OK")
    print("  PASS\n")

    await gw.disconnect()
    await engine.stop()
    print("=== ALL 10 TESTS PASSED ===")


if __name__ == "__main__":
    asyncio.run(main())
