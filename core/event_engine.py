"""
core/event_engine.py — 异步事件引擎

基于 asyncio.Queue 的发布-订阅模式，核心职责:
  - 解耦各模块：Gateway 产生事件，Strategy 消费事件
  - 按 EventType 路由到注册的异步 handler
  - 单个 handler 异常不影响其他 handler 和引擎运行

参考: DESIGN.md v0.2.1 — 4.2 事件引擎
"""

import asyncio
import logging
from typing import Callable, Coroutine, Dict, List, Optional, Set

from .models import Event, EventType

logger = logging.getLogger(__name__)

# handler 签名: async def handler(event: Event) -> None
Handler = Callable[[Event], Coroutine]


class EventEngine:
    """
    异步发布-订阅事件引擎。

    使用示例:
        engine = EventEngine()
        engine.register(EventType.TICK, my_tick_handler)
        await engine.start()
        engine.put(Event(type=EventType.TICK, data=tick))
        await engine.stop()
    """

    # 队列无界 (maxsize=0)，避免生产者因背压阻塞
    DEFAULT_QUEUE_SIZE = 0

    # _process 循环中 get() 的超时秒数，保证 stop() 能及时响应
    GET_TIMEOUT = 1.0

    def __init__(self, queue_size: int = DEFAULT_QUEUE_SIZE) -> None:
        """
        Args:
            queue_size: 事件队列最大长度。0 表示无界。
        """
        self._queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=queue_size)

        # 按事件类型索引的 handler 集合，用 dict 存 identity→handler 以支持精确 unregister
        self._handlers: Dict[EventType, Dict[int, Handler]] = {
            et: {} for et in EventType
        }

        self._running = False
        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # 公共接口
    # ------------------------------------------------------------------

    def register(self, event_type: EventType, handler: Handler) -> None:
        """
        注册异步事件处理器。

        同一个 handler 重复注册仅保留一次（幂等）。

        Args:
            event_type: 关注的事件类型。
            handler:    async callable，签名为 async def handler(event: Event) -> None。
        """
        self._handlers[event_type][id(handler)] = handler
        logger.debug(
            "注册 handler: type=%s handler=%s (共 %d 个)",
            event_type.value,
            getattr(handler, "__name__", handler),
            len(self._handlers[event_type]),
        )

    def unregister(self, event_type: EventType, handler: Handler) -> None:
        """
        注销事件处理器。

        Args:
            event_type: 事件类型。
            handler:    已注册的 handler。
        """
        self._handlers[event_type].pop(id(handler), None)
        logger.debug(
            "注销 handler: type=%s handler=%s (剩余 %d 个)",
            event_type.value,
            getattr(handler, "__name__", handler),
            len(self._handlers[event_type]),
        )

    def put(self, event: Event) -> None:
        """
        向事件队列投放事件（非阻塞）。

        若队列已满（仅在有界队列时可能），记录错误并丢弃事件，
        保证生产者不被阻塞。

        Args:
            event: Event 实例。
        """
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.error(
                "事件队列已满 (maxsize=%d)，丢弃事件: type=%s",
                self._queue.maxsize,
                event.type.value,
            )

    async def start(self) -> None:
        """
        启动事件处理循环。

        创建一个后台 asyncio.Task 运行 _process()。
        幂等：重复调用不会创建多个处理循环。
        """
        if self._running:
            logger.warning("EventEngine 已在运行中")
            return

        self._running = True
        self._task = asyncio.create_task(self._process())
        logger.info("EventEngine 已启动")

    async def stop(self, drain: bool = True) -> None:
        """
        停止事件处理循环。

        Args:
            drain: True 则停止前处理完队列中所有待处理事件。
        """
        if not self._running:
            return

        self._running = False

        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        if drain:
            await self._drain()

        logger.info("EventEngine 已停止")

    # ------------------------------------------------------------------
    # 查询接口
    # ------------------------------------------------------------------

    def handler_count(self, event_type: Optional[EventType] = None) -> int:
        """
        返回已注册 handler 数量。

        Args:
            event_type: 指定类型则只统计该类型，None 则统计全部。
        """
        if event_type is not None:
            return len(self._handlers[event_type])
        return sum(len(h) for h in self._handlers.values())

    @property
    def is_running(self) -> bool:
        return self._running

    # ------------------------------------------------------------------
    # 内部方法
    # ------------------------------------------------------------------

    async def _process(self) -> None:
        """
        后台事件处理循环。

        从 Queue 取事件 → 按 type 分发 → 并发调用所有注册 handler。
        使用 asyncio.wait_for 实现可中断的 get()，保证 stop() 能及时响应。
        """
        while self._running:
            try:
                event = await asyncio.wait_for(
                    self._queue.get(),
                    timeout=self.GET_TIMEOUT,
                )
            except asyncio.TimeoutError:
                # 超时仅意味着队列暂时为空，继续循环检查 _running
                continue
            except asyncio.CancelledError:
                break

            # 分发到该事件类型的所有 handler（创建独立 task 隔离异常）
            handlers = list(self._handlers[event.type].values())
            if handlers:
                # gather 并发执行，return_exceptions=True 保证单个异常不中断
                results = await asyncio.gather(
                    *(self._safe_call(h, event) for h in handlers),
                    return_exceptions=True,
                )
                for r in results:
                    if isinstance(r, Exception):
                        logger.error("handler 执行异常: %s", r, exc_info=r)
            else:
                logger.debug("事件 type=%s 无 handler，跳过", event.type.value)

    async def _safe_call(self, handler: Handler, event: Event) -> None:
        """
        安全调用单个 handler，异常直接抛出（由 gather 捕获）。
        """
        await handler(event)

    async def _drain(self) -> None:
        """
        处理完队列中所有剩余事件。
        """
        count = 0
        while not self._queue.empty():
            try:
                event = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            handlers = list(self._handlers[event.type].values())
            for h in handlers:
                try:
                    await h(event)
                except Exception:
                    logger.exception("drain handler 异常")
            count += 1
        if count:
            logger.info("drain 阶段处理了 %d 个剩余事件", count)
