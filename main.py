#!/usr/bin/env python3

import argparse
import collections
import json
import math
import random
import statistics
import sys
import threading
import time
import unittest
from dataclasses import dataclass
from typing import Any, Dict, Optional, Union


Number = Union[int, float]


@dataclass(eq=False)
class _Waiter:
    count: float
    entered: float
    deadline: Optional[float]
    event: Optional[threading.Event]
    sequence: int


class RateLimiter:
    MIN_SLEEP = 0.0001
    CANCEL_POLL_INTERVAL = 0.05
    EPSILON = 1e-10

    def __init__(self, rate_per_second: Number, capacity: Number):
        self.rate = self._positive_number(rate_per_second, "Rate")
        self.capacity = self._positive_number(capacity, "Capacity")
        self.tokens = self.capacity
        self.last_refill = time.monotonic()
        self.created_at = self.last_refill
        self.total_checks = 0
        self.total_denied = 0
        self.total_grants = 0
        self.total_wait_loops = 0
        self.total_wait_successes = 0
        self.total_timeouts = 0
        self.total_cancellations = 0
        self.total_wait_seconds = 0.0
        self.total_tokens_granted = 0.0
        self.total_reconfigurations = 0
        self.peak_waiters = 0
        self._sequence = 0
        self._queue = collections.deque()
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)

    @staticmethod
    def _positive_number(value: Number, name: str) -> float:
        if value is None or isinstance(value, bool):
            raise ValueError(f"{name} must be a positive finite number")
        try:
            result = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"{name} must be a positive finite number") from exc
        if not math.isfinite(result) or result <= 0:
            raise ValueError(f"{name} must be a positive finite number")
        return result

    @staticmethod
    def _nonnegative_number(value: Number, name: str) -> float:
        if value is None or isinstance(value, bool):
            raise ValueError(f"{name} must be a nonnegative finite number")
        try:
            result = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"{name} must be a nonnegative finite number") from exc
        if not math.isfinite(result) or result < 0:
            raise ValueError(f"{name} must be a nonnegative finite number")
        return result

    def _validate_count(self, count: Number) -> float:
        amount = self._positive_number(count, "Count")
        with self._lock:
            if amount > self.capacity:
                raise ValueError(
                    f"Requested token count ({amount}) exceeds bucket capacity ({self.capacity})"
                )
        return amount

    def _validate_max_wait(self, max_wait_seconds: Optional[Number]) -> Optional[float]:
        if max_wait_seconds is None:
            return None
        return self._nonnegative_number(max_wait_seconds, "Max wait time")

    def _refill_locked(self, now: Optional[float] = None) -> None:
        if now is None:
            now = time.monotonic()
        elapsed = max(0.0, now - self.last_refill)
        if elapsed > 0:
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_refill = now

    def _refill(self) -> None:
        with self._condition:
            self._refill_locked()

    def _enough_locked(self, count: float) -> bool:
        return self.tokens >= count or math.isclose(
            self.tokens, count, rel_tol=1e-12, abs_tol=1e-12
        )

    def _grant_locked(self, count: float) -> None:
        self.tokens = max(0.0, self.tokens - count)
        self.total_grants += 1
        self.total_tokens_granted += count

    def _try_consume_locked(self, count: float) -> bool:
        self._refill_locked()
        self.total_checks += 1
        if not self._queue and self._enough_locked(count):
            self._grant_locked(count)
            return True
        self.total_denied += 1
        return False

    def consume(self, count: Number = 1) -> bool:
        amount = self._validate_count(count)
        with self._condition:
            return self._try_consume_locked(amount)

    def allow_request(self) -> bool:
        return self.consume(1)

    def allow_requests(self, count: Number) -> bool:
        return self.consume(count)

    def wait_for_tokens(
        self,
        count: Number = 1,
        max_wait_seconds: Optional[Number] = None,
        cancel_event: Optional[threading.Event] = None,
    ) -> bool:
        amount = self._validate_count(count)
        timeout = self._validate_max_wait(max_wait_seconds)
        if cancel_event is not None and not isinstance(cancel_event, threading.Event):
            raise TypeError("cancel_event must be a threading.Event")
        entered = time.monotonic()
        deadline = None if timeout is None else entered + timeout
        with self._condition:
            if cancel_event is not None and cancel_event.is_set():
                self.total_cancellations += 1
                return False
            if self._try_consume_locked(amount):
                return True
            if timeout == 0:
                self.total_timeouts += 1
                return False
            self._sequence += 1
            waiter = _Waiter(amount, entered, deadline, cancel_event, self._sequence)
            self._queue.append(waiter)
            self.peak_waiters = max(self.peak_waiters, len(self._queue))
            try:
                while True:
                    now = time.monotonic()
                    self._refill_locked(now)
                    if cancel_event is not None and cancel_event.is_set():
                        self.total_cancellations += 1
                        return False
                    if self._queue[0] is waiter and self._enough_locked(amount):
                        self._grant_locked(amount)
                        self.total_wait_successes += 1
                        self.total_wait_seconds += now - entered
                        return True
                    if deadline is not None and now >= deadline:
                        self.total_timeouts += 1
                        return False
                    if self._queue[0] is waiter:
                        needed = max(0.0, amount - self.tokens)
                        delay = max(self.MIN_SLEEP, needed / self.rate)
                    else:
                        delay = None
                    if deadline is not None:
                        remaining = max(0.0, deadline - now)
                        delay = remaining if delay is None else min(delay, remaining)
                    if cancel_event is not None:
                        delay = self.CANCEL_POLL_INTERVAL if delay is None else min(
                            delay, self.CANCEL_POLL_INTERVAL
                        )
                    self.total_wait_loops += 1
                    self._condition.wait(timeout=delay)
            finally:
                self._queue.remove(waiter)
                self._condition.notify_all()

    def wait_and_consume(
        self,
        count: Number = 1,
        cancel_event: Optional[threading.Event] = None,
    ) -> None:
        if not self.wait_for_tokens(count, cancel_event=cancel_event):
            raise InterruptedError("Token acquisition cancelled")

    def get_available_tokens(self) -> float:
        with self._condition:
            self._refill_locked()
            return self.tokens

    def get_available_tokens_int(self) -> int:
        return int(self.get_available_tokens())

    def get_available_percentage(self) -> float:
        with self._condition:
            self._refill_locked()
            return self.tokens / self.capacity

    def is_full(self) -> bool:
        with self._condition:
            self._refill_locked()
            return math.isclose(self.tokens, self.capacity, rel_tol=1e-12, abs_tol=1e-12)

    def get_capacity(self) -> float:
        with self._condition:
            return self.capacity

    def get_rate(self) -> float:
        with self._condition:
            return self.rate

    def get_wait_time(self, count: Number = 1) -> float:
        amount = self._validate_count(count)
        with self._condition:
            self._refill_locked()
            queued = sum(item.count for item in self._queue)
            deficit = max(0.0, queued + amount - self.tokens)
            return deficit / self.rate

    def reconfigure(
        self,
        rate_per_second: Optional[Number] = None,
        capacity: Optional[Number] = None,
        preserve_fill_ratio: bool = False,
    ) -> None:
        new_rate = None if rate_per_second is None else self._positive_number(rate_per_second, "Rate")
        new_capacity = None if capacity is None else self._positive_number(capacity, "Capacity")
        with self._condition:
            self._refill_locked()
            target_capacity = self.capacity if new_capacity is None else new_capacity
            if self._queue and any(item.count > target_capacity for item in self._queue):
                raise ValueError("New capacity cannot be smaller than an outstanding request")
            ratio = self.tokens / self.capacity
            self.capacity = target_capacity
            if new_rate is not None:
                self.rate = new_rate
            if preserve_fill_ratio:
                self.tokens = min(self.capacity, ratio * self.capacity)
            else:
                self.tokens = min(self.capacity, self.tokens)
            self.total_reconfigurations += 1
            self._condition.notify_all()

    def get_statistics(self) -> Dict[str, Any]:
        with self._condition:
            self._refill_locked()
            now = time.monotonic()
            return {
                "rate": self.rate,
                "capacity": self.capacity,
                "tokens": self.tokens,
                "available_tokens_int": int(self.tokens),
                "available_percentage": self.tokens / self.capacity,
                "total_checks": self.total_checks,
                "total_grants": self.total_grants,
                "total_denied": self.total_denied,
                "total_wait_loops": self.total_wait_loops,
                "total_wait_successes": self.total_wait_successes,
                "total_timeouts": self.total_timeouts,
                "total_cancellations": self.total_cancellations,
                "total_wait_seconds": self.total_wait_seconds,
                "total_tokens_granted": self.total_tokens_granted,
                "total_reconfigurations": self.total_reconfigurations,
                "waiting_threads": len(self._queue),
                "peak_waiters": self.peak_waiters,
                "created_at": self.created_at,
                "uptime": now - self.created_at,
            }

    def reset(self) -> None:
        with self._condition:
            self.tokens = self.capacity
            self.last_refill = time.monotonic()
            self.total_checks = 0
            self.total_denied = 0
            self.total_grants = 0
            self.total_wait_loops = 0
            self.total_wait_successes = 0
            self.total_timeouts = 0
            self.total_cancellations = 0
            self.total_wait_seconds = 0.0
            self.total_tokens_granted = 0.0
            self.total_reconfigurations = 0
            self.peak_waiters = len(self._queue)
            self._condition.notify_all()

    def __repr__(self) -> str:
        with self._condition:
            self._refill_locked()
            return (
                f"RateLimiter(rate={self.rate}, capacity={self.capacity}, "
                f"tokens={self.tokens:.3f}, waiting={len(self._queue)})"
            )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class RateLimiterTests(unittest.TestCase):
    def test_initial_state(self):
        limiter = RateLimiter(5, 10)
        self.assertTrue(limiter.is_full())
        self.assertEqual(limiter.get_capacity(), 10)
        self.assertEqual(limiter.get_rate(), 5)

    def test_consume_and_refill(self):
        limiter = RateLimiter(100, 5)
        self.assertTrue(limiter.consume(5))
        self.assertFalse(limiter.consume(5))
        self.assertTrue(limiter.wait_for_tokens(1, 0.2))

    def test_invalid_values(self):
        for value in [0, -1, float("nan"), float("inf"), -float("inf"), True, None]:
            with self.assertRaises(ValueError):
                RateLimiter(value, 5)
            with self.assertRaises(ValueError):
                RateLimiter(5, value)
        limiter = RateLimiter(1, 5)
        for value in [0, -1, float("nan"), float("inf"), 6, True, None]:
            with self.assertRaises(ValueError):
                limiter.consume(value)

    def test_timeout(self):
        limiter = RateLimiter(1, 1)
        limiter.consume()
        self.assertFalse(limiter.wait_for_tokens(1, 0.01))
        self.assertEqual(limiter.get_statistics()["total_timeouts"], 1)

    def test_cancellation(self):
        limiter = RateLimiter(0.1, 1)
        limiter.consume()
        cancel = threading.Event()
        outcome = []
        thread = threading.Thread(target=lambda: outcome.append(limiter.wait_for_tokens(1, 2, cancel)))
        thread.start()
        time.sleep(0.03)
        cancel.set()
        thread.join(timeout=1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(outcome, [False])
        self.assertEqual(limiter.get_statistics()["waiting_threads"], 0)

    def test_fifo(self):
        limiter = RateLimiter(100, 1)
        limiter.consume()
        order = []
        threads = []
        for index in range(4):
            thread = threading.Thread(
                target=lambda i=index: (limiter.wait_and_consume(), order.append(i))
            )
            thread.start()
            threads.append(thread)
            time.sleep(0.003)
        for thread in threads:
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())
        self.assertEqual(order, [0, 1, 2, 3])

    def test_reconfigure(self):
        limiter = RateLimiter(1, 4)
        limiter.consume(3)
        limiter.reconfigure(rate_per_second=10, capacity=8)
        self.assertEqual(limiter.get_rate(), 10)
        self.assertEqual(limiter.get_capacity(), 8)
        self.assertLess(limiter.get_available_tokens(), 3)

    def test_concurrent_grants(self):
        limiter = RateLimiter(1000, 10)
        results = []
        threads = [
            threading.Thread(target=lambda: results.append(limiter.wait_for_tokens(1, 1)))
            for _ in range(30)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=2)
        self.assertEqual(results, [True] * 30)
        self.assertEqual(limiter.get_statistics()["total_grants"], 30)


def demo(rate: float, capacity: float, requests: int, workers: int, timeout: float) -> int:
    limiter = RateLimiter(rate, capacity)
    results = []
    results_lock = threading.Lock()
    started = time.monotonic()

    def worker(worker_id: int, jobs: int) -> None:
        local = []
        for request_id in range(jobs):
            began = time.monotonic()
            allowed = limiter.wait_for_tokens(1, timeout)
            local.append({
                "worker": worker_id,
                "request": request_id + 1,
                "allowed": allowed,
                "wait_seconds": round(time.monotonic() - began, 6),
            })
        with results_lock:
            results.extend(local)

    counts = [requests // workers] * workers
    for index in range(requests % workers):
        counts[index] += 1
    threads = [
        threading.Thread(target=worker, args=(index + 1, count))
        for index, count in enumerate(counts)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    waits = [item["wait_seconds"] for item in results]
    payload = {
        "configuration": {
            "rate_per_second": rate,
            "capacity": capacity,
            "requests": requests,
            "workers": workers,
            "timeout_seconds": timeout,
        },
        "results": {
            "allowed": sum(item["allowed"] for item in results),
            "denied": sum(not item["allowed"] for item in results),
            "elapsed_seconds": round(time.monotonic() - started, 6),
            "mean_wait_seconds": round(statistics.mean(waits), 6) if waits else 0,
            "max_wait_seconds": max(waits, default=0),
        },
        "statistics": limiter.get_statistics(),
        "requests": sorted(results, key=lambda item: (item["worker"], item["request"])),
    }
    print(json.dumps(payload, indent=2))
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Thread-safe FIFO token-bucket rate limiter")
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--rate", type=float, default=5)
    parser.add_argument("--capacity", type=float, default=10)
    parser.add_argument("--requests", type=int, default=20)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=5)
    args = parser.parse_args(argv)
    if args.test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(RateLimiterTests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    if args.requests < 0 or args.workers < 1:
        parser.error("--requests must be nonnegative and --workers must be positive")
    try:
        rate = RateLimiter._positive_number(args.rate, "Rate")
        capacity = RateLimiter._positive_number(args.capacity, "Capacity")
        timeout = RateLimiter._nonnegative_number(args.timeout, "Timeout")
    except ValueError as exc:
        parser.error(str(exc))
    return demo(rate, capacity, args.requests, args.workers, timeout)


if __name__ == "__main__":
    sys.exit(main())
