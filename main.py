import time
import threading
from typing import Optional, Dict, Any, Union


class RateLimiter:
    MIN_SLEEP = 0.0001

    def __init__(self, rate_per_second: Union[float, int], capacity: Union[float, int]):
        self._validate_constructor_args(rate_per_second, capacity)

        self.rate = float(rate_per_second)
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.last_refill = time.monotonic()
        self.total_checks = 0
        self.total_denied = 0
        self.total_grants = 0
        self.total_wait_loops = 0
        self.created_at = time.monotonic()
        self._lock = threading.Lock()

    def _validate_constructor_args(self, rate_per_second, capacity):
        if rate_per_second is None:
            raise ValueError("Rate must be defined")
        if capacity is None:
            raise ValueError("Capacity must be defined")

        try:
            rate_per_second = float(rate_per_second)
            capacity = float(capacity)
        except (TypeError, ValueError):
            raise ValueError("Rate and capacity must be numeric")

        if rate_per_second <= 0:
            raise ValueError("Rate must be positive")
        if capacity <= 0:
            raise ValueError("Capacity must be positive")

    def _validate_count(self, count: Union[float, int]) -> float:
        if count is None:
            raise ValueError("Count must be defined")

        try:
            count = float(count)
        except (TypeError, ValueError):
            raise ValueError("Count must be numeric")

        if count <= 0:
            raise ValueError("Count must be positive")

        if count > self.capacity:
            raise ValueError(
                f"Requested token count ({count}) exceeds bucket capacity ({self.capacity})"
            )

        return count

    def _validate_max_wait(self, max_wait_seconds: Optional[float]) -> Optional[float]:
        if max_wait_seconds is None:
            return None

        try:
            max_wait_seconds = float(max_wait_seconds)
        except (TypeError, ValueError):
            raise ValueError("Max wait time must be numeric")

        if max_wait_seconds < 0:
            raise ValueError("Max wait time cannot be negative")

        return max_wait_seconds

    def _clamp_tokens(self):
        if self.tokens < 0:
            self.tokens = 0.0
        elif self.tokens > self.capacity:
            self.tokens = self.capacity

    def _refill(self):
        now = time.monotonic()

        if now <= self.last_refill:
            return

        elapsed = now - self.last_refill
        self.tokens += elapsed * self.rate
        self.last_refill = now
        self._clamp_tokens()

    def _try_consume_locked(self, count: float) -> bool:
        self._refill()
        self.total_checks += 1

        if self.tokens >= count:
            self.tokens -= count
            self._clamp_tokens()
            self.total_grants += 1
            return True

        self.total_denied += 1
        return False

    def consume(self, count: float = 1) -> bool:
        count = self._validate_count(count)

        with self._lock:
            return self._try_consume_locked(count)

    def allow_request(self) -> bool:
        return self.consume(1)

    def allow_requests(self, count: float) -> bool:
        return self.consume(count)

    def wait_for_tokens(
        self,
        count: float = 1,
        max_wait_seconds: Optional[float] = None,
    ) -> bool:
        count = self._validate_count(count)
        max_wait_seconds = self._validate_max_wait(max_wait_seconds)
        start = time.monotonic()

        while True:
            with self._lock:
                if self._try_consume_locked(count):
                    return True

                if self.tokens >= count:
                    wait_time = 0.0
                else:
                    wait_time = (count - self.tokens) / self.rate

            if max_wait_seconds is not None:
                elapsed = time.monotonic() - start

                if elapsed >= max_wait_seconds:
                    return False

                remaining = max_wait_seconds - elapsed
                wait_time = min(wait_time, remaining)

                if wait_time <= 0:
                    return False

            if wait_time > 0:
                with self._lock:
                    self.total_wait_loops += 1

                time.sleep(max(wait_time, self.MIN_SLEEP))

    def get_available_tokens(self) -> float:
        with self._lock:
            self._refill()
            return self.tokens

    def get_available_tokens_int(self) -> int:
        return int(self.get_available_tokens())

    def get_capacity(self) -> float:
        return self.capacity

    def get_rate(self) -> float:
        return self.rate

    def get_wait_time(self, count: float = 1) -> float:
        count = self._validate_count(count)

        with self._lock:
            self._refill()

            if self.tokens >= count:
                return 0.0

            return (count - self.tokens) / self.rate

    def get_statistics(self) -> Dict[str, Any]:
        with self._lock:
            self._refill()

            return {
                "rate": self.rate,
                "capacity": self.capacity,
                "tokens": self.tokens,
                "available_tokens_int": int(self.tokens),
                "total_checks": self.total_checks,
                "total_grants": self.total_grants,
                "total_denied": self.total_denied,
                "total_wait_loops": self.total_wait_loops,
                "created_at": self.created_at,
                "uptime": time.monotonic() - self.created_at,
            }

    def reset(self):
        with self._lock:
            self.tokens = self.capacity
            self.last_refill = time.monotonic()
            self.total_checks = 0
            self.total_denied = 0
            self.total_grants = 0
            self.total_wait_loops = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


if __name__ == "__main__":
    limiter = RateLimiter(5, 10)

    print("Rate limiter created with rate 5/sec, capacity 10")
    print(f"Initial tokens: {limiter.get_available_tokens()}\n")

    for i in range(1, 13):
        if limiter.allow_request():
            print(f"Request {i}: Allowed")
        else:
            wait = limiter.get_wait_time(1)
            print(f"Request {i}: Rate limited. Need to wait {wait:.3f} seconds")

    print(f"\nAvailable tokens after loop: {limiter.get_available_tokens_int()}")

    print("\nWaiting 2 seconds...")
    time.sleep(2)

    print(f"After sleep, available tokens: {limiter.get_available_tokens():.3f}")

    if limiter.allow_requests(3):
        print("Bulk request for 3 tokens: Allowed")
    else:
        wait = limiter.get_wait_time(3)
        print(f"Bulk request for 3 tokens: Need to wait {wait:.3f} seconds")

    stats = limiter.get_statistics()

    print("\nStatistics:")
    print(f"  Total checks: {stats['total_checks']}")
    print(f"  Total grants: {stats['total_grants']}")
    print(f"  Total denied: {stats['total_denied']}")
    print(f"  Total wait loops: {stats['total_wait_loops']}")
    print(f"  Raw tokens: {stats['tokens']:.3f}")
    print(f"  Integer tokens: {stats['available_tokens_int']}")

    print("\nResetting limiter...")
    limiter.reset()
    print(f"After reset - Available tokens: {limiter.get_available_tokens():.3f}")

    print("\nTrying to request 20 tokens exceeds capacity of 10:")
    try:
        limiter.allow_requests(20)
    except ValueError as e:
        print(f"  Error caught: {e}")

    print("\nTesting wait_for_tokens with timeout:")
    if limiter.wait_for_tokens(8, 1):
        print("  Got 8 tokens within 1 second")
    else:
        print("  Failed to get 8 tokens within 1 second")

    print(f"\nFinal tokens: {limiter.get_available_tokens():.3f}")
