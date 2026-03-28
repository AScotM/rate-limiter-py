import time
import threading
from typing import Optional, Dict, Any, Union

class RateLimiter:
    """
    Token bucket rate limiter with high precision timing.
    
    Tokens are added to the bucket at a fixed rate per second up to a maximum capacity.
    Each request consumes one or more tokens. If insufficient tokens are available,
    the request is denied or delayed.
    """
    
    MIN_SLEEP = 0.0001
    
    def __init__(self, rate_per_second: Union[float, int], capacity: Union[float, int]):
        """
        Initialize a new rate limiter.
        
        Args:
            rate_per_second: Number of tokens added per second
            capacity: Maximum number of tokens the bucket can hold
        
        Raises:
            ValueError: If arguments are invalid
        """
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
        """Validate constructor arguments."""
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
        """Validate token count."""
        if count is None:
            raise ValueError("Count must be defined")
        
        try:
            count = float(count)
        except (TypeError, ValueError):
            raise ValueError("Count must be numeric")
        
        if count <= 0:
            raise ValueError("Count must be positive")
        
        if count > self.capacity:
            raise ValueError(f"Requested token count ({count}) exceeds bucket capacity ({self.capacity})")
        
        return count
    
    def _validate_max_wait(self, max_wait_seconds: Optional[float]) -> Optional[float]:
        """Validate max wait time."""
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
        """Ensure tokens stay within [0, capacity]."""
        if self.tokens < 0:
            self.tokens = 0
        if self.tokens > self.capacity:
            self.tokens = self.capacity
    
    def _refill(self):
        """Add tokens based on time elapsed since last refill."""
        now = time.monotonic()
        
        if now <= self.last_refill:
            if now == self.last_refill:
                return
            self.last_refill = now
            return
        
        time_passed = now - self.last_refill
        tokens_to_add = time_passed * self.rate
        
        if tokens_to_add > 0:
            self.tokens += tokens_to_add
            self.last_refill = now
            self._clamp_tokens()
    
    def _try_consume(self, count: float) -> bool:
        """
        Attempt to consume tokens without waiting.
        
        Args:
            count: Number of tokens to consume
            
        Returns:
            True if tokens were consumed, False otherwise
        """
        with self._lock:
            count = self._validate_count(count)
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
        """
        Attempt to consume tokens.
        
        Args:
            count: Number of tokens to consume (default: 1)
            
        Returns:
            True if tokens were consumed, False otherwise
        """
        return self._try_consume(count)
    
    def allow_request(self) -> bool:
        """Consume 1 token. Returns True if successful, False otherwise."""
        return self.consume(1)
    
    def allow_requests(self, count: float) -> bool:
        """
        Consume count tokens. Returns True if successful, False otherwise.
        
        Args:
            count: Number of tokens to consume
        """
        return self.consume(count)
    
    def wait_for_tokens(self, count: float, max_wait_seconds: Optional[float] = None) -> bool:
        """
        Attempt to consume tokens, waiting if necessary.
        
        Args:
            count: Number of tokens to consume
            max_wait_seconds: Maximum time to wait (None = wait indefinitely)
            
        Returns:
            True if tokens were obtained, False if timeout exceeded
        """
        count = self._validate_count(count)
        max_wait_seconds = self._validate_max_wait(max_wait_seconds)
        
        start = time.monotonic()
        
        while True:
            if self._try_consume(count):
                return True
            
            wait_time = self.get_wait_time(count)
            if wait_time < 0:
                wait_time = 0
            
            if max_wait_seconds is not None:
                elapsed = time.monotonic() - start
                if elapsed >= max_wait_seconds:
                    return False
                
                remaining = max_wait_seconds - elapsed
                if wait_time > remaining:
                    wait_time = remaining
                    if wait_time <= 0:
                        return False
            
            if wait_time > 0:
                self.total_wait_loops += 1
                sleep_time = max(wait_time, self.MIN_SLEEP)
                time.sleep(sleep_time)
    
    def get_available_tokens(self) -> float:
        """Return the current number of available tokens."""
        with self._lock:
            self._refill()
            return self.tokens
    
    def get_available_tokens_int(self) -> int:
        """Return the current number of available tokens truncated to integer."""
        return int(self.get_available_tokens())
    
    def get_capacity(self) -> float:
        """Return the bucket capacity."""
        return self.capacity
    
    def get_rate(self) -> float:
        """Return the token refill rate per second."""
        return self.rate
    
    def get_wait_time(self, count: float = 1) -> float:
        """
        Return the number of seconds needed to accumulate count tokens.
        
        Args:
            count: Number of tokens needed (default: 1)
        """
        count = self._validate_count(count)
        
        with self._lock:
            self._refill()
            
            if self.tokens >= count:
                return 0
            
            return (count - self.tokens) / self.rate
    
    def get_statistics(self) -> Dict[str, Any]:
        """Return usage statistics."""
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
        """Reset the limiter to its initial state."""
        with self._lock:
            self.tokens = self.capacity
            self.last_refill = time.monotonic()
            self.total_checks = 0
            self.total_denied = 0
            self.total_grants = 0
            self.total_wait_loops = 0
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        pass


if __name__ == "__main__":
    limiter = RateLimiter(5, 10)
    
    print(f"Rate limiter created with rate 5/sec, capacity 10")
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
    
    print("\nTrying to request 20 tokens (exceeds capacity of 10):")
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
