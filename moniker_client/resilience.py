"""Resilience utilities: retry with backoff, circuit breaker, batch operations."""

from __future__ import annotations

import logging
import time
import threading
from dataclasses import dataclass, field
from typing import TypeVar, Callable, Any

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RetryExhausted(Exception):
    """All retry attempts failed."""
    def __init__(self, message: str, last_exception: Exception | None = None):
        super().__init__(message)
        self.last_exception = last_exception


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_retries: int = 3
    base_delay_seconds: float = 0.5
    max_delay_seconds: float = 30.0
    exponential_base: float = 2.0
    retryable_status_codes: frozenset[int] = frozenset({429, 502, 503, 504})


def retry_with_backoff(
    func: Callable[..., T],
    config: RetryConfig | None = None,
    *args: Any,
    **kwargs: Any,
) -> T:
    """
    Execute a function with exponential backoff retry.

    Args:
        func: Function to call
        config: Retry configuration
        *args, **kwargs: Arguments to pass to func

    Returns:
        Result of func

    Raises:
        RetryExhausted: If all retries failed
    """
    if config is None:
        config = RetryConfig()

    last_exception: Exception | None = None

    for attempt in range(config.max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exception = e

            # Check if this is a retryable error
            is_retryable = False

            # httpx status code errors
            if hasattr(e, 'response') and hasattr(e.response, 'status_code'):
                if e.response.status_code in config.retryable_status_codes:
                    is_retryable = True

            # Connection errors are always retryable
            error_type = type(e).__name__
            if any(t in error_type for t in ('Connection', 'Timeout', 'Network')):
                is_retryable = True

            if not is_retryable or attempt == config.max_retries:
                raise

            # Calculate delay with exponential backoff + jitter
            delay = min(
                config.base_delay_seconds * (config.exponential_base ** attempt),
                config.max_delay_seconds,
            )
            # Add jitter (up to 25% of delay)
            import random
            delay *= (0.75 + random.random() * 0.5)

            logger.warning(
                f"Retry {attempt + 1}/{config.max_retries} after {delay:.1f}s: {e}"
            )
            time.sleep(delay)

    raise RetryExhausted(
        f"All {config.max_retries} retries exhausted",
        last_exception=last_exception,
    )


class ClientCircuitState:
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class ClientCircuitBreaker:
    """
    Client-side circuit breaker for the moniker service.

    Prevents hammering an unhealthy service with repeated requests.
    """
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    success_threshold: int = 2

    _state: str = field(default=ClientCircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _success_count: int = field(default=0, init=False)
    _opened_at: float = field(default=0.0, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def before_request(self) -> None:
        """Check circuit state before making a request."""
        with self._lock:
            if self._state == ClientCircuitState.CLOSED:
                return

            if self._state == ClientCircuitState.OPEN:
                if time.monotonic() - self._opened_at >= self.recovery_timeout:
                    self._state = ClientCircuitState.HALF_OPEN
                    self._success_count = 0
                    return
                raise ConnectionError(
                    f"Circuit breaker open - service unhealthy. "
                    f"Retry after {self.recovery_timeout - (time.monotonic() - self._opened_at):.0f}s"
                )

    def on_success(self) -> None:
        """Record successful request."""
        with self._lock:
            if self._state == ClientCircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._state = ClientCircuitState.CLOSED
                    self._failure_count = 0
            else:
                self._failure_count = 0

    def on_failure(self) -> None:
        """Record failed request."""
        with self._lock:
            self._failure_count += 1
            if self._state == ClientCircuitState.HALF_OPEN:
                self._state = ClientCircuitState.OPEN
                self._opened_at = time.monotonic()
            elif self._failure_count >= self.failure_threshold:
                self._state = ClientCircuitState.OPEN
                self._opened_at = time.monotonic()
                logger.warning(
                    f"Circuit breaker opened after {self._failure_count} failures"
                )

    @property
    def state(self) -> str:
        return self._state
