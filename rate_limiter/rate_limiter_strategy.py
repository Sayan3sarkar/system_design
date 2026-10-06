from abc import ABC, abstractmethod
from enum import StrEnum, auto
from time import time
from threading import Lock
from dataclasses import dataclass
from collections import deque


def current_time_milliseconds() -> int:
    return int(time() * 1000)


class RateLimiterStrategy(ABC):
    @abstractmethod
    def allow_request(self, user_id: str) -> bool:
        raise NotImplementedError("Can't directly call abstract based class")


class RateLimiterType(StrEnum):
    FIXED_WINDOW = auto()
    SLIDING_WINDOW_LOG = auto()
    SLIDING_WINDOW_COUNTER = auto()
    TOKEN_BUCKET = auto()
    LEAKY_BUCKET = auto()


class FixedWindow(RateLimiterStrategy):
    """
    CONS: Boundary edge case
    Example: allowed requests per minute is 100 -> at 59th second, 100 requests come -> accepted ->
            at 60th second, another 100 requests come -> accepted

        So even though per window validation is correct, but we send 200 requests in 2 seconds effectively
    """

    @dataclass
    class Window:
        window_start_time: int
        request_count: int = 0

    def __init__(self, max_requests: int, window_size_millseconds: int) -> None:
        self._max_requests = max_requests
        self._window_size_millseconds = window_size_millseconds
        self._user_windows: dict[str, FixedWindow.Window] = {}
        self._lock = Lock()  # to keep it thread safe

    def allow_request(self, user_id: str) -> bool:
        with self._lock:
            now = current_time_milliseconds()
            window = self._user_windows.get(user_id) or self.Window(now)

            # Resetting window
            if now - window.window_start_time >= self._window_size_millseconds:
                window.window_start_time = now
                window.request_count = 0

            # set current user window
            self._user_windows[user_id] = window

            if window.request_count < self._max_requests:
                window.request_count += 1
                return True
            return False


class SlidingWindowLog(RateLimiterStrategy):
    """
    FIX: Improvement on fixed window is that the current request coming is compared with relatable window size
    Eg: If my request at 59th second pushes 100 requests, and another 100 requests come at 62nd second, then the window
    is treated as 2 -62, and not 60-62
    CONS: Need to track timestamp of every incoming request per user, that consumes a lot of memory
    """

    def __init__(self, max_requests: int, window_size_millseconds: int) -> None:
        self._max_requests = max_requests
        self._window_size_millseconds = window_size_millseconds
        self._user_request_logs: dict[str, deque[int]] = (
            {}
        )  # the deque holds a list of timestamps
        self._lock = Lock()

    def allow_request(self, user_id: str) -> bool:
        with self._lock:
            now = current_time_milliseconds()
            request_log = self._user_request_logs.get(user_id) or deque()

            # Drop requests outside current window
            while request_log and (
                now - request_log[0] >= self._window_size_millseconds
            ):
                request_log.popleft()

            # set current user request log
            self._user_request_logs[user_id] = request_log
            if len(request_log) < self._max_requests:
                request_log.append(now)  # append current timestamp for given user
                return True

            return False


class SlidingWindowCounter(RateLimiterStrategy):
    """
    Fix: Improvement on Sliding Window Log is that per user, we're not having to store every timestamp
    in a deque. Instead we only store 3 vars: current_window_start, current_count, previous_count. So memory saved

    The idea is that we know how many requests came in the previous window, but we don't know at what time. So the idea
    is to assume that those requests in the previous window were distributed evenly. This helps do an estimation of the
    request count in the previous window (percentage based calculation)

    CONS: We are working on an assumption that the requests got distributed evenly in the previous window. For larger systems,
    this almost certainly proves wrong. Doesn't take into consideration burst requests
    """

    @dataclass
    class Window:
        current_window_start: int
        current_count: int = 0
        previous_count: int = 0

    def __init__(self, max_requests: int, window_size_millseconds: int) -> None:
        self._max_requests = max_requests
        self._window_size_millseconds = window_size_millseconds
        self._user_request_logs: dict[str, SlidingWindowCounter.Window] = {}
        self._lock = Lock()

    def allow_request(self, user_id: str) -> bool:
        with self._lock:
            # TODO: Implement
            return False


class TokenBucket(RateLimiterStrategy):

    @dataclass
    class Bucket:
        tokens: int
        last_refill_timestamp: int

    def __init__(self, bucket_capacity: int, refill_rate: int) -> None:
        self._bucket_capacity = bucket_capacity
        self._refill_rate = refill_rate
        self._user_buckets: dict[str, TokenBucket.Bucket] = {}
        self._lock = Lock()

    def allow_request(self, user_id: str) -> bool:
        with self._lock:
            now = current_time_milliseconds()
            bucket = self._user_buckets.get(user_id) or self.Bucket(
                tokens=self._bucket_capacity, last_refill_timestamp=now
            )

            # 1. Compute elapsed time in seconds since last refill for current user
            time_elapsed_seconds = (now - bucket.last_refill_timestamp) // 1000

            # 2. Update tokens: add new tokens to existing tokens, but cap it at the max capacity
            bucket.tokens = min(
                self._bucket_capacity,
                bucket.tokens + time_elapsed_seconds * self._refill_rate,
            )
            bucket.last_refill_timestamp = now

            # 3. Assign updated bucket to user id
            self._user_buckets[user_id] = bucket

            # 4. If token exists for that bucket, consume the same
            if bucket.tokens >= 1:
                bucket.tokens -= 1
                return True

            return False


class LeakyBucket(RateLimiterStrategy):
    @dataclass
    class Bucket:
        last_leak_timestamp: int
        current_water_level: int = 0

    def __init__(self, bucket_capacity: int, leak_rate: int) -> None:
        self._bucket_capacity = bucket_capacity
        self._leak_rate = leak_rate
        self._user_buckets: dict[str, LeakyBucket.Bucket] = {}
        self._lock = Lock()

    def allow_request(self, user_id: str) -> bool:
        now = current_time_milliseconds()
        bucket = self._user_buckets.get(user_id) or self.Bucket(now)

        elapsed_seconds = (now - bucket.last_leak_timestamp) // 1000
        leaked_amount = elapsed_seconds * self._leak_rate

        bucket.current_water_level = max(0, bucket.current_water_level - leaked_amount)
        bucket.last_leak_timestamp = now

        self._user_buckets[user_id] = bucket

        if bucket.current_water_level < self._bucket_capacity:
            bucket.current_water_level += 1
            return True
        return False
