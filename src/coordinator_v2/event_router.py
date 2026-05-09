"""
EventRouter - Central MQTT message dispatcher.

Routes incoming MQTT messages to registered handlers based on topic patterns.
Supports MQTT wildcard patterns:
- '+' matches a single level (e.g., 'linearjc/heartbeat/+')
- '#' matches multiple levels (e.g., 'linearjc/dev/#')
"""

import logging
import re
from typing import Awaitable, Callable

logger = logging.getLogger(__name__)


class EventRouter:
    """
    Routes MQTT messages to handlers based on topic patterns.

    Single point of message flow - replaces scattered callback lists.
    All handlers are async for consistency with aiomqtt.
    """

    def __init__(self) -> None:
        """Initialize empty handler registry."""
        self._handlers: dict[str, Callable[[str, dict], Awaitable[None]]] = {}
        self._pattern_cache: dict[str, re.Pattern[str]] = {}

    def register(
        self,
        pattern: str,
        handler: Callable[[str, dict], Awaitable[None]],
    ) -> None:
        """
        Register async handler for topic pattern.

        Args:
            pattern: MQTT topic pattern with optional wildcards (+ and #)
            handler: Async callback receiving (topic, payload)

        Raises:
            ValueError: If pattern already registered
        """
        if pattern in self._handlers:
            raise ValueError(f"Pattern already registered: {pattern}")

        self._handlers[pattern] = handler
        self._pattern_cache[pattern] = self._compile_pattern(pattern)
        logger.debug(f"Registered handler for pattern: {pattern}")

    def unregister(self, pattern: str) -> bool:
        """
        Unregister handler for pattern.

        Args:
            pattern: The pattern to unregister

        Returns:
            True if pattern was registered, False otherwise
        """
        if pattern in self._handlers:
            del self._handlers[pattern]
            del self._pattern_cache[pattern]
            logger.debug(f"Unregistered handler for pattern: {pattern}")
            return True
        return False

    async def route(self, topic: str, payload: dict) -> bool:
        """
        Route message to matching handler.

        Args:
            topic: MQTT topic string
            payload: Decoded message payload (dict)

        Returns:
            True if handler found and called, False otherwise
        """
        for pattern, handler in self._handlers.items():
            if self._matches(pattern, topic):
                try:
                    await handler(topic, payload)
                    return True
                except Exception as e:
                    logger.exception(f"Handler error for {pattern}: {e}")
                    return True  # Handler was found, even if it failed

        logger.debug(f"No handler for topic: {topic}")
        return False

    def _matches(self, pattern: str, topic: str) -> bool:
        """
        Check if topic matches pattern.

        Args:
            pattern: MQTT pattern with optional wildcards
            topic: Actual topic string

        Returns:
            True if topic matches pattern
        """
        regex = self._pattern_cache.get(pattern)
        if regex is None:
            regex = self._compile_pattern(pattern)
            self._pattern_cache[pattern] = regex
        return regex.match(topic) is not None

    @staticmethod
    def _compile_pattern(pattern: str) -> re.Pattern[str]:
        """
        Compile MQTT pattern to regex.

        MQTT wildcards:
        - '+' matches exactly one level (no slashes)
        - '#' matches zero or more levels (only at end)

        Args:
            pattern: MQTT topic pattern

        Returns:
            Compiled regex pattern
        """
        # Escape special regex characters except + and #
        escaped = re.escape(pattern)
        # Replace escaped wildcards with regex equivalents
        # \+ becomes [^/]+ (one level, no slashes)
        escaped = escaped.replace(r"\+", r"[^/]+")
        # \# becomes .* (any remaining path)
        escaped = escaped.replace(r"\#", r".*")
        return re.compile(f"^{escaped}$")

    def get_patterns(self) -> list[str]:
        """Get all registered patterns."""
        return list(self._handlers.keys())

    def __len__(self) -> int:
        """Return number of registered handlers."""
        return len(self._handlers)
