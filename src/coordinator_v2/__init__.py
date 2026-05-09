"""
LinearJC Coordinator v2 - Async Architecture

Phase 14.2: Full async rewrite using aiomqtt.

Key improvements over v1:
- Single-threaded async (no locks, no race conditions)
- Event-driven architecture (no polling)
- Explicit state machines (validated transitions)
- Heartbeat-based executor discovery (non-blocking)
- Clean chain continuation (no deadlocks)

Components:
- EventRouter: Pattern-based MQTT message routing
- ExecutorRegistry: Heartbeat-based executor tracking
- JobStateMachine: Explicit job state transitions
- JobTracker: Job execution registry
- TreeValidator: Output conflict detection
- TreeTracker: Chain execution state
- Coordinator: Main async coordinator
"""

__version__ = "2.0.0"
