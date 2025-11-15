"""
Logging utilities for LinearJC coordinator.

Provides:
- Structured logging with key=value fields
- Correlation ID context management
- Performance timing utilities
- Optional JSON output
"""
import json
import logging
import os
import time
from contextvars import ContextVar
from typing import Dict, Any, Optional
from contextlib import contextmanager


# Context variables for correlation IDs (thread-safe)
_tree_exec_id: ContextVar[Optional[str]] = ContextVar('tree_exec_id', default=None)
_job_exec_id: ContextVar[Optional[str]] = ContextVar('job_exec_id', default=None)
_executor_id: ContextVar[Optional[str]] = ContextVar('executor_id', default=None)


class StructuredFormatter(logging.Formatter):
    """
    Formatter that adds correlation IDs and structured fields.

    Format: [timestamp] [level] [component] correlation_ids key=value message
    """

    def __init__(self, json_output: bool = False):
        """
        Initialize formatter.

        Args:
            json_output: If True, output JSON lines instead of human-readable
        """
        super().__init__()
        self.json_output = json_output

    def format(self, record: logging.LogRecord) -> str:
        """Format log record with correlation IDs and structured fields."""

        if self.json_output:
            return self._format_json(record)
        else:
            return self._format_human(record)

    def _format_human(self, record: logging.LogRecord) -> str:
        """Human-readable format with key=value pairs."""
        # Timestamp in ISO 8601 format
        timestamp = self.formatTime(record, datefmt='%Y-%m-%dT%H:%M:%S')
        # Add milliseconds
        timestamp = f"{timestamp}.{int(record.msecs):03d}Z"

        # Component name (module name)
        component = record.name.split('.')[-1]  # Last part of module name

        # Build correlation ID fields
        corr_parts = []

        tree_exec = _tree_exec_id.get()
        if tree_exec:
            corr_parts.append(f"tree_exec={tree_exec}")

        job_exec = _job_exec_id.get()
        if job_exec:
            corr_parts.append(f"job_exec={job_exec}")

        executor = _executor_id.get()
        if executor:
            corr_parts.append(f"executor={executor}")

        corr_str = " ".join(corr_parts)

        # Build structured fields from 'extra' dict
        extra_fields = []
        if hasattr(record, 'fields') and isinstance(record.fields, dict):
            for key, value in record.fields.items():
                # Format value appropriately
                if isinstance(value, str):
                    extra_fields.append(f"{key}={value}")
                elif isinstance(value, (int, float)):
                    extra_fields.append(f"{key}={value}")
                else:
                    extra_fields.append(f"{key}={str(value)}")

        extra_str = " ".join(extra_fields)

        # Assemble final message
        parts = [
            f"[{timestamp}]",
            f"[{record.levelname}]",
            f"[{component}]"
        ]

        if corr_str:
            parts.append(corr_str)

        if extra_str:
            parts.append(extra_str)

        parts.append(record.getMessage())

        # Add exception info if present
        result = " ".join(parts)
        if record.exc_info:
            result += "\n" + self.formatException(record.exc_info)

        return result

    def _format_json(self, record: logging.LogRecord) -> str:
        """JSON format for machine parsing."""
        timestamp = self.formatTime(record, datefmt='%Y-%m-%dT%H:%M:%S')
        timestamp = f"{timestamp}.{int(record.msecs):03d}Z"

        log_entry = {
            "timestamp": timestamp,
            "level": record.levelname,
            "component": record.name.split('.')[-1],
            "message": record.getMessage()
        }

        # Add correlation IDs
        tree_exec = _tree_exec_id.get()
        if tree_exec:
            log_entry["tree_exec_id"] = tree_exec

        job_exec = _job_exec_id.get()
        if job_exec:
            log_entry["job_exec_id"] = job_exec

        executor = _executor_id.get()
        if executor:
            log_entry["executor_id"] = executor

        # Add structured fields
        if hasattr(record, 'fields') and isinstance(record.fields, dict):
            log_entry.update(record.fields)

        # Add exception if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, separators=(',', ':'))


def set_correlation_ids(
    tree_exec_id: Optional[str] = None,
    job_exec_id: Optional[str] = None,
    executor_id: Optional[str] = None
) -> None:
    """
    Set correlation IDs for current context.

    These IDs will be automatically included in all log messages
    until cleared or updated.

    Args:
        tree_exec_id: Tree execution ID
        job_exec_id: Job execution ID
        executor_id: Executor ID
    """
    if tree_exec_id is not None:
        _tree_exec_id.set(tree_exec_id)
    if job_exec_id is not None:
        _job_exec_id.set(job_exec_id)
    if executor_id is not None:
        _executor_id.set(executor_id)


def clear_correlation_ids() -> None:
    """Clear all correlation IDs from current context."""
    _tree_exec_id.set(None)
    _job_exec_id.set(None)
    _executor_id.set(None)


def get_correlation_ids() -> Dict[str, Optional[str]]:
    """
    Get current correlation IDs.

    Returns:
        Dict with tree_exec_id, job_exec_id, executor_id
    """
    return {
        'tree_exec_id': _tree_exec_id.get(),
        'job_exec_id': _job_exec_id.get(),
        'executor_id': _executor_id.get()
    }


@contextmanager
def correlation_context(
    tree_exec_id: Optional[str] = None,
    job_exec_id: Optional[str] = None,
    executor_id: Optional[str] = None
):
    """
    Context manager for temporary correlation IDs.

    IDs are restored to previous values when context exits.

    Example:
        with correlation_context(tree_exec_id="tree-123", job_exec_id="job-456"):
            logger.info("Processing job")  # IDs automatically included
    """
    # Save current values
    old_tree = _tree_exec_id.get()
    old_job = _job_exec_id.get()
    old_exec = _executor_id.get()

    try:
        # Set new values
        set_correlation_ids(tree_exec_id, job_exec_id, executor_id)
        yield
    finally:
        # Restore old values
        _tree_exec_id.set(old_tree)
        _job_exec_id.set(old_job)
        _executor_id.set(old_exec)


@contextmanager
def log_duration(operation: str, logger: Optional[logging.Logger] = None, **extra_fields):
    """
    Context manager that logs duration of an operation.

    Args:
        operation: Name of operation being timed
        logger: Logger to use (default: root logger)
        **extra_fields: Additional fields to include in log

    Example:
        with log_duration("input_preparation", inputs=2):
            prepare_inputs()
        # Logs: operation=input_preparation duration_ms=1234 inputs=2
    """
    if logger is None:
        logger = logging.getLogger()

    start_time = time.time()

    try:
        yield
    finally:
        duration_ms = int((time.time() - start_time) * 1000)
        fields = {
            'operation': operation,
            'duration_ms': duration_ms,
            **extra_fields
        }
        logger.info(
            f"Operation completed: {operation}",
            extra={'fields': fields}
        )


def log_with_fields(logger: logging.Logger, level: int, message: str, **fields):
    """
    Log a message with structured fields.

    Args:
        logger: Logger instance
        level: Log level (logging.INFO, logging.ERROR, etc.)
        message: Log message
        **fields: Structured fields as keyword arguments

    Example:
        log_with_fields(logger, logging.INFO, "Job starting",
                       state="starting", inputs=2)
    """
    logger.log(level, message, extra={'fields': fields})


def setup_logging(
    level: str = "INFO",
    json_output: bool = False,
    log_file: Optional[str] = None,
    module_levels: Optional[Dict[str, str]] = None
) -> None:
    """
    Setup logging configuration for coordinator.

    Args:
        level: Global log level (DEBUG, INFO, WARNING, ERROR)
        json_output: Enable JSON output instead of human-readable
        log_file: Optional log file path (in addition to stdout)
        module_levels: Per-module log levels, e.g. {'mqtt_client': 'DEBUG'}

    Environment Variables:
        LINEARJC_LOG_LEVEL: Override log level
        LINEARJC_LOG_JSON: Enable JSON output (1 or 0)
    """
    # Check environment variables
    level = os.getenv('LINEARJC_LOG_LEVEL', level).upper()
    json_output = os.getenv('LINEARJC_LOG_JSON', '0') == '1' or json_output

    # Validate level
    if level not in ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'):
        logging.warning(f"Invalid log level '{level}', using INFO")
        level = 'INFO'

    # Create formatter
    formatter = StructuredFormatter(json_output=json_output)

    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level))

    # Remove existing handlers
    root_logger.handlers.clear()

    # Add console handler with structured formatter
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Add file handler if specified
    if log_file:
        try:
            # Create log directory if needed
            log_path = os.path.dirname(log_file)
            if log_path and not os.path.exists(log_path):
                os.makedirs(log_path, mode=0o755)

            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
            logging.info(f"Logging to file: {log_file}")
        except Exception as e:
            logging.error(f"Failed to setup file logging: {e}")

    # Set per-module log levels
    if module_levels:
        for module_name, module_level in module_levels.items():
            module_level_upper = module_level.upper()
            if module_level_upper in ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'):
                # Get logger for coordinator.module_name
                module_logger = logging.getLogger(f'coordinator.{module_name}')
                module_logger.setLevel(getattr(logging, module_level_upper))
                logging.debug(f"Set log level for {module_name}: {module_level_upper}")
            else:
                logging.warning(f"Invalid log level for module {module_name}: {module_level}")

    logging.info(f"Logging initialized: level={level}, json={json_output}, file={log_file}")
