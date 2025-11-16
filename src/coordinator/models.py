"""
Data models for LinearJC coordinator.
"""
from typing import List, Dict, Optional
from pydantic import BaseModel, Field, field_validator
from pathlib import Path

from coordinator.security_utils import validate_job_id, validate_registry_key


class JobSchedule(BaseModel):
    """Job scheduling configuration."""
    min_daily: int = Field(ge=1, le=288, description="Minimum executions per 24h (max: every 5 min)")
    max_daily: int = Field(ge=1, le=288, description="Maximum executions per 24h (max: every 5 min)")

    @field_validator('max_daily')
    @classmethod
    def validate_max_ge_min(cls, v: int, info) -> int:
        """Ensure max_daily >= min_daily."""
        if 'min_daily' in info.data and v < info.data['min_daily']:
            raise ValueError(f"max_daily ({v}) must be >= min_daily ({info.data['min_daily']})")
        return v


class JobExecutor(BaseModel):
    """Executor configuration for a job."""
    user: str = Field(description="Unix user to run job as")
    timeout: int = Field(default=3600, ge=1, description="Timeout in seconds")


class Job(BaseModel):
    """Job definition."""
    id: str = Field(description="Unique job identifier")
    version: str = Field(description="Job version (semver)")
    depends_on: List[str] = Field(default_factory=list, description="List of job IDs this depends on")
    schedule: JobSchedule
    executor: JobExecutor
    inputs: Dict[str, str] = Field(default_factory=dict, description="Input name -> registry key")
    outputs: Dict[str, str] = Field(default_factory=dict, description="Output name -> registry key")

    # Metadata (not in YAML, added during loading)
    file_path: Optional[Path] = Field(default=None, exclude=True, description="Source file path")

    @field_validator('id')
    @classmethod
    def validate_id(cls, v: str) -> str:
        """Validate job ID format for security."""
        from coordinator.security_utils import validate_job_id
        return validate_job_id(v)

    @field_validator('depends_on')
    @classmethod
    def validate_depends_on(cls, v: List[str]) -> List[str]:
        """Validate dependency job IDs."""
        from coordinator.security_utils import validate_job_id
        return [validate_job_id(dep_id) for dep_id in v]

    @field_validator('inputs', 'outputs')
    @classmethod
    def validate_registry_refs(cls, v: Dict[str, str]) -> Dict[str, str]:
        """Validate registry key references."""
        from coordinator.security_utils import validate_registry_key
        # Validate registry keys (values)
        for name, key in v.items():
            validate_registry_key(key)
        return v

    def __hash__(self):
        """Make Job hashable for use in sets/dicts."""
        return hash(self.id)

    def __eq__(self, other):
        """Jobs are equal if they have the same ID."""
        if not isinstance(other, Job):
            return False
        return self.id == other.id


class JobFile(BaseModel):
    """Root structure of a job YAML file."""
    job: Job


class DataRegistryEntry(BaseModel):
    """Entry in the data registry."""
    type: str = Field(description="Storage type: filesystem or minio")
    # Filesystem fields
    path: Optional[str] = None
    path_type: Optional[str] = Field(
        default=None,
        description="Path type for filesystem: 'file' or 'directory'"
    )
    readable: Optional[bool] = True
    writable: Optional[bool] = True
    # Minio fields
    bucket: Optional[str] = None
    prefix: Optional[str] = None
    retention_days: Optional[int] = None
    # Note: All transfers use tar.gz automatically - no configuration needed

    @field_validator('type')
    @classmethod
    def validate_type(cls, v: str) -> str:
        """Validate storage type."""
        if v not in ['filesystem', 'minio']:
            raise ValueError(f"Invalid type: {v}. Must be 'filesystem' or 'minio'")
        return v

    @field_validator('path')
    @classmethod
    def validate_path_field(cls, v: Optional[str], info) -> Optional[str]:
        """Validate filesystem path for security (basic check)."""
        if v is None:
            return v

        # Basic validation - detailed validation happens at runtime
        # when we know the allowed roots from configuration
        from coordinator.security_utils import validate_path

        # Just do basic safety checks here (no shell chars, etc)
        # Full path validation with allowed_roots happens in Coordinator.load_data_registry()
        try:
            validate_path(v, allowed_roots=None, allow_relative=False, description="registry path")
        except Exception as e:
            raise ValueError(f"Invalid path in registry: {e}")

        return v

    @field_validator('path_type')
    @classmethod
    def validate_path_type(cls, v: Optional[str], info) -> Optional[str]:
        """Validate path_type field."""
        if v is None:
            return v

        # path_type only valid for filesystem type
        entry_type = info.data.get('type')
        if entry_type != 'filesystem':
            raise ValueError(f"path_type only valid for filesystem type, not {entry_type}")

        # Must be 'file' or 'directory'
        if v not in ['file', 'directory']:
            raise ValueError(f"path_type must be 'file' or 'directory', got: {v}")

        return v


class DataRegistry(BaseModel):
    """Data registry mapping logical names to storage locations."""
    registry: Dict[str, DataRegistryEntry]


class JobTree(BaseModel):
    """A linear tree of jobs that must run sequentially."""
    root: Job
    jobs: List[Job]  # Ordered list from root to leaf
    min_daily: int
    max_daily: int
    last_execution: Optional[float] = None  # Unix timestamp
    next_execution: Optional[float] = None  # Unix timestamp
    execution_history: List[float] = Field(default_factory=list, description="Recent execution timestamps (last 25h)")

    class Config:
        arbitrary_types_allowed = True

    def record_execution(self, timestamp: float) -> None:
        """
        Record a successful execution and prune old entries.

        Keeps only executions from the last 25 hours to answer
        "executions in last 24h" questions.

        Args:
            timestamp: Unix timestamp of execution
        """
        self.execution_history.append(timestamp)

        # Prune entries older than 25 hours
        cutoff = timestamp - (25 * 3600)
        self.execution_history = [t for t in self.execution_history if t > cutoff]

    def get_executions_last_24h(self, now: Optional[float] = None) -> int:
        """
        Count executions in the last 24 hours.

        Args:
            now: Current timestamp (defaults to time.time())

        Returns:
            Number of executions in last 24h
        """
        if now is None:
            import time
            now = time.time()

        cutoff = now - (24 * 3600)
        return sum(1 for t in self.execution_history if t > cutoff)

    def is_within_bounds(self, now: Optional[float] = None) -> bool:
        """
        Check if execution frequency is within min/max daily bounds.

        Args:
            now: Current timestamp (defaults to time.time())

        Returns:
            True if within bounds, False otherwise
        """
        count_24h = self.get_executions_last_24h(now)
        return self.min_daily <= count_24h <= self.max_daily

    def get_boundary_status(self, now: Optional[float] = None) -> str:
        """
        Get human-readable boundary status.

        Args:
            now: Current timestamp (defaults to time.time())

        Returns:
            Status string like "OK (2/2-4)" or "UNDER (0/2-4)" or "OVER (5/2-4)"
        """
        count_24h = self.get_executions_last_24h(now)

        if count_24h < self.min_daily:
            return f"UNDER ({count_24h}/{self.min_daily}-{self.max_daily})"
        elif count_24h > self.max_daily:
            return f"OVER ({count_24h}/{self.min_daily}-{self.max_daily})"
        else:
            return f"OK ({count_24h}/{self.min_daily}-{self.max_daily})"

    def __repr__(self):
        job_chain = " → ".join([j.id for j in self.jobs])
        return f"JobTree({job_chain}, schedule={self.min_daily}-{self.max_daily}/day)"


class CoordinatorConfig(BaseModel):
    """Coordinator configuration."""
    class MqttConfig(BaseModel):
        broker: str = "localhost"
        port: int = 1883
        keepalive: int = 60

    class MinioConfig(BaseModel):
        endpoint: str = "localhost:9000"
        access_key: str = "minioadmin"
        secret_key: str = "minioadmin"
        secure: bool = False
        temp_bucket: str = "linearjc-temp"

    class SchedulingConfig(BaseModel):
        loop_interval: int = 10
        message_max_age: int = 60

    class SigningConfig(BaseModel):
        shared_secret: str

    class LoggingConfig(BaseModel):
        level: str = Field(
            default="INFO",
            description="Global log level: DEBUG, INFO, WARNING, ERROR"
        )
        file: Optional[str] = Field(
            default=None,
            description="Optional log file path (in addition to stdout)"
        )
        json_format: bool = Field(
            default=False,
            description="Output logs in JSON format for machine parsing"
        )
        # Per-module log levels (optional)
        module_levels: Dict[str, str] = Field(
            default_factory=dict,
            description="Per-module log levels, e.g. {'mqtt_client': 'DEBUG', 'minio_manager': 'WARNING'}"
        )

    class SecurityConfig(BaseModel):
        """Security configuration."""
        allowed_data_roots: List[str] = Field(
            default_factory=lambda: ["/data", "/tmp/linearjc"],
            description="Allowed root directories for data registry filesystem paths"
        )
        validate_secrets: bool = Field(
            default=True,
            description="Validate secrets are not default/weak values at startup"
        )

    class ArchiveConfig(BaseModel):
        format: str = Field(
            default="tar.gz",
            description="Archive format for data transfers (only tar.gz supported in v0.1)"
        )

        @field_validator('format')
        @classmethod
        def validate_format(cls, v: str) -> str:
            """Validate archive format - only tar.gz supported currently."""
            if v != "tar.gz":
                raise ValueError(
                    f"Unsupported archive format: {v}. "
                    f"Only 'tar.gz' is supported in v0.1. "
                    f"Future versions will support tar.bz2, tar.xz, tar.zst."
                )
            return v

    mqtt: MqttConfig
    minio: MinioConfig
    jobs_dir: str
    data_registry: str
    work_dir: str = Field(
        default="/var/lib/linearjc/work",
        description="Working directory for temporary files during job execution"
    )
    scheduling: SchedulingConfig
    signing: SigningConfig
    logging: LoggingConfig
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    archive: ArchiveConfig = Field(default_factory=ArchiveConfig)


class CoordinatorConfigFile(BaseModel):
    """Root structure of coordinator config file."""
    coordinator: CoordinatorConfig
