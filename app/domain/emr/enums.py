from enum import Enum


class ClusterType(str, Enum):
    TRANSIENT = "TRANSIENT"
    PERSISTENT = "PERSISTENT"


class ClusterStatus(str, Enum):
    CREATING = "CREATING"
    RUNNING = "RUNNING"
    TERMINATING = "TERMINATING"
    TERMINATED = "TERMINATED"
    FAILED = "FAILED"


class StepType(str, Enum):
    SPARK_SUBMIT = "SPARK_SUBMIT"
    JAR = "JAR"
    COMMAND = "COMMAND"


class StepStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class ApplicationType(str, Enum):
    SPARK = "SPARK"
    HIVE = "HIVE"
    TEZ = "TEZ"
    PIG = "PIG"