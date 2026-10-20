from pydantic import BaseModel, Field

from app.domain.emr.enums import (
    ApplicationType,
    ClusterStatus,
    ClusterType,
    StepStatus,
    StepType,
)


class CreateClusterRequest(BaseModel):
    cluster_name: str = Field(default="emr-cluster", min_length=1, max_length=128)
    cluster_type: ClusterType = Field(default=ClusterType.TRANSIENT)
    applications: list[ApplicationType] = Field(default_factory=lambda: [ApplicationType.SPARK])
    instance_count: int = Field(default=1, ge=0, le=200)
    core_instance_count: int = Field(default=0, ge=0, le=200)
    task_instance_count: int = Field(default=0, ge=0, le=200)
    master_instance_type: str | None = Field(default=None)
    core_instance_type: str | None = Field(default=None)
    task_instance_type: str | None = Field(default=None)
    executor_cores: int = Field(default=2, ge=1, le=128)
    executor_memory: str = Field(default="2g")
    driver_memory: str = Field(default="1g")
    num_executors: int | None = Field(default=None, ge=1, le=1024)
    shuffle_partitions: int = Field(default=200, ge=1, le=10000)
    yarn_memory_overhead: str | None = Field(default=None)
    spark_conf: dict[str, str] = Field(default_factory=dict)
    tags: dict[str, str] = Field(default_factory=dict)
    auto_terminate: bool | None = Field(default=None)
    termination_delay_seconds: int | None = Field(default=None, ge=0, le=300)


class ClusterResponse(BaseModel):
    cluster_id: str
    cluster_name: str
    cluster_type: ClusterType
    cluster_status: ClusterStatus
    applications: list[ApplicationType]
    instance_count: int
    executor_cores: int
    executor_memory: str
    driver_memory: str
    num_executors: int | None
    shuffle_partitions: int
    spark_conf: dict[str, str]
    auto_terminate: bool
    termination_delay_seconds: int
    created_at: str
    updated_at: str
    started_at: str | None = None
    terminated_at: str | None = None
    failure_reason: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)


class ListClustersResponse(BaseModel):
    clusters: list[ClusterResponse]
    count: int


class SparkSubmitStepRequest(BaseModel):
    name: str = Field(default="spark-step", min_length=1, max_length=128)
    entrypoint: str = Field(min_length=1)
    app_args: list[str] = Field(default_factory=list)
    main_class: str | None = Field(default=None)
    jars: list[str] = Field(default_factory=list)
    packages: list[str] = Field(default_factory=list)
    py_files: list[str] = Field(default_factory=list)
    files: list[str] = Field(default_factory=list)
    driver_class_path: str | None = Field(default=None)
    executor_class_path: str | None = Field(default=None)
    master: str | None = Field(default=None)
    deploy_mode: str | None = Field(default=None)
    queue: str | None = Field(default=None)
    verbose: bool = Field(default=False)
    spark_conf_override: dict[str, str] = Field(default_factory=dict)


class JarStepRequest(BaseModel):
    name: str = Field(default="jar-step", min_length=1, max_length=128)
    entrypoint: str = Field(min_length=1)
    app_args: list[str] = Field(default_factory=list)
    main_class: str | None = Field(default=None)
    jars: list[str] = Field(default_factory=list)
    packages: list[str] = Field(default_factory=list)
    py_files: list[str] = Field(default_factory=list)
    files: list[str] = Field(default_factory=list)
    driver_class_path: str | None = Field(default=None)
    executor_class_path: str | None = Field(default=None)
    master: str | None = Field(default=None)
    deploy_mode: str | None = Field(default=None)
    queue: str | None = Field(default=None)
    verbose: bool = Field(default=False)
    spark_conf_override: dict[str, str] = Field(default_factory=dict)


class CommandStepRequest(BaseModel):
    name: str = Field(default="command-step", min_length=1, max_length=128)
    entrypoint: str = Field(min_length=1)
    app_args: list[str] = Field(default_factory=list)
    main_class: str | None = Field(default=None)
    command_args: list[str] = Field(default_factory=list)
    jars: list[str] = Field(default_factory=list)
    packages: list[str] = Field(default_factory=list)
    py_files: list[str] = Field(default_factory=list)
    files: list[str] = Field(default_factory=list)
    driver_class_path: str | None = Field(default=None)
    executor_class_path: str | None = Field(default=None)
    master: str | None = Field(default=None)
    deploy_mode: str | None = Field(default=None)
    queue: str | None = Field(default=None)
    verbose: bool = Field(default=False)
    spark_conf_override: dict[str, str] = Field(default_factory=dict)


class PythonStepRequest(BaseModel):
    name: str = Field(default="python-step", min_length=1, max_length=128)
    entrypoint: str = Field(min_length=1)
    app_args: list[str] = Field(default_factory=list)
    main_class: str | None = Field(default=None)
    py_files: list[str] = Field(default_factory=list)
    files: list[str] = Field(default_factory=list)
    driver_class_path: str | None = Field(default=None)
    executor_class_path: str | None = Field(default=None)
    master: str | None = Field(default=None)
    deploy_mode: str | None = Field(default=None)
    queue: str | None = Field(default=None)
    verbose: bool = Field(default=False)
    spark_conf_override: dict[str, str] = Field(default_factory=dict)


class AddStepRequest(BaseModel):
    step_type: StepType = Field(default=StepType.SPARK_SUBMIT)
    spark_submit: SparkSubmitStepRequest | None = Field(default=None)
    jar_step: JarStepRequest | None = Field(default=None)
    command_step: CommandStepRequest | None = Field(default=None)
    python_step: PythonStepRequest | None = Field(default=None)
    command_args: list[str] = Field(default_factory=list)
    jar_path: str | None = Field(default=None)
    name: str | None = Field(default=None)


class StepResponse(BaseModel):
    step_id: str
    cluster_id: str
    name: str
    step_type: StepType
    step_status: StepStatus
    entrypoint: str | None = None
    main_class: str | None = None
    app_args: list[str] = Field(default_factory=list)
    spark_submit_args_preview: list[str] = Field(default_factory=list)
    stdout_path_s3_key: str | None = None
    stderr_path_s3_key: str | None = None
    stdout_lines: int = 0
    stderr_lines: int = 0
    return_code: int | None = None
    duration_ms: int | None = None
    created_at: str
    updated_at: str
    started_at: str | None = None
    ended_at: str | None = None
    failure_reason: str | None = None
    log_uri: str | None = None
    jars: list[str] = Field(default_factory=list)
    packages: list[str] = Field(default_factory=list)
    py_files: list[str] = Field(default_factory=list)
    files: list[str] = Field(default_factory=list)
    command_args: list[str] = Field(default_factory=list)
    entrypoint_resolved: str | None = None
    step_params: dict[str, object] = Field(default_factory=dict)


class ListStepsResponse(BaseModel):
    steps: list[StepResponse]
    count: int


class ClusterDescriptionResponse(ClusterResponse):
    steps: list[StepResponse] = Field(default_factory=list)
    total_steps: int = 0
    running_steps: int = 0
    terminal_steps: int = 0


class TerminateClusterResponse(BaseModel):
    cluster_id: str
    cluster_status: ClusterStatus
    message: str


class CancelStepResponse(BaseModel):
    step_id: str
    cluster_id: str
    step_status: StepStatus
    message: str


class AddStepResponse(BaseModel):
    step_id: str
    cluster_id: str
    name: str
    step_type: StepType
    step_status: StepStatus
    message: str