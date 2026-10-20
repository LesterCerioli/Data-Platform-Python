from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass


@dataclass
class SparkSubmitCommand:
    spark_submit_path: str
    spark_conf: dict[str, str]
    app_args: list[str]
    entrypoint: str
    main_class: str | None = None
    jars: list[str] | None = None
    packages: list[str] | None = None
    py_files: list[str] | None = None
    files: list[str] | None = None
    driver_class_path: str | None = None
    executor_class_path: str | None = None
    master: str | None = None
    deploy_mode: str | None = None
    queue: str | None = None
    verbose: bool = False


@dataclass
class SparkProcessResult:
    return_code: int
    stdout_path_s3_key: str | None
    stderr_path_s3_key: str | None
    stdout_lines: int
    stderr_lines: int


class SparkExecutor(ABC):
    @abstractmethod
    def build_command(self, cmd: SparkSubmitCommand) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def run_and_stream(
        self,
        cmd: SparkSubmitCommand,
        cluster_id: str,
        step_id: str,
        on_stdout_line: Callable[[str], None] | None = None,
        on_stderr_line: Callable[[str], None] | None = None,
    ) -> SparkProcessResult:
        raise NotImplementedError