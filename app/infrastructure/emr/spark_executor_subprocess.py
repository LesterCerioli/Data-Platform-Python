import os
import subprocess
import tempfile
import threading
from collections.abc import Callable

from app.core.config import settings
from app.infrastructure.aws.s3_data_lake import DataLakeS3Client, get_data_lake_client
from app.infrastructure.emr.interfaces import (
    SparkExecutor,
    SparkProcessResult,
    SparkSubmitCommand,
)


class SubprocessSparkExecutor(SparkExecutor):
    def __init__(self, data_lake: DataLakeS3Client | None = None) -> None:
        self.data_lake = data_lake or get_data_lake_client()

    def build_command(self, cmd: SparkSubmitCommand) -> list[str]:
        parts: list[str] = [cmd.spark_submit_path]

        for k, v in sorted(cmd.spark_conf.items()):
            parts.extend(["--conf", f"{k}={v}"])

        if cmd.master:
            parts.extend(["--master", cmd.master])
        if cmd.deploy_mode:
            parts.extend(["--deploy-mode", cmd.deploy_mode])
        if cmd.queue:
            parts.extend(["--queue", cmd.queue])
        if cmd.main_class:
            parts.extend(["--class", cmd.main_class])
        if cmd.jars:
            parts.extend(["--jars", ",".join(cmd.jars)])
        if cmd.packages:
            parts.extend(["--packages", ",".join(cmd.packages)])
        if cmd.py_files:
            parts.extend(["--py-files", ",".join(cmd.py_files)])
        if cmd.files:
            parts.extend(["--files", ",".join(cmd.files)])
        if cmd.driver_class_path:
            parts.extend(["--driver-class-path", cmd.driver_class_path])
        if cmd.executor_class_path:
            parts.extend(["--executor-class-path", cmd.executor_class_path])

        if cmd.verbose:
            parts.append("--verbose")

        parts.append(cmd.entrypoint)
        parts.extend(cmd.app_args)

        return parts

    def _stream_to_s3(
        self,
        cluster_id: str,
        step_id: str,
        stdout_file: str,
        stderr_file: str,
        on_stdout_line: Callable[[str], None] | None,
        on_stderr_line: Callable[[str], None] | None,
    ) -> tuple[int, int]:
        stdout_lines = 0
        stderr_lines = 0

        def tail_stdout():
            nonlocal stdout_lines
            try:
                with open(stdout_file, encoding="utf-8", errors="replace") as f:
                    for line in f:
                        stdout_lines += 1
                        if on_stdout_line:
                            try:
                                on_stdout_line(line)
                            except Exception:
                                pass
                        key = self.data_lake.build_log_key(
                            cluster_id, step_id, "stdout.txt"
                        )
                        self.data_lake.append_text(key, line)
            except Exception:
                pass

        def tail_stderr():
            nonlocal stderr_lines
            try:
                with open(stderr_file, encoding="utf-8", errors="replace") as f:
                    for line in f:
                        stderr_lines += 1
                        if on_stderr_line:
                            try:
                                on_stderr_line(line)
                            except Exception:
                                pass
                        key = self.data_lake.build_log_key(
                            cluster_id, step_id, "stderr.txt"
                        )
                        self.data_lake.append_text(key, line)
            except Exception:
                pass

        t_out = threading.Thread(target=tail_stdout, daemon=True)
        t_err = threading.Thread(target=tail_stderr, daemon=True)
        t_out.start()
        t_err.start()
        t_out.join()
        t_err.join()

        return stdout_lines, stderr_lines

    def run_and_stream(
        self,
        cmd: SparkSubmitCommand,
        cluster_id: str,
        step_id: str,
        on_stdout_line: Callable[[str], None] | None = None,
        on_stderr_line: Callable[[str], None] | None = None,
    ) -> SparkProcessResult:
        command_list = self.build_command(cmd)

        with tempfile.TemporaryDirectory() as tmpdir:
            stdout_path = os.path.join(tmpdir, "stdout.log")
            stderr_path = os.path.join(tmpdir, "stderr.log")

            with (
                open(stdout_path, "w", encoding="utf-8") as f_out,
                open(stderr_path, "w", encoding="utf-8") as f_err,
            ):
                proc = subprocess.Popen(
                    command_list,
                    stdout=f_out,
                    stderr=f_err,
                    cwd=tmpdir,
                    env=os.environ.copy(),
                )
                return_code = proc.wait()

            stdout_lines, stderr_lines = self._stream_to_s3(
                cluster_id=cluster_id,
                step_id=step_id,
                stdout_file=stdout_path,
                stderr_file=stderr_path,
                on_stdout_line=on_stdout_line,
                on_stderr_line=on_stderr_line,
            )

        stdout_key = self.data_lake.build_log_key(cluster_id, step_id, "stdout.txt")
        stderr_key = self.data_lake.build_log_key(cluster_id, step_id, "stderr.txt")

        return SparkProcessResult(
            return_code=return_code,
            stdout_path_s3_key=stdout_key,
            stderr_path_s3_key=stderr_key,
            stdout_lines=stdout_lines,
            stderr_lines=stderr_lines,
        )


def get_spark_executor() -> SparkExecutor:
    return SubprocessSparkExecutor()