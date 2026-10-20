import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from app.core.config import settings
from app.domain.emr.enums import ClusterStatus, ClusterType, StepStatus, StepType
from app.infrastructure.aws.dynamodb_emr_repository import (
    EmrDynamoDBRepository,
    get_emr_repository,
)
from app.infrastructure.aws.s3_data_lake import DataLakeS3Client, get_data_lake_client
from app.infrastructure.emr.interfaces import SparkExecutor, SparkSubmitCommand
from app.infrastructure.emr.spark_executor_subprocess import get_spark_executor, SparkProcessResult
from app.services.emr.cluster_manager import ClusterManager
from app.utils.time_utils import utc_now_iso


class StepExecutor:
    def __init__(
        self,
        repository: EmrDynamoDBRepository | None = None,
        data_lake: DataLakeS3Client | None = None,
        spark_executor: SparkExecutor | None = None,
        cluster_manager: ClusterManager | None = None,
    ) -> None:
        self.repo = repository or get_emr_repository()
        self.data_lake = data_lake or get_data_lake_client()
        self.spark_executor = spark_executor or get_spark_executor()
        self.cluster_manager = cluster_manager or ClusterManager(self.repo)

    def _build_spark_conf_for_step(
        self, cluster: dict[str, Any], override: dict[str, str]
    ) -> dict[str, str]:
        base = cluster.get("spark_conf") or {}
        conf = dict(base)
        conf.update(override or {})
        return conf

    
    def add_jar_step(
        self,
        cluster_id: str,
        name: str,
        entrypoint: str,
        app_args: list[str],
        main_class: str | None,
        jars: list[str],
        packages: list[str],
        py_files: list[str],
        files: list[str],
        driver_class_path: str | None,
        executor_class_path: str | None,
        master: str | None,
        deploy_mode: str | None,
        queue: str | None,
        verbose: bool,
        spark_conf_override: dict[str, str],
    ) -> dict[str, Any]:
        cluster = self.cluster_manager.get_cluster(cluster_id)
        if not cluster:
            raise ValueError(f"Cluster not found: {cluster_id}")

        c_status = cluster.get("cluster_status")
        if c_status in (ClusterStatus.TERMINATED.value, ClusterStatus.TERMINATING.value, ClusterStatus.FAILED.value):
            raise ValueError(f"Cannot add step to cluster in status: {c_status}")

        step_id = f"s-{uuid.uuid4().hex[:12]}"
        now = utc_now_iso()

        ep = entrypoint.strip()
        if ep.startswith("s3://") or ep.startswith("s3a://"):
            entrypoint_resolved = ep.replace("s3://", "s3a://")
        elif ep.startswith("scripts/"):
            entrypoint_resolved = f"s3a://{settings.DATA_LAKE_BUCKET}/{ep.lstrip('/')}"
        elif "/" in ep and not ep.startswith("file://"):
            key = self.data_lake.build_script_key(ep)
            entrypoint_resolved = f"s3a://{settings.DATA_LAKE_BUCKET}/{key}"
        else:
            entrypoint_resolved = ep

        spark_conf_step = self._build_spark_conf_for_step(cluster, spark_conf_override)

        preview_cmd = self.spark_executor.build_command(
            SparkSubmitCommand(
                spark_submit_path=settings.SPARK_SUBMIT_PATH,
                spark_conf=spark_conf_step,
                app_args=list(app_args),
                entrypoint=entrypoint_resolved,
                main_class=main_class,
                jars=list(jars) or None,
                packages=list(packages) or None,
                py_files=list(py_files) or None,
                files=list(files) or None,
                driver_class_path=driver_class_path,
                executor_class_path=executor_class_path,
                master=master,
                deploy_mode=deploy_mode,
                queue=queue,
                verbose=verbose,
            )
        )

        item: dict[str, Any] = {
            "step_id": step_id,
            "cluster_id": cluster_id,
            "name": name,
            "step_type": StepType.JAR.value,
            "step_status": StepStatus.PENDING.value,
            "entrypoint": entrypoint_resolved,
            "main_class": main_class,
            "app_args": list(app_args),
            "spark_submit_args_preview": preview_cmd,
            "jars": list(jars),
            "packages": list(packages),
            "py_files": list(py_files),
            "files": list(files),
            "driver_class_path": driver_class_path,
            "executor_class_path": executor_class_path,
            "master": master,
            "deploy_mode": deploy_mode,
            "queue": queue,
            "verbose": verbose,
            "spark_conf_override": spark_conf_override,
            "spark_conf_effective": spark_conf_step,
            "stdout_path_s3_key": None,
            "stderr_path_s3_key": None,
            "stdout_lines": 0,
            "stderr_lines": 0,
            "return_code": None,
            "duration_ms": None,
            "created_at": now,
            "updated_at": now,
            "started_at": None,
            "ended_at": None,
            "failure_reason": None,
            "log_uri": f"s3://{settings.DATA_LAKE_BUCKET}/{self.data_lake.build_log_key(cluster_id, step_id, '')}".rstrip("/"),
            "jars_loaded": True,
        }

        self.repo.create_step(item)

        threading.Thread(
            target=self._run_step,
            args=(step_id,),
            daemon=True,
        ).start()

        return self.repo.get_step(step_id) or item

    
    def add_command_step(
        self,
        cluster_id: str,
        name: str,
        entrypoint: str,
        app_args: list[str],
        command_args: list[str],
        main_class: str | None,
        jars: list[str],
        packages: list[str],
        py_files: list[str],
        files: list[str],
        driver_class_path: str | None,
        executor_class_path: str | None,
        master: str | None,
        deploy_mode: str | None,
        queue: str | None,
        verbose: bool,
        spark_conf_override: dict[str, str],
    ) -> dict[str, Any]:
        cluster = self.cluster_manager.get_cluster(cluster_id)
        if not cluster:
            raise ValueError(f"Cluster not found: {cluster_id}")

        c_status = cluster.get("cluster_status")
        if c_status in (ClusterStatus.TERMINATED.value, ClusterStatus.TERMINATING.value, ClusterStatus.FAILED.value):
            raise ValueError(f"Cannot add step to cluster in status: {c_status}")

        step_id = f"s-{uuid.uuid4().hex[:12]}"
        now = utc_now_iso()

        ep = entrypoint.strip()
        if ep.startswith("s3://") or ep.startswith("s3a://"):
            entrypoint_resolved = ep.replace("s3://", "s3a://")
        elif ep.startswith("scripts/"):
            entrypoint_resolved = f"s3a://{settings.DATA_LAKE_BUCKET}/{ep.lstrip('/')}"
        elif "/" in ep and not ep.startswith("file://"):
            key = self.data_lake.build_script_key(ep)
            entrypoint_resolved = f"s3a://{settings.DATA_LAKE_BUCKET}/{key}"
        else:
            entrypoint_resolved = ep

        spark_conf_step = self._build_spark_conf_for_step(cluster, spark_conf_override)
        
        preview_parts = [settings.SPARK_SUBMIT_PATH]
        for k, v in sorted(spark_conf_step.items()):
            preview_parts.extend(["--conf", f"{k}={v}"])
        if master:
            preview_parts.extend(["--master", master])
        if deploy_mode:
            preview_parts.extend(["--deploy-mode", deploy_mode])
        if queue:
            preview_parts.extend(["--queue", queue])
        if main_class:
            preview_parts.extend(["--class", main_class])
        if jars:
            preview_parts.extend(["--jars", ",".join(jars)])
        if packages:
            preview_parts.extend(["--packages", ",".join(packages)])
        if py_files:
            preview_parts.extend(["--py-files", ",".join(py_files)])
        if files:
            preview_parts.extend(["--files", ",".join(files)])
        if driver_class_path:
            preview_parts.extend(["--driver-class-path", driver_class_path])
        if executor_class_path:
            preview_parts.extend(["--executor-class-path", executor_class_path])
        if verbose:
            preview_parts.append("--verbose")
        preview_parts.extend(["--"] + [entrypoint_resolved] + list(command_args))
        preview_cmd = " ".join(preview_parts)

        item: dict[str, Any] = {
            "step_id": step_id,
            "cluster_id": cluster_id,
            "name": name,
            "step_type": StepType.COMMAND.value,
            "step_status": StepStatus.PENDING.value,
            "entrypoint": entrypoint_resolved,
            "main_class": main_class,
            "app_args": list(app_args),
            "command_args": list(command_args),
            "spark_submit_args_preview": preview_cmd,
            "jars": list(jars),
            "packages": list(packages),
            "py_files": list(py_files),
            "files": list(files),
            "driver_class_path": driver_class_path,
            "executor_class_path": executor_class_path,
            "master": master,
            "deploy_mode": deploy_mode,
            "queue": queue,
            "verbose": verbose,
            "spark_conf_override": spark_conf_override,
            "spark_conf_effective": spark_conf_step,
            "stdout_path_s3_key": None,
            "stderr_path_s3_key": None,
            "stdout_lines": 0,
            "stderr_lines": 0,
            "return_code": None,
            "duration_ms": None,
            "created_at": now,
            "updated_at": now,
            "started_at": None,
            "ended_at": None,
            "failure_reason": None,
            "log_uri": f"s3://{settings.DATA_LAKE_BUCKET}/{self.data_lake.build_log_key(cluster_id, step_id, '')}".rstrip("/"),
        }

        self.repo.create_step(item)

        threading.Thread(
            target=self._run_step,
            args=(step_id,),
            daemon=True,
        ).start()

        return self.repo.get_step(step_id) or item

    
    def add_python_step(
        self,
        cluster_id: str,
        name: str,
        entrypoint: str,
        app_args: list[str],
        main_class: str | None,
        py_files: list[str],
        files: list[str],
        driver_class_path: str | None,
        executor_class_path: str | None,
        master: str | None,
        deploy_mode: str | None,
        queue: str | None,
        verbose: bool,
        spark_conf_override: dict[str, str],
    ) -> dict[str, Any]:
        cluster = self.cluster_manager.get_cluster(cluster_id)
        if not cluster:
            raise ValueError(f"Cluster not found: {cluster_id}")

        c_status = cluster.get("cluster_status")
        if c_status in (ClusterStatus.TERMINATED.value, ClusterStatus.TERMINATING.value, ClusterStatus.FAILED.value):
            raise ValueError(f"Cannot add step to cluster in status: {c_status}")

        step_id = f"s-{uuid.uuid4().hex[:12]}"
        now = utc_now_iso()

        ep = entrypoint.strip()
        if ep.startswith("s3://") or ep.startswith("s3a://"):
            entrypoint_resolved = ep.replace("s3://", "s3a://")
        elif ep.startswith("scripts/"):
            entrypoint_resolved = f"s3a://{settings.DATA_LAKE_BUCKET}/{ep.lstrip('/')}"
        elif "/" in ep and not ep.startswith("file://"):
            key = self.data_lake.build_script_key(ep)
            entrypoint_resolved = f"s3a://{settings.DATA_LAKE_BUCKET}/{key}"
        else:
            entrypoint_resolved = ep

        spark_conf_step = self._build_spark_conf_for_step(cluster, spark_conf_override)
        
        preview_cmd = self.spark_executor.build_command(
            SparkSubmitCommand(
                spark_submit_path=settings.SPARK_SUBMIT_PATH,
                spark_conf=spark_conf_step,
                app_args=list(app_args),
                entrypoint=entrypoint_resolved,
                main_class=main_class,
                jars=None,
                packages=None,
                py_files=list(py_files) or None,
                files=list(files) or None,
                driver_class_path=driver_class_path,
                executor_class_path=executor_class_path,
                master=master,
                deploy_mode=deploy_mode,
                queue=queue,
                verbose=verbose,
            )
        )

        item: dict[str, Any] = {
            "step_id": step_id,
            "cluster_id": cluster_id,
            "name": name,
            "step_type": StepType.PYTHON.value,
            "step_status": StepStatus.PENDING.value,
            "entrypoint": entrypoint_resolved,
            "main_class": main_class,
            "app_args": list(app_args),
            "spark_submit_args_preview": preview_cmd,
            "jars": [],
            "packages": [],
            "py_files": list(py_files),
            "files": list(files),
            "driver_class_path": driver_class_path,
            "executor_class_path": executor_class_path,
            "master": master,
            "deploy_mode": deploy_mode,
            "queue": queue,
            "verbose": verbose,
            "spark_conf_override": spark_conf_override,
            "spark_conf_effective": spark_conf_step,
            "stdout_path_s3_key": None,
            "stderr_path_s3_key": None,
            "stdout_lines": 0,
            "stderr_lines": 0,
            "return_code": None,
            "duration_ms": None,
            "created_at": now,
            "updated_at": now,
            "started_at": None,
            "ended_at": None,
            "failure_reason": None,
            "log_uri": f"s3://{settings.DATA_LAKE_BUCKET}/{self.data_lake.build_log_key(cluster_id, step_id, '')}".rstrip("/"),
        }

        self.repo.create_step(item)

        threading.Thread(
            target=self._run_step,
            args=(step_id,),
            daemon=True,
        ).start()

        return self.repo.get_step(step_id) or item

    
    def add_spark_submit_step(
        self,
        cluster_id: str,
        name: str,
        entrypoint: str,
        app_args: list[str],
        main_class: str | None,
        jars: list[str],
        packages: list[str],
        py_files: list[str],
        files: list[str],
        driver_class_path: str | None,
        executor_class_path: str | None,
        master: str | None,
        deploy_mode: str | None,
        queue: str | None,
        verbose: bool,
        spark_conf_override: dict[str, str],
    ) -> dict[str, Any]:
        return self.add_jar_step(
            cluster_id=cluster_id,
            name=name,
            entrypoint=entrypoint,
            app_args=app_args,
            main_class=main_class,
            jars=jars,
            packages=packages,
            py_files=py_files,
            files=files,
            driver_class_path=driver_class_path,
            executor_class_path=executor_class_path,
            master=master,
            deploy_mode=deploy_mode,
            queue=queue,
            verbose=verbose,
            spark_conf_override=spark_conf_override,
        )

    def _maybe_auto_terminate_cluster(self, cluster_id: str) -> None:
        cluster = self.repo.get_cluster(cluster_id)
        if not cluster:
            return

        if not cluster.get("auto_terminate"):
            return

        c_type = cluster.get("cluster_type")
        if c_type != ClusterType.TRANSIENT.value:
            return

        total = self.repo.count_steps(cluster_id)
        terminal = self.repo.count_terminal_steps(cluster_id)
        running = self.repo.count_running_steps(cluster_id)

        if total > 0 and terminal >= total and running == 0:
            delay = int(cluster.get("termination_delay_seconds") or settings.EMR_TRANSIENT_TERMINATION_DELAY_SECONDS)
            if delay > 0:
                time.sleep(delay)
            try:
                self.cluster_manager.terminate_cluster(cluster_id)
            except Exception:
                pass

    def _run_step(self, step_id: str) -> None:
        step = self.repo.get_step(step_id)
        if not step:
            return

        step_type = step.get("step_type")
        cluster_id = step["cluster_id"]
        now_run = utc_now_iso()

        self.repo.update_step_status(
            step_id=step_id,
            status=StepStatus.RUNNING,
            extra={"updated_at": now_run, "started_at": now_run},
        )

        cluster = self.repo.get_cluster(cluster_id)
        spark_conf_eff = step.get("spark_conf_effective") or (cluster.get("spark_conf") if cluster else {})

        # Build cmd based on step type
        if step_type == StepType.SPARK_SUBMIT.value or step_type == StepType.JAR.value:
            cmd = SparkSubmitCommand(
                spark_submit_path=settings.SPARK_SUBMIT_PATH,
                spark_conf=spark_conf_eff or {},
                app_args=list(step.get("app_args") or []),
                entrypoint=step.get("entrypoint") or "",
                main_class=step.get("main_class"),
                jars=step.get("jars") or None,
                packages=step.get("packages") or None,
                py_files=step.get("py_files") or None,
                files=step.get("files") or None,
                driver_class_path=step.get("driver_class_path"),
                executor_class_path=step.get("executor_class_path"),
                master=step.get("master"),
                deploy_mode=step.get("deploy_mode"),
                queue=step.get("queue"),
                verbose=bool(step.get("verbose")),
            )
        elif step_type == StepType.COMMAND.value:
            # For COMMAND steps, we run the entrypoint as a script with command_args
            cmd = SparkSubmitCommand(
                spark_submit_path=settings.SPARK_SUBMIT_PATH,
                spark_conf=spark_conf_eff or {},
                app_args=[] if not step.get("command_args") else list(step.get("command_args")),
                entrypoint=step.get("entrypoint") or "",
                main_class=None,
                jars=step.get("jars") or None,
                packages=step.get("packages") or None,
                py_files=step.get("py_files") or None,
                files=step.get("files") or None,
                driver_class_path=step.get("driver_class_path"),
                executor_class_path=step.get("executor_class_path"),
                master=step.get("master"),
                deploy_mode=step.get("deploy_mode"),
                queue=step.get("queue"),
                verbose=bool(step.get("verbose")),
            )
        elif step_type == StepType.PYTHON.value:
            cmd = SparkSubmitCommand(
                spark_submit_path=settings.SPARK_SUBMIT_PATH,
                spark_conf=spark_conf_eff or {},
                app_args=list(step.get("app_args") or []),
                entrypoint=step.get("entrypoint") or "",
                main_class=step.get("main_class"),
                jars=None,
                packages=None,
                py_files=step.get("py_files") or None,
                files=step.get("files") or None,
                driver_class_path=step.get("driver_class_path"),
                executor_class_path=step.get("executor_class_path"),
                master=step.get("master"),
                deploy_mode=step.get("deploy_mode"),
                queue=step.get("queue"),
                verbose=bool(step.get("verbose")),
            )
        else:
            cmd = SparkSubmitCommand(
                spark_submit_path=settings.SPARK_SUBMIT_PATH,
                spark_conf=spark_conf_eff or {},
                app_args=list(step.get("app_args") or []),
                entrypoint=step.get("entrypoint") or "",
                main_class=step.get("main_class"),
                jars=step.get("jars") or None,
                packages=step.get("packages") or None,
                py_files=step.get("py_files") or None,
                files=step.get("files") or None,
                driver_class_path=step.get("driver_class_path"),
                executor_class_path=step.get("executor_class_path"),
                master=step.get("master"),
                deploy_mode=step.get("deploy_mode"),
                queue=step.get("queue"),
                verbose=bool(step.get("verbose")),
            )

        res = self.spark_executor.run_and_stream(
            cmd=cmd,
            cluster_id=cluster_id,
            step_id=step_id,
            on_stdout_line=None,
            on_stderr_line=None,
        )

        ended_at = utc_now_iso()
        started_at_s = step.get("started_at") or now_run

        duration_ms: int | None = None
        try:
            def parse_dt(s: str) -> datetime:
                s2 = s.replace("Z", "+00:00") if s.endswith("Z") else s
                try:
                    dt = datetime.fromisoformat(s2)
                except Exception:
                    dt = datetime.fromisoformat(s2.replace("+00:00", ""))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt

            d0 = parse_dt(started_at_s)
            d1 = parse_dt(ended_at)
            duration_ms = max(0, int((d1 - d0).total_seconds() * 1000))
        except Exception:
            duration_ms = None

        # Map return code to status
        if res.return_code == 0:
            final_status = StepStatus.COMPLETED
        else:
            final_status = StepStatus.FAILED
            reason = f"step exited with code {res.return_code}"

        self.repo.update_step_status(
            step_id=step_id,
            status=final_status,
            extra={
                "updated_at": ended_at,
                "ended_at": ended_at,
                "stdout_path_s3_key": res.stdout_path_s3_key,
                "stderr_path_s3_key": res.stderr_path_s3_key,
                "stdout_lines": res.stdout_lines,
                "stderr_lines": res.stderr_lines,
                "return_code": res.return_code,
                "duration_ms": duration_ms,
                "failure_reason": reason[:500] if step_type == StepType.COMMAND.value else None,
            },
        )

        self._maybe_auto_terminate_cluster(cluster_id)

    def get_step(self, step_id: str) -> dict[str, Any] | None:
        return self.repo.get_step(step_id)

    def list_steps_by_cluster(self, cluster_id: str, limit: int = 100, status: StepStatus | None = None) -> list[dict[str, Any]]:
        return self.repo.list_steps_by_cluster(cluster_id, limit=limit, status=status)

    def cancel_step(self, step_id: str) -> dict[str, Any] | None:
        step = self.repo.get_step(step_id)
        if not step:
            return None

        s_status = step.get("step_status")
        if s_status in (StepStatus.COMPLETED.value, StepStatus.FAILED.value, StepStatus.CANCELLED.value):
            return step

        now = utc_now_iso()
        updated = self.repo.update_step_status(
            step_id=step_id,
            status=StepStatus.CANCELLED,
            extra={
                "updated_at": now,
                "ended_at": now,
                "failure_reason": "Step cancelled by request",
            },
        )
        return updated or step