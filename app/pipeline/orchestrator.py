from __future__ import annotations

import asyncio
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from app.pipeline.models import PipelineDef, PipelineRun, StepDef
from app.services.emr.step_executor import StepExecutor
from app.infrastructure.aws.s3_data_lake import DataLakeS3Client, get_data_lake_client


StepStatusType = str  # Use string to avoid circular imports


class PipelineOrchestrator:
    
    def __init__(
        self,
        step_executor: StepExecutor | None = None,
        data_lake: DataLakeS3Client | None = None,
    ) -> None:
        self.step_executor = step_executor or StepExecutor()
        self.data_lake = data_lake or get_data_lake_client()
        self.pipeline_repo: PipelineRepository | None = None

    def set_repo(self, repo: PipelineRepository) -> None:
        self.pipeline_repo = repo

    def _resolve_dependency_order(self, steps: list[StepDef]) -> list[StepDef]:
        
        step_map: dict[str, StepDef] = {s.name: s for s in steps}
        in_degree: dict[str, int] = {s.name: 0 for s in steps}
        adj: dict[str, list[str]] = {s.name: [] for s in steps}

        for s in steps:
            if s.depends_on:
                for dep in s.depends_on:
                    if dep in step_map:
                        adj[dep].append(s.name)
                        in_degree[s.name] += 1

        queue = deque([name for name, deg in in_degree.items() if deg == 0])
        ordered: list[str] = []

        while queue:
            name = queue.popleft()
            ordered.append(name)
            for neighbor in adj[name]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(ordered) != len(steps):
            raise ValueError("Circular dependency detected in pipeline steps")

        return [step_map[name] for name in ordered]

    def _are_deps_met(self, step: StepDef, completed_steps: Set[str], failed_steps: Set[str]) -> bool:
        
        if not step.depends_on:
            return True
        for dep in step.depends_on:
            if dep in failed_steps:
                return False
            if dep not in completed_steps:
                return False
        return True

    def submit_pipeline(self, pipeline: PipelineDef) -> str:
        
        pipeline_id = str(uuid.uuid4())

        run = self.pipeline_repo.create_run(pipeline_id, pipeline.name)
        ordered_steps = self._resolve_dependency_order(pipeline.steps)

        
        for step in ordered_steps:
            self.pipeline_repo.update_step(
                pipeline_id=pipeline_id,
                step_id=step.name,
                status="PENDING",
            )

        
        if self.pipeline_repo:
            self.pipeline_repo._runs[pipeline_id].steps = {
                step.name: {"status": "PENDING"} for step in ordered_steps
            }

        
        asyncio.create_task(self._execute_pipeline(pipeline_id, ordered_steps))

        return pipeline_id

    async def _execute_pipeline(self, pipeline_id: str, ordered_steps: list[StepDef]) -> None:
        
        run = self.pipeline_repo.get_run(pipeline_id)
        if not run:
            return

        completed_steps: Set[str] = set()
        failed_steps: Set[str] = set()
        running_steps: Set[str] = set()

        for step in ordered_steps:
            
            if not self._are_deps_met(step, completed_steps, failed_steps):
                
                for _ in range(60):
                    await asyncio.sleep(0.5)
                    if self._are_deps_met(step, completed_steps, failed_steps):
                        break
                else:
                    
                    self.pipeline_repo.update_step(
                        pipeline_id=pipeline_id,
                        step_id=step.name,
                        status="FAILED",
                        failure_reason=f"Dependency timeout for step {step.name}",
                    )
                    failed_steps.add(step.name)
                    continue

            
            if failed_steps and step.name not in failed_steps and not pipeline.idempotent:
                continue

            
            executed = False
            for attempt in range(step.retry_count + 1):
                try:
                    result = await self._execute_single_step(step)
                    status = result.get("status", "FAILED")
                    if status == "COMPLETED":
                        completed_steps.add(step.name)
                        running_steps.discard(step.name)
                        self.pipeline_repo.update_step(
                            pipeline_id=pipeline_id,
                            step_id=step.name,
                            status="COMPLETED",
                            stdout_path_s3_key=result.get("stdout_path_s3_key"),
                            stderr_path_s3_key=result.get("stderr_path_s3_key"),
                            stdout_lines=result.get("stdout_lines", 0),
                            stderr_lines=result.get("stderr_lines", 0),
                            return_code=result.get("return_code"),
                            failure_reason=result.get("failure_reason"),
                        )
                        executed = True
                        break
                    else:
                        if attempt < step.retry_count:
                            await asyncio.sleep(step.retry_delay_seconds or 5)
                            continue
                        failed_steps.add(step.name)
                        running_steps.discard(step.name)
                        self.pipeline_repo.update_step(
                            pipeline_id=pipeline_id,
                            step_id=step.name,
                            status="FAILED",
                            failure_reason=result.get("failure_reason", "Step failed"),
                        )
                        executed = True
                        break
                except Exception as e:
                    if attempt < step.retry_count:
                        await asyncio.sleep(step.retry_delay_seconds or 5)
                        continue
                    failed_steps.add(step.name)
                    running_steps.discard(step.name)
                    self.pipeline_repo.update_step(
                        pipeline_id=pipeline_id,
                        step_id=step.name,
                        status="FAILED",
                        failure_reason=str(e),
                    )
                    executed = True
                    break

            if not executed:
                
                failed_steps.add(step.name)
                running_steps.discard(step.name)
                self.pipeline_repo.update_step(
                    pipeline_id=pipeline_id,
                    step_id=step.name,
                    status="FAILED",
                    failure_reason="Step execution exhausted retries",
                )

        
        all_done = all(
            s.get("status") in ("COMPLETED", "FAILED", "CANCELLED")
            for s in run.steps.values()
        )
        if all_done:
            run.status = "COMPLETED" if not failed_steps else "FAILED"
            run.completed_at = datetime.now(timezone.utc).isoformat()
            self.pipeline_repo._runs[pipeline_id] = run

    async def _execute_single_step(self, step: StepDef) -> dict[str, Any]:
        
        step_type = step.step_type

        if step_type == "SPARK_SUBMIT":
            return await self._execute_spark_submit(step)
        elif step_type == "JAR":
            return await self._execute_jar_step(step)
        elif step_type == "COMMAND":
            return await self._execute_command_step(step)
        elif step_type == "PYTHON":
            return await self._execute_python_step(step)
        else:
            return {"status": "FAILED", "return_code": -1, "failure_reason": "Unknown step type"}

    async def _execute_spark_submit(self, step: StepDef) -> dict[str, Any]:
                
        from app.infrastructure.emr.interfaces import SparkSubmitCommand
        from app.infrastructure.emr.spark_executor_subprocess import get_spark_executor

        spark_exec = get_spark_executor()
        cmd = SparkSubmitCommand(
            spark_submit_path=self.step_executor.spark_executor.__dict__.get(
                "spark_submit_path", "spark-submit"
            ),
            spark_conf={},
            app_args=step.app_args or [],
            entrypoint=step.entrypoint,
            main_class=step.main_class,
            jars=step.jars or None,
            packages=step.packages or None,
            py_files=step.py_files or None,
            files=step.files or None,
            driver_class_path=step.driver_class_path,
            executor_class_path=step.executor_class_path,
            master=step.master,
            deploy_mode=step.deploy_mode,
            queue=step.queue,
            verbose=step.verbose,
        )
        
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: spark_exec.run_and_stream(
                cmd=cmd,
                cluster_id=f"pipeline-{step.name}",
                step_id=step.name,
                on_stdout_line=None,
                on_stderr_line=None,
            ),
        )

        return {
            "status": "COMPLETED" if result.return_code == 0 else "FAILED",
            "return_code": result.return_code,
            "stdout_path_s3_key": result.stdout_path_s3_key,
            "stderr_path_s3_key": result.stderr_path_s3_key,
            "stdout_lines": result.stdout_lines,
            "stderr_lines": result.stderr_lines,
            "failure_reason": f"spark-submit exited with code {result.return_code}" if result.return_code != 0 else None,
        }

    async def _execute_jar_step(self, step: StepDef) -> dict[str, Any]:
        
        from app.infrastructure.emr.interfaces import SparkSubmitCommand
        from app.infrastructure.emr.spark_executor_subprocess import get_spark_executor

        spark_exec = get_spark_executor()
        cmd = SparkSubmitCommand(
            spark_submit_path="spark-submit",
            spark_conf={},
            app_args=step.app_args or [],
            entrypoint=step.entrypoint or "",
            main_class=step.main_class,
            jars=step.jars or None,
            packages=step.packages or None,
            py_files=step.py_files or None,
            files=step.files or None,
            driver_class_path=step.driver_class_path,
            executor_class_path=step.executor_class_path,
            master=step.master,
            deploy_mode=step.deploy_mode,
            queue=step.queue,
            verbose=step.verbose,
        )

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            lambda: spark_exec.run_and_stream(
                cmd=cmd,
                cluster_id=f"pipeline-{step.name}",
                step_id=step.name,
                on_stdout_line=None,
                on_stderr_line=None,
            ),
        )

        return {
            "status": "COMPLETED" if result.return_code == 0 else "FAILED",
            "return_code": result.return_code,
            "stdout_path_s3_key": result.stdout_path_s3_key,
            "stderr_path_s3_key": result.stderr_path_s3_key,
            "stdout_lines": result.stdout_lines,
            "stderr_lines": result.stderr_lines,
            "failure_reason": f"JAR step exited with code {result.return_code}" if result.return_code != 0 else None,
        }

    async def _execute_command_step(self, step: StepDef) -> dict[str, Any]:
        """Execute a COMMAND step - runs arbitrary command."""
        import subprocess

        cmd = step.command_args or [step.entrypoint] if step.entrypoint else []
        import os

        import os

        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            stdout_path = os.path.join(tmpdir, "stdout.log")
            stderr_path = os.path.join(tmpdir, "stderr.log")

            proc = subprocess.Popen(
                cmd,
                stdout=open(stdout_path, "w", encoding="utf-8"),
                stderr=open(stderr_path, "w", encoding="utf-8"),
                cwd=tmpdir,
                env=os.environ.copy(),
            )
            return_code = proc.wait()

        return {
            "status": "COMPLETED" if return_code == 0 else "FAILED",
            "return_code": return_code,
            "stdout_path_s3_key": None,
            "stderr_path_s3_key": None,
            "stdout_lines": 0,
            "stderr_lines": 0,
            "failure_reason": f"Command exited with code {return_code}" if return_code != 0 else None,
        }

    async def _execute_python_step(self, step: StepDef) -> dict[str, Any]:
        """Execute a PYTHON step - runs a Python script."""
        import subprocess

        import os

        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            stdout_path = os.path.join(tmpdir, "stdout.log")
            stderr_path = os.path.join(tmpdir, "stderr.log")

            exec_cmd = ["python"]
            if step.entrypoint:
                exec_cmd.append(step.entrypoint)
            if step.app_args:
                exec_cmd.extend(step.app_args)
            if step.py_files:
                exec_cmd.extend(["--py-files", ",".join(step.py_files)])

            proc = subprocess.Popen(
                exec_cmd,
                stdout=open(stdout_path, "w", encoding="utf-8"),
                stderr=open(stderr_path, "w", encoding="utf-8"),
                cwd=tmpdir,
                env=os.environ.copy(),
            )
            return_code = proc.wait()

        return {
            "status": "COMPLETED" if return_code == 0 else "FAILED",
            "return_code": return_code,
            "stdout_path_s3_key": None,
            "stderr_path_s3_key": None,
            "stdout_lines": 0,
            "stderr_lines": 0,
            "failure_reason": f"Python step exited with code {return_code}" if return_code != 0 else None,
        }