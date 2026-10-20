import uuid
from typing import Any

from app.core.config import settings
from app.domain.emr.enums import ApplicationType, ClusterStatus, ClusterType
from app.infrastructure.aws.dynamodb_emr_repository import (
    EmrDynamoDBRepository,
    get_emr_repository,
)
from app.utils.time_utils import utc_now_iso


class ClusterManager:
    def __init__(self, repository: EmrDynamoDBRepository | None = None) -> None:
        self.repo = repository or get_emr_repository()

    def _build_spark_conf(self, base: dict[str, str]) -> dict[str, str]:
        conf: dict[str, str] = dict(base)
        if settings.EMR_ENABLE_SPARK_EVENT_LOGS:
            conf.setdefault("spark.eventLog.enabled", "true")
            conf.setdefault(
                "spark.eventLog.dir",
                f"s3a://{settings.DATA_LAKE_BUCKET}/{settings.EMR_SPARK_EVENT_LOGS_DIR}",
            )
        conf.setdefault("spark.sql.shuffle.partitions", str(settings.EMR_DEFAULT_SHUFFLE_PARTITIONS))
        conf.setdefault("spark.executor.memory", settings.EMR_DEFAULT_EXECUTOR_MEMORY)
        conf.setdefault("spark.executor.cores", str(settings.EMR_DEFAULT_EXECUTOR_CORES))
        conf.setdefault("spark.driver.memory", settings.EMR_DEFAULT_DRIVER_MEMORY)
        if settings.EMR_DEFAULT_YARN_MEMORY_OVERHEAD:
            conf.setdefault("spark.executor.memoryOverhead", settings.EMR_DEFAULT_YARN_MEMORY_OVERHEAD)
            conf.setdefault("spark.driver.memoryOverhead", settings.EMR_DEFAULT_YARN_MEMORY_OVERHEAD)
        return conf

    def create_cluster(
        self,
        cluster_name: str,
        cluster_type: ClusterType,
        applications: list[ApplicationType],
        instance_count: int,
        core_instance_count: int,
        task_instance_count: int,
        master_instance_type: str | None,
        core_instance_type: str | None,
        task_instance_type: str | None,
        executor_cores: int,
        executor_memory: str,
        driver_memory: str,
        num_executors: int | None,
        shuffle_partitions: int,
        yarn_memory_overhead: str | None,
        spark_conf: dict[str, str],
        tags: dict[str, str],
        auto_terminate: bool | None,
        termination_delay_seconds: int | None,
    ) -> dict[str, Any]:
        cluster_id = f"j-{uuid.uuid4().hex[:12]}"
        now = utc_now_iso()

        effective_auto_terminate = (
            auto_terminate
            if auto_terminate is not None
            else (cluster_type == ClusterType.TRANSIENT and settings.EMR_TRANSIENT_AUTO_TERMINATE)
        )
        effective_term_delay = (
            termination_delay_seconds
            if termination_delay_seconds is not None
            else settings.EMR_TRANSIENT_TERMINATION_DELAY_SECONDS
        )

        merged_spark_conf = dict(spark_conf)
        if shuffle_partitions:
            merged_spark_conf["spark.sql.shuffle.partitions"] = str(shuffle_partitions)
        merged_spark_conf["spark.executor.cores"] = str(executor_cores)
        merged_spark_conf["spark.executor.memory"] = executor_memory
        merged_spark_conf["spark.driver.memory"] = driver_memory
        if yarn_memory_overhead:
            merged_spark_conf["spark.executor.memoryOverhead"] = yarn_memory_overhead
            merged_spark_conf["spark.driver.memoryOverhead"] = yarn_memory_overhead

        final_spark_conf = self._build_spark_conf(merged_spark_conf)

        item: dict[str, Any] = {
            "cluster_id": cluster_id,
            "cluster_name": cluster_name,
            "cluster_type": cluster_type.value,
            "cluster_status": ClusterStatus.CREATING.value,
            "applications": [a.value for a in applications],
            "instance_count": instance_count,
            "core_instance_count": core_instance_count,
            "task_instance_count": task_instance_count,
            "master_instance_type": master_instance_type,
            "core_instance_type": core_instance_type,
            "task_instance_type": task_instance_type,
            "executor_cores": executor_cores,
            "executor_memory": executor_memory,
            "driver_memory": driver_memory,
            "num_executors": num_executors,
            "shuffle_partitions": shuffle_partitions,
            "yarn_memory_overhead": yarn_memory_overhead,
            "spark_conf": final_spark_conf,
            "auto_terminate": effective_auto_terminate,
            "termination_delay_seconds": effective_term_delay,
            "tags": tags or {},
            "created_at": now,
            "updated_at": now,
            "started_at": None,
            "terminated_at": None,
            "failure_reason": None,
        }

        created = self.repo.create_cluster(item)

        running = self.repo.update_cluster_status(
            cluster_id=cluster_id,
            status=ClusterStatus.RUNNING,
            extra={"updated_at": utc_now_iso()},
        )
        return running or created

    def get_cluster(self, cluster_id: str) -> dict[str, Any] | None:
        return self.repo.get_cluster(cluster_id)

    def list_clusters(self, limit: int = 50, status: ClusterStatus | None = None) -> list[dict[str, Any]]:
        return self.repo.list_clusters(limit=limit, status=status)

    def terminate_cluster(self, cluster_id: str, force: bool = True) -> dict[str, Any] | None:
        cluster = self.repo.get_cluster(cluster_id)
        if not cluster:
            return None

        current = cluster.get("cluster_status")
        if current == ClusterStatus.TERMINATED.value:
            return cluster

        now = utc_now_iso()
        terminating = self.repo.update_cluster_status(
            cluster_id=cluster_id,
            status=ClusterStatus.TERMINATING,
            extra={"updated_at": now},
        )
        terminated = self.repo.update_cluster_status(
            cluster_id=cluster_id,
            status=ClusterStatus.TERMINATED,
            extra={"updated_at": now, "terminated_at": now},
        )
        return terminated or terminating or cluster

    def mark_failed(self, cluster_id: str, reason: str) -> dict[str, Any] | None:
        now = utc_now_iso()
        return self.repo.update_cluster_status(
            cluster_id=cluster_id,
            status=ClusterStatus.FAILED,
            extra={
                "updated_at": now,
                "terminated_at": now,
                "failure_reason": reason[:500],
            },
        )