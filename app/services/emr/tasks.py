from app.domain.emr.enums import ClusterStatus
from app.infrastructure.aws.dynamodb_emr_repository import get_emr_repository
from app.services.emr.cluster_manager import ClusterManager


def sweep_transient_clusters() -> dict[str, int]:
    repo = get_emr_repository()
    cm = ClusterManager(repo)
    terminated_count = 0

    clusters = repo.list_clusters(limit=200)
    for c in clusters:
        c_id = c.get("cluster_id")
        c_status = c.get("cluster_status")
        c_type = c.get("cluster_type")
        auto_term = c.get("auto_terminate")

        if not c_id or c_type != "TRANSIENT" or not auto_term:
            continue

        if c_status in (ClusterStatus.RUNNING.value, ClusterStatus.CREATING.value):
            total = repo.count_steps(c_id)
            terminal = repo.count_terminal_steps(c_id)
            running = repo.count_running_steps(c_id)
            if total > 0 and terminal >= total and running == 0:
                try:
                    cm.terminate_cluster(c_id)
                    terminated_count += 1
                except Exception:
                    pass

        if c_status == ClusterStatus.TERMINATING.value:
            try:
                cm.terminate_cluster(c_id)
                terminated_count += 1
            except Exception:
                pass

    return {"terminated": terminated_count}