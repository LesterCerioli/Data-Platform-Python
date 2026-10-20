from typing import Annotated, Dict

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.domain.emr.enums import ClusterStatus, StepStatus, StepType
from app.schemas.emr import (
    AddStepRequest,
    AddStepResponse,
    CancelStepResponse,
    ClusterDescriptionResponse,
    ClusterResponse,
    CreateClusterRequest,
    ListClustersResponse,
    ListStepsResponse,
    StepResponse,
    TerminateClusterResponse,
)
from app.services.auth_service import get_current_user, issue_jwt, authenticate_client
from app.services.emr.cluster_manager import ClusterManager
from app.services.emr.step_executor import StepExecutor


def get_cluster_manager() -> ClusterManager:
    return ClusterManager()


def get_step_executor() -> StepExecutor:
    return StepExecutor()


router = APIRouter(prefix="/emr", tags=["emr"])


# Protected endpoint dependency
def require_auth(current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    """Dependency that requires valid authentication.

    FastAPI will raise 401 if the token is missing or invalid,
    otherwise returns the decoded JWT payload.
    """
    return current_user



@router.post(
    "/clusters",
    response_model=ClusterResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        400: {"description": "Invalid request payload"},
        500: {"description": "Internal error while creating cluster"},
    },
)
def create_cluster(
    payload: CreateClusterRequest,
    cm: Annotated[ClusterManager, Depends(get_cluster_manager)],
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    try:
        c = cm.create_cluster(
            cluster_name=payload.cluster_name,
            cluster_type=payload.cluster_type,
            applications=payload.applications,
            instance_count=payload.instance_count,
            core_instance_count=payload.core_instance_count,
            task_instance_count=payload.task_instance_count,
            master_instance_type=payload.master_instance_type,
            core_instance_type=payload.core_instance_type,
            task_instance_type=payload.task_instance_type,
            executor_cores=payload.executor_cores,
            executor_memory=payload.executor_memory,
            driver_memory=payload.driver_memory,
            num_executors=payload.num_executors,
            shuffle_partitions=payload.shuffle_partitions,
            yarn_memory_overhead=payload.yarn_memory_overhead,
            spark_conf=payload.spark_conf,
            tags=payload.tags,
            auto_terminate=payload.auto_terminate,
            termination_delay_seconds=payload.termination_delay_seconds,
        )
    except ValueError as ve:
        # Erros de validação de negócio -> culpa do cliente
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve)) from ve
    except Exception as e:
        # Falhas de infraestrutura / bugs -> culpa do servidor
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        ) from e

    if not c:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create cluster",
        )

    return ClusterResponse(**c)


@router.get(
    "/clusters",
    response_model=ListClustersResponse,
    responses={500: {"description": "Internal error while listing clusters"}},
)
def list_clusters(
    cm: Annotated[ClusterManager, Depends(get_cluster_manager)],
    limit: int = Query(default=50, ge=1, le=200),
    status_filter: ClusterStatus | None = Query(default=None, alias="status"),
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    clusters = cm.list_clusters(limit=limit, status=status_filter)
    return ListClustersResponse(
        clusters=[ClusterResponse(**cl) for cl in clusters],
        count=len(clusters),
    )


@router.get(
    "/clusters/{cluster_id}",
    response_model=ClusterDescriptionResponse,
    responses={
        404: {"description": "Cluster not found"},
        500: {"description": "Internal error while describing cluster"},
    },
)
def describe_cluster(
    cluster_id: str,
    cm: Annotated[ClusterManager, Depends(get_cluster_manager)],
    se: Annotated[StepExecutor, Depends(get_step_executor)],
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    c = cm.get_cluster(cluster_id)
    if not c:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Cluster not found"
        )

    steps_list = se.list_steps_by_cluster(cluster_id, limit=500)
    steps_resp = [StepResponse(**s) for s in steps_list]

    total = len(steps_resp)
    running = sum(1 for s in steps_resp if s.step_status == StepStatus.RUNNING)
    terminal = sum(
        1
        for s in steps_resp
        if s.step_status
        in (StepStatus.COMPLETED, StepStatus.FAILED, StepStatus.CANCELLED)
    )

    payload = {
        **c,
        "steps": steps_resp,
        "total_steps": total,
        "running_steps": running,
        "terminal_steps": terminal,
    }
    return ClusterDescriptionResponse(**payload)


@router.post(
    "/clusters/{cluster_id}/terminate",
    response_model=TerminateClusterResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        404: {"description": "Cluster not found"},
        500: {"description": "Internal error while terminating cluster"},
    },
)
def terminate_cluster(
    cluster_id: str,
    cm: Annotated[ClusterManager, Depends(get_cluster_manager)],
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    try:
        c = cm.terminate_cluster(cluster_id)
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve)) from ve
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        ) from e

    if not c:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Cluster not found"
        )

    return TerminateClusterResponse(
        cluster_id=c["cluster_id"],
        cluster_status=ClusterStatus(c["cluster_status"]),
        message="Cluster termination requested",
    )


@router.post(
    "/clusters/{cluster_id}/steps",
    response_model=AddStepResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        400: {"description": "Invalid step payload or unsupported step_type"},
        404: {"description": "Cluster not found"},
        500: {"description": "Internal error while adding step"},
    },
)
def add_step(
    cluster_id: str,
    payload: AddStepRequest,
    se: Annotated[StepExecutor, Depends(get_step_executor)],
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    try:
        step_type = StepType(payload.step_type)

        if step_type == StepType.SPARK_SUBMIT:
            ss = payload.spark_submit
            if not ss:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="spark_submit is required when step_type is SPARK_SUBMIT",
                )
            step = se.add_spark_submit_step(
                cluster_id=cluster_id,
                name=ss.name or payload.name or "spark-step",
                entrypoint=ss.entrypoint,
                app_args=ss.app_args,
                main_class=ss.main_class,
                jars=ss.jars,
                packages=ss.packages,
                py_files=ss.py_files,
                files=ss.files,
                driver_class_path=ss.driver_class_path,
                executor_class_path=ss.executor_class_path,
                master=ss.master,
                deploy_mode=ss.deploy_mode,
                queue=ss.queue,
                verbose=ss.verbose,
                spark_conf_override=ss.spark_conf_override,
            )
        elif step_type == StepType.JAR:
            js = payload.jar_step
            if not js:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="jar_step is required when step_type is JAR",
                )
            step = se.add_jar_step(
                cluster_id=cluster_id,
                name=js.name or payload.name or "jar-step",
                entrypoint=js.entrypoint,
                app_args=js.app_args,
                main_class=js.main_class,
                jars=js.jars,
                packages=js.packages,
                py_files=js.py_files,
                files=js.files,
                driver_class_path=js.driver_class_path,
                executor_class_path=js.executor_class_path,
                master=js.master,
                deploy_mode=js.deploy_mode,
                queue=js.queue,
                verbose=js.verbose,
                spark_conf_override=js.spark_conf_override,
            )
        elif step_type == StepType.COMMAND:
            cs = payload.command_step
            if not cs:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="command_step is required when step_type is COMMAND",
                )
            step = se.add_command_step(
                cluster_id=cluster_id,
                name=cs.name or payload.name or "command-step",
                entrypoint=cs.entrypoint,
                app_args=cs.app_args,
                command_args=cs.command_args,
                main_class=cs.main_class,
                jars=cs.jars,
                packages=cs.packages,
                py_files=cs.py_files,
                files=cs.files,
                driver_class_path=cs.driver_class_path,
                executor_class_path=cs.executor_class_path,
                master=cs.master,
                deploy_mode=cs.deploy_mode,
                queue=cs.queue,
                verbose=cs.verbose,
                spark_conf_override=cs.spark_conf_override,
            )
        elif step_type == StepType.PYTHON:
            ps = payload.python_step
            if not ps:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="python_step is required when step_type is PYTHON",
                )
            step = se.add_python_step(
                cluster_id=cluster_id,
                name=ps.name or payload.name or "python-step",
                entrypoint=ps.entrypoint,
                app_args=ps.app_args,
                main_class=ps.main_class,
                py_files=ps.py_files,
                files=ps.files,
                driver_class_path=ps.driver_class_path,
                executor_class_path=ps.executor_class_path,
                master=ps.master,
                deploy_mode=ps.deploy_mode,
                queue=ps.queue,
                verbose=ps.verbose,
                spark_conf_override=ps.spark_conf_override,
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported step_type: {step_type}",
            )
    except HTTPException:
        # Propaga HTTPExceptions já formatadas (400) sem reembrulhar
        raise
    except ValueError as ve:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve)
        ) from ve
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        ) from e

    if not step:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to add step",
        )

    return AddStepResponse(
        step_id=step["step_id"],
        cluster_id=step["cluster_id"],
        name=step["name"],
        step_type=StepType(step["step_type"]),
        step_status=StepStatus(step["step_status"]),
        message="Step submitted and running asynchronously",
    )


@router.get(
    "/clusters/{cluster_id}/steps",
    response_model=ListStepsResponse,
    responses={
        404: {"description": "Cluster not found"},
        500: {"description": "Internal error while listing steps"},
    },
)
def list_steps(
    cluster_id: str,
    se: Annotated[StepExecutor, Depends(get_step_executor)],
    cm: Annotated[ClusterManager, Depends(get_cluster_manager)],
    limit: int = Query(default=100, ge=1, le=500),
    status_filter: StepStatus | None = Query(default=None, alias="status"),
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    
    if not cm.get_cluster(cluster_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Cluster not found"
        )

    steps = se.list_steps_by_cluster(cluster_id, limit=limit, status=status_filter)
    return ListStepsResponse(
        steps=[StepResponse(**s) for s in steps],
        count=len(steps),
    )


@router.get(
    "/steps/{step_id}",
    response_model=StepResponse,
    responses={
        404: {"description": "Step not found"},
        500: {"description": "Internal error while describing step"},
    },
)
def describe_step(
    step_id: str,
    se: Annotated[StepExecutor, Depends(get_step_executor)],
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    s = se.get_step(step_id)
    if not s:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Step not found"
        )
    return StepResponse(**s)


@router.post(
    "/steps/{step_id}/cancel",
    response_model=CancelStepResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        404: {"description": "Step not found"},
        500: {"description": "Internal error while cancelling step"},
    },
)
def cancel_step(
    step_id: str,
    se: Annotated[StepExecutor, Depends(get_step_executor)],
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    try:
        s = se.cancel_step(step_id)
    except ValueError as ve:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve)
        ) from ve
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        ) from e

    if not s:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Step not found"
        )

    return CancelStepResponse(
        step_id=s["step_id"],
        cluster_id=s["cluster_id"],
        step_status=StepStatus(s["step_status"]),
        message="Step cancellation requested",
    )