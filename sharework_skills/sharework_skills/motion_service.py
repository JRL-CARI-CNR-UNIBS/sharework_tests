"""
motion_service.py

Plan → time-parametrize → execute pipeline for robot arm movements.

Depends on action_client.py for the low-level goal/result round-trip.
"""

from moveit_msgs.action import ExecuteTrajectory
from moveit_msgs.msg import MotionPlanResponse
from pose_constraints_msgs.action import PlanWithConstraints
from pose_constraints_msgs.msg import GeometricConstraintArray
from rclpy.action import ActionClient
from rclpy.node import Node
from time_parametrization_msgs.action import ApplyTimeParametrization

from .action_client import send_goal_and_wait
from .utils import is_success


def plan_and_execute(
    node: Node,
    plan_client: ActionClient,
    tp_client: ActionClient,
    exec_client: ActionClient,
    motion_plan_request,
    geometric_constraints: GeometricConstraintArray,
    *,
    plan_action_name: str,
    time_param_action_name: str,
    execute_action_name: str,
    max_vel: float,
    max_acc: float,
    cartesian_speed_limited_link: str,
    max_cartesian_speed: float,
    dry_run: bool,
    verbose: bool,
) -> None:
    """Run the full plan → time-parametrize → execute pipeline for one movement.

    Steps:
      1. Send a planning goal to *plan_client*.
      2. Send the unparametrized trajectory to *tp_client* for time scaling.
      3. Execute the parametrized trajectory via *exec_client* (skipped in dry-run mode).

    Raises RuntimeError on any failure.
    """
    # 1) Plan
    plan_goal = PlanWithConstraints.Goal()
    plan_goal.motion_plan_request = motion_plan_request
    plan_goal.constraints = geometric_constraints
    plan_goal.verbose = verbose

    plan_res = send_goal_and_wait(node, plan_client, plan_goal, plan_action_name)
    plan_out: MotionPlanResponse = plan_res.motion_plan_response
    if not is_success(plan_out.error_code):
        raise RuntimeError(f"Planning fallito (error_code={int(plan_out.error_code.val)})")

    # 2) Time parametrization
    tp_goal = ApplyTimeParametrization.Goal()
    tp_goal.unparametrized_motion_plan_response = plan_out
    tp_goal.max_velocity_scaling_factor = float(max_vel)
    tp_goal.max_acceleration_scaling_factor = float(max_acc)
    tp_goal.cartesian_speed_limited_link = str(cartesian_speed_limited_link)
    tp_goal.max_cartesian_speed = float(max_cartesian_speed)

    tp_res = send_goal_and_wait(node, tp_client, tp_goal, time_param_action_name)
    tp_out: MotionPlanResponse = tp_res.motion_plan_response
    if not is_success(tp_out.error_code):
        raise RuntimeError(f"Time parametrization fallita (error_code={int(tp_out.error_code.val)})")

    # 3) Execute
    if dry_run:
        return

    exec_goal = ExecuteTrajectory.Goal()
    exec_goal.trajectory = tp_out.trajectory
    exec_res = send_goal_and_wait(node, exec_client, exec_goal, execute_action_name)

    try:
        if not is_success(exec_res.error_code):
            raise RuntimeError(f"Esecuzione fallita (error_code={int(exec_res.error_code.val)})")
    except Exception:
        pass
