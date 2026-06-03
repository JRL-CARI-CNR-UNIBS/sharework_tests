"""
constraints.py

Builders for MoveIt MotionPlanRequest and GeometricConstraintArray objects.
All functions are pure (no side effects) and depend only on the node for
parameter values passed in as arguments.
"""

from typing import Any, Dict, List

from geometry_msgs.msg import Pose
from moveit_msgs.msg import (
    BoundingVolume,
    Constraints,
    JointConstraint,
    MotionPlanRequest,
    OrientationConstraint,
    PositionConstraint,
    RobotState,
)
from pose_constraints_msgs.msg import GeometricConstraint, GeometricConstraintArray

from .utils import set_xyz


# ---------------------------------------------------------------------------
# GeometricConstraintArray
# ---------------------------------------------------------------------------

def yaml_type_to_uint8(t: Any) -> int:
    """Convert a YAML constraint type string/int to the GeometricConstraint uint8."""
    if isinstance(t, int):
        return int(t)
    s = str(t).strip().lower()
    if s == "plane":
        return int(GeometricConstraint.PLANE)
    if s == "line":
        return int(GeometricConstraint.LINE)
    if s in ("angle", "orientation", "orient"):
        return int(GeometricConstraint.ANGLE)
    if s.isdigit():
        return int(s)
    raise RuntimeError(f"Tipo vincolo non supportato: '{t}'.")


def build_geometric_constraints_array(
    geometric_constraints: List[Dict[str, Any]],
    stamp,
) -> GeometricConstraintArray:
    """Build a GeometricConstraintArray from a list of YAML-parsed dicts."""
    arr = GeometricConstraintArray()
    try:
        arr.header.stamp = stamp
    except Exception:
        pass

    for item in geometric_constraints:
        if not isinstance(item, dict):
            continue

        gc = GeometricConstraint()
        gc.name = str(item.get("name", ""))
        gc.frame_id = str(item.get("frame", item.get("frame_id", "")))
        gc.type = int(yaml_type_to_uint8(item.get("type", "")))

        if gc.type == GeometricConstraint.PLANE:
            set_xyz(gc.plane_origin, item.get("origin", None), "origin")
            set_xyz(gc.plane_normal, item.get("normal", None), "normal")
            tol = item.get("plane_tolerance", item.get("tolerance", 0.0))
            gc.plane_tolerance = float(tol)

        elif gc.type == GeometricConstraint.LINE:
            set_xyz(gc.line_origin, item.get("origin", None), "origin")
            set_xyz(gc.line_direction, item.get("direction", None), "direction")
            gc.line_max_distance = float(item.get("max_distance", item.get("line_max_distance", 0.0)))

        elif gc.type == GeometricConstraint.ANGLE:
            set_xyz(gc.max_angle, item.get("max_angle", None), "max_angle")

        else:
            raise RuntimeError(f"GeometricConstraint.type non gestito: {gc.type}")

        arr.constraints.append(gc)

    return arr


# ---------------------------------------------------------------------------
# MotionPlanRequest – joint goal
# ---------------------------------------------------------------------------

def build_motion_plan_request(
    goal_configuration: List[float],
    start_state: RobotState,
    *,
    group_name: str,
    joint_names: List[str],
    num_planning_attempts: int,
    allowed_planning_time: float,
    max_vel: float,
    max_acc: float,
    joint_tolerance: float,
) -> MotionPlanRequest:
    """Build a joint-space MotionPlanRequest."""
    if len(goal_configuration) != len(joint_names):
        raise RuntimeError("goal_configuration size mismatch con joint_names.")

    req = MotionPlanRequest()
    req.group_name = group_name
    req.num_planning_attempts = num_planning_attempts
    req.allowed_planning_time = allowed_planning_time
    req.max_velocity_scaling_factor = max_vel
    req.max_acceleration_scaling_factor = max_acc
    req.start_state = start_state

    c = Constraints()
    c.name = "joint_goal"
    for jn, pos in zip(joint_names, goal_configuration):
        jc = JointConstraint()
        jc.joint_name = str(jn)
        jc.position = float(pos)
        jc.tolerance_above = joint_tolerance
        jc.tolerance_below = joint_tolerance
        jc.weight = 1.0
        c.joint_constraints.append(jc)
    req.goal_constraints.append(c)
    return req


# ---------------------------------------------------------------------------
# MotionPlanRequest – Cartesian goal
# ---------------------------------------------------------------------------

def build_motion_plan_request_from_cartesian_goal(
    cartesian_goal: Dict[str, Any],
    start_state: RobotState,
    *,
    group_name: str,
    num_planning_attempts: int,
    allowed_planning_time: float,
    max_vel: float,
    max_acc: float,
) -> MotionPlanRequest:
    """Build a Cartesian MotionPlanRequest."""
    req = MotionPlanRequest()
    req.group_name = group_name
    req.num_planning_attempts = num_planning_attempts
    req.allowed_planning_time = allowed_planning_time
    req.max_velocity_scaling_factor = max_vel
    req.max_acceleration_scaling_factor = max_acc
    req.start_state = start_state

    c = Constraints()
    c.name = "cartesian_goal"

    pose = Pose()
    pos = cartesian_goal.get("position", [0, 0, 0])
    pose.position.x = float(pos[0]) if len(pos) > 0 else 0.0
    pose.position.y = float(pos[1]) if len(pos) > 1 else 0.0
    pose.position.z = float(pos[2]) if len(pos) > 2 else 0.0

    orient = cartesian_goal.get("orientation", [0, 0, 0, 1])
    pose.orientation.x = float(orient[0]) if len(orient) > 0 else 0.0
    pose.orientation.y = float(orient[1]) if len(orient) > 1 else 0.0
    pose.orientation.z = float(orient[2]) if len(orient) > 2 else 0.0
    pose.orientation.w = float(orient[3]) if len(orient) > 3 else 1.0

    pos_constraints = PositionConstraint()
    ori_constraints = OrientationConstraint()

    pos_constraints.link_name = cartesian_goal.get("link_name", "")
    pos_constraints.target_point_offset.x = pose.position.x
    pos_constraints.target_point_offset.y = pose.position.y
    pos_constraints.target_point_offset.z = pose.position.z
    pos_constraints.constraint_region = BoundingVolume()
    pos_constraints.weight = 1.0
    pos_constraints.header.frame_id = cartesian_goal.get("frame_id", "")

    ori_constraints.link_name = cartesian_goal.get("link_name", "")
    ori_constraints.orientation = pose.orientation
    ori_constraints.absolute_x_axis_tolerance = 0.1
    ori_constraints.absolute_y_axis_tolerance = 0.1
    ori_constraints.absolute_z_axis_tolerance = 0.1
    ori_constraints.weight = 1.0
    ori_constraints.header.frame_id = cartesian_goal.get("frame_id", "")

    c.position_constraints.append(pos_constraints)
    c.orientation_constraints.append(ori_constraints)
    req.goal_constraints.append(c)
    return req
