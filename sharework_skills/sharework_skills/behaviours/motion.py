"""
behaviours/motion.py

ExecuteMotionTask — py_trees Behaviour that plans and executes one motion
task (joint-space or Cartesian) using the shared RobotContext.
"""

import py_trees

from ..constraints import (
    build_geometric_constraints_array,
    build_motion_plan_request,
    build_motion_plan_request_from_cartesian_goal,
)
from ..motion_service import plan_and_execute


class ExecuteMotionTask(py_trees.behaviour.Behaviour):
    """Plan and execute a single joint-space or Cartesian motion task.

    Parameters
    ----------
    name:
        Human-readable name shown in the BT visualisation.
    task:
        Raw dict parsed from the YAML tasks list.  Must contain either
        ``goal_configuration`` (list of floats) or ``cartesian_goal`` (dict).
    ctx:
        Shared :class:`~sharework_skills.robot_context.RobotContext` instance.
    """

    def __init__(self, name: str, task: dict, ctx) -> None:
        super().__init__(name)
        self._task = task
        self._ctx = ctx

    def update(self) -> py_trees.common.Status:
        ctx = self._ctx
        t = self._task

        try:
            start_state = ctx.build_start_state()

            if "goal_configuration" in t:
                goal_conf = t["goal_configuration"]
                if not isinstance(goal_conf, list):
                    raise RuntimeError("goal_configuration deve essere una lista di float.")
                mpr = build_motion_plan_request(
                    goal_conf,
                    start_state,
                    group_name=ctx.group_name,
                    joint_names=ctx.joint_names,
                    num_planning_attempts=ctx.num_planning_attempts,
                    allowed_planning_time=ctx.allowed_planning_time,
                    max_vel=ctx.max_vel,
                    max_acc=ctx.max_acc,
                    joint_tolerance=ctx.joint_tolerance,
                )

            elif "cartesian_goal" in t:
                cartesian_goal = t["cartesian_goal"]
                if not isinstance(cartesian_goal, dict):
                    raise RuntimeError("cartesian_goal deve essere un dizionario.")
                mpr = build_motion_plan_request_from_cartesian_goal(
                    cartesian_goal,
                    start_state,
                    group_name=ctx.group_name,
                    num_planning_attempts=ctx.num_planning_attempts,
                    allowed_planning_time=ctx.allowed_planning_time,
                    max_vel=ctx.max_vel,
                    max_acc=ctx.max_acc,
                )

            else:
                raise RuntimeError(
                    f"Task '{self.name}': nessun goal valido (goal_configuration o cartesian_goal)."
                )

            geom = t.get("geometric_constraints", []) or []
            if not isinstance(geom, list):
                raise RuntimeError("geometric_constraints deve essere una lista.")
            gca = build_geometric_constraints_array(geom, stamp=ctx.get_clock().now().to_msg())

            plan_and_execute(
                ctx,
                ctx.plan_client,
                ctx.tp_client,
                ctx.exec_client,
                mpr,
                gca,
                plan_action_name=ctx.plan_action_name,
                time_param_action_name=ctx.time_param_action_name,
                execute_action_name=ctx.execute_action_name,
                max_vel=ctx.max_vel,
                max_acc=ctx.max_acc,
                cartesian_speed_limited_link=ctx.cartesian_speed_limited_link,
                max_cartesian_speed=ctx.max_cartesian_speed,
                dry_run=ctx.dry_run,
                verbose=ctx.verbose,
            )

        except Exception as exc:
            ctx.get_logger().error(f"[{self.name}] {exc}")
            return py_trees.common.Status.FAILURE

        return py_trees.common.Status.SUCCESS
