"""
behaviours/gripper.py

GripperCommand  — send a gripper goal and check stall / reached.
FullCloseGuard  — return FAILURE if the gripper is fully closed
                  (object not grasped), triggering recovery.
"""

import py_trees

from ..gripper_service import send_gripper_goal


class GripperCommand(py_trees.behaviour.Behaviour):
    """Send a GripperCommand goal.

    Parameters
    ----------
    name:
        Human-readable label.
    task:
        Dict with keys ``position``, ``max_effort``, and optionally
        ``required_stall`` (bool, default False).
    ctx:
        Shared :class:`~sharework_skills.robot_context.RobotContext`.
    """

    def __init__(self, name: str, task: dict, ctx) -> None:
        super().__init__(name)
        self._task = task
        self._ctx = ctx

    def update(self) -> py_trees.common.Status:
        ctx = self._ctx
        t = self._task

        cmd_pos = float(t.get("position", 0.5))
        max_effort = float(t.get("max_effort", 0.5))
        required_stall = bool(t.get("required_stall", False))

        try:
            reached, stalled, final_pos = send_gripper_goal(
                ctx, ctx.gripper_client, cmd_pos, max_effort
            )
        except Exception as exc:
            ctx.get_logger().error(f"[{self.name}] eccezione gripper: {exc}")
            return py_trees.common.Status.FAILURE

        if required_stall and not stalled:
            ctx.get_logger().warn(f"[{self.name}] Stallo richiesto ma non rilevato.")
            return py_trees.common.Status.FAILURE

        if (not required_stall) and (not reached):
            ctx.get_logger().warn(f"[{self.name}] Posizione non raggiunta.")
            return py_trees.common.Status.FAILURE

        if ctx.is_full_close_value(final_pos):
            ctx.get_logger().warn(
                f"[{self.name}] FULL CLOSE rilevato (pos={final_pos:.4f}). Oggetto non afferrato."
            )
            return py_trees.common.Status.FAILURE

        return py_trees.common.Status.SUCCESS


class FullCloseGuard(py_trees.behaviour.Behaviour):
    """Read the gripper joint from the latest JointState and return FAILURE
    if the value is within ``full_close_tol`` of ``full_close_position``.

    Place this immediately after :class:`GripperCommand` inside the Suffix
    Sequence so that a failed grasp is caught early and the Selector fires
    the Recovery branch.
    """

    def __init__(self, name: str, ctx) -> None:
        super().__init__(name)
        self._ctx = ctx

    def update(self) -> py_trees.common.Status:
        ctx = self._ctx
        pos = ctx.get_gripper_position()
        if pos is not None and ctx.is_full_close_value(pos):
            ctx.get_logger().warn(
                f"[{self.name}] FULL CLOSE da joint_states (pos={pos:.4f}). Avvio recovery."
            )
            return py_trees.common.Status.FAILURE
        return py_trees.common.Status.SUCCESS
