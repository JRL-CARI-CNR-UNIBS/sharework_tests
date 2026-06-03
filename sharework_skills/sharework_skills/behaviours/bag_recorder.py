"""
behaviours/bag_recorder.py

StartBag / StopBag — thin py_trees wrappers around bag_recorder_service.

StopBag is safe to call even when no bag is active (returns SUCCESS
silently) so it can be placed in the Recovery branch without guard logic.
"""

import py_trees

from ..bag_recorder_service import start_bag_recording, stop_bag_recording


class StartBag(py_trees.behaviour.Behaviour):
    """Start a bag recording.

    Parameters
    ----------
    name:
        Human-readable label.
    task:
        Dict with keys ``name_prefix`` and ``topics``.
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

        name_prefix = str(t.get("name_prefix", self.name))
        topics = list(t.get("topics", []))
        if not topics:
            ctx.get_logger().error(f"[{self.name}] 'topics' vuoto o mancante.")
            return py_trees.common.Status.FAILURE

        try:
            ctx.active_bag_name = start_bag_recording(
                ctx,
                ctx.bag_start_client,
                name_prefix,
                topics,
                wait_for_server_sec=ctx.wait_for_server_sec,
            )
        except Exception as exc:
            ctx.get_logger().error(f"[{self.name}] {exc}")
            return py_trees.common.Status.FAILURE

        return py_trees.common.Status.SUCCESS


class StopBag(py_trees.behaviour.Behaviour):
    """Stop the active bag recording.

    Parameters
    ----------
    name:
        Human-readable label.
    task:
        Dict with optional keys ``discard`` (bool) and ``description`` (str).
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

        if ctx.active_bag_name is None:
            # No bag active — nothing to stop (safe in recovery path)
            ctx.get_logger().warn(f"[{self.name}] Nessuna bag attiva, skip.")
            return py_trees.common.Status.SUCCESS

        try:
            stop_bag_recording(
                ctx,
                ctx.bag_stop_client,
                ctx.active_bag_name,
                discard=bool(t.get("discard", False)),
                description=str(t.get("description", "")),
                wait_for_server_sec=ctx.wait_for_server_sec,
            )
            ctx.active_bag_name = None
        except Exception as exc:
            ctx.get_logger().error(f"[{self.name}] {exc}")
            return py_trees.common.Status.FAILURE

        return py_trees.common.Status.SUCCESS
