"""
gripper_service.py

Wrapper around the GripperCommand action client.
"""

from typing import Tuple

import rclpy
from control_msgs.action import GripperCommand
from rclpy.action import ActionClient
from rclpy.node import Node


def send_gripper_goal(
    node: Node,
    gripper_client: ActionClient,
    position: float,
    max_effort: float,
) -> Tuple[bool, bool, float]:
    """Send a GripperCommand goal and return ``(reached_goal, stalled, final_position)``.

    Returns ``(False, False, nan)`` on any failure so callers can handle it
    gracefully without a try/except.
    """
    goal_msg = GripperCommand.Goal()
    goal_msg.command.position = float(position)
    goal_msg.command.max_effort = float(max_effort)

    node.get_logger().info(f"Invio comando gripper: position={position}, max_effort={max_effort}")

    goal_future = gripper_client.send_goal_async(goal_msg)
    rclpy.spin_until_future_complete(node, goal_future)
    goal_handle = goal_future.result()

    if not goal_handle.accepted:
        node.get_logger().error("Goal gripper rifiutato.")
        return False, False, float("nan")

    result_future = goal_handle.get_result_async()
    rclpy.spin_until_future_complete(node, result_future)
    result = result_future.result().result

    if not result:
        node.get_logger().error("Risultato gripper non disponibile.")
        return False, False, float("nan")

    node.get_logger().info(
        f"Comando gripper completato: position={result.position}, "
        f"reached={result.reached_goal}, stalled={result.stalled}"
    )
    return bool(result.reached_goal), bool(result.stalled), float(result.position)
