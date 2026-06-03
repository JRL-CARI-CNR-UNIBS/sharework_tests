"""
action_client.py

Generic low-level helper for sending goals to ROS 2 action servers
and blocking until the result arrives.
"""

from typing import Any

import rclpy
from rclpy.action import ActionClient
from rclpy.node import Node


def send_goal_and_wait(node: Node, client: ActionClient, goal_msg: Any, label: str) -> Any:
    """Send *goal_msg* to *client* and block until the result arrives.

    Raises RuntimeError if the goal is rejected.
    """
    goal_future = client.send_goal_async(goal_msg)
    rclpy.spin_until_future_complete(node, goal_future)
    goal_handle = goal_future.result()
    if goal_handle is None or not goal_handle.accepted:
        raise RuntimeError(f"Goal rifiutato da action '{label}'")
    result_future = goal_handle.get_result_async()
    rclpy.spin_until_future_complete(node, result_future)
    return result_future.result().result
