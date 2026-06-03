"""
bag_recorder_service.py

Wrappers around the /bag_recorder/start and /bag_recorder/stop ROS 2 services.
"""

from datetime import datetime
from typing import List

import rclpy
from rclpy.node import Node

from bag_recorder_msgs.srv import StartRecording, StopRecording


def start_bag_recording(
    node: Node,
    client,
    name_prefix: str,
    topics: List[str],
    *,
    wait_for_server_sec: float = 10.0,
) -> str:
    """Call /bag_recorder/start and return the full bag_name used.

    The bag name is ``<name_prefix>_YYYYMMDDHHMMSS``.
    Raises RuntimeError if the service is unavailable or reports failure.
    """
    if not client.wait_for_service(timeout_sec=wait_for_server_sec):
        raise RuntimeError(
            f"Servizio '/bag_recorder/start' non disponibile entro {wait_for_server_sec}s."
        )

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    bag_name = f"{name_prefix}_{timestamp}"

    request = StartRecording.Request()
    request.topics = list(topics)
    request.bag_name = bag_name

    node.get_logger().info(f"Avvio registrazione bag: '{bag_name}' — topics: {topics}")

    future = client.call_async(request)
    rclpy.spin_until_future_complete(node, future)
    response = future.result()

    if response is None:
        raise RuntimeError("Nessuna risposta da '/bag_recorder/start'.")
    if not response.success:
        raise RuntimeError(f"'/bag_recorder/start' fallito: {response.message}")

    node.get_logger().info(f"Bag avviata: '{bag_name}' — {response.message}")
    return bag_name


def stop_bag_recording(
    node: Node,
    client,
    bag_name: str,
    *,
    discard: bool = False,
    description: str = "",
    wait_for_server_sec: float = 10.0,
) -> None:
    """Call /bag_recorder/stop for the given bag_name.

    Raises RuntimeError if the service is unavailable or reports failure.
    """
    if not client.wait_for_service(timeout_sec=wait_for_server_sec):
        raise RuntimeError(
            f"Servizio '/bag_recorder/stop' non disponibile entro {wait_for_server_sec}s."
        )

    request = StopRecording.Request()
    request.bag_name = bag_name
    request.discard = discard
    request.description = description

    node.get_logger().info(
        f"Stop registrazione bag: '{bag_name}' — discard={discard}, description='{description}'"
    )

    future = client.call_async(request)
    rclpy.spin_until_future_complete(node, future)
    response = future.result()

    if response is None:
        raise RuntimeError("Nessuna risposta da '/bag_recorder/stop'.")
    if not response.success:
        raise RuntimeError(f"'/bag_recorder/stop' fallito: {response.message}")

    node.get_logger().info(f"Bag fermata: '{bag_name}' — {response.message}")
