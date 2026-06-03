"""
grasp_service.py

Wrapper around the /get_grasps ROS 2 service (AnyGrasp).

Calls the service, transforms every detected pose from camera frame to
world frame via TF, and returns candidates sorted by score descending.
"""

from typing import List, Tuple

import rclpy
from geometry_msgs.msg import PoseStamped
from grasp_detection_msgs.srv import GetGrasps
from rclpy.node import Node
from tf2_geometry_msgs import do_transform_pose
from tf2_ros import TransformException
from tf2_ros.buffer import Buffer


def call_get_grasps(
    node: Node,
    grasp_client,
    tf_buffer: Buffer,
    pose_publisher,
    *,
    top_k: int = 20,
    wait_for_server_sec: float = 10.0,
) -> List[Tuple[PoseStamped, PoseStamped, float]]:
    """Call ``/get_grasps``, transform poses to world frame, and return the
    top *top_k* candidates sorted by score descending.

    Returns an empty list when no grasps are available or TF is missing.
    Raises RuntimeError if the service call itself fails.
    """
    if not grasp_client.wait_for_service(timeout_sec=wait_for_server_sec):
        raise RuntimeError(f"Servizio '/get_grasps' non disponibile entro {wait_for_server_sec}s.")

    request = GetGrasps.Request()
    future = grasp_client.call_async(request)
    rclpy.spin_until_future_complete(node, future)
    response = future.result()

    if not response:
        raise RuntimeError("Errore nella chiamata al servizio '/get_grasps'.")

    if not response.poses.poses:
        return []

    # Publish for debug / RViz visualisation
    pose_publisher.publish(response.poses)

    camera_frame = response.poses.header.frame_id
    try:
        tf = tf_buffer.lookup_transform("world", camera_frame, rclpy.time.Time())
    except TransformException as ex:
        node.get_logger().warn(f"TF non disponibile ({camera_frame}->world): {ex}")
        return []

    poses = response.poses.poses
    scores = (
        list(response.scores)
        if hasattr(response, "scores") and response.scores
        else [0.0] * len(poses)
    )
    n = min(len(poses), len(scores))

    candidates: List[Tuple[PoseStamped, PoseStamped, float]] = []
    for i in range(n):
        ps_cam = PoseStamped()
        ps_cam.header = response.poses.header
        ps_cam.pose = poses[i]

        pose_world = do_transform_pose(ps_cam.pose, tf)

        ps_world = PoseStamped()
        ps_world.header = ps_cam.header
        ps_world.header.frame_id = "world"
        ps_world.pose = pose_world

        candidates.append((ps_cam, ps_world, float(scores[i])))

    candidates.sort(key=lambda x: x[2], reverse=True)
    return candidates[:top_k]
