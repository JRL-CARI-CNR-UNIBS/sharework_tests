"""
behaviours/perception.py

GetGrasps — call the /get_grasps service, filter by min_grasp_score and
write the best candidate into ctx.current_grasp_cam / ctx.current_grasp_world.

Returns FAILURE when no valid grasp is found so the Retry decorator can
re-attempt automatically.
"""

import time

import py_trees
from geometry_msgs.msg import TransformStamped

from ..grasp_service import call_get_grasps


class GetGrasps(py_trees.behaviour.Behaviour):
    """Perception behaviour: call /get_grasps and store the best result.

    Parameters
    ----------
    name:
        Human-readable label.
    ctx:
        Shared :class:`~sharework_skills.robot_context.RobotContext`.
    """

    def __init__(self, name: str, ctx) -> None:
        super().__init__(name)
        self._ctx = ctx

    def update(self) -> py_trees.common.Status:
        ctx = self._ctx

        try:
            candidates = call_get_grasps(
                ctx,
                ctx.grasp_client,
                ctx.tf_buffer,
                ctx.grasp_pose_publisher,
                top_k=20,
                wait_for_server_sec=ctx.wait_for_server_sec,
            )
        except Exception as exc:
            ctx.get_logger().error(f"[{self.name}] Errore servizio grasp: {exc}")
            return py_trees.common.Status.FAILURE

        if not candidates:
            ctx.get_logger().warn(f"[{self.name}] Nessuna grasp rilevata.")
            time.sleep(ctx.empty_grasp_retry_delay_sec)
            return py_trees.common.Status.FAILURE

        ps_cam, ps_world, score = candidates[0]

        if score < ctx.min_grasp_score:
            ctx.get_logger().warn(
                f"[{self.name}] Score troppo basso ({score:.3f} < {ctx.min_grasp_score:.3f})."
            )
            time.sleep(ctx.empty_grasp_retry_delay_sec)
            return py_trees.common.Status.FAILURE

        ctx.get_logger().info(f"[{self.name}] Grasp accettata (score={score:.3f}).")
        ctx.current_grasp_cam = ps_cam
        ctx.current_grasp_world = ps_world
        self._publish_grasp_transforms()
        return py_trees.common.Status.SUCCESS

    # ------------------------------------------------------------------
    # TF helpers (mirrors node._publish_grasp_transforms)
    # ------------------------------------------------------------------

    def _publish_grasp_transforms(self) -> None:
        ctx = self._ctx
        if not ctx.current_grasp_cam or not ctx.current_grasp_world:
            return

        now = ctx.get_clock().now().to_msg()

        grasp_tf_cam = TransformStamped()
        grasp_tf_cam.header = ctx.current_grasp_cam.header
        grasp_tf_cam.header.stamp = now
        grasp_tf_cam.child_frame_id = "grasp_frame2"
        grasp_tf_cam.transform.translation.x = ctx.current_grasp_cam.pose.position.x
        grasp_tf_cam.transform.translation.y = ctx.current_grasp_cam.pose.position.y
        grasp_tf_cam.transform.translation.z = ctx.current_grasp_cam.pose.position.z
        grasp_tf_cam.transform.rotation = ctx.current_grasp_cam.pose.orientation
        ctx.tf_broadcaster.sendTransform(grasp_tf_cam)

        grasp_tf_world = TransformStamped()
        grasp_tf_world.header = ctx.current_grasp_world.header
        grasp_tf_world.header.stamp = now
        grasp_tf_world.child_frame_id = "grasp_frame"
        grasp_tf_world.transform.translation.x = ctx.current_grasp_world.pose.position.x
        grasp_tf_world.transform.translation.y = ctx.current_grasp_world.pose.position.y
        grasp_tf_world.transform.translation.z = ctx.current_grasp_world.pose.position.z
        grasp_tf_world.transform.rotation = ctx.current_grasp_world.pose.orientation
        ctx.tf_broadcaster.sendTransform(grasp_tf_world)

        approach_tf = TransformStamped()
        approach_tf.header = ctx.current_grasp_world.header
        approach_tf.header.stamp = now
        approach_tf.child_frame_id = "approach_frame"
        approach_tf.transform.translation.x = ctx.current_grasp_world.pose.position.x
        approach_tf.transform.translation.y = ctx.current_grasp_world.pose.position.y
        approach_tf.transform.translation.z = ctx.current_grasp_world.pose.position.z + 0.15
        approach_tf.transform.rotation = ctx.current_grasp_world.pose.orientation
        ctx.tf_broadcaster.sendTransform(approach_tf)
