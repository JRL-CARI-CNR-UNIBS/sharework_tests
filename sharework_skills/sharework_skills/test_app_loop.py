#!/usr/bin/env python3
"""
test_app_loop.py

Loop multi-oggetto robusto per pick&place con AnyGrasp.

REQUISITI (come richiesto):
1) NON proviamo mai candidati successivi "in memoria".
   - Prendiamo sempre e solo il grasp migliore (score massimo).
2) Qualsiasi errore durante la SUFFIX (approach/grasp/close/return/place/drop/open) =>
   - recovery (apri + return)
   - ricalcolo grasp da zero (nuova chiamata a /get_grasps)
3) Se /get_grasps ritorna vuoto (o score migliore troppo basso) => retry 5 volte con pausa 0.5s.
4) Check "FULL CLOSE":
   - se il gripper risulta chiuso al 100% (full_close_position) durante la SUFFIX => errore => recovery => ricalcolo.
   - check sia sul result dell'azione gripper, sia su /joint_states (dopo i movimenti).

PARAMETRI (pipeline_params.yaml, sotto pose_constraints_pipeline/ros__parameters):
- gripper_joint_name: "ur10e_robotiq_85_left_knuckle_joint"
- full_close_position: 0.8
- full_close_tol: 0.01
- min_grasp_score: 0.05
- empty_grasp_retries: 15
- empty_grasp_retry_delay_sec: 0.5
"""

import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import rclpy
from rclpy.action import ActionClient
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, DurabilityPolicy

import yaml

from sensor_msgs.msg import JointState

# Actions
from pose_constraints_msgs.action import PlanWithConstraints
from time_parametrization_msgs.action import ApplyTimeParametrization
from moveit_msgs.action import ExecuteTrajectory
from control_msgs.action import GripperCommand

# Service
from grasp_detection_msgs.srv import GetGrasps

# TF
from tf2_ros import TransformBroadcaster, TransformListener, TransformException
from tf2_ros.buffer import Buffer
from geometry_msgs.msg import PoseStamped, PoseArray, Pose, TransformStamped
from tf2_geometry_msgs import do_transform_pose  # in Humble spesso vuole Pose, non PoseStamped

# Messages
from pose_constraints_msgs.msg import GeometricConstraintArray, GeometricConstraint
from moveit_msgs.msg import (
    MotionPlanRequest,
    MotionPlanResponse,
    Constraints,
    JointConstraint,
    MoveItErrorCodes,
    RobotState,
    PositionConstraint,
    OrientationConstraint,
    BoundingVolume,
)


# =========================
# Utility
# =========================
def _set_xyz(obj: Any, xyz: Any, field_name: str) -> None:
    if xyz is None:
        return
    if not (isinstance(xyz, (list, tuple)) and len(xyz) == 3):
        raise RuntimeError(f"Campo '{field_name}' atteso come lista [x,y,z], ricevuto: {xyz}")
    if not (hasattr(obj, "x") and hasattr(obj, "y") and hasattr(obj, "z")):
        raise RuntimeError(f"Campo '{field_name}' non ha attributi x/y/z (tipo inatteso).")
    obj.x = float(xyz[0])
    obj.y = float(xyz[1])
    obj.z = float(xyz[2])


def _is_success(error_code_msg: Any) -> bool:
    try:
        return int(error_code_msg.val) == int(MoveItErrorCodes.SUCCESS)
    except Exception:
        return False


# =========================
# Control exceptions
# =========================
class FullCloseError(RuntimeError):
    """Il gripper risulta a chiusura completa (evento che nel tuo caso implica: oggetto non in mano/scena cambiata)."""
    pass


# =========================
# Node
# =========================
class PoseConstraintsPipelineNode(Node):
    def __init__(self) -> None:
        # stesso nome del nodo originale: pipeline_params.yaml si applica senza workaround
        super().__init__("pose_constraints_pipeline")

        # ---- Parameters (pipeline)
        self.declare_parameter("tasks_yaml_path", "")
        self.declare_parameter("group_name", "")
        self.declare_parameter("joint_names", [""])  # default non vuoto -> STRING_ARRAY

        self.declare_parameter("joint_states_topic", "/joint_states")
        self.declare_parameter("wait_joint_states_sec", 5.0)
        self.declare_parameter("max_joint_state_age_sec", 1.0)

        self.declare_parameter("joint_tolerance", 0.001)
        self.declare_parameter("num_planning_attempts", 1)
        self.declare_parameter("allowed_planning_time", 15.0)

        self.declare_parameter("max_velocity_scaling_factor", 0.1)
        self.declare_parameter("max_acceleration_scaling_factor", 0.1)

        self.declare_parameter("cartesian_speed_limited_link", "")
        self.declare_parameter("max_cartesian_speed", 0.25)

        self.declare_parameter("verbose", False)
        self.declare_parameter("stop_on_error", True)
        self.declare_parameter("dry_run", False)

        self.declare_parameter("plan_action_name", "/plan_with_constraints")
        self.declare_parameter("time_param_action_name", "/apply_time_parametrization")
        self.declare_parameter("execute_action_name", "/execute_trajectory")
        self.declare_parameter("wait_for_server_sec", 10.0)

        # ---- FULL CLOSE parameters
        self.declare_parameter("gripper_joint_name", "")
        self.declare_parameter("full_close_position", 0.8)
        self.declare_parameter("full_close_tol", 0.01)

        # ---- Grasp quality gate
        self.declare_parameter("min_grasp_score", 0.0)

        # ---- Retry grasp detection when empty/low-score
        self.declare_parameter("empty_grasp_retries", 15)
        self.declare_parameter("empty_grasp_retry_delay_sec", 0.5)

        # ---- Safety: evita loop infinito
        self.declare_parameter("max_recompute_attempts", 30)

        # ---- Read params
        self.tasks_yaml_path = str(self.get_parameter("tasks_yaml_path").value)

        self.group_name = str(self.get_parameter("group_name").value)
        self.get_logger().info(f"group_name = {self.group_name}")

        self.joint_names = [s for s in list(self.get_parameter("joint_names").value) if str(s).strip()]

        self.joint_states_topic = str(self.get_parameter("joint_states_topic").value)
        self.wait_joint_states_sec = float(self.get_parameter("wait_joint_states_sec").value)
        self.max_joint_state_age_sec = float(self.get_parameter("max_joint_state_age_sec").value)

        self.joint_tolerance = float(self.get_parameter("joint_tolerance").value)
        self.num_planning_attempts = int(self.get_parameter("num_planning_attempts").value)
        self.allowed_planning_time = float(self.get_parameter("allowed_planning_time").value)

        self.max_vel = float(self.get_parameter("max_velocity_scaling_factor").value)
        self.max_acc = float(self.get_parameter("max_acceleration_scaling_factor").value)

        self.cartesian_speed_limited_link = str(self.get_parameter("cartesian_speed_limited_link").value)
        self.max_cartesian_speed = float(self.get_parameter("max_cartesian_speed").value)

        self.verbose = bool(self.get_parameter("verbose").value)
        self.stop_on_error = bool(self.get_parameter("stop_on_error").value)
        self.dry_run = bool(self.get_parameter("dry_run").value)

        self.plan_action_name = str(self.get_parameter("plan_action_name").value)
        self.time_param_action_name = str(self.get_parameter("time_param_action_name").value)
        self.execute_action_name = str(self.get_parameter("execute_action_name").value)
        self.wait_for_server_sec = float(self.get_parameter("wait_for_server_sec").value)

        self.gripper_joint_name = str(self.get_parameter("gripper_joint_name").value)
        self.full_close_position = float(self.get_parameter("full_close_position").value)
        self.full_close_tol = float(self.get_parameter("full_close_tol").value)

        self.min_grasp_score = float(self.get_parameter("min_grasp_score").value)
        self.empty_grasp_retries = int(self.get_parameter("empty_grasp_retries").value)
        self.empty_grasp_retry_delay_sec = float(self.get_parameter("empty_grasp_retry_delay_sec").value)

        self.max_recompute_attempts = int(self.get_parameter("max_recompute_attempts").value)

        # ---- Validate
        if not self.tasks_yaml_path:
            raise RuntimeError("Parametro 'tasks_yaml_path' vuoto.")
        if not self.group_name:
            raise RuntimeError("Parametro 'group_name' vuoto.")
        if not self.joint_names:
            raise RuntimeError("Parametro 'joint_names' vuoto.")

        # ---- JointState cache
        self._latest_js: Optional[JointState] = None
        self._latest_js_rx_time = None

        qos = QoSProfile(depth=10)
        qos.reliability = ReliabilityPolicy.BEST_EFFORT
        qos.durability = DurabilityPolicy.VOLATILE
        self._js_sub = self.create_subscription(JointState, self.joint_states_topic, self._on_joint_state, qos)

        # ---- Action clients
        self.plan_client = ActionClient(self, PlanWithConstraints, self.plan_action_name)
        self.tp_client = ActionClient(self, ApplyTimeParametrization, self.time_param_action_name)
        self.exec_client = ActionClient(self, ExecuteTrajectory, self.execute_action_name)
        self.gripper_client = ActionClient(self, GripperCommand, "/robotiq_action_controller/gripper_cmd")

        # ---- Grasp service client + debug publisher + TF
        self.grasp_client = self.create_client(GetGrasps, "/get_grasps")
        self.publisher = self.create_publisher(PoseArray, "/grasp_poses", 10)

        self.tf_broadcaster = TransformBroadcaster(self)
        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(self.tf_buffer, self)

        self.grasp_pose: Optional[PoseStamped] = None          # in camera frame
        self.grasp_pose_in_world: Optional[PoseStamped] = None # in world frame
        self.tf_timer = self.create_timer(0.5, self.sendTransform)

        # flag: siamo nella SUFFIX del candidato corrente?
        self._in_candidate_suffix = False

    # =========================
    # JointState helpers
    # =========================
    def _on_joint_state(self, msg: JointState) -> None:
        self._latest_js = msg
        self._latest_js_rx_time = self.get_clock().now()

    def _wait_for_joint_state(self) -> None:
        deadline = self.get_clock().now() + rclpy.duration.Duration(seconds=self.wait_joint_states_sec)
        while rclpy.ok() and self._latest_js is None and self.get_clock().now() < deadline:
            rclpy.spin_once(self, timeout_sec=0.1)
        if self._latest_js is None:
            raise RuntimeError(
                f"Non ho ricevuto alcun JointState su '{self.joint_states_topic}' entro {self.wait_joint_states_sec}s."
            )

    def _build_start_state_from_joint_states(self) -> RobotState:
        if self._latest_js is None:
            raise RuntimeError("JointState non disponibile (cache vuota).")

        js = self._latest_js

        # warning se vecchio
        try:
            age = (self.get_clock().now() - self._latest_js_rx_time).nanoseconds / 1e9
            if age > self.max_joint_state_age_sec:
                self.get_logger().warn(
                    f"JointState ricevuto {age:.3f}s fa (> {self.max_joint_state_age_sec:.3f}s). "
                    "Potrebbe non rappresentare lo stato corrente."
                )
        except Exception:
            pass

        if not js.name or not js.position:
            raise RuntimeError("JointState name/position vuoti sul topic joint_states.")

        name_to_index = {n: i for i, n in enumerate(js.name)}

        ordered_pos: List[float] = []
        missing: List[str] = []
        for jn in self.joint_names:
            if jn not in name_to_index:
                missing.append(jn)
                continue
            idx = name_to_index[jn]
            if idx >= len(js.position):
                missing.append(jn)
                continue
            ordered_pos.append(float(js.position[idx]))

        if missing:
            raise RuntimeError(f"JointState non contiene tutti i giunti richiesti. Mancanti: {missing}")

        start = RobotState()
        start.joint_state.name = list(self.joint_names)
        start.joint_state.position = ordered_pos
        try:
            start.joint_state.header.stamp = js.header.stamp
        except Exception:
            pass
        try:
            start.is_diff = False
        except Exception:
            pass
        return start

    # =========================
    # FULL CLOSE helpers
    # =========================
    def _is_full_close_value(self, value: float) -> bool:
        if value != value:  # NaN
            return False
        return abs(float(value) - float(self.full_close_position)) <= float(self.full_close_tol)

    def _get_joint_position(self, joint_name: str) -> Optional[float]:
        if not joint_name or self._latest_js is None:
            return None
        try:
            idx = self._latest_js.name.index(joint_name)
            return float(self._latest_js.position[idx])
        except Exception:
            return None

    def _raise_if_gripper_full_close_from_joint_states(self, where: str) -> None:
        if not self.gripper_joint_name:
            return
        pos = self._get_joint_position(self.gripper_joint_name)
        if pos is None:
            return
        if self._is_full_close_value(pos):
            raise FullCloseError(
                f"[FULL CLOSE] (joint_states) {where}: {self.gripper_joint_name}={pos:.4f} "
                f"(~={self.full_close_position:.4f})"
            )

    # =========================
    # Servers
    # =========================
    def _wait_servers(self) -> None:
        for client, name in [
            (self.plan_client, self.plan_action_name),
            (self.tp_client, self.time_param_action_name),
            (self.exec_client, self.execute_action_name),
            (self.gripper_client, "/robotiq_action_controller/gripper_cmd"),
        ]:
            self.get_logger().info(f"Attendo action server: {name}")
            if not client.wait_for_server(timeout_sec=self.wait_for_server_sec):
                raise RuntimeError(f"Action server non disponibile entro {self.wait_for_server_sec}s: {name}")

    # =========================
    # YAML
    # =========================
    def _load_tasks(self) -> List[Dict[str, Any]]:
        p = Path(self.tasks_yaml_path)
        if not p.exists():
            raise RuntimeError(f"File YAML non trovato: {p}")

        data = yaml.safe_load(p.read_text())
        if not isinstance(data, dict) or "tasks" not in data:
            raise RuntimeError("YAML non valido: atteso mapping con chiave 'tasks'.")

        tasks = data["tasks"]
        if not isinstance(tasks, list):
            raise RuntimeError("YAML non valido: 'tasks' deve essere una lista.")
        return tasks

    # =========================
    # Constraints builders
    # =========================
    def _yaml_type_to_uint8(self, t: Any) -> int:
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

    def _build_geometric_constraints_array(self, geometric_constraints: List[Dict[str, Any]]) -> GeometricConstraintArray:
        arr = GeometricConstraintArray()
        try:
            arr.header.stamp = self.get_clock().now().to_msg()
        except Exception:
            pass

        for item in geometric_constraints:
            if not isinstance(item, dict):
                continue

            gc = GeometricConstraint()
            gc.name = str(item.get("name", ""))
            gc.frame_id = str(item.get("frame", item.get("frame_id", "")))
            gc.type = int(self._yaml_type_to_uint8(item.get("type", "")))

            if gc.type == GeometricConstraint.PLANE:
                _set_xyz(gc.plane_origin, item.get("origin", None), "origin")
                _set_xyz(gc.plane_normal, item.get("normal", None), "normal")
                tol = item.get("plane_tolerance", item.get("tolerance", 0.0))
                gc.plane_tolerance = float(tol)

            elif gc.type == GeometricConstraint.LINE:
                _set_xyz(gc.line_origin, item.get("origin", None), "origin")
                _set_xyz(gc.line_direction, item.get("direction", None), "direction")
                gc.line_max_distance = float(item.get("max_distance", item.get("line_max_distance", 0.0)))

            elif gc.type == GeometricConstraint.ANGLE:
                _set_xyz(gc.max_angle, item.get("max_angle", None), "max_angle")

            else:
                raise RuntimeError(f"GeometricConstraint.type non gestito: {gc.type}")

            arr.constraints.append(gc)

        return arr

    def _build_motion_plan_request(self, goal_configuration: List[float], start_state: RobotState) -> MotionPlanRequest:
        if len(goal_configuration) != len(self.joint_names):
            raise RuntimeError("goal_configuration size mismatch con joint_names.")

        req = MotionPlanRequest()
        req.group_name = self.group_name
        req.num_planning_attempts = self.num_planning_attempts
        req.allowed_planning_time = self.allowed_planning_time
        req.max_velocity_scaling_factor = self.max_vel
        req.max_acceleration_scaling_factor = self.max_acc
        req.start_state = start_state

        c = Constraints()
        c.name = "joint_goal"
        for jn, pos in zip(self.joint_names, goal_configuration):
            jc = JointConstraint()
            jc.joint_name = str(jn)
            jc.position = float(pos)
            jc.tolerance_above = self.joint_tolerance
            jc.tolerance_below = self.joint_tolerance
            jc.weight = 1.0
            c.joint_constraints.append(jc)
        req.goal_constraints.append(c)
        return req

    def _build_motion_plan_request_from_cartesian_goal(self, cartesian_goal: Dict[str, Any], start_state: RobotState) -> MotionPlanRequest:
        req = MotionPlanRequest()
        req.group_name = self.group_name
        req.num_planning_attempts = self.num_planning_attempts
        req.allowed_planning_time = self.allowed_planning_time
        req.max_velocity_scaling_factor = self.max_vel
        req.max_acceleration_scaling_factor = self.max_acc
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

    # =========================
    # Action helpers
    # =========================
    def _send_goal_and_wait(self, client: ActionClient, goal_msg: Any, label: str) -> Any:
        goal_future = client.send_goal_async(goal_msg)
        rclpy.spin_until_future_complete(self, goal_future)
        goal_handle = goal_future.result()
        if goal_handle is None or not goal_handle.accepted:
            raise RuntimeError(f"Goal rifiutato da action '{label}'")
        result_future = goal_handle.get_result_async()
        rclpy.spin_until_future_complete(self, result_future)
        return result_future.result().result

    def _send_gripper_goal(self, position: float, max_effort: float) -> Tuple[bool, bool, float]:
        goal_msg = GripperCommand.Goal()
        goal_msg.command.position = float(position)
        goal_msg.command.max_effort = float(max_effort)

        self.get_logger().info(f"Invio comando gripper: position={position}, max_effort={max_effort}")

        goal_future = self.gripper_client.send_goal_async(goal_msg)
        rclpy.spin_until_future_complete(self, goal_future)
        goal_handle = goal_future.result()

        if not goal_handle.accepted:
            self.get_logger().error("Goal gripper rifiutato.")
            return False, False, float("nan")

        result_future = goal_handle.get_result_async()
        rclpy.spin_until_future_complete(self, result_future)
        result = result_future.result().result

        if not result:
            self.get_logger().error("Risultato gripper non disponibile.")
            return False, False, float("nan")

        self.get_logger().info(
            f"Comando gripper completato: position={result.position}, reached={result.reached_goal}, stalled={result.stalled}"
        )
        #time.sleep(0.5)  # attendo un attimo per permettere a joint_states di aggiornarsi (e rilevare eventuale full close)
        return bool(result.reached_goal), bool(result.stalled), float(result.position)

    # =========================
    # AnyGrasp
    # =========================
    def _call_get_grasps_service_all(self, top_k: int = 20) -> List[Tuple[PoseStamped, PoseStamped, float]]:
        if not self.grasp_client.wait_for_service(timeout_sec=self.wait_for_server_sec):
            raise RuntimeError(f"Servizio '/get_grasps' non disponibile entro {self.wait_for_server_sec}s.")

        request = GetGrasps.Request()
        future = self.grasp_client.call_async(request)
        rclpy.spin_until_future_complete(self, future)
        response = future.result()

        if not response:
            raise RuntimeError("Errore nella chiamata al servizio '/get_grasps'.")

        if not response.poses.poses:
            return []

        # debug
        self.publisher.publish(response.poses)

        camera_frame = response.poses.header.frame_id
        try:
            t = self.tf_buffer.lookup_transform("world", camera_frame, rclpy.time.Time())
        except TransformException as ex:
            self.get_logger().warn(f"TF non disponibile ({camera_frame}->world): {ex}")
            return []

        poses = response.poses.poses
        scores = list(response.scores) if hasattr(response, "scores") and response.scores else [0.0] * len(poses)
        n = min(len(poses), len(scores))

        candidates: List[Tuple[PoseStamped, PoseStamped, float]] = []
        for i in range(n):
            ps_cam = PoseStamped()
            ps_cam.header = response.poses.header
            ps_cam.pose = poses[i]

            pose_world = do_transform_pose(ps_cam.pose, t)  # Pose -> Pose

            ps_world = PoseStamped()
            ps_world.header = ps_cam.header
            ps_world.header.frame_id = "world"
            ps_world.pose = pose_world

            candidates.append((ps_cam, ps_world, float(scores[i])))

        candidates.sort(key=lambda x: x[2], reverse=True)
        return candidates[:top_k]

    # =========================
    # TF publishing (grasp_frame / approach_frame)
    # =========================
    def _set_current_grasp(self, pose_cam: PoseStamped, pose_world: PoseStamped) -> None:
        self.grasp_pose = pose_cam
        self.grasp_pose_in_world = pose_world
        self.sendTransform()

    def sendTransform(self) -> None:
        if not self.grasp_pose or not self.grasp_pose_in_world:
            return

        # grasp_frame2 in camera frame (debug)
        grasp_tf_cam = TransformStamped()
        grasp_tf_cam.header = self.grasp_pose.header
        grasp_tf_cam.header.stamp = self.get_clock().now().to_msg()
        grasp_tf_cam.child_frame_id = "grasp_frame2"
        grasp_tf_cam.transform.translation.x = self.grasp_pose.pose.position.x
        grasp_tf_cam.transform.translation.y = self.grasp_pose.pose.position.y
        grasp_tf_cam.transform.translation.z = self.grasp_pose.pose.position.z
        grasp_tf_cam.transform.rotation = self.grasp_pose.pose.orientation
        self.tf_broadcaster.sendTransform(grasp_tf_cam)

        # grasp_frame in world frame
        grasp_tf_world = TransformStamped()
        grasp_tf_world.header = self.grasp_pose_in_world.header
        grasp_tf_world.header.stamp = self.get_clock().now().to_msg()
        grasp_tf_world.child_frame_id = "grasp_frame"
        grasp_tf_world.transform.translation.x = self.grasp_pose_in_world.pose.position.x
        grasp_tf_world.transform.translation.y = self.grasp_pose_in_world.pose.position.y
        grasp_tf_world.transform.translation.z = self.grasp_pose_in_world.pose.position.z
        grasp_tf_world.transform.rotation = self.grasp_pose_in_world.pose.orientation
        self.tf_broadcaster.sendTransform(grasp_tf_world)

        # approach_frame: 0.15m sopra (z world)
        approach_tf = TransformStamped()
        approach_tf.header = self.grasp_pose_in_world.header
        approach_tf.header.stamp = self.get_clock().now().to_msg()
        approach_tf.child_frame_id = "approach_frame"
        approach_tf.transform.translation.x = self.grasp_pose_in_world.pose.position.x
        approach_tf.transform.translation.y = self.grasp_pose_in_world.pose.position.y
        approach_tf.transform.translation.z = self.grasp_pose_in_world.pose.position.z + 0.15
        approach_tf.transform.rotation = self.grasp_pose_in_world.pose.orientation
        self.tf_broadcaster.sendTransform(approach_tf)

    # =========================
    # Execute single task
    # =========================
    def _execute_task(self, t: Dict[str, Any]) -> None:
        name = str(t.get("name", "task"))
        ttype = str(t.get("type", "")).strip()

        # --- Gripper
        if ttype == "gripper_control/Gripper":
            cmd_pos = float(t.get("position", 0.5))
            max_effort = float(t.get("max_effort", 0.5))
            required_stall = bool(t.get("required_stall", False))

            reached, stalled, final_pos = self._send_gripper_goal(cmd_pos, max_effort)

            if required_stall and not stalled:
                raise RuntimeError(f"Stallo richiesto ma non rilevato per il task: {name}")
            if (not required_stall) and (not reached):
                raise RuntimeError(f"Comando gripper fallito per il task: {name}")

            # full-close check sul result del gripper
            if self._is_full_close_value(final_pos):
                raise FullCloseError(
                    f"[FULL CLOSE] (gripper result) task='{name}': final_pos={final_pos:.4f} "
                    f"(~={self.full_close_position:.4f})"
                )
            return

        # --- Movement
        start_state = self._build_start_state_from_joint_states()

        if "goal_configuration" in t:
            goal_conf = t["goal_configuration"]
            if not isinstance(goal_conf, list):
                raise RuntimeError("goal_configuration deve essere una lista di float.")
            mpr = self._build_motion_plan_request(goal_conf, start_state)

        elif "cartesian_goal" in t:
            cartesian_goal = t["cartesian_goal"]
            if not isinstance(cartesian_goal, dict):
                raise RuntimeError("cartesian_goal deve essere un dizionario.")
            mpr = self._build_motion_plan_request_from_cartesian_goal(cartesian_goal, start_state)

        else:
            raise RuntimeError(f"Task '{name}': nessun goal valido trovato.")

        geom = t.get("geometric_constraints", []) or []
        if not isinstance(geom, list):
            raise RuntimeError("geometric_constraints deve essere una lista.")
        gca = self._build_geometric_constraints_array(geom)

        # 1) Plan
        plan_goal = PlanWithConstraints.Goal()
        plan_goal.motion_plan_request = mpr
        plan_goal.constraints = gca
        plan_goal.verbose = self.verbose

        plan_res = self._send_goal_and_wait(self.plan_client, plan_goal, self.plan_action_name)
        plan_out: MotionPlanResponse = plan_res.motion_plan_response
        if not _is_success(plan_out.error_code):
            raise RuntimeError(f"Planning fallito (error_code={int(plan_out.error_code.val)})")

        # 2) Time parametrization
        tp_goal = ApplyTimeParametrization.Goal()
        tp_goal.unparametrized_motion_plan_response = plan_out
        tp_goal.max_velocity_scaling_factor = float(self.max_vel)
        tp_goal.max_acceleration_scaling_factor = float(self.max_acc)
        tp_goal.cartesian_speed_limited_link = str(self.cartesian_speed_limited_link)
        tp_goal.max_cartesian_speed = float(self.max_cartesian_speed)

        tp_res = self._send_goal_and_wait(self.tp_client, tp_goal, self.time_param_action_name)
        tp_out: MotionPlanResponse = tp_res.motion_plan_response
        if not _is_success(tp_out.error_code):
            raise RuntimeError(f"Time parametrization fallita (error_code={int(tp_out.error_code.val)})")

        # 3) Execute
        if self.dry_run:
            return

        exec_goal = ExecuteTrajectory.Goal()
        exec_goal.trajectory = tp_out.trajectory
        exec_res = self._send_goal_and_wait(self.exec_client, exec_goal, self.execute_action_name)

        try:
            if not _is_success(exec_res.error_code):
                raise RuntimeError(f"Esecuzione fallita (error_code={int(exec_res.error_code.val)})")
        except Exception:
            pass

        # full-close check da joint_states dopo i movimenti (solo in SUFFIX)
        if self._in_candidate_suffix:
            self._raise_if_gripper_full_close_from_joint_states(where=f"after movement '{name}'")

    # =========================
    # Recovery
    # =========================
    def _recovery_open_and_return(self, suffix_tasks: List[Dict[str, Any]]) -> None:
        # 1) apri pinza
        try:
            self._send_gripper_goal(position=0.0, max_effort=50.0)
        except Exception:
            pass

        # 2) esegui return (se presente)
        try:
            return_task = next((tt for tt in suffix_tasks if str(tt.get("name", "")).lower() == "return"), None)
            if return_task is not None:
                old = self._in_candidate_suffix
                self._in_candidate_suffix = False  # evita check durante recovery
                try:
                    self._execute_task(return_task)
                finally:
                    self._in_candidate_suffix = old
        except Exception:
            pass

    # =========================
    # Main loop
    # =========================
    def run(self) -> None:
        self.get_logger().info(f"Attendo JointState su: {self.joint_states_topic}")
        self._wait_for_joint_state()
        self.get_logger().info("JointState ricevuto. Avvio pipeline.")
        self._wait_servers()

        tasks = self._load_tasks()
        self.get_logger().info(f"Caricati {len(tasks)} task da: {self.tasks_yaml_path}")

        grasp_idx = None
        for i, t in enumerate(tasks):
            if str(t.get("type", "")).strip() == "grasp_detection/GetGrasps":
                grasp_idx = i
                break
        if grasp_idx is None:
            raise RuntimeError("Nel YAML non esiste un task di tipo 'grasp_detection/GetGrasps'.")

        prefix_tasks = tasks[:grasp_idx]
        suffix_tasks = tasks[grasp_idx + 1 :]

        cycle_idx = 0
        recompute_attempts = 0

        while rclpy.ok():
            if recompute_attempts >= self.max_recompute_attempts:
                self.get_logger().error(
                    f"Raggiunto max_recompute_attempts={self.max_recompute_attempts}. Interrompo per sicurezza."
                )
                break

            self.get_logger().info("=======================================")
            self.get_logger().info(f"Ciclo pick&place (oggetto) #{cycle_idx + 1}")
            self.get_logger().info("=======================================")

            # ----------------
            # PREFIX
            # ----------------
            try:
                self._in_candidate_suffix = False
                for t in prefix_tasks:
                    self.get_logger().info(f"[PREFIX] {t.get('name', 'task')}")
                    self._execute_task(t)
            except Exception as e:
                self.get_logger().error(f"Errore PREFIX: {e}")
                if self.stop_on_error:
                    raise
                break

            # ----------------
            # PERCEPTION (retry se vuoto o score basso)
            # ----------------
            candidates: List[Tuple[PoseStamped, PoseStamped, float]] = []
            for attempt in range(1, self.empty_grasp_retries + 1):
                candidates = self._call_get_grasps_service_all(top_k=20)

                if not candidates:
                    self.get_logger().warn(
                        f"Nessuna grasp (tentativo {attempt}/{self.empty_grasp_retries}). Riprovo..."
                    )
                    time.sleep(self.empty_grasp_retry_delay_sec)
                    continue

                best_score = float(candidates[0][2])
                if best_score < self.min_grasp_score:
                    self.get_logger().warn(
                        f"Best score troppo basso ({best_score:.3f} < {self.min_grasp_score:.3f}) "
                        f"(tentativo {attempt}/{self.empty_grasp_retries}). Riprovo..."
                    )
                    candidates = []
                    time.sleep(self.empty_grasp_retry_delay_sec)
                    continue

                # ok
                break

            if not candidates:
                self.get_logger().info("Nessuna grasp valida dopo i retry: considero scena vuota. Termino.")
                break

            # ----------------
            # Usa SEMPRE il migliore (no candidato successivo)
            # ----------------
            ps_cam, ps_world, score = candidates[0]
            self.get_logger().info(f"Uso grasp migliore (score={score:.3f}).")
            self._set_current_grasp(ps_cam, ps_world)

            # ----------------
            # SUFFIX
            # ----------------
            try:
                self._in_candidate_suffix = True
                for t in suffix_tasks:
                    self.get_logger().info(f"[SUFFIX] {t.get('name', 'task')}")
                    self._execute_task(t)

                # SUCCESSO
                self._in_candidate_suffix = False
                recompute_attempts = 0
                cycle_idx += 1
                self.get_logger().info("Pick&place completato con successo.")
                continue

            except Exception as e:
                # QUALSIASI errore => recovery + ricalcolo scena
                self._in_candidate_suffix = False
                self.get_logger().warn(f"{e} -> RECOVERY + ricalcolo grasp da zero.")
                self._recovery_open_and_return(suffix_tasks)
                recompute_attempts += 1
                continue

        self.get_logger().info(f"Pipeline completata. Pick&place eseguiti: {cycle_idx}")


def main(argv=None) -> None:
    rclpy.init(args=argv)
    node = None
    try:
        node = PoseConstraintsPipelineNode()
        node.run()

        # Mantieni vivo il nodo (utile per TF/RViz)
        while rclpy.ok():
            rclpy.spin_once(node, timeout_sec=0.1)
            time.sleep(0.1)

    finally:
        if node is not None:
            node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main(sys.argv)