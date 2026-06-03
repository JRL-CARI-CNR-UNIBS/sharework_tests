"""
robot_context.py

Owns all ROS 2 clients, parameters, and shared state that every
Behaviour node needs.  Passed by reference so nothing is duplicated.
"""

from typing import List, Optional

import rclpy
import rclpy.duration
from control_msgs.action import GripperCommand as GripperCommandAction
from bag_recorder_msgs.srv import StartRecording, StopRecording
from geometry_msgs.msg import PoseArray, PoseStamped
from grasp_detection_msgs.srv import GetGrasps
from moveit_msgs.action import ExecuteTrajectory
from moveit_msgs.msg import RobotState
from pose_constraints_msgs.action import PlanWithConstraints
from rclpy.action import ActionClient
from rclpy.node import Node
from rclpy.qos import DurabilityPolicy, QoSProfile, ReliabilityPolicy
from sensor_msgs.msg import JointState
from tf2_ros import TransformBroadcaster, TransformListener
from tf2_ros.buffer import Buffer
from time_parametrization_msgs.action import ApplyTimeParametrization


class RobotContext(Node):
    """ROS 2 node that owns all action / service clients, parameters and
    shared mutable state (latest JointState, active bag name, current grasp)."""

    def __init__(self) -> None:
        super().__init__("pose_constraints_pipeline")
        self._declare_parameters()
        self._read_parameters()
        self._validate_parameters()
        self._init_interfaces()

    # ------------------------------------------------------------------
    # Parameters
    # ------------------------------------------------------------------

    def _declare_parameters(self) -> None:
        self.declare_parameter("tasks_yaml_path", "")
        self.declare_parameter("group_name", "")
        self.declare_parameter("joint_names", [""])

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

        self.declare_parameter("gripper_joint_name", "")
        self.declare_parameter("full_close_position", 0.8)
        self.declare_parameter("full_close_tol", 0.01)

        self.declare_parameter("min_grasp_score", 0.0)
        self.declare_parameter("empty_grasp_retries", 15)
        self.declare_parameter("empty_grasp_retry_delay_sec", 0.5)
        self.declare_parameter("max_recompute_attempts", 30)

    def _read_parameters(self) -> None:
        self.tasks_yaml_path = str(self.get_parameter("tasks_yaml_path").value)
        self.group_name = str(self.get_parameter("group_name").value)
        self.joint_names: List[str] = [
            s for s in list(self.get_parameter("joint_names").value) if str(s).strip()
        ]

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

    def _validate_parameters(self) -> None:
        if not self.tasks_yaml_path:
            raise RuntimeError("Parametro 'tasks_yaml_path' vuoto.")
        if not self.group_name:
            raise RuntimeError("Parametro 'group_name' vuoto.")
        if not self.joint_names:
            raise RuntimeError("Parametro 'joint_names' vuoto.")

    # ------------------------------------------------------------------
    # ROS interfaces
    # ------------------------------------------------------------------

    def _init_interfaces(self) -> None:
        # JointState cache
        self._latest_js: Optional[JointState] = None
        self._latest_js_rx_time = None

        qos = QoSProfile(depth=10)
        qos.reliability = ReliabilityPolicy.BEST_EFFORT
        qos.durability = DurabilityPolicy.VOLATILE
        self.create_subscription(JointState, self.joint_states_topic, self._on_joint_state, qos)

        # Action clients
        self.plan_client = ActionClient(self, PlanWithConstraints, self.plan_action_name)
        self.tp_client = ActionClient(self, ApplyTimeParametrization, self.time_param_action_name)
        self.exec_client = ActionClient(self, ExecuteTrajectory, self.execute_action_name)
        self.gripper_client = ActionClient(self, GripperCommandAction, "/robotiq_action_controller/gripper_cmd")

        # Service clients
        self.bag_start_client = self.create_client(StartRecording, "/bag_recorder/start")
        self.bag_stop_client = self.create_client(StopRecording, "/bag_recorder/stop")
        self.grasp_client = self.create_client(GetGrasps, "/get_grasps")

        # Publishers / TF
        self.grasp_pose_publisher = self.create_publisher(PoseArray, "/grasp_poses", 10)
        self.tf_broadcaster = TransformBroadcaster(self)
        self.tf_buffer = Buffer()
        self.tf_listener = TransformListener(self.tf_buffer, self)

        # Shared mutable state written by behaviours
        self.current_grasp_cam: Optional[PoseStamped] = None
        self.current_grasp_world: Optional[PoseStamped] = None
        self.active_bag_name: Optional[str] = None

    # ------------------------------------------------------------------
    # JointState helpers
    # ------------------------------------------------------------------

    def _on_joint_state(self, msg: JointState) -> None:
        self._latest_js = msg
        self._latest_js_rx_time = self.get_clock().now()

    def wait_for_joint_state(self) -> None:
        deadline = self.get_clock().now() + rclpy.duration.Duration(seconds=self.wait_joint_states_sec)
        while rclpy.ok() and self._latest_js is None and self.get_clock().now() < deadline:
            rclpy.spin_once(self, timeout_sec=0.1)
        if self._latest_js is None:
            raise RuntimeError(
                f"Non ho ricevuto alcun JointState su '{self.joint_states_topic}' "
                f"entro {self.wait_joint_states_sec}s."
            )

    def build_start_state(self) -> RobotState:
        if self._latest_js is None:
            raise RuntimeError("JointState non disponibile (cache vuota).")

        js = self._latest_js

        try:
            age = (self.get_clock().now() - self._latest_js_rx_time).nanoseconds / 1e9
            if age > self.max_joint_state_age_sec:
                self.get_logger().warn(
                    f"JointState ricevuto {age:.3f}s fa (> {self.max_joint_state_age_sec:.3f}s)."
                )
        except Exception:
            pass

        if not js.name or not js.position:
            raise RuntimeError("JointState name/position vuoti.")

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
            raise RuntimeError(f"JointState mancanti: {missing}")

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

    # ------------------------------------------------------------------
    # Gripper helpers
    # ------------------------------------------------------------------

    def is_full_close_value(self, value: float) -> bool:
        if value != value:  # NaN guard
            return False
        return abs(float(value) - float(self.full_close_position)) <= float(self.full_close_tol)

    def get_gripper_position(self) -> Optional[float]:
        if not self.gripper_joint_name or self._latest_js is None:
            return None
        try:
            idx = self._latest_js.name.index(self.gripper_joint_name)
            return float(self._latest_js.position[idx])
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Server readiness
    # ------------------------------------------------------------------

    def wait_servers(self) -> None:
        for client, name in [
            (self.plan_client,    self.plan_action_name),
            (self.tp_client,      self.time_param_action_name),
            (self.exec_client,    self.execute_action_name),
            (self.gripper_client, "/robotiq_action_controller/gripper_cmd"),
        ]:
            self.get_logger().info(f"Attendo action server: {name}")
            if not client.wait_for_server(timeout_sec=self.wait_for_server_sec):
                raise RuntimeError(
                    f"Action server non disponibile entro {self.wait_for_server_sec}s: {name}"
                )
